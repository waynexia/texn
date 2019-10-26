use chashmap::CHashMap;
use crossbeam::channel::Select;
use crossbeam::{channel, Receiver, Sender};
use futures::future::BoxFuture;
use futures::prelude::*;
use num_cpus;

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::future::Future;
use std::mem::{forget, ManuallyDrop};
use std::panic;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::time::Instant;

// take how many tasks from a queue in one term
const QUEUE_PRIVILIAGE: &'static [u64] = &[1024, 1, 1, 1, 1];
// the longest executed time a queue can hold (in micros)
const TIME_FEEDBACK: &'static [u64] = &[1_000, 10_000, 100_000, 1_000_000, 10_000_000];
// the most appear times a queue can hold
const CNT_FEEDBACK: &'static [u64] = &[5, 10, 15, 20, 25];

use adaptive_spawn::*;

// fn foo() -> ! {
//     panic!()
// }

#[derive(Clone)]
pub struct ThreadPool {
    queues: Vec<Arc<TaskQueue>>,
    // stats: token -> (appear_times,executed_time(in micros),queue_index)
    stats: Arc<CHashMap<u64, (u64, u64, usize)>>,
}

impl ThreadPool {
    pub fn new(num_threads: usize) -> ThreadPool {
        let mut queues = Vec::new();
        for _ in QUEUE_PRIVILIAGE {
            let (tx, rx) = channel::unbounded();
            let queue = Arc::new(TaskQueue { tx, rx });
            queues.push(queue);
        }
        let stats_for_constructor = Arc::new(CHashMap::new());
        for _ in 0..num_threads {
            let queues = queues.clone();
            let stats = stats_for_constructor.clone();
            thread::spawn(move || {
                let mut sel = Select::new();
                let mut rx_map = HashMap::new();
                for queue in &queues {
                    let idx = sel.recv(&queue.rx);
                    rx_map.insert(idx, &queue.rx);
                }
                loop {
                    let mut is_empty = true;
                    for ((queue, &limit), index) in queues.iter().zip(QUEUE_PRIVILIAGE).zip(0..) {
                        for task in queue.rx.try_iter().take(limit as usize) {
                            is_empty = false;
                            unsafe { poll_with_timer(&stats, task, index) };
                        }
                    }
                    if is_empty {
                        let oper = sel.select();
                        let rx = rx_map.get(&oper.index()).unwrap();
                        let task = oper.recv(*rx).unwrap();
                        let index = task.0.index.load(Ordering::SeqCst);
                        unsafe { poll_with_timer(&stats, task, index) };
                    }
                }
            });
        }
        ThreadPool {
            queues,
            stats: stats_for_constructor,
        }
    }

    pub fn spawn<F>(&self, task: F, token: u64, _nice: u8)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // at begin a token has top priority
        if !self.stats.contains_key(&token) {
            self.stats.insert(token, (1, 0, 0));
        }
        // otherwise use its own priority
        let (cnt, _, mut index) = *self.stats.get(&token).unwrap();
        if cnt > CNT_FEEDBACK[index] && index < CNT_FEEDBACK.len() - 1 {
            index += 1;
        }
        self.stats.upsert(
            token,
            || panic!(),
            |(c, _, i)| {
                *c += 1;
                *i = index
            },
        );
        let _ = self.queues[index]
            .tx
            .send(ArcTask::new(task, self.queues.clone(), token, index));
        // println!("{:?}", self.stats);
    }
}

unsafe fn poll_with_timer(
    stats: &CHashMap<u64, (u64, u64, usize)>,
    task: ArcTask,
    incoming_index: usize,
) {
    let token = task.0.token;
    // adjust queue level
    let (_, elapsed, mut index) = *stats.get(&token).unwrap();
    if incoming_index != index {
        let _ = task.0.queues[index].tx.send(clone_task(&*task.0));
        return;
    }
    if elapsed > TIME_FEEDBACK[index] && index < TIME_FEEDBACK.len() - 1 {
        index += 1;
        task.0.index.store(index, Ordering::SeqCst);
        stats.upsert(token, || panic!(), |(_, _, i)| *i = index);
        let _ = task.0.queues[index].tx.send(clone_task(&*task.0));
        return;
    }
    // polling
    let begin = Instant::now();
    task.poll();
    let elapsed = begin.elapsed().as_micros() as u64;
    stats.upsert(token, || panic!(), |(_, e, _)| *e += elapsed);
}

impl AdaptiveSpawn for ThreadPool {
    fn spawn_opt<Fut>(&self, f: Fut, opt: Options)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn(f, opt.token, opt.nice);
    }
}

impl Default for ThreadPool {
    fn default() -> ThreadPool {
        ThreadPool::new(num_cpus::get())
    }
}

struct TaskQueue {
    tx: Sender<ArcTask>,
    rx: Receiver<ArcTask>,
}

struct Task {
    task: UnsafeCell<BoxFuture<'static, ()>>,
    queues: Vec<Arc<TaskQueue>>,
    status: AtomicU8,
    token: u64,
    // this token belongs to which queue
    index: AtomicUsize,
}

#[derive(Clone)]
struct ArcTask(Arc<Task>);

const WAITING: u8 = 0; // --> POLLING
const POLLING: u8 = 1; // --> WAITING, REPOLL, or COMPLETE
const REPOLL: u8 = 2; // --> POLLING
const COMPLETE: u8 = 3; // No transitions out

unsafe impl Send for Task {}
unsafe impl Sync for Task {}

impl ArcTask {
    fn new<F>(future: F, queues: Vec<Arc<TaskQueue>>, token: u64, index: usize) -> ArcTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let future = Arc::new(Task {
            task: UnsafeCell::new(future.boxed()),
            queues,
            status: AtomicU8::new(WAITING),
            token,
            index: AtomicUsize::new(index),
        });
        let future: *const Task = Arc::into_raw(future) as *const Task;
        unsafe { task(future) }
    }

    unsafe fn poll(self) {
        self.0.status.store(POLLING, Ordering::SeqCst);
        let waker = ManuallyDrop::new(waker(&*self.0));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(_) = (&mut *self.0.task.get()).poll_unpin(&mut cx) {
                break self.0.status.store(COMPLETE, Ordering::SeqCst);
            }
            match self.0.status.compare_exchange(
                POLLING,
                WAITING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(_) => self.0.status.store(POLLING, Ordering::SeqCst),
            }
        }
    }
}

unsafe fn waker(task: *const Task) -> Waker {
    Waker::from_raw(RawWaker::new(
        task as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    ))
}

unsafe fn clone_raw(this: *const ()) -> RawWaker {
    let task = clone_task(this as *const Task);
    RawWaker::new(
        Arc::into_raw(task.0) as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    )
}

unsafe fn drop_raw(this: *const ()) {
    drop(task(this as *const Task))
}

unsafe fn wake_raw(this: *const ()) {
    let task = task(this as *const Task);
    let mut status = task.0.status.load(Ordering::SeqCst);
    loop {
        match status {
            WAITING => {
                match task.0.status.compare_exchange(
                    WAITING,
                    POLLING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        task.0.queues[task.0.index.load(Ordering::SeqCst)]
                            .tx
                            .send(clone_task(&*task.0))
                            .unwrap();
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task.0.status.compare_exchange(
                    POLLING,
                    REPOLL,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

unsafe fn wake_ref_raw(this: *const ()) {
    let task = ManuallyDrop::new(task(this as *const Task));
    let mut status = task.0.status.load(Ordering::SeqCst);
    loop {
        match status {
            WAITING => {
                match task.0.status.compare_exchange(
                    WAITING,
                    POLLING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        task.0.queues[task.0.index.load(Ordering::SeqCst)]
                            .tx
                            .send(clone_task(&*task.0))
                            .unwrap();
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task.0.status.compare_exchange(
                    POLLING,
                    REPOLL,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

unsafe fn task(future: *const Task) -> ArcTask {
    ArcTask(Arc::from_raw(future))
}

unsafe fn clone_task(future: *const Task) -> ArcTask {
    let task = task(future);
    forget(task.clone());
    task
}
