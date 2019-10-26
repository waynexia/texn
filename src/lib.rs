// use chashmap::CHashMap;
use crossbeam::channel::Select;
use crossbeam::{channel, Receiver, Sender};
use futures::future::BoxFuture;
use futures::prelude::*;
use lru_time_cache::LruCache;
use num_cpus;

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::future::Future;
use std::mem::{forget, ManuallyDrop};
// use std::panic;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::time::{Duration, Instant};

// take how many tasks from a queue in one term
const QUEUE_PRIVILIAGE: &'static [u64] = &[1024, 1, 1];
// the longest executed time a queue can hold (in micros)
const TIME_FEEDBACK: &'static [u64] = &[1_000, 30_000, 1_000_000];
// hashmap capacity
const MAX_ENTRY: usize = 100_000;
// time to live of a hashmap entry (in seconds)
const TTL: u64 = 20;

// external upper level tester
use adaptive_spawn::*;

// fn foo() -> ! {
//     panic!()
// }

#[derive(Clone)]
pub struct ThreadPool {
    queues: Vec<Arc<TaskQueue>>,
    // stats: token -> (executed_time(in micros),queue_index)
    stats: Arc<Mutex<LruCache<u64, (Arc<AtomicU64>, usize)>>>,
}

impl ThreadPool {
    pub fn new(num_threads: usize) -> ThreadPool {
        let mut queues = Vec::new();
        for _ in QUEUE_PRIVILIAGE {
            let (tx, rx) = channel::unbounded();
            let queue = Arc::new(TaskQueue { tx, rx });
            queues.push(queue);
        }
        // create stats
        let ttl = Duration::from_secs(TTL);
        let stats_for_constructor = LruCache::with_expiry_duration_and_capacity(ttl, MAX_ENTRY);
        let stats_for_constructor = Arc::new(Mutex::new(stats_for_constructor));
        // spawn threads
        for _ in 0..num_threads {
            let queues = queues.clone();
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
                            unsafe { poll_with_timer(task, index) };
                        }
                    }
                    if is_empty {
                        let oper = sel.select();
                        let rx = rx_map.get(&oper.index()).unwrap();
                        let task = oper.recv(*rx).unwrap();
                        let index = task.0.index.load(Ordering::SeqCst);
                        unsafe { poll_with_timer(task, index) };
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
        let mut stats = self.stats.lock().unwrap();
        // at begin a token has top priority
        if !stats.contains_key(&token) {
            stats.insert(token, (Arc::default(), 0));
        }
        // otherwise use its own priority
        let (elapsed, index) = &*stats.get_mut(&token).unwrap();
        let _ = self.queues[*index].tx.send(ArcTask::new(
            task,
            self.queues.clone(),
            *index,
            elapsed.clone(),
        ));
    }
}

unsafe fn poll_with_timer(task: ArcTask, incoming_index: usize) {
    let task_elapsed = task.0.elapsed.clone();
    // adjust queue level
    let mut index = task.0.index.load(Ordering::SeqCst);
    if incoming_index != index {
        let _ = task.0.queues[index].tx.send(clone_task(&*task.0));
        return;
    }
    if task_elapsed.load(Ordering::SeqCst) > TIME_FEEDBACK[index] && index < TIME_FEEDBACK.len() - 1
    {
        index += 1;
        task.0.index.store(index, Ordering::SeqCst);
        let _ = task.0.queues[index].tx.send(clone_task(&*task.0));
        return;
    }
    // polling
    let begin = Instant::now();
    task.poll();
    let elapsed = begin.elapsed().as_micros() as u64;
    task_elapsed.fetch_add(elapsed, Ordering::SeqCst);
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
        ThreadPool::new(num_cpus::get_physical())
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
    // this task belongs to which queue
    index: AtomicUsize,
    // this token's total epalsed time
    elapsed: Arc<AtomicU64>,
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
    fn new<F>(
        future: F,
        queues: Vec<Arc<TaskQueue>>,
        index: usize,
        elapsed: Arc<AtomicU64>,
    ) -> ArcTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let future = Arc::new(Task {
            task: UnsafeCell::new(future.boxed()),
            queues,
            status: AtomicU8::new(WAITING),
            index: AtomicUsize::new(index),
            elapsed,
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
