use chashmap::CHashMap;
use crossbeam::{channel, Receiver, Sender};
use futures::future::BoxFuture;
use futures::prelude::*;
use num_cpus;

use std::cell::UnsafeCell;
use std::future::Future;
use std::mem::{forget, ManuallyDrop};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;

const QUEUE_PRIVILIAGE: &'static [u64] = &[1, 2, 4];

use adaptive_spawn::*;

// fn foo() -> ! {
//     panic!()
// }

#[derive(Clone)]
pub struct ThreadPool {
    queues: Vec<Arc<TaskQueue>>,
    stats: CHashMap<u64, u64>,
}

impl ThreadPool {
    pub fn new(num_threads: usize) -> ThreadPool {
        let mut queues = Vec::new();
        for _ in QUEUE_PRIVILIAGE {
            let (tx, rx) = channel::unbounded();
            let queue = Arc::new(TaskQueue { tx, rx });
            queues.push(queue);
        }
        // let queues = Arc::new(queues);
        for _ in 0..num_threads {
            let queues = queues.clone();
            thread::spawn(move || loop {
                let mut index = 0;
                for queue in &queues {
                    let mut exec_cnt = 0;
                    while exec_cnt < QUEUE_PRIVILIAGE[index] {
                        if let Ok(task) = queue.rx.recv() {
                            unsafe { task.poll() };
                            exec_cnt += 1;
                        }
                        exec_cnt += 1;
                    }
                    index += 1;
                }
            });
        }
        ThreadPool {
            queues,
            stats: CHashMap::new(),
        }
    }

    pub fn spawn<F>(&self, task: F, token: u64, _nice: u8)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if !self.stats.contains_key(&token) {
            self.stats.insert(token, 1);
        }
        let mut index = 0;
        while index < QUEUE_PRIVILIAGE.len()
            && QUEUE_PRIVILIAGE[index] < *self.stats.get(&token).unwrap()
        {
            index += 1;
        }
        self.stats.upsert(token, || panic!(), |v| *v += 1);
        let queue = self.queues[index as usize].clone();
        let _ = queue.tx.send(ArcTask::new(task, queue.clone()));
    }
}

impl AdaptiveSpawn for ThreadPool{
    fn spawn_opt<Fut>(&self, f: Fut, opt: Options)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn(f,opt.token,opt.nice);
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
    queue: Arc<TaskQueue>,
    status: AtomicU8,
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
    fn new<F>(future: F, queue: Arc<TaskQueue>) -> ArcTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let future = Arc::new(Task {
            task: UnsafeCell::new(future.boxed()),
            queue,
            status: AtomicU8::new(WAITING),
        });
        let future: *const Task = Arc::into_raw(future) as *const Task;
        unsafe { task(future) }
    }

    unsafe fn poll(self) {
        self.0.status.store(POLLING, Ordering::SeqCst);
        // let waker = waker(&self);
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
    // let task = this as *const Task;
    let task = clone_task(this as *const Task);
    RawWaker::new(
        Arc::into_raw(task.0) as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    )
}

unsafe fn drop_raw(this: *const ()) {
    // drop(this as * const Task)
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
                        task.0.queue.tx.send(clone_task(&*task.0)).unwrap();
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
                        task.0.queue.tx.send(clone_task(&*task.0)).unwrap();
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
