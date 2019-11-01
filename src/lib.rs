#![feature(core_intrinsics)]

use crossbeam::{channel, Sender};
use futures::future::BoxFuture;
use futures::prelude::*;
use lazy_static::lazy_static;
use num_cpus;
use serde::Deserialize;

use std::cell::UnsafeCell;
use std::future::Future;
use std::intrinsics::{likely, unlikely};
use std::mem::{forget, ManuallyDrop};
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::time::{Duration, Instant};

use dropmap::DropMap;

mod dropmap;

#[derive(Debug, Deserialize)]
struct Config {
    num_thread: usize,
    // dropmap swap interval (in secs)
    swap_interval: u64,
    queue_privilige: Vec<u64>,
    time_feedback: Vec<u64>,
    percentage: u64,
}

lazy_static! {
    static ref CONFIG: Config = {
        // Config {num_thread: num_cpus::get_physical(),
        Config {num_thread: num_cpus::get(),
        swap_interval: 20,
        queue_privilige: vec![32, 4, 1],
        time_feedback: vec![1000, 300_000, 10_000_000],
        percentage: 80,
        }
        // let content = read_to_string("/root/config.toml").unwrap();
        // let content = read_to_string("/data/waynest/code/pingcap_hackathon2019/adaptive-thread-pool/texn/src/config.toml").unwrap();
        // toml::from_str(&content).unwrap()
    };
    // take how many tasks from a queue in one term
    static ref QUEUE_PRIVILEGE:&'static[u64] = &CONFIG.queue_privilige;
    static ref FIRST_PRIVILEGE: AtomicU64 = AtomicU64::new(QUEUE_PRIVILEGE[0]);
    // the longest executed time a queue can hold (in micros)
    static ref TIME_FEEDBACK:&'static[u64] = &CONFIG.time_feedback;
    // SMALL_TASK_CNT/(SMALL_TASK_CNT + HUGE_TASK_CNT) should equal to PERCENTAGE
    static ref PERCENTAGE: u64 = {
        if CONFIG.percentage > 90{
            panic!("percentage greater than 90%");
        }
        CONFIG.percentage
        };
    static ref MIN_PRI : u64 = 4;
    static ref MAX_PRI : u64 = 4096 / CONFIG.num_thread as u64;
}

static mut SMALL_TASK_CNT: u64 = 0;
static mut HUGE_TASK_CNT: u64 = 0;

pub static TOTAL_SELECT: AtomicU64 = AtomicU64::new(0);

// external upper level tester
use adaptive_spawn::{AdaptiveSpawn, Options};

struct ThreadCtx {
    handle: thread::JoinHandle<()>,
    todo_cnt: Arc<AtomicU64>,
}

impl ThreadCtx {
    fn unpark(&self) {
        self.handle.thread().unpark();
    }

    pub fn notify_once(&self) {
        if self.todo_cnt.fetch_add(1, SeqCst) > 9 {
            self.unpark();
        }
    }
}

#[derive(Clone)]
pub struct ThreadPool {
    // first priority, thread local task queues
    first_queues: Vec<Sender<ArcTask>>,
    // other shared queues
    queues: Arc<[Sender<ArcTask>]>,
    // stats: token -> (executed_time(in micros),queue_index)
    stats: DropMap<u64, (Arc<AtomicU64>, Arc<AtomicUsize>)>,
    num_threads: usize,
    first_queue_iter: Arc<AtomicUsize>,
    thds: Vec<Arc<ThreadCtx>>,
}

impl ThreadPool {
    pub fn new(num_threads: usize, f: Arc<dyn Fn() + Send + Sync + 'static>) -> ThreadPool {
        let mut queues = Vec::new();
        let mut rxs = Vec::new();
        let mut first_queues = Vec::new();
        for _ in QUEUE_PRIVILEGE.into_iter() {
            let (tx, rx) = channel::unbounded();
            queues.push(tx);
            rxs.push(rx);
        }
        // init
        let stats = DropMap::new(CONFIG.swap_interval);
        let queues: Arc<[Sender<ArcTask>]> = Arc::from(queues.into_boxed_slice());
        let mut thds = vec![];
        // spawn worker threads
        for _ in 0..num_threads {
            let (tx, rx) = channel::unbounded();
            first_queues.push(tx.clone());
            let mut rxs = rxs.clone();
            rxs.push(rx);
            rxs.swap_remove(0);
            let f = f.clone();
            let todo_cnt_param = Arc::new(AtomicU64::new(0));
            let todo_cnt = todo_cnt_param.clone();
            thds.push(Arc::new(ThreadCtx {
                handle: thread::spawn(move || {
                    f();
                    loop {
                        let mut is_empty = true;
                        for ((rx, &limit), index) in
                            rxs.iter().zip(QUEUE_PRIVILEGE.into_iter()).zip(0..)
                        {
                            if unsafe { likely(index == 0) } {
                                for task in
                                    rx.try_iter().take(FIRST_PRIVILEGE.load(SeqCst) as usize)
                                {
                                    is_empty = false;
                                    unsafe { poll_with_timer(task, index) };
                                }
                            } else {
                                for task in rx.try_iter().take(limit as usize) {
                                    is_empty = false;
                                    unsafe { poll_with_timer(task, index) };
                                }
                            }
                        }
                        if unsafe { unlikely(is_empty) } {
                            thread::park_timeout(Duration::from_micros(30));
                            todo_cnt.store(0, SeqCst);

                            TOTAL_SELECT.fetch_add(1, SeqCst);
                        }
                    }
                }),
                todo_cnt: todo_cnt_param,
            }));
        }
        // spawn adjustor thread
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));
                let small = unsafe { SMALL_TASK_CNT };
                let huge = unsafe { HUGE_TASK_CNT };
                if unsafe { unlikely(huge == 0) } {
                    // println!("set to max privilege");
                    FIRST_PRIVILEGE.store(*MAX_PRI, SeqCst);
                    continue;
                }
                let cur_perc: u64 = small * 100 / (small + huge);
                let first_privilege = FIRST_PRIVILEGE.load(SeqCst);
                // println!("{}  {} -> {}\t{}", small, huge, cur_perc, first_privilege);
                // need to decrease first priority
                if cur_perc > *PERCENTAGE + 5 {
                    let mut new_pri = first_privilege / 2;
                    if new_pri < *MIN_PRI {
                        new_pri = *MIN_PRI;
                    }
                    FIRST_PRIVILEGE.store(new_pri, SeqCst);
                }
                // need to increase first priority
                else if cur_perc < *PERCENTAGE - 5 {
                    let mut new_pri = first_privilege * 2;
                    if new_pri > *MAX_PRI {
                        new_pri = *MAX_PRI;
                    }
                    FIRST_PRIVILEGE.store(new_pri, SeqCst);
                }
                // reset counter
                unsafe {
                    SMALL_TASK_CNT = 0;
                    HUGE_TASK_CNT = 0;
                }
            }
        });
        ThreadPool {
            first_queues,
            queues,
            stats,
            num_threads,
            first_queue_iter: Arc::default(),
            thds,
        }
    }

    pub fn spawn<F>(&self, task: F, token: u64, nice: u8)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // at begin a token has top priority
        let (atom_elapsed, atom_index) = &*self
            .stats
            .get_or_insert(&token, (Arc::default(), Arc::default()));
        let index = atom_index.load(SeqCst);
        // first priority, will be send into thread local queue
        if unsafe { likely(index == 0 || nice == 0) } {
            let thd_idx = self.first_queue_iter.fetch_add(1, SeqCst) % self.num_threads;
            let sender = &self.first_queues[thd_idx];
            match sender.try_send(ArcTask::new(
                task,
                sender.clone(),
                self.thds[index].clone(),
                self.queues.clone(),
                atom_index.clone(),
                atom_elapsed.clone(),
                nice,
                token,
            )) {
                Ok(_) => {}
                Err(e) => {
                    self.queues[1].send(e.into_inner()).unwrap();
                }
            }
            self.thds[index].notify_once();
        }
        // other priority, belongs to shared queue
        else {
            self.queues[index]
                .send(ArcTask::new(
                    task,
                    // don't care
                    self.first_queues[0].clone(),
                    self.thds[index].clone(),
                    self.queues.clone(),
                    atom_index.clone(),
                    atom_elapsed.clone(),
                    nice,
                    token,
                ))
                .unwrap();
        }
    }

    pub fn new_from_config(f: Arc<dyn Fn() + Send + Sync + 'static>) -> ThreadPool {
        ThreadPool::new(CONFIG.num_thread, f)
    }
}

unsafe fn poll_with_timer(task: ArcTask, incoming_index: usize) {
    let task_elapsed = task.0.elapsed.clone();
    let mut index = task.0.index.load(SeqCst);
    // check uncompatible priority
    if unlikely(incoming_index < index) {
        // println!("resend {}, {}", incoming_index, index);
        task.0.queues[index].send(clone_task(&*task.0)).unwrap();
        return;
    }
    // adjust priority
    if unlikely(task_elapsed.load(SeqCst) > TIME_FEEDBACK[index])
        && index < TIME_FEEDBACK.len() - 1
        && task.0.nice != 0
    {
        // println!("downgrade to {}", index + 1);
        index += 1;
        task.0.index.store(index, SeqCst);
        task.0.queues[index].send(clone_task(&*task.0)).unwrap();
        return;
    }

    // polling
    let begin = Instant::now();
    task.poll();
    let elapsed = begin.elapsed().as_micros() as u64;

    if likely(index == 0) {
        SMALL_TASK_CNT += elapsed;
    } else {
        HUGE_TASK_CNT += elapsed;
    }
    task_elapsed.fetch_add(elapsed, SeqCst);
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
        ThreadPool::new(num_cpus::get_physical(), Arc::new(|| {}))
    }
}

/// A task contains local_queue, this is the thread-local queue where it originally comes from
/// should it be send into other thread-local queue when waking up?
struct Task {
    task: UnsafeCell<BoxFuture<'static, ()>>,
    local_queue: Sender<ArcTask>,
    thd: Arc<ThreadCtx>,
    queues: Arc<[Sender<ArcTask>]>,
    status: AtomicU8,
    // this task belongs to which queue
    index: Arc<AtomicUsize>,
    // this token's total epalsed time
    elapsed: Arc<AtomicU64>,
    nice: u8,
    _token: u64,
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
    #[inline]
    fn new<F>(
        future: F,
        local_queue: Sender<ArcTask>,
        thd: Arc<ThreadCtx>,
        queues: Arc<[Sender<ArcTask>]>,
        index: Arc<AtomicUsize>,
        elapsed: Arc<AtomicU64>,
        nice: u8,
        token: u64,
    ) -> ArcTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let future = Arc::new(Task {
            task: UnsafeCell::new(future.boxed()),
            local_queue,
            thd,
            queues,
            status: AtomicU8::new(WAITING),
            index,
            elapsed,
            nice,
            _token: token,
        });
        let future: *const Task = Arc::into_raw(future) as *const Task;
        unsafe { task(future) }
    }

    #[inline]
    unsafe fn poll(self) {
        self.0.status.store(POLLING, SeqCst);
        let waker = ManuallyDrop::new(waker(&*self.0));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(_) = (&mut *self.0.task.get()).poll_unpin(&mut cx) {
                break self.0.status.store(COMPLETE, SeqCst);
            }
            match self
                .0
                .status
                .compare_exchange(POLLING, WAITING, SeqCst, SeqCst)
            {
                Ok(_) => break,
                Err(_) => self.0.status.store(POLLING, SeqCst),
            }
        }
    }
}

#[inline]
unsafe fn waker(task: *const Task) -> Waker {
    Waker::from_raw(RawWaker::new(
        task as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    ))
}

#[inline]
unsafe fn clone_raw(this: *const ()) -> RawWaker {
    let task = clone_task(this as *const Task);
    RawWaker::new(
        Arc::into_raw(task.0) as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    )
}

#[inline]
unsafe fn drop_raw(this: *const ()) {
    drop(task(this as *const Task))
}

#[inline]
unsafe fn wake_raw(this: *const ()) {
    let task = task(this as *const Task);
    let mut status = task.0.status.load(SeqCst);
    loop {
        match status {
            WAITING => {
                match task
                    .0
                    .status
                    .compare_exchange(WAITING, POLLING, SeqCst, SeqCst)
                {
                    Ok(_) => {
                        let index = task.0.index.load(SeqCst);
                        let sender = if index == 0 {
                            &task.0.local_queue
                        } else {
                            &task.0.queues[index]
                        };
                        sender.send(clone_task(&*task.0)).unwrap();
                        if index == 0 {
                            task.0.thd.notify_once();
                        }
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task
                    .0
                    .status
                    .compare_exchange(POLLING, REPOLL, SeqCst, SeqCst)
                {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

#[inline]
unsafe fn wake_ref_raw(this: *const ()) {
    let task = ManuallyDrop::new(task(this as *const Task));
    let mut status = task.0.status.load(SeqCst);
    loop {
        match status {
            WAITING => {
                match task
                    .0
                    .status
                    .compare_exchange(WAITING, POLLING, SeqCst, SeqCst)
                {
                    Ok(_) => {
                        let index = task.0.index.load(SeqCst);
                        let sender = if index == 0 {
                            &task.0.local_queue
                        } else {
                            &task.0.queues[index]
                        };
                        sender.send(clone_task(&*task.0)).unwrap();
                        if index == 0 {
                            task.0.thd.notify_once();
                        }
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task
                    .0
                    .status
                    .compare_exchange(POLLING, REPOLL, SeqCst, SeqCst)
                {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

#[inline]
unsafe fn task(future: *const Task) -> ArcTask {
    ArcTask(Arc::from_raw(future))
}

#[inline]
unsafe fn clone_task(future: *const Task) -> ArcTask {
    let task = task(future);
    forget(task.clone());
    task
}
