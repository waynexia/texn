## Model
- Four(or some other numbers) shared task queue with different priority.
Each time a thread takes a task from one queue (decide by itself) and execute.
- The "token-time" or "token-times" map is global.
- A thread take tasks from many queues in a round-robin way. Priority influence
how many times will a thread take a task from a queue in one term. Every tasks
have chances to be executed so starvation problem is avoided.
- One token with more task appeared before will has lower priority. Only when
pushing a task into the queue will consider token and nice.
- A task will always stay in one exactly queue.

*time and nice will not be implemented for now*

## Data Structure
- ThreadPool
```rust
struct ThreadPool{
    queues: Arc<Vec<TaskQueue>>,
}
```

- TaskQueue
```rust
struct TaskQueue {
    // crossbeam channel
    tx: channel::Sender<Task>,
    rx: channel::Receiver<Task>,
}
```
Maybe a deque-like struct is better as a task after waking up needs to be pushed
into head

- Task
```rust
```

- AtomicFuture?
```rust
```

- waker
```rust
```