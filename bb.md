## Model
- Four(or some other numbers) shared task queue with different priority.
Each time a thread takes a task from one queue (decide by itself) and execute.
- The "token-time" or "token-times" map is global. It is a concurrent hashmap.
After every pre-setted interval the old data in this map will be clean out.
- A thread take tasks from many queues in a round-robin way. Priority influence
how many times will a thread take a task from a queue in one term. Every tasks
have chances to be executed so starvation problem is avoided.
- One token with more total executed time has lower priority.
- First priority queue is thread-independent.

*nice will not be implemented for now*

## Possible Optimization
- [x] take time into consideration
- [ ] after polled one task, try to push it in the front rather than tail
- [x] dynamic adjust which queue a task belong to