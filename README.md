# QueueForge

A distributed job queue written from scratch in C++17.

## Build and run
```
cmake -S . -B build
cmake --build build
./build/demo
ctest --test-dir build
```

## Roadmap
- [x] Job struct and thread-safe priority queue
- [ ] TCP broker (SUBMIT / FETCH / ACK / FAIL)
- [ ] Workers as separate processes
- [ ] Retries, backoff, visibility timeout, dead-letter queue
- [ ] Delayed jobs, write-ahead log, crash recovery
- [ ] Metrics endpoint, dashboard, load-test results
