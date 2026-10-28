# QueueForge design notes

## Goal
A job queue with a broker, producers and workers, built from scratch in C++.

## Job lifecycle
queued -> running -> done
                 \-> failed -> (retry) queued
                           \-> dead (after max_attempts)

## Components
- Broker: owns the queues, hands out jobs, tracks running jobs.
- Producer: submits jobs.
- Worker: fetches, runs, then ACKs or FAILs.
- WAL (write-ahead log): every state change is appended to disk so jobs survive a crash.

## Decisions log
- Priority queue ordered by (priority desc, id asc).
- Blocking pop uses a condition variable with a timeout so workers can shut down cleanly.

## Open questions
- Text protocol vs binary protocol?
- One thread per client vs epoll event loop?
