#pragma once
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <vector>

#include "queueforge/job.hpp"

namespace qf {

// Thread-safe priority queue.
// Higher priority first; for equal priority, lower id (older job) first.
class JobQueue {
public:
    void push(Job job) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            job.state = JobState::Queued;
            heap_.push(std::move(job));
        }
        cv_.notify_one();
    }

    // Blocks until a job is available, the timeout passes, or the queue is closed.
    std::optional<Job> pop(std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait_for(lock, timeout, [this] { return !heap_.empty() || closed_; });
        if (heap_.empty()) return std::nullopt;
        Job job = heap_.top();
        heap_.pop();
        return job;
    }

    // Wake all waiting workers; used for shutdown.
    void close() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            closed_ = true;
        }
        cv_.notify_all();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return heap_.size();
    }

private:
    struct Compare {
        bool operator()(const Job& a, const Job& b) const {
            if (a.priority != b.priority) return a.priority < b.priority;
            return a.id > b.id;
        }
    };

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::priority_queue<Job, std::vector<Job>, Compare> heap_;
    bool closed_ = false;
};

}  // namespace qf
