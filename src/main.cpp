// Week 1 demo: one in-process queue, three worker threads, 1000 jobs.
// Next step: move the queue behind a TCP server (the broker).
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "queueforge/job_queue.hpp"

int main() {
    qf::JobQueue queue;
    std::atomic<int> done{0};
    const int kJobs = 1000;
    const int kWorkers = 3;

    auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> workers;
    for (int w = 0; w < kWorkers; ++w) {
        workers.emplace_back([&queue, &done, kJobs] {
            while (done.load() < kJobs) {
                auto job = queue.pop(std::chrono::milliseconds(50));
                if (!job) continue;
                // Dummy task: pretend to work for 1 ms.
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                done.fetch_add(1);
            }
        });
    }

    for (int i = 1; i <= kJobs; ++i) {
        qf::Job j;
        j.id = i;
        j.type = "dummy";
        j.priority = i % 3;
        queue.push(std::move(j));
    }

    for (auto& t : workers) t.join();

    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - start).count();
    std::cout << "Processed " << done.load() << " jobs in " << ms << " ms\n";
    return 0;
}
