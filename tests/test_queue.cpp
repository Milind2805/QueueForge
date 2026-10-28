#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "queueforge/job_queue.hpp"

using namespace qf;

static Job make(uint64_t id, int priority) {
    Job j;
    j.id = id;
    j.priority = priority;
    return j;
}

TEST(JobQueue, HigherPriorityComesFirst) {
    JobQueue q;
    q.push(make(1, 0));
    q.push(make(2, 5));
    q.push(make(3, 1));
    EXPECT_EQ(q.pop()->id, 2u);
    EXPECT_EQ(q.pop()->id, 3u);
    EXPECT_EQ(q.pop()->id, 1u);
}

TEST(JobQueue, SamePriorityIsFifo) {
    JobQueue q;
    q.push(make(1, 1));
    q.push(make(2, 1));
    q.push(make(3, 1));
    EXPECT_EQ(q.pop()->id, 1u);
    EXPECT_EQ(q.pop()->id, 2u);
    EXPECT_EQ(q.pop()->id, 3u);
}

TEST(JobQueue, PopTimesOutWhenEmpty) {
    JobQueue q;
    EXPECT_FALSE(q.pop(std::chrono::milliseconds(20)).has_value());
}

TEST(JobQueue, CloseWakesWaitingWorkers) {
    JobQueue q;
    std::thread t([&q] { EXPECT_FALSE(q.pop(std::chrono::seconds(5)).has_value()); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    q.close();
    t.join();
}

TEST(JobQueue, NoJobLostOrDuplicatedUnderContention) {
    JobQueue q;
    const int kJobs = 5000;
    std::atomic<int> popped{0};
    std::vector<std::vector<uint64_t>> seen(4);

    std::vector<std::thread> workers;
    for (int w = 0; w < 4; ++w) {
        workers.emplace_back([&, w] {
            while (popped.load() < kJobs) {
                auto j = q.pop(std::chrono::milliseconds(20));
                if (j) {
                    seen[w].push_back(j->id);
                    popped.fetch_add(1);
                }
            }
        });
    }
    for (int i = 1; i <= kJobs; ++i) q.push(make(i, i % 4));
    for (auto& t : workers) t.join();

    std::set<uint64_t> all;
    for (auto& v : seen) all.insert(v.begin(), v.end());
    EXPECT_EQ(all.size(), static_cast<size_t>(kJobs));
}
