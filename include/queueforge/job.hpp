#pragma once
#include <chrono>
#include <cstdint>
#include <string>

namespace qf {

enum class JobState { Queued, Running, Done, Failed, Dead };

inline const char* to_string(JobState s) {
    switch (s) {
        case JobState::Queued:  return "queued";
        case JobState::Running: return "running";
        case JobState::Done:    return "done";
        case JobState::Failed:  return "failed";
        case JobState::Dead:    return "dead";
    }
    return "unknown";
}

struct Job {
    uint64_t id = 0;
    std::string type;       // e.g. "send_email", "resize_image"
    std::string payload;    // opaque data for the worker
    int priority = 0;       // higher runs first
    int attempts = 0;       // how many times it has been tried
    int max_attempts = 3;   // after this, it goes to the dead-letter queue
    JobState state = JobState::Queued;
    std::chrono::steady_clock::time_point created_at = std::chrono::steady_clock::now();
};

}  // namespace qf
