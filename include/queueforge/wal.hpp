#pragma once
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace qf {

// Wall-clock milliseconds. Unlike steady_clock, this still means something
// after a restart, so delayed jobs keep their run time.
inline int64_t wall_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

struct WalJob {
    uint64_t id = 0;
    int priority = 0;
    int64_t run_at_ms = 0;  // 0 = run immediately
    std::string type;
    std::string payload;
};

// Write-ahead log. One text line per event:
//   S <id> <priority> <run_at_ms> <type> <payload>   job submitted
//   A <id>                                            job finished (ACK)
//   D <id>                                            job gave up (dead)
// A job is "live" if it has an S line and no A or D line after it.
class Wal {
public:
    explicit Wal(std::string path, bool sync_every_write = true)
        : path_(std::move(path)), sync_(sync_every_write) {}
    ~Wal() { if (fd_ >= 0) ::close(fd_); }
    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    // Replay the log. Returns live jobs in submit order; sets max_id.
    std::vector<WalJob> recover(uint64_t& max_id) {
        std::unordered_map<uint64_t, WalJob> live;
        std::vector<uint64_t> order;
        max_id = 0;
        std::ifstream in(path_);
        std::string line;
        while (std::getline(in, line)) {
            std::istringstream ls(line);
            std::string tag;
            ls >> tag;
            if (tag == "S") {
                WalJob j;
                if (!(ls >> j.id >> j.priority >> j.run_at_ms >> j.type)) continue;  // damaged line
                std::getline(ls, j.payload);
                if (!j.payload.empty() && j.payload[0] == ' ') j.payload.erase(0, 1);
                max_id = std::max(max_id, j.id);
                live[j.id] = j;
                order.push_back(j.id);
            } else if (tag == "A" || tag == "D") {
                uint64_t id;
                if (ls >> id) live.erase(id);
            }
        }
        std::vector<WalJob> out;
        for (uint64_t id : order) {
            auto it = live.find(id);
            if (it == live.end()) continue;
            out.push_back(it->second);
            live.erase(it);
        }
        return out;
    }

    // Rewrite the log with only live jobs (so it does not grow forever),
    // then open it for appending.
    bool compact_and_open(const std::vector<WalJob>& live) {
        std::string tmp = path_ + ".tmp";
        int t = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (t < 0) return false;
        for (const auto& j : live) {
            std::string r = format(j);
            if (::write(t, r.data(), r.size()) != static_cast<ssize_t>(r.size())) {
                ::close(t);
                return false;
            }
        }
        ::fsync(t);
        ::close(t);
        if (std::rename(tmp.c_str(), path_.c_str()) != 0) return false;
        fd_ = ::open(path_.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
        return fd_ >= 0;
    }

    void log_submit(const WalJob& j) { append(format(j)); }
    void log_ack(uint64_t id) { append("A " + std::to_string(id) + "\n"); }
    void log_dead(uint64_t id) { append("D " + std::to_string(id) + "\n"); }

private:
    static std::string format(const WalJob& j) {
        return "S " + std::to_string(j.id) + " " + std::to_string(j.priority) + " " +
               std::to_string(j.run_at_ms) + " " + j.type + " " + j.payload + "\n";
    }

        // Group commit: every record is written under the lock, but fsync runs
    // OUTSIDE it. While one thread is in fsync, other threads append their
    // records; the next fsync then makes all of them durable in one go.
    // Each caller still returns only after its own record is on disk.
    void append(const std::string& rec) {
        std::unique_lock<std::mutex> lock(mu_);
        if (fd_ < 0) return;
        if (::write(fd_, rec.data(), rec.size()) != static_cast<ssize_t>(rec.size()))
            std::fprintf(stderr, "[wal] write failed\n");
        uint64_t my_seq = ++written_seq_;
        if (!sync_) return;

        while (synced_seq_ < my_seq) {
            if (!syncing_) {
                syncing_ = true;                 // I am the leader for this round
                uint64_t target = written_seq_;  // covers everything written so far
                lock.unlock();
                ::fsync(fd_);
                lock.lock();
                synced_seq_ = target;
                syncing_ = false;
                cv_.notify_all();
            } else {
                cv_.wait(lock);                  // a leader is syncing; wait for it
            }
        }
    }
    std::string path_;
    bool sync_;
    int fd_ = -1;
        std::mutex mu_;
    std::condition_variable cv_;
    uint64_t written_seq_ = 0;  // records written
    uint64_t synced_seq_ = 0;   // records known to be on disk
    bool syncing_ = false;      // a thread is inside fsync
};

}  // namespace qf