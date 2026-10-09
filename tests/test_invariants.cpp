#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cassert>
#include <cstring>
#include <mutex>
#include <algorithm>
#include <map>
#include <random>
#include <system_error>

namespace titan {
namespace raft {

using Term = uint64_t;
using Index = uint64_t;

struct LogEntry {
    Index index;
    Term term;
    std::vector<uint8_t> data;
};

enum class NodeState {
    Follower,
    Candidate,
    Leader
};

struct NodeConfig {
    uint64_t id{0};
    uint64_t election_timeout_ms{150};
    uint64_t heartbeat_timeout_ms{50};
    std::string storage_path;
};

class RaftException : public std::runtime_error {
public:
    explicit RaftException(const std::string& msg) : std::runtime_error(msg) {}
};

class StorageEngine {
public:
    explicit StorageEngine(std::string path) : path_(std::move(path)), current_term_(0), voted_for_(0), commit_index_(0) {}

    void save_metadata(Term term, uint64_t voted_for) {
        std::lock_guard<std::mutex> lock(mu_);
        current_term_ = term;
        voted_for_ = voted_for;
    }

    void get_metadata(Term& term, uint64_t& voted_for) {
        std::lock_guard<std::mutex> lock(mu_);
        term = current_term_;
        voted_for = voted_for_;
    }

    void append_entries(const std::vector<LogEntry>& entries) {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& entry : entries) {
            if (entry.index <= log_.size()) {
                log_[entry.index - 1] = entry;
            } else if (entry.index == log_.size() + 1) {
                log_.push_back(entry);
            } else {
                throw RaftException("Log gap detected during append");
            }
        }
    }

    void truncate(Index index) {
        std::lock_guard<std::mutex> lock(mu_);
        if (index == 0) {
            log_.clear();
            return;
        }
        if (index <= log_.size()) {
            log_.resize(index);
        }
    }

    std::vector<LogEntry> get_entries(Index start, Index end) {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<LogEntry> result;
        if (start == 0 || start > log_.size()) return result;
        size_t s_idx = start - 1;
        size_t e_idx = std::min(end, static_cast<Index>(log_.size()));
        for (size_t i = s_idx; i < e_idx; ++i) {
            result.push_back(log_[i]);
        }
        return result;
    }

    LogEntry get_entry(Index index) {
        std::lock_guard<std::mutex> lock(mu_);
        if (index == 0 || index > log_.size()) {
            throw RaftException("Index out of bounds");
        }
        return log_[index - 1];
    }

    Index last_index() {
        std::lock_guard<std::mutex> lock(mu_);
        return static_cast<Index>(log_.size());
    }

    Term last_term() {
        std::lock_guard<std::mutex> lock(mu_);
        if (log_.empty()) return 0;
        return log_.back().term;
    }

    void corrupt_last_entry() {
        std::lock_guard<std::mutex> lock(mu_);
        if (!log_.empty()) {
            log_.back().term = 999999; 
        }
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mu_);
        log_.clear();
        current_term_ = 0;
        voted_for_ = 0;
        commit_index_ = 0;
    }

private:
    std::string path_;
    std::mutex mu_;
    Term current_term_;
    uint64_t voted_for_;
    Index commit_index_;
    std::vector<LogEntry> log_;
};

struct AppendEntriesArgs {
    Term term;
    uint64_t leader_id;
    Index prev_log_index;
    Term prev_log_term;
    std::vector<LogEntry> entries;
    Index leader_commit;
};

struct AppendEntriesReply {
    Term term;
    bool success;
    Index match_index;
};

struct RequestVoteArgs {
    Term term;
    uint64_t candidate_id;
    Index last_log_index;
    Term last_log_term;
};

struct RequestVoteReply {
    Term term;
    bool vote_granted;
};

class RaftNode {
public:
    explicit RaftNode(NodeConfig config)
        : config_(config),
          state_(NodeState::Follower),
          current_term_(0),
          voted_for_(0),
          commit_index_(0),
          last_applied_(0),
          storage_(config.storage_path),
          stopped_(false) {}

    ~RaftNode() {
        stop();
    }

    void start() {
        std::lock_guard<std::mutex> lock(mu_);
        stopped_ = false;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            stopped_ = true;
        }
    }

    NodeState get_state() {
        std::lock_guard<std::mutex> lock(mu_);
        return state_;
    }

    Term get_current_term() {
        std::lock_guard<std::mutex> lock(mu_);
        return current_term_;
    }

    Index get_commit_index() {
        std::lock_guard<std::mutex> lock(mu_);
        return commit_index_;
    }

    bool propose(const std::vector<uint8_t>& data, Index& out_index, Term& out_term) {
        std::lock_guard<std::mutex> lock(mu_);
        if (state_ != NodeState::Leader || stopped_) {
            return false;
        }
        Index idx = storage_.last_index() + 1;
        LogEntry entry{idx, current_term_, data};
        storage_.append_entries({entry});
        out_index = idx;
        out_term = current_term_;
        return true;
    }

    RequestVoteReply handle_request_vote(const RequestVoteArgs& args) {
        std::lock_guard<std::mutex> lock(mu_);
        RequestVoteReply reply;
        reply.term = current_term_;
        reply.vote_granted = false;

        if (args.term > current_term_) {
            current_term_ = args.term;
            state_ = NodeState::Follower;
            voted_for_ = 0;
            storage_.save_metadata(current_term_, voted_for_);
        }

        if (args.term < current_term_) {
            return reply;
        }

        bool candidate_log_up_to_date = false;
        Index last_idx = storage_.last_index();
        Term last_t = storage_.last_term();

        if (args.last_log_term > last_t) {
            candidate_log_up_to_date = true;
        } else if (args.last_log_term == last_t && args.last_log_index >= last_idx) {
            candidate_log_up_to_date = true;
        }

        if ((voted_for_ == 0 || voted_for_ == args.candidate_id) && candidate_log_up_to_date) {
            voted_for_ = args.candidate_id;
            reply.vote_granted = true;
            storage_.save_metadata(current_term_, voted_for_);
        }

        reply.term = current_term_;
        return reply;
    }

    AppendEntriesReply handle_append_entries(const AppendEntriesArgs& args) {
        std::lock_guard<std::mutex> lock(mu_);
        AppendEntriesReply reply;
        reply.term = current_term_;
        reply.success = false;
        reply.match_index = 0;

        if (args.term > current_term_) {
            current_term_ = args.term;
            state_ = NodeState::Follower;
            voted_for_ = 0;
            storage_.save_metadata(current_term_, voted_for_);
        }

        if (args.term < current_term_) {
            return reply;
        }

        state_ = NodeState::Follower;

        if (args.prev_log_index > 0) {
            if (args.prev_log_index > storage_.last_index()) {
                return reply;
            }
            try {
                LogEntry entry = storage_.get_entry(args.prev_log_index);
                if (entry.term != args.prev_log_term) {
                    storage_.truncate(args.prev_log_index - 1);
                    return reply;
                }
            } catch (...) {
                return reply;
            }
        }

        if (!args.entries.empty()) {
            Index first_new_index = args.prev_log_index + 1;
            for (size_t i = 0; i < args.entries.size(); ++i) {
                Index current_idx = first_new_index + i;
                if (current_idx <= storage_.last_index()) {
                    LogEntry existing = storage_.get_entry(current_idx);
                    if (existing.term != args.entries[i].term) {
                        storage_.truncate(current_idx - 1);
                        storage_.append_entries(std::vector<LogEntry>(args.entries.begin() + i, args.entries.end()));
                        break;
                    }
                } else {
                    storage_.append_entries(std::vector<LogEntry>(args.entries.begin() + i, args.entries.end()));
                    break;
                }
            }
        }

        if (args.leader_commit > commit_index_) {
            commit_index_ = std::min(args.leader_commit, storage_.last_index());
        }

        reply.success = true;
        reply.match_index = storage_.last_index();
        reply.term = current_term_;
        return reply;
    }

    void force_state_leader(Term term) {
        std::lock_guard<std::mutex> lock(mu_);
        state_ = NodeState::Leader;
        current_term_ = term;
        voted_for_ = config_.id;
        storage_.save_metadata(current_term_, voted_for_);
    }

    void force_state_follower(Term term) {
        std::lock_guard<std::mutex> lock(mu_);
        state_ = NodeState::Follower;
        current_term_ = term;
        voted_for_ = 0;
        storage_.save_metadata(current_term_, voted_for_);
    }

    StorageEngine& get_storage() {
        return storage_;
    }

private:
    NodeConfig config_;
    NodeState state_;
    Term current_term_;
    uint64_t voted_for_;
    Index commit_index_;
    Index last_applied_;
    StorageEngine storage_;
    bool stopped_;
    std::mutex mu_;
};

class ClusterSimulator {
public:
    explicit ClusterSimulator(size_t size) {
        for (size_t i = 0; i < size; ++i) {
            NodeConfig cfg;
            cfg.id = i + 1;
            cfg.storage_path = "node_" + std::to_string(i + 1);
            nodes_.push_back(std::make_unique<RaftNode>(cfg));
        }
    }

    RaftNode& get_node(size_t idx) {
        return *nodes_[idx];