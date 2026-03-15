#include "raft_node.h"

// ─────────────────────────────────────────────────────────────────────────────
//  构造 / 析构
// ─────────────────────────────────────────────────────────────────────────────

RaftNode::RaftNode(int id, std::string addr, int port, const std::vector<PeerInfo>& peers)
    : node_id_(id),
      addr_(addr),
      port_(port),
      peers_(peers),
      total_nodes_(static_cast<int>(peers.size()) + 1),
      rpc_server_(port),
      rpc_client_(5)
{
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    service_impl_.SetNode(this);
}

RaftNode::~RaftNode() {
    myLog::get_instance()->flush_local_buffer();
}

// ─────────────────────────────────────────────────────────────────────────────
//  日志系统初始化
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::log_write() {
    if (m_close_log == 0) {
        std::string log_path = "./RaftLog_" + std::to_string(node_id_);
        if (m_log_write == 1) {
            myLog::get_instance()->init(log_path.c_str(), m_close_log, 2000, 800000, 800);
        } else {
            myLog::get_instance()->init(log_path.c_str(), m_close_log, 2000, 800000, 0);
        }
        LOG_INFO("Log file initialized: " + log_path);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  启动
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::start() {
    log_write();
    load_persistent_state(); // logger 已初始化，LOG_* 可安全调用
    rpc_server_.init(0, 1, 5);
    rpc_client_.init();
    rpc_server_.register_service(&service_impl_);
    rpc_server_.init_thread_pool();
    std::thread([this]() { rpc_server_.start(); }).detach();
    timer_thread_ = std::thread(&RaftNode::election_timer, this);
    timer_thread_.detach();
}

// ─────────────────────────────────────────────────────────────────────────────
//  状态机
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::apply_log(const std::string& cmd) {
    // 调用者持 mutex_（或在 load 时单线程调用）
    std::istringstream iss(cmd);
    std::string op;
    iss >> op;

    if (op == "PUT") {
        std::string key, value;
        iss >> key >> value;
        kv_store[key] = value;
        LOG_INFO("[StateMachine] PUT " + key + " = " + value);
    } else if (op == "DELETE") {
        std::string key;
        iss >> key;
        kv_store.erase(key);
        LOG_INFO("[StateMachine] DELETE " + key);
    } else if (op == "GET" || op == "LIST") {
        // 读命令不应进入日志，忽略（兼容旧日志文件）
    } else {
        LOG_ERROR("[StateMachine] Unknown command: " + cmd);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  读路径：leader local read（调用方持 mutex_）
// ─────────────────────────────────────────────────────────────────────────────

std::string RaftNode::kv_query(const std::string& cmd) {
    std::istringstream iss(cmd);
    std::string op;
    iss >> op;

    if (op == "GET") {
        std::string key;
        iss >> key;
        auto it = kv_store.find(key);
        return (it != kv_store.end()) ? it->second : "";
    }
    if (op == "LIST") {
        std::string prefix;
        iss >> prefix;
        std::string out;
        for (const auto& [k, v] : kv_store) {
            if (k.size() >= prefix.size() && k.substr(0, prefix.size()) == prefix) {
                out += k + "=" + v + "\n";
            }
        }
        return out;
    }
    return "";
}

// ─────────────────────────────────────────────────────────────────────────────
//  持久化
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::persist_state() {
    // 调用方持 mutex_
    std::ofstream ofs("raft_state_" + std::to_string(node_id_) + ".txt", std::ios::trunc);
    if (!ofs) { LOG_ERROR("Failed to open raft_state for writing"); return; }
    ofs << current_term_ << " " << voted_for_ << " " << log_commit_index_ << "\n";
    ofs.flush();
}

void RaftNode::persist_log() {
    // 全量覆盖写（用于日志截断后重写）
    std::ofstream ofs("raft_log_" + std::to_string(node_id_) + ".bin",
                      std::ios::binary | std::ios::trunc);
    for (const auto& entry : log_) {
        std::string bin;
        entry.SerializeToString(&bin);
        uint32_t len = static_cast<uint32_t>(bin.size());
        ofs.write(reinterpret_cast<char*>(&len), sizeof(len));
        ofs.write(bin.data(), len);
    }
    ofs.flush();
}

void RaftNode::persist_log_entry(const raft::LogEntry& entry) {
    std::ofstream ofs("raft_log_" + std::to_string(node_id_) + ".bin",
                      std::ios::binary | std::ios::app);
    if (!ofs) { LOG_ERROR("Failed to open raft_log for appending"); return; }
    std::string bin;
    if (!entry.SerializeToString(&bin)) { LOG_ERROR("Failed to serialize log entry"); return; }
    uint32_t len = static_cast<uint32_t>(bin.size());
    ofs.write(reinterpret_cast<char*>(&len), sizeof(len));
    ofs.write(bin.data(), len);
    ofs.flush();
}

// ─────────────────────────────────────────────────────────────────────────────
//  加载持久化状态（构造函数调用，单线程）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::load_persistent_state() {
    // 1. 加载 term / voted_for / commit_index
    {
        std::ifstream ifs("raft_state_" + std::to_string(node_id_) + ".txt");
        if (ifs) {
            // 兼容旧格式（2 字段）和新格式（3 字段）
            ifs >> current_term_ >> voted_for_;
            if (!(ifs >> log_commit_index_)) {
                log_commit_index_ = -1;
            }
        }
    }

    // 2. 加载 log
    {
        std::ifstream logifs("raft_log_" + std::to_string(node_id_) + ".bin",
                             std::ios::binary);
        while (logifs) {
            uint32_t len = 0;
            if (!logifs.read(reinterpret_cast<char*>(&len), sizeof(len))) break;
            std::string bin(len, '\0');
            if (!logifs.read(&bin[0], len)) break;
            raft::LogEntry entry;
            if (entry.ParseFromString(bin)) {
                log_.push_back(entry);
            } else {
                LOG_ERROR("Failed to parse a log entry on load");
            }
        }
    }

    // 确保 log_commit_index_ 不超出已有日志
    log_commit_index_ = std::min(log_commit_index_, (int)log_.size() - 1);

    // 3. 通过重放日志重建 kv_store（替代单独的 kv_store 文件）
    //    只 apply 已提交的条目，避免应用未提交条目
    for (int i = 0; i <= log_commit_index_; ++i) {
        apply_log(log_[i].command());
    }
    last_applied_ = log_commit_index_;

    LOG_INFO("Recovered: term=" + std::to_string(current_term_)
             + " voted_for=" + std::to_string(voted_for_)
             + " commit=" + std::to_string(log_commit_index_)
             + " log_size=" + std::to_string(log_.size())
             + " kv_size=" + std::to_string(kv_store.size()));
}

// ─────────────────────────────────────────────────────────────────────────────
//  选举定时器
// ─────────────────────────────────────────────────────────────────────────────

int RaftNode::random_election_timeout() {
    // 加宽随机范围，减少同步超时导致的 split vote
    return 600 + rand() % 400; // 600-1000ms
}

void RaftNode::election_timer() {
    auto startup_time = std::chrono::steady_clock::now();
    while (!stopped) {
        int timeout = random_election_timeout();
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout));

        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(mutex_);

        // 冷启动保护：1500ms 内不触发选举
        int startup_ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(now - startup_time).count());
        if (startup_ms < 1500) continue;

        if (role_ == Follower || role_ == Candidate) {
            int elapsed = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_heartbeat_time_).count());
            if (elapsed > timeout) {
                LOG_INFO("[Election] timeout, starting election...");
                start_election();
                last_heartbeat_time_ = std::chrono::steady_clock::now();
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  选举（调用方持 mutex_）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::start_election() {
    current_term_++;
    voted_for_ = node_id_;
    role_ = Candidate;
    persist_state();
    int votes = 1;

    LOG_INFO("[Election] Node " + std::to_string(node_id_)
             + " starting election for term " + std::to_string(current_term_));

    // B2: VoteRequest 加 lastLogIndex / lastLogTerm
    int my_last_index = static_cast<int>(log_.size()) - 1;
    int my_last_term  = log_.empty() ? -1 : log_.back().term();

    for (const auto& peer : peers_) {
        raft::VoteRequest req;
        req.set_term(current_term_);
        req.set_candidate_id(node_id_);
        req.set_last_log_index(my_last_index);
        req.set_last_log_term(my_last_term);

        std::string req_bin;
        req.SerializeToString(&req_bin);

        // 同步发送：election_timer 已持 mutex_，这里不再重复加锁
        // （send_heartbeat 的异步回调也持 mutex_，但那是不同线程，wait 无问题）
        std::string resp_bin = rpc_client_.call(
            peer.ip, peer.port, "raft.RaftService.RequestVote", req_bin);
        if (resp_bin.empty()) continue;

        raft::VoteResponse resp;
        if (!resp.ParseFromString(resp_bin)) continue;

        // 如果对方 term 更大，立刻降级
        if (resp.term() > current_term_) {
            current_term_ = resp.term();
            role_ = Follower;
            voted_for_ = -1;
            persist_state();
            return;
        }

        if (resp.vote_granted()) {
            votes++;
        }

        if (votes > total_nodes_ / 2 && role_ == Candidate) {
            role_ = Leader;
            LOG_INFO("[Election] Node " + std::to_string(node_id_)
                     + " became leader for term " + std::to_string(current_term_));
            for (const auto& p : peers_) {
                nextIndex_[p.id]  = static_cast<int>(log_.size());
                matchIndex_[p.id] = -1;
            }
            heartbeat_thread_ = std::thread(&RaftNode::send_heartbeat, this);
            heartbeat_thread_.detach();
            break;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  心跳 / 日志复制（C1: 持锁构建请求；B3: 响应中检测高 term 降级）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::send_heartbeat() {
    while (!stopped) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (role_ != Leader) break;

            for (const auto& peer : peers_) {
                int peer_id  = peer.id;
                int next_idx = nextIndex_[peer_id];

                raft::AppendRequest req;
                req.set_term(current_term_);
                req.set_leader_id(node_id_);
                req.set_leader_ip(addr_);
                req.set_leader_port(port_);
                req.set_leader_commit(log_commit_index_);

                if (next_idx > 0) {
                    req.set_prev_log_index(next_idx - 1);
                    req.set_prev_log_term(log_[next_idx - 1].term());
                } else {
                    req.set_prev_log_index(-1);
                    req.set_prev_log_term(-1);
                }

                for (int i = next_idx; i < static_cast<int>(log_.size()); ++i) {
                    req.add_entries()->CopyFrom(log_[i]);
                }

                // C2: 捕获发送时的 sent_up_to，而非回调时的 log_.size()-1
                int sent_up_to = static_cast<int>(log_.size()) - 1;

                std::string req_bin;
                req.SerializeToString(&req_bin);

                // call_async 拷贝参数后立即返回，不会死锁
                rpc_client_.call_async(peer.ip, peer.port,
                    "raft.RaftService.AppendEntries", req_bin,
                    [this, peer_id, sent_up_to](const std::string& resp_bin) {
                        if (resp_bin.empty()) return;
                        raft::AppendResponse resp;
                        if (!resp.ParseFromString(resp_bin)) return;

                        std::lock_guard<std::mutex> lk(mutex_);

                        // B3: 检测到更高 term，降级
                        if (resp.term() > current_term_) {
                            current_term_ = resp.term();
                            role_ = Follower;
                            voted_for_ = -1;
                            persist_state();
                            return;
                        }

                        if (role_ != Leader) return;

                        if (resp.success()) {
                            // C2: 使用发送时记录的 sent_up_to
                            if (sent_up_to > matchIndex_[peer_id]) {
                                matchIndex_[peer_id] = sent_up_to;
                                nextIndex_[peer_id]  = sent_up_to + 1;
                            }
                        } else {
                            nextIndex_[peer_id] = std::max(0, nextIndex_[peer_id] - 1);
                        }
                    });
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kHeartbeatIntervalMs));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  写路径：追加日志 + 广播（B1，调用方持 mutex_）
// ─────────────────────────────────────────────────────────────────────────────

int RaftNode::append_and_replicate(const std::string& cmd) {
    // PRECONDITION: mutex_ 已由调用方持有
    raft::LogEntry entry;
    entry.set_term(current_term_);
    entry.set_command(cmd);

    log_.push_back(entry);
    persist_log_entry(entry);

    int new_index = static_cast<int>(log_.size()) - 1;

    LOG_INFO("[Write] Appended log[" + std::to_string(new_index)
             + "] term=" + std::to_string(current_term_) + " cmd=" + cmd);

    for (const auto& peer : peers_) {
        int peer_id  = peer.id;
        int next_idx = nextIndex_[peer_id];

        raft::AppendRequest req;
        req.set_term(current_term_);
        req.set_leader_id(node_id_);
        req.set_leader_ip(addr_);
        req.set_leader_port(port_);
        req.set_leader_commit(log_commit_index_);

        if (next_idx > 0) {
            req.set_prev_log_index(next_idx - 1);
            req.set_prev_log_term(log_[next_idx - 1].term());
        } else {
            req.set_prev_log_index(-1);
            req.set_prev_log_term(-1);
        }

        for (int i = next_idx; i < static_cast<int>(log_.size()); ++i) {
            req.add_entries()->CopyFrom(log_[i]);
        }

        // C2: 捕获此刻的 sent_up_to
        int sent_up_to = new_index;

        std::string req_bin;
        req.SerializeToString(&req_bin);

        rpc_client_.call_async(peer.ip, peer.port,
            "raft.RaftService.AppendEntries", req_bin,
            [this, peer_id, sent_up_to](const std::string& resp_bin) {
                if (resp_bin.empty()) return;
                raft::AppendResponse resp;
                if (!resp.ParseFromString(resp_bin)) return;

                std::lock_guard<std::mutex> lk(mutex_);

                // B3: 高 term → 降级
                if (resp.term() > current_term_) {
                    current_term_ = resp.term();
                    role_ = Follower;
                    voted_for_ = -1;
                    persist_state();
                    commit_cv_.notify_all();   // 唤醒等待中的写请求，告知 leader 已失效
                    return;
                }

                if (role_ != Leader) return;

                if (resp.success()) {
                    if (sent_up_to > matchIndex_[peer_id]) {
                        matchIndex_[peer_id] = sent_up_to;
                        nextIndex_[peer_id]  = sent_up_to + 1;
                    }

                    // 尝试推进 commit index（Raft §5.3 / §5.4）
                    for (int N = log_commit_index_ + 1;
                         N < static_cast<int>(log_.size()); ++N) {
                        // 只能提交当前 term 的日志（Leader Completeness）
                        if (log_[N].term() != current_term_) continue;

                        int replicated = 1; // 自身
                        for (const auto& [id, idx] : matchIndex_) {
                            if (idx >= N) replicated++;
                        }
                        if (replicated > total_nodes_ / 2) {
                            log_commit_index_ = N;
                            LOG_INFO("[Commit] entry " + std::to_string(N) + " committed");
                        }
                    }

                    // E1: 用 last_applied_ 避免重复应用
                    for (int i = last_applied_ + 1; i <= log_commit_index_; ++i) {
                        apply_log(log_[i].command());
                        last_applied_ = i;
                    }

                    // B1: 持久化新 commit index，通知等待的写请求
                    persist_state();
                    commit_cv_.notify_all();
                } else {
                    nextIndex_[peer_id] = std::max(0, nextIndex_[peer_id] - 1);
                }
            });
    }

    return new_index;
}

// ─────────────────────────────────────────────────────────────────────────────
//  B1: 等待 commit（不持 mutex_）
// ─────────────────────────────────────────────────────────────────────────────

bool RaftNode::wait_for_commit(int target_index, int timeout_ms) {
    std::unique_lock<std::mutex> lk(mutex_);
    return commit_cv_.wait_for(lk,
        std::chrono::milliseconds(timeout_ms),
        [this, target_index] {
            return log_commit_index_ >= target_index || role_ != Leader;
        });
}

// ─────────────────────────────────────────────────────────────────────────────
//  处理 AppendEntries（follower / candidate 端）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_append_entries(const ::raft::AppendRequest* request,
                                     ::raft::AppendResponse*      response) {
    std::lock_guard<std::mutex> lock(mutex_);

    int term = request->term();

    if (term < current_term_) {
        response->set_term(current_term_);
        response->set_success(false);
        return;
    }

    // 收到合法 leader 消息：重置心跳时钟（无论是纯心跳还是携带日志）
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    if (term > current_term_) {
        current_term_ = term;
        voted_for_    = -1;
        persist_state();
        role_ = Follower;
    }

    leader_addr_ = request->leader_ip();
    leader_port_ = request->leader_port();

    // 一致性检查
    int prevLogIndex = request->prev_log_index();
    int prevLogTerm  = request->prev_log_term();
    if (prevLogIndex >= static_cast<int>(log_.size()) ||
        (prevLogIndex >= 0 && log_[prevLogIndex].term() != prevLogTerm)) {
        LOG_INFO("[AppendEntries] Inconsistency: prevLogIndex=" + std::to_string(prevLogIndex)
                 + " log_size=" + std::to_string(log_.size()));
        response->set_term(current_term_);
        response->set_success(false);
        return;
    }

    // 日志冲突处理 + 追加
    int idx = prevLogIndex + 1;
    for (int i = 0; i < request->entries_size(); ++i, ++idx) {
        if (idx < static_cast<int>(log_.size())) {
            if (log_[idx].term() != request->entries(i).term()) {
                log_.resize(idx);
                persist_log();
            } else {
                continue;
            }
        }
        log_.push_back(request->entries(i));
        persist_log_entry(request->entries(i));
    }

    // commitIndex 同步：E1 用 last_applied_ 避免重复应用
    if (request->leader_commit() > log_commit_index_) {
        int new_commit = std::min(static_cast<int>(request->leader_commit()),
                                  static_cast<int>(log_.size()) - 1);
        for (int i = last_applied_ + 1; i <= new_commit; ++i) {
            apply_log(log_[i].command());
            last_applied_ = i;
        }
        log_commit_index_ = new_commit;
        persist_state();
    }

    response->set_term(current_term_);
    response->set_success(true);
}

// ─────────────────────────────────────────────────────────────────────────────
//  处理 RequestVote（B2: 加日志完整性检查）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_request_vote(const ::raft::VoteRequest*  request,
                                   ::raft::VoteResponse*       response) {
    std::lock_guard<std::mutex> lock(mutex_);

    int term         = request->term();
    int candidate_id = request->candidate_id();

    if (term < current_term_) {
        response->set_term(current_term_);
        response->set_vote_granted(false);
        return;
    }

    if (term > current_term_) {
        current_term_ = term;
        voted_for_    = -1;
        persist_state();
        role_ = Follower;
    }

    // B2: 日志完整性检查（Raft §5.4.1）
    int my_last_term  = log_.empty() ? -1 : log_.back().term();
    int my_last_index = static_cast<int>(log_.size()) - 1;
    bool log_ok = (request->last_log_term() > my_last_term)
               || (request->last_log_term() == my_last_term
                   && request->last_log_index() >= my_last_index);

    if ((voted_for_ == -1 || voted_for_ == candidate_id) && log_ok) {
        voted_for_ = candidate_id;
        persist_state();
        response->set_vote_granted(true);
        LOG_INFO("[Vote] Granted vote to " + std::to_string(candidate_id)
                 + " for term " + std::to_string(term));
    } else {
        response->set_vote_granted(false);
        LOG_INFO("[Vote] Refused vote to " + std::to_string(candidate_id)
                 + " log_ok=" + std::to_string(log_ok)
                 + " voted_for=" + std::to_string(voted_for_));
    }

    response->set_term(current_term_);
}
