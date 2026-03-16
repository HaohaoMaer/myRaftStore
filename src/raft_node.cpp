#include "raft_node.h"
#include <fstream>
#include <cstring>   // memcpy
#include <zlib.h>    // crc32

// 将文件数据强制同步到磁盘（crash-safe 持久化）
static void fsync_path(const std::string& path) {
    int fd = ::open(path.c_str(), O_WRONLY);
    if (fd != -1) { ::fdatasync(fd); ::close(fd); }
}

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
      rpc_client_(5),
      rng_(static_cast<uint64_t>(
               std::chrono::steady_clock::now().time_since_epoch().count()) ^
           (static_cast<uint64_t>(id) * 2654435761u))
{
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    service_impl_.SetNode(this);
}

RaftNode::~RaftNode() {
    // 通知所有后台线程退出
    stopped = true;
    commit_cv_.notify_all();

    // 等待选举定时器线程退出（election_timer 检查 stopped 标志）
    if (timer_thread_.joinable())     timer_thread_.join();
    // 等待心跳线程退出（send_heartbeat 检查 stopped 标志）
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();

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
    std::thread([this]() { rpc_server_.start(); }).detach();  // RpcServer 无退出机制，保持 detach
    timer_thread_ = std::thread(&RaftNode::election_timer, this);
    // 不 detach：析构函数负责 join
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
    std::string path = "raft_state_" + std::to_string(node_id_) + ".txt";
    std::ofstream ofs(path, std::ios::trunc);
    if (!ofs) { LOG_ERROR("Failed to open raft_state for writing"); return; }
    ofs << current_term_ << " " << voted_for_ << " " << log_commit_index_ << "\n";
    ofs.flush();
    ofs.close();
    fsync_path(path);  // 确保数据写入磁盘，避免崩溃时状态丢失
}

void RaftNode::persist_log() {
    // 全量覆盖写（用于日志截断后重写）
    std::string path = "raft_log_" + std::to_string(node_id_) + ".bin";
    {
        std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
        for (const auto& entry : log_) {
            std::string bin;
            entry.SerializeToString(&bin);
            uint32_t len = static_cast<uint32_t>(bin.size());
            ofs.write(reinterpret_cast<char*>(&len), sizeof(len));
            ofs.write(bin.data(), len);
        }
        ofs.flush();
    }
    fsync_path(path);
}

void RaftNode::persist_log_entry(const raft::LogEntry& entry) {
    std::string path = "raft_log_" + std::to_string(node_id_) + ".bin";
    {
        std::ofstream ofs(path, std::ios::binary | std::ios::app);
        if (!ofs) { LOG_ERROR("Failed to open raft_log for appending"); return; }
        std::string bin;
        if (!entry.SerializeToString(&bin)) { LOG_ERROR("Failed to serialize log entry"); return; }
        uint32_t len = static_cast<uint32_t>(bin.size());
        ofs.write(reinterpret_cast<char*>(&len), sizeof(len));
        ofs.write(bin.data(), len);
        ofs.flush();
    }
    fsync_path(path);
}

// ─────────────────────────────────────────────────────────────────────────────
//  快照持久化
// ─────────────────────────────────────────────────────────────────────────────

// 文件格式：
//   [4B Magic: 0x52414654] [4B Version: 1]
//   [int32 snapshot_last_index] [int32 snapshot_last_term]
//   [uint32 data_length] [data_length bytes: SnapshotData protobuf]
//   [4B CRC32 校验和（覆盖 magic 到 data 末尾）]

static constexpr uint32_t kSnapshotMagic   = 0x52414654u;  // "RAFT"
static constexpr uint32_t kSnapshotVersion = 1u;

bool RaftNode::save_snapshot() {
    // 调用方持 mutex_
    // 构造 SnapshotData
    raft::SnapshotData snap_data;
    for (const auto& [k, v] : kv_store) {
        auto* pair = snap_data.add_kv_pairs();
        pair->set_key(k);
        pair->set_value(v);
    }

    std::string data_bin;
    if (!snap_data.SerializeToString(&data_bin)) {
        LOG_ERROR("[Snapshot] Failed to serialize SnapshotData");
        return false;
    }

    // 构建完整文件内容（不含 CRC32）
    std::string content;
    auto append_u32 = [&](uint32_t v) {
        content.append(reinterpret_cast<const char*>(&v), 4);
    };
    auto append_i32 = [&](int32_t v) {
        content.append(reinterpret_cast<const char*>(&v), 4);
    };

    append_u32(kSnapshotMagic);
    append_u32(kSnapshotVersion);
    append_i32(static_cast<int32_t>(snapshot_last_index_));
    append_i32(static_cast<int32_t>(snapshot_last_term_));
    uint32_t data_len = static_cast<uint32_t>(data_bin.size());
    append_u32(data_len);
    content.append(data_bin);

    // 计算 CRC32
    uint32_t crc = static_cast<uint32_t>(::crc32(0L, Z_NULL, 0));
    crc = static_cast<uint32_t>(
        ::crc32(crc, reinterpret_cast<const Bytef*>(content.data()), content.size()));
    content.append(reinterpret_cast<const char*>(&crc), 4);

    // 原子写：先写临时文件，再 rename
    std::string snap_file = "raft_snapshot_" + std::to_string(node_id_) + ".bin";
    std::string tmp_file  = snap_file + ".tmp";

    {
        std::ofstream ofs(tmp_file, std::ios::binary | std::ios::trunc);
        if (!ofs) {
            LOG_ERROR("[Snapshot] Failed to open tmp snapshot file for writing");
            return false;
        }
        ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
        ofs.flush();
    }

    if (std::rename(tmp_file.c_str(), snap_file.c_str()) != 0) {
        LOG_ERROR("[Snapshot] Failed to rename tmp snapshot file");
        return false;
    }

    LOG_INFO("[Snapshot] Saved snapshot: last_index=" + std::to_string(snapshot_last_index_)
             + " last_term=" + std::to_string(snapshot_last_term_)
             + " kv_size=" + std::to_string(kv_store.size()));
    return true;
}

bool RaftNode::load_snapshot() {
    // 单线程，启动时调用
    std::string snap_file = "raft_snapshot_" + std::to_string(node_id_) + ".bin";
    std::ifstream ifs(snap_file, std::ios::binary);
    if (!ifs) return false;

    // 读取全部内容
    std::string content((std::istreambuf_iterator<char>(ifs)),
                         std::istreambuf_iterator<char>());
    if (content.size() < 24u) {  // 4+4+4+4+4+4 = 24 bytes minimum
        LOG_ERROR("[Snapshot] Snapshot file too small");
        return false;
    }

    // 验证 CRC32
    uint32_t stored_crc;
    std::memcpy(&stored_crc, content.data() + content.size() - 4, 4);
    uint32_t calc_crc = static_cast<uint32_t>(::crc32(0L, Z_NULL, 0));
    calc_crc = static_cast<uint32_t>(
        ::crc32(calc_crc,
                reinterpret_cast<const Bytef*>(content.data()),
                content.size() - 4));
    if (calc_crc != stored_crc) {
        LOG_ERROR("[Snapshot] Snapshot CRC32 mismatch");
        return false;
    }

    size_t offset = 0;
    auto read_u32 = [&]() -> uint32_t {
        uint32_t v;
        std::memcpy(&v, content.data() + offset, 4);
        offset += 4;
        return v;
    };
    auto read_i32 = [&]() -> int32_t {
        int32_t v;
        std::memcpy(&v, content.data() + offset, 4);
        offset += 4;
        return v;
    };

    uint32_t magic   = read_u32();
    uint32_t version = read_u32();
    if (magic != kSnapshotMagic) {
        LOG_ERROR("[Snapshot] Invalid snapshot magic");
        return false;
    }
    if (version != kSnapshotVersion) {
        LOG_ERROR("[Snapshot] Unsupported snapshot version: " + std::to_string(version));
        return false;
    }

    int32_t  snap_index = read_i32();
    int32_t  snap_term  = read_i32();
    uint32_t data_len   = read_u32();

    if (offset + data_len + 4 > content.size()) {
        LOG_ERROR("[Snapshot] Snapshot data length mismatch");
        return false;
    }

    raft::SnapshotData snap_data;
    if (!snap_data.ParseFromArray(content.data() + offset, static_cast<int>(data_len))) {
        LOG_ERROR("[Snapshot] Failed to parse SnapshotData");
        return false;
    }

    // 重建 kv_store
    kv_store.clear();
    for (const auto& pair : snap_data.kv_pairs()) {
        kv_store[pair.key()] = pair.value();
    }

    snapshot_last_index_ = snap_index;
    snapshot_last_term_  = snap_term;
    last_applied_        = snap_index;

    LOG_INFO("[Snapshot] Loaded snapshot: last_index=" + std::to_string(snapshot_last_index_)
             + " last_term=" + std::to_string(snapshot_last_term_)
             + " kv_size=" + std::to_string(kv_store.size()));
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
//  take_snapshot()：截断日志并持久化快照（调用方持 mutex_）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::take_snapshot() {
    if (last_applied_ <= snapshot_last_index_) return;  // 无新内容可快照

    int old_snap_index   = snapshot_last_index_;
    int new_snap_index   = last_applied_;
    int new_snap_term    = log_term_at(new_snap_index);

    snapshot_last_index_ = new_snap_index;
    snapshot_last_term_  = new_snap_term;

    save_snapshot();

    // 截断 log_：删除逻辑索引 <= new_snap_index 的物理条目
    int erase_count = new_snap_index - old_snap_index;
    if (erase_count > 0 && erase_count <= static_cast<int>(log_.size())) {
        log_.erase(log_.begin(), log_.begin() + erase_count);
    } else if (erase_count > static_cast<int>(log_.size())) {
        log_.clear();
    }

    persist_log();

    LOG_INFO("[Snapshot] Took snapshot at index=" + std::to_string(new_snap_index)
             + " remaining_log=" + std::to_string(log_.size()));
}

// ─────────────────────────────────────────────────────────────────────────────
//  install_snapshot()：安装远端发来的快照（调用方持 mutex_）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::install_snapshot(int last_index, int last_term,
                                const raft::SnapshotData& data) {
    // 重建 kv_store
    kv_store.clear();
    for (const auto& pair : data.kv_pairs()) {
        kv_store[pair.key()] = pair.value();
    }

    // 尝试保留快照之后的日志条目
    // 若 log_ 中存在 last_index 且 term 匹配，则保留其后的条目
    bool kept_suffix = false;
    if (last_index > snapshot_last_index_) {
        int phys = log_physical_index(last_index);
        if (phys >= 0 && phys < static_cast<int>(log_.size()) &&
            log_[phys].term() == last_term) {
            log_.erase(log_.begin(), log_.begin() + phys + 1);
            kept_suffix = true;
        }
    }
    if (!kept_suffix) {
        log_.clear();
    }

    snapshot_last_index_ = last_index;
    snapshot_last_term_  = last_term;
    log_commit_index_    = std::max(log_commit_index_, last_index);
    last_applied_        = last_index;

    save_snapshot();
    persist_log();
    persist_state();

    LOG_INFO("[Snapshot] Installed snapshot: last_index=" + std::to_string(last_index)
             + " last_term=" + std::to_string(last_term)
             + " kv_size=" + std::to_string(kv_store.size()));
}

// ─────────────────────────────────────────────────────────────────────────────
//  send_install_snapshot_to()：向落后 Follower 发送快照（调用方持 mutex_）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::send_install_snapshot_to(const PeerInfo& peer) {
    raft::InstallSnapshotRequest req;
    req.set_term(current_term_);
    req.set_leader_id(node_id_);
    req.set_leader_ip(addr_);
    req.set_leader_port(port_);
    req.set_last_included_index(snapshot_last_index_);
    req.set_last_included_term(snapshot_last_term_);

    raft::SnapshotData* data = req.mutable_data();
    for (const auto& [k, v] : kv_store) {
        auto* pair = data->add_kv_pairs();
        pair->set_key(k);
        pair->set_value(v);
    }

    std::string req_bin;
    req.SerializeToString(&req_bin);

    int peer_id       = peer.id;
    int snap_index    = snapshot_last_index_;
    int snap_term_cap = current_term_;

    rpc_client_.call_async(peer.ip, peer.port,
        "raft.RaftService.InstallSnapshot", req_bin,
        [this, peer_id, snap_index, snap_term_cap](const std::string& resp_bin) {
            if (resp_bin.empty()) return;
            raft::InstallSnapshotResponse resp;
            if (!resp.ParseFromString(resp_bin)) return;

            std::lock_guard<std::mutex> lk(mutex_);

            if (resp.term() > current_term_) {
                current_term_ = resp.term();
                role_         = Follower;
                voted_for_    = -1;
                persist_state();
                return;
            }

            if (role_ != Leader) return;

            if (resp.success()) {
                if (snap_index > matchIndex_[peer_id]) {
                    matchIndex_[peer_id] = snap_index;
                    nextIndex_[peer_id]  = snap_index + 1;
                }
            }
        });
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

    // 2. 加载快照（若存在）：恢复 kv_store、snapshot 元数据、last_applied_
    load_snapshot();

    // 3. 加载 log（快照后的增量日志）
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

    // 4. 安全截断：确保 log_commit_index_ 在有效范围内
    log_commit_index_ = std::min(log_commit_index_, last_log_index());
    log_commit_index_ = std::max(log_commit_index_, snapshot_last_index_);

    // 5. 只重放增量（快照已覆盖 [0..snapshot_last_index_]）
    for (int i = last_applied_ + 1; i <= log_commit_index_; ++i) {
        apply_log(log_entry_at(i).command());
        last_applied_ = i;
    }

    LOG_INFO("Recovered: term=" + std::to_string(current_term_)
             + " voted_for=" + std::to_string(voted_for_)
             + " commit=" + std::to_string(log_commit_index_)
             + " snap_index=" + std::to_string(snapshot_last_index_)
             + " log_size=" + std::to_string(log_.size())
             + " kv_size=" + std::to_string(kv_store.size()));
}

// ─────────────────────────────────────────────────────────────────────────────
//  选举定时器
// ─────────────────────────────────────────────────────────────────────────────

int RaftNode::random_election_timeout() {
    // 使用成员 mt19937（每个节点种子不同），避免多节点同步超时导致 split vote
    std::uniform_int_distribution<int> dist(600, 1000);
    return dist(rng_);
}

void RaftNode::election_timer() {
    auto startup_time = std::chrono::steady_clock::now();
    while (!stopped) {
        int timeout = random_election_timeout();
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout));

        auto now = std::chrono::steady_clock::now();
        std::unique_lock<std::mutex> lock(mutex_);  // unique_lock 供 start_election 临时释放

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
                start_election(lock);  // lock 会在 RPC 期间临时释放，返回时已重新持有
                last_heartbeat_time_ = std::chrono::steady_clock::now();
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  选举（调用方传入 unique_lock；RPC 期间临时释放锁，返回时确保锁已重新持有）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::start_election(std::unique_lock<std::mutex>& lk) {
    current_term_++;
    voted_for_ = node_id_;
    role_ = Candidate;
    persist_state();
    int votes = 1;

    int my_term       = current_term_;
    int my_last_index = last_log_index();
    int my_last_term  = log_term_at(my_last_index);
    auto peers_snap   = peers_;  // 持锁时快照 peer 列表

    LOG_INFO("[Election] Node " + std::to_string(node_id_)
             + " starting election for term " + std::to_string(my_term));

    for (const auto& peer : peers_snap) {
        raft::VoteRequest req;
        req.set_term(my_term);
        req.set_candidate_id(node_id_);
        req.set_last_log_index(my_last_index);
        req.set_last_log_term(my_last_term);

        std::string req_bin;
        req.SerializeToString(&req_bin);

        lk.unlock();  // 释放锁，避免持锁期间阻塞心跳/日志复制
        std::string resp_bin = rpc_client_.call(
            peer.ip, peer.port, "raft.RaftService.RequestVote", req_bin);
        lk.lock();    // 重新持锁处理响应

        // 选举已过期（被更高 term 覆盖或角色已变）
        if (role_ != Candidate || current_term_ != my_term) return;

        if (resp_bin.empty()) continue;
        raft::VoteResponse resp;
        if (!resp.ParseFromString(resp_bin)) continue;

        if (resp.term() > current_term_) {
            current_term_ = resp.term();
            role_ = Follower;
            voted_for_ = -1;
            persist_state();
            return;
        }

        if (resp.vote_granted()) votes++;

        if (votes > total_nodes_ / 2 && role_ == Candidate) {
            role_ = Leader;
            LOG_INFO("[Election] Node " + std::to_string(node_id_)
                     + " became leader for term " + std::to_string(current_term_));
            for (const auto& p : peers_) {
                nextIndex_[p.id]  = last_log_index() + 1;
                matchIndex_[p.id] = -1;
            }
            // 若上一任期心跳线程仍可 join，先 detach（它会在下次循环检测 role_ != Leader 退出）
            if (heartbeat_thread_.joinable()) heartbeat_thread_.detach();
            heartbeat_thread_ = std::thread(&RaftNode::send_heartbeat, this);
            // 不 detach：析构函数负责 join
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

                // 若 Follower 落后于快照，发送 InstallSnapshot
                if (next_idx <= snapshot_last_index_) {
                    send_install_snapshot_to(peer);
                    continue;
                }

                raft::AppendRequest req;
                req.set_term(current_term_);
                req.set_leader_id(node_id_);
                req.set_leader_ip(addr_);
                req.set_leader_port(port_);
                req.set_leader_commit(log_commit_index_);

                if (next_idx > 0) {
                    req.set_prev_log_index(next_idx - 1);
                    req.set_prev_log_term(log_term_at(next_idx - 1));
                } else {
                    req.set_prev_log_index(-1);
                    req.set_prev_log_term(-1);
                }

                int ll_idx = last_log_index();
                for (int i = next_idx; i <= ll_idx; ++i) {
                    req.add_entries()->CopyFrom(log_entry_at(i));
                }

                // C2: 捕获发送时的 sent_up_to
                int sent_up_to = ll_idx;

                std::string req_bin;
                req.SerializeToString(&req_bin);

                rpc_client_.call_async(peer.ip, peer.port,
                    "raft.RaftService.AppendEntries", req_bin,
                    [this, peer_id, sent_up_to](const std::string& resp_bin) {
                        if (resp_bin.empty()) return;
                        raft::AppendResponse resp;
                        if (!resp.ParseFromString(resp_bin)) return;

                        std::lock_guard<std::mutex> lk(mutex_);

                        if (resp.term() > current_term_) {
                            current_term_ = resp.term();
                            role_ = Follower;
                            voted_for_ = -1;
                            persist_state();
                            return;
                        }

                        if (role_ != Leader) return;

                        if (resp.success()) {
                            if (sent_up_to > matchIndex_[peer_id]) {
                                matchIndex_[peer_id] = sent_up_to;
                                nextIndex_[peer_id]  = sent_up_to + 1;
                            }
                        } else {
                            nextIndex_[peer_id] = std::max(snapshot_last_index_ + 1,
                                                           nextIndex_[peer_id] - 1);
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

    int new_index = last_log_index();

    LOG_INFO("[Write] Appended log[" + std::to_string(new_index)
             + "] term=" + std::to_string(current_term_) + " cmd=" + cmd);

    for (const auto& peer : peers_) {
        int peer_id  = peer.id;
        int next_idx = nextIndex_[peer_id];

        // 若 Follower 落后于快照，发送 InstallSnapshot
        if (next_idx <= snapshot_last_index_) {
            send_install_snapshot_to(peer);
            continue;
        }

        raft::AppendRequest req;
        req.set_term(current_term_);
        req.set_leader_id(node_id_);
        req.set_leader_ip(addr_);
        req.set_leader_port(port_);
        req.set_leader_commit(log_commit_index_);

        if (next_idx > 0) {
            req.set_prev_log_index(next_idx - 1);
            req.set_prev_log_term(log_term_at(next_idx - 1));
        } else {
            req.set_prev_log_index(-1);
            req.set_prev_log_term(-1);
        }

        int ll_idx = last_log_index();
        for (int i = next_idx; i <= ll_idx; ++i) {
            req.add_entries()->CopyFrom(log_entry_at(i));
        }

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

                if (resp.term() > current_term_) {
                    current_term_ = resp.term();
                    role_ = Follower;
                    voted_for_ = -1;
                    persist_state();
                    commit_cv_.notify_all();
                    return;
                }

                if (role_ != Leader) return;

                if (resp.success()) {
                    if (sent_up_to > matchIndex_[peer_id]) {
                        matchIndex_[peer_id] = sent_up_to;
                        nextIndex_[peer_id]  = sent_up_to + 1;
                    }

                    // 尝试推进 commit index（Raft §5.3 / §5.4）
                    int ll = last_log_index();
                    for (int N = log_commit_index_ + 1; N <= ll; ++N) {
                        if (log_term_at(N) != current_term_) continue;

                        int replicated = 1;
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
                        apply_log(log_entry_at(i).command());
                        last_applied_ = i;
                    }

                    persist_state();
                    commit_cv_.notify_all();

                    // 快照触发检查
                    if (last_applied_ - snapshot_last_index_ >= kSnapshotThreshold) {
                        take_snapshot();
                    }
                } else {
                    nextIndex_[peer_id] = std::max(snapshot_last_index_ + 1,
                                                   nextIndex_[peer_id] - 1);
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

    last_heartbeat_time_ = std::chrono::steady_clock::now();

    if (term > current_term_) {
        current_term_ = term;
        voted_for_    = -1;
        persist_state();
        role_ = Follower;
    } else if (term == current_term_ && role_ == Candidate) {
        // Raft §5.2: Candidate 收到同 term 合法 leader 的 AppendEntries，必须降级为 Follower
        role_ = Follower;
    }

    leader_addr_ = request->leader_ip();
    leader_port_ = request->leader_port();

    // 一致性检查
    int prevLogIndex = request->prev_log_index();
    int prevLogTerm  = request->prev_log_term();

    // prevLogIndex 在快照范围内或等于 snapshot_last_index_ 均视为一致
    if (prevLogIndex > snapshot_last_index_) {
        if (prevLogIndex > last_log_index() ||
            log_term_at(prevLogIndex) != prevLogTerm) {
            LOG_INFO("[AppendEntries] Inconsistency: prevLogIndex=" + std::to_string(prevLogIndex)
                     + " last_log_index=" + std::to_string(last_log_index()));
            response->set_term(current_term_);
            response->set_success(false);
            return;
        }
    } else if (prevLogIndex >= 0 && prevLogIndex < snapshot_last_index_) {
        // prevLogIndex 在快照范围内，认为已一致，但要跳过快照范围内的条目
        // （这种情况下 leader 应发送 InstallSnapshot，这里尽量接受）
    }

    // 日志冲突处理 + 追加
    int idx = prevLogIndex + 1;
    bool truncated = false;
    for (int i = 0; i < request->entries_size(); ++i, ++idx) {
        if (idx <= snapshot_last_index_) continue;  // 跳过快照已覆盖的条目

        if (idx <= last_log_index()) {
            if (log_term_at(idx) != request->entries(i).term()) {
                // 截断：物理位置 log_physical_index(idx)
                log_.resize(log_physical_index(idx));
                persist_log();
                truncated = true;
            } else {
                continue;
            }
        }
        log_.push_back(request->entries(i));
        if (!truncated) {
            persist_log_entry(request->entries(i));
        }
    }
    if (truncated) {
        // 重写整个 log 文件（已在 persist_log() 中完成截断，但新追加的条目需重写）
        persist_log();
    }

    // commitIndex 同步
    if (request->leader_commit() > log_commit_index_) {
        int new_commit = std::min(static_cast<int>(request->leader_commit()),
                                  last_log_index());
        for (int i = last_applied_ + 1; i <= new_commit; ++i) {
            apply_log(log_entry_at(i).command());
            last_applied_ = i;
        }
        log_commit_index_ = new_commit;
        persist_state();

        // 快照触发检查
        if (last_applied_ - snapshot_last_index_ >= kSnapshotThreshold) {
            take_snapshot();
        }
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
    int my_last_index = last_log_index();
    int my_last_term  = log_term_at(my_last_index);
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

// ─────────────────────────────────────────────────────────────────────────────
//  处理 InstallSnapshot（Follower 端）
// ─────────────────────────────────────────────────────────────────────────────

void RaftNode::handle_install_snapshot(const ::raft::InstallSnapshotRequest* request,
                                       ::raft::InstallSnapshotResponse*      response) {
    std::lock_guard<std::mutex> lock(mutex_);

    int term = request->term();

    if (term < current_term_) {
        response->set_term(current_term_);
        response->set_success(false);
        return;
    }

    last_heartbeat_time_ = std::chrono::steady_clock::now();

    if (term > current_term_) {
        current_term_ = term;
        voted_for_    = -1;
        persist_state();
        role_ = Follower;
    }

    int last_index = request->last_included_index();
    int last_term  = request->last_included_term();

    // 忽略已有更新快照的请求
    if (last_index <= snapshot_last_index_) {
        LOG_INFO("[InstallSnapshot] Ignored (already have newer snapshot)");
        response->set_term(current_term_);
        response->set_success(true);
        return;
    }

    install_snapshot(last_index, last_term, request->data());

    response->set_term(current_term_);
    response->set_success(true);
}
