#pragma once
#include "raft_service_impl.h"
#include <my_rpc.h>
#include <condition_variable>

struct PeerInfo {
    int id;
    std::string ip;
    int port;
};

class RaftNode {
public:
    RaftNode(int node_id, std::string addr, int port, const std::vector<PeerInfo>& peers);
    ~RaftNode();

    void start(); // 启动 RPC 服务 + 定时任务

    // ── 状态机（由 commit 路径内部调用，或 load 时 replay）──────────────────
    void apply_log(const std::string& cmd);

    // ── 供 RaftServiceImpl 调用的公共接口 ───────────────────────────────────

    // 读操作：leader 直接从本地 kv_store 读（调用方需持 mutex_）
    std::string kv_query(const std::string& cmd);

    // 写操作：追加日志 + 向所有 peer 广播 AppendEntries（调用方需持 mutex_）
    // 返回新日志条目的 index；调用方需在释放 mutex_ 后调用 wait_for_commit
    int append_and_replicate(const std::string& cmd);

    // 等待 log_commit_index_ >= target_index（不持 mutex_）
    // timeout_ms 超时后返回 false
    bool wait_for_commit(int target_index, int timeout_ms);

private:
    void start_election();
    void send_heartbeat();
    void handle_append_entries(const ::raft::AppendRequest* request, ::raft::AppendResponse* response);
    void handle_request_vote(const ::raft::VoteRequest* request, ::raft::VoteResponse* response);
    void election_timer();
    void log_write();
    void persist_state();
    void persist_log();
    void persist_log_entry(const raft::LogEntry& entry);
    void load_persistent_state();
    int  random_election_timeout();

    // ── 持久化字段 ──────────────────────────────────────────────────────────
    int current_term_{0};
    int voted_for_{-1};
    int log_commit_index_{-1};  // 已提交的最高日志 index（持久化）
    int last_applied_{-1};      // 已应用到状态机的最高 index（运行时）

    // ── 节点信息 ────────────────────────────────────────────────────────────
    int         node_id_;
    std::string addr_;
    int         port_;
    std::string leader_addr_;
    int         leader_port_{0};

    // ── 集群 ────────────────────────────────────────────────────────────────
    std::vector<PeerInfo>               peers_;
    int                                 total_nodes_;
    std::unordered_map<int, int>        nextIndex_;   // peer_id → 下次发的 index
    std::unordered_map<int, int>        matchIndex_;  // peer_id → 已确认最高 index

    // ── 日志 ────────────────────────────────────────────────────────────────
    std::vector<raft::LogEntry>                    log_;
    std::unordered_map<std::string, std::string>   kv_store;

    // ── 角色 / 同步原语 ─────────────────────────────────────────────────────
    enum Role { Follower, Candidate, Leader };
    Role role_{Follower};

    std::mutex              mutex_;       // 保护所有 Raft 状态
    std::condition_variable commit_cv_;   // 配合 mutex_，通知写请求提交完成

    // ── 时间 ────────────────────────────────────────────────────────────────
    std::chrono::steady_clock::time_point last_heartbeat_time_;

    // ── 线程 ────────────────────────────────────────────────────────────────
    std::atomic<bool> stopped{false};
    std::thread       timer_thread_;
    std::thread       heartbeat_thread_;

    // ── RPC ─────────────────────────────────────────────────────────────────
    myrpc::RpcServer rpc_server_;
    myrpc::RpcClient rpc_client_;
    RaftServiceImpl  service_impl_;

    // ── 日志系统 ─────────────────────────────────────────────────────────────
    int m_close_log{0};
    int m_log_write{1};

    static constexpr int kHeartbeatIntervalMs = 100;

    friend class RaftServiceImpl;
};
