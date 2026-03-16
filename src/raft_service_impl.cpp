#include "raft_node.h"
#include "raft_service_impl.h"

void RaftServiceImpl::RequestVote(::google::protobuf::RpcController*,
    const ::raft::VoteRequest* request,
    ::raft::VoteResponse* response,
    ::google::protobuf::Closure* done)
{
    myrpc::ClosureGuard guard(done);  // ClosureGuard 析构时调用 done->Run()，不再手动调用
    node_->handle_request_vote(request, response);
}

void RaftServiceImpl::AppendEntries(::google::protobuf::RpcController*,
    const ::raft::AppendRequest* request,
    ::raft::AppendResponse* response,
    ::google::protobuf::Closure* done)
{
    myrpc::ClosureGuard guard(done);
    node_->handle_append_entries(request, response);
}

void RaftServiceImpl::ClientStorage(::google::protobuf::RpcController*,
    const ::raft::ClientRequest* request,
    ::raft::ClientResponse* response,
    ::google::protobuf::Closure* done)
{
    myrpc::ClosureGuard guard(done);

    const std::string& cmd = request->command();
    bool is_read = (cmd.rfind("GET ", 0) == 0 || cmd.rfind("LIST ", 0) == 0);

    {
        // ── 持锁段：检查 leader 身份，处理读请求或追加写日志 ────────────────
        std::unique_lock<std::mutex> lk(node_->mutex_);

        if (node_->role_ != RaftNode::Leader) {
            response->set_success(false);
            response->set_addr(node_->leader_addr_);
            response->set_port(node_->leader_port_);
            return;  // guard 析构时调用 done->Run()
        }

        if (is_read) {
            // A3/A4: 读路径 — leader local read，不走 Raft log
            response->set_success(true);
            response->set_value(node_->kv_query(cmd));
            return;
        }

        // 写路径：追加日志 + 广播，持锁结束后等待 commit
        int target_index = node_->append_and_replicate(cmd);
        // lk 析构 → mutex_ 释放

        // B1: 在 mutex_ 释放后等待多数派提交（wait_for_commit 内部重新加锁）
        lk.unlock();
        bool committed = node_->wait_for_commit(target_index, 3000);
        response->set_success(committed);
    }
    // guard 析构时调用 done->Run()
}

void RaftServiceImpl::InstallSnapshot(::google::protobuf::RpcController*,
    const ::raft::InstallSnapshotRequest* request,
    ::raft::InstallSnapshotResponse* response,
    ::google::protobuf::Closure* done)
{
    myrpc::ClosureGuard guard(done);
    node_->handle_install_snapshot(request, response);
}
