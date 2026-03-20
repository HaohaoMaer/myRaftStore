#pragma once
#include "raft.pb.h"
#include "closure_guard.h"

class RaftNode;

class RaftServiceImpl : public raft::RaftService {
 public:
  explicit RaftServiceImpl():node_(nullptr){}

  void SetNode(RaftNode* node) {
      node_ = node;
  }

  void RequestVote(::google::protobuf::RpcController* controller,
            const ::raft::VoteRequest* request,
            ::raft::VoteResponse* response,
            ::google::protobuf::Closure* done);

  void AppendEntries(::google::protobuf::RpcController* controller,
                const ::raft::AppendRequest* request,
                ::raft::AppendResponse* response,
                ::google::protobuf::Closure* done);

  void ClientStorage(::google::protobuf::RpcController* controller,
            const ::raft::ClientRequest* request,
            ::raft::ClientResponse* response,
            ::google::protobuf::Closure* done);

  void InstallSnapshot(::google::protobuf::RpcController* controller,
            const ::raft::InstallSnapshotRequest* request,
            ::raft::InstallSnapshotResponse* response,
            ::google::protobuf::Closure* done);

private:
    RaftNode* node_;
};