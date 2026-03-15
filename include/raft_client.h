#pragma once

#include "my_rpc.h"
#include "raft_service_impl.h"
#include "raft.pb.h"
#include <vector>
#include <string>
#include <mutex>

// ─────────────────────────────────────────────────────────────────────────────
//  RaftClient — 带 leader 自动重定向的 Raft KV 客户端
//
//  集群节点列表在构造时给定；每次请求先尝试缓存的 leader 地址，
//  若收到 success=false + redirect 则更新 leader 并重试，
//  最多重试 max_retries 次。
// ─────────────────────────────────────────────────────────────────────────────
class RaftClient {
public:
    // cluster_nodes: Raft 集群所有节点的 {ip, port}
    explicit RaftClient(std::vector<std::pair<std::string, int>> cluster_nodes)
        : nodes_(std::move(cluster_nodes)),
          leader_ip_(nodes_[0].first),
          leader_port_(nodes_[0].second),
          rpc_client_(1)
    {}

    // PUT key value  →  success / failure
    bool put(const std::string& key, const std::string& value) {
        return send_write("PUT " + key + " " + value);
    }

    // DELETE key  →  success / failure
    bool del(const std::string& key) {
        return send_write("DELETE " + key);
    }

    // GET key  →  value string（若不存在返回 ""）
    std::string get(const std::string& key) {
        return send_read("GET " + key);
    }

    // LIST prefix  →  换行分隔的 "key=value" 列表
    std::string list(const std::string& prefix) {
        return send_read("LIST " + prefix);
    }

private:
    // 发送写命令（PUT / DELETE），处理 leader 重定向
    bool send_write(const std::string& cmd) {
        std::string cur_ip; int cur_port;
        {
            std::lock_guard<std::mutex> lk(mu_);
            cur_ip = leader_ip_; cur_port = leader_port_;
        }

        // 先尝试缓存 leader，再轮询其余节点
        auto order = build_order(cur_ip, cur_port);

        for (const auto& [ip, port] : order) {
            raft::ClientResponse resp;
            if (!do_call(ip, port, cmd, resp)) continue;

            if (resp.success()) {
                std::lock_guard<std::mutex> lk(mu_);
                leader_ip_ = ip; leader_port_ = port;
                return true;
            }

            // 收到 redirect hint
            if (!resp.addr().empty() && resp.port() > 0) {
                std::lock_guard<std::mutex> lk(mu_);
                leader_ip_ = resp.addr(); leader_port_ = resp.port();
            }
        }
        return false;
    }

    // 发送读命令（GET / LIST），处理 leader 重定向
    std::string send_read(const std::string& cmd) {
        std::string cur_ip; int cur_port;
        {
            std::lock_guard<std::mutex> lk(mu_);
            cur_ip = leader_ip_; cur_port = leader_port_;
        }

        auto order = build_order(cur_ip, cur_port);

        for (const auto& [ip, port] : order) {
            raft::ClientResponse resp;
            if (!do_call(ip, port, cmd, resp)) continue;

            if (resp.success()) {
                std::lock_guard<std::mutex> lk(mu_);
                leader_ip_ = ip; leader_port_ = port;
                return resp.value();
            }

            if (!resp.addr().empty() && resp.port() > 0) {
                std::lock_guard<std::mutex> lk(mu_);
                leader_ip_ = resp.addr(); leader_port_ = resp.port();
            }
        }
        return "";
    }

    // 构造尝试顺序：先 leader 候选，再其余节点
    std::vector<std::pair<std::string, int>>
    build_order(const std::string& hint_ip, int hint_port) const {
        std::vector<std::pair<std::string, int>> order;
        order.push_back({hint_ip, hint_port});
        for (const auto& n : nodes_) {
            if (n.first != hint_ip || n.second != hint_port)
                order.push_back(n);
        }
        return order;
    }

    // 执行一次 RPC，成功解析返回 true
    bool do_call(const std::string& ip, int port,
                 const std::string& cmd,
                 raft::ClientResponse& resp) {
        raft::ClientRequest req;
        req.set_command(cmd);
        std::string req_bin;
        req.SerializeToString(&req_bin);
        try {
            std::string resp_bin = rpc_client_.call(
                ip, port, "raft.RaftService.ClientStorage", req_bin);
            return resp.ParseFromString(resp_bin);
        } catch (...) {
            return false;
        }
    }

    std::vector<std::pair<std::string, int>> nodes_;
    std::string leader_ip_;
    int         leader_port_;
    myrpc::RpcClient rpc_client_;
    std::mutex  mu_;
};
