#pragma once

#include "raft_client.h"
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <sstream>
#include <functional>

// ─────────────────────────────────────────────────────────────────────────────
//  服务注册与发现设计说明
//
//  Key 格式 : /svc/{service_name}/{instance_id}
//  Value 格式: {ip}:{port}:{unix_timestamp_sec}
//
//  示例:
//    Key   = /svc/calculator/node1
//    Value = 127.0.0.1:9001:1709000000
//
//  TTL 语义:
//    ServiceRegistry 每隔 (ttl_sec/3) 秒更新一次 timestamp（心跳）。
//    ServiceDiscovery::discover() 过滤掉 timestamp 超过 ttl_sec 的条目，
//    视为实例已失联。
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
//  ServiceRegistry — 服务提供方：注册 / 心跳 / 注销
// ─────────────────────────────────────────────────────────────────────────────
class ServiceRegistry {
public:
    // cluster_nodes : Raft 集群节点列表 {ip, port}
    // service_name  : 服务名，如 "calculator"
    // instance_id   : 本实例唯一 ID，如 "node1"
    // ip, port      : 本实例对外提供 RPC 服务的地址
    // ttl_sec       : TTL 秒数，心跳间隔 = ttl_sec / 3
    ServiceRegistry(std::vector<std::pair<std::string, int>> cluster_nodes,
                    std::string service_name,
                    std::string instance_id,
                    std::string ip,
                    int port,
                    int ttl_sec = 30);

    ~ServiceRegistry();

    // 注册服务（写入 Raft KV + 启动心跳线程）
    bool register_service();

    // 注销服务（删除 Raft KV + 停止心跳线程）
    bool unregister_service();

private:
    void heartbeat_loop();
    std::string make_key() const;
    std::string make_value() const;

    RaftClient  raft_client_;
    std::string service_name_;
    std::string instance_id_;
    std::string ip_;
    int         port_;
    int         ttl_sec_;

    std::atomic<bool> stopped_{true};
    std::thread heartbeat_thread_;
};

// ─────────────────────────────────────────────────────────────────────────────
//  ServiceDiscovery — 服务消费方：发现 + 负载均衡
// ─────────────────────────────────────────────────────────────────────────────
class ServiceDiscovery {
public:
    struct Endpoint {
        std::string ip;
        int port;
    };

    explicit ServiceDiscovery(
        std::vector<std::pair<std::string, int>> cluster_nodes,
        int ttl_sec = 30);

    // 返回服务 service_name 的所有存活实例（过滤 TTL 过期条目）
    std::vector<Endpoint> discover(const std::string& service_name);

    // 轮询负载均衡：返回一个实例；若无存活实例，返回 {"", 0}
    Endpoint pick_one(const std::string& service_name);

private:
    // 解析单条 "key=value" 行，成功返回 true
    static bool parse_entry(const std::string& line,
                            int ttl_sec,
                            Endpoint& out);

    RaftClient raft_client_;
    int        ttl_sec_;
    int        robin_counter_{0};
    std::mutex mu_;
};
