#include "service_registry.h"
#include "my_log.h"

#include <ctime>
#include <sstream>

// ─────────────────────────────────────────────────────────────────────────────
//  ServiceRegistry
// ─────────────────────────────────────────────────────────────────────────────

ServiceRegistry::ServiceRegistry(
    std::vector<std::pair<std::string, int>> cluster_nodes,
    std::string service_name,
    std::string instance_id,
    std::string ip,
    int port,
    int ttl_sec)
    : raft_client_(std::move(cluster_nodes)),
      service_name_(std::move(service_name)),
      instance_id_(std::move(instance_id)),
      ip_(std::move(ip)),
      port_(port),
      ttl_sec_(ttl_sec)
{}

ServiceRegistry::~ServiceRegistry() {
    if (!stopped_) {
        unregister_service();
    }
}

bool ServiceRegistry::register_service() {
    if (!raft_client_.put(make_key(), make_value())) {
        LOG_ERROR("[ServiceRegistry] register failed for " + make_key());
        return false;
    }
    LOG_INFO("[ServiceRegistry] registered: " + make_key() + " = " + make_value());

    stopped_ = false;
    heartbeat_thread_ = std::thread(&ServiceRegistry::heartbeat_loop, this);
    return true;
}

bool ServiceRegistry::unregister_service() {
    stopped_ = true;
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }

    bool ok = raft_client_.del(make_key());
    if (ok) {
        LOG_INFO("[ServiceRegistry] unregistered: " + make_key());
    } else {
        LOG_ERROR("[ServiceRegistry] unregister failed for " + make_key());
    }
    return ok;
}

void ServiceRegistry::heartbeat_loop() {
    // 心跳间隔 = ttl / 3，确保在 TTL 到期前刷新时间戳
    int interval_sec = std::max(1, ttl_sec_ / 3);
    while (!stopped_) {
        std::this_thread::sleep_for(std::chrono::seconds(interval_sec));
        if (stopped_) break;
        if (!raft_client_.put(make_key(), make_value())) {
            LOG_ERROR("[ServiceRegistry] heartbeat failed for " + make_key());
        } else {
            LOG_INFO("[ServiceRegistry] heartbeat: " + make_key());
        }
    }
}

std::string ServiceRegistry::make_key() const {
    return "/svc/" + service_name_ + "/" + instance_id_;
}

std::string ServiceRegistry::make_value() const {
    long long ts = static_cast<long long>(std::time(nullptr));
    return ip_ + ":" + std::to_string(port_) + ":" + std::to_string(ts);
}

// ─────────────────────────────────────────────────────────────────────────────
//  ServiceDiscovery
// ─────────────────────────────────────────────────────────────────────────────

ServiceDiscovery::ServiceDiscovery(
    std::vector<std::pair<std::string, int>> cluster_nodes,
    int ttl_sec)
    : raft_client_(std::move(cluster_nodes)),
      ttl_sec_(ttl_sec)
{}

std::vector<ServiceDiscovery::Endpoint>
ServiceDiscovery::discover(const std::string& service_name) {
    std::string prefix = "/svc/" + service_name + "/";
    std::string raw = raft_client_.list(prefix);

    std::vector<Endpoint> result;
    std::istringstream ss(raw);
    std::string line;
    while (std::getline(ss, line)) {
        if (line.empty()) continue;
        Endpoint ep;
        if (parse_entry(line, ttl_sec_, ep)) {
            result.push_back(ep);
        }
    }
    return result;
}

ServiceDiscovery::Endpoint
ServiceDiscovery::pick_one(const std::string& service_name) {
    auto endpoints = discover(service_name);
    if (endpoints.empty()) {
        return {"", 0};
    }
    std::lock_guard<std::mutex> lk(mu_);
    int idx = robin_counter_++ % static_cast<int>(endpoints.size());
    return endpoints[idx];
}

// 解析一行 "key=value"，value 格式为 "ip:port:timestamp"
// 若 timestamp 距今超过 ttl_sec，视为过期，返回 false
bool ServiceDiscovery::parse_entry(const std::string& line,
                                   int ttl_sec,
                                   Endpoint& out) {
    // 找 '='，拆出 value
    auto eq_pos = line.find('=');
    if (eq_pos == std::string::npos) return false;
    std::string value = line.substr(eq_pos + 1);

    // value = "ip:port:timestamp"
    // 最后一个 ':' 后是 timestamp
    auto last_colon = value.rfind(':');
    if (last_colon == std::string::npos) return false;
    std::string ts_str = value.substr(last_colon + 1);

    // 中间段 = "ip:port"
    std::string ip_port = value.substr(0, last_colon);
    auto colon = ip_port.rfind(':');
    if (colon == std::string::npos) return false;
    std::string ip   = ip_port.substr(0, colon);
    std::string port_str = ip_port.substr(colon + 1);

    if (ip.empty() || port_str.empty() || ts_str.empty()) return false;

    long long ts = std::stoll(ts_str);
    long long now = static_cast<long long>(std::time(nullptr));
    if (now - ts > ttl_sec) {
        // 超过 TTL，视为失联
        return false;
    }

    out.ip   = ip;
    out.port = std::stoi(port_str);
    return true;
}
