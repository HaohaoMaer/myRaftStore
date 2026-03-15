#include "service_registry.h"
#include "my_log.h"

#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>

// 连接 3 节点 Raft 集群的地址列表
static const std::vector<std::pair<std::string, int>> CLUSTER = {
    {"127.0.0.1", 8000},
    {"127.0.0.1", 8001},
    {"127.0.0.1", 8002},
};

// ─── 辅助：打印测试标题 ───────────────────────────────────────────────────────
static void section(const std::string& title) {
    std::cout << "\n===== " << title << " =====\n";
}

// ─── 测试1：注册 + 发现 ────────────────────────────────────────────────────────
static void test_register_and_discover() {
    section("Test 1: register and discover");

    ServiceRegistry reg(CLUSTER, "calculator", "inst1", "127.0.0.1", 9001);
    bool ok = reg.register_service();
    assert(ok && "register_service() failed");
    std::cout << "  register_service OK\n";

    // 短暂等待 Raft 提交
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ServiceDiscovery disc(CLUSTER);
    auto endpoints = disc.discover("calculator");
    assert(!endpoints.empty() && "discover() returned empty list");
    std::cout << "  discover() found " << endpoints.size() << " instance(s):\n";
    for (const auto& ep : endpoints) {
        std::cout << "    " << ep.ip << ":" << ep.port << "\n";
    }

    reg.unregister_service();
    std::cout << "  unregister_service OK\n";
}

// ─── 测试2：多实例 + 轮询负载均衡 ──────────────────────────────────────────────
static void test_multiple_instances_round_robin() {
    section("Test 2: multiple instances + round-robin");

    ServiceRegistry reg1(CLUSTER, "calculator", "inst1", "127.0.0.1", 9001);
    ServiceRegistry reg2(CLUSTER, "calculator", "inst2", "127.0.0.1", 9002);
    ServiceRegistry reg3(CLUSTER, "calculator", "inst3", "127.0.0.1", 9003);

    assert(reg1.register_service());
    assert(reg2.register_service());
    assert(reg3.register_service());
    std::cout << "  3 instances registered\n";

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ServiceDiscovery disc(CLUSTER);

    // pick_one() 轮询：收集 6 次结果
    std::cout << "  pick_one() x6: ";
    for (int i = 0; i < 6; ++i) {
        auto ep = disc.pick_one("calculator");
        assert(ep.port > 0 && "pick_one() returned empty");
        std::cout << ep.ip << ":" << ep.port << "  ";
    }
    std::cout << "\n";

    reg1.unregister_service();
    reg2.unregister_service();
    reg3.unregister_service();
    std::cout << "  all instances unregistered\n";
}

// ─── 测试3：注销后不再出现在发现结果中 ─────────────────────────────────────────
static void test_unregister_removes_from_discovery() {
    section("Test 3: unregister removes instance");

    ServiceRegistry reg(CLUSTER, "storage", "s1", "127.0.0.1", 9100);
    assert(reg.register_service());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ServiceDiscovery disc(CLUSTER);
    auto before = disc.discover("storage");
    assert(!before.empty() && "should have 1 instance before unregister");
    std::cout << "  before unregister: " << before.size() << " instance(s)\n";

    reg.unregister_service();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto after = disc.discover("storage");
    assert(after.empty() && "should have 0 instances after unregister");
    std::cout << "  after  unregister: " << after.size() << " instance(s) [expected 0]\n";
}

// ─── 测试4：TTL 过期过滤（手动模拟：写入一个超期时间戳） ──────────────────────
static void test_ttl_expired_filtered() {
    section("Test 4: TTL-expired entry is filtered out");

    // 直接写一条 timestamp=1（1970 年，必然超期）进 Raft KV
    RaftClient rc(CLUSTER);
    bool ok = rc.put("/svc/ghost/inst1", "127.0.0.1:9200:1");
    assert(ok && "PUT expired entry failed");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // ttl_sec=30，timestamp=1 距今 >> 30s，应被过滤
    ServiceDiscovery disc(CLUSTER, /*ttl_sec=*/30);
    auto eps = disc.discover("ghost");
    assert(eps.empty() && "expired entry should be filtered out");
    std::cout << "  expired entry correctly filtered (0 results)\n";

    // 清理
    rc.del("/svc/ghost/inst1");
}

// ─── 测试5：GET 读取注册信息（验证值格式正确写入）────────────────────────────
static void test_raw_get() {
    section("Test 5: raw GET value format");

    ServiceRegistry reg(CLUSTER, "echo", "e1", "10.0.0.1", 7777);
    assert(reg.register_service());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    RaftClient rc(CLUSTER);
    std::string val = rc.get("/svc/echo/e1");
    std::cout << "  GET /svc/echo/e1 = \"" << val << "\"\n";

    // value 应含两个 ':'（ip:port:timestamp）
    int colon_count = 0;
    for (char c : val) if (c == ':') ++colon_count;
    assert(colon_count == 2 && "value format should be ip:port:timestamp");
    std::cout << "  value format OK (ip:port:timestamp)\n";

    reg.unregister_service();
}

int main() {
    myLog::get_instance()->init("./SvcDiscLog", 0, 2000, 800000, 800);

    test_register_and_discover();
    test_multiple_instances_round_robin();
    test_unregister_removes_from_discovery();
    test_ttl_expired_filtered();
    test_raw_get();

    std::cout << "\nAll service discovery tests passed.\n";
    return 0;
}
