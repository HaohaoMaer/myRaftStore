// raft_bench.cpp — Raft KV 集群 QPS 压测
//
// 测量三类业务：
//   write  - 纯写（PUT），走完整 Raft 共识路径（追加日志 → 多数派确认 → 提交）
//   read   - 纯读（GET），Leader 本地直读，不进共识
//   mixed  - 混合，默认 80% GET / 20% PUT
//
// 用法:
//   ./raft_bench [mode] [threads] [duration_sec] [read_ratio_pct]
//     mode          : write | read | mixed  (默认 write)
//     threads       : 并发线程数              (默认 8)
//     duration_sec  : 持续时间（秒）          (默认 30)
//     read_ratio_pct: mixed 模式读比例 0-100  (默认 80)
//
// 前提: 已启动 3 节点 Raft 集群
//   ./raft_node_main 0 127.0.0.1 8000 ...
//   ./raft_node_main 1 127.0.0.1 8001 ...
//   ./raft_node_main 2 127.0.0.1 8002 ...

#include "raft_client.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

// ─── 统计量 ──────────────────────────────────────────────────────────────────

struct Stats {
    std::atomic<int64_t> ok{0};
    std::atomic<int64_t> fail{0};
};

// 每线程延迟样本（微秒），最多采集 kMaxSamples 条避免内存压力
static constexpr int kMaxSamples = 200'000;

struct ThreadResult {
    int64_t ok{0};
    int64_t fail{0};
    std::vector<int64_t> latencies_us;  // per-op 延迟
};

// ─── 压测工作线程 ────────────────────────────────────────────────────────────

// cluster 节点列表
static const std::vector<std::pair<std::string, int>> kNodes = {
    {"127.0.0.1", 8000},
    {"127.0.0.1", 8001},
    {"127.0.0.1", 8002},
};

// mode: 0=write 1=read 2=mixed
void bench_worker(int thread_id, int mode, int duration_sec,
                  int read_ratio_pct, ThreadResult& result) {
    RaftClient client(kNodes);

    result.latencies_us.reserve(std::min(kMaxSamples, duration_sec * 5000));

    // 读模式：在已预填充的 key 池里随机选
    static constexpr int kKeyPoolSize = 1000;
    std::mt19937 rng(thread_id * 1234567891ULL);
    std::uniform_int_distribution<int> key_dist(0, kKeyPoolSize - 1);
    std::uniform_int_distribution<int> op_dist(0, 99);

    int64_t seq = 0;
    const auto end_time =
        std::chrono::steady_clock::now() + std::chrono::seconds(duration_sec);

    while (std::chrono::steady_clock::now() < end_time) {
        bool do_write;
        if (mode == 0)      do_write = true;
        else if (mode == 1) do_write = false;
        else                do_write = (op_dist(rng) >= read_ratio_pct);

        auto t0 = std::chrono::steady_clock::now();
        bool ok;

        if (do_write) {
            // 写路径：key = "t<tid>_<seq>"，value = seq 的十进制
            std::string key = "t" + std::to_string(thread_id)
                              + "_" + std::to_string(seq);
            ok = client.put(key, std::to_string(seq));
        } else {
            // 读路径：从预填充的共享 key 池里读
            std::string key = "bench_" + std::to_string(key_dist(rng));
            std::string val = client.get(key);
            ok = !val.empty();
        }

        auto t1 = std::chrono::steady_clock::now();
        int64_t us =
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

        if (ok) {
            result.ok++;
            if ((int)result.latencies_us.size() < kMaxSamples)
                result.latencies_us.push_back(us);
        } else {
            result.fail++;
        }
        seq++;
    }
}

// ─── 延迟百分位计算 ───────────────────────────────────────────────────────────

// 将所有线程的样本合并后计算百分位（原地修改，不再需要原始顺序）
void print_latency_stats(std::vector<int64_t>& all_us) {
    if (all_us.empty()) { std::cout << "  (no samples)\n"; return; }
    std::sort(all_us.begin(), all_us.end());
    auto pct = [&](double p) -> int64_t {
        size_t idx = static_cast<size_t>(p / 100.0 * all_us.size());
        if (idx >= all_us.size()) idx = all_us.size() - 1;
        return all_us[idx];
    };
    double avg = static_cast<double>(
        std::accumulate(all_us.begin(), all_us.end(), int64_t{0})) /
        all_us.size();
    std::cout << "  Samples : " << all_us.size() << "\n"
              << "  Avg     : " << static_cast<int64_t>(avg) << " us\n"
              << "  p50     : " << pct(50)  << " us\n"
              << "  p95     : " << pct(95)  << " us\n"
              << "  p99     : " << pct(99)  << " us\n"
              << "  p99.9   : " << pct(99.9) << " us\n"
              << "  Max     : " << all_us.back() << " us\n";
}

// ─── 预填充读 key 池 ──────────────────────────────────────────────────────────

bool pre_populate(int key_count) {
    RaftClient client(kNodes);
    std::cout << "[bench] Pre-populating " << key_count
              << " keys for read workload...\n";
    int ok = 0;
    for (int i = 0; i < key_count; ++i) {
        if (client.put("bench_" + std::to_string(i), std::to_string(i * 10)))
            ok++;
    }
    std::cout << "[bench] Pre-populate done: " << ok << "/" << key_count
              << " succeeded.\n";
    return ok >= key_count / 2;  // 至少一半成功才继续
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    myLog::get_instance()->init("./RaftBenchLog", 0, 2000, 800000, 800);

    // 解析参数
    std::string mode_str    = (argc > 1) ? argv[1] : "write";
    int threads             = (argc > 2) ? std::stoi(argv[2]) : 8;
    int duration_sec        = (argc > 3) ? std::stoi(argv[3]) : 30;
    int read_ratio_pct      = (argc > 4) ? std::stoi(argv[4]) : 80;

    int mode;
    if      (mode_str == "read")  mode = 1;
    else if (mode_str == "mixed") mode = 2;
    else { mode_str = "write";    mode = 0; }

    std::cout << "========== Raft KV Benchmark ==========\n"
              << "  Mode     : " << mode_str << "\n"
              << "  Threads  : " << threads << "\n"
              << "  Duration : " << duration_sec << " s\n";
    if (mode == 2)
        std::cout << "  Read%    : " << read_ratio_pct << "%\n";
    std::cout << "  Cluster  : 127.0.0.1:8000/8001/8002\n"
              << "=======================================\n";

    // read/mixed 需要预填充 key
    if (mode == 1 || mode == 2) {
        if (!pre_populate(1000)) {
            std::cerr << "[bench] Pre-populate failed, aborting.\n";
            return 1;
        }
    }

    // 启动工作线程
    std::vector<ThreadResult> results(threads);
    std::vector<std::thread> thread_pool;
    thread_pool.reserve(threads);

    auto wall_start = std::chrono::steady_clock::now();
    for (int i = 0; i < threads; ++i) {
        thread_pool.emplace_back(bench_worker, i, mode, duration_sec,
                                 read_ratio_pct, std::ref(results[i]));
    }
    for (auto& t : thread_pool) t.join();
    auto wall_end = std::chrono::steady_clock::now();

    double elapsed_sec =
        std::chrono::duration<double>(wall_end - wall_start).count();

    // 汇总
    int64_t total_ok = 0, total_fail = 0;
    std::vector<int64_t> all_us;
    for (auto& r : results) {
        total_ok   += r.ok;
        total_fail += r.fail;
        all_us.insert(all_us.end(),
                      r.latencies_us.begin(), r.latencies_us.end());
    }

    double qps = total_ok / elapsed_sec;

    std::cout << "\n========== Results ===================\n"
              << "  Elapsed  : " << static_cast<int64_t>(elapsed_sec * 1000)
              << " ms\n"
              << "  OK       : " << total_ok   << "\n"
              << "  Fail     : " << total_fail << "\n"
              << "  QPS      : " << static_cast<int64_t>(qps) << " ops/s\n"
              << "---------- Latency -------------------\n";
    print_latency_stats(all_us);
    std::cout << "======================================\n";

    std::cout.flush();
    _exit(0);
}
