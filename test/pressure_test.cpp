#include "my_rpc.h"
#include "serviceImpl.h"

std::atomic<int> success_count(0);
std::atomic<int> fail_count(0);

void pressure_worker(int id, int duration_sec) {
    myrpc::RpcClient client(1);  // 线程数仅用于 call_async，同步 call() 不需要
    auto end_time = std::chrono::steady_clock::now() + std::chrono::seconds(duration_sec);

    while (std::chrono::steady_clock::now() < end_time) {
        testrpc::AddRequest req;
        req.set_a(id);
        req.set_b(1);
        std::string req_bin;
        req.SerializeToString(&req_bin);

        try {
            std::string resp_bin = client.call("127.0.0.1", 12345, "testrpc.CalculatorService.Add", req_bin);
            testrpc::AddResponse resp;
            resp.ParseFromString(resp_bin);
            success_count++;
        } catch (...) {
            fail_count++;
        }
    }
}

int main() {
    myLog::get_instance()->init("./PressureLog", 0, 2000, 800000, 800);

    // 启动 RPC 服务器并注册服务方法
    myrpc::RpcServer server(12345);

    server.init(0, 1, 5); // 初始化服务器
    ThreadLocalBuffers::local_buffer.clear();
    server.log_write(); // 写入日志
    calculator calculateservice;
    server.register_service(&calculateservice);
    // 后台启动服务器
    server.init_thread_pool(); // 初始化线程池
    
    std::thread([&]() {
        server.start();
    }).detach();

    const int thread_num = 512;
    const int duration_sec = 10;
    std::vector<std::thread> threads;

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < thread_num; ++i) {
        threads.emplace_back(pressure_worker, i, duration_sec);
    }
    for (auto& t : threads) t.join();
    auto end = std::chrono::steady_clock::now();

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Total Time: " << elapsed_ms << " ms\n";
    std::cout << "Success: " << success_count << "\n";
    std::cout << "Fail: " << fail_count << "\n";
    std::cout << "QPS: " << (success_count.load() * 1000.0 / elapsed_ms) << "\n";

    // 用 _exit 跳过析构：server 的 detached 线程仍在运行，
    // 若走普通 return，~RpcServer() 会销毁 thread pool 的 mutex，
    // 而 server 线程仍在调用 add_task() → EINVAL。
    std::cout.flush();
    _exit(0);
}
