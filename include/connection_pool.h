#pragma once

#include <string>
#include <queue>
#include <mutex>
#include <unordered_map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

namespace myrpc {

class ConnectionPool {
public:
    // Meyer's Singleton — 线程安全（C++11）
    static ConnectionPool& instance() {
        static ConnectionPool inst;
        return inst;
    }

    // 从池中借出一个到 (ip:port) 的连接 fd
    // 优先复用池中空闲连接（含健康检测），池空则新建，池满则返回 -1
    int acquire(const std::string& ip, int port);

    // 将成功用完的 fd 归还到池中（调用方保证 fd 处于阻塞模式）
    void release(const std::string& ip, int port, int fd);

    // 废弃一个出错的连接（直接 close，归还 total 名额）
    void discard(const std::string& ip, int port, int fd);

    // 修改单端点最大连接数（默认 10）
    void set_max_per_endpoint(int n);

private:
    ConnectionPool()  = default;
    ~ConnectionPool();
    ConnectionPool(const ConnectionPool&)            = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    // 建立一条新的 TCP 连接，失败返回 -1
    int  make_connection(const std::string& ip, int port);

    // 惰性健康检测：MSG_PEEK + MSG_DONTWAIT
    bool is_alive(int fd);

    static std::string make_key(const std::string& ip, int port) {
        return ip + ":" + std::to_string(port);
    }

    struct EndpointPool {
        std::queue<int> idle_fds;
        int total{0};   // 已分配连接总数（含正在使用中的）
    };

    std::mutex mtx_;
    std::unordered_map<std::string, EndpointPool> pools_;
    int max_per_endpoint_{10};
};

} // namespace myrpc
