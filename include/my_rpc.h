#pragma once

#include <string>
#include <queue>
#include <functional>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <time.h>
#include <sys/time.h>
#include <stdarg.h>
#include "rpc.pb.h"
#include "my_log.h"
#include "thread_pool.h"  // 引入线程池
#include "metrics_manager.h"
#include "connection_pool.h"
//#include <bthread/bthread.h>  // 引入 bthread 库

// =========================
// 支持多服务、多方法的极简 RPC 框架（模仿 bRPC 架构）
// =========================
namespace myrpc {

class RpcServer {
public:
    struct MethodProperty{
        google::protobuf::Service* service;
        const google::protobuf::MethodDescriptor* method;
    };

    void init(int close_log, int log_write, int thread_num);

    void log_write();

    RpcServer(int port) : port_(port) {}
    ~RpcServer();

    // 注册一个服务（service_name）和它的多个方法（method -> handler）
    void register_service(google::protobuf::Service* service);

    // 启动服务器，使用 epoll 管理连接
    void init_thread_pool();
    void start();

private:
    void handle_client(int fd);

    int port_;
    int m_close_log;
    int m_log_write;
    ThreadPool *m_threadpool; // 线程池对象
    int m_thread_num;
    std::atomic<int> active_connections_{0};
    int epfd_{-1};       // epoll fd，提升为成员供 handle_client() 重新注册 fd
    int server_fd_{-1};  // listening socket，提升为成员供析构关闭
    std::unordered_map<std::string, MethodProperty> handlers_; // key: Service.Method
};

class RpcClient {
public:
    RpcClient(int num): thread_num_(num) {
    }

    void init();
    
    // 发送请求："Service.Method|参数"
    std::string call(const std::string& ip, int port, const std::string& full_method_name, const std::string& args);
    void call_async(const std::string& ip, int port,
        const std::string& full_method_name,
        const std::string& args,
        std::function<void(const std::string&)> callback);


private:
    ThreadPool *m_threadpool; // 线程池对象
    int thread_num_;
    std::string ip_;
    int port_;
};
}

