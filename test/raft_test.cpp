#include "client.h"
int main() {
    Client client;
    std::string log_path = "./ClientLog";
    myLog::get_instance()->init(log_path.c_str(), 0, 2000, 800000, 800);  // 初始化日志

    std::vector<std::pair<std::string, int>> node_list = {
        {"127.0.0.1", 8000},
        {"127.0.0.1", 8001},
        {"127.0.0.1", 8002}
    };

    std::string leader_ip = "127.0.0.1";
    int leader_port = 8000;

    for (int i = 0; i < 26; i++) {
        std::string key = std::string(1, 'a' + i);
        std::string value = std::to_string(i);
        std::string command = "PUT " + key + " " + value;

        bool success = false;

        // 优先使用缓存 leader
        std::vector<std::pair<std::string, int>> try_order = {{leader_ip, leader_port}};
        for (const auto& node : node_list) {
            if (node.first != leader_ip || node.second != leader_port) {
                try_order.push_back(node);  // 其余节点追加
            }
        }

        for (const auto& [ip, port] : try_order) {
            std::string resp_bin = client.send_request(ip, port, command);
            raft::ClientResponse resp;
            if (!resp.ParseFromString(resp_bin)) {
                continue;
            }

            if (resp.success()) {
                leader_ip = ip;
                leader_port = port;
                success = true;
                break;
            } else {
                // 更新 Leader 尝试真正 Leader（下一次优先尝试）
                leader_ip = resp.addr();
                leader_port = resp.port();
            }
        }

        if (!success) {
            std::cout << "Failed to send command: " << command << std::endl;
        } else {
            std::cout << "Command sent successfully: " << command << std::endl;
        }
    }

    return 0;
}
