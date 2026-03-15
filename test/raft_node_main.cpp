#include "raft_node.h"
#include <sstream>

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <ip> <port> [peer_ip:peer_port]..." << std::endl;
        return 1;
    }

    int node_id = std::stoi(argv[1]);
    std::string ip = argv[2];
    int port = std::stoi(argv[3]);

    std::vector<PeerInfo> peers;
    for (int i = 4; i < argc; ++i) {
        std::string peer_arg = argv[i];
        size_t colon_pos = peer_arg.find(':');
        if (colon_pos == std::string::npos) {
            std::cerr << "Invalid peer format: " << peer_arg << std::endl;
            continue;
        }
        PeerInfo peer;
        peer.id = std::stoi(peer_arg.substr(0, colon_pos));
        std::string peer_addr= peer_arg.substr(colon_pos + 1);
        size_t colon_pos_ = peer_addr.find(':');
        if (colon_pos_ == std::string::npos) {
            std::cerr << "Invalid peer format: " << peer_arg << std::endl;
            continue;
        }
        peer.ip = peer_addr.substr(0, colon_pos_);
        peer.port = std::stoi(peer_addr.substr(colon_pos_ + 1));
        peers.emplace_back(peer);
    }

    RaftNode node(node_id, ip, port, peers);
    std::cout << "Starting Raft node..." << std::endl;
    node.start();

    std::cout << "Raft node " << node_id << " running at " << ip << ":" << port << std::endl;

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    return 0;
}
