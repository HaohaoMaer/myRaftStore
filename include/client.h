#include "my_rpc.h"
#include "raft_service_impl.h"

class Client{

public:
    std::string send_request(std::string leader_ip, int leader_port, const std::string& cmd)
    {
        myrpc::RpcClient rpc_client(5);
        raft::ClientRequest req;
        req.set_command(cmd);
        std::string req_bin;
        req.SerializeToString(&req_bin);

        std::cout << "Sending request to leader: " << leader_ip << ":" << leader_port << std::endl;
        std::string resp_bin = rpc_client.call(leader_ip, leader_port, "raft.RaftService.ClientStorage", req_bin);
        return resp_bin;
    }
};