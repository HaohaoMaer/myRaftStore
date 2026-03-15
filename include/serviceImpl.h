#include "rpc.pb.h"
#include "closure_guard.h"

class calculator : public testrpc::CalculatorService {
 public:

  void Add(::google::protobuf::RpcController* controller,
            const ::testrpc::AddRequest* request,
            ::testrpc::AddResponse* response,
            ::google::protobuf::Closure* done) override {
            myrpc::ClosureGuard guard(done);
            int a = request->a();
            int b = request->b();
            int sum = a + b;
            response->set_sum(sum);
           }

  void Sub(::google::protobuf::RpcController* controller,
                const ::testrpc::SubRequest* request,
                ::testrpc::SubResponse* response,
                ::google::protobuf::Closure* done) override {
                myrpc::ClosureGuard guard(done);
                int a = request->a();
                int b = request->b();
                int diff = a - b;
                response->set_diff(diff);
    }
};