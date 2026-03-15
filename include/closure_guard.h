#ifndef MYBRPC_CLOSURE_GUARD_H
#define MYBRPC_CLOSURE_GUARD_H
#include <google/protobuf/service.h>

namespace myrpc {

// RAII: Call Run() of the closure on destruction.
class ClosureGuard {
public:
    ClosureGuard() : _done(NULL) {}

    // Constructed with a closure which will be Run() inside dtor.
    explicit ClosureGuard(google::protobuf::Closure* done) : _done(done) {}
    
    // Run internal closure if it's not NULL.
    ~ClosureGuard() {
        if (_done) {
            _done->Run();
        }
    }

    // Run internal closure if it's not NULL and set it to `done'.
    void reset(google::protobuf::Closure* done) {
        if (_done) {
            _done->Run();
        }
        _done = done;
    }

    // Return and set internal closure to NULL.
    google::protobuf::Closure* release() {
        google::protobuf::Closure* const prev_done = _done;
        _done = NULL;
        return prev_done;
    }

    // True if no closure inside.
    bool empty() const { return _done == NULL; }

    // Exchange closure with another guard.
    void swap(ClosureGuard& other) { std::swap(_done, other._done); }
    
private:
    google::protobuf::Closure* _done;
};

} // namespace brpc
#endif 