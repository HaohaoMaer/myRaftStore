// status_http_server.cpp
#include "httplib.h"
#include "metrics_manager.h"
#include <iostream>

void start_http_status_server(int port = 8080) {
    httplib::Server svr;

    svr.Get("/status", [](const httplib::Request&, httplib::Response& res) {
        std::ostringstream oss;
        oss << "{\n"
            << "  \"rpc_call_count\": " << MetricsManager::instance().get_rpc_calls() << ",\n"
            << "  \"concurrent_rpcs\": " << MetricsManager::instance().get_concurrent() << "\n"
            << "}\n";
        res.set_content(oss.str(), "application/json");
    });

    std::cout << "HTTP status server started at http://localhost:" << port << "/status\n";
    svr.listen("0.0.0.0", port);
}
