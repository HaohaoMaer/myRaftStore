cmake_minimum_required(VERSION 3.10)

# Project name
project(mybRPC)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Debug)

# Add source files
file(GLOB_RECURSE SOURCES ${CMAKE_SOURCE_DIR} src/*.cpp src/*.cc)

# 单独指定 main.cpp
set(MAIN_SRC ${CMAKE_SOURCE_DIR}/src/main.cpp)

# 去除 main.cpp，作为 pressure_test 的源文件集合
set(PRESSURE_SOURCES ${SOURCES})
list(REMOVE_ITEM PRESSURE_SOURCES ${MAIN_SRC})

# 去除 main.cpp，作为 raft_node_main 的源文件集合
set(RAFTNODE_SOURCES ${SOURCES})
list(REMOVE_ITEM RAFTNODE_SOURCES ${MAIN_SRC})

# 去除 main.cpp，作为 pressure_test 的源文件集合
set(RAFTTEST_SOURCES ${SOURCES})
list(REMOVE_ITEM RAFTTEST_SOURCES ${MAIN_SRC})

# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)
message(STATUS "Include directories: ${PROJECT_SOURCE_DIR}/include")

# Create the executable
add_executable(main ${SOURCES})

# Link libraries (if any)
target_link_libraries(main ${Protobuf_LIBRARIES} z)


add_executable(pressure_test test/pressure_test.cpp ${PRESSURE_SOURCES}) # 如果你生成的 proto 文件在这里
target_link_libraries(pressure_test ${Protobuf_LIBRARIES} pthread z)

add_executable(raft_node_main test/raft_node_main.cpp ${RAFTNODE_SOURCES}) # 如果你生成的 proto 文件在这里
target_link_libraries(raft_node_main ${Protobuf_LIBRARIES} pthread z)

add_executable(raft_test test/raft_test.cpp ${RAFTTEST_SOURCES}) # 如果你生成的 proto 文件在这里
target_link_libraries(raft_test ${Protobuf_LIBRARIES} pthread z)

set(SVCDISCO_SOURCES ${SOURCES})
list(REMOVE_ITEM SVCDISCO_SOURCES ${MAIN_SRC})

add_executable(service_discovery_test test/service_discovery_test.cpp ${SVCDISCO_SOURCES})
target_link_libraries(service_discovery_test ${Protobuf_LIBRARIES} pthread z)

set(RAFTBENCH_SOURCES ${SOURCES})
list(REMOVE_ITEM RAFTBENCH_SOURCES ${MAIN_SRC})

add_executable(raft_bench test/raft_bench.cpp ${RAFTBENCH_SOURCES})
target_link_libraries(raft_bench ${Protobuf_LIBRARIES} pthread z)