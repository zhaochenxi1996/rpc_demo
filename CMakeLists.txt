cmake_minimum_required(VERSION 3.10)
project(rpc_framework)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(echo_server
    src/server/echo_server.cpp
    src/server/event_loop.cpp
    src/server/tcp_connection.cpp
    src/server/thread_pool.cpp
)

target_compile_options(echo_server PRIVATE -Wall -Wextra -g -O2)