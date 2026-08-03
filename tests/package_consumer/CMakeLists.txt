cmake_minimum_required(VERSION 3.20)

project(xrpc_package_consumer LANGUAGES CXX)

find_package(xrpc CONFIG REQUIRED)

add_executable(xrpc_package_consumer main.cpp)
target_link_libraries(xrpc_package_consumer PRIVATE xrpc::xrpc)
