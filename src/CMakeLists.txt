set(XRPC_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/protocol/xrpc")
set(XRPC_GEN_DIR "${CMAKE_BINARY_DIR}/gen/protocol")

file(MAKE_DIRECTORY "${XRPC_GEN_DIR}")

add_library(xrpc_proto OBJECT "${XRPC_PROTO_ROOT}/frame_metadata.proto")

target_link_libraries(xrpc_proto PUBLIC protobuf::libprotobuf)

target_include_directories(xrpc_proto PUBLIC "${XRPC_GEN_DIR}")

xrpc_protobuf_generate(
    TARGET xrpc_proto
    LANGUAGE cpp
    IMPORT_DIRS "${XRPC_PROTO_ROOT}"
    PROTOC_OUT_DIR "${XRPC_GEN_DIR}"
)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "xrpc io_uring support is only available on Linux")
endif()

set(XRPC_SOURCES
    io/socket.cpp
    io/uring_context.cpp
    io/uring_context_operations.cpp
    io/uring_context_control.cpp
    protocol/frame_header.cpp
    protocol/frame_codec.cpp
    common/xrpc_exception.cpp
    client/tcp_transport.cpp
    client/rpc_client_impl.cpp
    client/rpc_client.cpp
    naming/service_discovery.cpp
    naming/static_discovery.cpp
    naming/consul/consul_http_client.cpp
    naming/consul/consul_discovery.cpp
    naming/consul/consul_registrar.cpp
    server/service_registry.cpp
    server/server_config.cpp
    server/rpc_frame_stream.cpp
    server/rpc_server.cpp
    server/rpc_server_impl.cpp
    server/connection_io_loop.cpp
    server/dispatch_mailbox.cpp
    server/worker_pool.cpp
    server/server_connection.cpp
)

add_library(xrpc STATIC ${XRPC_SOURCES} $<TARGET_OBJECTS:xrpc_proto>)

add_library(xrpc::xrpc ALIAS xrpc)

target_include_directories(
    xrpc
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${XRPC_GEN_DIR}>
        $<INSTALL_INTERFACE:include>
    PRIVATE ${PROJECT_SOURCE_DIR}/src/include ${PROJECT_SOURCE_DIR}/src
)

target_include_directories(xrpc PRIVATE "${XRPC_GEN_DIR}")

target_link_libraries(
    xrpc
    PUBLIC
        "$<BUILD_INTERFACE:protobuf::libprotobuf>"
        "$<INSTALL_INTERFACE:xrpc_dependencies::protobuf>"
    PRIVATE
        "$<BUILD_INTERFACE:nlohmann_json::nlohmann_json>"
        "$<INSTALL_INTERFACE:xrpc_dependencies::nlohmann_json>"
        "$<BUILD_INTERFACE:LibUring::LibUring>"
        "$<INSTALL_INTERFACE:xrpc_dependencies::liburing>"
        Threads::Threads
)

target_compile_features(xrpc PUBLIC cxx_std_20)
