set(XRPC_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/protocol/xrpc")
set(XRPC_GEN_DIR "${CMAKE_BINARY_DIR}/gen/protocol")

file(MAKE_DIRECTORY "${XRPC_GEN_DIR}")

add_library(xrpc_proto OBJECT "${XRPC_PROTO_ROOT}/xrpc_header.proto")

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
    io/uring_awaitable.cpp
    io/uring_context.cpp
    protocol/fixed_header.cpp
    protocol/frame_codec.cpp
    rpc/xrpc_exception.cpp
    rpc/protocol_adapter.cpp
    rpc/client/client_config.cpp
    rpc/client/endpoint_selector.cpp
    rpc/client/endpoint_state_table.cpp
    rpc/client/client_channel.cpp
    rpc/client/tcp_transport.cpp
    rpc/client/rpc_client_runtime.cpp
    rpc/client/rpc_client.cpp
    rpc/naming/endpoint_resolver.cpp
    rpc/naming/consul_http_client.cpp
    rpc/naming/consul_agent_client.cpp
    rpc/naming/consul_resolver.cpp
    rpc/naming/consul_registrar.cpp
    rpc/server/service_registry.cpp
    rpc/server/server_config.cpp
    rpc/server/rpc_server_controller.cpp
    rpc/server/rpc_session.cpp
    rpc/server/rpc_server.cpp
    transport/byte_buffer.cpp
    transport/connection_io_loop.cpp
    transport/dispatch_completion_queue.cpp
    transport/thread_pool_executor.cpp
    transport/tcp_connection.cpp
    transport/tcp_server.cpp
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
