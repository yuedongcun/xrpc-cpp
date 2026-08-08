set(XRPC_TEST_RUNTIME_DIR "${CMAKE_BINARY_DIR}/tests")
set(XRPC_TEST_GEN_DIR "${CMAKE_BINARY_DIR}/gen/tests")
file(MAKE_DIRECTORY "${XRPC_TEST_RUNTIME_DIR}" "${XRPC_TEST_GEN_DIR}")

add_library(xrpc_test_proto OBJECT proto/echo.proto)
target_link_libraries(xrpc_test_proto PUBLIC protobuf::libprotobuf)
target_include_directories(xrpc_test_proto PUBLIC "${XRPC_TEST_GEN_DIR}")

add_library(xrpc_test_support STATIC test_support/runtime_util.cpp)
target_include_directories(
    xrpc_test_support
    PUBLIC
        "${PROJECT_SOURCE_DIR}/src/include"
        "${PROJECT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}"
)
target_link_libraries(xrpc_test_support PUBLIC xrpc::xrpc Threads::Threads)

xrpc_protobuf_generate(
    TARGET xrpc_test_proto
    LANGUAGE cpp
    IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTOC_OUT_DIR "${XRPC_TEST_GEN_DIR}"
)

set(XRPC_TEST_UNIT_TIMEOUT 5)
set(XRPC_TEST_INTEGRATION_TIMEOUT 15)
set(XRPC_TEST_EXTERNAL_TIMEOUT 60)

function(xrpc_add_test test_name test_source)
    set(one_value_args TIMEOUT)
    set(multi_value_args LABELS)
    cmake_parse_arguments(
        XRPC_TEST
        ""
        "${one_value_args}"
        "${multi_value_args}"
        ${ARGN}
    )

    add_executable(${test_name} ${test_source})
    set_target_properties(
        ${test_name}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${XRPC_TEST_RUNTIME_DIR}"
    )
    target_include_directories(
        ${test_name}
        PRIVATE
            "${PROJECT_SOURCE_DIR}/src/include"
            "${PROJECT_SOURCE_DIR}/src"
            "${CMAKE_CURRENT_SOURCE_DIR}"
    )
    target_link_libraries(
        ${test_name}
        PRIVATE
            xrpc::xrpc
            xrpc_test_proto
            xrpc_test_support
            GTest::gmock_main
            Threads::Threads
    )
    add_test(NAME ${test_name} COMMAND ${test_name})
    set_tests_properties(
        ${test_name}
        PROPERTIES
            LABELS "${XRPC_TEST_LABELS}"
            TIMEOUT "${XRPC_TEST_TIMEOUT}"
    )
endfunction()

xrpc_add_test(
    io_socket_test io/io_socket_test.cpp
    LABELS unit io
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    common_task_test common/task_test.cpp
    LABELS unit runtime
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    common_status_or_test common/status_or_test.cpp
    LABELS unit common error
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    transport_byte_buffer_test transport/transport_byte_buffer_test.cpp
    LABELS unit transport
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    thread_pool_executor_test transport/thread_pool_executor_test.cpp
    LABELS unit runtime transport
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    protocol_test protocol/protocol_test.cpp
    LABELS unit protocol
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    xrpc_exception_test rpc/xrpc_exception_test.cpp
    LABELS unit error
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    service_registry_test rpc/server/service_registry_test.cpp
    LABELS unit rpc server
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    rpc_session_test rpc/server/rpc_session_test.cpp
    LABELS unit rpc protocol
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    protocol_adapter_test rpc/protocol_adapter_test.cpp
    LABELS unit rpc protocol
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    raw_call_result_test rpc/raw_call_result_test.cpp
    LABELS unit rpc error
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    tcp_transport_test rpc/client/tcp_transport_test.cpp
    LABELS unit rpc client transport
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    endpoint_resolver_test rpc/naming/endpoint_resolver_test.cpp
    LABELS unit rpc naming
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    endpoint_selector_test rpc/client/endpoint_selector_test.cpp
    LABELS unit rpc client
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    endpoint_state_table_test rpc/client/endpoint_state_table_test.cpp
    LABELS unit rpc client
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    client_channel_test rpc/client/client_channel_test.cpp
    LABELS unit rpc client
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    consul_resolver_test rpc/naming/consul_resolver_test.cpp
    LABELS unit rpc naming consul
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)
xrpc_add_test(
    consul_registrar_test rpc/naming/consul_registrar_test.cpp
    LABELS unit rpc naming consul
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    consul_resolver_integration_test rpc/naming/consul_resolver_integration_test.cpp
    LABELS external integration e2e rpc naming consul
    TIMEOUT ${XRPC_TEST_EXTERNAL_TIMEOUT}
)
xrpc_add_test(
    rpc_client_endpoint_test rpc/client/rpc_client_endpoint_test.cpp
    LABELS integration e2e rpc client
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)
xrpc_add_test(
    rpc_client_thread_safety_test rpc/client/rpc_client_thread_safety_test.cpp
    LABELS integration e2e rpc client concurrency
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)
xrpc_add_test(
    rpc_server_lifecycle_test rpc/server/rpc_server_lifecycle_test.cpp
    LABELS integration rpc server lifecycle
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)
xrpc_add_test(
    io_uring_awaitable_test io/uring_awaitable_test.cpp
    LABELS runtime io_uring
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)
xrpc_add_test(
    io_uring_context_post_test io/uring_context_post_test.cpp
    LABELS runtime io_uring concurrency
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)
xrpc_add_test(
    tcp_connection_io_uring_test transport/tcp_connection_io_uring_test.cpp
    LABELS integration runtime transport io_uring
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)
xrpc_add_test(
    tcp_server_io_uring_test transport/tcp_server_io_uring_test.cpp
    LABELS integration runtime transport io_uring
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)
