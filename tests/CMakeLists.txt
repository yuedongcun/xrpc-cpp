set(XRPC_TEST_RUNTIME_DIR "${CMAKE_BINARY_DIR}/tests")
set(XRPC_TEST_GEN_DIR "${CMAKE_BINARY_DIR}/gen/tests")
file(MAKE_DIRECTORY "${XRPC_TEST_RUNTIME_DIR}" "${XRPC_TEST_GEN_DIR}")

add_library(xrpc_test_proto OBJECT proto/echo.proto)
target_link_libraries(xrpc_test_proto PUBLIC protobuf::libprotobuf)
target_include_directories(xrpc_test_proto PUBLIC "${XRPC_TEST_GEN_DIR}")

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
    cmake_parse_arguments(XRPC_TEST "" "${one_value_args}" "" ${ARGN})

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
            GTest::gmock_main
            Threads::Threads
    )
    add_test(NAME ${test_name} COMMAND ${test_name})
    if(XRPC_TEST_TIMEOUT)
        set_tests_properties(${test_name} PROPERTIES TIMEOUT "${XRPC_TEST_TIMEOUT}")
    endif()
endfunction()

xrpc_add_test(common_task_test common/task_test.cpp TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT})
xrpc_add_test(io_uring_awaitable_test io/uring_awaitable_test.cpp TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT})
xrpc_add_test(protocol_test protocol/protocol_test.cpp TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT})
xrpc_add_test(tcp_connection_io_uring_test transport/tcp_connection_io_uring_test.cpp TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT})
xrpc_add_test(rpc_session_test rpc/server/rpc_session_test.cpp TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT})
xrpc_add_test(rpc_server_lifecycle_test rpc/server/rpc_server_lifecycle_test.cpp TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT})
xrpc_add_test(endpoint_selector_test rpc/client/endpoint_selector_test.cpp TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT})
xrpc_add_test(rpc_client_endpoint_test rpc/client/rpc_client_endpoint_test.cpp TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT})
xrpc_add_test(
    rpc_client_thread_safety_test
    rpc/client/rpc_client_thread_safety_test.cpp
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)

xrpc_add_test(
    consul_resolver_integration_test
    rpc/naming/consul_resolver_integration_test.cpp
    TIMEOUT ${XRPC_TEST_EXTERNAL_TIMEOUT}
)
set_tests_properties(consul_resolver_integration_test PROPERTIES LABELS external)
