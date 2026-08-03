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
set(XRPC_TEST_TOOLING_TIMEOUT 15)

function(xrpc_set_test_metadata test_name)
    set(one_value_args TIMEOUT)
    set(multi_value_args LABELS)
    cmake_parse_arguments(
        XRPC_TEST
        ""
        "${one_value_args}"
        "${multi_value_args}"
        ${ARGN}
    )

    if(XRPC_TEST_LABELS)
        set_tests_properties(
            ${test_name}
            PROPERTIES LABELS "${XRPC_TEST_LABELS}"
        )
    endif()
    if(XRPC_TEST_TIMEOUT)
        set_tests_properties(
            ${test_name}
            PROPERTIES TIMEOUT "${XRPC_TEST_TIMEOUT}"
        )
    endif()
endfunction()

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
    xrpc_set_test_metadata(
        ${test_name}
        LABELS ${XRPC_TEST_LABELS}
        TIMEOUT ${XRPC_TEST_TIMEOUT}
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
xrpc_add_test(
    observability_metrics_test observability/metrics_test.cpp
    LABELS unit observability
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    prometheus_metric_sink_test observability/prometheus_metric_sink_test.cpp
    LABELS unit observability prometheus
    TIMEOUT ${XRPC_TEST_UNIT_TIMEOUT}
)
xrpc_add_test(
    prometheus_exporter_test observability/prometheus_exporter_test.cpp
    LABELS integration observability prometheus transport
    TIMEOUT ${XRPC_TEST_INTEGRATION_TIMEOUT}
)

add_test(
    NAME benchmark_v1_smoke_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_smoke.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_smoke_config_test
    LABELS tooling benchmark config smoke
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_server_capacity_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_server_capacity.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_server_capacity_config_test
    LABELS tooling benchmark config
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_protobuf_smoke_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_protobuf_smoke.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_protobuf_smoke_config_test
    LABELS tooling benchmark config protobuf smoke
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_protobuf_capacity_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_protobuf_capacity.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_protobuf_capacity_config_test
    LABELS tooling benchmark config protobuf
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_rpc_client_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_rpc_client.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_rpc_client_config_test
    LABELS tooling benchmark config client
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_raw_high_load_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_raw_high_load.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_raw_high_load_config_test
    LABELS tooling benchmark config
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_protobuf_steady_state_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_protobuf_steady_state.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_protobuf_steady_state_config_test
    LABELS tooling benchmark config protobuf
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_protobuf_ceiling_connections_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_protobuf_ceiling_connections.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_protobuf_ceiling_connections_config_test
    LABELS tooling benchmark config protobuf
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_protobuf_ceiling_load_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_protobuf_ceiling_load.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_protobuf_ceiling_load_config_test
    LABELS tooling benchmark config protobuf
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_protobuf_ceiling_boundary_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_protobuf_ceiling_boundary.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_protobuf_ceiling_boundary_config_test
    LABELS tooling benchmark config protobuf
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_protobuf_ceiling_verify_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_protobuf_ceiling_verify.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_protobuf_ceiling_verify_config_test
    LABELS tooling benchmark config protobuf
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME benchmark_v1_protobuf_overload_probe_config_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/benchmark/run_suite.py" --config
        "${PROJECT_SOURCE_DIR}/tools/benchmark/configs/v1_protobuf_overload_probe.json"
        --validate-config
)
xrpc_set_test_metadata(
    benchmark_v1_protobuf_overload_probe_config_test
    LABELS tooling benchmark config protobuf
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME ha_consul_failover_smoke_dry_run_test
    COMMAND "${PROJECT_SOURCE_DIR}/tools/ha/consul_failover_smoke.py" --dry-run
)
xrpc_set_test_metadata(
    ha_consul_failover_smoke_dry_run_test
    LABELS tooling ha dry_run consul smoke
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME ha_consul_failover_smoke_invalid_args_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/ha/consul_failover_smoke.py" --dry-run
        --client-metrics-port=19101
)
xrpc_set_test_metadata(
    ha_consul_failover_smoke_invalid_args_test
    LABELS tooling ha dry_run consul negative
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
set_tests_properties(
    ha_consul_failover_smoke_invalid_args_test
    PROPERTIES WILL_FAIL TRUE
)

add_test(
    NAME ha_consul_fault_injection_redundant_server_kill_dry_run_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/ha/consul_fault_injection.py" --dry-run
        --scenario=redundant-server-kill
)
xrpc_set_test_metadata(
    ha_consul_fault_injection_redundant_server_kill_dry_run_test
    LABELS tooling ha dry_run consul
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME ha_consul_fault_injection_resolver_empty_dry_run_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/ha/consul_fault_injection.py" --dry-run
        --scenario=resolver-empty --min-client-failures=1
)
xrpc_set_test_metadata(
    ha_consul_fault_injection_resolver_empty_dry_run_test
    LABELS tooling ha dry_run consul
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME ha_consul_fault_injection_delayed_registration_dry_run_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/ha/consul_fault_injection.py" --dry-run
        --scenario=delayed-registration --min-client-failures=1
)
xrpc_set_test_metadata(
    ha_consul_fault_injection_delayed_registration_dry_run_test
    LABELS tooling ha dry_run consul
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
add_test(
    NAME ha_consul_fault_injection_invalid_args_test
    COMMAND
        "${PROJECT_SOURCE_DIR}/tools/ha/consul_fault_injection.py" --dry-run
        --scenario=delayed-registration --duration-s=2 --fault-after-s=1
        --replacement-delay-s=2
)
xrpc_set_test_metadata(
    ha_consul_fault_injection_invalid_args_test
    LABELS tooling ha dry_run consul negative
    TIMEOUT ${XRPC_TEST_TOOLING_TIMEOUT}
)
set_tests_properties(
    ha_consul_fault_injection_invalid_args_test
    PROPERTIES WILL_FAIL TRUE
)
