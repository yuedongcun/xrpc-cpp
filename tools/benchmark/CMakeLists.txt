set(BENCHMARK_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(BENCHMARK_GEN_DIR "${CMAKE_BINARY_DIR}/gen/benchmark")
set(BENCHMARK_BIN_DIR "${CMAKE_BINARY_DIR}/tools/benchmark")

file(MAKE_DIRECTORY "${BENCHMARK_GEN_DIR}" "${BENCHMARK_BIN_DIR}")

add_library(xrpc_benchmark_proto STATIC "${BENCHMARK_PROTO_ROOT}/echo.proto")
target_link_libraries(xrpc_benchmark_proto PUBLIC protobuf::libprotobuf)
target_include_directories(xrpc_benchmark_proto PUBLIC "${BENCHMARK_GEN_DIR}")

protobuf_generate(
    TARGET xrpc_benchmark_proto
    LANGUAGE cpp
    IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTOC_OUT_DIR "${BENCHMARK_GEN_DIR}"
)

add_library(
    xrpc_benchmark_common
    STATIC
    common/benchmark_config.cpp
    common/benchmark_stats.cpp
    common/benchmark_client.cpp
    common/firehose_client.cpp
    common/rpc_client_benchmark.cpp
    common/benchmark_server.cpp
)
target_include_directories(
    xrpc_benchmark_common
    PUBLIC
        "${PROJECT_SOURCE_DIR}/src/include"
        "${PROJECT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}"
)
target_link_libraries(
    xrpc_benchmark_common
    PUBLIC xrpc::xrpc xrpc_benchmark_proto Threads::Threads
)
function(xrpc_add_benchmark target source)
    add_executable(${target} ${source})
    target_include_directories(
        ${target}
        PRIVATE
            "${PROJECT_SOURCE_DIR}/src/include"
            "${PROJECT_SOURCE_DIR}/src"
            "${CMAKE_CURRENT_SOURCE_DIR}"
    )
    target_link_libraries(
        ${target}
        PRIVATE xrpc_benchmark_common xrpc_benchmark_proto Threads::Threads
    )
    set_target_properties(
        ${target}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${BENCHMARK_BIN_DIR}"
    )
endfunction()

xrpc_add_benchmark(xrpc_benchmark_client client_main.cpp)
xrpc_add_benchmark(xrpc_benchmark_server server_main.cpp)
