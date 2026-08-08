set(BENCHMARK_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(BENCHMARK_GEN_DIR "${CMAKE_BINARY_DIR}/gen/benchmark")
set(BENCHMARK_BIN_DIR "${CMAKE_BINARY_DIR}/tools/benchmark")

file(MAKE_DIRECTORY "${BENCHMARK_GEN_DIR}" "${BENCHMARK_BIN_DIR}")

add_library(xrpc_benchmark_proto STATIC "${BENCHMARK_PROTO_ROOT}/echo.proto")
target_link_libraries(xrpc_benchmark_proto PUBLIC protobuf::libprotobuf)
target_include_directories(xrpc_benchmark_proto PUBLIC "${BENCHMARK_GEN_DIR}")

xrpc_protobuf_generate(
    TARGET xrpc_benchmark_proto
    LANGUAGE cpp
    IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTOC_OUT_DIR "${BENCHMARK_GEN_DIR}"
)

function(xrpc_add_benchmark target)
    add_executable(${target} ${ARGN})
    target_include_directories(
        ${target}
        PRIVATE
            "${PROJECT_SOURCE_DIR}/src/include"
            "${PROJECT_SOURCE_DIR}/src"
            "${CMAKE_CURRENT_SOURCE_DIR}"
    )
    target_link_libraries(
        ${target}
        PRIVATE xrpc::xrpc xrpc_benchmark_proto Threads::Threads
    )
    set_target_properties(
        ${target}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${BENCHMARK_BIN_DIR}"
    )
endfunction()

xrpc_add_benchmark(
    xrpc_benchmark_server
    server/benchmark_server.cpp
)
xrpc_add_benchmark(
    xrpc_benchmark_firehose
    firehose/firehose_client.cpp
    benchmark_stats.cpp
)
xrpc_add_benchmark(
    xrpc_benchmark_client
    rpc_client/rpc_client_benchmark.cpp
    benchmark_stats.cpp
)
