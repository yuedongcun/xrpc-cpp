set(EXAMPLE_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(EXAMPLE_GEN_DIR "${CMAKE_BINARY_DIR}/gen/examples")
set(EXAMPLE_BIN_DIR "${CMAKE_BINARY_DIR}/examples")

file(MAKE_DIRECTORY "${EXAMPLE_GEN_DIR}")

add_library(xrpc_example_proto STATIC "${EXAMPLE_PROTO_ROOT}/echo.proto")
target_link_libraries(xrpc_example_proto PUBLIC protobuf::libprotobuf)
target_include_directories(xrpc_example_proto PUBLIC "${EXAMPLE_GEN_DIR}")

xrpc_protobuf_generate(
    TARGET xrpc_example_proto
    LANGUAGE cpp
    IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTOC_OUT_DIR "${EXAMPLE_GEN_DIR}"
)

function(xrpc_add_example target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE xrpc::xrpc xrpc_example_proto)
    target_include_directories(
        ${target}
        PRIVATE "${PROJECT_SOURCE_DIR}/src/include" "${PROJECT_SOURCE_DIR}/src"
    )
    set_target_properties(
        ${target}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${EXAMPLE_BIN_DIR}"
    )
endfunction()

xrpc_add_example(xrpc_echo_server echo_server.cpp)
xrpc_add_example(xrpc_echo_client echo_client.cpp)
