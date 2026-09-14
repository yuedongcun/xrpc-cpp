# Verify actual installed artifacts, relocation, and linkage without host packages.
function(run_checked)
    execute_process(
        COMMAND ${ARGV}
        RESULT_VARIABLE result
        OUTPUT_VARIABLE output
        ERROR_VARIABLE error
    )
    if(NOT result STREQUAL "0")
        message(FATAL_ERROR "Command failed (${result}): ${ARGV}\n${output}\n${error}")
    endif()
endfunction()

string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef test_id)
set(test_root "${XRPC_BUILD_DIR}/tests/install-${test_id}")
set(prefix "${test_root}/relocated")

run_checked("${CMAKE_COMMAND}" --install "${XRPC_BUILD_DIR}"
    --config "${XRPC_BUILD_CONFIG}" --prefix "${test_root}/original")
file(RENAME "${test_root}/original" "${prefix}")

run_checked("${CMAKE_COMMAND}" -S "${XRPC_CONSUMER_SOURCE_DIR}" -B "${test_root}/consumer"
    "-DCMAKE_CXX_COMPILER=${XRPC_CXX_COMPILER}"
    "-DCMAKE_BUILD_TYPE=${XRPC_BUILD_CONFIG}"
    "-DCMAKE_PREFIX_PATH=${prefix}"
    "-DCMAKE_FIND_ROOT_PATH=${prefix}"
    "-DCMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY"
    "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF"
    "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF")
run_checked("${CMAKE_COMMAND}" --build "${test_root}/consumer" --config "${XRPC_BUILD_CONFIG}")
run_checked("${test_root}/consumer/xrpc_package_consumer")

file(REMOVE_RECURSE "${test_root}")
