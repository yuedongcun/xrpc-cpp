# Keep all vendored dependency configuration in one place so the top-level
# build never needs to discover these libraries from the host system.

set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_CONFORMANCE OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_PROTOC_BINARIES ON CACHE BOOL "" FORCE)
set(protobuf_BUILD_LIBPROTOC ON CACHE BOOL "" FORCE)
set(protobuf_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(protobuf_WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(protobuf_INSTALL ON CACHE BOOL "" FORCE)
add_subdirectory(protobuf)

set(JSON_BuildTests OFF CACHE INTERNAL "")
set(JSON_Install ON CACHE INTERNAL "")
add_subdirectory(nlohmann_json)

# Scope glog's generic options so they do not change other dependencies.
function(xrpc_add_glog)
    set(BUILD_SHARED_LIBS OFF)
    set(BUILD_TESTING OFF)
    set(WITH_GFLAGS OFF)
    set(WITH_GTEST OFF)
    set(WITH_GMOCK OFF)
    set(WITH_UNWIND none)
    add_subdirectory(glog)
endfunction()
xrpc_add_glog()

if(XRPC_BUILD_TESTS)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
    add_subdirectory(googletest EXCLUDE_FROM_ALL)
endif()

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildLibUring.cmake")
