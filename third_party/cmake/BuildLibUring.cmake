find_program(XRPC_MAKE_EXECUTABLE NAMES gmake make REQUIRED)

set(XRPC_LIBURING_SOURCE_DIR "${PROJECT_SOURCE_DIR}/third_party/liburing")
set(XRPC_LIBURING_STAGE_DIR "${CMAKE_BINARY_DIR}/third_party/liburing")
set(XRPC_LIBURING_LIBRARY "${XRPC_LIBURING_STAGE_DIR}/src/liburing.a")

# liburing 2.5's configure script writes generated files into its source tree.
# Build a copied tree under the CMake build directory to keep vendored sources
# pristine while still using liburing's own supported build machinery.
file(MAKE_DIRECTORY "${XRPC_LIBURING_STAGE_DIR}")
file(MAKE_DIRECTORY "${XRPC_LIBURING_STAGE_DIR}/src/include")
file(
    GLOB_RECURSE XRPC_LIBURING_INPUTS
    CONFIGURE_DEPENDS "${XRPC_LIBURING_SOURCE_DIR}/*"
)

add_custom_command(
    OUTPUT "${XRPC_LIBURING_LIBRARY}"
    COMMAND
        ${CMAKE_COMMAND} -E copy_directory "${XRPC_LIBURING_SOURCE_DIR}"
        "${XRPC_LIBURING_STAGE_DIR}"
    COMMAND
        "${XRPC_LIBURING_STAGE_DIR}/configure"
        "--cc=${CMAKE_C_COMPILER}" "--cxx=${CMAKE_CXX_COMPILER}"
    COMMAND
        "${XRPC_MAKE_EXECUTABLE}" -C "${XRPC_LIBURING_STAGE_DIR}/src"
        liburing.a
    DEPENDS ${XRPC_LIBURING_INPUTS}
    WORKING_DIRECTORY "${XRPC_LIBURING_STAGE_DIR}"
    COMMENT "Building vendored liburing"
    VERBATIM
)

add_custom_target(xrpc_vendor_liburing_build DEPENDS "${XRPC_LIBURING_LIBRARY}")

add_library(xrpc_vendor_liburing STATIC IMPORTED GLOBAL)
set_target_properties(
    xrpc_vendor_liburing
    PROPERTIES
        IMPORTED_LOCATION "${XRPC_LIBURING_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES
            "${XRPC_LIBURING_STAGE_DIR}/src/include"
)
add_dependencies(xrpc_vendor_liburing xrpc_vendor_liburing_build)

add_library(LibUring::LibUring ALIAS xrpc_vendor_liburing)
