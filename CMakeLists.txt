cmake_minimum_required(VERSION 3.20)

project(xrpc VERSION 0.1.0 LANGUAGES C CXX)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

option(XRPC_BUILD_EXAMPLES "Build xrpc examples" OFF)
option(XRPC_BUILD_TESTS "Build xrpc tests" OFF)
option(XRPC_BUILD_TOOLS "Build xrpc tools" OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# -----------------------------------------------------------------------
# Dependencies
# -----------------------------------------------------------------------
find_package(Threads REQUIRED)
add_subdirectory(third_party)
include(cmake/XrpcProtobufGenerate.cmake)

# -----------------------------------------------------------------------
# Main library
# -----------------------------------------------------------------------
add_subdirectory(src)

# -----------------------------------------------------------------------
# Subdirectories
# -----------------------------------------------------------------------
if(XRPC_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(XRPC_BUILD_TOOLS)
    add_subdirectory(tools)
endif()

if(XRPC_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# -----------------------------------------------------------------------
# Installation
# -----------------------------------------------------------------------
install(
    TARGETS xrpc
    EXPORT xrpcTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY include/xrpc DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(
    EXPORT xrpcTargets
    FILE xrpcTargets.cmake
    NAMESPACE xrpc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xrpc
)

configure_package_config_file(
    cmake/xrpcConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/xrpcConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xrpc
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/xrpcConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/xrpcConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/xrpcConfigVersion.cmake"
        cmake/FindLibUring.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xrpc
)
