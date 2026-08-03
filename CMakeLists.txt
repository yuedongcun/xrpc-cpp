cmake_minimum_required(VERSION 3.20)

project(xrpc VERSION 0.1.0 LANGUAGES CXX)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(XRPC_BUILD_EXAMPLES "Build xrpc examples" OFF)
option(XRPC_BUILD_TESTS "Build xrpc tests" OFF)
option(XRPC_BUILD_TOOLS "Build xrpc tools" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CLANG_VERSION 20)
set(XRPC_BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build_support")
set(XRPC_CLANG_SEARCH_PATH
    "/usr/local/bin"
    "/usr/bin"
    "/usr/local/opt/llvm/bin"
    "/usr/lib/llvm-${CLANG_VERSION}/bin"
    "/usr/local/opt/llvm@${CLANG_VERSION}/bin"
    "/opt/homebrew/opt/llvm@${CLANG_VERSION}/bin/"
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if(CMAKE_CXX_COMPILER_VERSION MATCHES "^${CLANG_VERSION}\\.")
        message(
            STATUS
            "Using recommended compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}"
        )
    else()
        message(
            WARNING
            "Recommend using clang-${CLANG_VERSION} for xrpc. "
            "Current compiler is ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION} "
            "at ${CMAKE_CXX_COMPILER}."
        )
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    message(
        WARNING
        "You are using AppleClang ${CMAKE_CXX_COMPILER_VERSION}. "
        "For xrpc development, recommend LLVM clang-${CLANG_VERSION}."
    )
else()
    message(
        WARNING
        "Recommend using clang-${CLANG_VERSION} for xrpc. "
        "Current compiler is ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION} "
        "at ${CMAKE_CXX_COMPILER}."
    )
endif()

# clang-format
find_program(
    CLANG_FORMAT_BIN
    NAMES clang-format-${CLANG_VERSION} clang-format
    HINTS ${XRPC_CLANG_SEARCH_PATH}
)

if("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
    message(WARNING "xrpc/main couldn't find clang-format.")
else()
    message(STATUS "xrpc/main found clang-format at ${CLANG_FORMAT_BIN}")
endif()

# clang-tidy
find_program(
    CLANG_TIDY_BIN
    NAMES clang-tidy-${CLANG_VERSION} clang-tidy
    HINTS ${XRPC_CLANG_SEARCH_PATH}
)

if("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
    message(WARNING "xrpc/main couldn't find clang-tidy.")
else()
    set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
    message(STATUS "xrpc/main found clang-tidy at ${CLANG_TIDY_BIN}")
endif()

find_program(
    CLANG_APPLY_REPLACEMENTS_BIN
    NAMES clang-apply-replacements-${CLANG_VERSION} clang-apply-replacements
    HINTS ${XRPC_CLANG_SEARCH_PATH}
)

if(
    "${CLANG_APPLY_REPLACEMENTS_BIN}"
        STREQUAL
        "CLANG_APPLY_REPLACEMENTS_BIN-NOTFOUND"
)
    message(WARNING "xrpc/main couldn't find clang-apply-replacements.")
else()
    set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
    message(
        STATUS
        "xrpc/main found clang-apply-replacements at ${CLANG_APPLY_REPLACEMENTS_BIN}"
    )
endif()

# -----------------------------------------------------------------------
# Dependencies
# -----------------------------------------------------------------------
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(LibUring REQUIRED)

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
    add_subdirectory(example)
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

# #####################################################################################################################
# MAKE TARGETS
# #####################################################################################################################

# #########################################
# "make format"
# "make check-format"
# #########################################

string(
    CONCAT XRPC_FORMAT_DIRS
    "${CMAKE_CURRENT_SOURCE_DIR}/include,"
    "${CMAKE_CURRENT_SOURCE_DIR}/src,"
    "${CMAKE_CURRENT_SOURCE_DIR}/tests,"
    "${CMAKE_CURRENT_SOURCE_DIR}/example,"
    "${CMAKE_CURRENT_SOURCE_DIR}/tools,"
    "${CMAKE_CURRENT_SOURCE_DIR}/config,"
)

# Runs clang format and updates files in place.
add_custom_target(
    format
    ${XRPC_BUILD_SUPPORT_DIR}/run_clang_format.py ${CLANG_FORMAT_BIN}
    ${XRPC_BUILD_SUPPORT_DIR}/clang_format_exclusions.txt --source_dirs
    ${XRPC_FORMAT_DIRS} --fix --quiet
)

# Runs clang format and exits with a non-zero exit code if any files need to be reformatted.
add_custom_target(
    check-format
    ${XRPC_BUILD_SUPPORT_DIR}/run_clang_format.py ${CLANG_FORMAT_BIN}
    ${XRPC_BUILD_SUPPORT_DIR}/clang_format_exclusions.txt --source_dirs
    ${XRPC_FORMAT_DIRS} --quiet
)

# ##########################################################
# "make check-clang-tidy"
# "make fix-clang-tidy"
# ##########################################################

set(XRPC_CLANG_TIDY_FILES "${CMAKE_CURRENT_SOURCE_DIR}/(src|tools)/.*")
set(XRPC_CLANG_TIDY_HEADER_FILTER
    "^${CMAKE_CURRENT_SOURCE_DIR}/(include|src|tools)/.*"
)

# Runs clang-tidy and exits with a non-zero exit code if any errors are found.
# Note that clang-tidy automatically looks for a .clang-tidy file in parent directories.
add_custom_target(
    check-clang-tidy
    ${XRPC_BUILD_SUPPORT_DIR}/run_clang_tidy.py -quiet -clang-tidy-binary
    ${CLANG_TIDY_BIN} -p ${CMAKE_BINARY_DIR}
    "-header-filter=${XRPC_CLANG_TIDY_HEADER_FILTER}" ${XRPC_CLANG_TIDY_FILES}
    VERBATIM
)

# Runs clang-tidy and applies suggested fixes.
add_custom_target(
    fix-clang-tidy
    ${XRPC_BUILD_SUPPORT_DIR}/run_clang_tidy.py -clang-tidy-binary
    ${CLANG_TIDY_BIN} -p ${CMAKE_BINARY_DIR}
    "-header-filter=${XRPC_CLANG_TIDY_HEADER_FILTER}"
    -clang-apply-replacements-binary ${CLANG_APPLY_REPLACEMENTS_BIN} -fix
    ${XRPC_CLANG_TIDY_FILES}
    VERBATIM
)
