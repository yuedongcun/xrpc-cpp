# Generate C++ protobuf sources using the protoc target built from the vendored
# protobuf tree. This intentionally implements only the options used by xrpc.
function(xrpc_protobuf_generate)
    set(one_value_args TARGET LANGUAGE PROTOC_OUT_DIR)
    set(multi_value_args IMPORT_DIRS)
    cmake_parse_arguments(
        XRPC_PROTO "" "${one_value_args}" "${multi_value_args}" ${ARGN}
    )

    if(XRPC_PROTO_UNPARSED_ARGUMENTS)
        message(
            FATAL_ERROR
            "Unsupported xrpc_protobuf_generate arguments: ${XRPC_PROTO_UNPARSED_ARGUMENTS}"
        )
    endif()

    if(NOT XRPC_PROTO_TARGET OR NOT TARGET ${XRPC_PROTO_TARGET})
        message(FATAL_ERROR "xrpc_protobuf_generate requires an existing TARGET")
    endif()

    if(NOT XRPC_PROTO_LANGUAGE STREQUAL "cpp")
        message(FATAL_ERROR "xrpc only supports C++ protobuf generation")
    endif()

    if(NOT XRPC_PROTO_PROTOC_OUT_DIR)
        message(FATAL_ERROR "xrpc_protobuf_generate requires PROTOC_OUT_DIR")
    endif()

    get_filename_component(
        source_root "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE
    )
    set(proto_path_args "--proto_path=${source_root}")
    foreach(import_dir IN LISTS XRPC_PROTO_IMPORT_DIRS)
        get_filename_component(
            import_root "${import_dir}" ABSOLUTE
            BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
        )
        if(NOT import_root STREQUAL source_root)
            list(APPEND proto_path_args "--proto_path=${import_root}")
        endif()
    endforeach()

    get_target_property(target_sources ${XRPC_PROTO_TARGET} SOURCES)
    set(generated_sources)

    foreach(source IN LISTS target_sources)
        if(NOT source MATCHES "\\.proto$")
            continue()
        endif()

        get_filename_component(
            proto_file "${source}" ABSOLUTE
            BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
        )

        file(RELATIVE_PATH proto_relative "${source_root}" "${proto_file}")
        if(proto_relative MATCHES "^\\.\\./" OR proto_relative STREQUAL "..")
            message(
                FATAL_ERROR
                "${proto_file} is outside ${CMAKE_CURRENT_SOURCE_DIR}"
            )
        endif()

        string(REGEX REPLACE "\\.proto$" ".pb.cc" generated_cc "${proto_relative}")
        string(REGEX REPLACE "\\.proto$" ".pb.h" generated_h "${proto_relative}")
        set(generated_cc "${XRPC_PROTO_PROTOC_OUT_DIR}/${generated_cc}")
        set(generated_h "${XRPC_PROTO_PROTOC_OUT_DIR}/${generated_h}")
        get_filename_component(generated_dir "${generated_cc}" DIRECTORY)
        file(MAKE_DIRECTORY "${generated_dir}")

        add_custom_command(
            OUTPUT "${generated_cc}" "${generated_h}"
            COMMAND protobuf::protoc
            ARGS
                ${proto_path_args}
                "--cpp_out=${XRPC_PROTO_PROTOC_OUT_DIR}"
                "${proto_file}"
            DEPENDS "${proto_file}" protobuf::protoc
            COMMENT "Generating C++ protobuf sources from ${proto_relative}"
            VERBATIM
        )

        list(APPEND generated_sources "${generated_cc}" "${generated_h}")
    endforeach()

    if(NOT generated_sources)
        message(FATAL_ERROR "${XRPC_PROTO_TARGET} has no .proto sources")
    endif()

    set_source_files_properties(${generated_sources} PROPERTIES GENERATED TRUE)
    target_sources(${XRPC_PROTO_TARGET} PRIVATE ${generated_sources})
endfunction()
