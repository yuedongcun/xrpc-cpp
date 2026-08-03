find_path(LibUring_INCLUDE_DIR NAMES liburing.h)
find_library(LibUring_LIBRARY NAMES uring)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    LibUring
    REQUIRED_VARS LibUring_LIBRARY LibUring_INCLUDE_DIR
)

if(LibUring_FOUND AND NOT TARGET LibUring::LibUring)
    add_library(LibUring::LibUring UNKNOWN IMPORTED)
    set_target_properties(
        LibUring::LibUring
        PROPERTIES
            IMPORTED_LOCATION "${LibUring_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${LibUring_INCLUDE_DIR}"
    )
endif()

mark_as_advanced(LibUring_INCLUDE_DIR LibUring_LIBRARY)
