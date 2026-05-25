set(SRC_LIST
    ob_sandbox.cpp
    ob_sandbox_setup.cpp
    ${CMAKE_BINARY_DIR}/src/observer/ob_version.cpp
)

add_executable(ob_sandbox ${SRC_LIST})

target_compile_definitions(ob_sandbox PRIVATE
    PACKAGE_NAME="${PROJECT_NAME}"
    PACKAGE_VERSION="${PROJECT_VERSION}"
    PACKAGE_STRING="${PROJECT_NAME} ${PROJECT_VERSION}"
    RELEASEID="${BUILD_NUMBER}"
)

target_link_libraries(
    ob_sandbox PRIVATE
    -static-libgcc
    -static-libstdc++
    oblib
    ${DEP_DIR}/lib64/libminijail.a
    ${DEP_DIR}/lib64/libcap.a
)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/deps/oblib/src/
)
