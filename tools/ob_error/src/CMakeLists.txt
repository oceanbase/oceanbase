add_definitions(-D__ERROR_CODE_PARSER_)
set(SRC_LIST ob_error.cpp os_errno.cpp ${CMAKE_SOURCE_DIR}/src/share/ob_errno.cpp)

add_executable(ob_error ${SRC_LIST})

add_library(oberror SHARED ${SRC_LIST})

include_directories(
    ${CMAKE_SOURCE_DIR}/
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/deps/oblib/src
)
