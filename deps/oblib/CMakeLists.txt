cmake_minimum_required(VERSION 3.20)
project(oblib
  VERSION 1.0.0
  LANGUAGES CXX C ASM)

if (NOT "$ENV{DEP_DIR}" STREQUAL "")
  set(DEP_DIR "$ENV{DEP_DIR}" CACHE INTERNAL "Copied from environment variable")
endif()

if("${DEP_DIR}" STREQUAL "")
  set(DEP_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel")
endif()

message(STATUS "This is BINARY dir " ${PROJECT_BINARY_DIR})
message(STATUS "This is SOURCE dir " ${PROJECT_SOURCE_DIR})

file(COPY . DESTINATION . FILES_MATCHING PATTERN "alloc_flow_records")
file(COPY . DESTINATION . FILES_MATCHING PATTERN "*.result")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(src)
add_subdirectory(unittest EXCLUDE_FROM_ALL)
