cmake_minimum_required(VERSION 3.20.0)
project(oblib
  VERSION 1.0.0
  LANGUAGES CXX C ASM)

if (NOT "$ENV{DEP_DIR}" STREQUAL "")
  set(DEP_DIR "$ENV{DEP_DIR}" CACHE INTERNAL "Copied from environment variable")
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(src)
add_subdirectory(unittest EXCLUDE_FROM_ALL)
