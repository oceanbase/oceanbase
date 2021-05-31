cmake_minimum_required(VERSION 3.20.0)

include(cmake/Utils.cmake)
include(cmake/Env.cmake)

project("OceanBase CE"
  VERSION 3.1.0
  DESCRIPTION "OceanBase distributed database system"
  HOMEPAGE_URL "https://www.oceanbase.com/"
  LANGUAGES CXX C ASM)

ob_define(WITH_OSS OFF)

if(ENABLE_DEBUG_LOG)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_DEBUG_LOG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_DEBUG_LOG")
endif()

if(WITH_OSS)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_WITH_OSS")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_WITH_OSS")
endif()

message(STATUS "This is BINARY dir " ${PROJECT_BINARY_DIR})
message(STATUS "This is SOURCE dir " ${PROJECT_SOURCE_DIR})

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(deps/easy)
add_subdirectory(deps/oblib)
add_subdirectory(src)

include(CMakeDependentOption)
# OB_BUILD_RPM => include tools and build them.
# otherwise    => include tools but don't build them.
option(OB_INCLUDE_TOOLS "" ON)
cmake_dependent_option(
  OB_BUILD_TOOLS "Build tools" ON
  "OB_BUILD_RPM" OFF)
# OB_BUILD_RPM => don't include unittest or build them.
# otherwise    => include unittest but don't build them.
cmake_dependent_option(
  OB_INCLUDE_UNITTEST "Include unittest" ON
  "NOT OB_BUILD_RPM" OFF)
option(OB_BUILD_UNITTEST "" OFF)

# OB_BUILD_RPM => don't include test or build them.
# otherwise    => include test but don't build them.
cmake_dependent_option(
  OB_INCLUDE_TEST "Include test" ON
  "NOT OB_BUILD_RPM" OFF)
option(OB_BUILD_TEST "" OFF)

include(CTest)
if (OB_BUILD_UNITTEST)
  add_subdirectory(unittest)
elseif(OB_INCLUDE_UNITTEST)
  add_subdirectory(unittest EXCLUDE_FROM_ALL)
endif()

include(cmake/RPM.cmake)
