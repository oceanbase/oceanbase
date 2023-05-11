cmake_minimum_required(VERSION 3.20)

include(cmake/Utils.cmake)
include(cmake/Env.cmake)

project("OceanBase_CE"
  VERSION 4.1.0.1
  DESCRIPTION "OceanBase distributed database system"
  HOMEPAGE_URL "https://open.oceanbase.com/"
  LANGUAGES CXX C ASM)
message(STATUS "open source build enabled")


if(ENABLE_COMPILE_DLL_MODE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCOMPILE_DLL_MODE")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCOMPILE_DLL_MODE")
endif()

if(OB_ERRSIM)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DERRSIM")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DERRSIM")
  set(OB_TRANS_ERRSIM ON)
endif()

if(OB_DIS_SEARRAY)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DDISABLE_SE_ARRAY")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDISABLE_SE_ARRAY")
endif()

if(OB_TRANS_ERRSIM)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DTRANS_ERRSIM")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DTRANS_ERRSIM")
endif()

if(TRANS_MODULE_TEST)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DTRANS_MODULE_TEST")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DTRANS_MODULE_TEST")
endif()

if(ENABLE_LATCH_DIAGNOSE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_LATCH_DIAGNOSE")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_LATCH_DIAGNOSE")
endif()

if(ENABLE_SMART_VAR_CHECK)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_SMART_VAR_CHECK")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_SMART_VAR_CHECK")
endif()

if(ENABLE_DEBUG_LOG)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_DEBUG_LOG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_DEBUG_LOG")
endif()

if (OB_GPERF_MODE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D__NEED_PERF__")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__NEED_PERF__")
endif()

if (ENABLE_PERF_MODE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DPERF_MODE")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPERF_MODE")
endif()

if (ENABLE_MEMORY_DIAGNOSIS)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DMEMORY_DIAGNOSIS=1")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DMEMORY_DIAGNOSIS=1")
endif()

if (ENABLE_OBJ_LEAK_CHECK)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_OBJ_LEAK_CHECK=1")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_OBJ_LEAK_CHECK=1")
endif()

if (ENABLE_FATAL_ERROR_HANG)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DFATAL_ERROR_HANG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DFATAL_ERROR_HANG")
endif()

if(OB_USE_ASAN)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DOB_USE_ASAN")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOB_USE_ASAN")
endif()

if (ENABLE_SANITY)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_SANITY")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_SANITY")
endif()

message(STATUS "This is BINARY dir " ${PROJECT_BINARY_DIR})
message(STATUS "This is SOURCE dir " ${PROJECT_SOURCE_DIR})

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(deps/easy)
add_subdirectory(deps/oblib)
add_subdirectory(src/objit)
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

# OB_BUILD_RPM => build sql proxy parser
cmake_dependent_option(
  OB_BUILD_LIBOB_SQL_PROXY_PARSER "Build lib_sql_proxy_parser" OFF
  "NOT OB_BUILD_RPM" ON
)

# OB_BUILD_RPM => build OB_BUILD_LIBOBTABLE
cmake_dependent_option(
  OB_BUILD_LIBOBTABLE "Build liboblog" OFF
  "NOT OB_BUILD_RPM" ON
)

# OB_BUILD_RPM => build OB_BUILD_OBADMIN
cmake_dependent_option(
  OB_BUILD_OBADMIN "Build ob_admin" OFF
  "NOT OB_BUILD_RPM" ON
)

include(CTest)
if (OB_BUILD_UNITTEST)
  add_subdirectory(unittest)
elseif(OB_INCLUDE_UNITTEST)
  add_subdirectory(unittest EXCLUDE_FROM_ALL)
endif()


if (OB_BUILD_TOOLS)
  add_subdirectory(tools)
elseif (OB_INCLUDE_TOOLS)
  add_subdirectory(tools EXCLUDE_FROM_ALL)
endif()

include(cmake/RPM.cmake)
