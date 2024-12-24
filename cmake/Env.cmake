ob_define(DEBUG_PREFIX "-fdebug-prefix-map=${CMAKE_SOURCE_DIR}=.")
ob_define(FILE_PREFIX "-ffile-prefix-map=${CMAKE_SOURCE_DIR}=.")
ob_define(OB_LD_BIN ld)
ob_define(ASAN_IGNORE_LIST "${CMAKE_SOURCE_DIR}/asan_ignore_list.txt")

ob_define(DEP_3RD_DIR "${CMAKE_SOURCE_DIR}/deps/3rd")
ob_define(DEVTOOLS_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/devtools")
ob_define(DEP_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel")

ob_define(BUILD_CDC_ONLY OFF)
ob_define(OB_USE_CLANG ON)
ob_define(OB_ERRSIM OFF)
ob_define(BUILD_NUMBER 1)
ob_define(OB_GPERF_MODE OFF)
ob_define(OB_TRANS_ERRSIM OFF)
ob_define(OB_DIS_SEARRAY OFF)
ob_define(ENABLE_LATCH_DIAGNOSE OFF)
ob_define(ENABLE_MEMORY_DIAGNOSIS OFF)
ob_define(ENABLE_OBJ_LEAK_CHECK OFF)
ob_define(ENABLE_FATAL_ERROR_HANG ON)
ob_define(ENABLE_SMART_VAR_CHECK OFF)
ob_define(ENABLE_COMPILE_DLL_MODE OFF)
ob_define(OB_CMAKE_RULES_CHECK ON)
ob_define(OB_STATIC_LINK_LGPL_DEPS ON)
ob_define(HOTFUNC_PATH "${CMAKE_SOURCE_DIR}/hotfuncs.txt")
ob_define(OB_BUILD_CCLS OFF)
ob_define(LTO_JOBS all)
ob_define(LTO_CACHE_DIR "${CMAKE_BINARY_DIR}/cache")
ob_define(LTO_CACHE_POLICY cache_size=100%:cache_size_bytes=0k:cache_size_files=0:prune_after=0s:prune_interval=72h)
ob_define(NEED_PARSER_CACHE ON)
# get compiler from build.sh
ob_define(OB_CC "")
ob_define(OB_CXX "")

# 'ENABLE_PERF_MODE' use for offline system insight performance test
# PERF_MODE macro controls many special code path in system
# we can open this to benchmark our system partial/layered
ob_define(ENABLE_PERF_MODE OFF)

# begin of unity build config
ob_define(OB_MAX_UNITY_BATCH_SIZE 30)
# the global switch of unity build, default is 'ON'
ob_define(OB_ENABLE_UNITY ON)

ob_define(OB_BUILD_OPENSOURCE ON)

ob_define(OB_DISABLE_LSE OFF)

ob_define(OB_DISABLE_PIE OFF)

ob_define(OB_ENABLE_MCMODEL OFF)

ob_define(USE_LTO_CACHE OFF)

if(WITH_COVERAGE)
  # -ftest-coverage to generate .gcno file
  # -fprofile-arcs to generate .gcda file
  # -DDBUILD_COVERAGE marco use to mark 'coverage build type' and to handle some special case
  set(CMAKE_COVERAGE_COMPILE_OPTIONS -ftest-coverage -fprofile-arcs -Xclang -coverage-version=408R -DBUILD_COVERAGE)
  set(CMAKE_COVERAGE_EXE_LINKER_OPTIONS "-ftest-coverage -fprofile-arcs")

  add_compile_options(${CMAKE_COVERAGE_COMPILE_OPTIONS})
  set(DEBUG_PREFIX "")
  set(FILE_PREFIX "")
endif()

ob_define(AUTO_FDO_OPT "")
if(ENABLE_AUTO_FDO)
  # file name pattern [observer.prof.{current_timestamp ms}]
  set(AUTO_FDO_OPT "-finline-functions -fprofile-sample-use=${CMAKE_SOURCE_DIR}/observer.prof.1702984872675")
endif()

ob_define(THIN_LTO_OPT "")
ob_define(THIN_LTO_CONCURRENCY_LINK "")

if(ENABLE_THIN_LTO)
  set(THIN_LTO_OPT "-flto=thin")
  set(THIN_LTO_CONCURRENCY_LINK "-Wl,--thinlto-jobs=${LTO_JOBS}")
  if(USE_LTO_CACHE)
    set(THIN_LTO_CONCURRENCY_LINK "${THIN_LTO_CONCURRENCY_LINK},--thinlto-cache-dir=${LTO_CACHE_DIR},--thinlto-cache-policy=${LTO_CACHE_POLICY}")
  endif()
endif()

set(HOTFUNC_OPT "")
if(ENABLE_HOTFUNC)
  set(HOTFUNC_OPT "-Wl,--no-warn-symbol-ordering,--symbol-ordering-file,${HOTFUNC_PATH}")
endif()

set(BOLT_OPT "")
if((ENABLE_BOLT OR (NOT DEFINED ENABLE_BOLT AND ENABLE_BOLT_AUTO)) AND NOT OB_BUILD_OPENSOURCE)
  EXECUTE_PROCESS(COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCHITECTURE)
  if( ${ARCHITECTURE} STREQUAL "x86_64" )
    message(STATUS "build with bolt opt")
    set(BOLT_OPT "-Wl,--emit-relocs")
    ob_define(OB_ENABLE_BOLT ON)
  endif()
endif()

ob_define(CPP_STANDARD_20 OFF)
if(CPP_STANDARD_20)
  message(STATUS "Using C++20 standard")
  set(CMAKE_CXX_FLAGS "-std=gnu++20")
  ob_define(CPP_STANDARD_20 ON)
  add_definitions(-DCPP_STANDARD_20)
else()
  message(STATUS "Using C++11 standard")
  set(CMAKE_CXX_FLAGS "-std=gnu++11")
endif()

if(OB_DISABLE_PIE)
  message(STATUS "build without pie")
  set(PIE_OPT "")
else()
  message(STATUS "build with pie")
  set(PIE_OPT "-pie")
endif()

set(ob_close_modules_static_name "")
set(ob_close_deps_static_name "")

if (OB_BUILD_OPENSOURCE)
  # 开源模式
  set(OB_BUILD_CLOSE_MODULES OFF)
else()
  # 闭源模式
  set(OB_BUILD_CLOSE_MODULES ON)
endif()

if(OB_BUILD_CLOSE_MODULES)
  # SECURITY, 包含3个功能点
  ob_define(OB_BUILD_TDE_SECURITY ON)
  ob_define(OB_BUILD_AUDIT_SECURITY ON)
  ob_define(OB_BUILD_LABEL_SECURITY ON)
  # SPM功能
  ob_define(OB_BUILD_SPM ON)
  # share storage
  ob_define(OB_BUILD_SHARED_STORAGE ON)

  # oracle
  ob_define(OB_BUILD_ORACLE_PARSER ON)
  ob_define(OB_BUILD_ORACLE_PL ON)
  # dblink
  ob_define(OB_BUILD_DBLINK ON)
  # odps
  ob_define(OB_BUILD_CPP_ODPS ON)
  # 仲裁功能
  ob_define(OB_BUILD_ARBITRATION ON)

  # 日志存储压缩
  ob_define(OB_BUILD_LOG_STORAGE_COMPRESS ON)

  # 默认使用BABASSL
  ob_define(OB_USE_BABASSL ON)
  add_definitions(-DOB_USE_BABASSL)
  # 默认使用OB_USE_DRCMSG
  ob_define(OB_USE_DRCMSG ON)
  add_definitions(-DOB_USE_DRCMSG)
endif()

# 下面开始逻辑控制
if(OB_BUILD_CLOSE_MODULES)
  add_definitions(-DOB_BUILD_CLOSE_MODULES)
endif()

if(OB_BUILD_TDE_SECURITY)
  add_definitions(-DOB_BUILD_TDE_SECURITY)
endif()

if(OB_BUILD_AUDIT_SECURITY)
  add_definitions(-DOB_BUILD_AUDIT_SECURITY)
endif()

if(OB_BUILD_LABEL_SECURITY)
  add_definitions(-DOB_BUILD_LABEL_SECURITY)
endif()

if(OB_BUILD_SHARED_STORAGE)
  add_definitions(-DOB_BUILD_SHARED_STORAGE)
endif()

if(OB_BUILD_SPM)
  add_definitions(-DOB_BUILD_SPM)
endif()

if(OB_BUILD_ORACLE_PARSER)
  add_definitions(-DOB_BUILD_ORACLE_PARSER)
endif()

if(OB_BUILD_ORACLE_PL)
  add_definitions(-DOB_BUILD_ORACLE_PL)
endif()

if(OB_BUILD_ARBITRATION)
  add_definitions(-DOB_BUILD_ARBITRATION)
endif()

if(OB_BUILD_LOG_STORAGE_COMPRESS)
  add_definitions(-DOB_BUILD_LOG_STORAGE_COMPRESS)
endif()

if(OB_BUILD_DBLINK)
  add_definitions(-DOB_BUILD_DBLINK)
endif()

if(OB_BUILD_CPP_ODPS)
  add_definitions(-DOB_BUILD_CPP_ODPS)
endif()

# should not use initial-exec for tls-model if building OBCDC.
if(BUILD_CDC_ONLY)
  add_definitions(-DOB_BUILD_CDC_DISABLE_VSAG)
else()
  add_definitions(-DENABLE_INITIAL_EXEC_TLS_MODEL)
endif()

set(OB_OBJCOPY_BIN "${DEVTOOLS_DIR}/bin/objcopy")

# NO RELERO: -Wl,-znorelro
# Partial RELRO: -Wl,-z,relro
# Full RELRO: -Wl,-z,relro,-z,now
ob_define(OB_RELRO_FLAG "-Wl,-z,relro,-z,now")

ob_define(OB_USE_CCACHE OFF)
if (OB_USE_CCACHE)
  find_program(OB_CCACHE ccache PATHS "${DEVTOOLS_DIR}/bin" NO_DEFAULT_PATH)
  if (NOT OB_CCACHE)
    message(FATAL_ERROR "cannot find ccache.")
  else()
    set(CMAKE_C_COMPILER_LAUNCHER ${OB_CCACHE})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${OB_CCACHE})
  endif()
endif(OB_USE_CCACHE)

if (OB_USE_CLANG)

  if (OB_CC)
    message(STATUS "Using OB_CC compiler: ${OB_CC}")
  else()
    find_program(OB_CC clang
    "${DEVTOOLS_DIR}/bin"
      NO_DEFAULT_PATH)
  endif()

  if(CPP_STANDARD_20)
    execute_process(COMMAND ${OB_CC} --version OUTPUT_VARIABLE CLANG_VERSION_STRING)
    string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" CLANG_VERSION ${CLANG_VERSION_STRING})
    if(CLANG_VERSION VERSION_LESS "17.0.0")
      message(FATAL_ERROR "Clang version must be at least 17.0.0 if CPP_STANDARD_20 is ON, Please run the cmake process with '-DCPP_STANDARD_20=ON --init'")
    endif()
  endif()

  if (OB_CXX)
    message(STATUS "Using OB_CXX compiler: ${OB_CXX}")
  else()
    find_program(OB_CXX clang++
    "${DEVTOOLS_DIR}/bin"
      NO_DEFAULT_PATH)
  endif()

  find_file(GCC9 devtools
    PATHS ${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase
    NO_DEFAULT_PATH)
  set(_CMAKE_TOOLCHAIN_PREFIX llvm-)
  set(_CMAKE_TOOLCHAIN_LOCATION "${DEVTOOLS_DIR}/bin")

  if (OB_USE_ASAN)
    ob_define(CMAKE_ASAN_FLAG "-mllvm -asan-stack=0 -fsanitize=address -fno-optimize-sibling-calls -fsanitize-blacklist=${ASAN_IGNORE_LIST}")
  endif()

  if (OB_USE_LLD)
    set(LD_OPT "-fuse-ld=${DEVTOOLS_DIR}/bin/ld.lld -Wno-unused-command-line-argument")
    set(REORDER_COMP_OPT "-ffunction-sections -fdebug-info-for-profiling")
    set(REORDER_LINK_OPT "-Wl,--no-rosegment,--build-id=sha1 ${HOTFUNC_OPT}")
    set(OB_LD_BIN "${DEVTOOLS_DIR}/bin/ld.lld")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${FILE_PREFIX} ${AUTO_FDO_OPT} ${THIN_LTO_OPT} -fcolor-diagnostics ${REORDER_COMP_OPT} -fmax-type-align=8 ${CMAKE_ASAN_FLAG}")
  set(CMAKE_C_FLAGS "--gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${FILE_PREFIX} ${AUTO_FDO_OPT} ${THIN_LTO_OPT} -fcolor-diagnostics ${REORDER_COMP_OPT} -fmax-type-align=8 ${CMAKE_ASAN_FLAG}")
  set(CMAKE_CXX_LINK_FLAGS "${LD_OPT} --gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${FILE_PREFIX} ${AUTO_FDO_OPT}")
  set(CMAKE_SHARED_LINKER_FLAGS "${LD_OPT} -Wl,-z,noexecstack ${THIN_LTO_CONCURRENCY_LINK} ${REORDER_LINK_OPT}")
  set(CMAKE_EXE_LINKER_FLAGS "${LD_OPT} -Wl,-z,noexecstack ${PIE_OPT} ${THIN_LTO_CONCURRENCY_LINK} ${REORDER_LINK_OPT} ${CMAKE_COVERAGE_EXE_LINKER_OPTIONS}")
else() # not clang, use gcc
if(OB_BUILD_OPENSOURCE)
  message("gcc9 not support currently, please set OB_USE_CLANG ON and we will finish it as soon as possible")
else()

  if (OB_CC)
    message(STATUS "Using OB_CC compiler: ${OB_CC}")
  else()
    find_program(OB_CC gcc
      PATHS ${DEVTOOLS_DIR}/bin
      NO_DEFAULT_PATH)
  endif()

  if (OB_CXX)
    message(STATUS "Using OB_CXX compiler: ${OB_CXX}")
  else()
    find_program(OB_CXX g++
      PATHS ${DEVTOOLS_DIR}/bin
      NO_DEFAULT_PATH)
  endif()

  if (OB_USE_LLD)
    set(LD_OPT "-B${CMAKE_SOURCE_DIR}/rpm/.compile")
    set(REORDER_COMP_OPT "-ffunction-sections")
    set(REORDER_LINK_OPT "${HOTFUNC_OPT}")
  endif()
  set(CMAKE_CXX_FLAGS "${LD_OPT} -fdiagnostics-color ${REORDER_COMP_OPT}")
  set(CMAKE_C_FLAGS "${LD_OPT} -fdiagnostics-color ${REORDER_COMP_OPT}")
  set(CMAKE_SHARED_LINKER_FLAGS "-z noexecstack ${REORDER_LINK_OPT}")
  set(CMAKE_EXE_LINKER_FLAGS "-z noexecstack ${REORDER_LINK_OPT}")
endif()
endif()

if (OB_BUILD_CCLS)
  # ccls场景采用更大的unity的联合编译单元，ccls是非完整编译，调用clang AST接口，单元的size和耗时成指数衰减
  set(OB_MAX_UNITY_BATCH_SIZE 200)
  # -DCCLS_LASY_ENABLE 给全局设置上，将采用ccls懒加载模式，主要针对单测case，当添加上-DCCLS_LASY_OFF，首次将会进行检索
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCCLS_LASY_ENABLE")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCCLS_LASY_ENABLE")
endif()

if (OB_CC AND OB_CXX)
  set(CMAKE_C_COMPILER ${OB_CC})
  set(CMAKE_CXX_COMPILER ${OB_CXX})
else()
  message(FATAL_ERROR "can't find suitable compiler")
endif()

find_program(OB_COMPILE_EXECUTABLE ob-compile)
if (NOT OB_COMPILE_EXECUTABLE)
  message(STATUS "ob-compile not found, compile locally.")
else()
  set(CMAKE_C_COMPILER_LAUNCHER ${OB_COMPILE_EXECUTABLE})
  set(CMAKE_CXX_COMPILER_LAUNCHER ${OB_COMPILE_EXECUTABLE})
  set(CMAKE_C_LINKER_LAUNCHER ${OB_COMPILE_EXECUTABLE})
  set(CMAKE_CXX_LINKER_LAUNCHER ${OB_COMPILE_EXECUTABLE})
endif()

option(OB_ENABLE_AVX2 "enable AVX2 and related instruction set support for x86_64" OFF)

include(CMakeFindBinUtils)
EXECUTE_PROCESS(COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCHITECTURE )
if( ${ARCHITECTURE} STREQUAL "x86_64" )
    set(MTUNE_CFLAGS -mtune=core2)
    set(ARCH_LDFLAGS "")
    set(OCI_DEVEL_INC "${DEP_3RD_DIR}/usr/include/oracle/12.2/client64")
else()
    if (${OB_DISABLE_LSE})
      message(STATUS "build with no-lse")
      set(MARCH_CFLAGS "-march=armv8-a+crc")
    else()
      message(STATUS "build with lse")
      set(MARCH_CFLAGS "-march=armv8-a+crc+lse")
    endif()
    set(MTUNE_CFLAGS "-mtune=generic" )
    set(ARCH_LDFLAGS "-l:libatomic.a")
    set(OCI_DEVEL_INC "${DEP_3RD_DIR}/usr/include/oracle/19.10/client64")
endif()

EXECUTE_PROCESS(COMMAND grep -Po "release [0-9]{1}" /etc/redhat-release COMMAND awk "{print $2}" COMMAND tr -d '\n' OUTPUT_VARIABLE KERNEL_RELEASE ERROR_QUIET)
