ob_define(DEBUG_PREFIX "-fdebug-prefix-map=${CMAKE_SOURCE_DIR}=.")
ob_define(OB_LD_BIN ld)
ob_define(ASAN_IGNORE_LIST "${CMAKE_SOURCE_DIR}/asan_ignore_list.txt")

ob_define(DEP_3RD_DIR "${CMAKE_SOURCE_DIR}/deps/3rd")
ob_define(DEVTOOLS_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/devtools")
ob_define(DEP_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel")

ob_define(OB_BUILD_CDC OFF)
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
# get compiler from build.sh
ob_define(OB_CC "")
ob_define(OB_CXX "")

# 'ENABLE_PERF_MODE' use for offline system insight performance test
# PERF_MODE macro controls many special code path in system
# we can open this to benchmark our system partial/layered
ob_define(ENABLE_PERF_MODE OFF)

# begin of unity build config
ob_define(OB_MAX_UNITY_BATCH_SIZE 30)
# the global switch of unity build, defualt is 'ON'
ob_define(OB_ENABLE_UNITY ON)

ob_define(OB_BUILD_OPENSOURCE ON)


if(WITH_COVERAGE)
  # -ftest-coverage to generate .gcno file
  # -fprofile-arcs to generate .gcda file
  # -DDBUILD_COVERAGE marco use to mark 'coverage build type' and to handle some speical case
  set(CMAKE_COVERAGE_COMPILE_OPTIONS -ftest-coverage -fprofile-arcs -Xclang -coverage-version=408R -DBUILD_COVERAGE)
  set(CMAKE_COVERAGE_EXE_LINKER_OPTIONS "-ftest-coverage -fprofile-arcs")

  add_compile_options(${CMAKE_COVERAGE_COMPILE_OPTIONS})
  set(DEBUG_PREFIX "")
endif()

ob_define(AUTO_FDO_OPT "")
if(ENABLE_AUTO_FDO)
  # file name pattern [observer.prof.{current_timestamp ms}]
  set(AUTO_FDO_OPT "-fprofile-sample-use=${CMAKE_SOURCE_DIR}/observer.prof.1693473964865")
endif()

ob_define(THIN_LTO_OPT "")
ob_define(THIN_LTO_CONCURRENCY_LINK "")

if(ENABLE_THIN_LTO)
  set(THIN_LTO_OPT "-flto=thin -fwhole-program-vtables")
  set(THIN_LTO_CONCURRENCY_LINK "-Wl,--thinlto-jobs=32,--lto-whole-program-visibility")
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
  # 字符集
  ob_define(OB_BUILD_FULL_CHARSET ON)
  # SPM功能
  ob_define(OB_BUILD_SPM ON)

  # oralce
  ob_define(OB_BUILD_ORACLE_PARSER ON)
  ob_define(OB_BUILD_ORACLE_PL ON)
  ob_define(OB_BUILD_ORACLE_XML ON)
  # dblink
  ob_define(OB_BUILD_DBLINK ON)
  # 仲裁功能
  ob_define(OB_BUILD_ARBITRATION ON)

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

if(OB_BUILD_FULL_CHARSET)
  add_definitions(-DOB_BUILD_FULL_CHARSET)
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

if(OB_BUILD_ORACLE_XML)
  add_definitions(-DOB_BUILD_ORACLE_XML)
endif()

if(OB_BUILD_ARBITRATION)
  add_definitions(-DOB_BUILD_ARBITRATION)
endif()

if(OB_BUILD_DBLINK)
  add_definitions(-DOB_BUILD_DBLINK)
endif()

# should not use initial-exec for tls-model if building OBCDC.
if(NOT OB_BUILD_CDC)
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
    ob_define(CMAKE_ASAN_FLAG "-fstack-protector-strong -fsanitize=address -fno-optimize-sibling-calls -fsanitize-blacklist=${ASAN_IGNORE_LIST}")
  endif()

  if (OB_USE_LLD)
    set(LD_OPT "-fuse-ld=${DEVTOOLS_DIR}/bin/ld.lld")
    set(REORDER_COMP_OPT "-ffunction-sections -fdebug-info-for-profiling")
    set(REORDER_LINK_OPT "-Wl,--no-rosegment,--build-id=sha1,--no-warn-symbol-ordering,--symbol-ordering-file,${HOTFUNC_PATH}")
    set(OB_LD_BIN "${DEVTOOLS_DIR}/bin/ld.lld")
  endif()
  set(CMAKE_CXX_FLAGS "--gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${AUTO_FDO_OPT} ${THIN_LTO_OPT} -fcolor-diagnostics ${REORDER_COMP_OPT} -fmax-type-align=8 ${CMAKE_ASAN_FLAG} -std=gnu++11")
  set(CMAKE_C_FLAGS "--gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${AUTO_FDO_OPT} ${THIN_LTO_OPT} -fcolor-diagnostics ${REORDER_COMP_OPT} -fmax-type-align=8 ${CMAKE_ASAN_FLAG}")
  set(CMAKE_CXX_LINK_FLAGS "${LD_OPT} ${THIN_LTO_CONCURRENCY_LINK} --gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${AUTO_FDO_OPT}")
  set(CMAKE_SHARED_LINKER_FLAGS "${LD_OPT} -Wl,-z,noexecstack ${THIN_LTO_CONCURRENCY_LINK} ${REORDER_LINK_OPT}")
  set(CMAKE_EXE_LINKER_FLAGS "${LD_OPT} -Wl,-z,noexecstack -pie ${THIN_LTO_CONCURRENCY_LINK} ${REORDER_LINK_OPT} ${CMAKE_COVERAGE_EXE_LINKER_OPTIONS}")
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
    set(REORDER_LINK_OPT "-Wl,--no-warn-symbol-ordering,--symbol-ordering-file,${HOTFUNC_PATH}")
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
    set(MARCH_CFLAGS "-march=armv8-a+crc" )
    set(MTUNE_CFLAGS "-mtune=generic" )
    set(ARCH_LDFLAGS "-l:libatomic.a")
    set(OCI_DEVEL_INC "${DEP_3RD_DIR}/usr/include/oracle/19.10/client64")
endif()

EXECUTE_PROCESS(COMMAND grep -Po "release [0-9]{1}" /etc/redhat-release COMMAND awk "{print $2}" COMMAND tr -d '\n' OUTPUT_VARIABLE KERNEL_RELEASE ERROR_QUIET)
