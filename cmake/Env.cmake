ob_define(DEVTOOLS_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/devtools")
ob_define(DEP_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel")

ob_define(OB_USE_CLANG ON)
ob_define(OB_USE_LLVM_LIBTOOLS ON)
ob_define(OB_COMPRESS_DEBUG_SECTIONS OFF)
ob_define(OB_STATIC_LINK_LGPL_DEPS ON)

ob_define(OB_USE_CCACHE OFF)
ob_define(OB_ENABLE_PCH ON)
ob_define(OB_ENABLE_LIB_PCH ${OB_ENABLE_PCH})
ob_define(OB_ENABLE_SERVER_PCH ${OB_ENABLE_PCH})
ob_define(OB_ENALBE_UNITY ON)
ob_define(OB_MAX_UNITY_BATCH_SIZE 30)

ob_define(OB_RELEASEID 1)

set(OBJCOPY_BIN "${DEVTOOLS_DIR}/bin/objcopy")
set(LD_BIN "${DEVTOOLS_DIR}/bin/ld")

# share compile cache between different directories
set(DEBUG_PREFIX "-fdebug-prefix-map=${CMAKE_SOURCE_DIR}=.")

set(LD_OPT "-Wl,--build-id=uuid")
set(BUILD_OPT "${DEBUG_PREFIX}")

if (OB_USE_LLVM_LIBTOOLS)
  # use llvm-ar llvm-ranlib llvm-objcopy ld.lld... 
  set(_CMAKE_TOOLCHAIN_PREFIX llvm-)
  set(_CMAKE_TOOLCHAIN_LOCATION "${DEVTOOLS_DIR}/bin")
  set(LD_BIN "${DEVTOOLS_DIR}/bin/ld.lld")
  set(OBJCOPY_BIN "${DEVTOOLS_DIR}/bin/llvm-objcopy")
endif()

if (OB_USE_CCACHE)
  find_program(OB_CCACHE ccache
    PATHS "${DEVTOOLS_DIR}/bin"
    NO_DEFAULT_PATH)
  if (NOT OB_CCACHE)
    message(WARNING "CCACHE NOT FOUND, COMPILE CACHE MAY NOT WORK.")
  else()
    set(CMAKE_C_COMPILER_LAUNCHER ${OB_CCACHE})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${OB_CCACHE})
  endif()
endif()

if (OB_COMPRESS_DEBUG_SECTIONS)
  set(LD_OPT "${LD_OPT} -Wl,--compress-debug-sections=zlib")
endif()

if (OB_USE_CLANG)
  find_program(OB_CC clang
    PATHS "${DEVTOOLS_DIR}/bin"
    NO_DEFAULT_PATH)
  find_program(OB_CXX clang++
    PATHS "${DEVTOOLS_DIR}/bin"
    NO_DEFAULT_PATH)
  set(BUILD_OPT "${BUILD_OPT} --gcc-toolchain=${DEVTOOLS_DIR} -fcolor-diagnostics")
  # just make embedded clang and ccache happy...
  set(BUILD_OPT "${BUILD_OPT} -I${DEVTOOLS_DIR}/lib/clang/11.0.1/include")
  set(LD_OPT "${LD_OPT} -Wl,-z,noexecstack")

  if (OB_USE_LLVM_LIBTOOLS)
    set(LD_OPT "${LD_OPT} -fuse-ld=${LD_BIN}")
  endif()

  set(CMAKE_CXX_FLAGS "${BUILD_OPT}")
  set(CMAKE_C_FLAGS "${BUILD_OPT}")
  set(CMAKE_CXX_LINK_FLAGS "${BUILD_OPT}")
  set(CMAKE_C_LINK_FLAGS "${BUILD_OPT}")

  set(CMAKE_SHARED_LINKER_FLAGS "${LD_OPT}")
  set(CMAKE_EXE_LINKER_FLAGS "${LD_OPT}")

else() # not clang, use gcc52
  find_program(OB_CC gcc
    PATHS "${DEVTOOLS_DIR}/bin"
    NO_DEFAULT_PATH)
  find_program(OB_CXX g++
    PATHS "${DEVTOOLS_DIR}/bin"
    NO_DEFAULT_PATH)

  set(BUILD_OPT "${BUILD_OPT} -fdiagnostics-color")
  set(LD_OPT "${LD_OPT} -z noexecstack")

  if (OB_USE_LLVM_LIBTOOLS)
    #gcc not support -fuse-ld option
    set(BUILD_OPT "${BUILD_OPT} -B${CMAKE_SOURCE_DIR}/deps/3rd/compile")
  endif()

  set(CMAKE_CXX_FLAGS "${BUILD_OPT}")
  set(CMAKE_C_FLAGS "${BUILD_OPT}")

  set(CMAKE_SHARED_LINKER_FLAGS "${LD_OPT}")
  set(CMAKE_EXE_LINKER_FLAGS "${LD_OPT}")
endif()

if (OB_CC AND OB_CXX)
  set(CMAKE_C_COMPILER ${OB_CC})
  set(CMAKE_CXX_COMPILER ${OB_CXX})
else()
  message(FATAL_ERROR "COMPILER NOT FOUND")
endif()

include(CMakeFindBinUtils)
EXECUTE_PROCESS(COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCHITECTURE)
message(STATUS "DETECT BUILD ARCH: " ${ARCHITECTURE})
if(${ARCHITECTURE} STREQUAL "x86_64")
    set(MTUNE_CFLAGS "-mtune=core2")
    set(ARCH_LDFLAGS "")
else()
    message(FATAL_ERROR "UNSUPPORT BUILD ARCH: ${ARCHITECTURE}")
endif()
