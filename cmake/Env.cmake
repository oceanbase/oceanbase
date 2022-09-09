include(CMakeFindBinUtils)
EXECUTE_PROCESS(COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCHITECTURE)
message(STATUS "DETECT BUILD ARCH: " ${ARCHITECTURE})

ob_define(DEVTOOLS_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/devtools")
ob_define(DEP_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel")

ob_define(OB_ENABLE_LIB_PCH ${OB_ENABLE_PCH})
ob_define(OB_ENABLE_SERVER_PCH ${OB_ENABLE_PCH})

# share compile cache between different directories
set(DEBUG_PREFIX "-fdebug-prefix-map=${CMAKE_SOURCE_DIR}=.")

set(LD_OPT "-Wl,--build-id=uuid")
set(BUILD_OPT "${DEBUG_PREFIX}")

if(${ARCHITECTURE} STREQUAL "sw_64")
  set(OB_USE_CLANG OFF)
  set(OB_USE_LLVM_LIBTOOLS OFF)
  set(OBJCOPY_BIN "${DEVTOOLS_DIR}/swgcc830_native_tools/usr/bin/objcopy")
  set(LD_BIN "${DEVTOOLS_DIR}/swgcc830_native_tools/usr/bin/ld")
  set(OB_CC "${DEVTOOLS_DIR}/swgcc830_native_tools/usr/bin/gcc")
  set(OB_CXX "${DEVTOOLS_DIR}/swgcc830_native_tools/usr/bin/g++")
  set(BUILD_OPT "${BUILD_OPT} --sysroot=${DEVTOOLS_DIR}/swgcc830_native_tools/")
endif()

if (OB_USE_LLVM_LIBTOOLS)
# create ld symlink to ld.lld for gcc
if(EXISTS "${DEVTOOLS_DIR}/bin/ld.lld")
    execute_process(COMMAND cmake -E create_symlink
        "${DEVTOOLS_DIR}/bin/ld.lld" # Old name
        "${CMAKE_SOURCE_DIR}/deps/3rd/compile/ld" # New name
        )
elseif(EXISTS "/usr/bin/ld.lld")
    execute_process(COMMAND cmake -E create_symlink
        "/usr/bin/ld.lld" # Old name
        "${CMAKE_SOURCE_DIR}/deps/3rd/compile/ld" # New name
        )
endif()
endif()

if(OB_USE_LLVM_LIBTOOLS)
  # use llvm-ar llvm-ranlib llvm-objcopy ld.lld...
  set(_CMAKE_TOOLCHAIN_PREFIX llvm-)
  set(_CMAKE_TOOLCHAIN_LOCATION "${DEVTOOLS_DIR}/bin")
  find_program(LD_BIN ld.lld PATHS "${DEVTOOLS_DIR}/bin" "/usr/bin")
  find_program(OBJCOPY_BIN llvm-objcopy PATHS "${DEVTOOLS_DIR}/bin" "/usr/bin")
else()
  find_program(LD_BIN ld PATHS "${CMAKE_SOURCE_DIR}/deps/3rd/compile" "${DEVTOOLS_DIR}/bin" "/usr/bin")
  find_program(OBJCOPY_BIN objcopy PATHS "${DEVTOOLS_DIR}/bin" "/usr/bin")
endif()

if (OB_USE_CCACHE)
  find_program(OB_CCACHE ccache
    PATHS "${DEVTOOLS_DIR}/bin" "/usr/bin"
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
    PATHS "${DEVTOOLS_DIR}/bin" "/usr/bin"
    NO_DEFAULT_PATH)
  find_program(OB_CXX clang++
    PATHS "${DEVTOOLS_DIR}/bin" "/usr/bin"
    NO_DEFAULT_PATH)
  set(BUILD_OPT "${BUILD_OPT} -fcolor-diagnostics")
  # just make embedded clang and ccache happy...
  set(LD_OPT "${LD_OPT} -Wl,-z,noexecstack")

  if (OB_USE_ASAN)
    ob_define(CMAKE_ASAN_FLAG "-fstack-protector-strong -fsanitize=address -fno-optimize-sibling-calls")
    set(BUILD_OPT "${BUILD_OPT} ${CMAKE_ASAN_FLAG} ")
  endif()


  if (OB_USE_LLVM_LIBTOOLS)
    set(LD_OPT "${LD_OPT} -fuse-ld=${LD_BIN}")
  endif()

  set(CMAKE_CXX_FLAGS "${BUILD_OPT}")
  set(CMAKE_C_FLAGS "${BUILD_OPT}")
  set(CMAKE_CXX_LINK_FLAGS "${BUILD_OPT}")
  set(CMAKE_C_LINK_FLAGS "${BUILD_OPT}")

  set(CMAKE_SHARED_LINKER_FLAGS "${LD_OPT}")
  set(CMAKE_EXE_LINKER_FLAGS "${LD_OPT}")

else() # not clang, use gcc (such as gcc52 in x86_64)

  if(NOT DEFINED OB_CC)
    find_program(OB_CC gcc
      PATHS "${DEVTOOLS_DIR}/bin" "/usr/bin"
      NO_DEFAULT_PATH)
  endif()

  if(NOT DEFINED OB_CXX)
    find_program(OB_CC g++
      PATHS "${DEVTOOLS_DIR}/bin" "/usr/bin"
      NO_DEFAULT_PATH)
  endif()

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

if(${ARCHITECTURE} STREQUAL "x86_64")
    set(MTUNE_CFLAGS "-mtune=core2")
    set(ARCH_LDFLAGS "")
elseif(${ARCHITECTURE} STREQUAL "aarch64")
    set(MARCH_CFLAGS "-march=armv8-a+crc" )
    set(MTUNE_CFLAGS "-mtune=generic" )
    set(ARCH_LDFLAGS "-l:libatomic.a")
elseif(${ARCHITECTURE} STREQUAL "loongarch64")
    set(MARCH_CFLAGS "-march=la464" "-mcmodel=large")
    set(MTUNE_CFLAGS "-mabi=lp64d")
    set(ARCH_LDFLAGS "-latomic")
elseif(${ARCHITECTURE} STREQUAL "sw_64")
    set(ARCH_LDFLAGS "-latomic -llzma")
else()
    message(FATAL_ERROR "UNSUPPORT BUILD ARCH: ${ARCHITECTURE}")
endif()
