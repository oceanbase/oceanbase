cmake_minimum_required (VERSION 3.18)
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    cmake_policy(SET CMP0135 NEW)
endif()

project (VSAG
  VERSION 0.9.0
  LANGUAGES C CXX
)

message ("CPU ARCH: ${CMAKE_HOST_SYSTEM_PROCESSOR}")

macro(vsag_add_exe_linker_flag flag)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${flag}")
endmacro()

macro(vsag_add_shared_linker_flag flag)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${flag}")
endmacro()

macro(maybe_add_dependencies depender)
    if (TARGET ${depender})
        foreach (dependee ${ARGN})
            if (TARGET ${dependee})
                add_dependencies(${depender} ${dependee})
            endif()
        endforeach()
    endif()
endmacro()

# options
option (ENABLE_JEMALLOC "Whether to link jemalloc to all executables" OFF)
option (ENABLE_ASAN "Whether to turn AddressSanitizer ON or OFF" OFF)
option (ENABLE_COVERAGE "Whether to turn unit test coverage ON or OFF" OFF)
option (ENABLE_FUZZ_TEST "Whether to turn Fuzz Test ON or OFF" OFF)
option (ENABLE_WERROR "Whether to error on warnings" ON)
option (ENABLE_TSAN "Whether to turn Thread Sanitizer ON or OFF" OFF)
option (ENABLE_FRAME_POINTER "Whether to build with -fno-omit-frame-pointer" ON)
option (ENABLE_THIN_LTO "Whether to build with thin lto -flto=thin" OFF)
option (ENABLE_CCACHE "Whether to open ccache" OFF)
option (ENABLE_COVERAGE "Enable gcov (debug, Linux builds only)" OFF)
option (ENABLE_INTEL_MKL "Enable intel-mkl (x86 platform only)" OFF)
option (ENABLE_CXX11_ABI "Use CXX11 ABI"OFF)
option (ENABLE_LIBCXX "Use libc++ instead of libstdc++" ON)
option (ENABLE_TOOLS "Whether compile vsag tools" ON)
option (DISABLE_SSE_FORCE "Force disable sse and higher instructions" OFF)
option (DISABLE_AVX_FORCE "Force disable avx and higher instructions" OFF)
option (DISABLE_AVX2_FORCE "Force disable avx2 and higher instructions" OFF)
option (DISABLE_AVX512_FORCE "Force disable avx512 instructions" OFF)

set (ROARING_DISABLE_AVX512 ON)
set (ENABLE_PYBINDS ON)

if (ENABLE_CXX11_ABI)
  add_definitions (-D_GLIBCXX_USE_CXX11_ABI=1)
else ()
  add_definitions (-D_GLIBCXX_USE_CXX11_ABI=0)
endif ()

if (ENABLE_WERROR)
    add_compile_options(-Werror)
endif()

vsag_add_exe_linker_flag(-static-libstdc++)
vsag_add_shared_linker_flag(-static-libstdc++)
vsag_add_shared_linker_flag(-fvisibility=hidden)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL 17 OR CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 17)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -gdwarf-4")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -gdwarf-4 -Wno-deprecated-builtins")
    else()
        set(ENABLE_LIBCXX OFF)
    endif()
else()
    set(ENABLE_LIBCXX OFF)
endif()

if (ENABLE_LIBCXX)
    message(STATUS "Using libc++")

    set(LIBCXX_SEARCH_PATH /opt/alibaba-cloud-compiler/lib64)
    find_library(LIBCXX_STATIC libc++.a PATHS ${LIBCXX_SEARCH_PATH})
    find_library(LIBCXXABI_STATIC libc++abi.a PATHS ${LIBCXX_SEARCH_PATH})
    if (LIBCXX_STATIC AND LIBCXXABI_STATIC)
        get_filename_component(LIBCXX_DIR "${LIBCXX_STATIC}" DIRECTORY)
        message(STATUS "Found libc++ at ${LIBCXX_STATIC}")
        message(STATUS "Found libc++abi at ${LIBCXXABI_STATIC}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")

        vsag_add_shared_linker_flag("-fuse-ld=lld")
        vsag_add_shared_linker_flag("-Wl,-rpath,${LIBCXX_DIR}")
        vsag_add_exe_linker_flag("-fuse-ld=lld")
        vsag_add_exe_linker_flag("-Wl,-rpath,${LIBCXX_DIR}")
        set(CMAKE_CXX_STANDARD_LIBRARIES "${CMAKE_CXX_STANDARD_LIBRARIES} ${LIBCXX_STATIC} ${LIBCXXABI_STATIC}")
    else()
        message(FATAL_ERROR "libc++ or libc++abi not found")
    endif()
else()
    message(STATUS "Using libstdc++")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libstdc++")
    endif()
endif()

# ccache
find_program (CCACHE_FOUND ccache)
if (CCACHE_FOUND AND ENABLE_CCACHE)
 message (STATUS "Compiling with ccache")
 set_property (GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
 set_property (GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")

set(ld_flags
    "-L${CMAKE_INSTALL_PREFIX}/lib"
    "-L${CMAKE_INSTALL_PREFIX}/lib64"
)
if (EXISTS "/usr/lib/${CMAKE_LIBRARY_ARCHITECTURE}")
    set(ld_flags
        "${ld_flags}"
        "-L/usr/lib/${CMAKE_LIBRARY_ARCHITECTURE}"
    )
endif()
if (EXISTS "/usr/lib64/${CMAKE_LIBRARY_ARCHITECTURE}")
    set(ld_flags
        "${ld_flags}"
        "-L/usr/lib64/${CMAKE_LIBRARY_ARCHITECTURE}"
    )
endif()
if (EXISTS "/opt/alibaba-cloud-compiler/lib64/")
    set(ld_flags
        "${ld_flags}"
        "-L/opt/alibaba-cloud-compiler/lib64/"
    )
endif()
string(JOIN " " ld_flags ${ld_flags})

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")

set(common_cmake_args
    "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
    "-DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fPIC -gdwarf-4 "
    "-DCMAKE_C_FLAGS=${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fPIC -gdwarf-4 "
    "-DCMAKE_EXE_LINKER_FLAGS=${extra_link_libs}"
    "-DCMAKE_SHARED_LINKER_FLAGS=${extra_link_libs}"
    "-DCMAKE_INCLUDE_PATH=${CMAKE_INSTALL_PREFIX}/include"
    "-DCMAKE_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib:/opt/alibaba-cloud-compiler/lib64"
)

set(common_configure_envs
    "env"
    "CC=${CMAKE_C_COMPILER}"
    "CXX=${CMAKE_CXX_COMPILER}"
    "CFLAGS=${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fPIC -O2 -D_DEFAULT_SOURCE -D_GNU_SOURCE -gdwarf-4"
    "CXXFLAGS=${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fPIC -O2 -D_DEFAULT_SOURCE -D_GNU_SOURCE -gdwarf-4"
    "CPPFLAGS=-isystem ${CMAKE_INSTALL_PREFIX}/include"
    "LDFLAGS=-Wl,-rpath=\\\\$\\$ORIGIN ${ld_flags}"
    "PATH=${BUILDING_PATH}:$ENV{PATH}"
    "ACLOCAL_PATH=${ACLOCAL_PATH}"
)

# set the default compilation parallelism of thirdparties
if (NOT NUM_BUILDING_JOBS)
  set (NUM_BUILDING_JOBS 4)
endif ()

# thirdparties of lib
include (cmake/CheckSIMDCompilerFlag.cmake)
include (ExternalProject)
include (extern/json/json.cmake)
include (extern/boost/boost.cmake)
include (extern/openblas/openblas.cmake)
include (extern/mkl/mkl.cmake)
include (extern/diskann/diskann.cmake)
include (extern/spdlog/spdlog.cmake)
include (extern/catch2/catch2.cmake)
include (extern/cpuinfo/cpuinfo.cmake)
include (extern/fmt/fmt.cmake)
include (extern/thread_pool/thread_pool.cmake)
include (extern/roaringbitmap/roaringbitmap.cmake)

# thirdparties of tools
if (ENABLE_TOOLS)
    include (extern/hdf5/hdf5.cmake)
endif ()

include_directories (${CMAKE_CURRENT_BINARY_DIR}/spdlog/install/include)
include_directories (include)

set (CMAKE_CXX_STANDARD 17)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL 17 OR CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 17)
        set (CMAKE_CXX_STANDARD 20)
    endif()
endif()

# debug or release
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Werror=suggest-override")
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Ofast")
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-incompatible-pointer-types-discards-qualifiers")
endif()

# coverage
if (ENABLE_COVERAGE AND NOT WIN32 AND NOT APPLE)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -DCOVERAGE_TEST_USE_GCOV")
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -DCOVERAGE_TEST_USE_GCOV")
endif ()

# asan
if (ENABLE_ASAN)
  if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0))
    message(STATUS "Compiling with AddressSanitizer and UndefinedBehaviorSanitizer")
    set (ASAN_FLAGS "-g -fsanitize=address,undefined -fno-omit-frame-pointer -fno-optimize-sibling-calls -fsanitize-recover=address -fno-sanitize=vptr")
  else ()
    message(STATUS "Compiling with AddressSanitizer")
    set (ASAN_FLAGS "-g -fsanitize=address -fno-omit-frame-pointer -static-libasan")
  endif ()

  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ASAN_FLAGS}")
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${ASAN_FLAGS}")
  set (CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} ${ASAN_FLAGS}")
endif ()

# tsan
if (ENABLE_TSAN)
    if (ENABLE_ASAN)
        message (FATAL_ERROR "ENABLE_TSAN cannot be combined with ENABLE_ASAN")
    endif ()
    set (CMAKE_REQUIRED_FLAGS "-fsanitize=thread")
    set (ENV{TSAN_OPTIONS} "report_atomic_races=0")
    add_compile_options (-fsanitize=thread)
    add_compile_options (-g)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif ()

# sources
add_subdirectory (src)
add_subdirectory (mockimpl)
#add_subdirectory (examples/cpp)

# tests
if (ENABLE_TESTS)
     add_subdirectory (tests)
endif ()

# pybinds
if (ENABLE_PYBINDS)
    find_package (Python3 REQUIRED COMPONENTS Interpreter Development)
    include (extern/pybind11/pybind11.cmake)
    pybind11_add_module (pyvsag python_bindings/binding.cpp)
    target_link_libraries (pyvsag PRIVATE pybind11::module vsag_test)
endif ()

# install
install (DIRECTORY include/
	DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install (TARGETS vsag_test
	LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install (TARGETS vsag_static_test
	LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
if (ENABLE_TESTS AND USE_CXX11_ABI)
    install (TARGETS test_performance
        LIBRARY DESTINATION "${CMAKE_INSTALL_BINDIR}")
endif ()

# version
find_package (Git)
add_custom_target (version
  ${CMAKE_COMMAND} -D SRC=${CMAKE_CURRENT_SOURCE_DIR}/src/version.h.in
                   -D DST=${CMAKE_CURRENT_SOURCE_DIR}/src/version.h
                   -D GIT_EXECUTABLE=${GIT_EXECUTABLE}
                   -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/GenerateVersionHeader.cmake
  )
add_dependencies (vsag_test version)

# set path
set(VSAG_BASE_DIR ${CMAKE_BINARY_DIR})
set(VSAG_LIB_DIR ${VSAG_BASE_DIR}/_deps)

# Create shared library
add_library(ob_vsag_test SHARED ob_vsag_lib.cpp)
target_compile_options(ob_vsag_test PRIVATE -std=c++17)
target_include_directories(ob_vsag_test PRIVATE
                           ${VSAG_BASE_DIR}/include
                           ${VSAG_LIB_DIR}/nlohmann_json-src/include
                           ${VSAG_LIB_DIR}/roaringbitmap-src/include
                           ${VSAG_LIB_DIR}/fmt-src/include)
target_compile_definitions(ob_vsag_test PRIVATE _GLIBCXX_USE_CXX11_ABI=0)
target_link_libraries(ob_vsag_test PUBLIC vsag_static_test -static-libstdc++ -static-libgcc)
add_dependencies(ob_vsag_test vsag_static_test)

# Create static library
add_library(ob_vsag_static_test STATIC ob_vsag_lib.cpp)
target_compile_options(ob_vsag_static_test PRIVATE -std=c++17)
target_compile_definitions(ob_vsag_static_test PUBLIC _GLIBCXX_USE_CXX11_ABI=0)
target_include_directories(ob_vsag_static_test PUBLIC
                           ${VSAG_BASE_DIR}/include
                           ${VSAG_LIB_DIR}/nlohmann_json-src/include
                           ${VSAG_LIB_DIR}/roaringbitmap-src/include
                           ${VSAG_LIB_DIR}/fmt-src/include)
add_dependencies(ob_vsag_static_test vsag_static_test)

add_subdirectory (example)
