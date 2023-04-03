cmake_minimum_required(VERSION 3.20)

project(objit
  VERSION 1.0.0
  LANGUAGES C CXX)

  message(STATUS "${DEP_3RD_DIR}")
  if (NOT DEP_3RD_DIR)
    message(FATAL_ERROR "DEP_3RD_DIR not set")
  endif()

set(LLVM_DIR "${DEP_3RD_DIR}/usr/local/oceanbase/devtools/lib/cmake/llvm")
find_package(LLVM REQUIRED CONFIG)

include(cmake/libutils.cmake)
include(cmake/custom.cmake)

message(STATUS "This is BINARY dir " ${PROJECT_BINARY_DIR})
message(STATUS "This is SOURCE dir " ${PROJECT_SOURCE_DIR})

include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})
add_definitions(-Wno-deprecated)
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0 -g -O2 -frtti)

# Find the libraries that correspond to the LLVM components
# that we wish to use
if( ${ARCHITECTURE} STREQUAL "x86_64" )
  LLVM_MAP_COMPONENTS_TO_LIBNAMES(llvm_libs Support Core IRReader ExecutionEngine OrcJit McJit X86CodeGen X86AsmParser runtimedyld bitreader bitwriter object objectyaml target DebugInfoDWARF Symbolize)
else()
  LLVM_MAP_COMPONENTS_TO_LIBNAMES(llvm_libs Support Core IRReader ExecutionEngine OrcJit McJit AArch64CodeGen AArch64AsmParser runtimedyld bitreader bitwriter object objectyaml target DebugInfoDWARF Symbolize)
endif()
set(CMAKE_MODULE_PATH "${LLVM_CMAKE_DIR}")
include(${LLVM_CMAKE_DIR}/HandleLLVMOptions.cmake)

add_subdirectory(src)

