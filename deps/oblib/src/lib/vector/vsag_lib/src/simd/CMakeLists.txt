
set (SIMD_SRCS generic.cpp simd.cpp)
if (DIST_CONTAINS_SSE)
  set (SIMD_SRCS ${SIMD_SRCS} sse.cpp)
  set_source_files_properties (sse.cpp PROPERTIES COMPILE_FLAGS "-msse -msse2 -msse3 -mssse3 -msse4 -msse4a -msse4.1 -msse4.2")
  add_definitions (-DENABLE_SSE=1)
endif ()
if (DIST_CONTAINS_AVX)
  set (SIMD_SRCS ${SIMD_SRCS} avx.cpp)
  set_source_files_properties (avx.cpp PROPERTIES COMPILE_FLAGS "-mavx")
  add_definitions (-DENABLE_AVX=1)
endif ()
if (DIST_CONTAINS_AVX2)
  set_source_files_properties (avx.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mfma")
  add_definitions (-DENABLE_AVX2=1)
endif ()
if (DIST_CONTAINS_AVX512)
  set (SIMD_SRCS ${SIMD_SRCS} avx512.cpp)
  set_source_files_properties (avx512.cpp PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512pf -mavx512er -mavx512cd -mavx512vl -mavx512bw -mavx512dq -mavx512ifma -mavx512vbmi")
  add_definitions (-DENABLE_AVX512=1)
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftree-vectorize")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-calloc")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-realloc")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-free")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -fopenmp-simd")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -funroll-loops")

add_library (simd ${SIMD_SRCS})

target_link_libraries (simd PRIVATE cpuinfo)

install (
  TARGETS simd
  ARCHIVE DESTINATION lib
  )
