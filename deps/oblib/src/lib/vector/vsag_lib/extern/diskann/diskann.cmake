include_directories(${CMAKE_CURRENT_BINARY_DIR}/boost/install/include
  ${CMAKE_CURRENT_BINARY_DIR}/openblas/install/include
  ${AIO_INCLUDE_DIR}
  )
link_directories(${CMAKE_CURRENT_BINARY_DIR}/openblas/install/lib)

include_directories(extern/diskann/DiskANN/include)
set(DISKANN_SOURCES
    extern/diskann/DiskANN/src/abstract_data_store.cpp
    extern/diskann/DiskANN/src/ann_exception.cpp
    extern/diskann/DiskANN/src/disk_utils.cpp
    extern/diskann/DiskANN/src/distance.cpp
    extern/diskann/DiskANN/src/index.cpp
    extern/diskann/DiskANN/src/in_mem_graph_store.cpp
    extern/diskann/DiskANN/src/in_mem_data_store.cpp
    extern/diskann/DiskANN/src/local_file_reader.cpp
    extern/diskann/DiskANN/src/math_utils.cpp
    extern/diskann/DiskANN/src/natural_number_map.cpp
    extern/diskann/DiskANN/src/in_mem_data_store.cpp
    extern/diskann/DiskANN/src/in_mem_graph_store.cpp
    extern/diskann/DiskANN/src/natural_number_set.cpp
    extern/diskann/DiskANN/src/memory_mapper.cpp
    extern/diskann/DiskANN/src/partition.cpp
    extern/diskann/DiskANN/src/pq.cpp
    extern/diskann/DiskANN/src/pq_flash_index.cpp
    extern/diskann/DiskANN/src/scratch.cpp
    extern/diskann/DiskANN/src/logger.cpp
    extern/diskann/DiskANN/src/utils.cpp
    extern/diskann/DiskANN/src/filter_utils.cpp
    extern/diskann/DiskANN/src/index_factory.cpp
    extern/diskann/DiskANN/src/abstract_index.cpp
    )

# not working without FMA
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mfma -msse2 -ftree-vectorize -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -fopenmp -fopenmp-simd -funroll-loops -Wfatal-errors -DUSE_AVX2")

add_library(diskann STATIC ${DISKANN_SOURCES})
# work
if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64")
  target_compile_options(diskann PRIVATE -mavx -msse2 -ftree-vectorize -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -fopenmp -fopenmp-simd -funroll-loops -Wfatal-errors)
else ()
  target_compile_options(diskann PRIVATE -ftree-vectorize -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -fopenmp -fopenmp-simd -funroll-loops -Wfatal-errors)
endif ()
set_property(TARGET diskann PROPERTY CXX_STANDARD 17)
add_dependencies(diskann boost openblas)

if(CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64" AND ENABLE_INTEL_MKL)
  if(NOT MKL_PATH OR NOT OMP_PATH)
    add_dependencies(diskann mkl)
  endif()
endif()
target_link_libraries(diskann
  ${BLAS_LIBRARIES}
  gfortran
)

install (
  TARGETS diskann
  ARCHIVE DESTINATION lib
  )
