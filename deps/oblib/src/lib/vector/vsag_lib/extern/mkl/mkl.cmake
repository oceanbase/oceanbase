# find_package(MKL CONFIG REQUIRED)

if (CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64" AND ENABLE_INTEL_MKL)
    set(POSSIBLE_OMP_PATHS "/opt/intel/oneapi/compiler/2024.2/lib/libiomp5.so;/opt/intel/oneapi/compiler/2024.2/lib/libiomp5.so;/opt/intel/oneapi/compiler/latest/linux/compiler/lib/intel64_lin/libiomp5.so;/usr/lib/x86_64-linux-gnu/libiomp5.so;/opt/intel/lib/intel64_lin/libiomp5.so;/opt/intel/compilers_and_libraries_2020.4.304/linux/compiler/lib/intel64_lin/libiomp5.so")
    foreach(POSSIBLE_OMP_PATH ${POSSIBLE_OMP_PATHS})
        if (EXISTS ${POSSIBLE_OMP_PATH})
            get_filename_component(OMP_PATH ${POSSIBLE_OMP_PATH} DIRECTORY)
        endif()
    endforeach()

    set(POSSIBLE_MKL_LIB_PATHS "/opt/intel/oneapi/mkl/latest/lib/libmkl_core.so;/opt/intel/oneapi/mkl/latest/lib/intel64/libmkl_core.so;/usr/lib/x86_64-linux-gnu/libmkl_core.so;/opt/intel/mkl/lib/intel64/libmkl_core.so")
    foreach(POSSIBLE_MKL_LIB_PATH ${POSSIBLE_MKL_LIB_PATHS})
        if (EXISTS ${POSSIBLE_MKL_LIB_PATH})
            get_filename_component(MKL_PATH ${POSSIBLE_MKL_LIB_PATH} DIRECTORY)
        endif()
    endforeach()

    set(POSSIBLE_MKL_INCLUDE_PATHS "/opt/intel/oneapi/mkl/latest/include;/usr/include/mkl;/opt/intel/mkl/include/;")
    foreach(POSSIBLE_MKL_INCLUDE_PATH ${POSSIBLE_MKL_INCLUDE_PATHS})
        if (EXISTS ${POSSIBLE_MKL_INCLUDE_PATH})
            set(MKL_INCLUDE_PATH ${POSSIBLE_MKL_INCLUDE_PATH})
        endif()
    endforeach()

    if(NOT MKL_PATH OR NOT OMP_PATH)
        message(FATAL_ERROR "Could not find Intel OMP or MKL in standard locations, disable intel-mkl or install intel-mkl")
    else()
        if (EXISTS ${MKL_PATH}/libmkl_def.so.2)
            set(MKL_DEF_SO ${MKL_PATH}/libmkl_def.so.2)
        elseif(EXISTS ${MKL_PATH}/libmkl_def.so)
            set(MKL_DEF_SO ${MKL_PATH}/libmkl_def.so)
        else()
            message(FATAL_ERROR "Despite finding MKL, libmkl_def.so was not found in expected locations.")
        endif()
        add_library(mkl UNKNOWN IMPORTED)
        set_target_properties(mkl PROPERTIES
            IMPORTED_LOCATION ${MKL_PATH}
            INTERFACE_INCLUDE_DIRECTORIES ${MKL_INCLUDE_PATH}
        )
    endif()

    link_directories (${MKL_PATH})
    include_directories (${MKL_INCLUDE_PATH})
    set (BLAS_LIBRARIES
        ${MKL_PATH}/libmkl_intel_lp64.so
        ${MKL_PATH}/libmkl_sequential.so
        ${MKL_PATH}/libmkl_core.so
        ${MKL_PATH}/libmkl_def.so
        ${MKL_PATH}/libmkl_avx2.so
        ${MKL_PATH}/libmkl_mc3.so
        ${MKL_PATH}/libmkl_gf_lp64.so
        ${MKL_PATH}/libmkl_core.so
        ${MKL_PATH}/libmkl_intel_thread.so
        ${OMP_PATH}/libiomp5.so
    )

    foreach(mkllib ${BLAS_LIBRARIES})
        install(FILES ${mkllib} DESTINATION ${CMAKE_INSTALL_LIBDIR})
    endforeach()
    message ("enable intel-mkl as blas backend")
else ()
    set(BLAS_LIBRARIES libopenblas.a)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang") 
        list(PREPEND BLAS_LIBRARIES omp)
    else() 
        list(PREPEND BLAS_LIBRARIES gomp)
    endif()
    message ("enable openblas as blas backend")
endif ()
