
set(name openblas)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
set(install_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/install)
ExternalProject_Add(
    ${name}
    URL https://github.com/OpenMathLib/OpenBLAS/releases/download/v0.3.23/OpenBLAS-0.3.23.tar.gz
        # this url is maintained by the vsag project, if it's broken, please try
        #  the latest commit or contact the vsag project
        http://vsagcache.oss-rg-china-mainland.aliyuncs.com/openblas/OpenBLAS-0.3.23.tar.gz
    URL_HASH MD5=115634b39007de71eb7e75cf7591dfb2
    DOWNLOAD_NAME OpenBLAS-v0.3.23.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND
        ${common_configure_envs}
        OMP_NUM_THREADS=1
        PATH=/usr/lib/ccache:$ENV{PATH}
        LD_LIBRARY_PATH=/opt/alibaba-cloud-compiler/lib64/:$ENV{LD_LIBRARY_PATH}
        make USE_THREAD=0 USE_LOCKING=1 -j${NUM_BUILDING_JOBS}
    INSTALL_COMMAND
        make PREFIX=${install_dir} install
    BUILD_IN_SOURCE 1
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
    DOWNLOAD_NO_PROGRESS 1
    INACTIVITY_TIMEOUT 5
    TIMEOUT 150
)

include_directories(${install_dir}/include)
link_directories (${install_dir}/lib)
link_directories (${install_dir}/lib64)

file(GLOB LIB_DIR_EXIST CHECK_DIRECTORIES LIST_DIRECTORIES true ${install_dir}/lib)
if(LIB_DIR_EXIST)
    file(GLOB LIB_FILES ${install_dir}/lib/lib*.a)
    foreach(lib_file ${LIB_FILES})
        install(FILES ${lib_file}
                DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
    )
    endforeach()
endif()

file(GLOB LIB64_DIR_EXIST CHECK_DIRECTORIES LIST_DIRECTORIES true ${install_dir}/lib64)
if(LIB64_DIR_EXIST)
    file(GLOB LIB64_FILES ${install_dir}/lib64/lib*.a)
    foreach(lib64_file ${LIB64_FILES})
        install(FILES ${lib64_file}
                DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
    )
    endforeach()
endif()
