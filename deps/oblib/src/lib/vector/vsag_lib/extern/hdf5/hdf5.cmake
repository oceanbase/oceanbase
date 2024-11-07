
include(ExternalProject)

set(name hdf5)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
set(install_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/install)
ExternalProject_Add(
    ${name}
    URL https://github.com/HDFGroup/hdf5/archive/refs/tags/hdf5_1.14.4.tar.gz
        # this url is maintained by the vsag project, if it's broken, please try
        #  the latest commit or contact the vsag project
        http://vsagcache.oss-rg-china-mainland.aliyuncs.com/hdf5/hdf5_1.14.4.tar.gz
    URL_HASH MD5=fdea52afcce07ed6c3e2a36e7fa11f21
    DOWNLOAD_NAME hdf5_1.14.4.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND
        cmake ${common_cmake_args} -DHDF5_ENABLE_NONSTANDARD_FEATURE_FLOAT16=OFF -DCMAKE_INSTALL_PREFIX=${install_dir} -DHDF5_BUILD_CPP_LIB=ON -S. -Bbuild
    BUILD_COMMAND
        cmake --build build --target install --parallel ${NUM_BUILDING_JOBS}
    INSTALL_COMMAND
        ""
    BUILD_IN_SOURCE 1
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
    DOWNLOAD_NO_PROGRESS 1
    INACTIVITY_TIMEOUT 5
    TIMEOUT 150

    BUILD_BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/${name}/build/bin/libhdf5_cpp.a
        ${CMAKE_CURRENT_BINARY_DIR}/${name}/build/bin/libhdf5.a
)

include_directories (${install_dir}/include)
link_directories (${install_dir}/lib)
