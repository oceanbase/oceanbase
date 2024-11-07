
set(name spdlog)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
set(install_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/install)
ExternalProject_Add(
    ${name}
    URL https://github.com/gabime/spdlog/archive/refs/tags/v1.12.0.tar.gz
        # this url is maintained by the vsag project, if it's broken, please try
        #  the latest commit or contact the vsag project
        http://vsagcache.oss-rg-china-mainland.aliyuncs.com/spdlog/v1.12.0.tar.gz
    URL_HASH MD5=6b4446526264c1d1276105482adc18d1
    DOWNLOAD_NAME spdlog-1.12.0.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND
    	cmake -DCMAKE_INSTALL_PREFIX=${install_dir} -S. -Bbuild
    BUILD_COMMAND
	cmake --build build --target install --parallel 4
    INSTALL_COMMAND
        ""
    BUILD_IN_SOURCE 1
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
    DOWNLOAD_NO_PROGRESS 1
    INACTIVITY_TIMEOUT 5
    TIMEOUT 30
)
