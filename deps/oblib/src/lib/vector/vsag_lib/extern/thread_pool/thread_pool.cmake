
include(FetchContent)

FetchContent_Declare(
        thread_pool
        URL https://github.com/log4cplus/ThreadPool/archive/refs/heads/master.tar.gz
            # this url is maintained by the vsag project, if it's broken, please try
            #  the latest commit or contact the vsag project
            http://vsagcache.oss-rg-china-mainland.aliyuncs.com/thread_pool/master.tar.gz
            URL_HASH MD5=99f810ce40388f6e142e62d99c9b076a

        DOWNLOAD_NO_PROGRESS 1
        INACTIVITY_TIMEOUT 5
        TIMEOUT 30
)

FetchContent_MakeAvailable(thread_pool)
include_directories(${thread_pool_SOURCE_DIR}/)
