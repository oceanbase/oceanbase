
include(FetchContent)

FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/archive/refs/tags/v3.11.3.tar.gz
        # this url is maintained by the vsag project, if it's broken, please try
        #  the latest commit or contact the vsag project
        http://vsagcache.oss-rg-china-mainland.aliyuncs.com/nlohmann_json/v3.11.3.tar.gz
    URL_HASH MD5=d603041cbc6051edbaa02ebb82cf0aa9
    DOWNLOAD_NO_PROGRESS 1
    INACTIVITY_TIMEOUT 5
    TIMEOUT 30
)

FetchContent_MakeAvailable(nlohmann_json)
include_directories(${nlohmann_json_SOURCE_DIR}/include)
