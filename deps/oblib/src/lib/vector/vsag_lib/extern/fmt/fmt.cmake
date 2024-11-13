
include(FetchContent)

# suppress "stringop-overflow" warning which caused by a compiler bug in gcc 10 or earlier
# ref: https://github.com/fmtlib/fmt/issues/2708
set (FMT_SYSTEM_HEADERS ON)

FetchContent_Declare(
    fmt
    URL https://github.com/fmtlib/fmt/archive/refs/tags/10.2.1.tar.gz
        # this url is maintained by the vsag project, if it's broken, please try
        #  the latest commit or contact the vsag project
        http://vsagcache.oss-rg-china-mainland.aliyuncs.com/fmt/10.2.1.tar.gz
    URL_HASH MD5=dc09168c94f90ea890257995f2c497a5
    DOWNLOAD_NO_PROGRESS 1
    INACTIVITY_TIMEOUT 5
    TIMEOUT 30
)

# exclude fmt in vsag installation
FetchContent_GetProperties(fmt)
if(NOT fmt_POPULATED)
  FetchContent_Populate(fmt)
  add_subdirectory(${fmt_SOURCE_DIR} ${fmt_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${fmt_SOURCE_DIR}/include)
