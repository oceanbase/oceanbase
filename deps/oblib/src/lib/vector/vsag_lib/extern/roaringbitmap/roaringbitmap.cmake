include(FetchContent)
FetchContent_Declare(
		roaringbitmap
		URL https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v3.0.1.tar.gz
                    # this url is maintained by the vsag project, if it's broken, please try
                    #  the latest commit or contact the vsag project
		    http://vsagcache.oss-rg-china-mainland.aliyuncs.com/roaringbitmap/v3.0.1.tar.gz
		URL_HASH MD5=463db911f97d5da69393d4a3190f9201
                DOWNLOAD_NO_PROGRESS 0
                INACTIVITY_TIMEOUT 5
                TIMEOUT 150
)

set(ROARING_USE_CPM OFF)
set(ENABLE_ROARING_TESTS OFF)

if (DISABLE_AVX_FORCE OR NOT COMPILER_AVX_SUPPORTED)
  set(ROARING_DISABLE_AVX ON)
endif ()

if (DISABLE_AVX512_FORCE OR NOT COMPILER_AVX512_SUPPORTED)
  set (ROARING_DISABLE_AVX512 ON)
endif ()

# exclude roaringbitmap in vsag installation
FetchContent_GetProperties(roaringbitmap)
if(NOT roaringbitmap_POPULATED)
  FetchContent_Populate(roaringbitmap)
  add_subdirectory(${roaringbitmap_SOURCE_DIR} ${roaringbitmap_BINARY_DIR} EXCLUDE_FROM_ALL)
  target_compile_options(roaring PRIVATE -Wno-unused-function)
endif()
