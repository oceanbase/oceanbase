cmake_minimum_required(VERSION 3.20)
project(easy
  VERSION 1.1.22
  LANGUAGES C CXX)

message(STATUS "check deps for libeasy")
if (NOT DEP_DIR)
  message(FATAL_ERROR "DEP_DIR not set")
endif()


add_library(easy_base INTERFACE)

set(INNER_INCLUDE_DIRS
  ${CMAKE_CURRENT_SOURCE_DIR}/src/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src/
)

set_property(GLOBAL PROPERTY EASY_INCLUDE_DIRS
  ${INNER_INCLUDE_DIRS}
)

if(OB_USE_BABASSL)
  target_include_directories(
    easy_base INTERFACE
    ${INNER_INCLUDE_DIRS}
    ${DEP_3RD_DIR}/usr/local/babassl-ob/include
  )
else()
  target_include_directories(
    easy_base INTERFACE
    ${INNER_INCLUDE_DIRS}
    ${DEP_DIR}/include
  )
endif()


if (OB_USE_CLANG)
  set(EASY_CC_WNO -fno-strict-aliasing -Wno-unused-variable -Wno-unused-function -fmax-type-align=8
  -Wno-tautological-constant-out-of-range-compare)
else()
  set(EASY_CC_WNO -fno-strict-aliasing -Wno-unused-variable -Wno-implicit-function-declaration 
  -Wno-unused-but-set-variable -Wno-unused-function)
endif()


target_compile_options(easy_base
  INTERFACE
  -D_GLIBCXX_USE_CXX11_ABI=0 -D_GNU_SOURCE -D__STDC_LIMIT_MACROS
  ${MARCH_CFLAGS} ${MTUNE_CFLAGS}
  $<$<COMPILE_LANGUAGE:C>:-g -Wall -Werror -fPIC ${EASY_CC_WNO}>
)

target_link_libraries(easy_base
  INTERFACE
  -L${DEP_DIR}/lib
  -L${DEP_3RD_DIR}/usr/local/babassl-ob/lib/
  -lpthread -lm -l:libssl.a -l:libcrypto.a
  ${ARCH_LDFLAGS}
)


target_compile_features(easy_base INTERFACE cxx_std_11)

add_subdirectory(src)
add_subdirectory(test EXCLUDE_FROM_ALL)
