# Define object library cos
add_library(cos_sdk_objs OBJECT
  cos_c_sdk/cos_api.h
  cos_c_sdk/cos_auth.c
  cos_c_sdk/cos_auth.h
  cos_c_sdk/cos_bucket.c
  cos_c_sdk/cos_buf.c
  cos_c_sdk/cos_buf.h
  cos_c_sdk/cos_crc64.c
  cos_c_sdk/cos_crc64.h
  cos_c_sdk/cos_define.c
  cos_c_sdk/cos_define.h
  cos_c_sdk/cos_fstack.c
  cos_c_sdk/cos_fstack.h
  cos_c_sdk/cos_http_io.c
  cos_c_sdk/cos_http_io.h
  cos_c_sdk/cos_list.h
  cos_c_sdk/cos_log.c
  cos_c_sdk/cos_log.h
  cos_c_sdk/cos_multipart.c
  cos_c_sdk/cos_object.c
  cos_c_sdk/cos_resumable.c
  cos_c_sdk/cos_resumable.h
  cos_c_sdk/cos_status.c
  cos_c_sdk/cos_status.h
  cos_c_sdk/cos_string.h
  cos_c_sdk/cos_string.c
  cos_c_sdk/cos_sys_define.h
  cos_c_sdk/cos_sys_util.c
  cos_c_sdk/cos_sys_util.h
  cos_c_sdk/cos_transport.c
  cos_c_sdk/cos_transport.h
  cos_c_sdk/cos_utility.c
  cos_c_sdk/cos_utility.h
  cos_c_sdk/cos_xml.c
  cos_c_sdk/cos_xml.h
  ob_singleton.h
  ob_cos_wrapper.h
  ob_cos_wrapper.cpp)

include_directories(./cos_c_sdk)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  # using GCC
  target_compile_options(cos_sdk_objs PRIVATE -fvisibility=hidden -Wno-unused-parameter -Wno-type-limits)
else ()
  # using Clang
  target_compile_options(cos_sdk_objs PRIVATE -fvisibility=hidden -Wno-error=unused-parameter -Wno-error=type-limits)
endif()

target_link_libraries(cos_sdk_objs PUBLIC oblib_base_base)

add_custom_command(
  OUTPUT cos_sdk_objs.o
  COMMAND ${OB_LD_BIN} -r $<TARGET_OBJECTS:cos_sdk_objs> -o cos_sdk_objs.tmp.o
  COMMAND objcopy --localize-hidden cos_sdk_objs.tmp.o cos_sdk_objs.o
  DEPENDS cos_sdk_objs
  BYPRODUCTS cos_sdk_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/cos_sdk_objs.o)
oblib_add_library(cos_sdk cos_sdk_objs.o)
