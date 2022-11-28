oblib_add_library(restore OBJECT
  ob_i_storage.h
  ob_storage.cpp
  ob_storage_file.cpp
  ob_storage_file.h
  ob_storage.h
  ob_storage_oss_base.cpp
  ob_storage_oss_base.h
  ob_storage_path.cpp
  ob_storage_path.h
  cos/ob_storage_cos_obj.cpp
  cos/ob_storage_cos_obj.h
  ob_storage_cos.cpp
  ob_storage_cos.h
  ob_object_device.cpp
  ob_object_device.h)

add_library(oss INTERFACE)
target_include_directories(oss
  INTERFACE
  ${DEP_DIR}/include/apr-1
  ${DEP_DIR}/include
  ${DEP_DIR}/include/oss_c_sdk
  ${DEP_DIR}/include/mxml
)

target_link_libraries(oss
  INTERFACE
  ${DEP_DIR}/lib/liboss_c_sdk_static.a
  ${DEP_DIR}/lib/libaprutil-1.a
  ${DEP_DIR}/lib/libapr-1.a
  ${DEP_DIR}/lib/libmxml.a)

add_subdirectory(cos)

target_link_libraries(restore PUBLIC oss cos_sdk oblib_base)
