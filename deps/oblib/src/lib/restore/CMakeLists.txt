ob_set_subtarget(oblib_lib_restore common
  ob_storage.cpp
  ob_storage_file.cpp
#  ob_storage_oss_base.cpp
  ob_storage_path.cpp)

ob_lib_add_target(oblib_lib_restore)

#add_library(oss INTERFACE)

#target_include_directories(oss
#  INTERFACE
#  ${DEP_DIR}/include/apr-1
#  ${DEP_DIR}/include/oss_c_sdk
#  ${DEP_DIR}/include/mxml)
  
#target_link_libraries(oss
#  INTERFACE
#  ${DEP_DIR}/lib/liboss_c_sdk_static.a
#  ${DEP_DIR}/lib/libaprutil-1.a
#  ${DEP_DIR}/lib/libapr-1.a
#  ${DEP_DIR}/lib/libmxml.a)
  
#target_link_libraries(restore PUBLIC oss oblib_base)
