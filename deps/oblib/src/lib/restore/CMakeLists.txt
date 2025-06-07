add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)
oblib_add_library(restore OBJECT
  ob_i_storage.h
  ob_i_storage.cpp
  ob_storage.cpp
  ob_storage_file.cpp
  ob_storage_file.h
  ob_storage.h
  ob_storage_oss_base.cpp
  ob_storage_oss_base.h
  ob_storage_obdal_base.h
  ob_storage_obdal_base.cpp
  ob_storage_path.cpp
  ob_storage_path.h
  ob_storage_info.cpp
  ob_storage_info.h
  hdfs/ob_storage_hdfs_cache.cpp
  hdfs/ob_storage_hdfs_cache.h
  hdfs/ob_storage_hdfs_jni_base.cpp
  hdfs/ob_storage_hdfs_jni_base.h
  ob_object_device.cpp
  hmac_signature.cpp
  ob_object_device.h
  ob_object_storage_base.cpp)

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

oblib_add_library(s3 ob_storage_s3_base.cpp)
target_link_directories(s3 PUBLIC ${DEP_3RD_DIR}/usr/local/oceanbase/deps/devel/lib64)
target_link_libraries(s3
  PUBLIC
  libaws-cpp-sdk-s3.a
  libaws-cpp-sdk-core.a
  libaws-crt-cpp.a
  libaws-c-mqtt.a
  libaws-c-event-stream.a
  libaws-c-s3.a
  libaws-c-auth.a
  libaws-c-http.a
  libaws-c-io.a
  libs2n.a
  libaws-c-compression.a
  libaws-c-cal.a
  libaws-c-sdkutils.a
  libaws-checksums.a
  libaws-c-common.a
)
target_link_libraries(s3 PUBLIC oblib_base_without_pass)

execute_process(
  COMMAND sh -c "rm -rf libopendal && mkdir libopendal"
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
execute_process(
  COMMAND sh -c "cp ${DEP_DIR}/lib/libopendal_c.a libopendal"
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
execute_process(
  COMMAND sh -c "cd libopendal && ar x libopendal_c.a && ${OB_LD_BIN} -r *.o -o ../libopendal.o"
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
execute_process(
  COMMAND sh -c "cd libopendal && find . -maxdepth 1 -name '*.o' -print0 | xargs -0 -I {} nm {} | awk '$2 == \"T\" || $2 == \"D\" || $2 == \"B\" || $2 == \"R\" {print $3}' | grep -v -E 'opendal|ob_new_span|ob_drop_span' > ../symbol.txt"
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_command(
  OUTPUT opendal.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-symbols=symbol.txt libopendal.o
  COMMAND ${OB_OBJCOPY_BIN} --localize-hidden libopendal.o opendal.o
  COMMAND rm -rf libopendal
  DEPENDS ${DEP_DIR}/lib/libopendal_c.a
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Generating opendal.o"
  VERBATIM
)

add_library(opendal opendal.o)
set_target_properties(opendal PROPERTIES LINKER_LANGUAGE CXX)

oblib_add_library(obdal OBJECT obdal/obdal_accessor.cpp)
target_link_libraries(obdal PUBLIC oblib_base_without_pass opendal)


target_link_libraries(restore PUBLIC oss oblib_base s3 obdal)