ob_set_subtarget(obtable_base_object_list common
  libobtable.cpp
  ob_hkv_table.cpp
  ob_tablet_location_proxy.cpp
  ob_pstore.cpp
  ob_table_impl.cpp
  ob_table_rpc_impl.cpp
  ob_table_service_client.cpp
)

ob_add_new_object_target(obtable_base_objects obtable_base_object_list)

target_link_libraries(obtable_base_objects PUBLIC oceanbase)

add_library(obtable
  SHARED
  EXCLUDE_FROM_ALL)

target_link_libraries(obtable
  PUBLIC obtable_base_objects
  PRIVATE
  -Wl,--whole-archive
  obtable_base_objects
  -Wl,--no-whole-archive
  -Wl,--allow-multiple-definition)
set_target_properties(obtable PROPERTIES SOVERSION 1 VERSION 1.0.0)

add_library(obtable_static
  STATIC
  EXCLUDE_FROM_ALL
  ${CMAKE_BINARY_DIR}/src/observer/ob_version.cpp)
target_link_libraries(obtable_static PUBLIC ${obtable_base} ${ob_objects} ${oblib_object_libraries})
