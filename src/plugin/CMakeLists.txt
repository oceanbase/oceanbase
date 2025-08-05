#SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/ob_plugin_builtin.cpp PROPERTIES GENERATED 1)
ob_set_subtarget(ob_plugin sys
  sys/ob_plugin_builtin.cpp
  sys/ob_plugin_mgr.cpp
  sys/ob_plugin_handle.cpp
  sys/ob_plugin_entry_handle.cpp
  sys/ob_plugin_dl_handle.cpp
  sys/ob_plugin_utils.cpp
  sys/ob_plugin_load_param.cpp
  sys/ob_plugin_helper.cpp
)

ob_set_subtarget(ob_plugin adaptor
  adaptor/ob_plugin_adaptor.cpp
  adaptor/ob_plugin_ftparser_adaptor.cpp
)

ob_set_subtarget(ob_plugin share
  share/ob_properties.cpp
)

ob_set_subtarget(ob_plugin external_table_arrow
  external_table/ob_external_arrow_data_loader.cpp
  external_table/ob_external_arrow_object.cpp
  external_table/ob_external_arrow_status.cpp
)

ob_set_subtarget(ob_plugin external_table
  external_table/ob_external_builtin_register.cpp
  external_table/ob_external_filter.cpp
  external_table/ob_external_format.cpp
  external_table/ob_external_java_plugin.cpp
  external_table/ob_external_jni_utils.cpp
  external_table/ob_external_struct.cpp
  external_table/ob_external_table_schema.cpp
  external_table/ob_plugin_external_table_row_iter.cpp
)

ob_server_add_target(ob_plugin)

add_library(ob_plugin_export_static
  STATIC
  export/ob_plugin_charset_info.cpp
  export/ob_plugin.cpp
  export/ob_plugin_ftparser.cpp
  export/ob_plugin_allocator.cpp
  export/ob_plugin_log.cpp
)

target_link_libraries(ob_plugin_export_static PUBLIC ob_base)
