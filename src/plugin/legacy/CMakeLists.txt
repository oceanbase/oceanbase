# Legacy plugin framework (SDK + plugin manager + adaptors + builtin external table plugins).
ob_set_subtarget(ob_plugin sys
  ${CMAKE_CURRENT_LIST_DIR}/sys/ob_plugin_builtin.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sys/ob_plugin_mgr.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sys/ob_plugin_handle.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sys/ob_plugin_entry_handle.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sys/ob_plugin_dl_handle.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sys/ob_plugin_utils.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sys/ob_plugin_load_param.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sys/ob_plugin_helper.cpp
)

ob_set_subtarget(ob_plugin adaptor
  ${CMAKE_CURRENT_LIST_DIR}/adaptor/ob_plugin_adaptor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/adaptor/ob_plugin_ftparser_adaptor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/adaptor/ob_plugin_kms_adaptor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/adaptor/ob_plugin_kms_client.cpp
)

ob_set_subtarget(ob_plugin share
  ${CMAKE_CURRENT_LIST_DIR}/share/ob_properties.cpp
)

ob_set_subtarget(ob_plugin external_table_arrow
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_arrow_object.cpp
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_arrow_status.cpp
)

ob_set_subtarget(ob_plugin external_table
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_builtin_register.cpp
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_filter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_format.cpp
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_java_plugin.cpp
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_jni_utils.cpp
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_struct.cpp
  ${CMAKE_CURRENT_LIST_DIR}/external_table/ob_external_table_schema.cpp
)

# Legacy plugin SDK implementations (obp_*). Exported from the observer binary
# via -Wl,--export-dynamic-symbol=obp_* for dlopen'd plugins to resolve.
add_library(ob_plugin_export_static
  STATIC
  ${CMAKE_CURRENT_LIST_DIR}/export/ob_plugin_charset_info.cpp
  ${CMAKE_CURRENT_LIST_DIR}/export/ob_plugin.cpp
  ${CMAKE_CURRENT_LIST_DIR}/export/ob_plugin_ftparser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/export/ob_plugin_allocator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/export/ob_plugin_kms.cpp
  ${CMAKE_CURRENT_LIST_DIR}/export/ob_plugin_log.cpp
)

target_link_libraries(ob_plugin_export_static PUBLIC ob_base)
