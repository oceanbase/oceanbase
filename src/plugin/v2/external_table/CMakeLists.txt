# External-table format plugin consumers: OB-side registry / protocol / schema
# parsing for the contract header (../include/ob_external_table_plugin.h).
# The generic loader and mem/io host capabilities live one level up (v2/).
ob_set_subtarget(ob_plugin v2_external_table
  ${CMAKE_CURRENT_LIST_DIR}/ob_ext_format_registry.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ob_ext_type_mapper.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ob_ext_json_protocol.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ob_ext_schema_parser.cpp
)

# External-table-specific OB-side consumers: table metadata + contract destroy
# wrappers. (The HostApi assembly lives one level up in v2/host/.)
ob_set_subtarget(ob_plugin v2_external_table_host
  ${CMAKE_CURRENT_LIST_DIR}/ob_ext_plugin_util.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ob_ext_table_metadata.cpp
)
