# v2 plugin framework: pieces usable by ANY v2 plugin type (not just external
# tables) — the dlopen loader plus host capabilities (mem / io) and the HostApi
# assembly (ob_ext_host_provider) the plugins consume through the callback table.
ob_set_subtarget(ob_plugin v2_common
  ${CMAKE_CURRENT_LIST_DIR}/loader/ob_ext_plugin_loader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/host/ob_ext_mem_pool.cpp
  ${CMAKE_CURRENT_LIST_DIR}/host/ob_ext_file_system.cpp
  ${CMAKE_CURRENT_LIST_DIR}/host/ob_ext_host_provider.cpp
)

# Plugin-type-specific contract + OB-side consumers (external table first).
include(${CMAKE_CURRENT_LIST_DIR}/external_table/CMakeLists.txt)
