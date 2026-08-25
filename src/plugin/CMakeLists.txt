# Two plugin systems live under src/plugin/:
#   legacy/              - the original general plugin framework (SDK, manager, adaptors)
#   v2/                  - the next-gen plugin framework: generic loader + host
#                          capabilities at v2/, per-type contracts below (v2/external_table/)
include(${CMAKE_CURRENT_SOURCE_DIR}/legacy/CMakeLists.txt)
include(${CMAKE_CURRENT_SOURCE_DIR}/v2/CMakeLists.txt)

ob_server_add_target(ob_plugin)
