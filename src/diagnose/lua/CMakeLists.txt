ob_set_subtarget(ob_diagnose_lua common
  ob_lua_api.cpp
  ob_lua_handler.cpp
)

ob_server_add_target(ob_diagnose_lua)

target_link_libraries(ob_diagnose_lua INTERFACE
  ${DEP_DIR}/lib/liblua.a)