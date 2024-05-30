execute_process(
  COMMAND bash gen_parser.sh
  RESULT_VARIABLE RET
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/src/pl/parser
  OUTPUT_FILE _gen_pl_parser.output
  ERROR_FILE _gen_parser.error)
if(RET AND NOT RET EQUAL 0)
  message(FATAL_ERROR "Generate pl parser failed and check error in ${PROJECT_SOURCE_DIR}/src/pl/parser/_gen_parser.error")
endif()

ob_set_subtarget(ob_pl ALONE
  parser/ob_pl_parser.cpp
  parser/pl_non_reserved_keywords_mysql_mode.c
  parser/pl_parser_base.c
  parser/pl_parser_mysql_mode_lex.c
  parser/pl_parser_mysql_mode_tab.c
)

ob_set_subtarget(ob_pl common
  ob_pl.cpp
  ob_pl_adt_service.cpp
  ob_pl_allocator.cpp
  ob_pl_code_generator.cpp
  ob_pl_compile.cpp
  ob_pl_di_adt_service.cpp
  ob_pl_exception_handling.cpp
  ob_pl_interface_pragma.cpp
  ob_pl_package.cpp
  ob_pl_package_manager.cpp
  ob_pl_package_state.cpp
  ob_pl_package_guard.cpp
  ob_pl_resolver.cpp
  ob_pl_router.cpp
  ob_pl_stmt.cpp
  ob_pl_type.cpp
  ob_pl_user_type.cpp
  ob_pl_persistent.cpp
)

ob_set_subtarget(ob_pl pl_cache
  pl_cache/ob_pl_cache.cpp
  pl_cache/ob_pl_cache_mgr.cpp
  pl_cache/ob_pl_cache_object.cpp
)

ob_set_subtarget(ob_pl sys_package
  sys_package/ob_dbms_external_table.cpp
  sys_package/ob_dbms_stats.cpp
  sys_package/ob_dbms_scheduler_mysql.cpp
  sys_package/ob_dbms_application.cpp
  sys_package/ob_dbms_session.cpp
  sys_package/ob_dbms_monitor.cpp
  sys_package/ob_dbms_upgrade.cpp
  sys_package/ob_dbms_sql.cpp
  sys_package/ob_dbms_user_define_rule.cpp
  sys_package/ob_dbms_mview_mysql.cpp
  sys_package/ob_dbms_mview_stats_mysql.cpp
  sys_package/ob_dbms_workload_repository.cpp
  sys_package/ob_pl_dbms_resource_manager.cpp
  sys_package/ob_pl_dbms_trusted_certificate_manager.cpp
  sys_package/ob_dbms_limit_calculator_mysql.cpp
)

ob_set_subtarget(ob_pl dblink
  dblink/ob_pl_dblink_guard.cpp
)

ob_server_add_target(ob_pl)
