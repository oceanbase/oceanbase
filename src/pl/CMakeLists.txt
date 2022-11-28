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
  ob_pl_error_log.cpp
  ob_pl_exception_handling.cpp
  ob_pl_interface_pragma.cpp
  ob_pl_lock.cpp
  ob_pl_package.cpp
  ob_pl_package_manager.cpp
  ob_pl_package_state.cpp
  ob_pl_package_guard.cpp
  ob_pl_resolver.cpp
  ob_pl_router.cpp
  ob_pl_stmt.cpp
  ob_pl_type.cpp
  ob_pl_user_type.cpp
  ob_pl_warning.cpp
)


ob_set_subtarget(ob_pl sys_package
  sys_package/ob_dbms_stats.cpp
  sys_package/ob_dbms_scheduler_mysql.cpp
  sys_package/ob_dbms_application.cpp
  sys_package/ob_dbms_session.cpp
  sys_package/ob_dbms_monitor.cpp
  sys_package/ob_dbms_upgrade.cpp
  sys_package/ob_dbms_sql.cpp
)

ob_server_add_target(ob_pl)
