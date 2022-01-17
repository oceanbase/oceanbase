add_executable(ob_admin 
  clog_tool/cmd_args_parser.h
  clog_tool/ob_admin_clog_v2_executor.cpp
  clog_tool/ob_func_utils.cpp
  clog_tool/ob_ilog_entry_parser.cpp
  clog_tool/ob_log_entry_filter.cpp
  clog_tool/ob_log_entry_parser.cpp
  usec_tool/ob_admin_usec_executor.cpp
  slog_tool/ob_admin_slog_executor.cpp
  dumpsst/ob_admin_dumpsst_executor.cpp
  dumpsst/ob_admin_cmp_micro_executor.cpp
  dumpsst/ob_admin_dumpsst_utils.cpp
  dumpsst/ob_admin_dumpsst_print_helper.cpp
  archive_tool/ob_fake_archive_log_file_store.cpp
  archive_tool/ob_admin_log_archive_executor.cpp
  archive_tool/ob_archive_entry_parser.cpp
  archive_tool/ob_archive_fake_entry_iterator.cpp
  archive_tool/ob_archive_fake_file_store.cpp
  backup_tool/ob_admin_dump_backup_data_executor.cpp
  ob_admin_executor.cpp
  main.cpp)

set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -lmariadb -laio")
if (OB_STATIC_LINK_LGPL_DEPS)
   set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -l:libmariadbclient.a -l:libaio.a")
endif()

target_link_libraries(ob_admin
  PRIVATE
  oceanbase_static
  -static-libgcc
  -static-libstdc++
  ${LGPL_DEPS}
  )


target_include_directories(ob_admin
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR})
