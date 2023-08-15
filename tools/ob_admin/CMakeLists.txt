set(ob_admin_tde_files "")
if(OB_BUILD_TDE_SECURITY)
  set(ob_admin_tde_files "${CMAKE_SOURCE_DIR}/close_modules/tde_security/tools/ob_admin/dump_key/ob_admin_dump_key_executor.cpp")
endif()

if (OB_BUILD_OPENSOURCE)
add_executable(ob_admin
  #  clog_tool/cmd_args_parser.h
  #  clog_tool/ob_admin_clog_v2_executor.cpp
  #  clog_tool/ob_admin_clog_v2_executor.h
  #  clog_tool/ob_func_utils.cpp
  #  clog_tool/ob_func_utils.h
  #  clog_tool/ob_ilog_entry_parser.cpp
  #  clog_tool/ob_ilog_entry_parser.h
  #  clog_tool/ob_log_entry_filter.cpp
  #  clog_tool/ob_log_entry_filter.h
  #  clog_tool/ob_log_entry_parser.cpp
  #  clog_tool/ob_log_entry_parser.h

  #  dumpsst/ob_admin_cmp_micro_executor.cpp
  #  dumpsst/ob_admin_cmp_micro_executor.h
  dumpsst/ob_admin_dumpsst_executor.cpp
  dumpsst/ob_admin_dumpsst_executor.h
  dumpsst/ob_admin_dumpsst_print_helper.cpp
  dumpsst/ob_admin_dumpsst_print_helper.h
  io_bench/ob_admin_io_executor.cpp
  io_bench/ob_admin_io_executor.h
  main.cpp
  ob_admin_executor.cpp
  ob_admin_executor.h
  server_tool/ob_admin_routine.cpp
  server_tool/ob_admin_routine.h
  server_tool/ob_admin_server_executor.cpp
  server_tool/ob_admin_server_executor.h
  #  server_tool/ob_admin_utils.h
  slog_tool/ob_admin_slog_executor.cpp
  slog_tool/ob_admin_slog_executor.h
  dump_enum_value/ob_admin_dump_enum_value_executor.cpp
  dump_enum_value/ob_admin_dump_enum_value_executor.h
  dump_ckpt/ob_admin_dump_ckpt_executor.cpp
  dump_ckpt/ob_admin_dump_ckpt_executor.h

  #  archive_tool/ob_fake_archive_log_file_store.h
  #  archive_tool/ob_fake_archive_log_file_store.cpp
  #  archive_tool/ob_admin_log_archive_executor.h
  #  archive_tool/ob_admin_log_archive_executor.cpp
  #  archive_tool/ob_archive_entry_parser.h
  #  archive_tool/ob_archive_entry_parser.cpp
  #  archive_tool/ob_archive_fake_entry_iterator.h
  #  archive_tool/ob_archive_fake_entry_iterator.cpp
  #  archive_tool/ob_archive_fake_file_store.h
  #  archive_tool/ob_archive_fake_file_store.cpp

  backup_tool/ob_admin_dump_backup_data_executor.h
  backup_tool/ob_admin_dump_backup_data_executor.cpp

  #trx_tool/ob_admin_trx_executor.h
  #trx_tool/ob_admin_trx_executor.cpp

  log_tool/dump/ob_admin_dump_block.cpp
  log_tool/parser/ob_admin_parser_log_entry.cpp
  log_tool/parser/ob_admin_parser_group_entry.cpp
  log_tool/ob_admin_log_tool_executor.cpp
  ${ob_admin_tde_files}
  )

else()
add_executable(ob_admin
  #  clog_tool/cmd_args_parser.h
  #  clog_tool/ob_admin_clog_v2_executor.cpp
  #  clog_tool/ob_admin_clog_v2_executor.h
  #  clog_tool/ob_func_utils.cpp
  #  clog_tool/ob_func_utils.h
  #  clog_tool/ob_ilog_entry_parser.cpp
  #  clog_tool/ob_ilog_entry_parser.h
  #  clog_tool/ob_log_entry_filter.cpp
  #  clog_tool/ob_log_entry_filter.h
  #  clog_tool/ob_log_entry_parser.cpp
  #  clog_tool/ob_log_entry_parser.h

  #  dumpsst/ob_admin_cmp_micro_executor.cpp
  #  dumpsst/ob_admin_cmp_micro_executor.h
  dumpsst/ob_admin_dumpsst_executor.cpp
  dumpsst/ob_admin_dumpsst_executor.h
  dumpsst/ob_admin_dumpsst_print_helper.cpp
  dumpsst/ob_admin_dumpsst_print_helper.h
  io_bench/ob_admin_io_executor.cpp
  io_bench/ob_admin_io_executor.h
  main.cpp
  ob_admin_executor.cpp
  ob_admin_executor.h
  server_tool/ob_admin_routine.cpp
  server_tool/ob_admin_routine.h
  server_tool/ob_admin_server_executor.cpp
  server_tool/ob_admin_server_executor.h
  #  server_tool/ob_admin_utils.h
  slog_tool/ob_admin_slog_executor.cpp
  slog_tool/ob_admin_slog_executor.h

  dump_enum_value/ob_admin_dump_enum_value_executor.cpp
  dump_enum_value/ob_admin_dump_enum_value_executor.h
  dump_ckpt/ob_admin_dump_ckpt_executor.cpp
  dump_ckpt/ob_admin_dump_ckpt_executor.h

  #  archive_tool/ob_fake_archive_log_file_store.h
  #  archive_tool/ob_fake_archive_log_file_store.cpp
  #  archive_tool/ob_admin_log_archive_executor.h
  #  archive_tool/ob_admin_log_archive_executor.cpp
  #  archive_tool/ob_archive_entry_parser.h
  #  archive_tool/ob_archive_entry_parser.cpp
  #  archive_tool/ob_archive_fake_entry_iterator.h
  #  archive_tool/ob_archive_fake_entry_iterator.cpp
  #  archive_tool/ob_archive_fake_file_store.h
  #  archive_tool/ob_archive_fake_file_store.cpp

  backup_tool/ob_admin_dump_backup_data_executor.h
  backup_tool/ob_admin_dump_backup_data_executor.cpp

  #trx_tool/ob_admin_trx_executor.h
  #trx_tool/ob_admin_trx_executor.cpp

  log_tool/dump/ob_admin_dump_block.cpp
  log_tool/parser/ob_admin_parser_log_entry.cpp
  log_tool/parser/ob_admin_parser_group_entry.cpp
  log_tool/ob_admin_log_tool_executor.cpp
  ${ob_admin_tde_files}
  )

endif()

target_include_directories(ob_admin PRIVATE)
target_link_libraries(ob_admin
  PRIVATE
  -Wl,--start-group
  oceanbase_static
  ob_sql_static
  ob_storage_static
  ob_share_static
  ${ob_close_modules_static_name}
  -Wl,--end-group
  -static-libgcc
  -static-libstdc++)
