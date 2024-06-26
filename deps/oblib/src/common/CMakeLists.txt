ob_set_subtarget(oblib_common common
  data_buffer.cpp
  ob_accuracy.cpp
  ob_balance_filter.cpp
  ob_clock_generator.cpp
  ob_common_utility.cpp
  ob_field.cpp
  ob_member.cpp
  ob_member_list.ipp
  ob_learner_list.ipp
  ob_queue_thread.cpp
  ob_range.cpp
  ob_record_header.cpp
  ob_role.cpp
  ob_segmented_buffer.cpp
  ob_server_ext.cpp
  ob_smart_call.cpp
  ob_smart_var.cpp
  ob_store_format.cpp
  ob_store_range.cpp
  ob_string_buf.ipp
  ob_tablet_id.cpp
  ob_target_specific.cpp
  ob_timeout_ctx.cpp
  ob_zone_type.cpp
  ob_version_def.cpp
  ob_tenant_data_version_mgr.cpp
)

ob_set_subtarget(oblib_common common_mixed
  cell/ob_cell_reader.cpp
  cell/ob_cell_writer.cpp
  object/ob_obj_compare.cpp
  object/ob_obj_type.cpp
  object/ob_object.cpp
  row/ob_row.cpp
  row/ob_row_checksum.cpp
  row/ob_row_desc.cpp
  row/ob_row_store.cpp
  row/ob_row_util.cpp
  rowkey/ob_rowkey.cpp
  rowkey/ob_rowkey_info.cpp
  rowkey/ob_store_rowkey.cpp
  sql_mode/ob_sql_mode_utils.cpp
)

ob_set_subtarget(oblib_common log
  log/ob_direct_log_reader.cpp
  log/ob_log_cursor.cpp
  log/ob_log_data_writer.cpp
  log/ob_log_dir_scanner.cpp
  log/ob_log_entry.cpp
  log/ob_log_generator.cpp
  log/ob_log_reader.cpp
  log/ob_single_log_reader.cpp
)

ob_set_subtarget(oblib_common storage
  storage/ob_fd_simulator.cpp
  storage/ob_freeze_define.cpp
  storage/ob_io_device.cpp
  storage/ob_sequence.cpp
)

if (OB_ERRSIM)
  ob_set_subtarget(oblib_common errsim_module
    errsim_module/ob_errsim_module_type.cpp
    errsim_module/ob_tenant_errsim_event.cpp
    errsim_module/ob_errsim_module_interface.cpp
  )
endif()

ob_lib_add_target(oblib_common)
