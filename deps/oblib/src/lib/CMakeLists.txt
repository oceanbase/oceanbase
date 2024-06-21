add_subdirectory(compress)
add_subdirectory(restore)

ob_set_subtarget(oblib_lib ALONE
  timezone/ob_timezone_util.cpp
  geo/ob_geo_func_difference.cpp
  geo/ob_geo_func_covered_by.cpp
  geo/ob_geo_func_within.cpp
  geo/ob_geo_func_union.cpp
  geo/ob_geo_func_touches.cpp
  geo/ob_geo_func_equals.cpp
  geo/ob_geo_func_intersects.cpp
  geo/ob_geo_func_symdifference.cpp
  geo/ob_geo_func_distance.cpp
  geo/ob_geo_func_crosses.cpp
  geo/ob_geo_func_overlaps.cpp
)

ob_set_subtarget(oblib_lib charset
  charset/ob_ctype_bin.cc
  charset/ob_ctype.cc
  charset/ob_ctype_gb18030.cc
  charset/ob_ctype_gbk.cc
  charset/ob_ctype_latin1.cc
  charset/ob_ctype_mb.cc
  charset/ob_ctype_simple.cc
  charset/ob_ctype_uca.cc
  charset/ob_ctype_utf8.cc
  charset/ob_ctype_utf16.cc
  charset/ob_dtoa.cc
  charset/uca900_ja_tbls.cc
  charset/uca900_zh_tbls.cc
  charset/uca900_zh2_tbls.cc
  charset/uca900_zh3_tbls.cc
  charset/ob_charset.cpp
)

ob_set_subtarget(oblib_lib common
  ob_abort.cpp
  ob_date_unit_type.cpp
  ob_define.cpp
  ob_lib_config.cpp
  ob_name_id_def.cpp
  ob_replica_define.cpp
  ob_running_mode.cpp
  runtime.cpp
  worker.cpp
)

ob_set_subtarget(oblib_lib geo
  geo/ob_geo_common.cpp
  geo/ob_geo_bin.cpp
  geo/ob_geo_ibin.cpp
  geo/ob_geo_bin_iter.ipp
  geo/ob_geo_tree.cpp
  geo/ob_s2adapter.cpp
  geo/ob_geo.cpp
  geo/ob_wkt_parser.cpp
  geo/ob_geo_utils.cpp
  geo/ob_srs_wkt_parser.cpp
  geo/ob_srs_info.cpp
  geo/ob_geo_visitor.cpp
  geo/ob_geo_wkb_size_visitor.cpp
  geo/ob_geo_wkb_visitor.cpp
  geo/ob_geo_coordinate_range_visitor.cpp
  geo/ob_geo_latlong_check_visitor.cpp
  geo/ob_geo_longtitude_correct_visitor.cpp
  geo/ob_geo_to_tree_visitor.cpp
  geo/ob_geo_to_s2_visitor.cpp
  geo/ob_geo_to_wkt_visitor.cpp
  geo/ob_geo_reverse_coordinate_visitor.cpp
  geo/ob_geo_normalize_visitor.cpp
  geo/ob_geo_denormalize_visitor.cpp
  geo/ob_geo_wkb_check_visitor.cpp
  geo/ob_geo_zoom_in_visitor.cpp
  geo/ob_geo_func_envelope.cpp
  geo/ob_geo_func_area.cpp
  geo/ob_geo_func_correct.cpp
  geo/ob_geo_func_disjoint.cpp
  geo/ob_geo_func_utils.cpp
  geo/ob_geo_func_distance_sphere.cpp
  geo/ob_geo_func_transform.cpp
  geo/ob_geo_func_box.cpp
  geo/ob_geo_func_buffer.cpp
  geo/ob_geo_func_isvalid.cpp
  geo/ob_geometry_cast.cpp
  geo/ob_sdo_geo_func_to_wkb.cpp
  geo/ob_wkb_to_sdo_geo_visitor.cpp
  geo/ob_wkb_to_json_visitor.cpp
  geo/ob_wkb_byte_order_visitor.cpp
  geo/ob_geo_3d.cpp
  geo/ob_sdo_geo_object.cpp
  geo/ob_geo_func_centroid.cpp
  geo/ob_geo_interior_point_visitor.cpp
  geo/ob_geo_box_clip_visitor.cpp
  geo/ob_geo_elevation_visitor.cpp
  geo/ob_geo_func_length.cpp
  geo/ob_geo_mvt_encode_visitor.cpp
  geo/ob_geo_mvt.cpp
  geo/ob_vector_tile.pb-c.c
  geo/ob_geo_affine_visitor.cpp
  geo/ob_geo_grid_visitor.cpp
  geo/ob_geo_simplify_visitor.cpp
  geo/ob_geo_box_clip_visitor.cpp
  geo/ob_geo_func_dissolve_polygon.cpp
  geo/ob_geo_cache.cpp
  geo/ob_geo_vertex_collect_visitor.cpp
  geo/ob_geo_segment_collect_visitor.cpp
  geo/ob_geo_cache_polygon.cpp
  geo/ob_geo_cache_point.cpp
  geo/ob_geo_cache_linestring.cpp
  geo/ob_geo_segment_intersect_analyzer.cpp
  geo/ob_geo_topology_calculate.cpp
  geo/ob_point_location_analyzer.cpp
  geo/ob_geo_point_location_visitor.cpp
)

ob_set_subtarget(oblib_lib roaringbitmap
  roaringbitmap/ob_roaringbitmap.cpp
  roaringbitmap/ob_rb_utils.cpp
  roaringbitmap/ob_rb_bin.cpp
)

ob_set_subtarget(oblib_lib encode
  encode/ob_base64_encode.cpp
  encode/ob_quoted_printable_encode.cpp
  encode/ob_uuencode.cpp
)

ob_set_subtarget(oblib_lib codec
  codec/ob_fast_bp_func.cpp
  codec/ob_generated_scalar_bp_func.cpp
)

ob_set_subtarget(oblib_lib_simd codec_simd
  codec/ob_fast_delta.cpp
  codec/ob_generated_unalign_simd_bp_func.cpp
)

ob_lib_add_target(oblib_lib_simd)

if (${ARCHITECTURE} STREQUAL "x86_64")
  target_compile_options(oblib_lib_simd
    PRIVATE -mavx2 -mssse3
    )
endif()

ob_set_subtarget(oblib_lib common_mixed
  atomic/ob_atomic_reference.cpp
  checksum/ob_crc64.cpp
  checksum/ob_parity_check.cpp
  container/ob_vector.ipp
  cpu/ob_cpu_topology.cpp
  encode/ob_base64_encode.cpp
  encode/ob_quoted_printable_encode.cpp
  encode/ob_uuencode.cpp
  encrypt/ob_encrypted_helper.cpp
  file/file_directory_utils.cpp
  file/ob_file.cpp
  file/ob_string_util.cpp
  hash/ob_concurrent_hash_map.cpp
  hash/ob_hashutils.cpp
  hash/xxhash.c
  hash_func/murmur_hash.cpp
  lob/ob_lob_base.cpp
  json/ob_json.cpp
  json/ob_json_print_utils.cpp
  json/ob_yson.cpp
  json_type/ob_json_path.cpp
  json_type/ob_json_tree.cpp
  json_type/ob_json_bin.cpp
  json_type/ob_json_base.cpp
  json_type/ob_json_parse.cpp
  json_type/ob_json_schema.cpp
  json_type/ob_json_diff.cpp
  lds/ob_lds_define.cpp
  net/ob_addr.cpp
  net/ob_net_util.cpp
  number/ob_number_v2.cpp
  objectpool/ob_pool.ipp
  objectpool/ob_server_object_pool.cpp
  profile/ob_atomic_event.cpp
  profile/ob_perf_event.cpp
  profile/ob_profile_log.cpp
  profile/ob_trace_id.cpp
  profile/ob_trace_id_adaptor.cpp
  queue/ob_dedup_queue.cpp
  queue/ob_lighty_queue.cpp
  queue/ob_link_queue.cpp
  queue/ob_ms_queue.cpp
  random/ob_random.cpp
  rc/context.cpp
  rc/ob_rc.cpp
  rowid/ob_urowid.cpp
  stat/ob_di_tls.cpp
  stat/ob_di_cache.cpp
  stat/ob_diagnose_info.cpp
  stat/ob_latch_define.cpp
  stat/ob_session_stat.cpp
  stat/ob_stat_template.cpp
  statistic_event/ob_stat_event.cpp
  string/ob_hex_utils_base.cpp
  string/ob_sql_string.cpp
  string/ob_string.cpp
  string/ob_strings.cpp
  string/ob_string_buffer.cpp
  task/ob_timer.cpp
  task/ob_timer_monitor.cpp
  thread_local/ob_tsi_utils.cpp
  thread_local/thread_buffer.cpp
  time/Time.cpp
  time/ob_cur_time.cpp
  time/ob_time_utility.cpp
  time/ob_tsc_timestamp.cpp
  timezone/ob_oracle_format_models.cpp
  timezone/ob_time_convert.cpp
  timezone/ob_timezone_info.cpp
  trace/ob_trace.cpp
  trace/ob_trace_event.cpp
  wait_event/ob_wait_class.cpp
  wait_event/ob_wait_event.cpp
  wide_integer/ob_wide_integer.cpp
  wide_integer/ob_wide_integer_cmp_funcs.cpp
  wide_integer/ob_wide_integer_str_funcs.cpp
  udt/ob_udt_type.cpp
  xml/ob_mul_mode_reader.cpp
  xml/ob_xml.cpp
  xml/ob_xml_parser.cpp
  xml/ob_libxml2_sax_handler.cpp
  xml/ob_tree_base.cpp
  xml/ob_xml_tree.cpp
  xml/ob_xml_util.cpp
  xml/ob_xpath.cpp
  xml/ob_multi_mode_bin.cpp
  xml/ob_xml_bin.cpp
  xml/ob_multi_mode_interface.cpp
  xml/ob_path_parser.cpp
  xml/ob_binary_aggregate.cpp
  locale/ob_locale_type.cc
  locale/ob_locale.cpp
)

ob_set_subtarget(oblib_lib lock
  lock/cond.cpp
  lock/mutex.cpp
  lock/ob_bucket_lock.cpp
  lock/ob_latch.cpp
  lock/ob_thread_cond.cpp
  lock/ob_rwlock.cpp
  lock/ob_futex.cpp
  lock/ob_bucket_qsync_lock.cpp
  lock/ob_qsync_lock.cpp
)

ob_set_subtarget(oblib_lib mysqlclient
  mysqlclient/ob_isql_client.cpp
  mysqlclient/ob_dblink_error_trans.cpp
  mysqlclient/ob_mysql_connection.cpp
  mysqlclient/ob_mysql_connection_pool.cpp
  mysqlclient/ob_mysql_prepared_param.cpp
  mysqlclient/ob_mysql_prepared_result.cpp
  mysqlclient/ob_mysql_prepared_statement.cpp
  mysqlclient/ob_mysql_proxy.cpp
  mysqlclient/ob_mysql_proxy_util.cpp
  mysqlclient/ob_mysql_result.cpp
  mysqlclient/ob_mysql_result_impl.cpp
  mysqlclient/ob_mysql_statement.cpp
  mysqlclient/ob_mysql_transaction.cpp
  mysqlclient/ob_server_connection_pool.cpp
  mysqlclient/ob_single_connection_proxy.cpp
  mysqlclient/ob_single_mysql_connection_pool.cpp
)

ob_set_subtarget(oblib_lib oblog
  oblog/ob_async_log_struct.cpp
  oblog/ob_base_log_buffer.cpp
  oblog/ob_base_log_writer.cpp
  oblog/ob_easy_log.cpp
  oblog/ob_log.cpp
  oblog/ob_log_compressor.cpp
  oblog/ob_log_dba_event.cpp
  oblog/ob_log_module.ipp
  oblog/ob_log_time_fmt.cpp
  oblog/ob_trace_log.cpp
  oblog/ob_warning_buffer.cpp
  oblog/ob_syslog_rate_limiter.cpp
)

ob_set_subtarget(oblib_lib signal
  signal/ob_signal_handlers.cpp
  signal/ob_signal_processor.cpp
  signal/ob_signal_struct.cpp
  signal/ob_signal_utils.cpp
  signal/ob_signal_worker.cpp
  signal/ob_libunwind.c
)

ob_set_subtarget(oblib_lib thread
  thread/ob_async_task_queue.cpp
  thread/ob_dynamic_thread_pool.cpp
  thread/ob_reentrant_thread.cpp
  thread/ob_simple_thread_pool.cpp
  thread/ob_map_queue_thread_pool.cpp
  thread/ob_work_queue.cpp
  thread/thread_mgr.cpp
  thread/thread_mgr_interface.cpp
  thread/protected_stack_allocator.cpp
  thread/thread.cpp
  thread/threads.cpp
  thread/ob_tenant_hook.cpp
  thread/ob_pthread.cpp
)

ob_set_subtarget(oblib_lib utility
  utility/ob_fast_convert.cpp
  utility/ob_hang_fatal_error.cpp
  utility/ob_print_utils.cpp
  utility/ob_serialization_helper.cpp
  utility/ob_tracepoint.cpp
  utility/ob_utility.cpp
  utility/utility.cpp
  utility/ob_backtrace.cpp
  utility/ob_proto_trans_util.cpp
  utility/ob_unify_serialize.cpp
  utility/ob_rpc_authentication_utility.cpp
)

ob_set_subtarget(oblib_lib ash
  ash/ob_active_session_guard.cpp
)

ob_set_subtarget(oblib_lib ssl
  ssl/ob_ssl_config.cpp
)

ob_set_subtarget(oblib_lib vtoa
  vtoa/ob_vtoa_util.cpp
)

ob_lib_add_target(oblib_lib)

ob_set_subtarget(oblib_lib_bitmap common
  container/ob_bitmap.cpp
)

ob_lib_add_target(oblib_lib_bitmap)

ob_set_subtarget(ob_malloc_object_list common_alloc
  alloc/abit_set.cpp
  alloc/alloc_assist.cpp
  alloc/alloc_failed_reason.cpp
  alloc/alloc_func.cpp
  alloc/alloc_struct.cpp
  alloc/block_set.cpp
  alloc/memory_dump.cpp
  alloc/ob_malloc_allocator.cpp
  alloc/ob_malloc_callback.cpp
  alloc/ob_malloc_sample_struct.cpp
  alloc/ob_malloc_time_monitor.cpp
  alloc/ob_tenant_ctx_allocator.cpp
  alloc/object_mgr.cpp
  alloc/object_set.cpp
  alloc/memory_sanity.cpp
  alloc/ob_futex_v2.cpp
  alloc/ob_latch_v2.cpp
  resource/achunk_mgr.cpp
  resource/ob_resource_mgr.cpp
  allocator/ob_allocator_v2.cpp
  allocator/ob_block_alloc_mgr.cpp
  allocator/ob_concurrent_fifo_allocator.cpp
  allocator/ob_ctx_define.cpp
  allocator/ob_delay_free_allocator.cpp
  allocator/ob_fifo_allocator.cpp
  allocator/ob_hazard_ref.cpp
  allocator/ob_libeasy_mem_pool.cpp
  allocator/ob_malloc.cpp
  allocator/ob_mem_leak_checker.cpp
  allocator/ob_mod_define.cpp
  allocator/ob_page_manager.cpp
  allocator/ob_slice_alloc.cpp
  allocator/ob_sql_mem_leak_checker.cpp
  allocator/ob_tc_malloc.cpp
)

ob_add_new_object_target(ob_malloc_object ob_malloc_object_list)
target_link_libraries(ob_malloc_object oblib_base_base_base)
add_library(ob_malloc STATIC)
target_link_libraries(ob_malloc ob_malloc_object)

add_library(malloc_hook STATIC
  alloc/malloc_hook.cpp
  alloc/malloc_hook.h)
target_link_libraries(malloc_hook oblib_base)

if(OB_BUILD_CLOSE_MODULES)
  target_link_libraries(oblib_lib PUBLIC
  ${DEP_3RD_DIR}/usr/local/lib/libxslt.a
  ${DEP_3RD_DIR}/usr/local/lib/libexslt.a
  ${ob_close_deps_static_name}
  )
endif()

target_link_libraries(oblib_lib
  PUBLIC ob_malloc compress restore
  ${DEP_DIR}/lib/libxml2.a
  ${DEP_DIR}/lib/liblzma.a
)
