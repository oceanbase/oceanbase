add_library(obcdc_base INTERFACE)
target_include_directories(obcdc_base INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(obcdc_base
  INTERFACE ob_base oceanbase::oblogmsg_static easy oblib
  ${DEP_DIR}/lib64/librocksdb.a
  )

set(SRC_LIST
  libobcdc.h libobcdc.cpp
  ob_concurrent_seq_queue.h ob_concurrent_seq_queue.cpp
  ob_easy_hazard_map.h
  ob_log_adapt_string.h ob_log_adapt_string.cpp
  ob_log_adapt_string.h ob_log_adapt_string.cpp
  ob_log_all_svr_cache.h ob_log_all_svr_cache.cpp
  ob_log_binlog_record.h ob_log_binlog_record.cpp
  ob_log_binlog_record_pool.h ob_log_binlog_record_pool.cpp
  ob_log_binlog_record_queue.h ob_log_binlog_record_queue.cpp
  ob_log_batch_buffer.h ob_log_batch_buffer.cpp
  ob_log_batch_buffer_task.h ob_log_batch_buffer_task.cpp
  ob_log_block.h ob_log_block.cpp
  ob_log_buf.h ob_log_buf.cpp
  ob_log_cluster_id_filter.cpp ob_log_cluster_id_filter.h
  ob_log_common.h
  ob_log_config.h ob_log_config.cpp
  ob_log_ddl_handler.cpp ob_log_ddl_handler.h
  ob_log_ddl_parser.h ob_log_ddl_parser.cpp
  ob_log_dlist.h
  ob_log_dml_parser.h ob_log_dml_parser.cpp
  ob_log_factory.h ob_log_factory.cpp
  ob_log_fake_common_config.h
  ob_log_fetch_log_rpc.h ob_log_fetch_log_rpc.cpp
  ob_log_fetch_stat_info.h ob_log_fetch_stat_info.cpp
  ob_log_fetch_stream.h ob_log_fetch_stream.cpp
  ob_log_fetch_stream_container.h ob_log_fetch_stream_container.cpp
  ob_log_fetch_stream_pool.h ob_log_fetch_stream_pool.cpp
  ob_log_fetch_stream_type.h ob_log_fetch_stream_type.cpp
  ob_log_fetcher.h ob_log_fetcher.cpp
  ob_log_fetcher_dead_pool.h ob_log_fetcher_dead_pool.cpp
  ob_log_fetcher_dispatcher.cpp ob_log_fetcher_dispatcher.h
  ob_log_fetcher_heartbeat_worker.cpp ob_log_fetcher_heartbeat_worker.h
  ob_log_fetcher_heartbeat_worker.h ob_log_fetcher_heartbeat_worker.cpp
  ob_log_fetcher_idle_pool.h ob_log_fetcher_idle_pool.cpp
  ob_log_formatter.h ob_log_formatter.cpp
  ob_log_entry_task_pool.h ob_log_entry_task_pool.cpp
  ob_log_store_service.h
  ob_log_store_service_stat.h ob_log_store_service_stat.cpp
  ob_log_store_key.h ob_log_store_key.cpp
  ob_log_store_task.h ob_log_store_task.cpp
  ob_log_reader.h ob_log_reader.cpp
  ob_log_storager.h ob_log_storager.cpp
  ob_log_hbase_mode.h ob_log_hbase_mode.cpp
  ob_log_work_mode.h ob_log_work_mode.cpp
  ob_log_instance.h ob_log_instance.cpp
  ob_log_lighty_list.h
  ob_log_main.c
  ob_log_meta_manager.h ob_log_meta_manager.cpp
  ob_log_mysql_connector.h ob_log_mysql_connector.cpp
  ob_log_mysql_proxy.h ob_log_mysql_proxy.cpp
  ob_log_rollback_section.cpp
  ob_log_part_fetch_ctx.h ob_log_part_fetch_ctx.cpp
  ob_log_part_fetch_mgr.h ob_log_part_fetch_mgr.cpp
  ob_log_part_mgr.h ob_log_part_mgr.cpp
  ob_log_table_id_cache.h ob_log_table_id_cache.cpp
  ob_log_part_progress_controller.h ob_log_part_progress_controller.cpp
  ob_log_part_serve_info.h
  ob_log_part_svr_list.h ob_log_part_svr_list.cpp
  ob_log_svr_blacklist.h ob_log_svr_blacklist.cpp
  ob_log_part_trans_dispatcher.cpp ob_log_part_trans_dispatcher.h
  ob_log_part_trans_parser.h ob_log_part_trans_parser.cpp
  ob_log_part_trans_resolver.h ob_log_part_trans_resolver.cpp
  ob_log_part_trans_resolver_factory.cpp ob_log_part_trans_resolver_factory.h
  ob_log_part_trans_task.cpp ob_log_part_trans_task.h
  ob_log_part_trans_task.h ob_log_part_trans_task.cpp
  ob_log_part_trans_task_queue.h ob_log_part_trans_task_queue.cpp
  ob_log_entry_wrapper.h ob_log_entry_wrapper.cpp
  ob_log_resource_collector.h ob_log_resource_collector.cpp
  ob_log_resource_recycle_task.h
  ob_log_rpc.h ob_log_rpc.cpp
  ob_log_schema_cache_info.h ob_log_schema_cache_info.cpp
  ob_log_schema_getter.h ob_log_schema_getter.cpp
  ob_log_sequencer1.h ob_log_sequencer1.cpp
  ob_log_server_priority.h ob_log_server_priority.cpp
  ob_log_sql_server_provider.h ob_log_sql_server_provider.cpp
  ob_log_start_log_id_locator.h ob_log_start_log_id_locator.cpp
  ob_log_stream_worker.h ob_log_stream_worker.cpp
  ob_log_svr_finder.h ob_log_svr_finder.cpp
  ob_log_svr_stream.h ob_log_svr_stream.cpp
  ob_log_systable_helper.h ob_log_systable_helper.cpp
  ob_log_table_matcher.h ob_log_table_matcher.cpp
  ob_log_start_schema_matcher.h ob_log_start_schema_matcher.cpp
  ob_log_task_pool.h
  ob_log_timer.h ob_log_timer.cpp
  ob_log_timezone_info_getter.h ob_log_timezone_info_getter.cpp
  ob_log_trace_id.h ob_log_trace_id.cpp
  ob_log_trans_ctx.h ob_log_trans_ctx.cpp
  ob_log_trans_ctx_mgr.h ob_log_trans_ctx_mgr.cpp
  ob_log_trans_dispatch_ctx.h ob_log_trans_dispatch_ctx.cpp
  ob_log_trans_log.h ob_log_trans_log.cpp
  ob_log_trans_msg_sorter.h ob_log_trans_msg_sorter.cpp
  ob_log_trans_redo_dispatcher.h ob_log_trans_redo_dispatcher.cpp
  ob_log_trans_stat_mgr.h ob_log_trans_stat_mgr.cpp
  ob_log_utils.h ob_log_utils.cpp
  ob_map_queue.h
  ob_map_queue_thread.h
  ob_ms_queue_thread.h
  ob_obj2str_helper.h ob_obj2str_helper.cpp
  ob_seq_thread.h
  ob_small_arena.h ob_small_arena.cpp
  ob_log_tenant_task_queue.h ob_log_tenant_task_queue.cpp
  ob_log_tenant.h ob_log_tenant.cpp
  ob_log_tenant_mgr.h ob_log_tenant_mgr.cpp
  ob_log_part_info.h ob_log_part_info.cpp
  ob_log_part_callback.h
  ob_log_ref_state.h ob_log_ref_state.cpp
  ob_log_committer.h ob_log_committer.cpp
  ob_log_rocksdb_store_service.h ob_log_rocksdb_store_service.cpp
  )
add_library(obcdc_objects OBJECT ${SRC_LIST})
target_link_libraries(obcdc_objects PUBLIC obcdc_base)
target_compile_definitions(obcdc_objects PRIVATE ENABLE_DEBUG_LOG)
target_compile_options(obcdc_objects PRIVATE -Werror)

disable_pch(obcdc_objects)
set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -l:libmariadbclient.a -laio")

if (OB_SO_CACHE)
  add_library(obcdc SHARED IMPORTED GLOBAL)
  set_target_properties(obcdc PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/libobcdc.so"
    IMPORTED_LINK_INTERFACE_LIBRARIES obcdc_base)
else()
  add_library(obcdc SHARED ${CMAKE_BINARY_DIR}/src/observer/ob_version.cpp)
  target_link_libraries(obcdc
    PUBLIC obcdc_base
    PRIVATE
    -Wl,--whole-archive
    $<TARGET_OBJECTS:obcdc_objects>
    -Wl,--no-whole-archive
    -Wl,--start-group
    oceanbase_static
    -Wl,--end-group
    PRIVATE -static-libgcc -static-libstdc++
    -Wl,-Bsymbolic
    -Wl,-e,so_main
    ${LGPL_DEPS}
    )
  add_dependencies(obcdc obcdc_objects)
  set_target_properties(obcdc PROPERTIES SOVERSION 1 VERSION 1.0.0)
endif()

add_library(obcdc_objects_miner OBJECT ${SRC_LIST})
disable_pch(obcdc_objects_miner)
target_link_libraries(obcdc_objects_miner PUBLIC obcdc_base)
add_library(obcdc_static
  STATIC
  ${CMAKE_BINARY_DIR}/src/observer/ob_version.cpp)
target_link_libraries(obcdc_static
  PUBLIC obcdc_objects_miner
  -Wl,--start-group
  oceanbase_static
  -Wl,--end-group
  PRIVATE -static-libgcc -static-libstdc++
  ${LGPL_DEPS}
  )
set_target_properties(obcdc_static PROPERTIES OUTPUT_NAME "obcdc")
