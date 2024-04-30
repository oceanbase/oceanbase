add_library(obcdc_msg STATIC IMPORTED)
if (OB_USE_DRCMSG)
  set_target_properties(obcdc_msg PROPERTIES
    IMPORTED_LOCATION ${DEP_DIR}/lib/libdrcmsg.a)
  set_property(GLOBAL PROPERTY CDC_MSG_HEADER_DIR ${DEP_DIR}/include/drcmsg)
else()
  set_target_properties(obcdc_msg PROPERTIES
    IMPORTED_LOCATION ${DEP_DIR}/lib/liboblogmsg.a)
  set_property(GLOBAL PROPERTY CDC_MSG_HEADER_DIR ${DEP_DIR}/include/oblogmsg)
endif()

add_library(rocksdb STATIC IMPORTED)
set_target_properties(rocksdb PROPERTIES
  IMPORTED_LOCATION ${DEP_DIR}/lib64/librocksdb.a)

add_library(obcdc_base INTERFACE)
target_include_directories(obcdc_base INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(obcdc_base
  INTERFACE ob_base
  obcdc_msg rocksdb
  ${OB_RELRO_FLAG} -Wl,-T,${CMAKE_SOURCE_DIR}/rpm/ld.lds)

ob_set_subtarget(obcdc_object_list common
  libobcdc.cpp
  ob_cdc_auto_config_mgr.cpp
  ob_cdc_define.cpp
  ob_cdc_tablet_to_table_info.cpp
  ob_cdc_lob_ctx.cpp
  ob_cdc_lob_data_merger.cpp
  ob_cdc_lob_aux_meta_storager.cpp
  ob_cdc_global_info.cpp
  ob_cdc_lob_aux_table_schema_info.cpp
  ob_cdc_lob_aux_table_parse.cpp
  ob_cdc_malloc_sample_info.cpp
  ob_cdc_server_endpoint_access_info.cpp
  ob_cdc_tenant_endpoint_provider.cpp
  ob_cdc_udt.cpp
  ob_concurrent_seq_queue.cpp
  ob_log_adapt_string.cpp
  ob_log_batch_buffer.cpp
  ob_log_batch_buffer_task.cpp
  ob_log_binlog_record.cpp
  ob_log_binlog_record_pool.cpp
  ob_log_binlog_record_queue.cpp
  ob_log_block.cpp
  ob_log_buf.cpp
  ob_log_cluster_id_filter.cpp
  ob_log_committer.cpp
  ob_log_config.cpp
  ob_log_sys_ls_task_handler.cpp
  ob_log_ddl_processor.cpp
  ob_log_ls_op_processor.cpp
  ob_log_ddl_parser.cpp
  ob_log_dml_parser.cpp
  ob_log_entry_task_pool.cpp
  ob_log_meta_data_service.cpp
  ob_log_meta_data_fetcher.cpp
  ob_log_meta_data_refresh_mode.cpp
  ob_log_meta_data_baseline_loader.cpp
  ob_log_meta_data_struct.cpp
  ob_log_meta_data_queryer.cpp
  ob_log_meta_data_replayer.cpp
  ob_log_schema_incremental_replay.cpp
  ob_log_meta_data_fetcher_dispatcher.cpp
  ob_log_factory.cpp
  ob_log_fetch_log_rpc.cpp
  ob_log_fetch_stream_container.cpp
  ob_log_fetch_stream_container_mgr.cpp
  ob_log_fetch_stream_pool.cpp
  ob_log_fetch_stream_type.cpp
  ob_log_fetcher.cpp
  ob_log_fetcher_dead_pool.cpp
  ob_log_fetcher_dispatcher.cpp
  ob_log_fetcher_idle_pool.cpp
  ob_log_fetching_mode.cpp
  ob_log_formatter.cpp
  ob_log_hbase_mode.cpp
  ob_log_instance.cpp
  ob_log_main.c
  ob_log_meta_manager.cpp
  ob_log_mysql_connector.cpp
  ob_log_mysql_proxy.cpp
  ob_log_ls_fetch_ctx.cpp
  ob_log_ls_fetch_stream.cpp
  ob_log_ls_fetch_mgr.cpp
  ob_log_part_mgr.cpp
  ob_log_ls_info.cpp
  ob_log_ls_getter.cpp
  ob_log_ls_mgr.cpp
  ob_log_part_progress_controller.cpp
  ob_log_part_trans_dispatcher.cpp
  ob_log_part_trans_parser.cpp
  ob_log_all_ddl_operation_schema_info.cpp
  ob_cdc_part_trans_resolver.cpp
  ob_log_part_trans_resolver_factory.cpp
  ob_log_part_trans_task.cpp
  ob_log_part_trans_task_queue.cpp
  ob_log_reader.cpp
  ob_log_ref_state.cpp
  ob_log_resource_collector.cpp
  ob_log_rocksdb_store_service.cpp
  ob_log_rollback_section.cpp
  ob_log_rpc.cpp
  ob_log_schema_cache_info.cpp
  ob_log_schema_getter.cpp
  ob_log_sequencer1.cpp
  ob_log_server_priority.cpp
  ob_log_sql_server_provider.cpp
  ob_cdc_tenant_sql_server_provider.cpp
  ob_log_start_lsn_locator.cpp
  ob_log_start_schema_matcher.cpp
  ob_log_storager.cpp
  ob_log_store_key.cpp
  ob_log_store_service_stat.cpp
  ob_log_store_task.cpp
  ob_log_svr_blacklist.cpp
  ob_log_systable_helper.cpp
  ob_log_table_id_cache.cpp
  ob_log_table_matcher.cpp
  ob_log_tenant.cpp
  ob_log_tic_update_info.cpp
  ob_log_tenant_mgr.cpp
  ob_log_tenant_task_queue.cpp
  ob_log_timer.cpp
  ob_log_timezone_info_getter.cpp
  ob_log_trace_id.cpp
  ob_log_trans_ctx.cpp
  ob_log_trans_ctx_mgr.cpp
  ob_log_trans_dispatch_ctx.cpp
  ob_cdc_multi_data_source_info.cpp
  ob_log_trans_log.cpp
  ob_log_trans_msg_sorter.cpp
  ob_log_trans_redo_dispatcher.cpp
  ob_log_trans_stat_mgr.cpp
  ob_log_utils.cpp
  ob_log_work_mode.cpp
  ob_ls_worker.cpp
  ob_obj2str_helper.cpp
  ob_small_arena.cpp
)

ob_add_new_object_target(obcdc_objects obcdc_object_list)

target_link_libraries(obcdc_objects PUBLIC obcdc_base)
target_compile_definitions(obcdc_objects PRIVATE ENABLE_DEBUG_LOG)
if (OB_STRICT_COMPILE_FLAGS)
  target_compile_options(obcdc_objects PRIVATE)
else()
  target_compile_options(obcdc_objects PRIVATE -Werror)
endif()
if (OB_SO_CACHE)
  add_library(obcdc SHARED IMPORTED GLOBAL)
  set_target_properties(obcdc PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/libobcdc.so"
    IMPORTED_LINK_INTERFACE_LIBRARIES obcdc_base
    )
else()
  add_library(obcdc SHARED ${CMAKE_BINARY_DIR}/src/observer/ob_version.cpp)
  target_link_libraries(obcdc
    PUBLIC obcdc_base
    PRIVATE
    -Wl,--whole-archive
    obcdc_objects
    -Wl,--no-whole-archive
    -Wl,--start-group
    oceanbase_static
    -Wl,--end-group
    -static-libgcc -static-libstdc++
    -Wl,-Bsymbolic
    -Wl,-e,so_main
    easy
    )
  if(OB_BUILD_OPENSOURCE)
  set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -lmariadb")
  if (OB_STATIC_LINK_LGPL_DEPS)
    set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -l:libmariadbclient.a")
  endif()
  target_link_libraries(obcdc
    PRIVATE
    ${LGPL_DEPS}
    )
  endif()
  add_dependencies(obcdc obcdc_objects)
endif()
set_target_properties(obcdc PROPERTIES
  PUBLIC_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/libobcdc.h;${CMAKE_SOURCE_DIR}/deps/oblib/src/lib/ob_errno.h"
  SOVERSION ${CMAKE_PROJECT_VERSION_MAJOR}
  VERSION ${CMAKE_PROJECT_VERSION})

ob_add_new_object_target(obcdc_objects_miner obcdc_object_list)

target_link_libraries(obcdc_objects_miner PUBLIC obcdc_base)
add_library(obcdc_static
  STATIC
  ${CMAKE_BINARY_DIR}/src/observer/ob_version.cpp)

target_link_libraries(obcdc_static
  PUBLIC obcdc_objects
  -Wl,--start-group
  oceanbase_static
  -Wl,--end-group
  PRIVATE easy -static-libgcc -static-libstdc++)
set_target_properties(obcdc_static PROPERTIES OUTPUT_NAME "obcdc")
