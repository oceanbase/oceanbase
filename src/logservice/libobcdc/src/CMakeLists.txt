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
  IMPORTED_LOCATION ${CDC_ROCKSDB_LIBRARY})

add_library(obcdc_base INTERFACE)
target_include_directories(obcdc_base INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(obcdc_base SYSTEM INTERFACE
  ${CDC_DEP_DIR}/include)
target_link_libraries(obcdc_base
  INTERFACE -Wl,--whole-archive mock_di -Wl,--no-whole-archive ob_base
  obcdc_msg rocksdb
  ${OB_RELRO_FLAG} -Wl,-T,${CMAKE_SOURCE_DIR}/rpm/ld.lds)

# LTO / sample PGO for libobcdc (see libobcdc/profile/)
set(CDC_PROFILE_DIR ${CMAKE_CURRENT_LIST_DIR}/../profile)
message(STATUS "libobcdc: ENABLE_CDC_COMPILE_OPTIMIZATIONS=${ENABLE_CDC_COMPILE_OPTIMIZATIONS}")
set(CDC_COMPILE_OPTS "")
set(CDC_LINK_OPTS "")

if(ENABLE_CDC_THIN_LTO)
  list(APPEND CDC_COMPILE_OPTS "-flto=thin")
  list(APPEND CDC_LINK_OPTS "-flto=thin")
  if(LTO_JOBS)
    list(APPEND CDC_LINK_OPTS "-Wl,--thinlto-jobs=${LTO_JOBS}")
  endif()
  if(USE_LTO_CACHE AND LTO_CACHE_DIR AND LTO_CACHE_POLICY)
    list(APPEND CDC_LINK_OPTS
      "-Wl,--thinlto-cache-dir=${LTO_CACHE_DIR}"
      "-Wl,--thinlto-cache-policy=${LTO_CACHE_POLICY}")
  endif()
  message(STATUS "libobcdc: ENABLE_CDC_THIN_LTO=ON")
endif()

if(ENABLE_CDC_SAMPLE_PGO)
  if(${ARCHITECTURE} STREQUAL "x86_64")
    set(CDC_SAMPLE_PROFILE "${CDC_PROFILE_DIR}/cdc-x86_64.prof")
  elseif(${ARCHITECTURE} STREQUAL "aarch64")
    set(CDC_SAMPLE_PROFILE "${CDC_PROFILE_DIR}/cdc-aarch64.prof")
  else()
    message(FATAL_ERROR "ENABLE_CDC_SAMPLE_PGO: unsupported ARCHITECTURE=${ARCHITECTURE}")
  endif()
  if(NOT EXISTS "${CDC_SAMPLE_PROFILE}")
    message(WARNING "libobcdc: CDC sample profile not found: ${CDC_SAMPLE_PROFILE}")
  endif()
  list(APPEND CDC_COMPILE_OPTS "-finline-functions" "-fprofile-sample-use=${CDC_SAMPLE_PROFILE}")
  list(APPEND CDC_LINK_OPTS "-fprofile-sample-use=${CDC_SAMPLE_PROFILE}")
  message(STATUS "libobcdc: ENABLE_CDC_SAMPLE_PGO=ON, profile=${CDC_SAMPLE_PROFILE}")
endif()

if(ENABLE_CDC_HOTFUNC)
  if(${ARCHITECTURE} STREQUAL "x86_64")
    set(CDC_HOTFUNC_PATH "${CDC_PROFILE_DIR}/cdc-hotfuncs-x86_64.txt")
  elseif(${ARCHITECTURE} STREQUAL "aarch64")
    set(CDC_HOTFUNC_PATH "${CDC_PROFILE_DIR}/cdc-hotfuncs-aarch64.txt")
  else()
    message(FATAL_ERROR "ENABLE_CDC_HOTFUNC: unsupported ARCHITECTURE=${ARCHITECTURE}")
  endif()
  if(NOT EXISTS "${CDC_HOTFUNC_PATH}")
    message(WARNING "libobcdc: CDC hotfunc file not found: ${CDC_HOTFUNC_PATH}")
  endif()
  list(APPEND CDC_LINK_OPTS
    "-Wl,--no-warn-symbol-ordering"
    "-Wl,--symbol-ordering-file,${CDC_HOTFUNC_PATH}")
  message(STATUS "libobcdc: ENABLE_CDC_HOTFUNC=ON, file=${CDC_HOTFUNC_PATH}")
endif()

function(apply_cdc_lto_pgo_options target)
  if(CDC_COMPILE_OPTS)
    target_compile_options(${target} PRIVATE ${CDC_COMPILE_OPTS})
  endif()
  if(CDC_LINK_OPTS)
    target_link_options(${target} PRIVATE ${CDC_LINK_OPTS})
  endif()
endfunction()

if( ${ARCHITECTURE} STREQUAL "x86_64" )
  add_definitions(-DARC_X86)
endif()

ob_set_subtarget(obcdc_object_list common
  libobcdc.cpp
  ob_cdc_auto_config_mgr.cpp
  ob_cdc_define.cpp
  ob_cdc_tablet_to_table_info.cpp
  ob_cdc_lob_ctx.cpp
  ob_cdc_lob_data_merger.cpp
  ob_cdc_lob_aux_meta_storager.cpp
  ob_cdc_update_split_merge_storager.cpp
  ob_cdc_update_split_merge_cache.cpp
  ob_cdc_update_split_merge_payload.cpp
  ob_cdc_global_info.cpp
  ob_cdc_lob_aux_table_schema_info.cpp
  ob_cdc_lob_aux_table_parse.cpp
  ob_cdc_malloc_sample_info.cpp
  ob_cdc_miss_log_handler.cpp
  ob_cdc_server_endpoint_access_info.cpp
  ob_cdc_tenant_checkpoint.cpp
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
  ob_log_disk_io_monitor.cpp
  ob_log_sys_ls_task_handler.cpp
  ob_log_ddl_processor.cpp
  ob_log_ls_op_processor.cpp
  ob_log_lsn_filter.cpp
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
  ob_log_trans_id_filter.cpp
  ob_cdc_multi_data_source_info.cpp
  ob_log_trans_log.cpp
  ob_log_trans_msg_sorter.cpp
  ob_log_trans_redo_dispatcher.cpp
  ob_log_trans_stat_mgr.cpp
  ob_log_utils.cpp
  ob_log_work_mode.cpp
  ob_ls_worker.cpp
  ob_obj2str_helper.cpp
  ob_cdc_sql_udt_dict_schema_ctx.cpp
  ob_small_arena.cpp
)

ob_add_new_object_target(obcdc_objects obcdc_object_list)
add_dependencies(obcdc_objects ob_cdc_deps)

target_link_libraries(obcdc_objects PUBLIC obcdc_base)
target_compile_definitions(obcdc_objects PRIVATE ENABLE_DEBUG_LOG)
apply_cdc_lto_pgo_options(obcdc_objects)
if (OB_STRICT_COMPILE_FLAGS)
  target_compile_options(obcdc_objects PRIVATE -Wno-c++20-extensions)
else()
  target_compile_options(obcdc_objects PRIVATE -Werror -Wno-c++20-extensions)
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
    ${STATIC_LIBGCC_FLAGS} ${STATIC_LIBSTDCXX_FLAGS}
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
  apply_cdc_lto_pgo_options(obcdc)
  if (BUILD_CDC_ONLY)
    # Compress DWARF sections in-place after linking so eu-strip can process the binary
    # without hitting size limits. Full debug info is preserved; the debuginfo RPM
    # generation pipeline (find-debuginfo.sh) is unaffected.
    add_custom_command(TARGET obcdc POST_BUILD
      COMMAND ${OB_OBJCOPY_BIN} --compress-debug-sections $<TARGET_FILE:obcdc>
      COMMENT "Compressing DWARF sections in libobcdc.so to reduce eu-strip input size"
    )
  endif()
endif()
set_target_properties(obcdc PROPERTIES
  PUBLIC_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/libobcdc.h;${CMAKE_SOURCE_DIR}/deps/oblib/src/lib/ob_errno.h"
  SOVERSION ${CMAKE_PROJECT_VERSION_MAJOR}
  VERSION ${CMAKE_PROJECT_VERSION})

ob_add_new_object_target(obcdc_objects_miner obcdc_object_list)
add_dependencies(obcdc_objects_miner ob_cdc_deps)

target_link_libraries(obcdc_objects_miner PUBLIC obcdc_base)
apply_cdc_lto_pgo_options(obcdc_objects_miner)
add_library(obcdc_static
    STATIC
    ${CMAKE_BINARY_DIR}/src/observer/ob_version.cpp)
if (BUILD_CDC_ONLY)
  set_target_properties(obcdc_static PROPERTIES EXCLUDE_FROM_ALL TRUE)
endif()

target_link_libraries(obcdc_static
  PUBLIC obcdc_objects
  -Wl,--start-group
  oceanbase_static
  -Wl,--end-group
  PRIVATE easy ${STATIC_LIBGCC_FLAGS} ${STATIC_LIBSTDCXX_FLAGS})
apply_cdc_lto_pgo_options(obcdc_static)
set_target_properties(obcdc_static PROPERTIES OUTPUT_NAME "obcdc")
