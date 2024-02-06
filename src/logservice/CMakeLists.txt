ob_set_subtarget(ob_logservice archiveservice
  archiveservice/ob_archive_allocator.cpp
  archiveservice/ob_archive_define.cpp
  archiveservice/ob_archive_fetcher.cpp
  archiveservice/ob_archive_file_utils.cpp
  archiveservice/ob_archive_io.cpp
  archiveservice/ob_archive_persist_mgr.cpp
  archiveservice/ob_archive_round_mgr.cpp
  archiveservice/ob_archive_sender.cpp
  archiveservice/ob_archive_sequencer.cpp
  archiveservice/ob_archive_service.cpp
  archiveservice/ob_archive_task.cpp
  archiveservice/ob_archive_task_queue.cpp
  archiveservice/ob_archive_util.cpp
  archiveservice/ob_ls_mgr.cpp
  archiveservice/ob_ls_task.cpp
  archiveservice/ob_start_archive_helper.cpp
  archiveservice/dynamic_buffer.cpp
  archiveservice/large_buffer_pool.cpp
  archiveservice/ob_archive_scheduler.cpp
  archiveservice/ob_archive_timer.cpp
  archiveservice/ob_ls_meta_recorder.cpp
  archiveservice/ob_ls_meta_record_task.cpp
)

ob_set_subtarget(ob_logservice cdcservice
  cdcservice/ob_cdc_fetcher.cpp
  cdcservice/ob_cdc_req.cpp
  cdcservice/ob_cdc_rpc_processor.cpp
  cdcservice/ob_cdc_rpc_proxy.cpp
  cdcservice/ob_cdc_service.cpp
  cdcservice/ob_cdc_service_monitor.cpp
  cdcservice/ob_cdc_start_lsn_locator.cpp
  cdcservice/ob_cdc_struct.cpp
  cdcservice/ob_cdc_util.cpp
)

ob_set_subtarget(ob_logservice common
  ob_append_callback.cpp
  ob_garbage_collector.cpp
  ob_location_adapter.cpp
  ob_log_base_header.cpp
  ob_log_handler.cpp
  ob_log_handler_base.cpp
  ob_log_service.cpp
  ob_ls_adapter.cpp
  ob_reporter_adapter.cpp
  ob_switch_leader_adapter.cpp
  ob_server_log_block_mgr.cpp
  ob_log_flashback_service.cpp
  ob_net_keepalive_adapter.cpp
  ob_log_external_storage_handler.cpp
  ob_log_external_storage_io_task.cpp
  ob_log_monitor.cpp
  ob_log_external_storage_handler.cpp
  ob_log_external_storage_io_task.cpp
)

ob_set_subtarget(ob_logservice common_mixed
  applyservice/ob_log_apply_service.cpp
  logrpc/ob_log_request_handler.cpp
  logrpc/ob_log_rpc_req.cpp
  rcservice/ob_role_change_handler.cpp
  rcservice/ob_role_change_service.cpp
  replayservice/ob_log_replay_service.cpp
  replayservice/ob_replay_handler.cpp
  replayservice/ob_replay_status.cpp
  replayservice/ob_tablet_replay_executor.cpp
)

ob_set_subtarget(ob_logservice leader_coordinator
  leader_coordinator/ob_failure_detector.cpp
  leader_coordinator/ob_leader_coordinator.cpp
  leader_coordinator/table_accessor.cpp
  leader_coordinator/election_priority_impl/election_priority_impl.cpp
  leader_coordinator/election_priority_impl/election_priority_v0.cpp
  leader_coordinator/election_priority_impl/election_priority_v1.cpp
)

ob_set_subtarget(ob_logservice logrouteservice
  logrouteservice/ob_all_server_info.cpp
  logrouteservice/ob_all_zone_info.cpp
  logrouteservice/ob_all_units_info.cpp
  logrouteservice/ob_external_server_blacklist.cpp
  logrouteservice/ob_log_all_svr_cache.cpp
  logrouteservice/ob_log_route_key.cpp
  logrouteservice/ob_log_route_service.cpp
  logrouteservice/ob_log_route_struct.cpp
  logrouteservice/ob_log_systable_queryer.cpp
  logrouteservice/ob_ls_log_stat_info.cpp
  logrouteservice/ob_ls_server_list.cpp
  logrouteservice/ob_server_blacklist.cpp
  logrouteservice/ob_server_priority.cpp
)

ob_set_subtarget(ob_logservice palf
  palf/block_gc_timer_task.cpp
  palf/fetch_log_engine.cpp
  palf/log_block_handler.cpp
  palf/log_block_header.cpp
  palf/log_block_mgr.cpp
  palf/log_checksum.cpp
  palf/log_config_mgr.cpp
  palf/log_define.cpp
  palf/log_engine.cpp
  palf/log_entry.cpp
  palf/log_entry_header.cpp
  palf/log_group_buffer.cpp
  palf/log_group_entry.cpp
  palf/log_group_entry_header.cpp
  palf/log_io_task.cpp
  palf/log_io_task_cb_thread_pool.cpp
  palf/log_io_task_cb_utils.cpp
  palf/log_io_worker.cpp
  palf/log_iterator_storage.cpp
  palf/log_learner.cpp
  palf/log_loop_thread.cpp
  palf/log_meta.cpp
  palf/log_meta_entry.cpp
  palf/log_meta_entry_header.cpp
  palf/log_meta_info.cpp
  palf/log_net_service.cpp
  palf/log_reader.cpp
  palf/log_reader_utils.cpp
  palf/log_reconfirm.cpp
  palf/log_req.cpp
  palf/log_request_handler.cpp
  palf/log_rpc.cpp
  palf/log_rpc_processor.cpp
  palf/log_rpc_proxy.cpp
  palf/log_simple_member_list.cpp
  palf/log_sliding_window.cpp
  palf/log_state_mgr.cpp
  palf/log_storage.cpp
  palf/log_task.cpp
  palf/log_writer_utils.cpp
  palf/lsn.cpp
  palf/lsn_allocator.cpp
  palf/palf_base_info.cpp
  palf/palf_callback_wrapper.cpp
  palf/palf_env.cpp
  palf/palf_env_impl.cpp
  palf/palf_handle.cpp
  palf/palf_handle_impl.cpp
  palf/log_cache.cpp
  palf/palf_handle_impl_guard.cpp
  palf/log_block_pool_interface.cpp
  palf/palf_options.cpp
  palf/log_mode_mgr.cpp
  palf/log_updater.cpp
  palf/log_io_utils.cpp
  palf/log_io_worker_wrapper.cpp
  palf/log_throttle.cpp
)

ob_set_subtarget(ob_logservice palf_election
  palf/election/algorithm/election_acceptor.cpp
  palf/election/algorithm/election_impl.cpp
  palf/election/algorithm/election_proposer.cpp
  palf/election/message/election_message.cpp
  palf/election/utils/election_event_recorder.cpp
  palf/election/utils/election_member_list.cpp
  palf/election/utils/election_utils.cpp
)

ob_set_subtarget(ob_logservice restoreservice
  restoreservice/ob_fetch_log_task.cpp
  restoreservice/ob_log_restore_define.cpp
  restoreservice/ob_log_restore_handler.cpp
  restoreservice/ob_log_restore_rpc.cpp
  restoreservice/ob_log_restore_rpc_define.cpp
  restoreservice/ob_log_restore_service.cpp
  restoreservice/ob_remote_data_generator.cpp
  restoreservice/ob_remote_fetch_log.cpp
  restoreservice/ob_remote_fetch_log_worker.cpp
  restoreservice/ob_remote_location_adaptor.cpp
  restoreservice/ob_remote_log_source.cpp
  restoreservice/ob_remote_log_source_allocator.cpp
  restoreservice/ob_log_archive_piece_mgr.cpp
  restoreservice/ob_remote_error_reporter.cpp
  restoreservice/ob_log_restore_allocator.cpp
  restoreservice/ob_remote_fetch_context.cpp
  restoreservice/ob_log_restore_scheduler.cpp
  restoreservice/ob_log_restore_driver_base.cpp
  restoreservice/ob_log_restore_net_driver.cpp
  restoreservice/ob_log_restore_archive_driver.cpp
  restoreservice/ob_restore_log_function.cpp
  restoreservice/ob_remote_log_writer.cpp
)

ob_set_subtarget(ob_logservice data_dictionary
  data_dictionary/ob_data_dict_utils.cpp
  data_dictionary/ob_data_dict_struct.cpp
  data_dictionary/ob_data_dict_service.cpp
  data_dictionary/ob_data_dict_sql_client.cpp
  data_dictionary/ob_data_dict_storager.cpp
  data_dictionary/ob_data_dict_iterator.cpp
  data_dictionary/ob_data_dict_meta_info.cpp
)

ob_set_subtarget(ob_logservice logfetcher
  logfetcher/ob_log_fetcher.cpp
  logfetcher/ob_log_fetching_mode.cpp
  logfetcher/ob_log_ls_fetch_ctx.cpp
  logfetcher/ob_log_fetcher_ls_ctx_default_factory.cpp
  logfetcher/ob_log_ls_fetch_mgr.cpp
  logfetcher/ob_log_fetcher_idle_pool.cpp
  logfetcher/ob_log_fetcher_dead_pool.cpp
  logfetcher/ob_log_start_lsn_locator.cpp
  logfetcher/ob_log_part_progress_controller.cpp
  logfetcher/ob_log_fetch_stream_container.cpp
  logfetcher/ob_log_fetch_stream_container_mgr.cpp
  logfetcher/ob_ls_worker.cpp
  logfetcher/ob_log_rpc.cpp
  logfetcher/ob_log_fetch_log_rpc.cpp
  logfetcher/ob_log_fetch_stream_type.cpp
  logfetcher/ob_log_fetch_stream_pool.cpp
  logfetcher/ob_log_fetcher_switch_info.cpp
  logfetcher/ob_log_ls_fetch_stream.cpp
  logfetcher/ob_log_fetch_stat_info.cpp
  logfetcher/ob_log_trace_id.cpp
  logfetcher/ob_log_utils.cpp
  logfetcher/ob_log_progress_info.cpp
  logfetcher/ob_log_timer.cpp
  logfetcher/ob_log_config.cpp
)

ob_set_subtarget(ob_logservice common_util
  common_util/ob_log_time_utils.cpp
  common_util/ob_log_ls_define.cpp
)

ob_server_add_target(ob_logservice)
add_subdirectory(libobcdc)
