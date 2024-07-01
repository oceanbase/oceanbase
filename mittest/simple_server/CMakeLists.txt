set(OBSERVER_TEST_SRCS
        env/ob_simple_server.cpp
        env/ob_simple_server_restart_helper.cpp
        env/ob_simple_cluster_test_base.cpp
        )

add_library(observer_test ${OBSERVER_TEST_SRCS})

target_include_directories(observer_test PUBLIC
  ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/mittest)
target_link_libraries(observer_test PUBLIC
  oceanbase
  mit_env
)

function(ob_unittest_observer case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock observer_test oceanbase)
  set_tests_properties(${case} PROPERTIES TIMEOUT 360)
  set_tests_properties(${case} PROPERTIES LABELS "simpleserver")
endfunction()

function(ob_ha_unittest_observer case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock observer_test oceanbase)
endfunction()

function(errsim_ha_unittest_observer case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock observer_test oceanbase)
endfunction()

function(ob_freeze_observer case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock observer_test oceanbase)
endfunction()

function(ob_offline_observer case case_file)
  add_executable(${case}
    EXCLUDE_FROM_ALL
    ${case_file}
    ${OBSERVER_TEST_SRCS}
    ../${MIT_SRCS}
    )
  target_include_directories(${case} PUBLIC
    ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/mittest)
  target_link_libraries(${case}
    PRIVATE
    -Wl,--start-group
    oceanbase_static
    ob_sql_static
    ob_storage_static
    -Wl,--end-group
    -static-libgcc
    -static-libstdc++
    gtest
    gmock)
endfunction()

ob_offline_observer(test_simple_ob test_ob_simple_cluster.cpp)
ob_offline_observer(test_transfer_tx test_transfer_tx.cpp)
ob_offline_observer(test_tx_data test_tx_data.cpp)

ob_unittest_observer(test_transfer_no_kill_tx test_transfer_tx.cpp)
ob_unittest_observer(test_standby_balance test_standby_balance_ls_group.cpp)
ob_unittest_observer(test_ls_recover test_ls_recover.cpp)
ob_unittest_observer(test_ob_simple_cluster test_ob_simple_cluster.cpp)
ob_unittest_observer(test_ob_partition_balance test_ob_partition_balance.cpp)
ob_unittest_observer(test_ls_status_operator test_ls_status_operator.cpp)
ob_unittest_observer(test_balance_operator test_tenant_balance_operator.cpp)
ob_unittest_observer(test_transfer_partition_task test_transfer_partition_task.cpp)
ob_unittest_observer(test_mds_table_checkpoint test_mds_table_checkpoint.cpp)
ob_unittest_observer(test_ob_black_list_service test_ob_black_list_service.cpp)
ob_unittest_observer(test_ob_queuing_table test_ob_queuing_table.cpp)
ob_unittest_observer(test_ob_table_lock_service test_ob_table_lock_service.cpp)
ob_unittest_observer(test_ob_obj_lock_garbage_collector test_ob_obj_lock_garbage_collector.cpp)
ob_unittest_observer(test_observer_expand_shrink test_observer_expand_shrink.cpp)
ob_unittest_observer(test_replay_from_middle test_replay_from_middle.cpp)
ob_unittest_observer(test_special_tablet_flush test_special_tablet_flush.cpp)
ob_unittest_observer(test_tx_data_table_mit test_tx_data_table_mit.cpp)
ob_unittest_observer(test_tx_ctx_table_mit test_tx_ctx_table_mit.cpp)
ob_unittest_observer(test_lock_table_persistence test_lock_table_persistence.cpp)
ob_unittest_observer(test_tx_recover test_tx_recovery.cpp)
ob_unittest_observer(test_tx_recover2 test_tx_recovery2.cpp)
ob_unittest_observer(test_tx_recover3 test_tx_recovery3.cpp)
ob_unittest_observer(test_transfer_task_operator test_transfer_task_operator.cpp)
ob_unittest_observer(test_ob_tablet_to_ls_operator test_ob_tablet_to_ls_operator.cpp)
ob_unittest_observer(test_tenant_transfer_service test_tenant_transfer_service.cpp)
ob_unittest_observer(test_schema_service_sql_impl test_schema_service_sql_impl.cpp)
ob_unittest_observer(test_location_service test_location_service.cpp)
ob_unittest_observer(test_mds_tx_ctx_recover_mem_leak test_mds_tx_ctx_recover_mem_leak.cpp)
ob_unittest_observer(test_role_change_service test_role_change_service.cpp)
ob_unittest_observer(test_arbitration_service_rpc test_arbitration_service_rpc.cpp)
ob_unittest_observer(test_arbitration_service_table_operator test_arbitration_service_table_operator.cpp)
ob_unittest_observer(test_add_remove_replace_arbitration_service test_add_remove_replace_arbitration_service.cpp)
ob_unittest_observer(test_create_tenant_with_arbitration_service test_create_tenant_with_arbitration_service.cpp)
ob_unittest_observer(test_arbitration_service_replica_task_table_operator test_arbitration_service_replica_task_table_operator.cpp)
ob_unittest_observer(test_change_arb_service_status test_change_arb_service_status.cpp)
ob_unittest_observer(test_big_tx_data test_big_tx_data.cpp)
ob_unittest_observer(test_fast_commit_report fast_commit_report.cpp)
#ob_unittest_observer(test_mvcc_gc test_mvcc_gc.cpp)
ob_unittest_observer(test_ob_simple_rto test_ob_simple_rto.cpp)
ob_unittest_observer(test_ddl_task test_ddl_task.cpp)
ob_unittest_observer(test_all_virtual_proxy_partition_info_default_value test_all_virtual_proxy_partition_info_default_value.cpp)
ob_unittest_observer(test_get_stopped_zone_list test_get_stopped_zone_list.cpp)
ob_unittest_observer(test_lock_table_with_tx test_lock_table_with_tx.cpp)
ob_unittest_observer(test_ob_detect_manager_in_simple_server test_ob_detect_manager_in_simple_server.cpp)
ob_unittest_observer(test_transfer_lock_info_operator storage_ha/test_transfer_lock_info_operator.cpp)
ob_unittest_observer(test_mds_recover test_mds_recover.cpp)
ob_unittest_observer(test_keep_alive_min_start_scn test_keep_alive_min_start_scn.cpp)
ob_unittest_observer(test_ls_replica test_ls_replica.cpp)
ob_unittest_observer(test_create_clone_tenant_resource_pool test_create_clone_tenant_resource_pool.cpp)
ob_unittest_observer(test_ob_admin_arg test_ob_admin_arg.cpp)
ob_unittest_observer(test_tablet_autoinc_mgr test_tablet_autoinc_mgr.cpp)
ob_unittest_observer(test_tablet_memtable_mit test_tablet_memtable_mit.cpp)
ob_unittest_observer(test_tenant_snapshot_service test_tenant_snapshot_service.cpp)
ob_unittest_observer(test_callbacks_with_reverse_order test_callbacks_with_reverse_order.cpp)
ob_unittest_observer(test_transfer_tx_data test_transfer_with_smaller_tx_data.cpp)
ob_unittest_observer(test_transfer_in_after_abort test_transfer_in_after_abort.cpp)
ob_unittest_observer(test_transfer_commit_action test_transfer_with_commit_action.cpp)
ob_unittest_observer(test_transfer_rollback_to test_transfer_between_rollback_to.cpp)
ob_unittest_observer(test_memtable_new_safe_to_destroy test_memtable_new_safe_to_destroy.cpp)
ob_unittest_observer(test_tablet_to_ls_cache test_tablet_to_ls_cache.cpp)

####### freeze case #######
#ob_freeze_observer(test_frequently_freeze freeze/test_frequently_freeze.cpp)
ob_freeze_observer(test_ob_minor_freeze freeze/test_ob_minor_freeze.cpp)
####### freeze case #######


# TODO(muwei.ym): open later
ob_ha_unittest_observer(test_transfer_handler storage_ha/test_transfer_handler.cpp)
ob_ha_unittest_observer(test_transfer_and_restart_basic storage_ha/test_transfer_and_restart_basic.cpp)
ob_ha_unittest_observer(test_transfer_start_stage_restart_without_mds_flush storage_ha/test_transfer_start_stage_restart_without_mds_flush.cpp)
ob_ha_unittest_observer(test_transfer_doing_stage_restart_without_mds_flush storage_ha/test_transfer_doing_stage_restart_without_mds_flush.cpp)
ob_ha_unittest_observer(test_transfer_complete_restart_without_mds_flush storage_ha/test_transfer_complete_restart_without_mds_flush.cpp)
ob_ha_unittest_observer(test_transfer_doing_stage_restart_with_mds_flush storage_ha/test_transfer_doing_stage_restart_with_mds_flush.cpp)
ob_ha_unittest_observer(test_transfer_complete_restart_with_mds_flush storage_ha/test_transfer_complete_restart_with_mds_flush.cpp)
ob_ha_unittest_observer(test_transfer_with_empty_shell storage_ha/test_transfer_with_empty_shell.cpp)
ob_ha_unittest_observer(test_mds_transaction test_mds_transaction.cpp)
errsim_ha_unittest_observer(errsim_test_transfer_handler errsim/storage_ha/errsim_test_transfer_handler.cpp)