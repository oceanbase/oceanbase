function(rs_unittest case)
  ob_unittest(${ARGV})
  target_sources(${case} PRIVATE 
  ob_rs_test_utils.h 
  ob_rs_test_utils.cpp 
  fake_zone_manager.cpp
  fake_freeze_info_manager.cpp
  fake_zone_merge_manager.cpp
  )
  target_link_libraries(${case} PRIVATE mockcontainer)
endfunction()


#rs_unittest(test_bootstrap)
rs_unittest(test_rs_reentrant_thread)
rs_unittest(test_tenant_all_zone_merge_strategy)
rs_unittest(test_primary_ls_service)
#rs_unittest(test_multi_cluster_manager)
#rs_unittest(test_ob_backup_task_scheduler)
#rs_unittest(test_zone_merge_manager)
#rs_unittest(test_freeze_info_manager)
rs_unittest(test_archive_checkpoint)
rs_unittest(test_import_table)
rs_unittest(test_heartbeat_service)
rs_unittest(test_parallel_ddl_control)