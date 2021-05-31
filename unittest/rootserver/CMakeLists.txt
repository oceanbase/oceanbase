function(rs_unittest case)
  ob_unittest(${ARGV})
  target_sources(${case} PRIVATE ob_rs_test_utils.h ob_rs_test_utils.cpp fake_zone_manager.cpp)
endfunction()


#rs_unittest(test_bootstrap)
#rs_unittest(test_recovery_helper)
#rs_unittest(test_multi_cluster_manager)
