set(OBSERVER_TEST_SRCS
  env/ob_simple_replica.cpp
  # env/ob_simple_server_restart_helper.cpp
  env/ob_multi_replica_test_base.cpp
  )

add_library(simple_replica_test ${OBSERVER_TEST_SRCS})

target_include_directories(
  simple_replica_test PUBLIC
  ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/mittest)
  target_link_libraries(simple_replica_test PUBLIC
  oceanbase
  )

function(ob_unittest_multi_replica case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock simple_replica_test oceanbase)
  set_tests_properties(${case} PROPERTIES TIMEOUT 600)
  set_tests_properties(${case} PROPERTIES LABELS "simple_replica")
endfunction()

ob_unittest_multi_replica(test_ob_multi_replica_basic)
ob_unittest_multi_replica(test_ob_dup_table_basic)
ob_unittest_multi_replica(test_ob_dup_table_leader_switch)
ob_unittest_multi_replica(test_ob_dup_table_tablet_gc)
ob_unittest_multi_replica(test_mds_replay_from_ctx_table)
