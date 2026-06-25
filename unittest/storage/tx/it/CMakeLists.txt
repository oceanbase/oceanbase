add_library(mock_lock_wait_mgr SHARED
  ob_mock_request_msg.h
  ob_mock_request_msg.cpp
  ob_mock_lock_wait_mgr.h
  ob_mock_lock_wait_mgr.cpp)

target_link_libraries(mock_lock_wait_mgr PUBLIC oceanbase mockcontainer)

add_library(tx_node OBJECT tx_node.cpp)
target_link_libraries(tx_node PUBLIC oceanbase mock_lock_wait_mgr)

function(tx_it_test case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PUBLIC tx_node oceanbase)
endfunction()


tx_it_test(test_tx)
tx_it_test(test_tx_free_route)
tx_it_test(test_tx_ctx)
tx_it_test(test_register_mds)
if(OB_HOTSPOT_GROUP_COMMIT)
  tx_it_test(test_hotspot_tx_basic)
  tx_it_test(test_hotspot_tx_bugfix)
  tx_it_test(test_hotspot_tx_leader_switch)
  tx_it_test(test_hotspot_free_cbs_deadlock_integration)
endif()
#tx_it_test(test_tx_perf)
tx_it_test(test_lock_wait_mgr)
