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
)

function(ob_unittest_observer case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock observer_test oceanbase)
  set_tests_properties(${case} PROPERTIES TIMEOUT 360)
  set_tests_properties(${case} PROPERTIES LABELS "simpleserver")
endfunction()

ob_unittest_observer(test_ls_recover test_ls_recover.cpp)
ob_unittest_observer(test_ls_status_operator test_ls_status_operator.cpp)
ob_unittest_observer(test_ob_black_list_service test_ob_black_list_service.cpp)
ob_unittest_observer(test_ob_minor_freeze test_ob_minor_freeze.cpp)
ob_unittest_observer(test_ob_simple_cluster test_ob_simple_cluster.cpp)
ob_unittest_observer(test_ob_table_lock_service test_ob_table_lock_service.cpp)
ob_unittest_observer(test_observer_expand_shrink test_observer_expand_shrink.cpp)
ob_unittest_observer(test_replay_from_middle test_replay_from_middle.cpp)
ob_unittest_observer(test_special_tablet_flush test_special_tablet_flush.cpp)
ob_unittest_observer(test_tx_data_table_mit test_tx_data_table_mit.cpp)
ob_unittest_observer(test_lock_table_persistence test_lock_table_persistence.cpp)
ob_unittest_observer(test_tx_recover test_tx_recovery.cpp)
ob_unittest_observer(test_tx_recover2 test_tx_recovery2.cpp)
ob_unittest_observer(test_tx_recover3 test_tx_recovery3.cpp)
