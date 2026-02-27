# set(OBSERVER_TEST_SRCS
#         env/ob_simple_server.cpp
#         env/ob_simple_server_restart_helper.cpp
#         env/ob_simple_cluster_test_base.cpp
#         )
#
# add_library(observer_test ${OBSERVER_TEST_SRCS})
#
# target_include_directories(observer_test PUBLIC
#   ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/mittest)
# target_link_libraries(observer_test PUBLIC
#   oceanbase
#   mit_env
# )
function(ob_unittest_observer case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock observer_test oceanbase)
  target_link_directories(${case} PUBLIC ${CMAKE_SOURCE_DIR}/mittest)
  set_tests_properties(${case} PROPERTIES TIMEOUT 3600)
endfunction()

ob_unittest_observer(test_macro_cache_multi_version_gc test_macro_cache_multi_version_gc.cpp)
ob_unittest_observer(test_shared_storage_ls_gc test_shared_storage_ls_gc.cpp)
ob_unittest_observer(test_inc_shared_storage_gc test_inc_shared_storage_gc.cpp)
ob_unittest_observer(test_inc_shared_storage_err_gc test_inc_shared_storage_err_gc.cpp)
ob_unittest_observer(test_shared_storage_tx_data_gc test_shared_storage_tx_data_gc.cpp)
ob_unittest_observer(test_storage_cache_policy_prewarmer test_storage_cache_policy_prewarmer.cpp)
ob_unittest_observer(test_storage_cache_prewarm_macro_cache test_storage_cache_prewarm_macro_cache.cpp)
ob_unittest_observer(test_macro_cache_evict test_macro_cache_evict.cpp)
ob_unittest_observer(test_ss_split_prewarm test_ss_split_prewarm.cpp)
ob_unittest_observer(test_tablet_split_shared_storage_gc test_tablet_split_shared_storage_gc.cpp)
ob_unittest_observer(test_inc_shared_storage_macro_block_check test_inc_shared_storage_macro_block_check.cpp)
