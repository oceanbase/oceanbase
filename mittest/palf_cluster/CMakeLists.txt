set(PALF_CLUSTER_TEST_SRCS
  env/ob_simple_log_cluster.cpp
  env/ob_simple_log_server.cpp
  logservice/ob_log_client.cpp
  logservice/log_service.cpp
  logservice/ls_adapter.cpp
  logservice/role_coordinator_handler.cpp
  logservice/role_coordinator.cpp
  rpc/palf_cluster_rpc_req.cpp
  rpc/palf_cluster_request_handler.cpp)

add_library(palf_cluster_test ${PALF_CLUSTER_TEST_SRCS})
target_include_directories(palf_cluster_test PUBLIC
  ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/mittest)
target_link_libraries(palf_cluster_test PUBLIC oceanbase)

function(ob_unittest_clog case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock palf_cluster_test oceanbase)
  set_tests_properties(${case} PROPERTIES TIMEOUT 600)
endfunction()

ob_unittest_clog(test_palf_bench_server test_palf_bench_server.cpp)
ob_unittest_clog(test_palf_bench_client test_palf_bench_client.cpp)