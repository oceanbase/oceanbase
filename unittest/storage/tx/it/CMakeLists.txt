add_library(tx_node OBJECT tx_node.cpp)
target_link_libraries(tx_node PUBLIC oceanbase)

function(tx_it_test case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PUBLIC tx_node oceanbase)
endfunction()


tx_it_test(test_tx)
tx_it_test(test_tx_free_route)
tx_it_test(test_tx_ctx)
tx_it_test(test_register_mds)
#tx_it_test(test_tx_perf)


