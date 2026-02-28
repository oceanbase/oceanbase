add_library(mock_lock_wait_mgr SHARED
  ob_mock_request_msg.cpp
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
#tx_it_test(test_tx_perf)
tx_it_test(test_lock_wait_mgr)
function(tx_it_test_with_param case use_hash_index)
    set(init_flag ${use_hash_index})
    set(target_name "${case}_use_hash_index_${init_flag}")
    ob_unittest(${target_name} ${case}.cpp tx_node.cpp)
    target_compile_definitions(${target_name} PRIVATE TX_NODE_MEMTABLE_USE_HASH_INDEX_FLAG=${init_flag})
    target_link_libraries(${target_name} PUBLIC oceanbase mock_lock_wait_mgr)
endfunction()

tx_it_test_with_param(test_tx false)
tx_it_test_with_param(test_tx_free_route false)
tx_it_test_with_param(test_tx_ctx false)
tx_it_test_with_param(test_register_mds false)
#tx_it_test_with_param(test_tx_perf false)