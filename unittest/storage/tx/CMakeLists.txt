add_library(mock_2pc_ctx SHARED
  ob_mailbox.h
  ob_mailbox.cpp
  ob_mock_2pc_ctx.h
  ob_mock_2pc_ctx.cpp)

target_link_libraries(mock_2pc_ctx PUBLIC oceanbase mockcontainer)

add_library(mock_tx_ctx SHARED
  ob_mailbox.h
  ob_mailbox.cpp
  ob_mock_tx_ctx.h
  ob_mock_tx_ctx.cpp)

add_library(mock_tx_log_adapter SHARED
  ob_mock_tx_log_adapter.cpp ob_mock_tx_log_adapter.h)

target_link_libraries(mock_tx_ctx PUBLIC oceanbase mockcontainer)
target_link_libraries(mock_tx_log_adapter PUBLIC oceanbase mockcontainer)

function(c2pc_unittest case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PUBLIC mock_2pc_ctx)
endfunction()

function(tx_unittest case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PUBLIC mock_tx_ctx mock_tx_log_adapter)
endfunction()

c2pc_unittest(test_simple_tx_commit)
c2pc_unittest(test_dup_msg_tx_commit)
c2pc_unittest(test_cycle_commit)
tx_unittest(test_simple_tx_ctx)
tx_unittest(test_ls_log_writer)
tx_unittest(test_ob_trans_hashmap)

storage_unittest(test_ob_black_list)
storage_unittest(test_ob_tx_log)
storage_unittest(test_ob_timestamp_service)
storage_unittest(test_ob_trans_rpc)
storage_unittest(test_ob_tx_msg)
storage_unittest(test_undo_action)
storage_unittest(test_ob_id_meta)
storage_unittest(test_ob_standby_read)
storage_unittest(test_ob_standby_read_transfer)
storage_unittest(test_ob_trans_tlog)
add_subdirectory(it)

storage_unittest(test_dup_table_lease)

storage_unittest(test_redo_submitter)
storage_unittest(test_trans_callback_mgr_fill_redo)
storage_unittest(test_misc)
