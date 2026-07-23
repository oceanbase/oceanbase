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
storage_unittest(test_ob_id_meta)
storage_unittest(test_ob_standby_read)
storage_unittest(test_ob_standby_read_transfer)
storage_unittest(test_ob_trans_tlog)
add_subdirectory(it)

storage_unittest(test_dup_table_lease)

storage_unittest(test_log_cb_pool_limit)

storage_unittest(test_redo_submitter)
storage_unittest(test_trans_callback_mgr_fill_redo)
storage_unittest(test_misc)
storage_unittest(test_trans_ctx_lock)

# Phase 02: TxRedoFlushStatus enum refactoring tests
storage_unittest(test_tx_redo_flush_status_enum)
storage_unittest(test_tx_redo_flush_status_enum_extended)

# Phase 02: Integration scenario tests (cross-review recommended)
storage_unittest(test_tx_integration_scenarios)

if(NOT OB_HOTSPOT_GROUP_COMMIT)
  storage_unittest(test_hotspot_ce_fallback)
endif()

# Hotspot leader switch extended tests (PREPARING/COLLECTING abort path)
storage_unittest(test_hotspot_tx_leader_switch_extended)

# Role-safe transit function tests (GAP-01~03, GAP-05~07)
storage_unittest(test_tx_transit_functions)

# Free hotspot CBs deadlock fix tests
if(OB_HOTSPOT_GROUP_COMMIT)
  storage_unittest(test_hotspot_free_cbs_deadlock)
  # Phase 01: Memory Analysis tests (MEM-01, MEM-02)
  storage_unittest(test_ob_tx_ctx_memory)

  # Hotspot edge cases and memory management tests (review feedback)
  storage_unittest(test_hotspot_edge_cases)

  # ObTxHotspotRedoStat correctness (LOG_PLAN §4 / §8.1):
  # concurrent INC, peak CAS, ts_once, reset, log trigger conditions,
  # cache-line alignment, negative-bytes guard.
  storage_unittest(test_hotspot_redo_stat)
endif()
