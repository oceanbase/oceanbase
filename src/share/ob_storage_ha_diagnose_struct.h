/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_SHARE_HA_MONITOR_STRUCT_
#define OCEABASE_SHARE_HA_MONITOR_STRUCT_

#include "share/transfer/ob_transfer_info.h"
#include "share/ob_ls_id.h"
#include "share/ob_define.h"              // share::ObTaskId

namespace oceanbase
{
namespace share
{

// Two variants per module map to two history tables
// (__all_storage_ha_error_diagnose_history / __all_storage_ha_perf_diagnose_history).
enum class ObStorageHADiagModule : uint8_t
{
  TRANSFER_ERROR_DIAGNOSE = 0,
  TRANSFER_PERF_DIAGNOSE = 1,
  MAX_MODULE
};

// Observer-local execution phase for a transfer — the value that lands in
// the TYPE column of __all_virtual_storage_ha_{error,perf}_diagnose. This
// is *not* the task-level ObTransferStatus: it carries the src/dst role
// (START_IN vs START_OUT, FINISH_IN vs FINISH_OUT) and has BACKFILLED as
// a distinct sub-phase of DOING, both of which matter for local diagnosis
// and are invisible in the task status.
enum class ObStorageHADiagTaskType : uint8_t
{
  TRANSFER_START = 0,        // handler driving START on src_ls leader
  TRANSFER_DOING = 1,        // handler driving DOING on dst_ls leader
  TRANSFER_ABORT = 2,        // handler driving ABORT on src_ls leader
  TRANSFER_START_IN = 3,     // start_transfer_in log replay on dst side
  TRANSFER_START_OUT = 4,    // start_transfer_out log replay on src side (reserved)
  TRANSFER_FINISH_IN = 5,    // finish_transfer_in log replay on dst side
  TRANSFER_FINISH_OUT = 6,   // finish_transfer_out log replay on src side (reserved)
  TRANSFER_BACKFILLED = 7,   // backfill dags on dst side
  MAX_TYPE
};

inline bool is_valid_ha_diag_task_type(const ObStorageHADiagTaskType t)
{
  return static_cast<uint8_t>(t) < static_cast<uint8_t>(ObStorageHADiagTaskType::MAX_TYPE);
}

const char *ha_diag_task_type_str(const ObStorageHADiagTaskType t);

enum class ObStorageHACostItemName : int //FARM COMPAT WHITELIST
{
  TRANSFER_START_BEGIN = 0,
  LOCK_MEMBER_LIST = 1,
  PRECHECK_LS_REPALY_SCN = 2,
  CHECK_START_STATUS_TRANSFER_TABLETS = 3,
  CHECK_SRC_LS_HAS_ACTIVE_TRANS = 4,
  UPDATE_ALL_TABLET_TO_LS = 5,
  LOCK_TABLET_ON_DEST_LS_FOR_TABLE_LOCK = 6,
  BLOCK_AND_KILL_TX = 7,
  REGISTER_TRANSFER_START_OUT = 8,
  DEST_LS_GET_START_SCN = 9,
  UNBLOCK_TX = 10,
  WAIT_SRC_LS_REPLAY_TO_START_SCN = 11,
  SRC_LS_GET_TABLET_META = 12,
  REGISTER_TRANSFER_START_IN = 13,
  START_TRANS_COMMIT = 14,
  TRANSFER_START_END = 15,
  TRANSFER_FINISH_BEGIN = 16,
  UNLOCK_SRC_AND_DEST_LS_MEMBER_LIST = 17,
  CHECK_LS_LOGICAL_TABLE_REPLACED = 18,
  LOCK_LS_MEMBER_LIST_IN_DOING = 19,
  CHECK_LS_LOGICAL_TABLE_REPLACED_LATER = 20,
  REGISTER_TRANSFER_FINISH_IN = 21,
  WAIT_TRANSFER_TABLET_STATUS_NORMAL = 22,
  WAIT_ALL_LS_REPLICA_REPLAY_FINISH_SCN = 23,
  REGISTER_TRANSFER_OUT = 24,
  UNLOCK_TABLET_FOR_LOCK = 25,
  UNLOCK_LS_MEMBER_LIST = 26,
  FINISH_TRANS_COMMIT = 27,
  TRANSFER_FINISH_END = 28,
  TRANSFER_ABORT_BEGIN = 29,
  UNLOCK_MEMBER_LIST_IN_ABORT = 30,
  ABORT_TRANS_COMMIT = 31,
  TRANSFER_ABORT_END = 32,
  TRANSFER_BACKFILLED_TABLE_BEGIN = 33,
  TX_BACKFILL = 34,
  UNLOCK_MEMBER_LIST_IN_START = 35,
  ON_REGISTER_SUCCESS = 36,
  ON_REPLAY_SUCCESS = 37,
  TRANSFER_REPLACE_BEGIN = 38,
  TRANSFER_REPLACE_END = 39,
  TRANSFER_BACKFILL_START = 40,
  STOP_LS_SCHEDULE_MEMDIUM = 41,
  MAX_NAME
};

// Strings rendered into the module / type / cost_item / result_msg columns
// of the inflight virtual tables and history tables.
extern const char *const ObStorageDiagModuleStr[];
extern const char *const ObStorageDiagTaskTypeStr[];
extern const char *const ObStorageHACostItemNameStr[];
extern const char *const ObTransferErrorDiagMsg[];

} // namespace share
} // namespace oceanbase
#endif
