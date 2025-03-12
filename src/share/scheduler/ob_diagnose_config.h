/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

//SUSPECT_INFO_TYPE_DEF(suspect_info_type, info_priority, with_comment, info_str, int_info_cnt, ...)
#ifdef SUSPECT_INFO_TYPE_DEF
SUSPECT_INFO_TYPE_DEF(SUSPECT_MEMTABLE_CANT_MINOR_MERGE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "memtable can not minor merge",
    2, {"memtable end_scn", "memtable timestamp"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_CANT_SCHEDULE_MINOR_MERGE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_MID, false, "can't schedule minor merge",
    3, {"min_snapshot_version", "max_snapshot_version", "mini_sstable_cnt"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_CANT_MAJOR_MERGE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_MID, false, "need major merge but can't merge now",
    5, {"compaction_scn", "tablet_snapshot_version", "ls_weak_read_ts_ready", "need_force_freeze", "max_serialized_medium_scn"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_SCHEDULE_MEDIUM_FAILED, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_MID, false, "schedule medium failed",
    3, {"compaction_scn", "store_column_cnt", "error_code"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_SSTABLE_COUNT_NOT_SAFE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH, true, "sstable count is not safe",
    4, {"minor_compact_trigger", "major_table_count", "minor_tables_count", "first_minor_start_scn"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_SUBMIT_LOG_FOR_FREEZE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "traverse_trans_to_submit_redo_log failed",
    2, {"ret", "fail_tx_id"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_REC_SCN_NOT_STABLE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "memtable rec_scn not stable",
    2, {"rec_scn", "max_consequent_callbacked_scn"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_NOT_READY_FOR_FLUSH, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_MID, false, "memtable not ready for flush",
    5, {"is_frozen_memtable", "get_write_ref", "get_unsynced_cnt", "current_right_boundary", "get_end_scn"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_MEMTABLE_CANT_CREATE_DAG, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "memtable can not create dag successfully",
    3, {"error_code", "has been ready for flush time", "ready for flush time"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_SUSPEND_MERGE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "merge has been paused",
    2, {"schedule_scn", "is_row_store"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_INVALID_DATA_VERSION, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "invalid data version to schedule medium merge",
    2, {"curr_data_version", "target_data_version"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_FAILED_TO_REFRESH_LS_LOCALITY, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "refresh ls locality cache failed",
    1, {"errno"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_RS_SCHEDULE_ERROR, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_MID, false, "rs check progress failed",
    3, {"compaction_scn", "errno", "unfinish_table_cnt"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_COMPACTION_REPORT_ADD_FAILED, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH, false, "compaction report task add failed",
    1, {"errno"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_COMPACTION_REPORT_PROGRESS_FAILED, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH, false, "compaction report task process failed",
    1, {"errno"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_LS_CANT_MERGE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "ls can't schedule merge",
    2, {"weak_read_ts", "ls_status"})
#ifdef OB_BUILD_SHARED_STORAGE
SUSPECT_INFO_TYPE_DEF(SUSPECT_SS_START_MERGE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "failed to start ss merge",
    2, {"broadcast_version", "error_code"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_LS_MERGE_HUNG, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_MID, false, "ls merge maybe hung",
    2, {"compaction_scn", "ls_state"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_LS_SCHEDULE_DAG, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH, false, "ls failed to schedule verify ckm dag",
    3, {"compaction_scn", "dag_type", "error_code"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_TABLET_CANT_MERGE, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH, false, "tablet can't schedule merge",
    3, {"data_complete", "last_major_snapshot", "is_transfer_tablet"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_UPDATE_TALBET_STATE_FAILED, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH, false, "update tablet state failed",
    3, {"compaction_scn", "is_verified", "is_merged"})
#endif
SUSPECT_INFO_TYPE_DEF(SUSPECT_MV_IN_CREATION, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false,
                      "materialized view creation has not finished", 2, {"schedule_scn", "is_row_store"})
SUSPECT_INFO_TYPE_DEF(SUSPECT_INFO_TYPE_MAX, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, false, "", 0, {})
#endif

#ifndef SRC_SHARE_SCHEDULER_OB_DIAGNOSE_CONFIG_H_
#define SRC_SHARE_SCHEDULER_OB_DIAGNOSE_CONFIG_H_

#include "ob_dag_scheduler_config.h"

namespace oceanbase
{
namespace share
{
static const int64_t DIAGNOSE_INFO_STR_FMT_MAX_NUM = 8;
struct ObDiagnoseInfoStruct {
  int64_t int_size;
  ObDiagnoseInfoPrio priority;
  bool with_comment;
  const char *info_str;
  const char *info_str_fmt[DIAGNOSE_INFO_STR_FMT_MAX_NUM];
};

enum ObSuspectInfoType : uint8_t
{
#define SUSPECT_INFO_TYPE_DEF(suspect_info_type, info_priority, with_comment, info_str, int_info_cnt, ...) suspect_info_type,
#include "ob_diagnose_config.h"
#undef SUSPECT_INFO_TYPE_DEF
};

enum ObDiagnoseTabletType {
  TYPE_SPECIAL, // can't ensure the type
  TYPE_MINI_MERGE,
  TYPE_MINOR_MERGE,
  TYPE_MEDIUM_MERGE,   // for medium & major in storage
  TYPE_REPORT,
  TYPE_RS_MAJOR_MERGE, // for tenant major in RS
  TYPE_TX_TABLE_MERGE,
  TYPE_MDS_MINI_MERGE,
  TYPE_BATCH_EXECUTE, // for batch execute dag
  TYPE_S2_REFRESH, // for shared storage
  TYPE_MICRO_MINI_MERGE,
  TYPE_DIAGNOSE_TABLET_MAX
};

static bool is_valid_diagnose_tablet_type(const ObDiagnoseTabletType type)
{
  return type >= TYPE_SPECIAL && type < TYPE_DIAGNOSE_TABLET_MAX;
}

static constexpr ObDiagnoseInfoStruct OB_SUSPECT_INFO_TYPES[] = {
  #define SUSPECT_INFO_TYPE_DEF(suspect_info_type, info_priority, with_comment, info_str, int_info_cnt, ...) \
    {int_info_cnt, info_priority, with_comment, info_str, ##__VA_ARGS__},
  #include "ob_diagnose_config.h"
  #undef SUSPECT_INFO_TYPE_DEF
};

static_assert(sizeof(OB_SUSPECT_INFO_TYPES) / sizeof(ObDiagnoseInfoStruct) == SUSPECT_INFO_TYPE_MAX + 1, "Not enough initializer");

static constexpr ObDiagnoseInfoStruct OB_DAG_WARNING_INFO_TYPES[] = {
#define DAG_SCHEDULER_DAG_TYPE_DEF(dag_type, init_dag_prio, sys_task_type, dag_type_str, dag_module_str, diagnose_with_comment, diagnose_int_info_cnt, ...) \
    {diagnose_int_info_cnt, ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH, diagnose_with_comment, dag_type_str, ##__VA_ARGS__},
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_TYPE_DEF
};

static_assert(sizeof(OB_DAG_WARNING_INFO_TYPES) / sizeof(ObDiagnoseInfoStruct) == ObDagType::DAG_TYPE_MAX + 1, "Not enough initializer");

} // namespace share
} // namespace oceanbase
#endif
