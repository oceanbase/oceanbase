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

#ifdef SUSPECT_INFO_TYPE_DEF
SUSPECT_INFO_TYPE_DEF(SUSPECT_MEMTABLE_CANT_MINOR_MERGE)
SUSPECT_INFO_TYPE_DEF(SUSPECT_CANT_SCHEDULE_MINOR_MERGE)
SUSPECT_INFO_TYPE_DEF(SUSPECT_CANT_MAJOR_MERGE)
SUSPECT_INFO_TYPE_DEF(SUSPECT_SCHEDULE_MEDIUM_FAILED)
SUSPECT_INFO_TYPE_DEF(SUSPECT_SSTABLE_COUNT_NOT_SAFE)
SUSPECT_INFO_TYPE_DEF(SUSPECT_SUBMIT_LOG_FOR_FREEZE)
SUSPECT_INFO_TYPE_DEF(SUSPECT_REC_SCN_NOT_STABLE)
SUSPECT_INFO_TYPE_DEF(SUSPECT_NOT_READY_FOR_FLUSH)
SUSPECT_INFO_TYPE_DEF(SUSPECT_MEMTABLE_CANT_CREATE_DAG)
SUSPECT_INFO_TYPE_DEF(SUSPECT_INVALID_DATA_VERSION)
SUSPECT_INFO_TYPE_DEF(SUSPECT_FAILED_TO_REFRESH_LS_LOCALITY)
SUSPECT_INFO_TYPE_DEF(SUSPECT_LOCALITY_CHANGE)
SUSPECT_INFO_TYPE_DEF(SUSPECT_INFO_TYPE_MAX)
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
  bool with_comment;
  const char *info_str;
  const char *info_str_fmt[DIAGNOSE_INFO_STR_FMT_MAX_NUM];
};

enum ObSuspectInfoType
{
#define SUSPECT_INFO_TYPE_DEF(suspect_info_type) suspect_info_type,
#include "ob_diagnose_config.h"
#undef SUSPECT_INFO_TYPE_DEF
};

static constexpr ObDiagnoseInfoStruct OB_SUSPECT_INFO_TYPES[] = {
  {2, false, "memtable can not minor merge", {"memtable end_scn", "memtable timestamp"}},
  {3, false, "can't schedule minor merge",
    {"min_snapshot_version", "max_snapshot_version", "mini_sstable_cnt"}},
  {3, false, "need major merge but can't merge now",
    {"medium_snapshot", "is_tablet_data_status_complete", "max_serialized_medium_scn"}},
  {3, false, "schedule medium failed",
    {"compaction_scn", "store_column_cnt", "error_code"}},
  {3, true, "sstable count is not safe", {"major_table_count", "minor_tables_count", "first_minor_start_scn"}},
  {2, false, "traverse_trans_to_submit_redo_log failed", {"ret", "fail_tx_id"}},
  {2, false, "memtable rec_scn not stable", {"rec_scn", "max_consequent_callbacked_scn"}},
  {5, false, "memtable not ready for flush",
    {"is_frozen_memtable", "get_write_ref", "get_unsynced_cnt", "current_right_boundary", "get_end_scn"}},
  {3, false, "memtable can not create dag successfully", {"error_code", "has been ready for flush time", "ready for flush time"}},
  {2, false, "invalid data version to schedule medium merge", {"curr_data_version", "target_data_version"}},
  {1, false, "refresh ls locality cache failed", {"errno"}},
  {1, true, "maybe bad case: locality change and leader change", {"leader_exist"}},
  {0, false, "", {}},
};

static_assert(sizeof(OB_SUSPECT_INFO_TYPES) / sizeof(ObDiagnoseInfoStruct) == SUSPECT_INFO_TYPE_MAX + 1, "Not enough initializer");

static constexpr ObDiagnoseInfoStruct OB_DAG_WARNING_INFO_TYPES[] = {
  {3, true, "DAG_MINI_MERGE", {"ls_id", "tablet_id", "compaction_scn"}},
  {3, true, "DAG_MINOR_MERGE", {"ls_id", "tablet_id", "compaction_scn"}},
  {3, true, "DAG_MAJOR_MERGE", {"ls_id", "tablet_id", "compaction_scn"}},
  {3, true, "DAG_TX_TABLE_MERGE", {"ls_id", "tablet_id", "compaction_scn"}},
  {0, false, "DAG_WRITE_CKPT", {}},
  {3, false, "DAG_TYPE_MDS_TABLE_MERGE", {"ls_id", "tablet_id", "flush_scn"}},

  {7, false, "complement data task",
    {"ls_id", "source_tablet_id", "dest_tablet_id", "data_table_id", "target_table_id", "schema_version", "snapshot_version"}},
  {2, false, "unique check task", {"tablet_id", "index_id"}},
  {0, false, "DAG_SQL_BUILD_INDEX", {}},
  {3, false, "ddl table merge task", {"ls_id", "tablet_id", "rec_scn"}},

  {3, true, "DAG_INITIAL_COMPLETE_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {3, true, "DAG_START_COMPLETE_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {3, true, "DAG_FINISH_COMPLETE_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {3, true, "DAG_INITIAL_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {3, true, "DAG_START_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {3, true, "DAG_SYS_TABLETS_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {4, true, "DAG_TABLET_MIGRATION", {"tenant_id", "ls_id", "tablet_id", "op_type"}},
  {3, true, "DAG_DATA_TABLETS_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {4, true, "DAG_TABLET_GROUP_MIGRATION", {"tenant_id", "ls_id", "first_tablet_id", "op_type"}},
  {3, true, "DAG_MIGRATION_FINISH", {"tenant_id", "ls_id", "op_type"}},
  {3, true, "DAG_INITIAL_PREPARE_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {3, true, "DAG_START_PREPARE_MIGRATION", {"tenant_id", "ls_id", "op_type"}},
  {3, true, "DAG_FINISH_PREPARE_MIGRATION", {"tenant_id", "ls_id", "op_type"}},

  {0, false, "DAG_FAST_MIGRATE", {}},
  {0, false, "DAG_VALIDATE", {}},

  {2, true, "DAG_TABLET_BACKFILL_TX", {"ls_id", "tablet_id"}},
  {1, true, "DAG_FINISH_BACKFILL_TX", {"ls_id"}},

  {1, false, "DAG_BACKUP_META", {"ls_id"}},
  {5, false, "DAG_BACKUP_PREPAER", {"tenant_id", "backup_set_id", "ls_id", "turn_id", "retry_id"}},
  {3, false, "DAG_BACKUP_FINISH", {"tenant_id", "backup_set_id", "ls_id"}},
  {7, false, "DAG_BACKUP_DATA",
    {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id", "task_id"}},
  {7, false, "DAG_PREFETCH_BACKUP_INFO",
    {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id", "task_id"}},
  {6, false, "DAG_BACKUP_INDEX_REBUILD",
    {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id"}},
  {3, false, "DAG_BACKUP_COMPLEMENT_LOG", {"tenant_id", "backup_set_id", "ls_id"}},

  {0, false, "DAG_BACKUP_BACKUPSET", {}},
  {0, false, "DAG_BACKUP_ARCHIVELOG", {}},

  {2, true, "DAG_INITIAL_LS_RESTORE", {"ls_id", "is_leader"}},
  {2, true, "DAG_START_LS_RESTORE", {"ls_id", "is_leader"}},
  {2, true, "DAG_SYS_TABLETS_RESTORE", {"ls_id", "is_leader"}},
  {2, true, "DAG_DATA_TABLETS_META_RESTORE", {"ls_id", "is_leader"}},
  {3, true, "DAG_TABLET_GROUP_META_RESTORE", {"ls_id", "first_tablet_id", "is_leader"}},
  {2, true, "DAG_FINISH_LS_RESTORE", {"ls_id", "is_leader"}},
  {3, true, "DAG_INITIAL_TABLET_GROUP_RESTORE", {"ls_id", "first_tablet_id", "is_leader"}},
  {3, true, "DAG_START_TABLET_GROUP_RESTORE", {"ls_id", "first_tablet_id", "is_leader"}},
  {3, true, "DAG_FINISH_TABLET_GROUP_RESTORE", {"ls_id", "first_tablet_id", "is_leader"}},
  {3, true, "DAG_TABLET_RESTORE", {"ls_id", "tablet_id", "is_leader"}},

  {4, true, "DAG_BACKUP_CLEAN", {"tenant_id", "task_id", "ls_id", "id"}},

  {1, true, "DAG_REMOVE_MEMBER", {"ls_id"}},

  {3, true, "DAG_TRANSFER_BACKFILL_TX", {"tenant_id", "ls_id", "start_scn"}},
  {2, true, "TRANSFER_REPLACE_TABLE", {"tenant_id", "ls_id"}},
  {4, true, "DAG_TTL_TASK", {"tenant_id", "ls_id", "table_id", "tablet_id"}},

  {0, false, "", {}},
};

static_assert(sizeof(OB_DAG_WARNING_INFO_TYPES) / sizeof(ObDiagnoseInfoStruct) == ObDagType::DAG_TYPE_MAX + 1, "Not enough initializer");

} // namespace share
} // namespace oceanbase
#endif
