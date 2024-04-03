/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifdef DAG_SCHEDULER_DAG_NET_TYPE_DEF
// DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_ENUM, DAG_NET_TYPE_STR)
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_MIGRATION, "DAG_NET_MIGRATION")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_PREPARE_MIGARTION, "DAG_NET_PREPARE_MIGRATION")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_COMPLETE_MIGARTION, "DAG_NET_COMPLETE_MIGRATION")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_TRANSFER, "DAG_NET_TRANSFER")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_BACKUP, "DAG_NET_BACKUP")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_RESTORE, "DAG_NET_RESTORE")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_BACKUP_CLEAN, "DAG_NET_TYPE_BACKUP_CLEAN")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_CO_MAJOR, "DAG_NET_TYPE_CO_MAJOR")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TRANSFER_BACKFILL_TX, "DAG_NET_TRANSFER_BACKFILL_TX")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_MAX, "DAG_NET_TYPE_MAX")
#endif

#ifdef DAG_SCHEDULER_DAG_PRIO_DEF
// DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_ENUM, DAG_PRIORITY_SCORE, DAG_PRIORITY_STR)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_COMPACTION_HIGH,   6, "PRIO_COMPACTION_HIGH")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_HA_HIGH,        8, "PRIO_HA_HIGH")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_COMPACTION_MID, 6, "PRIO_COMPACTION_MID")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_HA_MID,         5, "PRIO_HA_MID")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_COMPACTION_LOW, 6, "PRIO_COMPACTION_LOW")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_HA_LOW,         2, "PRIO_HA_LOW")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_DDL,            2, "PRIO_DDL")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_DDL_HIGH,       6, "PRIO_DDL_HIGH")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_TTL,            2, "PRIO_TTL")
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_MAX,            0, "INVALID")
#endif

#ifdef DAG_SCHEDULER_DAG_TYPE_DEF
// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_ENUM, DAG_DEFAULT_PRIO, SYS_TASK_TYPE, DAG_TYPE_STR, DAG_MODULE_STR, DIAGNOSE_WITH_COMMENT, DIAGNOSE_PRIORITY, DIAGNOSE_INT_INFO_CNT, DIAGNOSE_INT_FMT_STR)
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MINI_MERGE, ObDagPrio::DAG_PRIO_COMPACTION_HIGH, ObSysTaskType::SSTABLE_MINI_MERGE_TASK, "MINI_MERGE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MERGE_EXECUTE, ObDagPrio::DAG_PRIO_COMPACTION_MID, ObSysTaskType::SSTABLE_MINOR_MERGE_TASK, "MINOR_EXECUTE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MAJOR_MERGE, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "MAJOR_MERGE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_CO_MERGE_BATCH_EXECUTE, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "CO_MERGE_BATCH_EXECUTE", "COMPACTION",
    false, 5, {"ls_id", "tablet_id", "compaction_scn", "start_cg_idx", "end_cg_idx"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_CO_MERGE_PREPARE, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "CO_MERGE_PREPARE", "COMPACTION",
    false, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_CO_MERGE_SCHEDULE, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "CO_MERGE_SCHEDULE", "COMPACTION",
    false, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_CO_MERGE_FINISH, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "CO_MERGE_FINISH", "COMPACTION",
    false, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TX_TABLE_MERGE, ObDagPrio::DAG_PRIO_COMPACTION_HIGH, ObSysTaskType::SPECIAL_TABLE_MERGE_TASK, "TX_TABLE_MERGE", "COMPACTION",
    false, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_WRITE_CKPT, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::WRITE_CKPT_TASK, "WRITE_CKPT", "COMPACTION",
    false, 2, {"ls_id", "tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MDS_MINI_MERGE, ObDagPrio::DAG_PRIO_COMPACTION_HIGH, ObSysTaskType::MDS_MINI_MERGE_TASK, "MDS_MINI_MERGE", "COMPACTION",
    false, 3, {"ls_id", "tablet_id", "flush_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BATCH_FREEZE_TABLETS, ObDagPrio::DAG_PRIO_COMPACTION_HIGH, ObSysTaskType::BATCH_FREEZE_TABLET_TASK, "BATCH_FREEZE", "COMPACTION",
    false, 2, {"ls_id", "tablet_count"})
// NOTICE: if you add/delete a compaction dag type here, remember to alter function is_compaction_dag and get_diagnose_tablet_type in ob_tenant_dag_scheduler.h

DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_DDL, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TASK, "DDL_COMPLEMENT", "DDL",
    true, 7, {"ls_id", "source_tablet_id", "dest_tablet_id", "data_table_id", "target_table_id", "schema_version", "snapshot_version"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_UNIQUE_CHECKING, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TASK, "UNIQUE_CHECK", "DDL",
    true, 2, {"tablet_id", "index_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SQL_BUILD_INDEX, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TASK, "SQL_BUILD_INDEX", "DDL",
    true, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_DDL_KV_MERGE, ObDagPrio::DAG_PRIO_DDL_HIGH, ObSysTaskType::DDL_KV_MERGE_TASK, "DDL_KV_MERGE", "DDL",
    true, 3, {"ls_id", "tablet_id", "rec_scn"})

// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MIGRATE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "MIGRATE", "MIGRATE")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INITIAL_COMPLETE_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "INITIAL_COMPLETE_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_START_COMPLETE_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "START_COMPLETE_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_COMPLETE_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "FINISH_COMPLETE_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INITIAL_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "INITIAL_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_START_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "START_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SYS_TABLETS_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "SYS_TABLETS_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TABLET_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "TABLET_MIGRATION", "MIGRATE",
    true, 4, {"tenant_id", "ls_id", "tablet_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_DATA_TABLETS_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "DATA_TABLETS_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TABLET_GROUP_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "TABLET_GROUP_MIGRATION", "MIGRATE",
    true, 4, {"tenant_id", "ls_id", "first_tablet_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MIGRATION_FINISH, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "MIGRATION_FINISH", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INITIAL_PREPARE_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "INITIAL_PREPARE_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_START_PREPARE_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "START_PREPARE_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_PREPARE_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "FINISH_PREPARE_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
// DAG_TYPE_MIGRATE END
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FAST_MIGRATE, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::MIGRATION_TASK, "FAST_MIGRATE", "MIGRATE",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_VALIDATE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::MIGRATION_TASK, "VALIDATE", "MIGRATE",
    false, 0, {})

// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::BACKFILL_TX_TASK, "BACKFILL_TX", "BACKFILL_TX")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TABLET_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::BACKFILL_TX_TASK, "TABLET_BACKFILL_TX", "BACKFILL_TX",
    true, 2, {"ls_id", "tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::BACKFILL_TX_TASK, "FINISH_BACKFILL_TX", "BACKFILL_TX",
    true, 1, {"ls_id"})

// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP", "BACKUP")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_META, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_META", "BACKUP",
    false, 1, {"ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_PREPARE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_PREPARE", "BACKUP",
    false, 5, {"tenant_id", "backup_set_id", "ls_id", "turn_id", "retry_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_FINISH, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_FINISH", "BACKUP",
    false, 3, {"tenant_id", "backup_set_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_DATA, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_DATA", "BACKUP",
    false, 7, {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id", "task_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_PREFETCH_BACKUP_INFO, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "PREFETCH_BACKUP_INFO", "BACKUP",
    false, 7, {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id", "task_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_INDEX_REBUILD, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_INDEX_REBUILD", "BACKUP",
    false, 6, {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_COMPLEMENT_LOG, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_COMPLEMENT_LOG", "BACKUP",
    false, 3, {"tenant_id", "backup_set_id", "ls_id"})
// DAG_TYPE_BACKUP END
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_BACKUPSET, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_BACKUPSET_TASK, "BACKUP_BACKUPSET", "BACKUP",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_ARCHIVELOG, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_ARCHIVELOG_TASK, "BACKUP_ARCHIVELOG", "BACKUP",
    false, 0, {})

// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "RESTORE", "RESTORE",)
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INITIAL_LS_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "INITIAL_LS_RESTORE", "RESTORE",
    true, 2, {"ls_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_START_LS_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "START_LS_RESTORE", "RESTORE",
    true, 2, {"ls_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SYS_TABLETS_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "SYS_TABLETS_RESTORE", "RESTORE",
    true, 2, {"ls_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_DATA_TABLETS_META_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "DATA_TABLETS_META_RESTORE", "RESTORE",
    true, 2, {"ls_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TABLET_GROUP_META_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "TABLET_GROUP_META_RESTORE", "RESTORE",
    true, 3, {"ls_id", "first_tablet_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_LS_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "FINISH_LS_RESTORE", "RESTORE",
    true, 2, {"ls_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INITIAL_TABLET_GROUP_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "INITIAL_TABLET_GROUP_RESTORE", "RESTORE",
    true, 3, {"ls_id", "first_tablet_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_START_TABLET_GROUP_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "START_TABLET_GROUP_RESTORE", "RESTORE",
    true, 3, {"ls_id", "first_tablet_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_TABLET_GROUP_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "FINISH_TABLET_GROUP_RESTORE", "RESTORE",
    true, 3, {"ls_id", "first_tablet_id", "is_leader"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TABLET_RESTORE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::RESTORE_TASK, "TABLET_RESTORE", "RESTORE",
    true, 3, {"ls_id", "tablet_id", "is_leader"})
// DAG_TYPE_RESTORE END

DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_CLEAN, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_CLEAN_TASK, "BACKUP_CLEAN", "BACKUP_CLEAN",
    true, 4, {"tenant_id", "task_id", "ls_id", "id"})

DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_REMOVE_MEMBER, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::REMOVE_MEMBER_TASK, "REMOVE_MEMBER", "REMOVE_MEMBER",
    true, 1, {"ls_id"})

// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TRANSFER, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "TRANSFER", "TRANSFER")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TRANSFER_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "TRANSFER_BACKFILL_TX", "TRANSFER",
    true, 3, {"tenant_id", "src_ls_id", "start_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TRANSFER_REPLACE_TABLE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "TRANSFER_REPLACE_TABLE", "TRANSFER",
    true, 2, {"tenant_id", "desc_ls_id"})
// DAG_TYPE_TRANSFER END
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TTL, ObDagPrio::DAG_PRIO_TTL, ObSysTaskType::TABLE_API_TTL_TASK, "TTL_DELTE_DAG", "TTL",
    false, 4, {"tenant_id", "ls_id", "table_id", "tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TENANT_SNAPSHOT_CREATE, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::TENANT_SNAPSHOT_CREATE_TASK, "TENANT_SNAPSHOT_CREATE", "TSNAP_CR8",
    false, 1, {"tsnap_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TENANT_SNAPSHOT_GC, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::TENANT_SNAPSHOT_GC_TASK, "TENANT_SNAPSHOT_GC","TSNAP_GC",
    false, 1, {"tsnap_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MAX, ObDagPrio::DAG_PRIO_MAX, ObSysTaskType::MAX_SYS_TASK_TYPE, "DAG_TYPE_MAX", "INVALID", false, 0, {})
#endif

#ifdef DIAGNOSE_INFO_PRIORITY_DEF
DIAGNOSE_INFO_PRIORITY_DEF(DIAGNOSE_PRIORITY_LOW)
DIAGNOSE_INFO_PRIORITY_DEF(DIAGNOSE_PRIORITY_MID)
DIAGNOSE_INFO_PRIORITY_DEF(DIAGNOSE_PRIORITY_HIGH)
#endif

#ifndef SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_CONFIG_H_
#define SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_CONFIG_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_sys_task_stat.h"

namespace oceanbase
{
namespace share
{

enum class ObDiagnoseInfoPrio : uint32_t
{
#define DIAGNOSE_INFO_PRIORITY_DEF(diagnose_info_prio) diagnose_info_prio,
#include "ob_dag_scheduler_config.h"
#undef DIAGNOSE_INFO_PRIORITY_DEF
};

struct ObDagPrioStruct
{
  int64_t score_;
  const char *dag_prio_str_;
  TO_STRING_KV(K_(score), K_(dag_prio_str));
};

struct ObDagPrio
{
  enum ObDagPrioEnum
  {
#define DAG_SCHEDULER_DAG_PRIO_DEF(dag_prio, score, dag_prio_str) dag_prio,
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_PRIO_DEF
  };
};

static constexpr ObDagPrioStruct OB_DAG_PRIOS[] = {
#define DAG_SCHEDULER_DAG_PRIO_DEF(dag_prio, score, dag_prio_str) \
    {score, dag_prio_str},
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_PRIO_DEF
};

struct ObDagNetTypeStruct
{
  const char *dag_net_type_str_;
  TO_STRING_KV(K_(dag_net_type_str));
};

struct ObDagNetType
{
  enum ObDagNetTypeEnum
  {
#define DAG_SCHEDULER_DAG_NET_TYPE_DEF(dag_net_type, dag_net_type_str) dag_net_type,
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_NET_TYPE_DEF
  };
};

static constexpr ObDagNetTypeStruct OB_DAG_NET_TYPES[] = {
#define DAG_SCHEDULER_DAG_NET_TYPE_DEF(dag_net_type, dag_net_type_str) \
    {dag_net_type_str},
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_NET_TYPE_DEF
};

struct ObDagTypeStruct
{
  ObDagPrio::ObDagPrioEnum init_dag_prio_;
  ObSysTaskType sys_task_type_;
  const char *dag_type_str_;
  const char *dag_module_str_;
  TO_STRING_KV(K_(init_dag_prio), K_(sys_task_type), K_(dag_type_str), K_(dag_module_str));
};

struct ObDagType
{
  enum ObDagTypeEnum
  {
#define DAG_SCHEDULER_DAG_TYPE_DEF(dag_type, init_dag_prio, sys_task_type, dag_type_str, dag_module_str, diagnose_with_comment, diagnose_priority, diagnose_int_info_cnt, ...) dag_type,
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_TYPE_DEF
  };
};

static constexpr ObDagTypeStruct OB_DAG_TYPES[] = {
#define DAG_SCHEDULER_DAG_TYPE_DEF(dag_type, init_dag_prio, sys_task_type, dag_type_str, dag_module_str, diagnose_with_comment, diagnose_priority, diagnose_int_info_cnt, ...) \
    {init_dag_prio, sys_task_type, dag_type_str, dag_module_str},
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_TYPE_DEF
};

} // namespace share
} // namespace oceanbase
#endif
