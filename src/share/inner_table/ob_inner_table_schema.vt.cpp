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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_inner_table_schema.h"

namespace oceanbase
{
namespace share
{
VTMapping vt_mappings[5000];
bool vt_mapping_init()
{
   int64_t start_idx = common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID + 1;
   {
   int64_t idx = OB_ALL_VIRTUAL_AUTO_INCREMENT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_AUTO_INCREMENT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_BALANCE_JOB_HISTORY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_BALANCE_JOB_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_BALANCE_JOB_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_BALANCE_TASK_HISTORY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_BALANCE_TASK_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_BALANCE_TASK_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLL_TYPE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_IDX_COLL_NAME_TYPE_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_IDX_TB_COLUMN_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_IDX_COLUMN_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_IDX_COLUMN_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_STAT_HISTORY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORA_IDX_COLUMN_STAT_HIS_SAVTIME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_STAT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_COLUMN_USAGE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_USAGE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_CONSTRAINT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_IDX_CST_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_CONSTRAINT_IDX_CST_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_CONTEXT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORA_IDX_CTX_NAMESPACE_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DAM_CLEANUP_JOBS_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DAM_LAST_ARCH_TS_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DATABASE_PRIVILEGE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORA_IDX_DB_PRIV_DB_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DATABASE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_IDX_DB_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DATABASE_IDX_DB_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DATA_DICTIONARY_IN_LOG_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DBLINK_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_IDX_OWNER_DBLINK_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_IDX_DBLINK_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DBLINK_IDX_DBLINK_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DBMS_LOCK_ALLOCATED_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DEF_SUB_PART_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_IDX_DEF_SUB_PART_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_EXTERNAL_TABLE_FILE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_FOREIGN_KEY_COLUMN_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_FOREIGN_KEY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_CHILD_TID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_PARENT_TID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_FREEZE_INFO_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_FREEZE_INFO_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_HISTOGRAM_STAT_HISTORY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORA_IDX_HISTOGRAM_STAT_HIS_SAVTIME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_HISTOGRAM_STAT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_JOB_LOG_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_JOB_LOG_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_JOB_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORA_IDX_JOB_POWNER_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_JOB_IDX_JOB_POWNER_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_LS_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_LS_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_MONITOR_MODIFIED_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_OPTSTAT_GLOBAL_PREFS_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_OPTSTAT_USER_PREFS_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_OUTLINE_HISTORY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_OUTLINE_HISTORY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_OUTLINE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_OUTLINE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PACKAGE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_IDX_DB_PKG_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_IDX_PKG_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PACKAGE_IDX_PKG_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PART_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_IDX_PART_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PART_IDX_PART_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PLAN_BASELINE_ITEM_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_IDX_SPM_ITEM_SQL_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_IDX_SPM_ITEM_VALUE_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_PLAN_BASELINE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_PLAN_BASELINE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RECYCLEBIN_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_IDX_RECYCLEBIN_DB_TYPE_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_IDX_RECYCLEBIN_ORI_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RES_MGR_CONSUMER_GROUP_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RES_MGR_DIRECTIVE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RES_MGR_MAPPING_RULE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RES_MGR_PLAN_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_ATTRIBUTE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_ATTRIBUTE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_CONTEXT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_IDX_RLS_CONTEXT_TABLE_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_GROUP_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_IDX_RLS_GROUP_TABLE_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_POLICY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_IDX_RLS_POLICY_TABLE_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_IDX_RLS_POLICY_GROUP_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_RLS_SECURITY_COLUMN_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_ROUTINE_PARAM_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_IDX_ROUTINE_PARAM_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_ROUTINE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_DB_ROUTINE_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_ROUTINE_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_ROUTINE_PKG_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SEQUENCE_OBJECT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_IDX_SEQ_OBJ_DB_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_IDX_SEQ_OBJ_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SEQUENCE_VALUE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SPM_CONFIG_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SPM_CONFIG_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SUB_PART_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_IDX_SUB_PART_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SYNONYM_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_IDX_DB_SYNONYM_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_IDX_SYNONYM_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLEGROUP_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_IDX_TG_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLEGROUP_IDX_TG_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLET_TO_LS_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_IDX_TABLET_TO_LS_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_IDX_TABLET_TO_TABLE_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_PRIVILEGE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_IDX_TB_PRIV_DB_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_IDX_TB_PRIV_TB_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_DATA_TABLE_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_IDX_DATA_TABLE_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_DB_TB_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_IDX_DB_TB_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_TB_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_IDX_TB_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_STAT_HISTORY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORA_IDX_TABLE_STAT_HIS_SAVTIME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TABLE_STAT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_STAT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_CONSTRAINT_COLUMN_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_DEPENDENCY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_IDX_DEPENDENCY_REF_OBJ_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_DIRECTORY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORA_IDX_DIRECTORY_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_ERROR_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_KEYSTORE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_IDX_KEYSTORE_MASTER_KEY_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJAUTH_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_IDX_OBJAUTH_GRANTOR_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_IDX_OBJAUTH_GRANTEE_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJECT_TYPE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_IDX_OBJ_TYPE_DB_OBJ_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_IDX_OBJ_TYPE_OBJ_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_COMPONENT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORA_IDX_OLS_COM_POLICY_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_LABEL_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_POLICY_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_TAG_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_POLICY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_IDX_OLS_POLICY_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_IDX_OLS_POLICY_COL_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_USER_LEVEL_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_IDX_OLS_LEVEL_UID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_IDX_OLS_LEVEL_POLICY_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_PROFILE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_IDX_PROFILE_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_REWRITE_RULES_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_REWRITE_RULES_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_IDX_GRANTEE_ROLE_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SCHEDULER_JOB_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SCHEDULER_PROGRAM_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SECURITY_AUDIT_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_IDX_AUDIT_TYPE_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SYSAUTH_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TABLESPACE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TIME_ZONE_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TIME_ZONE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TRIGGER_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_TRIGGER_BASE_OBJ_ID_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_DB_TRIGGER_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_TRIGGER_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TRANSFER_TASK_HISTORY_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TRANSFER_TASK_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TRANSFER_TASK_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TYPE_ATTR_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_IDX_TYPE_ATTR_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TYPE_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_IDX_DB_TYPE_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TYPE_IDX_DB_TYPE_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_IDX_TYPE_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_TYPE_IDX_TYPE_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_USER_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   {
   int64_t idx = OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_IDX_UR_NAME_REAL_AGENT_TID - start_idx;
   VTMapping &tmp_vt_mapping = vt_mappings[idx];
   tmp_vt_mapping.mapping_tid_ = OB_ALL_USER_IDX_UR_NAME_TID;
   tmp_vt_mapping.is_real_vt_ = true;
   }

   return true;
} // end define vt_mappings

bool inited_vt = vt_mapping_init();

} // end namespace share
} // end namespace oceanbase
