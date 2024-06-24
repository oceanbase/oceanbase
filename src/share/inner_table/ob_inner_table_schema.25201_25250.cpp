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

#include "share/schema/ob_schema_macro_define.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_table_schema.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
namespace share
{

int ObInnerTableSchema::dba_ob_freeze_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_FREEZE_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_FREEZE_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT FROZEN_SCN,          CLUSTER_VERSION,          SCHEMA_VERSION,          GMT_CREATE,          GMT_MODIFIED   FROM SYS.ALL_VIRTUAL_FREEZE_INFO_REAL_AGENT   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_ls_replica_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_REPLICA_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_REPLICA_TASKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT LS_ID,          TASK_TYPE,          TASK_ID,          TASK_STATUS,          CAST(CASE PRIORITY               WHEN 1 THEN 'HIGH'               WHEN 2 THEN 'LOW'               ELSE NULL END AS CHAR(5)) AS PRIORITY,          TARGET_REPLICA_SVR_IP,          TARGET_REPLICA_SVR_PORT,          TARGET_PAXOS_REPLICA_NUMBER,          TARGET_REPLICA_TYPE,          CASE SOURCE_REPLICA_SVR_IP               WHEN '' THEN NULL               ELSE SOURCE_REPLICA_SVR_IP END AS SOURCE_REPLICA_SVR_IP,          SOURCE_REPLICA_SVR_PORT,          SOURCE_PAXOS_REPLICA_NUMBER,          CASE SOURCE_REPLICA_TYPE               WHEN '' THEN NULL               ELSE SOURCE_REPLICA_TYPE END AS SOURCE_REPLICA_TYPE,          TASK_EXEC_SVR_IP,          TASK_EXEC_SVR_PORT,          CAST(GMT_CREATE AS TIMESTAMP(6)) AS CREATE_TIME,          CAST(SCHEDULE_TIME AS TIMESTAMP(6)) AS START_TIME,          CAST(GMT_MODIFIED AS TIMESTAMP(6)) AS MODIFY_TIME,          "COMMENT"   FROM SYS.ALL_VIRTUAL_LS_REPLICA_TASK   WHERE     TENANT_ID = EFFECTIVE_TENANT_ID()   )   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_ls_replica_task_plan_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_LS_REPLICA_TASK_PLAN_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_LS_REPLICA_TASK_PLAN_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT LS_ID,          TASK_TYPE,          CAST(CASE PRIORITY               WHEN 0 THEN 'HIGH'               WHEN 1 THEN 'LOW'               ELSE NULL END AS CHAR(5)) AS PRIORITY,          TARGET_REPLICA_SVR_IP,          TARGET_REPLICA_SVR_PORT,          TARGET_PAXOS_REPLICA_NUMBER,          TARGET_REPLICA_TYPE,          CASE SOURCE_REPLICA_SVR_IP               WHEN '' THEN NULL               ELSE SOURCE_REPLICA_SVR_IP END AS SOURCE_REPLICA_SVR_IP,          SOURCE_REPLICA_SVR_PORT,          SOURCE_PAXOS_REPLICA_NUMBER,          CASE SOURCE_REPLICA_TYPE               WHEN '' THEN NULL               ELSE SOURCE_REPLICA_TYPE END AS SOURCE_REPLICA_TYPE,          TASK_EXEC_SVR_IP,          TASK_EXEC_SVR_PORT,          "COMMENT"   FROM SYS.ALL_VIRTUAL_LS_REPLICA_TASK_PLAN   )   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_scheduler_windows_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SCHEDULER_WINDOWS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SCHEDULER_WINDOWS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(T.POWNER AS VARCHAR2(128)) AS OWNER,     CAST(T.JOB_NAME AS VARCHAR2(128)) AS WINDOW_NAME,     CAST(NULL AS VARCHAR2(128)) AS RESOURCE_PLAN,     CAST(NULL AS VARCHAR2(4000)) AS SCHEDULE_OWNER,     CAST(NULL AS VARCHAR2(4000)) AS SCHEDULE_NAME,     CAST(NULL AS VARCHAR2(8)) AS SCHEDULE_TYPE,     CAST(T.START_DATE AS TIMESTAMP(6) WITH TIME ZONE) AS START_DATE,     CAST(T.REPEAT_INTERVAL AS VARCHAR2(4000)) AS REPEAT_INTERVAL,     CAST(T.END_DATE AS TIMESTAMP(6) WITH TIME ZONE) AS END_DATE,     CAST((TIMESTAMP'1970-01-01 08:00:00' + T.MAX_RUN_DURATION / (60 * 60 * 24) - TIMESTAMP'1970-01-01 08:00:00') AS INTERVAL DAY(3) TO SECOND(0)) AS DURATION,     CAST(NULL AS VARCHAR2(4)) AS WINDOW_PRIORITY,     CAST(T.NEXT_DATE AS TIMESTAMP(6) WITH TIME ZONE) AS NEXT_RUN_DATE,     CAST(T.LAST_DATE AS TIMESTAMP(6) WITH TIME ZONE) AS LAST_START_DATE,     CAST(T.ENABLED AS VARCHAR2(5)) AS ENABLED,     CAST(NULL AS VARCHAR2(5)) AS ACTIVE,     CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS MANUAL_OPEN_TIME,     CAST(NULL AS INTERVAL DAY(3) TO SECOND(0)) AS MANUAL_DURATION,     CAST(T.COMMENTS AS VARCHAR2(4000)) AS COMMENTS   FROM SYS.ALL_VIRTUAL_TENANT_SCHEDULER_JOB_REAL_AGENT T WHERE T.JOB > 0 AND T.JOB_NAME in ('MONDAY_WINDOW',     'TUESDAY_WINDOW', 'WEDNESDAY_WINDOW', 'THURSDAY_WINDOW', 'FRIDAY_WINDOW', 'SATURDAY_WINDOW', 'SUNDAY_WINDOW')   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_scheduler_windows_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SCHEDULER_WINDOWS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SCHEDULER_WINDOWS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT OWNER,                              WINDOW_NAME,                              RESOURCE_PLAN,                              SCHEDULE_OWNER,                              SCHEDULE_NAME,                              SCHEDULE_TYPE,                              START_DATE,                              REPEAT_INTERVAL,                              END_DATE,                              DURATION,                              WINDOW_PRIORITY,                              NEXT_RUN_DATE,                              LAST_START_DATE,                              ENABLED,                              ACTIVE,                              MANUAL_OPEN_TIME,                              MANUAL_DURATION,                              COMMENTS  FROM SYS.ALL_SCHEDULER_WINDOWS)__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_database_privilege_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_DATABASE_PRIVILEGE_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_DATABASE_PRIVILEGE_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(    SELECT A.USER_ID USER_ID,           B.USER_NAME USERNAME,           A.DATABASE_NAME DATABASE_NAME,           A.GMT_CREATE GMT_CREATE,           A.GMT_MODIFIED GMT_MODIFIED,           (CASE WHEN A.PRIV_ALTER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER,           (CASE WHEN A.PRIV_CREATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE,           (CASE WHEN A.PRIV_DELETE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DELETE,           (CASE WHEN A.PRIV_DROP = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DROP,           (CASE WHEN A.PRIV_GRANT_OPTION = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_GRANT_OPTION,           (CASE WHEN A.PRIV_INSERT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INSERT,           (CASE WHEN A.PRIV_UPDATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_UPDATE,           (CASE WHEN A.PRIV_SELECT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SELECT,           (CASE WHEN A.PRIV_INDEX = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INDEX,           (CASE WHEN A.PRIV_CREATE_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_VIEW,           (CASE WHEN A.PRIV_SHOW_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SHOW_VIEW   FROM SYS.ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT A INNER JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT B         ON A.TENANT_ID = B.TENANT_ID AND A.USER_ID = B.USER_ID;   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_tenants_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TENANTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TENANTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT A.TENANT_ID,        TENANT_NAME,        (CASE             WHEN A.TENANT_ID = 1 THEN 'SYS'             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 'META'             ELSE 'USER'         END) AS TENANT_TYPE,        A.gmt_create AS CREATE_TIME,        A.gmt_modified AS MODIFY_TIME,        PRIMARY_ZONE,        LOCALITY,        CASE previous_locality           WHEN '' THEN NULL           ELSE previous_locality        END AS PREVIOUS_LOCALITY,        CASE compatibility_mode           WHEN 0 THEN 'MYSQL'           WHEN 1 THEN 'ORACLE'           ELSE NULL        END AS COMPATIBILITY_MODE,        STATUS,        CASE in_recyclebin           WHEN 0 THEN 'NO'           ELSE 'YES'        END AS IN_RECYCLEBIN,         CASE locked           WHEN 0 THEN 'NO'           ELSE 'YES'        END AS LOCKED,         (CASE             WHEN A.TENANT_ID = 1 THEN 'PRIMARY'             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 'PRIMARY'             ELSE TENANT_ROLE         END) AS TENANT_ROLE,         (CASE             WHEN A.TENANT_ID = 1 THEN 'NORMAL'             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 'NORMAL'             ELSE SWITCHOVER_STATUS         END) AS SWITCHOVER_STATUS,         (CASE             WHEN A.TENANT_ID = 1 THEN 0             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 0             ELSE SWITCHOVER_EPOCH         END) AS SWITCHOVER_EPOCH,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN NULL             ELSE SYNC_SCN         END) AS SYNC_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN NULL             ELSE REPLAYABLE_SCN         END) AS REPLAYABLE_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN NULL             ELSE READABLE_SCN         END) AS READABLE_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN NULL             ELSE RECOVERY_UNTIL_SCN         END) AS RECOVERY_UNTIL_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN 'NOARCHIVELOG'             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 'NOARCHIVELOG'             ELSE LOG_MODE         END) AS LOG_MODE,        ARBITRATION_SERVICE_STATUS,        UNIT_NUM,        COMPATIBLE,        (CASE             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 1             ELSE B.MAX_LS_ID END) AS MAX_LS_ID,        RESTORE_DATA_MODE FROM SYS.ALL_VIRTUAL_TENANT_SYS_AGENT A LEFT JOIN SYS.ALL_VIRTUAL_TENANT_INFO B     ON A.TENANT_ID = B.TENANT_ID LEFT JOIN      (SELECT TENANT_ID,             (CASE                  WHEN TENANT_ID < 1 THEN NULL                  WHEN TENANT_ID != 1 THEN TENANT_ID - 1                  ELSE NULL              END) AS META_TENANT_ID,             MIN(UNIT_COUNT) AS UNIT_NUM      FROM SYS.ALL_VIRTUAL_RESOURCE_POOL_SYS_AGENT      GROUP BY TENANT_ID) C     ON A.TENANT_ID = C.TENANT_ID OR A.TENANT_ID = C.META_TENANT_ID LEFT JOIN      (SELECT TENANT_ID,             MIN(VALUE) AS COMPATIBLE      FROM SYS.ALL_VIRTUAL_TENANT_PARAMETER      WHERE NAME = 'compatible'      GROUP BY TENANT_ID) D     ON A.TENANT_ID = D.TENANT_ID WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_policies_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_POLICIES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_POLICIES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(P.POLICY_FUNCTION_SCHEMA AS VARCHAR2(128)) AS PF_OWNER,       CAST(P.POLICY_PACKAGE_NAME AS VARCHAR2(128)) AS PACKAGE,       CAST(P.POLICY_FUNCTION_NAME AS VARCHAR2(128)) AS FUNCTION,       CAST(DECODE(BITAND(P.STMT_TYPE,1), 0, 'NO', 'YES') AS VARCHAR2(3)) AS SEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2), 0, 'NO', 'YES') AS VARCHAR2(3)) AS INS,       CAST(DECODE(BITAND(P.STMT_TYPE,4), 0, 'NO', 'YES') AS VARCHAR2(3)) AS UPD,       CAST(DECODE(BITAND(P.STMT_TYPE,8), 0, 'NO', 'YES') AS VARCHAR2(3)) AS DEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2048), 0, 'NO', 'YES') AS VARCHAR2(3)) AS IDX,       CAST(DECODE(P.CHECK_OPT, 0, 'NO', 'YES') AS VARCHAR2(3)) AS CHK_OPTION,       CAST(DECODE(P.ENABLE_FLAG, 0, 'NO', 'YES') AS VARCHAR2(3)) AS ENABLE,       CAST(DECODE(BITAND(P.STMT_TYPE,16), 0, 'NO', 'YES') AS VARCHAR2(3)) AS STATIC_POLICY,       CAST(CASE BITAND(P.STMT_TYPE,16+64+128+256+8192+16384+32768+524288)         WHEN 16 THEN 'STATIC'         WHEN 64 THEN 'SHARED_STATIC'         WHEN 128 THEN 'CONTEXT_SENSITIVE'         WHEN 256 THEN 'SHARED_CONTEXT_SENSITIVE'         WHEN 8192 THEN 'XDS1'         WHEN 16384 THEN 'XDS2'         WHEN 32768 THEN 'XDS3'         WHEN 524288 THEN 'OLS'         ELSE 'DYNAMIC' END AS VARCHAR2(24)) AS POLICY_TYPE,       CAST(DECODE(BITAND(P.STMT_TYPE,512), 512, 'YES', 'NO') AS VARCHAR2(3)) AS LONG_PREDICATE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON P.TENANT_ID = G.TENANT_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON P.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON P.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_policies_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_POLICIES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_POLICIES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(P.POLICY_FUNCTION_SCHEMA AS VARCHAR2(128)) AS PF_OWNER,       CAST(P.POLICY_PACKAGE_NAME AS VARCHAR2(128)) AS PACKAGE,       CAST(P.POLICY_FUNCTION_NAME AS VARCHAR2(128)) AS FUNCTION,       CAST(DECODE(BITAND(P.STMT_TYPE,1), 0, 'NO', 'YES') AS VARCHAR2(3)) AS SEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2), 0, 'NO', 'YES') AS VARCHAR2(3)) AS INS,       CAST(DECODE(BITAND(P.STMT_TYPE,4), 0, 'NO', 'YES') AS VARCHAR2(3)) AS UPD,       CAST(DECODE(BITAND(P.STMT_TYPE,8), 0, 'NO', 'YES') AS VARCHAR2(3)) AS DEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2048), 0, 'NO', 'YES') AS VARCHAR2(3)) AS IDX,       CAST(DECODE(P.CHECK_OPT, 0, 'NO', 'YES') AS VARCHAR2(3)) AS CHK_OPTION,       CAST(DECODE(P.ENABLE_FLAG, 0, 'NO', 'YES') AS VARCHAR2(3)) AS ENABLE,       CAST(DECODE(BITAND(P.STMT_TYPE,16), 0, 'NO', 'YES') AS VARCHAR2(3)) AS STATIC_POLICY,       CAST(CASE BITAND(P.STMT_TYPE,16+64+128+256+8192+16384+32768+524288)         WHEN 16 THEN 'STATIC'         WHEN 64 THEN 'SHARED_STATIC'         WHEN 128 THEN 'CONTEXT_SENSITIVE'         WHEN 256 THEN 'SHARED_CONTEXT_SENSITIVE'         WHEN 8192 THEN 'XDS1'         WHEN 16384 THEN 'XDS2'         WHEN 32768 THEN 'XDS3'         WHEN 524288 THEN 'OLS'         ELSE 'DYNAMIC' END AS VARCHAR2(24)) AS POLICY_TYPE,       CAST(DECODE(BITAND(P.STMT_TYPE,512), 512, 'YES', 'NO') AS VARCHAR2(3)) AS LONG_PREDICATE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON P.TENANT_ID = G.TENANT_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON P.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)         AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON P.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::user_policies_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_POLICIES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_POLICIES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(P.POLICY_FUNCTION_SCHEMA AS VARCHAR2(128)) AS PF_OWNER,       CAST(P.POLICY_PACKAGE_NAME AS VARCHAR2(128)) AS PACKAGE,       CAST(P.POLICY_FUNCTION_NAME AS VARCHAR2(128)) AS FUNCTION,       CAST(DECODE(BITAND(P.STMT_TYPE,1), 0, 'NO', 'YES') AS VARCHAR2(3)) AS SEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2), 0, 'NO', 'YES') AS VARCHAR2(3)) AS INS,       CAST(DECODE(BITAND(P.STMT_TYPE,4), 0, 'NO', 'YES') AS VARCHAR2(3)) AS UPD,       CAST(DECODE(BITAND(P.STMT_TYPE,8), 0, 'NO', 'YES') AS VARCHAR2(3)) AS DEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2048), 0, 'NO', 'YES') AS VARCHAR2(3)) AS IDX,       CAST(DECODE(P.CHECK_OPT, 0, 'NO', 'YES') AS VARCHAR2(3)) AS CHK_OPTION,       CAST(DECODE(P.ENABLE_FLAG, 0, 'NO', 'YES') AS VARCHAR2(3)) AS ENABLE,       CAST(DECODE(BITAND(P.STMT_TYPE,16), 0, 'NO', 'YES') AS VARCHAR2(3)) AS STATIC_POLICY,       CAST(CASE BITAND(P.STMT_TYPE,16+64+128+256+8192+16384+32768+524288)         WHEN 16 THEN 'STATIC'         WHEN 64 THEN 'SHARED_STATIC'         WHEN 128 THEN 'CONTEXT_SENSITIVE'         WHEN 256 THEN 'SHARED_CONTEXT_SENSITIVE'         WHEN 8192 THEN 'XDS1'         WHEN 16384 THEN 'XDS2'         WHEN 32768 THEN 'XDS3'         WHEN 524288 THEN 'OLS'         ELSE 'DYNAMIC' END AS VARCHAR2(24)) AS POLICY_TYPE,       CAST(DECODE(BITAND(P.STMT_TYPE,512), 512, 'YES', 'NO') AS VARCHAR2(3)) AS LONG_PREDICATE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON P.TENANT_ID = G.TENANT_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON P.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND T.DATABASE_ID = USERENV('SCHEMAID')         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_policy_groups_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_POLICY_GROUPS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_POLICY_GROUPS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(G.POLICY_GROUP_NAME AS VARCHAR2(128)) AS POLICY_GROUP,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON G.TENANT_ID = T.TENANT_ID         AND G.TENANT_ID = EFFECTIVE_TENANT_ID()         AND G.TABLE_ID = T.TABLE_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON G.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_policy_groups_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_POLICY_GROUPS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_POLICY_GROUPS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(G.POLICY_GROUP_NAME AS VARCHAR2(128)) AS POLICY_GROUP,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON G.TENANT_ID = T.TENANT_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)         AND G.TENANT_ID = EFFECTIVE_TENANT_ID()         AND G.TABLE_ID = T.TABLE_ID         AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON G.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::user_policy_groups_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_POLICY_GROUPS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_POLICY_GROUPS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(G.POLICY_GROUP_NAME AS VARCHAR2(128)) AS POLICY_GROUP,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON G.TENANT_ID = T.TENANT_ID         AND G.TENANT_ID = EFFECTIVE_TENANT_ID()         AND G.TABLE_ID = T.TABLE_ID         AND T.DATABASE_ID = USERENV('SCHEMAID')         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_policy_contexts_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_POLICY_CONTEXTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_POLICY_CONTEXTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(C.CONTEXT_NAME AS VARCHAR2(128)) AS NAMESPACE,       CAST(C.ATTRIBUTE AS VARCHAR2(128)) AS ATTRIBUTE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT C       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON C.TENANT_ID = T.TENANT_ID         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TABLE_ID = T.TABLE_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON C.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_policy_contexts_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_POLICY_CONTEXTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_POLICY_CONTEXTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(C.CONTEXT_NAME AS VARCHAR2(128)) AS NAMESPACE,       CAST(C.ATTRIBUTE AS VARCHAR2(128)) AS ATTRIBUTE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT C       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON C.TENANT_ID = T.TENANT_ID         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TABLE_ID = T.TABLE_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)         AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON C.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::user_policy_contexts_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_POLICY_CONTEXTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_POLICY_CONTEXTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(C.CONTEXT_NAME AS VARCHAR2(128)) AS NAMESPACE,       CAST(C.ATTRIBUTE AS VARCHAR2(128)) AS ATTRIBUTE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT C       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON C.TENANT_ID = T.TENANT_ID         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TABLE_ID = T.TABLE_ID         AND T.DATABASE_ID = USERENV('SCHEMAID')         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_sec_relevant_cols_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SEC_RELEVANT_COLS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SEC_RELEVANT_COLS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(C.COLUMN_NAME AS VARCHAR2(128)) AS SEC_REL_COLUMN,       CAST(DECODE(BITAND(P.STMT_TYPE,4096), 0, 'NONE', 'ALL_ROWS') AS VARCHAR2(8)) AS COLUMN_OPTION,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT SC       JOIN         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P         ON SC.TENANT_ID = P.TENANT_ID         AND SC.TENANT_ID = EFFECTIVE_TENANT_ID()         AND SC.RLS_POLICY_ID = P.RLS_POLICY_ID       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON SC.TENANT_ID = G.TENANT_ID         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON SC.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON SC.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID       JOIN         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C         ON SC.TENANT_ID = C.TENANT_ID         AND P.TABLE_ID = C.TABLE_ID         AND SC.COLUMN_ID = C.COLUMN_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_sec_relevant_cols_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SEC_RELEVANT_COLS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SEC_RELEVANT_COLS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(C.COLUMN_NAME AS VARCHAR2(128)) AS SEC_REL_COLUMN,       CAST(DECODE(BITAND(P.STMT_TYPE,4096), 0, 'NONE', 'ALL_ROWS') AS VARCHAR2(8)) AS COLUMN_OPTION,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT SC       JOIN         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P         ON SC.TENANT_ID = P.TENANT_ID         AND SC.TENANT_ID = EFFECTIVE_TENANT_ID()         AND SC.RLS_POLICY_ID = P.RLS_POLICY_ID       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON SC.TENANT_ID = G.TENANT_ID         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON SC.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)         AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON SC.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID       JOIN         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C         ON SC.TENANT_ID = C.TENANT_ID         AND P.TABLE_ID = C.TABLE_ID         AND SC.COLUMN_ID = C.COLUMN_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::user_sec_relevant_cols_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_SEC_RELEVANT_COLS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SEC_RELEVANT_COLS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(C.COLUMN_NAME AS VARCHAR2(128)) AS SEC_REL_COLUMN,       CAST(DECODE(BITAND(P.STMT_TYPE,4096), 0, 'NONE', 'ALL_ROWS') AS VARCHAR2(8)) AS COLUMN_OPTION,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT SC       JOIN         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P         ON SC.TENANT_ID = P.TENANT_ID         AND SC.TENANT_ID = EFFECTIVE_TENANT_ID()         AND SC.RLS_POLICY_ID = P.RLS_POLICY_ID       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON SC.TENANT_ID = G.TENANT_ID         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON SC.TENANT_ID = T.TENANT_ID         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)         AND P.TABLE_ID = T.TABLE_ID         AND T.DATABASE_ID = USERENV('SCHEMAID')       JOIN         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C         ON SC.TENANT_ID = C.TENANT_ID         AND P.TABLE_ID = C.TABLE_ID         AND SC.COLUMN_ID = C.COLUMN_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_ls_arb_replica_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_ARB_REPLICA_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_ARB_REPLICA_TASKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(GMT_CREATE AS TIMESTAMP(6)) AS CREATE_TIME,          CAST(GMT_MODIFIED AS TIMESTAMP(6)) AS MODIFY_TIME,          TENANT_ID,          LS_ID,          TASK_ID,          TRACE_ID,          TASK_TYPE,          ARBITRATION_SERVICE,          ARBITRATION_SERVICE_TYPE,          "COMMENT"   FROM SYS.ALL_VIRTUAL_LS_ARB_REPLICA_TASK   WHERE     TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_ls_arb_replica_task_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          LS_ID,          TASK_ID,          EXECUTE_RESULT,          CAST(CREATE_TIME AS TIMESTAMP(6)) AS CREATE_TIME,          CAST(FINISH_TIME AS TIMESTAMP(6)) AS FINISH_TIME,          TRACE_ID,          TASK_TYPE,          ARBITRATION_SERVICE,          ARBITRATION_SERVICE_TYPE,          "COMMENT"   FROM SYS.ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY   WHERE     TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_rsrc_io_directives_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RSRC_IO_DIRECTIVES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RSRC_IO_DIRECTIVES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       PLAN,       GROUP_OR_SUBPLAN,       COMMENTS,       MIN_IOPS,       MAX_IOPS,       WEIGHT_IOPS     FROM        SYS.ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_db_links_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DB_LINKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DB_LINKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT             CAST('PUBLIC' AS VARCHAR2(128)) AS OWNER,            CAST(A.DBLINK_NAME AS VARCHAR2(128)) AS DB_LINK,            CAST(A.USER_NAME AS VARCHAR2(128)) AS USERNAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_NAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_OWNER,            CAST(CASE DRIVER_PROTO WHEN 1 THEN A.CONN_STRING ELSE (A.HOST_IP || ':' || TO_CHAR(A.HOST_PORT)) END AS VARCHAR2(2000))AS HOST,            CAST(A.GMT_CREATE AS DATE) AS CREATED,            CAST('' AS VARCHAR2(3)) AS HIDDEN,            CAST('' AS VARCHAR2(3)) AS SHARD_INTERNAL,            CAST('YES' AS VARCHAR2(3)) AS VALID,            CAST('' AS VARCHAR2(3)) AS INTRA_CDB,            A.TENANT_NAME AS TENANT_NAME,            A.REVERSE_TENANT_NAME AS REVERSE_TENANT_NAME,            A.CLUSTER_NAME AS CLUSTER_NAME,            A.REVERSE_CLUSTER_NAME AS REVERSE_CLUSTER_NAME,            A.REVERSE_HOST_IP AS REVERSE_HOST,            A.REVERSE_HOST_PORT AS REVERSE_PORT,            A.REVERSE_USER_NAME AS REVERSE_USERNAME     FROM SYS.ALL_VIRTUAL_DBLINK_REAL_AGENT A     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID() AND EXISTS (SELECT 1 from            SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB            WHERE DB.DATABASE_ID = USERENV('SCHEMAID')            OR USER_CAN_ACCESS_OBJ(1, A.DBLINK_ID, DB.DATABASE_ID) = 1) )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_db_links_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_DB_LINKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_DB_LINKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT             CAST('PUBLIC' AS VARCHAR2(128)) AS OWNER,            CAST(A.DBLINK_NAME AS VARCHAR2(128)) AS DB_LINK,            CAST(A.USER_NAME AS VARCHAR2(128)) AS USERNAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_NAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_OWNER,            CAST(CASE DRIVER_PROTO WHEN 1 THEN A.CONN_STRING ELSE (A.HOST_IP || ':' || TO_CHAR(A.HOST_PORT)) END AS VARCHAR2(2000))AS HOST,            CAST(A.GMT_CREATE AS DATE) AS CREATED,            CAST('' AS VARCHAR2(3)) AS HIDDEN,            CAST('' AS VARCHAR2(3)) AS SHARD_INTERNAL,            CAST('YES' AS VARCHAR2(3)) AS VALID,            CAST('' AS VARCHAR2(3)) AS INTRA_CDB,            A.TENANT_NAME AS TENANT_NAME,            A.REVERSE_TENANT_NAME AS REVERSE_TENANT_NAME,            A.CLUSTER_NAME AS CLUSTER_NAME,            A.REVERSE_CLUSTER_NAME AS REVERSE_CLUSTER_NAME,            A.REVERSE_HOST_IP AS REVERSE_HOST,            A.REVERSE_HOST_PORT AS REVERSE_PORT,            A.REVERSE_USER_NAME AS REVERSE_USERNAME     FROM SYS.ALL_VIRTUAL_DBLINK_REAL_AGENT A     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID(); )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::user_db_links_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_DB_LINKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_DB_LINKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT            A.DBLINK_NAME AS DB_LINK,            A.USER_NAME AS USERNAME,            CAST('' AS VARCHAR2(30)) AS PASSWORD,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_NAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_OWNER,            CAST(CASE DRIVER_PROTO WHEN 1 THEN A.CONN_STRING ELSE (A.HOST_IP || ':' || TO_CHAR(A.HOST_PORT)) END AS VARCHAR2(2000))AS HOST,            CAST(A.GMT_CREATE AS DATE) AS CREATED,            CAST('' AS VARCHAR2(3)) AS HIDDEN,            CAST('' AS VARCHAR2(3)) AS SHARD_INTERNAL,            CAST('YES' AS VARCHAR2(3)) AS VALID,            CAST('' AS VARCHAR2(3)) AS INTRA_CDB,            A.TENANT_NAME AS TENANT_NAME,            A.REVERSE_TENANT_NAME AS REVERSE_TENANT_NAME,            A.CLUSTER_NAME AS CLUSTER_NAME,            A.REVERSE_CLUSTER_NAME AS REVERSE_CLUSTER_NAME,            A.REVERSE_HOST_IP AS REVERSE_HOST,            A.REVERSE_HOST_PORT AS REVERSE_PORT,            A.REVERSE_USER_NAME AS REVERSE_USERNAME     FROM SYS.ALL_VIRTUAL_DBLINK_REAL_AGENT A,          SYS.ALL_VIRTUAL_USER_REAL_AGENT B     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID() AND           A.OWNER_ID = B.USER_ID AND           B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER'); )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_task_opt_stat_gather_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT         CAST(TENANT_ID           AS     NUMBER) AS TENANT_ID,         CAST(TASK_ID             AS     VARCHAR2(36)) AS TASK_ID,         CAST((CASE  WHEN type = 0 THEN 'MANUAL GATHER'                ELSE ( CASE  WHEN type = 1 THEN 'AUTO GATHER'                          ELSE ( CASE  WHEN type IS NULL THEN NULL                                   ELSE 'UNDEFINED GATHER' END )END ) END ) AS VARCHAR2(16)) AS TYPE,         CAST((CASE WHEN RET_CODE = 0 THEN 'SUCCESS'                 ELSE (CASE WHEN RET_CODE IS NULL THEN NULL                       ELSE (CASE WHEN RET_CODE = -5065 THEN 'CANCELED' ELSE 'FAILED' END) END) END) AS VARCHAR2(8)) AS STATUS,         CAST(TABLE_COUNT         AS     NUMBER) AS TASK_TABLE_COUNT,         CAST(FAILED_COUNT  AS     NUMBER) AS FAILED_COUNT,         CAST(START_TIME          AS     TIMESTAMP(6)) AS TASK_START_TIME,         CAST(END_TIME            AS     TIMESTAMP(6)) AS TASK_END_TIME     FROM         SYS.ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_table_opt_stat_gather_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT         CAST(DB.DATABASE_NAME         AS     VARCHAR2(128)) AS OWNER,         CAST(V.TABLE_NAME             AS     VARCHAR2(256)) AS TABLE_NAME,         CAST(STAT.TASK_ID             AS     VARCHAR2(36)) AS TASK_ID,         CAST((CASE WHEN RET_CODE = 0 THEN 'SUCCESS'                 ELSE (CASE WHEN RET_CODE IS NULL THEN NULL                       ELSE (CASE WHEN RET_CODE = -5065 THEN 'CANCELED' ELSE 'FAILED' END) END) END) AS VARCHAR2(8)) AS STATUS,         CAST(STAT.START_TIME          AS     TIMESTAMP(6)) AS START_TIME,         CAST(STAT.END_TIME            AS     TIMESTAMP(6)) AS END_TIME,         CAST(STAT.MEMORY_USED         AS     NUMBER) AS MEMORY_USED,         CAST(STAT.STAT_REFRESH_FAILED_LIST      AS     VARCHAR2(4096)) AS STAT_REFRESH_FAILED_LIST,         CAST(STAT.PROPERTIES       AS     VARCHAR2(4096)) AS PROPERTIES     FROM     (         (SELECT TENANT_ID,                 DATABASE_ID,                 TABLE_ID,                 TABLE_ID AS PARTITION_ID,                 TABLE_NAME,                 NULL AS PARTITION_NAME,                 NULL AS SUBPARTITION_NAME,                 NULL AS PARTITION_POSITION,                 NULL AS SUBPARTITION_POSITION,                'TABLE' AS OBJECT_TYPE           FROM              SYS.ALL_VIRTUAL_CORE_ALL_TABLE         UNION ALL         SELECT TENANT_ID,                DATABASE_ID,                TABLE_ID,                CASE WHEN PART_LEVEL = 0 THEN TABLE_ID ELSE -1 END AS PARTITION_ID,                TABLE_NAME,                NULL AS PARTITION_NAME,                NULL AS SUBPARTITION_NAME,                NULL AS PARTITION_POSITION,                NULL AS SUBPARTITION_POSITION,                'TABLE' AS OBJECT_TYPE         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         WHERE T.TABLE_TYPE IN (0,2,3,8,9)         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1))     ) V     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db         ON db.tenant_id = V.tenant_id         AND db.database_id = V.database_id         AND V.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN         SYS.ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND STAT.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_wr_active_session_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_WR_ACTIVE_SESSION_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_WR_ACTIVE_SESSION_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT        ASH.CLUSTER_ID AS CLUSTER_ID,        ASH.TENANT_ID AS TENANT_ID,        ASH.SNAP_ID AS SNAP_ID,        ASH.SVR_IP AS SVR_IP,        ASH.SVR_PORT AS SVR_PORT,        ASH.SAMPLE_ID AS SAMPLE_ID,        ASH.SESSION_ID AS SESSION_ID,        ASH.SAMPLE_TIME AS SAMPLE_TIME,        ASH.USER_ID AS USER_ID,        ASH.SESSION_TYPE AS SESSION_TYPE,        CAST(DECODE(ASH.EVENT_NO, 0, 'ON CPU', 'WAITING') AS VARCHAR2(7)) AS SESSION_STATE,       ASH.SQL_ID AS SQL_ID,        ASH.TRACE_ID AS TRACE_ID,        ASH.EVENT_NO AS EVENT_NO,        ASH.EVENT_ID AS EVENT_ID,        ASH.TIME_WAITED AS TIME_WAITED,        ASH.P1 AS P1,        ASH.P2 AS P2,        ASH.P3 AS P3,        ASH.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,        ASH.GROUP_ID AS GROUP_ID,        ASH.TX_ID AS TX_ID,        ASH.BLOCKING_SESSION_ID AS BLOCKING_SESSION_ID,        ASH.TIME_MODEL AS TIME_MODEL,        CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 1) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_PARSE,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 2) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_PL_PARSE,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 4) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_PLAN_CACHE,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 8) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_SQL_OPTIMIZE,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 16) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_SQL_EXECUTION,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 32) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_PX_EXECUTION,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 64) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_SEQUENCE_LOAD,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 128) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_COMMITTING,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 256) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_STORAGE_READ,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 512) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_STORAGE_WRITE,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 1024) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_REMOTE_DAS_EXECUTION,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 2048) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_FILTER_ROWS,       ASH.PROGRAM AS PROGRAM,       ASH.MODULE AS MODULE,        ASH.ACTION AS ACTION,        ASH.CLIENT_ID AS CLIENT_ID,        ASH.BACKTRACE AS BACKTRACE,        ASH.PLAN_ID AS PLAN_ID,       ASH.TM_DELTA_TIME AS TM_DELTA_TIME,       ASH.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME,       ASH.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME,       ASH.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 2048) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_PLSQL_COMPILATION,       CAST(CASE WHEN BITAND(ASH.TIME_MODEL , 4096) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_PLSQL_EXECUTION,       ASH.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,       ASH.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,       ASH.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,       ASH.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,       ASH.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,       ASH.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME   FROM      SYS.ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY ASH,      SYS.ALL_VIRTUAL_WR_SNAPSHOT SNAP    WHERE      ASH.TENANT_ID = EFFECTIVE_TENANT_ID()      AND ASH.CLUSTER_ID = SNAP.CLUSTER_ID      AND ASH.TENANT_ID = SNAP.TENANT_ID      AND ASH.SNAP_ID = SNAP.SNAP_ID      AND SNAP.STATUS = 0;   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_wr_snapshot_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_WR_SNAPSHOT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_WR_SNAPSHOT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CLUSTER_ID,          TENANT_ID,          SNAP_ID,          SVR_IP,          SVR_PORT,          BEGIN_INTERVAL_TIME,          END_INTERVAL_TIME,          SNAP_FLAG,          STARTUP_TIME   FROM SYS.ALL_VIRTUAL_WR_SNAPSHOT   WHERE STATUS = 0          AND TENANT_ID=EFFECTIVE_TENANT_ID();   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_wr_statname_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_WR_STATNAME_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_WR_STATNAME_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CLUSTER_ID,          TENANT_ID,          STAT_ID,          STAT_NAME   FROM SYS.ALL_VIRTUAL_WR_STATNAME   WHERE TENANT_ID=EFFECTIVE_TENANT_ID();   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_wr_sysstat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_WR_SYSSTAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_WR_SYSSTAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT      STAT.CLUSTER_ID AS CLUSTER_ID,      STAT.TENANT_ID AS TENANT_ID,      STAT.SNAP_ID AS SNAP_ID,      STAT.SVR_IP AS SVR_IP,      STAT.SVR_PORT AS SVR_PORT,      STAT.STAT_ID AS STAT_ID,      STAT.VALUE AS VALUE    FROM      SYS.ALL_VIRTUAL_WR_SYSSTAT STAT,      SYS.ALL_VIRTUAL_WR_SNAPSHOT SNAP    WHERE      STAT.TENANT_ID = EFFECTIVE_TENANT_ID()      AND STAT.CLUSTER_ID = SNAP.CLUSTER_ID      AND STAT.TENANT_ID = SNAP.TENANT_ID      AND STAT.SNAP_ID = SNAP.SNAP_ID      AND SNAP.STATUS = 0;   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_log_restore_source_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LOG_RESTORE_SOURCE_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LOG_RESTORE_SOURCE_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,     ID,     TYPE,     VALUE,     RECOVERY_UNTIL_SCN   FROM SYS.ALL_VIRTUAL_LOG_RESTORE_SOURCE   WHERE TENANT_ID=EFFECTIVE_TENANT_ID();   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_external_table_files_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_EXTERNAL_TABLE_FILES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_EXTERNAL_TABLE_FILES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT           B.TABLE_NAME AS TABLE_NAME,           C.DATABASE_NAME AS OWNER,           P.PART_NAME AS PARTITION_NAME,           A.FILE_URL AS FILE_URL,           A.FILE_SIZE AS FILE_SIZE         FROM           SYS.ALL_VIRTUAL_EXTERNAL_TABLE_FILE_REAL_AGENT A           INNER JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B ON A.TABLE_ID = B.TABLE_ID AND bitand((B.TABLE_MODE / 4096), 15) IN (0,1)           INNER JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C ON B.DATABASE_ID = C.DATABASE_ID AND B.TENANT_ID = C.TENANT_ID           LEFT JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON A.PART_ID = P.PART_ID AND P.TENANT_ID = C.TENANT_ID         WHERE B.TENANT_ID = EFFECTIVE_TENANT_ID() AND B.TABLE_TYPE = 14 AND               (A.DELETE_VERSION = 9223372036854775807 OR A.DELETE_VERSION < A.CREATE_VERSION)     )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_ob_external_table_files_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_OB_EXTERNAL_TABLE_FILES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_OB_EXTERNAL_TABLE_FILES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       B.TABLE_NAME AS TABLE_NAME,       C.DATABASE_NAME AS OWNER,       P.PART_NAME AS PARTITION_NAME,       A.FILE_URL AS FILE_URL,       A.FILE_SIZE AS FILE_SIZE     FROM        SYS.ALL_VIRTUAL_EXTERNAL_TABLE_FILE_REAL_AGENT A        INNER JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B ON A.TABLE_ID = B.TABLE_ID  AND bitand((B.TABLE_MODE / 4096), 15) IN (0,1)        INNER JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C ON B.DATABASE_ID = C.DATABASE_ID AND B.TENANT_ID = C.TENANT_ID        LEFT JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON A.PART_ID = P.PART_ID AND P.TENANT_ID = C.TENANT_ID     WHERE B.TENANT_ID = EFFECTIVE_TENANT_ID() AND B.TABLE_TYPE = 14 AND           (C.DATABASE_ID = USERENV('SCHEMAID') OR USER_CAN_ACCESS_OBJ(1, B.TABLE_ID, C.DATABASE_ID) = 1) AND           (A.DELETE_VERSION = 9223372036854775807 OR A.DELETE_VERSION < A.CREATE_VERSION)     )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_balance_jobs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BALANCE_JOBS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BALANCE_JOBS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT JOB_ID,          GMT_CREATE AS CREATE_TIME,          GMT_MODIFIED AS MODIFY_TIME,          BALANCE_STRATEGY_NAME AS BALANCE_STRATEGY,          JOB_TYPE,          TARGET_UNIT_NUM,          TARGET_PRIMARY_ZONE_NUM,          STATUS,          "COMMENT",          MAX_END_TIME   FROM SYS.ALL_VIRTUAL_BALANCE_JOB_REAL_AGENT   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_balance_job_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BALANCE_JOB_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BALANCE_JOB_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT JOB_ID,          CREATE_TIME,          FINISH_TIME,          BALANCE_STRATEGY_NAME AS BALANCE_STRATEGY,          JOB_TYPE,          TARGET_UNIT_NUM,          TARGET_PRIMARY_ZONE_NUM,          STATUS,          "COMMENT",          MAX_END_TIME   FROM SYS.ALL_VIRTUAL_BALANCE_JOB_HISTORY_REAL_AGENT   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_balance_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BALANCE_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BALANCE_TASKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TASK_ID,          GMT_CREATE AS CREATE_TIME,          GMT_MODIFIED AS MODIFY_TIME,          TASK_TYPE,          SRC_LS,          DEST_LS,          PART_LIST,          FINISHED_PART_LIST,          PART_COUNT,          FINISHED_PART_COUNT,          LS_GROUP_ID,          STATUS,          PARENT_LIST,          CHILD_LIST,          CURRENT_TRANSFER_TASK_ID,          JOB_ID,          "COMMENT",          BALANCE_STRATEGY   FROM SYS.ALL_VIRTUAL_BALANCE_TASK_REAL_AGENT   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_balance_task_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BALANCE_TASK_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BALANCE_TASK_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TASK_ID,          CREATE_TIME,          FINISH_TIME,          TASK_TYPE,          SRC_LS,          DEST_LS,          PART_LIST,          FINISHED_PART_LIST,          PART_COUNT,          FINISHED_PART_COUNT,          LS_GROUP_ID,          STATUS,          PARENT_LIST,          CHILD_LIST,          CURRENT_TRANSFER_TASK_ID,          JOB_ID,          "COMMENT",          BALANCE_STRATEGY   FROM SYS.ALL_VIRTUAL_BALANCE_TASK_HISTORY_REAL_AGENT   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_transfer_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TRANSFER_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TRANSFER_TASKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TASK_ID,          GMT_CREATE AS CREATE_TIME,          GMT_MODIFIED AS MODIFY_TIME,          SRC_LS,          DEST_LS,          PART_LIST,          PART_COUNT,          NOT_EXIST_PART_LIST,          LOCK_CONFLICT_PART_LIST,          TABLE_LOCK_TABLET_LIST,          TABLET_LIST,          TABLET_COUNT,          START_SCN,          FINISH_SCN,          STATUS,          TRACE_ID,          RESULT,          BALANCE_TASK_ID,          TABLE_LOCK_OWNER_ID,          "COMMENT"   FROM SYS.ALL_VIRTUAL_TRANSFER_TASK_REAL_AGENT   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_transfer_task_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TRANSFER_TASK_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TRANSFER_TASK_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TASK_ID,          CREATE_TIME,          FINISH_TIME,          SRC_LS,          DEST_LS,          PART_LIST,          PART_COUNT,          NOT_EXIST_PART_LIST,          LOCK_CONFLICT_PART_LIST,          TABLE_LOCK_TABLET_LIST,          TABLET_LIST,          TABLET_COUNT,          START_SCN,          FINISH_SCN,          STATUS,          TRACE_ID,          RESULT,          BALANCE_TASK_ID,          TABLE_LOCK_OWNER_ID,          "COMMENT"   FROM SYS.ALL_VIRTUAL_TRANSFER_TASK_HISTORY_REAL_AGENT   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_ob_px_p2p_datahub_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_PX_P2P_DATAHUB_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_PX_P2P_DATAHUB_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(           SELECT           SVR_IP,           SVR_PORT,           CAST(TRACE_ID AS CHAR(64)) AS TRACE_ID,           CAST(DATAHUB_ID AS NUMBER) AS DATAHUB_ID,           CAST(MESSAGE_TYPE AS VARCHAR2(256)) AS MESSAGE_TYPE,           CAST(TENANT_ID AS NUMBER) as TENANT_ID,           CAST(HOLD_SIZE AS NUMBER) as HOLD_SIZE,           CAST(TIMEOUT_TS AS TIMESTAMP) as TIMEOUT_TS,           CAST(START_TIME AS TIMESTAMP) as START_TIME         FROM SYS.ALL_VIRTUAL_PX_P2P_DATAHUB  )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_px_p2p_datahub_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_PX_P2P_DATAHUB_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_PX_P2P_DATAHUB_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(         SELECT SVR_IP,           SVR_PORT,           TRACE_ID,           DATAHUB_ID,           MESSAGE_TYPE,           TENANT_ID,           HOLD_SIZE,           TIMEOUT_TS,           START_TIME FROM SYS.GV$OB_PX_P2P_DATAHUB     WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_sql_join_filter_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_SQL_JOIN_FILTER_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_SQL_JOIN_FILTER_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(         SELECT           SVR_IP,           SVR_PORT,           CAST(NULL AS NUMBER) AS QC_SESSION_ID,           CAST(NULL AS NUMBER) AS QC_INSTANCE_ID,           CAST(NULL AS NUMBER) AS SQL_PLAN_HASH_VALUE,           CAST(OTHERSTAT_5_VALUE AS NUMBER) as FILTER_ID,           CAST(NULL AS NUMBER) as BITS_SET,           CAST(OTHERSTAT_1_VALUE AS NUMBER) as FILTERED,           CAST(OTHERSTAT_3_VALUE AS NUMBER) as PROBED,           CAST(NULL AS NUMBER) as ACTIVE,           CAST(TENANT_ID AS NUMBER) as CON_ID,           CAST(TRACE_ID AS CHAR(64)) as TRACE_ID         FROM SYS.ALL_VIRTUAL_SQL_PLAN_MONITOR         WHERE plan_operation = 'PHY_JOIN_FILTER'  )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_sql_join_filter_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_SQL_JOIN_FILTER_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_SQL_JOIN_FILTER_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,           SVR_PORT,           QC_SESSION_ID,           QC_INSTANCE_ID,           SQL_PLAN_HASH_VALUE,           FILTER_ID,           BITS_SET,           FILTERED,           PROBED,           ACTIVE,           CON_ID,           TRACE_ID FROM SYS.GV$SQL_JOIN_FILTER     WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_table_stat_stale_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLE_STAT_STALE_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLE_STAT_STALE_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( WITH V AS (SELECT   NVL(T.TABLE_ID, VT.TABLE_ID) AS TABLE_ID,   NVL(T.TABLET_ID, VT.TABLET_ID) AS TABLET_ID,   NVL(T.INSERTS, 0) + NVL(VT.INSERT_ROW_COUNT, 0) - NVL(T.LAST_INSERTS, 0) AS INSERTS,   NVL(T.UPDATES, 0) + NVL(VT.UPDATE_ROW_COUNT, 0) - NVL(T.LAST_UPDATES, 0) AS UPDATES,   NVL(T.DELETES, 0) + NVL(VT.DELETE_ROW_COUNT, 0) - NVL(T.LAST_DELETES, 0) AS DELETES   FROM   SYS.ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT T   FULL JOIN   SYS.ALL_VIRTUAL_DML_STATS VT   ON T.TABLE_ID = VT.TABLE_ID   AND T.TABLET_ID = VT.TABLET_ID   AND T.TENANT_ID = EFFECTIVE_TENANT_ID()   AND VT.TENANT_ID = EFFECTIVE_TENANT_ID() ) SELECT   CAST(TM.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,   CAST(TM.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,   CAST(TM.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,   CAST(TM.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,   CAST(TS.ROW_CNT AS NUMBER) AS LAST_ANALYZED_ROWS,   TS.LAST_ANALYZED AS LAST_ANALYZED_TIME,   CAST(TM.INSERTS AS NUMBER) AS INSERTS,   CAST(TM.UPDATES AS NUMBER) AS UPDATES,   CAST(TM.DELETES AS NUMBER) AS DELETES,   CAST(NVL(CAST(UP.VALCHAR AS NUMBER), CAST(GP.SPARE4 AS NUMBER)) AS NUMBER) STALE_PERCENT,   CAST(CASE WHEN TS.ROW_CNT IS NOT NULL        THEN CASE WHEN (TM.INSERTS + TM.UPDATES + TM.DELETES) > TS.ROW_CNT * NVL(CAST(UP.VALCHAR AS NUMBER), CAST(GP.SPARE4 AS NUMBER)) / 100             THEN 'YES' ELSE 'NO' END        ELSE CASE WHEN (TM.INSERTS + TM.UPDATES + TM.DELETES) > 0             THEN 'YES' ELSE 'NO' END        END AS VARCHAR2(3)) AS IS_STALE FROM (SELECT   T.TENANT_ID,   T.TABLE_ID,   CASE T.PART_LEVEL WHEN 0 THEN T.TABLE_ID WHEN 1 THEN P.PART_ID WHEN 2 THEN SP.SUB_PART_ID END AS PARTITION_ID,   DB.DATABASE_NAME,   T.TABLE_NAME,   P.PART_NAME,   SP.SUB_PART_NAME,   NVL(V.INSERTS, 0) AS INSERTS,   NVL(V.UPDATES, 0) AS UPDATES,   NVL(V.DELETES, 0) AS DELETES FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB   ON DB.DATABASE_ID = T.DATABASE_ID   AND T.TENANT_ID = EFFECTIVE_TENANT_ID()   AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)   AND DB.TENANT_ID = EFFECTIVE_TENANT_ID() LEFT JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P   ON T.TABLE_ID = P.TABLE_ID   AND P.TENANT_ID = EFFECTIVE_TENANT_ID() LEFT JOIN SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP   ON T.TABLE_ID = SP.TABLE_ID   AND P.PART_ID = SP.PART_ID   AND SP.TENANT_ID = EFFECTIVE_TENANT_ID() LEFT JOIN V ON T.TABLE_ID = V.TABLE_ID AND V.TABLET_ID = CASE T.PART_LEVEL WHEN 0 THEN T.TABLET_ID WHEN 1 THEN P.TABLET_ID WHEN 2 THEN SP.TABLET_ID END WHERE T.TABLE_TYPE IN (0, 3, 8, 9) UNION ALL SELECT   MIN(T.TENANT_ID),   MIN(T.TABLE_ID),   -1 AS PARTITION_ID,   DB.DATABASE_NAME,   T.TABLE_NAME,   NULL AS PART_NAME,   NULL AS SUB_PART_NAME,   SUM(NVL(V.INSERTS, 0)) AS INSERTS,   SUM(NVL(V.UPDATES, 0)) AS UPDATES,   SUM(NVL(V.DELETES, 0)) AS DELETES FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB   ON DB.DATABASE_ID = T.DATABASE_ID   AND T.TENANT_ID = EFFECTIVE_TENANT_ID()   AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1) JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P   ON T.TABLE_ID = P.TABLE_ID   AND P.TENANT_ID = EFFECTIVE_TENANT_ID() LEFT JOIN V ON T.TABLE_ID = V.TABLE_ID AND V.TABLET_ID = P.TABLET_ID WHERE T.TABLE_TYPE IN (0, 3, 8, 9) AND T.PART_LEVEL = 1 GROUP BY DB.DATABASE_NAME,          T.TABLE_NAME UNION ALL SELECT   MIN(T.TENANT_ID),   MIN(T.TABLE_ID),   MIN(P.PART_ID) AS PARTITION_ID,   DB.DATABASE_NAME,   T.TABLE_NAME,   P.PART_NAME,   NULL AS SUB_PART_NAME,   SUM(NVL(V.INSERTS, 0)) AS INSERTS,   SUM(NVL(V.UPDATES, 0)) AS UPDATES,   SUM(NVL(V.DELETES, 0)) AS DELETES FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB   ON DB.DATABASE_ID = T.DATABASE_ID   AND T.TENANT_ID = EFFECTIVE_TENANT_ID()   AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1) JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P   ON T.TENANT_ID = P.TENANT_ID AND T.TABLE_ID = P.TABLE_ID JOIN SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP   ON T.TENANT_ID = SP.TENANT_ID AND T.TABLE_ID = SP.TABLE_ID AND P.PART_ID = SP.PART_ID LEFT JOIN V ON T.TABLE_ID = V.TABLE_ID AND V.TABLET_ID = SP.TABLET_ID WHERE T.TABLE_TYPE IN (0, 3, 8, 9) AND T.PART_LEVEL = 2 GROUP BY DB.DATABASE_NAME,         T.TABLE_NAME,         P.PART_NAME UNION ALL SELECT   MIN(T.TENANT_ID),   MIN(T.TABLE_ID),   -1 AS PARTITION_ID,   DB.DATABASE_NAME,   T.TABLE_NAME,   NULL AS PART_NAME,   NULL AS SUB_PART_NAME,   SUM(NVL(V.INSERTS, 0)) AS INSERTS,   SUM(NVL(V.UPDATES, 0)) AS UPDATES,   SUM(NVL(V.DELETES, 0)) AS DELETES FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB   ON DB.DATABASE_ID = T.DATABASE_ID   AND T.TENANT_ID = EFFECTIVE_TENANT_ID()   AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1) JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P   ON T.TABLE_ID = P.TABLE_ID   AND P.TENANT_ID = EFFECTIVE_TENANT_ID() JOIN SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP   ON T.TABLE_ID = SP.TABLE_ID   AND P.PART_ID = SP.PART_ID   AND SP.TENANT_ID = EFFECTIVE_TENANT_ID() LEFT JOIN V ON T.TABLE_ID = V.TABLE_ID AND V.TABLET_ID = SP.TABLET_ID WHERE T.TABLE_TYPE IN (0, 3, 8, 9) AND T.PART_LEVEL = 2 GROUP BY DB.DATABASE_NAME,         T.TABLE_NAME ) TM LEFT JOIN SYS.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT TS   ON TM.TABLE_ID = TS.TABLE_ID   AND TM.PARTITION_ID = TS.PARTITION_ID   AND TM.TENANT_ID = EFFECTIVE_TENANT_ID() LEFT JOIN SYS.ALL_VIRTUAL_OPTSTAT_USER_PREFS_REAL_AGENT UP   ON TM.TABLE_ID = UP.TABLE_ID   AND UP.PNAME = 'STALE_PERCENT'   AND UP.TENANT_ID = EFFECTIVE_TENANT_ID() JOIN SYS.ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_REAL_AGENT GP   ON GP.SNAME = 'STALE_PERCENT' )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}


} // end namespace share
} // end namespace oceanbase
