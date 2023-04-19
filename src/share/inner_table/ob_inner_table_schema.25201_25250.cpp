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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(T.POWNER AS VARCHAR2(128)) AS OWNER,     CAST(T.JOB_NAME AS VARCHAR2(128)) AS WINDOW_NAME,     CAST(NULL AS VARCHAR2(128)) AS RESOURCE_PLAN,     CAST(NULL AS VARCHAR2(4000)) AS SCHEDULE_OWNER,     CAST(NULL AS VARCHAR2(4000)) AS SCHEDULE_NAME,     CAST(NULL AS VARCHAR2(8)) AS SCHEDULE_TYPE,     CAST(T.START_DATE AS TIMESTAMP(6) WITH TIME ZONE) AS START_DATE,     CAST(T.REPEAT_INTERVAL AS VARCHAR2(4000)) AS REPEAT_INTERVAL,     CAST(T.END_DATE AS TIMESTAMP(6) WITH TIME ZONE) AS END_DATE,     CAST((TIMESTAMP'1970-01-01 08:00:00' + T.MAX_RUN_DURATION / (60 * 60 * 24) - TIMESTAMP'1970-01-01 08:00:00') AS INTERVAL DAY(3) TO SECOND(0)) AS DURATION,     CAST(NULL AS VARCHAR2(4)) AS WINDOW_PRIORITY,     CAST(T.NEXT_DATE AS TIMESTAMP(6) WITH TIME ZONE) AS NEXT_RUN_DATE,     CAST(T.LAST_DATE AS TIMESTAMP(6) WITH TIME ZONE) AS LAST_START_DATE,     CAST(T.ENABLED AS VARCHAR2(5)) AS ENABLED,     CAST(NULL AS VARCHAR2(5)) AS ACTIVE,     CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS MANUAL_OPEN_TIME,     CAST(NULL AS INTERVAL DAY(3) TO SECOND(0)) AS MANUAL_DURATION,     CAST(T.COMMENTS AS VARCHAR2(4000)) AS COMMENTS   FROM SYS.ALL_VIRTUAL_TENANT_SCHEDULER_JOB_REAL_AGENT T WHERE T.JOB_NAME in ('MONDAY_WINDOW',     'TUESDAY_WINDOW', 'WEDNESDAY_WINDOW', 'THURSDAY_WINDOW', 'FRIDAY_WINDOW', 'SATURDAY_WINDOW', 'SUNDAY_WINDOW')   )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT * FROM SYS.ALL_SCHEDULER_WINDOWS)__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT A.TENANT_ID,        TENANT_NAME,        (CASE             WHEN A.TENANT_ID = 1 THEN 'SYS'             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 'META'             ELSE 'USER'         END) AS TENANT_TYPE,        A.gmt_create AS CREATE_TIME,        A.gmt_modified AS MODIFY_TIME,        PRIMARY_ZONE,        LOCALITY,        CASE previous_locality           WHEN '' THEN NULL           ELSE previous_locality        END AS PREVIOUS_LOCALITY,        CASE compatibility_mode           WHEN 0 THEN 'MYSQL'           WHEN 1 THEN 'ORACLE'           ELSE NULL        END AS COMPATIBILITY_MODE,        STATUS,        CASE in_recyclebin           WHEN 0 THEN 'NO'           ELSE 'YES'        END AS IN_RECYCLEBIN,         CASE locked           WHEN 0 THEN 'NO'           ELSE 'YES'        END AS LOCKED,         (CASE             WHEN A.TENANT_ID = 1 THEN 'PRIMARY'             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 'PRIMARY'             ELSE TENANT_ROLE         END) AS TENANT_ROLE,         (CASE             WHEN A.TENANT_ID = 1 THEN 'NORMAL'             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 'NORMAL'             ELSE SWITCHOVER_STATUS         END) AS SWITCHOVER_STATUS,         (CASE             WHEN A.TENANT_ID = 1 THEN 0             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 0             ELSE SWITCHOVER_EPOCH         END) AS SWITCHOVER_EPOCH,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN NULL             ELSE SYNC_SCN         END) AS SYNC_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN NULL             ELSE REPLAYABLE_SCN         END) AS REPLAYABLE_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN NULL             ELSE READABLE_SCN         END) AS READABLE_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN NULL             ELSE RECOVERY_UNTIL_SCN         END) AS RECOVERY_UNTIL_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN 'NOARCHIVELOG'             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 'NOARCHIVELOG'             ELSE LOG_MODE         END) AS LOG_MODE,        ARBITRATION_SERVICE_STATUS FROM SYS.ALL_VIRTUAL_TENANT_SYS_AGENT A LEFT JOIN SYS.ALL_VIRTUAL_TENANT_INFO B     ON A.TENANT_ID = B.TENANT_ID WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(P.POLICY_FUNCTION_SCHEMA AS VARCHAR2(128)) AS PF_OWNER,       CAST(P.POLICY_PACKAGE_NAME AS VARCHAR2(128)) AS PACKAGE,       CAST(P.POLICY_FUNCTION_NAME AS VARCHAR2(128)) AS FUNCTION,       CAST(DECODE(BITAND(P.STMT_TYPE,1), 0, 'NO', 'YES') AS VARCHAR2(3)) AS SEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2), 0, 'NO', 'YES') AS VARCHAR2(3)) AS INS,       CAST(DECODE(BITAND(P.STMT_TYPE,4), 0, 'NO', 'YES') AS VARCHAR2(3)) AS UPD,       CAST(DECODE(BITAND(P.STMT_TYPE,8), 0, 'NO', 'YES') AS VARCHAR2(3)) AS DEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2048), 0, 'NO', 'YES') AS VARCHAR2(3)) AS IDX,       CAST(DECODE(P.CHECK_OPT, 0, 'NO', 'YES') AS VARCHAR2(3)) AS CHK_OPTION,       CAST(DECODE(P.ENABLE_FLAG, 0, 'NO', 'YES') AS VARCHAR2(3)) AS ENABLE,       CAST(DECODE(BITAND(P.STMT_TYPE,16), 0, 'NO', 'YES') AS VARCHAR2(3)) AS STATIC_POLICY,       CAST(CASE BITAND(P.STMT_TYPE,16+64+128+256+8192+16384+32768+524288)         WHEN 16 THEN 'STATIC'         WHEN 64 THEN 'SHARED_STATIC'         WHEN 128 THEN 'CONTEXT_SENSITIVE'         WHEN 256 THEN 'SHARED_CONTEXT_SENSITIVE'         WHEN 8192 THEN 'XDS1'         WHEN 16384 THEN 'XDS2'         WHEN 32768 THEN 'XDS3'         WHEN 524288 THEN 'OLS'         ELSE 'DYNAMIC' END AS VARCHAR2(24)) AS POLICY_TYPE,       CAST(DECODE(BITAND(P.STMT_TYPE,512), 512, 'YES', 'NO') AS VARCHAR2(3)) AS LONG_PREDICATE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON P.TENANT_ID = G.TENANT_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON P.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON P.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(P.POLICY_FUNCTION_SCHEMA AS VARCHAR2(128)) AS PF_OWNER,       CAST(P.POLICY_PACKAGE_NAME AS VARCHAR2(128)) AS PACKAGE,       CAST(P.POLICY_FUNCTION_NAME AS VARCHAR2(128)) AS FUNCTION,       CAST(DECODE(BITAND(P.STMT_TYPE,1), 0, 'NO', 'YES') AS VARCHAR2(3)) AS SEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2), 0, 'NO', 'YES') AS VARCHAR2(3)) AS INS,       CAST(DECODE(BITAND(P.STMT_TYPE,4), 0, 'NO', 'YES') AS VARCHAR2(3)) AS UPD,       CAST(DECODE(BITAND(P.STMT_TYPE,8), 0, 'NO', 'YES') AS VARCHAR2(3)) AS DEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2048), 0, 'NO', 'YES') AS VARCHAR2(3)) AS IDX,       CAST(DECODE(P.CHECK_OPT, 0, 'NO', 'YES') AS VARCHAR2(3)) AS CHK_OPTION,       CAST(DECODE(P.ENABLE_FLAG, 0, 'NO', 'YES') AS VARCHAR2(3)) AS ENABLE,       CAST(DECODE(BITAND(P.STMT_TYPE,16), 0, 'NO', 'YES') AS VARCHAR2(3)) AS STATIC_POLICY,       CAST(CASE BITAND(P.STMT_TYPE,16+64+128+256+8192+16384+32768+524288)         WHEN 16 THEN 'STATIC'         WHEN 64 THEN 'SHARED_STATIC'         WHEN 128 THEN 'CONTEXT_SENSITIVE'         WHEN 256 THEN 'SHARED_CONTEXT_SENSITIVE'         WHEN 8192 THEN 'XDS1'         WHEN 16384 THEN 'XDS2'         WHEN 32768 THEN 'XDS3'         WHEN 524288 THEN 'OLS'         ELSE 'DYNAMIC' END AS VARCHAR2(24)) AS POLICY_TYPE,       CAST(DECODE(BITAND(P.STMT_TYPE,512), 512, 'YES', 'NO') AS VARCHAR2(3)) AS LONG_PREDICATE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON P.TENANT_ID = G.TENANT_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON P.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON P.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(P.POLICY_FUNCTION_SCHEMA AS VARCHAR2(128)) AS PF_OWNER,       CAST(P.POLICY_PACKAGE_NAME AS VARCHAR2(128)) AS PACKAGE,       CAST(P.POLICY_FUNCTION_NAME AS VARCHAR2(128)) AS FUNCTION,       CAST(DECODE(BITAND(P.STMT_TYPE,1), 0, 'NO', 'YES') AS VARCHAR2(3)) AS SEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2), 0, 'NO', 'YES') AS VARCHAR2(3)) AS INS,       CAST(DECODE(BITAND(P.STMT_TYPE,4), 0, 'NO', 'YES') AS VARCHAR2(3)) AS UPD,       CAST(DECODE(BITAND(P.STMT_TYPE,8), 0, 'NO', 'YES') AS VARCHAR2(3)) AS DEL,       CAST(DECODE(BITAND(P.STMT_TYPE,2048), 0, 'NO', 'YES') AS VARCHAR2(3)) AS IDX,       CAST(DECODE(P.CHECK_OPT, 0, 'NO', 'YES') AS VARCHAR2(3)) AS CHK_OPTION,       CAST(DECODE(P.ENABLE_FLAG, 0, 'NO', 'YES') AS VARCHAR2(3)) AS ENABLE,       CAST(DECODE(BITAND(P.STMT_TYPE,16), 0, 'NO', 'YES') AS VARCHAR2(3)) AS STATIC_POLICY,       CAST(CASE BITAND(P.STMT_TYPE,16+64+128+256+8192+16384+32768+524288)         WHEN 16 THEN 'STATIC'         WHEN 64 THEN 'SHARED_STATIC'         WHEN 128 THEN 'CONTEXT_SENSITIVE'         WHEN 256 THEN 'SHARED_CONTEXT_SENSITIVE'         WHEN 8192 THEN 'XDS1'         WHEN 16384 THEN 'XDS2'         WHEN 32768 THEN 'XDS3'         WHEN 524288 THEN 'OLS'         ELSE 'DYNAMIC' END AS VARCHAR2(24)) AS POLICY_TYPE,       CAST(DECODE(BITAND(P.STMT_TYPE,512), 512, 'YES', 'NO') AS VARCHAR2(3)) AS LONG_PREDICATE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON P.TENANT_ID = G.TENANT_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON P.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND T.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(G.POLICY_GROUP_NAME AS VARCHAR2(128)) AS POLICY_GROUP,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON G.TENANT_ID = T.TENANT_ID         AND G.TENANT_ID = EFFECTIVE_TENANT_ID()         AND G.TABLE_ID = T.TABLE_ID       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON G.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(G.POLICY_GROUP_NAME AS VARCHAR2(128)) AS POLICY_GROUP,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON G.TENANT_ID = T.TENANT_ID         AND G.TENANT_ID = EFFECTIVE_TENANT_ID()         AND G.TABLE_ID = T.TABLE_ID         AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON G.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(G.POLICY_GROUP_NAME AS VARCHAR2(128)) AS POLICY_GROUP,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON G.TENANT_ID = T.TENANT_ID         AND G.TENANT_ID = EFFECTIVE_TENANT_ID()         AND G.TABLE_ID = T.TABLE_ID         AND T.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(C.CONTEXT_NAME AS VARCHAR2(128)) AS NAMESPACE,       CAST(C.ATTRIBUTE AS VARCHAR2(128)) AS ATTRIBUTE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT C       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON C.TENANT_ID = T.TENANT_ID         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TABLE_ID = T.TABLE_ID       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON C.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(C.CONTEXT_NAME AS VARCHAR2(128)) AS NAMESPACE,       CAST(C.ATTRIBUTE AS VARCHAR2(128)) AS ATTRIBUTE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT C       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON C.TENANT_ID = T.TENANT_ID         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TABLE_ID = T.TABLE_ID         AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON C.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(C.CONTEXT_NAME AS VARCHAR2(128)) AS NAMESPACE,       CAST(C.ATTRIBUTE AS VARCHAR2(128)) AS ATTRIBUTE,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT C       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON C.TENANT_ID = T.TENANT_ID         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TABLE_ID = T.TABLE_ID         AND T.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(C.COLUMN_NAME AS VARCHAR2(128)) AS SEC_REL_COLUMN,       CAST(DECODE(BITAND(P.STMT_TYPE,4096), 0, 'NONE', 'ALL_ROWS') AS VARCHAR2(8)) AS COLUMN_OPTION,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT SC       JOIN         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P         ON SC.TENANT_ID = P.TENANT_ID         AND SC.TENANT_ID = EFFECTIVE_TENANT_ID()         AND SC.RLS_POLICY_ID = P.RLS_POLICY_ID       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON SC.TENANT_ID = G.TENANT_ID         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON SC.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON SC.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID       JOIN         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C         ON SC.TENANT_ID = C.TENANT_ID         AND P.TABLE_ID = C.TABLE_ID         AND SC.COLUMN_ID = C.COLUMN_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OBJECT_OWNER,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(C.COLUMN_NAME AS VARCHAR2(128)) AS SEC_REL_COLUMN,       CAST(DECODE(BITAND(P.STMT_TYPE,4096), 0, 'NONE', 'ALL_ROWS') AS VARCHAR2(8)) AS COLUMN_OPTION,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT SC       JOIN         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P         ON SC.TENANT_ID = P.TENANT_ID         AND SC.TENANT_ID = EFFECTIVE_TENANT_ID()         AND SC.RLS_POLICY_ID = P.RLS_POLICY_ID       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON SC.TENANT_ID = G.TENANT_ID         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON SC.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)       JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON SC.TENANT_ID = DB.TENANT_ID         AND T.DATABASE_ID = DB.DATABASE_ID       JOIN         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C         ON SC.TENANT_ID = C.TENANT_ID         AND P.TABLE_ID = C.TABLE_ID         AND SC.COLUMN_ID = C.COLUMN_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,       CAST(NVL(G.POLICY_GROUP_NAME, 'SYS_DEFAULT') AS VARCHAR2(128)) AS POLICY_GROUP,       CAST(P.POLICY_NAME AS VARCHAR2(128)) AS POLICY_NAME,       CAST(C.COLUMN_NAME AS VARCHAR2(128)) AS SEC_REL_COLUMN,       CAST(DECODE(BITAND(P.STMT_TYPE,4096), 0, 'NONE', 'ALL_ROWS') AS VARCHAR2(8)) AS COLUMN_OPTION,       CAST('NO' AS VARCHAR2(3)) AS COMMON,       CAST('NO' AS VARCHAR2(3)) AS INHERITED     FROM         SYS.ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT SC       JOIN         SYS.ALL_VIRTUAL_RLS_POLICY_REAL_AGENT P         ON SC.TENANT_ID = P.TENANT_ID         AND SC.TENANT_ID = EFFECTIVE_TENANT_ID()         AND SC.RLS_POLICY_ID = P.RLS_POLICY_ID       LEFT JOIN         SYS.ALL_VIRTUAL_RLS_GROUP_REAL_AGENT G         ON SC.TENANT_ID = G.TENANT_ID         AND P.RLS_GROUP_ID = G.RLS_GROUP_ID       JOIN         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         ON SC.TENANT_ID = T.TENANT_ID         AND P.TABLE_ID = T.TABLE_ID         AND T.DATABASE_ID = USERENV('SCHEMAID')       JOIN         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C         ON SC.TENANT_ID = C.TENANT_ID         AND P.TABLE_ID = C.TABLE_ID         AND SC.COLUMN_ID = C.COLUMN_ID )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT             B.USER_NAME AS OWNER,            A.DBLINK_NAME AS DB_LINK,            A.USER_NAME AS USERNAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_NAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_OWNER,            CAST(CASE DRIVER_PROTO WHEN 1 THEN A.CONN_STRING ELSE (A.HOST_IP || ':' || TO_CHAR(A.HOST_PORT)) END AS VARCHAR2(2000))AS HOST,            CAST(A.GMT_CREATE AS DATE) AS CREATED,            CAST('' AS VARCHAR2(3)) AS HIDDEN,            CAST('' AS VARCHAR2(3)) AS SHARD_INTERNAL,            CAST('YES' AS VARCHAR2(3)) AS VALID,            CAST('' AS VARCHAR2(3)) AS INTRA_CDB,            A.TENANT_NAME AS TENANT_NAME,            A.REVERSE_TENANT_NAME AS REVERSE_TENANT_NAME,            A.CLUSTER_NAME AS CLUSTER_NAME,            A.REVERSE_CLUSTER_NAME AS REVERSE_CLUSTER_NAME,            A.REVERSE_HOST_IP AS REVERSE_HOST,            A.REVERSE_HOST_PORT AS REVERSE_PORT,            A.REVERSE_USER_NAME AS REVERSE_USERNAME     FROM SYS.ALL_VIRTUAL_DBLINK_REAL_AGENT A,           SYS.ALL_VIRTUAL_USER_REAL_AGENT B,          SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID() AND            A.OWNER_ID = B.USER_ID AND B.USER_NAME = DB.DATABASE_NAME AND           (DB.DATABASE_ID = USERENV('SCHEMAID') OR USER_CAN_ACCESS_OBJ(1, A.DBLINK_ID, DB.DATABASE_ID) = 1) )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT             B.USER_NAME AS OWNER,            A.DBLINK_NAME AS DB_LINK,            A.USER_NAME AS USERNAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_NAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_OWNER,            CAST(CASE DRIVER_PROTO WHEN 1 THEN A.CONN_STRING ELSE (A.HOST_IP || ':' || TO_CHAR(A.HOST_PORT)) END AS VARCHAR2(2000))AS HOST,            CAST(A.GMT_CREATE AS DATE) AS CREATED,            CAST('' AS VARCHAR2(3)) AS HIDDEN,            CAST('' AS VARCHAR2(3)) AS SHARD_INTERNAL,            CAST('YES' AS VARCHAR2(3)) AS VALID,            CAST('' AS VARCHAR2(3)) AS INTRA_CDB,            A.TENANT_NAME AS TENANT_NAME,            A.REVERSE_TENANT_NAME AS REVERSE_TENANT_NAME,            A.CLUSTER_NAME AS CLUSTER_NAME,            A.REVERSE_CLUSTER_NAME AS REVERSE_CLUSTER_NAME,            A.REVERSE_HOST_IP AS REVERSE_HOST,            A.REVERSE_HOST_PORT AS REVERSE_PORT,            A.REVERSE_USER_NAME AS REVERSE_USERNAME     FROM SYS.ALL_VIRTUAL_DBLINK_REAL_AGENT A,          SYS.ALL_VIRTUAL_USER_REAL_AGENT B     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID() AND A.OWNER_ID = B.USER_ID; )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT             A.DBLINK_NAME AS DB_LINK,            A.USER_NAME AS USERNAME,            CAST('' AS VARCHAR2(30)) AS PASSWORD,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_NAME,            CAST('' AS VARCHAR2(128)) AS CREDENTIAL_OWNER,            CAST(CASE DRIVER_PROTO WHEN 1 THEN A.CONN_STRING ELSE (A.HOST_IP || ':' || TO_CHAR(A.HOST_PORT)) END AS VARCHAR2(2000))AS HOST,            CAST(A.GMT_CREATE AS DATE) AS CREATED,            CAST('' AS VARCHAR2(3)) AS HIDDEN,            CAST('' AS VARCHAR2(3)) AS SHARD_INTERNAL,            CAST('YES' AS VARCHAR2(3)) AS VALID,            CAST('' AS VARCHAR2(3)) AS INTRA_CDB,            A.TENANT_NAME AS TENANT_NAME,            A.REVERSE_TENANT_NAME AS REVERSE_TENANT_NAME,            A.CLUSTER_NAME AS CLUSTER_NAME,            A.REVERSE_CLUSTER_NAME AS REVERSE_CLUSTER_NAME,            A.REVERSE_HOST_IP AS REVERSE_HOST,            A.REVERSE_HOST_PORT AS REVERSE_PORT,            A.REVERSE_USER_NAME AS REVERSE_USERNAME     FROM SYS.ALL_VIRTUAL_DBLINK_REAL_AGENT A,          SYS.ALL_VIRTUAL_USER_REAL_AGENT B     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID() AND            A.OWNER_ID = B.USER_ID AND            B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER'); )__"))) {
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
