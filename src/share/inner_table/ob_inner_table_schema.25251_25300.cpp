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

int ObInnerTableSchema::dbms_lock_allocated_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBMS_LOCK_ALLOCATED_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBMS_LOCK_ALLOCATED_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT NAME, LOCKID, EXPIRATION FROM SYS.ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::dba_wr_control_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_WR_CONTROL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_WR_CONTROL_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT        SETTING.TENANT_ID AS TENANT_ID,        SETTING.SNAP_INTERVAL AS SNAP_INTERVAL,        SETTING.RETENTION AS RETENTION,        SETTING.TOPNSQL AS TOPNSQL   FROM      SYS.ALL_VIRTUAL_WR_CONTROL SETTING    WHERE      SETTING.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_HISTORY_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT            (CASE                 WHEN A.LS_ID IS NULL THEN B.LS_ID                ELSE A.LS_ID END) AS LS_ID,           (CASE                WHEN A.LS_GROUP_ID IS NULL THEN B.LS_GROUP_ID                ELSE A.LS_GROUP_ID END) AS LS_GROUP_ID,           (CASE                WHEN A.STATUS IS NULL THEN B.STATUS                ELSE A.STATUS END) AS STATUS,           (CASE                WHEN A.FLAG IS NULL THEN B.FLAG                ELSE A.FLAG END) AS FLAG,           (CASE                 WHEN A.LS_ID = 1 THEN 0                ELSE B.CREATE_SCN END) AS CREATE_SCN     FROM SYS.DBA_OB_LS A          FULL JOIN SYS.ALL_VIRTUAL_LS_REAL_AGENT B               ON A.LS_ID = B.LS_ID   )__"))) {
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

int ObInnerTableSchema::dba_ob_tenant_event_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TENANT_EVENT_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TENANT_EVENT_HISTORY_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     gmt_create AS "TIMESTAMP",     MODULE,     EVENT,     NAME1, VALUE1,     NAME2, VALUE2,     NAME3, VALUE3,     NAME4, VALUE4,     NAME5, VALUE5,     NAME6, VALUE6,     EXTRA_INFO,     SVR_IP,     SVR_PORT,     TRACE_ID,     COST_TIME,     RET_CODE,     ERROR_MSG   FROM SYS.ALL_VIRTUAL_TENANT_EVENT_HISTORY   WHERE TENANT_ID=EFFECTIVE_TENANT_ID();   )__"))) {
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

int ObInnerTableSchema::dba_scheduler_job_run_details_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SCHEDULER_JOB_RUN_DETAILS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SCHEDULER_JOB_RUN_DETAILS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT                         CAST(NULL AS NUMBER) AS LOG_ID,                         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS LOG_DATE,                         CAST(NULL AS VARCHAR(128)) AS OWNER,                         CAST(NULL AS VARCHAR(128)) AS JOB_NAME,                         CAST(NULL AS VARCHAR(128)) AS JOB_SUBNAME,                         CAST(NULL AS VARCHAR(128)) AS STATUS,                         CODE,                         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS REQ_START_DATE,                         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS ACTUAL_START_DATE,                         CAST(NULL AS NUMBER) AS RUN_DURATION,                         CAST(NULL AS VARCHAR(128)) AS INSTANCE_ID,                         CAST(NULL AS NUMBER) AS SESSION_ID,                         CAST(NULL AS VARCHAR(128)) AS SLAVE_PID,                         CAST(NULL AS NUMBER) AS CPU_USED,                         CAST(NULL AS VARCHAR(128)) AS CREDENTIAL_OWNER,                         CAST(NULL AS VARCHAR(128)) AS CREDENTIAL_NAME,                         CAST(NULL AS VARCHAR(128)) AS DESTINATION_OWNER,                         CAST(NULL AS VARCHAR(128)) AS DESTINATION,                         MESSAGE,                         JOB,                         TIME,                         JOB_CLASS,                         GMT_CREATE,                         GMT_MODIFIED                        FROM SYS.ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_REAL_AGENT ) UNION ALL ( SELECT                         LOG_ID,                         LOG_DATE,                         OWNER,                         JOB_NAME,                         JOB_SUBNAME,                         STATUS,                         CODE,                         REQ_START_DATE,                         ACTUAL_START_DATE,                         RUN_DURATION,                         INSTANCE_ID,                         SESSION_ID,                         SLAVE_PID,                         CPU_USED,                         CREDENTIAL_OWNER,                         CREDENTIAL_NAME,                         DESTINATION_OWNER,                         DESTINATION,                         MESSAGE,                         JOB,                         TIME,                         JOB_CLASS,                         GMT_CREATE,                         GMT_MODIFIED                        FROM SYS.ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT ) )__"))) {
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

int ObInnerTableSchema::dba_scheduler_job_classes_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SCHEDULER_JOB_CLASSES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SCHEDULER_JOB_CLASSES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     T.JOB_CLASS_NAME AS JOB_CLASS_NAME,     T.RESOURCE_CONSUMER_GROUP AS RESOURCE_CONSUMER_GROUP,     T.SERVICE AS SERVICE,     T.LOGGING_LEVEL AS LOGGING_LEVEL,     T.LOG_HISTORY AS LOG_HISTORY,     T.COMMENTS AS COMMENTS     FROM SYS.ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_REAL_AGENT T )__"))) {
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

int ObInnerTableSchema::dba_ob_recover_table_jobs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RECOVER_TABLE_JOBS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RECOVER_TABLE_JOBS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE          TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')       END AS FINISH_TIMESTAMP,     STATUS,     AUX_TENANT_NAME,     TARGET_TENANT_NAME,     IMPORT_ALL,     DB_LIST,     TABLE_LIST,     RESTORE_SCN,     CASE       WHEN RESTORE_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(RESTORE_SCN)       END AS RESTORE_SCN_DISPLAY,     RESTORE_OPTION,     BACKUP_DEST,     BACKUP_SET_LIST,     BACKUP_PIECE_LIST,     BACKUP_PASSWD,     EXTERNAL_KMS_INFO,     REMAP_DB_LIST,     REMAP_TABLE_LIST,     REMAP_TABLEGROUP_LIST,     REMAP_TABLESPACE_LIST,     RESULT,     "COMMENT",     DESCRIPTION     FROM SYS.ALL_VIRTUAL_RECOVER_TABLE_JOB     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_recover_table_job_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS FINISH_TIMESTAMP,     STATUS,     AUX_TENANT_NAME,     TARGET_TENANT_NAME,     IMPORT_ALL,     DB_LIST,     TABLE_LIST,     RESTORE_SCN,     CASE       WHEN RESTORE_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(RESTORE_SCN)       END AS RESTORE_SCN_DISPLAY,     RESTORE_OPTION,     BACKUP_DEST,     BACKUP_SET_LIST,     BACKUP_PIECE_LIST,     BACKUP_PASSWD,     EXTERNAL_KMS_INFO,     REMAP_DB_LIST,     REMAP_TABLE_LIST,     REMAP_TABLEGROUP_LIST,     REMAP_TABLESPACE_LIST,     RESULT,     "COMMENT",     DESCRIPTION     FROM SYS.ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_import_table_jobs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_IMPORT_TABLE_JOBS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_IMPORT_TABLE_JOBS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE          TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')       END AS FINISH_TIMESTAMP,     SRC_TENANT_NAME,     SRC_TENANT_ID,     STATUS,     IMPORT_ALL,     DB_LIST,     TABLE_LIST,     REMAP_DB_LIST,     REMAP_TABLE_LIST,     REMAP_TABLEGROUP_LIST,     REMAP_TABLESPACE_LIST,     TOTAL_TABLE_COUNT,     FINISHED_TABLE_COUNT,     FAILED_TABLE_COUNT,     RESULT,     "COMMENT",     DESCRIPTION     FROM SYS.ALL_VIRTUAL_IMPORT_TABLE_JOB     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_import_table_job_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS FINISH_TIMESTAMP,     SRC_TENANT_NAME,     SRC_TENANT_ID,     STATUS,     IMPORT_ALL,     DB_LIST,     TABLE_LIST,     REMAP_DB_LIST,     REMAP_TABLE_LIST,     REMAP_TABLEGROUP_LIST,     REMAP_TABLESPACE_LIST,     TOTAL_TABLE_COUNT,     FINISHED_TABLE_COUNT,     FAILED_TABLE_COUNT,     RESULT,     "COMMENT",     DESCRIPTION     FROM SYS.ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_import_table_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_IMPORT_TABLE_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_IMPORT_TABLE_TASKS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TASK_ID,     JOB_ID,     SRC_TENANT_ID,     SRC_TABLESPACE,     SRC_TABLEGROUP,     SRC_DATABASE,     SRC_TABLE,     SRC_PARTITION,     TARGET_TABLESPACE,     TARGET_TABLEGROUP,     TARGET_DATABASE,     TARGET_TABLE,     TABLE_COLUMN,     STATUS,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     TO_CHAR(COMPLETION_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS COMPLETION_TIMESTAMP,     CUMULATIVE_TS,     TOTAL_INDEX_COUNT,     IMPORTED_INDEX_COUNT,     FAILED_INDEX_COUNT,     TOTAL_CONSTRAINT_COUNT,     IMPORTED_CONSTRAINT_COUNT,     FAILED_CONSTRAINT_COUNT,     TOTAL_REF_CONSTRAINT_COUNT,     IMPORTED_REF_CONSTRAINT_COUNT,     FAILED_REF_CONSTRAINT_COUNT,     RESULT,     "COMMENT"     FROM SYS.ALL_VIRTUAL_IMPORT_TABLE_TASK     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_import_table_task_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TASK_ID,     JOB_ID,     SRC_TENANT_ID,     SRC_TABLESPACE,     SRC_TABLEGROUP,     SRC_DATABASE,     SRC_TABLE,     SRC_PARTITION,     TARGET_TABLESPACE,     TARGET_TABLEGROUP,     TARGET_DATABASE,     TARGET_TABLE,     TABLE_COLUMN,     STATUS,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     TO_CHAR(COMPLETION_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS COMPLETION_TIMESTAMP,     CUMULATIVE_TS,     TOTAL_INDEX_COUNT,     IMPORTED_INDEX_COUNT,     FAILED_INDEX_COUNT,     TOTAL_CONSTRAINT_COUNT,     IMPORTED_CONSTRAINT_COUNT,     FAILED_CONSTRAINT_COUNT,     TOTAL_REF_CONSTRAINT_COUNT,     IMPORTED_REF_CONSTRAINT_COUNT,     FAILED_REF_CONSTRAINT_COUNT,     RESULT,     "COMMENT"     FROM SYS.ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_transfer_partition_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TRANSFER_PARTITION_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TRANSFER_PARTITION_TASKS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TASK_ID,          GMT_CREATE AS CREATE_TIME,          GMT_MODIFIED AS MODIFY_TIME,          TABLE_ID,          OBJECT_ID,          DEST_LS,          BALANCE_JOB_ID,          TRANSFER_TASK_ID,          STATUS,          "COMMENT"   FROM SYS.ALL_VIRTUAL_TRANSFER_PARTITION_TASK_REAL_AGENT   )__"))) {
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

int ObInnerTableSchema::dba_ob_transfer_partition_task_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TASK_ID,          CREATE_TIME,          FINISH_TIME,          TABLE_ID,          OBJECT_ID,          DEST_LS,          BALANCE_JOB_ID,          TRANSFER_TASK_ID,          STATUS,          "COMMENT"   FROM SYS.ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_REAL_AGENT   )__"))) {
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

int ObInnerTableSchema::user_users_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_USERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_USERS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       B.USER_NAME AS USERNAME,       B.USER_ID AS USERID,       CAST(CASE WHEN B.IS_LOCKED = 1 THEN 'LOCKED' ELSE 'OPEN' END as VARCHAR2(32)) AS ACCOUNT_STATUS,       CAST(NULL as DATE) AS LOCK_DATE,       CAST(NULL as DATE) AS EXPIRY_DATE,       CAST(NULL as VARCHAR2(30)) AS DEFAULT_TABLESPACE,       CAST(NULL as VARCHAR2(30)) AS TEMPORARY_TABLESPACE,       CAST(NULL as VARCHAR2(30)) AS LOCAL_TEMP_TABLESPACE,       CAST(B.GMT_CREATE AS DATE) AS CREATED,       CAST(NULL as VARCHAR2(30)) AS INITIAL_RSRC_CONSUMER_GROUP,       CAST(NULL as VARCHAR2(4000)) AS EXTERNAL_NAME,       CAST('N' as VARCHAR2(1)) AS PROXY_ONLY_CONNECT,       CAST('NO' as VARCHAR2(3)) AS COMMON,       CAST('N' as VARCHAR2(1)) AS ORACLE_MAINTAINED,       CAST('NO' as VARCHAR2(3)) AS INHERITED,       CAST('USING_NLS_COMP' as VARCHAR2(100)) AS DEFAULT_COLLATION,       CAST('NO' as VARCHAR2(3)) AS IMPLICIT,       CAST('NO' as VARCHAR2(3)) AS ALL_SHARD,       CAST(B.PASSWORD_LAST_CHANGED AS DATE) AS PASSWORD_CHANGE_DATE     FROM       SYS.ALL_VIRTUAL_USER_REAL_AGENT B     WHERE       B.TYPE = 0       AND B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')       AND B.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_mview_logs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVIEW_LOGS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVIEW_LOGS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS LOG_OWNER,       CAST(D.TABLE_NAME AS VARCHAR2(128)) AS MASTER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS LOG_TABLE,       CAST(NULL AS VARCHAR2(128)) AS LOG_TRIGGER,       CAST(DECODE(bitand(D.TABLE_MODE, 66048), 66048, 'YES', 'NO') AS  VARCHAR2(3)) AS ROWIDS,       CAST(DECODE(bitand(D.TABLE_MODE, 66048), 0, 'YES', 'NO') AS  VARCHAR2(3)) AS PRIMARY_KEY,       CAST('NO' AS VARCHAR2(3)) AS OBJECT_ID,       CAST(         DECODE((           SELECT COUNT(*)             FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C1,                  SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C2             WHERE B.TABLE_ID = C1.TABLE_ID               AND C1.COLUMN_ID >= 16               AND C1.COLUMN_ID < 65520               AND D.TABLE_ID = C2.TABLE_ID               AND C2.ROWKEY_POSITION != 0               AND C1.COLUMN_ID != C2.COLUMN_ID               AND C1.TENANT_ID = EFFECTIVE_TENANT_ID()               AND C2.TENANT_ID = EFFECTIVE_TENANT_ID()           ), 0, 'NO', 'YES') AS VARCHAR2(3)       ) AS FILTER_COLUMNS,       CAST('YES' AS VARCHAR2(3)) AS SEQUENCE,       CAST('YES' AS VARCHAR2(3)) AS INCLUDE_NEW_VALUES,       CAST(DECODE(C.PURGE_MODE, 1, 'YES', 'NO') AS VARCHAR2(3)) AS PURGE_ASYNCHRONOUS,       CAST(DECODE(C.PURGE_MODE, 2, 'YES', 'NO') AS VARCHAR2(3)) AS PURGE_DEFERRED,       CAST(C.PURGE_START AS DATE) AS PURGE_START /* TODO: DD-MON-YYYY */,       CAST(C.PURGE_NEXT AS VARCHAR2(200)) AS PURGE_INTERVAL,       CAST(C.LAST_PURGE_DATE AS DATE) AS LAST_PURGE_DATE /* TODO: DD-MON-YYYY */,       CAST(0 AS NUMBER) AS LAST_PURGE_STATUS,       CAST(C.LAST_PURGE_ROWS AS NUMBER) AS NUM_ROWS_PURGED,       CAST('YES' AS VARCHAR2(3)) AS COMMIT_SCN_BASED,       CAST('NO' AS VARCHAR2(3)) AS STAGING_LOG     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MLOG_REAL_AGENT C,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D     WHERE A.DATABASE_ID = B.DATABASE_ID        AND B.TABLE_ID = C.MLOG_ID       AND B.TABLE_TYPE = 15       AND B.DATA_TABLE_ID = D.TABLE_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND D.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_mview_logs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_MVIEW_LOGS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_MVIEW_LOGS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   	SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS LOG_OWNER,       CAST(D.TABLE_NAME AS VARCHAR2(128)) AS MASTER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS LOG_TABLE,       CAST(NULL AS VARCHAR2(128)) AS LOG_TRIGGER,       CAST(DECODE(bitand(D.TABLE_MODE, 66048), 66048, 'YES', 'NO') AS  VARCHAR2(3)) AS ROWIDS,       CAST(DECODE(bitand(D.TABLE_MODE, 66048), 0, 'YES', 'NO') AS  VARCHAR2(3)) AS PRIMARY_KEY,       CAST('NO' AS VARCHAR2(3)) AS OBJECT_ID,       CAST(         DECODE((           SELECT COUNT(*)             FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C1,                  SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C2             WHERE B.TABLE_ID = C1.TABLE_ID               AND C1.COLUMN_ID >= 16               AND C1.COLUMN_ID < 65520               AND D.TABLE_ID = C2.TABLE_ID               AND C2.ROWKEY_POSITION != 0               AND C1.COLUMN_ID != C2.COLUMN_ID               AND C1.TENANT_ID = EFFECTIVE_TENANT_ID()               AND C2.TENANT_ID = EFFECTIVE_TENANT_ID()           ), 0, 'NO', 'YES') AS VARCHAR2(3)       ) AS FILTER_COLUMNS,       CAST('YES' AS VARCHAR2(3)) AS SEQUENCE,       CAST('YES' AS VARCHAR2(3)) AS INCLUDE_NEW_VALUES,       CAST(DECODE(C.PURGE_MODE, 1, 'YES', 'NO') AS VARCHAR2(3)) AS PURGE_ASYNCHRONOUS,       CAST(DECODE(C.PURGE_MODE, 2, 'YES', 'NO') AS VARCHAR2(3)) AS PURGE_DEFERRED,       CAST(C.PURGE_START AS DATE) AS PURGE_START /* TODO: DD-MON-YYYY */,       CAST(C.PURGE_NEXT AS VARCHAR2(200)) AS PURGE_INTERVAL,       CAST(C.LAST_PURGE_DATE AS DATE) AS LAST_PURGE_DATE /* TODO: DD-MON-YYYY */,       CAST(0 AS NUMBER) AS LAST_PURGE_STATUS,       CAST(C.LAST_PURGE_ROWS AS NUMBER) AS NUM_ROWS_PURGED,       CAST('YES' AS VARCHAR2(3)) AS COMMIT_SCN_BASED,       CAST('NO' AS VARCHAR2(3)) AS STAGING_LOG     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MLOG_REAL_AGENT C,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D     WHERE A.DATABASE_ID = B.DATABASE_ID        AND B.TABLE_ID = C.MLOG_ID       AND B.TABLE_TYPE = 15       AND B.DATA_TABLE_ID = D.TABLE_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       AND (A.DATABASE_ID = USERENV('SCHEMAID')         OR USER_CAN_ACCESS_OBJ(1, B.TABLE_ID, B.DATABASE_ID) = 1) )__"))) {
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

int ObInnerTableSchema::user_mview_logs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_MVIEW_LOGS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVIEW_LOGS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   	SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS LOG_OWNER,       CAST(D.TABLE_NAME AS VARCHAR2(128)) AS MASTER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS LOG_TABLE,       CAST(NULL AS VARCHAR2(128)) AS LOG_TRIGGER,       CAST(DECODE(bitand(D.TABLE_MODE, 66048), 66048, 'YES', 'NO') AS  VARCHAR2(3)) AS ROWIDS,       CAST(DECODE(bitand(D.TABLE_MODE, 66048), 0, 'YES', 'NO') AS  VARCHAR2(3)) AS PRIMARY_KEY,       CAST('NO' AS VARCHAR2(3)) AS OBJECT_ID,       CAST(         DECODE((           SELECT COUNT(*)             FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C1,                  SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C2             WHERE B.TABLE_ID = C1.TABLE_ID               AND C1.COLUMN_ID >= 16               AND C1.COLUMN_ID < 65520               AND D.TABLE_ID = C2.TABLE_ID               AND C2.ROWKEY_POSITION != 0               AND C1.COLUMN_ID != C2.COLUMN_ID               AND C1.TENANT_ID = EFFECTIVE_TENANT_ID()               AND C2.TENANT_ID = EFFECTIVE_TENANT_ID()           ), 0, 'NO', 'YES') AS VARCHAR2(3)       ) AS FILTER_COLUMNS,       CAST('YES' AS VARCHAR2(3)) AS SEQUENCE,       CAST('YES' AS VARCHAR2(3)) AS INCLUDE_NEW_VALUES,       CAST(DECODE(C.PURGE_MODE, 1, 'YES', 'NO') AS VARCHAR2(3)) AS PURGE_ASYNCHRONOUS,       CAST(DECODE(C.PURGE_MODE, 2, 'YES', 'NO') AS VARCHAR2(3)) AS PURGE_DEFERRED,       CAST(C.PURGE_START AS DATE) AS PURGE_START /* TODO: DD-MON-YYYY */,       CAST(C.PURGE_NEXT AS VARCHAR2(200)) AS PURGE_INTERVAL,       CAST(C.LAST_PURGE_DATE AS DATE) AS LAST_PURGE_DATE /* TODO: DD-MON-YYYY */,       CAST(0 AS NUMBER) AS LAST_PURGE_STATUS,       CAST(C.LAST_PURGE_ROWS AS NUMBER) AS NUM_ROWS_PURGED,       CAST('YES' AS VARCHAR2(3)) AS COMMIT_SCN_BASED,       CAST('NO' AS VARCHAR2(3)) AS STAGING_LOG     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MLOG_REAL_AGENT C,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D     WHERE A.DATABASE_ID = B.DATABASE_ID        AND B.TABLE_ID = C.MLOG_ID       AND B.TABLE_TYPE = 15       AND B.DATA_TABLE_ID = D.TABLE_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       AND A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::dba_mviews_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVIEWS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVIEWS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS CONTAINER_NAME,       B.VIEW_DEFINITION AS QUERY /* TODO: LONG */,       CAST(LENGTH(B.VIEW_DEFINITION) AS NUMBER) AS QUERY_LEN,       CAST('N' AS VARCHAR2(1)) AS UPDATABLE,       CAST(NULL AS VARCHAR2(128)) AS UPDATE_LOG,       CAST(NULL AS VARCHAR2(128)) AS MASTER_ROLLBACK_SEG,       CAST(NULL AS VARCHAR2(128)) AS MASTER_LINK,       CAST(         CASE bitand((B.TABLE_MODE / 134217728), 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS REWRITE_ENABLED,       CAST(NULL AS VARCHAR2(9)) AS REWRITE_CAPABILITY,       CAST(         DECODE(C.REFRESH_MODE, 0, 'NEVER',                                1, 'DEMAND',                                2, 'COMMIT',                                3, 'STATEMENT',                                   NULL         ) AS VARCHAR2(6)       ) AS REFRESH_MODE,       CAST(         DECODE(C.REFRESH_METHOD, 0, 'NEVER',                                  1, 'COMPLETE',                                  2, 'FAST',                                  3, 'FORCE',                                     NULL          ) AS VARCHAR2(8)       ) AS REFRESH_METHOD,       CAST(         DECODE(C.BUILD_MODE, 0, 'IMMEDIATE',                              1, 'DEFERRED',                              2, 'PERBUILT',                                 NULL         ) AS VARCHAR2(9)       ) AS BUILD_MODE,       CAST(NULL AS VARCHAR2(18)) AS FAST_REFRESHABLE,       CAST(         DECODE(C.LAST_REFRESH_TYPE, 0, 'COMPLETE',                                     1, 'FAST',                                        'NA'         ) AS VARCHAR2(8)       ) AS LAST_REFRESH_TYPE,       CAST(C.LAST_REFRESH_DATE AS DATE) AS LAST_REFRESH_DATE /* TODO: DD-MON-YYYY */,       CAST(C.LAST_REFRESH_DATE + C.LAST_REFRESH_TIME / 86400 AS DATE) AS LAST_REFRESH_END_TIME /* TODO: DD-MON-YYYY */,       CAST(NULL AS VARCHAR2(19)) AS STALENESS,       CAST(NULL AS VARCHAR2(19)) AS AFTER_FAST_REFRESH,       CAST(DECODE(C.BUILD_MODE, 2, 'Y', 'N') AS VARCHAR2(1)) AS UNKNOWN_PREBUILT,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_PLSQL_FUNC,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_EXTERNAL_TABLE,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_CONSIDER_FRESH,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_IMPORT,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_TRUSTED_FD,       CAST(NULL AS VARCHAR2(19)) AS COMPILE_STATE,       CAST('Y' AS VARCHAR2(1)) AS USE_NO_INDEX,       CAST(NULL AS DATE) AS STALE_SINCE,       CAST(NULL AS NUMBER) AS NUM_PCT_TABLES,       CAST(NULL AS NUMBER) AS NUM_FRESH_PCT_REGIONS,       CAST(NULL AS NUMBER) AS NUM_STALE_PCT_REGIONS,       CAST('NO' AS VARCHAR2(3)) AS SEGMENT_CREATED,       CAST(NULL AS VARCHAR2(128)) AS EVALUATION_EDITION,       CAST(NULL AS VARCHAR2(128)) AS UNUSABLE_BEFORE,       CAST(NULL AS VARCHAR2(128)) AS UNUSABLE_BEGINNING,       CAST(NULL AS VARCHAR2(100)) AS DEFAULT_COLLATION,       CAST(         CASE bitand((B.TABLE_MODE / 268435456), 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS ON_QUERY_COMPUTATION     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MVIEW_REAL_AGENT C     WHERE A.DATABASE_ID = B.DATABASE_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_mviews_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_MVIEWS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_MVIEWS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS CONTAINER_NAME,       B.VIEW_DEFINITION AS QUERY /* TODO: LONG */,       CAST(LENGTH(B.VIEW_DEFINITION) AS NUMBER) AS QUERY_LEN,       CAST('N' AS VARCHAR2(1)) AS UPDATABLE,       CAST(NULL AS VARCHAR2(128)) AS UPDATE_LOG,       CAST(NULL AS VARCHAR2(128)) AS MASTER_ROLLBACK_SEG,       CAST(NULL AS VARCHAR2(128)) AS MASTER_LINK,       CAST(         CASE bitand((B.TABLE_MODE / 134217728), 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS REWRITE_ENABLED,       CAST(NULL AS VARCHAR2(9)) AS REWRITE_CAPABILITY,       CAST(         DECODE(C.REFRESH_MODE, 0, 'NEVER',                                1, 'DEMAND',                                2, 'COMMIT',                                3, 'STATEMENT',                                   NULL         ) AS VARCHAR2(6)       ) AS REFRESH_MODE,       CAST(         DECODE(C.REFRESH_METHOD, 0, 'NEVER',                                  1, 'COMPLETE',                                  2, 'FAST',                                  3, 'FORCE',                                     NULL          ) AS VARCHAR2(8)       ) AS REFRESH_METHOD,       CAST(         DECODE(C.BUILD_MODE, 0, 'IMMEDIATE',                              1, 'DEFERRED',                              2, 'PERBUILT',                                 NULL         ) AS VARCHAR2(9)       ) AS BUILD_MODE,       CAST(NULL AS VARCHAR2(18)) AS FAST_REFRESHABLE,       CAST(         DECODE(C.LAST_REFRESH_TYPE, 0, 'COMPLETE',                                     1, 'FAST',                                        'NA'         ) AS VARCHAR2(8)       ) AS LAST_REFRESH_TYPE,       CAST(C.LAST_REFRESH_DATE AS DATE) AS LAST_REFRESH_DATE /* TODO: DD-MON-YYYY */,       CAST(C.LAST_REFRESH_DATE + C.LAST_REFRESH_TIME / 86400 AS DATE) AS LAST_REFRESH_END_TIME /* TODO: DD-MON-YYYY */,       CAST(NULL AS VARCHAR2(19)) AS STALENESS,       CAST(NULL AS VARCHAR2(19)) AS AFTER_FAST_REFRESH,       CAST(DECODE(C.BUILD_MODE, 2, 'Y', 'N') AS VARCHAR2(1)) AS UNKNOWN_PREBUILT,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_PLSQL_FUNC,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_EXTERNAL_TABLE,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_CONSIDER_FRESH,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_IMPORT,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_TRUSTED_FD,       CAST(NULL AS VARCHAR2(19)) AS COMPILE_STATE,       CAST('Y' AS VARCHAR2(1)) AS USE_NO_INDEX,       CAST(NULL AS DATE) AS STALE_SINCE,       CAST(NULL AS NUMBER) AS NUM_PCT_TABLES,       CAST(NULL AS NUMBER) AS NUM_FRESH_PCT_REGIONS,       CAST(NULL AS NUMBER) AS NUM_STALE_PCT_REGIONS,       CAST('NO' AS VARCHAR2(3)) AS SEGMENT_CREATED,       CAST(NULL AS VARCHAR2(128)) AS EVALUATION_EDITION,       CAST(NULL AS VARCHAR2(128)) AS UNUSABLE_BEFORE,       CAST(NULL AS VARCHAR2(128)) AS UNUSABLE_BEGINNING,       CAST(NULL AS VARCHAR2(100)) AS DEFAULT_COLLATION,       CAST(         CASE bitand((B.TABLE_MODE / 268435456), 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS ON_QUERY_COMPUTATION     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MVIEW_REAL_AGENT C     WHERE A.DATABASE_ID = B.DATABASE_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND (A.DATABASE_ID = USERENV('SCHEMAID')         OR USER_CAN_ACCESS_OBJ(1, B.TABLE_ID, B.DATABASE_ID) = 1) )__"))) {
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

int ObInnerTableSchema::user_mviews_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_MVIEWS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVIEWS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS CONTAINER_NAME,       B.VIEW_DEFINITION AS QUERY /* TODO: LONG */,       CAST(LENGTH(B.VIEW_DEFINITION) AS NUMBER) AS QUERY_LEN,       CAST('N' AS VARCHAR2(1)) AS UPDATABLE,       CAST(NULL AS VARCHAR2(128)) AS UPDATE_LOG,       CAST(NULL AS VARCHAR2(128)) AS MASTER_ROLLBACK_SEG,       CAST(NULL AS VARCHAR2(128)) AS MASTER_LINK,       CAST(         CASE bitand((B.TABLE_MODE / 134217728), 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS REWRITE_ENABLED,       CAST(NULL AS VARCHAR2(9)) AS REWRITE_CAPABILITY,       CAST(         DECODE(C.REFRESH_MODE, 0, 'NEVER',                                1, 'DEMAND',                                2, 'COMMIT',                                3, 'STATEMENT',                                   NULL         ) AS VARCHAR2(6)       ) AS REFRESH_MODE,       CAST(         DECODE(C.REFRESH_METHOD, 0, 'NEVER',                                  1, 'COMPLETE',                                  2, 'FAST',                                  3, 'FORCE',                                     NULL          ) AS VARCHAR2(8)       ) AS REFRESH_METHOD,       CAST(         DECODE(C.BUILD_MODE, 0, 'IMMEDIATE',                              1, 'DEFERRED',                              2, 'PERBUILT',                                 NULL         ) AS VARCHAR2(9)       ) AS BUILD_MODE,       CAST(NULL AS VARCHAR2(18)) AS FAST_REFRESHABLE,       CAST(         DECODE(C.LAST_REFRESH_TYPE, 0, 'COMPLETE',                                     1, 'FAST',                                        'NA'         ) AS VARCHAR2(8)       ) AS LAST_REFRESH_TYPE,       CAST(C.LAST_REFRESH_DATE AS DATE) AS LAST_REFRESH_DATE /* TODO: DD-MON-YYYY */,       CAST(C.LAST_REFRESH_DATE + C.LAST_REFRESH_TIME / 86400 AS DATE) AS LAST_REFRESH_END_TIME /* TODO: DD-MON-YYYY */,       CAST(NULL AS VARCHAR2(19)) AS STALENESS,       CAST(NULL AS VARCHAR2(19)) AS AFTER_FAST_REFRESH,       CAST(DECODE(C.BUILD_MODE, 2, 'Y', 'N') AS VARCHAR2(1)) AS UNKNOWN_PREBUILT,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_PLSQL_FUNC,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_EXTERNAL_TABLE,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_CONSIDER_FRESH,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_IMPORT,       CAST('N' AS VARCHAR2(1)) AS UNKNOWN_TRUSTED_FD,       CAST(NULL AS VARCHAR2(19)) AS COMPILE_STATE,       CAST('Y' AS VARCHAR2(1)) AS USE_NO_INDEX,       CAST(NULL AS DATE) AS STALE_SINCE,       CAST(NULL AS NUMBER) AS NUM_PCT_TABLES,       CAST(NULL AS NUMBER) AS NUM_FRESH_PCT_REGIONS,       CAST(NULL AS NUMBER) AS NUM_STALE_PCT_REGIONS,       CAST('NO' AS VARCHAR2(3)) AS SEGMENT_CREATED,       CAST(NULL AS VARCHAR2(128)) AS EVALUATION_EDITION,       CAST(NULL AS VARCHAR2(128)) AS UNUSABLE_BEFORE,       CAST(NULL AS VARCHAR2(128)) AS UNUSABLE_BEGINNING,       CAST(NULL AS VARCHAR2(100)) AS DEFAULT_COLLATION,       CAST(         CASE bitand((B.TABLE_MODE / 268435456), 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS ON_QUERY_COMPUTATION     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MVIEW_REAL_AGENT C     WHERE A.DATABASE_ID = B.DATABASE_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::dba_mvref_stats_sys_defaults_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_STATS_SYS_DEFAULTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_STATS_SYS_DEFAULTS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(PARAMETER_NAME AS CHAR(16)) AS PARAMETER_NAME,       CAST(VALUE AS VARCHAR2(40)) AS VALUE     FROM     (       /* COLLECTION_LEVEL */       SELECT         'COLLECTION_LEVEL' PARAMETER_NAME,         DECODE(NVL(MAX(COLLECTION_LEVEL), 1),                0, 'NONE',                1, 'TYPICAL',                2, 'ADVANCED',                   NULL) VALUE       FROM         SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT       WHERE         TENANT_ID = EFFECTIVE_TENANT_ID()        UNION ALL        /* RETENTION_PERIOD */       SELECT         'RETENTION_PERIOD' PARAMETER_NAME,         TO_CHAR(NVL(MAX(RETENTION_PERIOD), 31)) VALUE       FROM         SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT       WHERE         TENANT_ID = EFFECTIVE_TENANT_ID()     ) )__"))) {
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

int ObInnerTableSchema::user_mvref_stats_sys_defaults_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_MVREF_STATS_SYS_DEFAULTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVREF_STATS_SYS_DEFAULTS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(PARAMETER_NAME AS CHAR(16)) AS PARAMETER_NAME,       CAST(VALUE AS VARCHAR2(40)) AS VALUE     FROM     (       /* COLLECTION_LEVEL */       SELECT         'COLLECTION_LEVEL' PARAMETER_NAME,         DECODE(NVL(MAX(COLLECTION_LEVEL), 1),                0, 'NONE',                1, 'TYPICAL',                2, 'ADVANCED',                   NULL) VALUE       FROM         SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT       WHERE         TENANT_ID = EFFECTIVE_TENANT_ID()        UNION ALL        /* RETENTION_PERIOD */       SELECT         'RETENTION_PERIOD' PARAMETER_NAME,         TO_CHAR(NVL(MAX(RETENTION_PERIOD), 31)) VALUE       FROM         SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT       WHERE         TENANT_ID = EFFECTIVE_TENANT_ID()     ) )__"))) {
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

int ObInnerTableSchema::dba_mvref_stats_params_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_STATS_PARAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_STATS_PARAMS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(MV_OWNER AS VARCHAR2(128)) AS MV_OWNER,       CAST(MV_NAME AS VARCHAR2(128)) AS MV_NAME,       CAST(         DECODE(COLLECTION_LEVEL, 0, 'NONE',                                  1, 'TYPICAL',                                  2, 'ADVANCED',                                     NULL         ) AS VARCHAR2(8)       ) AS COLLECTION_LEVEL,       CAST(RETENTION_PERIOD AS NUMBER) AS RETENTION_PERIOD     FROM     (       WITH DEFVALS AS        (         SELECT           NVL(MAX(COLLECTION_LEVEL), 1) AS COLLECTION_LEVEL,           NVL(MAX(RETENTION_PERIOD), 31) AS RETENTION_PERIOD         FROM           SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT         WHERE           TENANT_ID = EFFECTIVE_TENANT_ID()       )        SELECT         A.DATABASE_NAME MV_OWNER,         B.TABLE_NAME MV_NAME,         NVL(C.COLLECTION_LEVEL, D.COLLECTION_LEVEL) COLLECTION_LEVEL,         NVL(C.RETENTION_PERIOD, D.RETENTION_PERIOD) RETENTION_PERIOD       FROM         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,         (           SELECT TENANT_ID, MVIEW_ID, COLLECTION_LEVEL, RETENTION_PERIOD FROM SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_REAL_AGENT           RIGHT OUTER JOIN           (             SELECT TENANT_ID, MVIEW_ID FROM SYS.ALL_VIRTUAL_MVIEW_REAL_AGENT           )           USING (TENANT_ID, MVIEW_ID)         ) C,         DEFVALS D       WHERE A.DATABASE_ID = B.DATABASE_ID         AND B.TABLE_ID = C.MVIEW_ID         AND B.TABLE_TYPE = 7         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()     ) )__"))) {
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

int ObInnerTableSchema::user_mvref_stats_params_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_MVREF_STATS_PARAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVREF_STATS_PARAMS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(MV_OWNER AS VARCHAR2(128)) AS MV_OWNER,       CAST(MV_NAME AS VARCHAR2(128)) AS MV_NAME,       CAST(         DECODE(COLLECTION_LEVEL, 0, 'NONE',                                  1, 'TYPICAL',                                  2, 'ADVANCED',                                     NULL         ) AS VARCHAR2(8)       ) AS COLLECTION_LEVEL,       CAST(RETENTION_PERIOD AS NUMBER) AS RETENTION_PERIOD     FROM     (       WITH DEFVALS AS        (         SELECT           NVL(MAX(COLLECTION_LEVEL), 1) AS COLLECTION_LEVEL,           NVL(MAX(RETENTION_PERIOD), 31) AS RETENTION_PERIOD         FROM           SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT         WHERE           TENANT_ID = EFFECTIVE_TENANT_ID()       )        SELECT         A.DATABASE_NAME MV_OWNER,         B.TABLE_NAME MV_NAME,         NVL(C.COLLECTION_LEVEL, D.COLLECTION_LEVEL) COLLECTION_LEVEL,         NVL(C.RETENTION_PERIOD, D.RETENTION_PERIOD) RETENTION_PERIOD       FROM         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,         (           SELECT TENANT_ID, MVIEW_ID, COLLECTION_LEVEL, RETENTION_PERIOD FROM SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_REAL_AGENT           RIGHT OUTER JOIN           (             SELECT TENANT_ID, MVIEW_ID FROM SYS.ALL_VIRTUAL_MVIEW_REAL_AGENT           )           USING (TENANT_ID, MVIEW_ID)         ) C,         DEFVALS D       WHERE A.DATABASE_ID = B.DATABASE_ID         AND B.TABLE_ID = C.MVIEW_ID         AND B.TABLE_TYPE = 7         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')     ) )__"))) {
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

int ObInnerTableSchema::dba_mvref_run_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_RUN_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_RUN_STATS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.USER_NAME AS VARCHAR2(128)) AS RUN_OWNER,       CAST(B.REFRESH_ID AS NUMBER) AS REFRESH_ID,       CAST(B.NUM_MVS_TOTAL AS NUMBER) AS NUM_MVS,       CAST(B.MVIEWS AS VARCHAR2(4000)) AS MVIEWS,       CAST(B.BASE_TABLES AS VARCHAR2(4000)) AS BASE_TABLES,       CAST(B.METHOD AS VARCHAR2(4000)) AS METHOD,       CAST(B.ROLLBACK_SEG AS VARCHAR2(4000)) AS ROLLBACK_SEG,       CAST(DECODE(B.PUSH_DEFERRED_RPC, 1, 'Y', 'N') AS CHAR(1)) AS PUSH_DEFERRED_RPC,       CAST(DECODE(B.REFRESH_AFTER_ERRORS, 1, 'Y', 'N') AS CHAR(1)) AS REFRESH_AFTER_ERRORS,       CAST(B.PURGE_OPTION AS NUMBER) AS PURGE_OPTION,       CAST(B.PARALLELISM AS NUMBER) AS PARALLELISM,       CAST(B.HEAP_SIZE AS NUMBER) AS HEAP_SIZE,       CAST(DECODE(B.ATOMIC_REFRESH, 1, 'Y', 'N') AS CHAR(1)) AS ATOMIC_REFRESH,       CAST(DECODE(B.NESTED, 1, 'Y', 'N') AS CHAR(1)) AS NESTED,       CAST(DECODE(B.OUT_OF_PLACE, 1, 'Y', 'N') AS CHAR(1)) AS OUT_OF_PLACE,       CAST(B.NUMBER_OF_FAILURES AS NUMBER) AS NUMBER_OF_FAILURES,       CAST(B.START_TIME AS TIMESTAMP(6)) AS START_TIME,       CAST(B.END_TIME AS TIMESTAMP(6)) AS END_TIME,       CAST(B.ELAPSED_TIME AS NUMBER) AS ELAPSED_TIME,       CAST(0 AS NUMBER) AS LOG_SETUP_TIME,       CAST(B.LOG_PURGE_TIME AS NUMBER) AS LOG_PURGE_TIME,       CAST(DECODE(B.COMPLETE_STATS_AVALIABLE, 1, 'Y', 'N') AS CHAR(1)) AS COMPLETE_STATS_AVAILABLE     FROM       SYS.ALL_VIRTUAL_USER_REAL_AGENT A,       SYS.ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_REAL_AGENT B,       (         SELECT           C1.TENANT_ID AS TENANT_ID,           C1.REFRESH_ID AS REFRESH_ID         FROM           SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_REAL_AGENT C1,           SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C2         WHERE C1.TENANT_ID = C2.TENANT_ID           AND C1.MVIEW_ID = C2.TABLE_ID         GROUP BY C1.TENANT_ID, C1.REFRESH_ID       ) C     WHERE A.USER_ID = B.RUN_USER_ID       AND B.REFRESH_ID = C.REFRESH_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::user_mvref_run_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_MVREF_RUN_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVREF_RUN_STATS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(B.REFRESH_ID AS NUMBER) AS REFRESH_ID,       CAST(B.NUM_MVS_TOTAL AS NUMBER) AS NUM_MVS,       CAST(B.MVIEWS AS VARCHAR2(4000)) AS MVIEWS,       CAST(B.BASE_TABLES AS VARCHAR2(4000)) AS BASE_TABLES,       CAST(B.METHOD AS VARCHAR2(4000)) AS METHOD,       CAST(B.ROLLBACK_SEG AS VARCHAR2(4000)) AS ROLLBACK_SEG,       CAST(DECODE(B.PUSH_DEFERRED_RPC, 1, 'Y', 'N') AS CHAR(1)) AS PUSH_DEFERRED_RPC,       CAST(DECODE(B.REFRESH_AFTER_ERRORS, 1, 'Y', 'N') AS CHAR(1)) AS REFRESH_AFTER_ERRORS,       CAST(B.PURGE_OPTION AS NUMBER) AS PURGE_OPTION,       CAST(B.PARALLELISM AS NUMBER) AS PARALLELISM,       CAST(B.HEAP_SIZE AS NUMBER) AS HEAP_SIZE,       CAST(DECODE(B.ATOMIC_REFRESH, 1, 'Y', 'N') AS CHAR(1)) AS ATOMIC_REFRESH,       CAST(DECODE(B.NESTED, 1, 'Y', 'N') AS CHAR(1)) AS NESTED,       CAST(DECODE(B.OUT_OF_PLACE, 1, 'Y', 'N') AS CHAR(1)) AS OUT_OF_PLACE,       CAST(B.NUMBER_OF_FAILURES AS NUMBER) AS NUMBER_OF_FAILURES,       CAST(B.START_TIME AS TIMESTAMP(6)) AS START_TIME,       CAST(B.END_TIME AS TIMESTAMP(6)) AS END_TIME,       CAST(B.ELAPSED_TIME AS NUMBER) AS ELAPSED_TIME,       CAST(0 AS NUMBER) AS LOG_SETUP_TIME,       CAST(B.LOG_PURGE_TIME AS NUMBER) AS LOG_PURGE_TIME,       CAST(DECODE(B.COMPLETE_STATS_AVALIABLE, 1, 'Y', 'N') AS CHAR(1)) AS COMPLETE_STATS_AVAILABLE     FROM       SYS.ALL_VIRTUAL_USER_REAL_AGENT A,       SYS.ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_REAL_AGENT B,       (         SELECT           C1.TENANT_ID AS TENANT_ID,           C1.REFRESH_ID AS REFRESH_ID         FROM           SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_REAL_AGENT C1,           SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C2         WHERE C1.TENANT_ID = C2.TENANT_ID           AND C1.MVIEW_ID = C2.TABLE_ID         GROUP BY C1.TENANT_ID, C1.REFRESH_ID       ) C     WHERE A.USER_ID = B.RUN_USER_ID       AND B.REFRESH_ID = C.REFRESH_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND A.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::dba_mvref_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_STATS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MV_NAME,       CAST(C.REFRESH_ID AS NUMBER) AS REFRESH_ID,       CAST(         DECODE(C.REFRESH_TYPE, 0, 'COMPLETE',                                1, 'FAST',                                   NULL         ) AS VARCHAR2(30)       ) AS REFRESH_METHOD,       CAST(NULL AS VARCHAR2(4000)) AS REFRESH_OPTIMIZATIONS,       CAST(NULL AS VARCHAR2(4000)) AS ADDITIONAL_EXECUTIONS,       CAST(C.START_TIME AS TIMESTAMP(6)) AS START_TIME,       CAST(C.END_TIME AS TIMESTAMP(6)) AS END_TIME,       CAST(C.ELAPSED_TIME AS NUMBER) AS ELAPSED_TIME,       CAST(0 AS NUMBER) AS LOG_SETUP_TIME,       CAST(C.LOG_PURGE_TIME AS NUMBER) AS LOG_PURGE_TIME,       CAST(C.INITIAL_NUM_ROWS AS NUMBER) AS INITIAL_NUM_ROWS,       CAST(C.FINAL_NUM_ROWS AS NUMBER) AS FINAL_NUM_ROWS     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_REAL_AGENT C     WHERE A.DATABASE_ID = B.DATABASE_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::user_mvref_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_MVREF_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVREF_STATS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MV_NAME,       CAST(C.REFRESH_ID AS NUMBER) AS REFRESH_ID,       CAST(         DECODE(C.REFRESH_TYPE, 0, 'COMPLETE',                                1, 'FAST',                                   NULL         ) AS VARCHAR2(30)       ) AS REFRESH_METHOD,       CAST(NULL AS VARCHAR2(4000)) AS REFRESH_OPTIMIZATIONS,       CAST(NULL AS VARCHAR2(4000)) AS ADDITIONAL_EXECUTIONS,       CAST(C.START_TIME AS TIMESTAMP(6)) AS START_TIME,       CAST(C.END_TIME AS TIMESTAMP(6)) AS END_TIME,       CAST(C.ELAPSED_TIME AS NUMBER) AS ELAPSED_TIME,       CAST(0 AS NUMBER) AS LOG_SETUP_TIME,       CAST(C.LOG_PURGE_TIME AS NUMBER) AS LOG_PURGE_TIME,       CAST(C.INITIAL_NUM_ROWS AS NUMBER) AS INITIAL_NUM_ROWS,       CAST(C.FINAL_NUM_ROWS AS NUMBER) AS FINAL_NUM_ROWS     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MVIEW_REFRESH_STATS_REAL_AGENT C     WHERE A.DATABASE_ID = B.DATABASE_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::dba_mvref_change_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_CHANGE_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_CHANGE_STATS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS TBL_OWNER,       CAST(D.TABLE_NAME AS VARCHAR2(128)) AS TBL_NAME,       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MV_NAME,       CAST(E.REFRESH_ID AS NUMBER) AS REFRESH_ID,       CAST(E.NUM_ROWS_INS AS NUMBER) AS NUM_ROWS_INS,       CAST(E.NUM_ROWS_UPD AS NUMBER) AS NUM_ROWS_UPD,       CAST(E.NUM_ROWS_DEL AS NUMBER) AS NUM_ROWS_DEL,       CAST(0 AS NUMBER) AS NUM_ROWS_DL_INS,       CAST('N' AS CHAR(1)) AS PMOPS_OCCURRED,       CAST(NULL AS VARCHAR2(4000)) AS PMOP_DETAILS,       CAST(E.NUM_ROWS AS NUMBER) AS NUM_ROWS     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D,       SYS.ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_REAL_AGENT E     WHERE A.DATABASE_ID = B.DATABASE_ID       AND C.DATABASE_ID = D.DATABASE_ID       AND E.MVIEW_ID = B.TABLE_ID       AND E.DETAIL_TABLE_ID = D.TABLE_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       AND E.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::user_mvref_change_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_MVREF_CHANGE_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVREF_CHANGE_STATS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS TBL_OWNER,       CAST(D.TABLE_NAME AS VARCHAR2(128)) AS TBL_NAME,       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MV_NAME,       CAST(E.REFRESH_ID AS NUMBER) AS REFRESH_ID,       CAST(E.NUM_ROWS_INS AS NUMBER) AS NUM_ROWS_INS,       CAST(E.NUM_ROWS_UPD AS NUMBER) AS NUM_ROWS_UPD,       CAST(E.NUM_ROWS_DEL AS NUMBER) AS NUM_ROWS_DEL,       CAST(0 AS NUMBER) AS NUM_ROWS_DL_INS,       CAST('N' AS CHAR(1)) AS PMOPS_OCCURRED,       CAST(NULL AS VARCHAR2(4000)) AS PMOP_DETAILS,       CAST(E.NUM_ROWS AS NUMBER) AS NUM_ROWS     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D,       SYS.ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_REAL_AGENT E     WHERE A.DATABASE_ID = B.DATABASE_ID       AND C.DATABASE_ID = D.DATABASE_ID       AND E.MVIEW_ID = B.TABLE_ID       AND E.DETAIL_TABLE_ID = D.TABLE_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       AND E.TENANT_ID = EFFECTIVE_TENANT_ID()       AND A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::dba_mvref_stmt_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_STMT_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_STMT_STATS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MV_NAME,       CAST(C.REFRESH_ID AS NUMBER) AS REFRESH_ID,       CAST(C.STEP AS NUMBER) AS STEP,       CAST(C.SQLID AS VARCHAR2(32)) AS SQLID /* TODO: VARCHAR2(14) */,       C.STMT AS STMT /* TODO: CLOB */,       CAST(C.EXECUTION_TIME AS NUMBER) AS EXECUTION_TIME,       C.EXECUTION_PLAN AS EXECUTION_PLAN /* TODO: XMLTYPE STORAGE BINARY */     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_REAL_AGENT C     WHERE A.DATABASE_ID = B.DATABASE_ID       AND B.TABLE_ID = C.MVIEW_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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
