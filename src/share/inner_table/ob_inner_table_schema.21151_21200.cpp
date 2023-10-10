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

int ObInnerTableSchema::column_privileges_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_COLUMN_PRIVILEGES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_COLUMN_PRIVILEGES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       CAST(NULL AS CHAR(292)) AS GRANTEE,       CAST('def' AS CHAR(512)) AS TABLE_CATALOG,       CAST(NULL AS CHAR(64)) AS TABLE_SCHEMA,       CAST(NULL AS CHAR(64)) AS TABLE_NAME,       CAST(NULL AS CHAR(64)) AS COLUMN_NAME,       CAST(NULL AS CHAR(64)) AS PRIVILEGE_TYPE,       CAST(NULL AS CHAR(3))  AS IS_GRANTABLE     FROM DUAL     WHERE 1 = 0 )__"))) {
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

int ObInnerTableSchema::view_table_usage_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_VIEW_TABLE_USAGE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_VIEW_TABLE_USAGE_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select     cast('def' as CHAR(64)) AS VIEW_CATALOG,     v.VIEW_SCHEMA as VIEW_SCHEMA,     v.VIEW_NAME as VIEW_NAME,     t.TABLE_SCHEMA as TABLE_SCHEMA,     t.TABLE_NAME as TABLE_NAME,     cast('def' as CHAR(64)) AS TABLE_CATALOG     from     (select o.tenant_id,             o.database_name as VIEW_SCHEMA,             o.table_name as VIEW_NAME,             d.dep_obj_id as DEP_OBJ_ID,             d.ref_obj_id as REF_OBJ_ID      from (select t.tenant_id,                   d.database_name as database_name,                   t.table_name as table_name,                   t.table_id as table_id            from oceanbase.__all_table as t            join oceanbase.__all_database as d            on t.tenant_id = d.tenant_id and t.database_id = d.database_id) o            join oceanbase.__all_tenant_dependency d            on o.tenant_id = d.tenant_id and d.dep_obj_id = o.table_id) v       join       (select o.tenant_id,              o.database_name as TABLE_SCHEMA,              o.table_name as TABLE_NAME,              d.dep_obj_id as DEP_OBJ_ID,              d.ref_obj_id as REF_OBJ_ID       from (select t.tenant_id,                    d.database_name as database_name,                    t.table_name as table_name,                    t.table_id as table_id             from oceanbase.__all_table as t             join oceanbase.__all_database as d             on t.tenant_id = d.tenant_id and t.database_id = d.database_id) o             join oceanbase.__all_tenant_dependency d             on o.tenant_id = d.tenant_id and d.ref_obj_id = o.table_id) t      on t.tenant_id = v.tenant_id and v.dep_obj_id = t.dep_obj_id and v.ref_obj_id = t.ref_obj_id     where v.tenant_id = 0 )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_JOBS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     EXECUTOR_TENANT_ID,     PLUS_ARCHIVELOG,     BACKUP_TYPE,     JOB_LEVEL,     ENCRYPTION_MODE,     PASSWD,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     RESULT,     COMMENT,     DESCRIPTION,     PATH     FROM OCEANBASE.__all_virtual_backup_job )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_job_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_JOB_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_JOB_HISTORY_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     EXECUTOR_TENANT_ID,     PLUS_ARCHIVELOG,     BACKUP_TYPE,     JOB_LEVEL,     ENCRYPTION_MODE,     PASSWD,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     RESULT,     COMMENT,     DESCRIPTION,     PATH     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_JOB_HISTORY )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_TASKS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     TASK_ID,     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     START_SCN,     END_SCN,     USER_LS_START_SCN,     ENCRYPTION_MODE,     PASSWD,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     COMMENT,     PATH,     MINOR_TURN_ID,     MAJOR_TURN_ID     FROM OCEANBASE.__all_virtual_backup_task )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_TASK_HISTORY_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     TASK_ID,     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     START_SCN,     END_SCN,     USER_LS_START_SCN,     ENCRYPTION_MODE,     PASSWD,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     COMMENT,     PATH,     MINOR_TURN_ID,     MAJOR_TURN_ID     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_TASK_HISTORY )__"))) {
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

int ObInnerTableSchema::files_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_FILES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_FILES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT * FROM oceanbase.__all_virtual_files)__"))) {
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

int ObInnerTableSchema::dba_ob_tenants_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TENANTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TENANTS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT A.TENANT_ID,        TENANT_NAME,        (CASE             WHEN A.TENANT_ID = 1 THEN 'SYS'             WHEN (A.TENANT_ID & 0x1) = 1 THEN 'META'             ELSE 'USER'         END) AS TENANT_TYPE,        A.gmt_create AS CREATE_TIME,        A.gmt_modified AS MODIFY_TIME,        PRIMARY_ZONE,        LOCALITY,        CASE previous_locality           WHEN "" THEN NULL           ELSE previous_locality        END AS PREVIOUS_LOCALITY,        CASE compatibility_mode           WHEN 0 THEN 'MYSQL'           WHEN 1 THEN 'ORACLE'           ELSE NULL        END AS COMPATIBILITY_MODE,        STATUS,        CASE in_recyclebin           WHEN 0 THEN 'NO'           ELSE 'YES'        END AS IN_RECYCLEBIN,         CASE locked           WHEN 0 THEN 'NO'           ELSE 'YES'        END AS LOCKED,         (CASE             WHEN A.TENANT_ID = 1 THEN 'PRIMARY'             WHEN (A.TENANT_ID & 0x1) = 1 THEN 'PRIMARY'             ELSE TENANT_ROLE         END) AS TENANT_ROLE,         (CASE             WHEN A.TENANT_ID = 1 THEN 'NORMAL'             WHEN (A.TENANT_ID & 0x1) = 1 THEN 'NORMAL'             ELSE SWITCHOVER_STATUS         END) AS SWITCHOVER_STATUS,         (CASE             WHEN A.TENANT_ID = 1 THEN 0             WHEN (A.TENANT_ID & 0x1) = 1 THEN 0             ELSE SWITCHOVER_EPOCH         END) AS SWITCHOVER_EPOCH,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (A.TENANT_ID & 0x1) = 1 THEN NULL             ELSE SYNC_SCN         END) AS SYNC_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (A.TENANT_ID & 0x1) = 1 THEN NULL             ELSE REPLAYABLE_SCN         END) AS REPLAYABLE_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (A.TENANT_ID & 0x1) = 1 THEN NULL             ELSE READABLE_SCN         END) AS READABLE_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN NULL             WHEN (A.TENANT_ID & 0x1) = 1 THEN NULL             ELSE RECOVERY_UNTIL_SCN         END) AS RECOVERY_UNTIL_SCN,         (CASE             WHEN A.TENANT_ID = 1 THEN 'NOARCHIVELOG'             WHEN (A.TENANT_ID & 0x1) = 1 THEN 'NOARCHIVELOG'             ELSE LOG_MODE         END) AS LOG_MODE,        ARBITRATION_SERVICE_STATUS,        UNIT_NUM,        COMPATIBLE,        (CASE             WHEN (MOD(A.TENANT_ID, 2)) = 1 THEN 1             ELSE B.MAX_LS_ID END) AS MAX_LS_ID FROM OCEANBASE.__ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT AS A LEFT JOIN OCEANBASE.__ALL_VIRTUAL_TENANT_INFO AS B     ON A.TENANT_ID = B.TENANT_ID LEFT JOIN     (SELECT TENANT_ID,             (CASE                  WHEN TENANT_ID < 1 THEN NULL                  WHEN TENANT_ID != 1 THEN TENANT_ID - 1                  ELSE NULL              END) AS META_TENANT_ID,             MIN(UNIT_COUNT) AS UNIT_NUM      FROM OCEANBASE.__ALL_VIRTUAL_RESOURCE_POOL_MYSQL_SYS_AGENT      GROUP BY TENANT_ID) AS C     ON A.TENANT_ID = C.TENANT_ID OR A.TENANT_ID = C.META_TENANT_ID LEFT JOIN     (SELECT TENANT_ID,             MIN(VALUE) AS COMPATIBLE      FROM OCEANBASE.__ALL_VIRTUAL_TENANT_PARAMETER      WHERE NAME = 'compatible'      GROUP BY TENANT_ID) AS D     ON A.TENANT_ID = D.TENANT_ID )__"))) {
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

int ObInnerTableSchema::dba_ob_units_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_UNITS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_UNITS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT T.unit_id AS UNIT_ID,         CASE R.tenant_id            WHEN -1 THEN NULL            ELSE R.tenant_id        END AS TENANT_ID,         T.status AS STATUS,        T.resource_pool_id AS RESOURCE_POOL_ID,        UNIT_GROUP_ID,        T.gmt_create AS CREATE_TIME,        T.gmt_modified AS MODIFY_TIME,        ZONE,        SVR_IP,        SVR_PORT,         CASE migrate_from_svr_ip            WHEN "" THEN NULL            ELSE migrate_from_svr_ip        END AS MIGRATE_FROM_SVR_IP,         CASE migrate_from_svr_ip            WHEN "" THEN NULL            ELSE migrate_from_svr_port        END AS MIGRATE_FROM_SVR_PORT,         CASE migrate_from_svr_ip            WHEN "" THEN NULL            ELSE (CASE manual_migrate WHEN 0 THEN 'NO' ELSE 'YES' END)        END AS MANUAL_MIGRATE,         R.unit_config_id AS UNIT_CONFIG_ID,         U.MAX_CPU AS MAX_CPU,        U.MIN_CPU AS MIN_CPU,        U.MEMORY_SIZE AS MEMORY_SIZE,        U.LOG_DISK_SIZE AS LOG_DISK_SIZE,        U.MAX_IOPS AS MAX_IOPS,        U.MIN_IOPS AS MIN_IOPS,        U.IOPS_WEIGHT AS IOPS_WEIGHT FROM   oceanbase.__all_unit T,   oceanbase.__all_resource_pool R,   oceanbase.__all_unit_config U WHERE   T.resource_pool_id = R.resource_pool_id and R.unit_config_id = U.unit_config_id )__"))) {
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

int ObInnerTableSchema::dba_ob_unit_configs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_UNIT_CONFIGS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_UNIT_CONFIGS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT UNIT_CONFIG_ID,        NAME,        gmt_create AS CREATE_TIME,        gmt_modified AS MODIFY_TIME,        MAX_CPU,        MIN_CPU,        MEMORY_SIZE,        LOG_DISK_SIZE,        MAX_IOPS,        MIN_IOPS,        IOPS_WEIGHT FROM oceanbase.__all_unit_config )__"))) {
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

int ObInnerTableSchema::dba_ob_resource_pools_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RESOURCE_POOLS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RESOURCE_POOLS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT RESOURCE_POOL_ID,        NAME,        CASE TENANT_ID WHEN -1 THEN NULL ELSE TENANT_ID END AS TENANT_ID,        gmt_create AS CREATE_TIME,        gmt_modified AS MODIFY_TIME,        UNIT_COUNT,        UNIT_CONFIG_ID,        ZONE_LIST,        CASE replica_type           WHEN 0 THEN "FULL"           WHEN 5 THEN "LOGONLY"           WHEN 16 THEN "READONLY"           WHEN 261 THEN "ENCRYPTION LOGONLY"           ELSE NULL        END AS REPLICA_TYPE FROM oceanbase.__all_resource_pool )__"))) {
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

int ObInnerTableSchema::dba_ob_servers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SERVERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SERVERS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT SVR_IP,        SVR_PORT,        ID,        ZONE,        inner_port AS SQL_PORT,         CASE with_rootserver           WHEN 1 THEN 'YES'           ELSE 'NO' END        AS WITH_ROOTSERVER,         STATUS,         CASE start_service_time           WHEN 0 THEN NULL           ELSE usec_to_time(start_service_time) END        AS START_SERVICE_TIME,         CASE stop_time           WHEN 0 THEN NULL           ELSE usec_to_time(stop_time) END        AS STOP_TIME,         CASE block_migrate_in_time           WHEN 0 THEN NULL           ELSE usec_to_time(block_migrate_in_time)  END        AS BLOCK_MIGRATE_IN_TIME,         gmt_create AS CREATE_TIME,        gmt_modified AS MODIFY_TIME,         BUILD_VERSION,         CASE last_offline_time           WHEN 0 THEN NULL           ELSE usec_to_time(last_offline_time) END        AS LAST_OFFLINE_TIME FROM oceanbase.__all_server )__"))) {
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

int ObInnerTableSchema::dba_ob_zones_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ZONES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ZONES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   S.zone as ZONE,   S.create_time AS CREATE_TIME,   M.modify_time AS MODIFY_TIME,   STATUS,   IDC,   REGION,   TYPE FROM   (SELECT zone, info AS status, gmt_create AS create_time FROM oceanbase.__all_zone WHERE name = 'status') S,   (SELECT zone, info AS idc FROM oceanbase.__all_zone WHERE name = 'idc') I,   (SELECT zone, info AS region FROM oceanbase.__all_zone WHERE name = 'region') R,   (SELECT zone, info AS type FROM oceanbase.__all_zone WHERE name = 'zone_type') T,   (SELECT zone, max(gmt_modified) AS modify_time FROM oceanbase.__all_zone where zone != '' group by zone) M WHERE S.zone = I.zone and S.zone = R.zone and S.zone = T.zone and S.zone = M.zone )__"))) {
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

int ObInnerTableSchema::dba_ob_rootservice_event_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ROOTSERVICE_EVENT_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ROOTSERVICE_EVENT_HISTORY_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   gmt_create AS `TIMESTAMP`,   MODULE,   EVENT,   NAME1, VALUE1,   NAME2, VALUE2,   NAME3, VALUE3,   NAME4, VALUE4,   NAME5, VALUE5,   NAME6, VALUE6,   EXTRA_INFO,   RS_SVR_IP,   RS_SVR_PORT FROM oceanbase.__all_rootservice_event_history )__"))) {
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

int ObInnerTableSchema::dba_ob_tenant_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TENANT_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TENANT_JOBS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   JOB_ID,   JOB_TYPE,   JOB_STATUS,   RESULT_CODE,   PROGRESS,   gmt_create AS START_TIME,   gmt_modified AS MODIFY_TIME,   TENANT_ID,   SQL_TEXT,   EXTRA_INFO,   RS_SVR_IP,   RS_SVR_PORT FROM oceanbase.__all_rootservice_job WHERE   JOB_TYPE in (     'ALTER_TENANT_LOCALITY',     'ROLLBACK_ALTER_TENANT_LOCALITY',     'SHRINK_RESOURCE_TENANT_UNIT_NUM',     'ALTER_TENANT_PRIMARY_ZONE',     'ALTER_RESOURCE_TENANT_UNIT_NUM',     'UPGRADE_POST_ACTION',     'UPGRADE_SYSTEM_VARIABLE',     'UPGRADE_SYSTEM_TABLE',     'UPGRADE_BEGIN',     'UPGRADE_VIRTUAL_SCHEMA',     'UPGRADE_SYSTEM_PACKAGE',     'UPGRADE_ALL_POST_ACTION',     'UPGRADE_INSPECTION',     'UPGRADE_END',     'UPGRADE_ALL'   )   AND TENANT_ID != 0 )__"))) {
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

int ObInnerTableSchema::dba_ob_unit_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_UNIT_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_UNIT_JOBS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   JOB_ID,   JOB_TYPE,   JOB_STATUS,   RESULT_CODE,   PROGRESS,   gmt_create AS START_TIME,   gmt_modified AS MODIFY_TIME,   CASE tenant_id WHEN -1 THEN NULL ELSE tenant_id END AS TENANT_ID,   UNIT_ID,   SQL_TEXT,   EXTRA_INFO,   RS_SVR_IP,   RS_SVR_PORT FROM oceanbase.__all_rootservice_job WHERE   JOB_TYPE in (     'MIGRATE_UNIT'   ) )__"))) {
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

int ObInnerTableSchema::dba_ob_server_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SERVER_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SERVER_JOBS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   JOB_ID,   JOB_TYPE,   JOB_STATUS,   RESULT_CODE,   PROGRESS,   gmt_create AS START_TIME,   gmt_modified AS MODIFY_TIME,   SVR_IP,   SVR_PORT,   SQL_TEXT,   EXTRA_INFO,   RS_SVR_IP,   RS_SVR_PORT FROM oceanbase.__all_rootservice_job WHERE   JOB_TYPE in (     'DELETE_SERVER'   ) )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_locations_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_LOCATIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_LOCATIONS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT NOW(6) AS CREATE_TIME,          NOW(6) AS MODIFY_TIME,          LS_ID,          SVR_IP,          SVR_PORT,          SQL_PORT,          ZONE,          (CASE ROLE WHEN 1 THEN "LEADER" ELSE "FOLLOWER" END) AS ROLE,          (CASE ROLE WHEN 1 THEN MEMBER_LIST ELSE NULL END) AS MEMBER_LIST,          (CASE ROLE WHEN 1 THEN PAXOS_REPLICA_NUMBER ELSE NULL END) AS PAXOS_REPLICA_NUMBER,          (CASE REPLICA_TYPE           WHEN 0   THEN "FULL"           WHEN 5   THEN "LOGONLY"           WHEN 16  THEN "READONLY"           WHEN 261 THEN "ENCRYPTION LOGONLY"           ELSE NULL END) AS REPLICA_TYPE,          (CASE ROLE WHEN 1 THEN LEARNER_LIST ELSE "" END) AS LEARNER_LIST,          (CASE REBUILD            WHEN 0  THEN "FALSE"            ELSE "TRUE" END) AS REBUILD   FROM OCEANBASE.__ALL_VIRTUAL_CORE_META_TABLE   WHERE     EFFECTIVE_TENANT_ID() = 1   )   UNION ALL   (   SELECT GMT_CREATE AS CREATE_TIME,          GMT_MODIFIED AS MODIFY_TIME,          LS_ID,          SVR_IP,          SVR_PORT,          SQL_PORT,          ZONE,          (CASE ROLE WHEN 1 THEN "LEADER" ELSE "FOLLOWER" END) AS ROLE,          (CASE ROLE WHEN 1 THEN MEMBER_LIST ELSE NULL END) AS MEMBER_LIST,          (CASE ROLE WHEN 1 THEN PAXOS_REPLICA_NUMBER ELSE NULL END) AS PAXOS_REPLICA_NUMBER,          (CASE REPLICA_TYPE           WHEN 0   THEN "FULL"           WHEN 5   THEN "LOGONLY"           WHEN 16  THEN "READONLY"           WHEN 261 THEN "ENCRYPTION LOGONLY"           ELSE NULL END) AS REPLICA_TYPE,          (CASE ROLE WHEN 1 THEN LEARNER_LIST ELSE "" END) AS LEARNER_LIST,          (CASE REBUILD            WHEN 0  THEN "FALSE"            ELSE "TRUE" END) AS REBUILD   FROM OCEANBASE.__ALL_VIRTUAL_LS_META_TABLE   WHERE     TENANT_ID = EFFECTIVE_TENANT_ID() AND TENANT_ID != 1   )   )__"))) {
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

int ObInnerTableSchema::cdb_ob_ls_locations_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_LS_LOCATIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_LS_LOCATIONS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT NOW(6) AS CREATE_TIME,          NOW(6) AS MODIFY_TIME,          TENANT_ID,          LS_ID,          SVR_IP,          SVR_PORT,          SQL_PORT,          ZONE,          (CASE ROLE WHEN 1 THEN "LEADER" ELSE "FOLLOWER" END) AS ROLE,          (CASE ROLE WHEN 1 THEN MEMBER_LIST ELSE NULL END) AS MEMBER_LIST,          (CASE ROLE WHEN 1 THEN PAXOS_REPLICA_NUMBER ELSE NULL END) AS PAXOS_REPLICA_NUMBER,          (CASE REPLICA_TYPE           WHEN 0   THEN "FULL"           WHEN 5   THEN "LOGONLY"           WHEN 16  THEN "READONLY"           WHEN 261 THEN "ENCRYPTION LOGONLY"           ELSE NULL END) AS REPLICA_TYPE,          (CASE ROLE WHEN 1 THEN LEARNER_LIST ELSE "" END) AS LEARNER_LIST,          (CASE REBUILD            WHEN 0  THEN "FALSE"            ELSE "TRUE" END) AS REBUILD   FROM OCEANBASE.__ALL_VIRTUAL_CORE_META_TABLE   )   UNION ALL   (   SELECT GMT_CREATE AS CREATE_TIME,          GMT_MODIFIED AS MODIFY_TIME,          TENANT_ID,          LS_ID,          SVR_IP,          SVR_PORT,          SQL_PORT,          ZONE,          (CASE ROLE WHEN 1 THEN "LEADER" ELSE "FOLLOWER" END) AS ROLE,          (CASE ROLE WHEN 1 THEN MEMBER_LIST ELSE NULL END) AS MEMBER_LIST,          (CASE ROLE WHEN 1 THEN PAXOS_REPLICA_NUMBER ELSE NULL END) AS PAXOS_REPLICA_NUMBER,          (CASE REPLICA_TYPE           WHEN 0   THEN "FULL"           WHEN 5   THEN "LOGONLY"           WHEN 16  THEN "READONLY"           WHEN 261 THEN "ENCRYPTION LOGONLY"           ELSE NULL END) AS REPLICA_TYPE,          (CASE ROLE WHEN 1 THEN LEARNER_LIST ELSE "" END) AS LEARNER_LIST,          (CASE REBUILD            WHEN 0  THEN "FALSE"            ELSE "TRUE" END) AS REBUILD   FROM OCEANBASE.__ALL_VIRTUAL_LS_META_TABLE   WHERE TENANT_ID != 1   )   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablet_to_ls_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLET_TO_LS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLET_TO_LS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT CAST(TABLE_ID AS SIGNED) AS TABLET_ID,          CAST(1 AS SIGNED) AS LS_ID   FROM OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )   UNION ALL   (   SELECT CAST(TABLE_ID AS SIGNED) AS TABLET_ID,          CAST(1 AS SIGNED) AS LS_ID   FROM OCEANBASE.__ALL_TABLE   WHERE TENANT_ID = 0      AND ((TABLE_ID > 0 AND TABLE_ID < 10000)           OR (TABLE_ID > 50000 AND TABLE_ID < 70000)           OR (TABLE_ID > 100000 AND TABLE_ID < 200000))   )   UNION ALL   (   SELECT TABLET_ID,          LS_ID   FROM OCEANBASE.__ALL_TABLET_TO_LS   )   )__"))) {
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

int ObInnerTableSchema::cdb_ob_tablet_to_ls_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TABLET_TO_LS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TABLET_TO_LS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT TENANT_ID,          CAST(TABLE_ID AS SIGNED) AS TABLET_ID,          CAST(1 AS SIGNED) AS LS_ID   FROM OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE   )   UNION ALL   (   SELECT TENANT_ID,          CAST(TABLE_ID AS SIGNED) AS TABLET_ID,          CAST(1 AS SIGNED) AS LS_ID   FROM OCEANBASE.__ALL_VIRTUAL_TABLE   WHERE (TABLE_ID > 0 AND TABLE_ID < 10000)          OR (TABLE_ID > 50000 AND TABLE_ID < 70000)          OR (TABLE_ID > 100000 AND TABLE_ID < 200000)   )   UNION ALL   (   SELECT TENANT_ID,          TABLET_ID,          LS_ID   FROM OCEANBASE.__ALL_VIRTUAL_TABLET_TO_LS   )   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablet_replicas_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLET_REPLICAS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLET_REPLICAS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT gmt_create AS CREATE_TIME,          gmt_modified AS MODIFY_TIME,          TABLET_ID,          SVR_IP,          SVR_PORT,          LS_ID,          COMPACTION_SCN,          DATA_SIZE,          REQUIRED_SIZE   FROM OCEANBASE.__ALL_VIRTUAL_TABLET_META_TABLE   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::cdb_ob_tablet_replicas_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TABLET_REPLICAS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TABLET_REPLICAS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT gmt_create AS CREATE_TIME,          gmt_modified AS MODIFY_TIME,          TENANT_ID,          TABLET_ID,          SVR_IP,          SVR_PORT,          LS_ID,          COMPACTION_SCN,          DATA_SIZE,          REQUIRED_SIZE   FROM OCEANBASE.__ALL_VIRTUAL_TABLET_META_TABLE   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablegroups_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLEGROUPS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLEGROUPS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TABLEGROUP_NAME,                    CAST("NONE" AS CHAR(13)) AS PARTITIONING_TYPE,           CAST("NONE" AS CHAR(13)) AS SUBPARTITIONING_TYPE,           CAST(NULL AS SIGNED) AS PARTITION_COUNT,           CAST(NULL AS SIGNED) AS DEF_SUBPARTITION_COUNT,           CAST(NULL AS SIGNED) AS PARTITIONING_KEY_COUNT,           CAST(NULL AS SIGNED) AS SUBPARTITIONING_KEY_COUNT,           SHARDING            FROM OCEANBASE.__ALL_TABLEGROUP   )__"))) {
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

int ObInnerTableSchema::cdb_ob_tablegroups_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TABLEGROUPS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TABLEGROUPS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,           TABLEGROUP_NAME,           CAST("NONE" AS CHAR(13)) AS PARTITIONING_TYPE,           CAST("NONE" AS CHAR(13)) AS SUBPARTITIONING_TYPE,           CAST(NULL AS SIGNED) AS PARTITION_COUNT,           CAST(NULL AS SIGNED)  AS DEF_SUBPARTITION_COUNT,           CAST(NULL AS SIGNED) AS PARTITIONING_KEY_COUNT,           CAST(NULL AS SIGNED) AS SUBPARTITIONING_KEY_COUNT,           SHARDING    FROM OCEANBASE.__ALL_VIRTUAL_TABLEGROUP   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablegroup_partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLEGROUP_PARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLEGROUP_PARTITIONS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST("" AS CHAR(128)) AS TABLEGROUP_NAME,           CAST("NO" AS CHAR(3)) AS COMPOSITE,           CAST("" AS CHAR(64)) AS PARTITION_NAME,           CAST(NULL AS SIGNED) AS SUBPARTITION_COUNT,           CAST(NULL AS CHAR(4096)) AS HIGH_VALUE,           CAST(NULL AS SIGNED) AS HIGH_VALUE_LENGTH,           CAST(NULL AS UNSIGNED) AS PARTITION_POSITION   FROM      DUAL   WHERE      0 = 1   )__"))) {
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

int ObInnerTableSchema::cdb_ob_tablegroup_partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TABLEGROUP_PARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TABLEGROUP_PARTITIONS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(NULL AS SIGNED) AS TENANT_ID,           CAST('' AS CHAR(128)) AS TABLEGROUP_NAME,           CAST('NO' AS CHAR(3)) AS COMPOSITE,           CAST('' AS CHAR(64)) AS PARTITION_NAME,           CAST(NULL AS SIGNED) AS SUBPARTITION_COUNT,           CAST(NULL AS CHAR(4096)) AS HIGH_VALUE,           CAST(NULL AS SIGNED) AS HIGH_VALUE_LENGTH,           CAST(NULL AS UNSIGNED) AS PARTITION_POSITION   FROM        DUAL   WHERE       0 = 1   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablegroup_subpartitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST("" AS CHAR(128)) AS TABLEGROUP_NAME,           CAST("" AS CHAR(64)) AS PARTITION_NAME,           CAST("" AS CHAR(64)) AS SUBPARTITION_NAME,           CAST(NULL AS CHAR(4096)) AS HIGH_VALUE,           CAST(NULL AS SIGNED) AS HIGH_VALUE_LENGTH,           CAST(NULL AS UNSIGNED) AS PARTITION_POSITION,           CAST(NULL AS UNSIGNED) AS SUBPARTITION_POSITION    FROM        DUAL    WHERE       0 = 1   )__"))) {
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

int ObInnerTableSchema::cdb_ob_tablegroup_subpartitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TABLEGROUP_SUBPARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TABLEGROUP_SUBPARTITIONS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(NULL AS SIGNED) AS TENANT_ID,           CAST("" AS CHAR(128)) AS TABLEGROUP_NAME,           CAST("" AS CHAR(64)) AS PARTITION_NAME,           CAST("" AS CHAR(64)) AS SUBPARTITION_NAME,           CAST(NULL AS CHAR(4096)) AS HIGH_VALUE,           CAST(NULL AS SIGNED) AS HIGH_VALUE_LENGTH,           CAST(NULL AS UNSIGNED) AS PARTITION_POSITION,           CAST(NULL AS UNSIGNED) AS SUBPARTITION_POSITION     FROM          DUAL    WHERE         0 = 1   )__"))) {
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

int ObInnerTableSchema::dba_ob_databases_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_DATABASES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_DATABASES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT D.DATABASE_NAME AS DATABASE_NAME,          (CASE D.IN_RECYCLEBIN WHEN 0 THEN 'NO' ELSE 'YES' END) AS IN_RECYCLEBIN,          C.COLLATION AS COLLATION,          (CASE D.READ_ONLY WHEN 0 THEN 'NO' ELSE 'YES' END) AS READ_ONLY,          D.COMMENT AS COMMENT   FROM OCEANBASE.__ALL_DATABASE AS D   LEFT JOIN OCEANBASE.__TENANT_VIRTUAL_COLLATION AS C   ON D.COLLATION_TYPE = C.COLLATION_TYPE   )__"))) {
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

int ObInnerTableSchema::cdb_ob_databases_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_DATABASES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_DATABASES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT D.TENANT_ID,          D.DATABASE_NAME AS DATABASE_NAME,          (CASE D.IN_RECYCLEBIN WHEN 0 THEN 'NO' ELSE 'YES' END) AS IN_RECYCLEBIN,          C.COLLATION AS COLLATION,          (CASE D.READ_ONLY WHEN 0 THEN 'NO' ELSE 'YES' END) AS READ_ONLY,          D.COMMENT AS COMMENT   FROM OCEANBASE.__ALL_VIRTUAL_DATABASE AS D   LEFT JOIN OCEANBASE.__TENANT_VIRTUAL_COLLATION AS C   ON D.COLLATION_TYPE = C.COLLATION_TYPE   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablegroup_tables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLEGROUP_TABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLEGROUP_TABLES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TG.TABLEGROUP_NAME AS TABLEGROUP_NAME,          D.DATABASE_NAME AS OWNER,          T.TABLE_NAME AS TABLE_NAME,          TG.SHARDING AS SHARDING   FROM OCEANBASE.__ALL_TABLE AS T   JOIN OCEANBASE.__ALL_DATABASE AS D   ON T.TENANT_ID = D.TENANT_ID AND T.DATABASE_ID = D.DATABASE_ID   JOIN OCEANBASE.__ALL_TABLEGROUP AS TG   ON T.TENANT_ID = TG.TENANT_ID AND T.TABLEGROUP_ID = TG.TABLEGROUP_ID   WHERE T.TABLE_TYPE in (0, 3, 6)   )__"))) {
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

int ObInnerTableSchema::cdb_ob_tablegroup_tables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TABLEGROUP_TABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TABLEGROUP_TABLES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT T.TENANT_ID AS TENANT_ID,          TG.TABLEGROUP_NAME AS TABLEGROUP_NAME,          D.DATABASE_NAME AS OWNER,          T.TABLE_NAME AS TABLE_NAME,          TG.SHARDING AS SHARDING   FROM OCEANBASE.__ALL_VIRTUAL_TABLE AS T   JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE AS D   ON T.TENANT_ID = D.TENANT_ID AND T.DATABASE_ID = D.DATABASE_ID   JOIN OCEANBASE.__ALL_VIRTUAL_TABLEGROUP AS TG   ON T.TENANT_ID = TG.TENANT_ID AND T.TABLEGROUP_ID = TG.TABLEGROUP_ID   WHERE T.TABLE_TYPE in (0, 3, 6)   )__"))) {
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

int ObInnerTableSchema::dba_ob_zone_major_compaction_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ZONE_MAJOR_COMPACTION_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ZONE_MAJOR_COMPACTION_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT ZONE,          BROADCAST_SCN,          LAST_MERGED_SCN AS LAST_SCN,          USEC_TO_TIME(LAST_MERGED_TIME) AS LAST_FINISH_TIME,          USEC_TO_TIME(MERGE_START_TIME) AS START_TIME,          (CASE MERGE_STATUS                 WHEN 0 THEN 'IDLE'                 WHEN 1 THEN 'COMPACTING'                 ELSE 'UNKNOWN' END) AS STATUS   FROM OCEANBASE.__ALL_VIRTUAL_ZONE_MERGE_INFO   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::cdb_ob_zone_major_compaction_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_ZONE_MAJOR_COMPACTION_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_ZONE_MAJOR_COMPACTION_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          ZONE,          BROADCAST_SCN,          LAST_MERGED_SCN AS LAST_SCN,          USEC_TO_TIME(LAST_MERGED_TIME) AS LAST_FINISH_TIME,          USEC_TO_TIME(MERGE_START_TIME) AS START_TIME,          (CASE MERGE_STATUS                 WHEN 0 THEN 'IDLE'                 WHEN 1 THEN 'COMPACTING'                 ELSE 'UNKNOWN' END) AS STATUS   FROM OCEANBASE.__ALL_VIRTUAL_ZONE_MERGE_INFO   )__"))) {
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

int ObInnerTableSchema::dba_ob_major_compaction_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_MAJOR_COMPACTION_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_MAJOR_COMPACTION_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT FROZEN_SCN,          USEC_TO_TIME(FROZEN_SCN/1000) AS FROZEN_TIME,          GLOBAL_BROADCAST_SCN,          LAST_MERGED_SCN AS LAST_SCN,          USEC_TO_TIME(LAST_MERGED_TIME) AS LAST_FINISH_TIME,          USEC_TO_TIME(MERGE_START_TIME) AS START_TIME,          (CASE MERGE_STATUS                 WHEN 0 THEN 'IDLE'                 WHEN 1 THEN 'COMPACTING'                 WHEN 2 THEN 'VERIFYING'                 ELSE 'UNKNOWN' END) AS STATUS,          (CASE IS_MERGE_ERROR WHEN 0 THEN 'NO' ELSE 'YES' END) AS IS_ERROR,          (CASE SUSPEND_MERGING WHEN 0 THEN 'NO' ELSE 'YES' END) AS IS_SUSPENDED,          (CASE ERROR_TYPE                 WHEN 0 THEN ''                 WHEN 1 THEN 'CHECKSUM_ERROR'                 ELSE 'UNKNOWN' END) AS INFO   FROM OCEANBASE.__ALL_VIRTUAL_MERGE_INFO   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::cdb_ob_major_compaction_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_MAJOR_COMPACTION_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_MAJOR_COMPACTION_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          FROZEN_SCN,          USEC_TO_TIME(FROZEN_SCN/1000) AS FROZEN_TIME,          GLOBAL_BROADCAST_SCN,          LAST_MERGED_SCN AS LAST_SCN,          USEC_TO_TIME(LAST_MERGED_TIME) AS LAST_FINISH_TIME,          USEC_TO_TIME(MERGE_START_TIME) AS START_TIME,          (CASE MERGE_STATUS                 WHEN 0 THEN 'IDLE'                 WHEN 1 THEN 'COMPACTING'                 WHEN 2 THEN 'VERIFYING'                 ELSE 'UNKNOWN' END) AS STATUS,          (CASE IS_MERGE_ERROR WHEN 0 THEN 'NO' ELSE 'YES' END) AS IS_ERROR,          (CASE SUSPEND_MERGING WHEN 0 THEN 'NO' ELSE 'YES' END) AS IS_SUSPENDED,          (CASE ERROR_TYPE                 WHEN 0 THEN ''                 WHEN 1 THEN 'CHECKSUM_ERROR'                 ELSE 'UNKNOWN' END) AS INFO   FROM OCEANBASE.__ALL_VIRTUAL_MERGE_INFO   )__"))) {
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

int ObInnerTableSchema::cdb_objects_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OBJECTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OBJECTS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(         SELECT       CAST(A.TENANT_ID AS SIGNED) AS CON_ID,       CAST(B.DATABASE_NAME AS CHAR(128)) AS OWNER,       CAST(A.OBJECT_NAME AS CHAR(128)) AS OBJECT_NAME,       CAST(A.SUBOBJECT_NAME AS CHAR(128)) AS SUBOBJECT_NAME,       CAST(A.OBJECT_ID AS SIGNED) AS OBJECT_ID,       CAST(A.DATA_OBJECT_ID AS SIGNED) AS DATA_OBJECT_ID,       CAST(A.OBJECT_TYPE AS CHAR(23)) AS OBJECT_TYPE,       CAST(A.GMT_CREATE AS DATETIME) AS CREATED,       CAST(A.GMT_MODIFIED AS DATETIME) AS LAST_DDL_TIME,       CAST(A.GMT_CREATE AS DATETIME) AS TIMESTAMP,       CAST(A.STATUS AS CHAR(7)) AS STATUS,       CAST(A.TEMPORARY AS CHAR(1)) AS TEMPORARY,       CAST(A.`GENERATED` AS CHAR(1)) AS "GENERATED",       CAST(A.SECONDARY AS CHAR(1)) AS SECONDARY,       CAST(A.NAMESPACE AS SIGNED) AS NAMESPACE,       CAST(A.EDITION_NAME AS CHAR(128)) AS EDITION_NAME,       CAST(NULL AS CHAR(18)) AS SHARING,       CAST(NULL AS CHAR(1)) AS EDITIONABLE,       CAST(NULL AS CHAR(1)) AS ORACLE_MAINTAINED,       CAST(NULL AS CHAR(1)) AS APPLICATION,       CAST(NULL AS CHAR(1)) AS DEFAULT_COLLATION,       CAST(NULL AS CHAR(1)) AS DUPLICATED,       CAST(NULL AS CHAR(1)) AS SHARDED,       CAST(NULL AS CHAR(1)) AS IMPORTED_OBJECT,       CAST(NULL AS SIGNED) AS CREATED_APPID,       CAST(NULL AS SIGNED) AS CREATED_VSNID,       CAST(NULL AS SIGNED) AS MODIFIED_APPID,       CAST(NULL AS SIGNED) AS MODIFIED_VSNID     FROM (        SELECT A.TENANT_ID,              USEC_TO_TIME(B.SCHEMA_VERSION) AS GMT_CREATE,              USEC_TO_TIME(A.SCHEMA_VERSION) AS GMT_MODIFIED,              A.DATABASE_ID,              A.TABLE_NAME AS OBJECT_NAME,              NULL AS SUBOBJECT_NAME,              CAST(A.TABLE_ID AS SIGNED) AS OBJECT_ID,              A.TABLET_ID AS DATA_OBJECT_ID,              'TABLE' AS OBJECT_TYPE,              'VALID' AS STATUS,              'N' AS TEMPORARY,              'N' AS "GENERATED",              'N' AS SECONDARY,              0 AS NAMESPACE,              NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE A       JOIN OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE B         ON A.TENANT_ID = B.TENANT_ID AND B.TABLE_NAME = '__all_core_table'        UNION ALL        SELECT       TENANT_ID       ,GMT_CREATE       ,GMT_MODIFIED       ,DATABASE_ID       ,CAST((CASE              WHEN DATABASE_ID = 201004 THEN TABLE_NAME              WHEN TABLE_TYPE = 5 THEN SUBSTR(TABLE_NAME, 7 + POSITION('_' IN SUBSTR(TABLE_NAME, 7)))              ELSE TABLE_NAME END) AS CHAR(128)) AS OBJECT_NAME       ,NULL SUBOBJECT_NAME       ,TABLE_ID OBJECT_ID       ,(CASE WHEN TABLET_ID != 0 THEN TABLET_ID ELSE NULL END) DATA_OBJECT_ID       ,CASE WHEN TABLE_TYPE IN (0,3,6,8,9,14) THEN 'TABLE'             WHEN TABLE_TYPE IN (2) THEN 'VIRTUAL TABLE'             WHEN TABLE_TYPE IN (1,4) THEN 'VIEW'             WHEN TABLE_TYPE IN (5) THEN 'INDEX'             WHEN TABLE_TYPE IN (7) THEN 'MATERIALIZED VIEW'             ELSE NULL END AS OBJECT_TYPE       ,CAST(CASE WHEN TABLE_TYPE IN (5) THEN CASE WHEN INDEX_STATUS = 2 THEN 'VALID'               WHEN INDEX_STATUS = 3 THEN 'CHECKING'               WHEN INDEX_STATUS = 4 THEN 'INELEGIBLE'               WHEN INDEX_STATUS = 5 THEN 'ERROR'               ELSE 'UNUSABLE' END             ELSE  CASE WHEN OBJECT_STATUS = 1 THEN 'VALID' ELSE 'INVALID' END END AS CHAR(10)) AS STATUS       ,CASE WHEN TABLE_TYPE IN (6,8,9) THEN 'Y'           ELSE 'N' END AS TEMPORARY       ,CASE WHEN TABLE_TYPE IN (0,1) THEN 'Y'           ELSE 'N' END AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM       OCEANBASE.__ALL_VIRTUAL_TABLE       WHERE TABLE_TYPE != 12 AND TABLE_TYPE != 13        UNION ALL        SELECT          CST.TENANT_ID          ,CST.GMT_CREATE          ,CST.GMT_MODIFIED          ,DB.DATABASE_ID          ,CST.constraint_name AS OBJECT_NAME          ,NULL AS SUBOBJECT_NAME          ,TBL.TABLE_ID AS OBJECT_ID          ,NULL AS DATA_OBJECT_ID          ,'INDEX' AS OBJECT_TYPE          ,'VALID' AS STATUS          ,'N' AS TEMPORARY          ,'N' AS "GENERATED"          ,'N' AS SECONDARY          ,0 AS NAMESPACE          ,NULL AS EDITION_NAME          FROM OCEANBASE.__ALL_VIRTUAL_CONSTRAINT CST, OCEANBASE.__ALL_VIRTUAL_TABLE TBL, OCEANBASE.__ALL_VIRTUAL_DATABASE DB          WHERE CST.TENANT_ID = TBL.TENANT_ID AND TBL.TENANT_ID = DB.TENANT_ID AND DB.DATABASE_ID = TBL.DATABASE_ID AND TBL.TABLE_ID = CST.TABLE_ID and CST.CONSTRAINT_TYPE = 1        UNION ALL        SELECT       P.TENANT_ID       ,P.GMT_CREATE       ,P.GMT_MODIFIED       ,T.DATABASE_ID       ,CAST((CASE              WHEN T.DATABASE_ID = 201004 THEN T.TABLE_NAME              WHEN T.TABLE_TYPE = 5 THEN SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7)))              ELSE T.TABLE_NAME END) AS CHAR(128)) AS OBJECT_NAME       ,P.PART_NAME SUBOBJECT_NAME       ,P.PART_ID OBJECT_ID       ,CASE WHEN P.TABLET_ID != 0 THEN P.TABLET_ID ELSE NULL END AS DATA_OBJECT_ID       ,(CASE WHEN T.TABLE_TYPE = 5 THEN 'INDEX PARTITION' ELSE 'TABLE PARTITION' END) AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       , NULL AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_TABLE T JOIN OCEANBASE.__ALL_VIRTUAL_PART P ON T.TABLE_ID = P.TABLE_ID       WHERE T.TENANT_ID = P.TENANT_ID        UNION ALL        SELECT       SUBP.TENANT_ID       ,SUBP.GMT_CREATE       ,SUBP.GMT_MODIFIED       ,T.DATABASE_ID       ,CAST((CASE              WHEN T.DATABASE_ID = 201004 THEN T.TABLE_NAME              WHEN T.TABLE_TYPE = 5 THEN SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7)))              ELSE T.TABLE_NAME END) AS CHAR(128)) AS OBJECT_NAME       ,SUBP.SUB_PART_NAME SUBOBJECT_NAME       ,SUBP.SUB_PART_ID OBJECT_ID       ,SUBP.TABLET_ID AS DATA_OBJECT_ID       ,(CASE WHEN T.TABLE_TYPE = 5 THEN 'INDEX SUBPARTITION' ELSE 'TABLE SUBPARTITION' END) AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       ,'Y' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_TABLE T, OCEANBASE.__ALL_VIRTUAL_PART P,OCEANBASE.__ALL_VIRTUAL_SUB_PART SUBP       WHERE T.TABLE_ID =P.TABLE_ID AND P.TABLE_ID=SUBP.TABLE_ID AND P.PART_ID =SUBP.PART_ID       AND T.TENANT_ID = P.TENANT_ID AND P.TENANT_ID = SUBP.TENANT_ID        UNION ALL        SELECT       P.TENANT_ID       ,P.GMT_CREATE       ,P.GMT_MODIFIED       ,P.DATABASE_ID       ,P.PACKAGE_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,P.PACKAGE_ID OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,CASE WHEN TYPE = 1 THEN 'PACKAGE'             WHEN TYPE = 2 THEN 'PACKAGE BODY'             ELSE NULL END AS OBJECT_TYPE       ,CASE WHEN EXISTS                   (SELECT OBJ_ID FROM OCEANBASE.__ALL_VIRTUAL_ERROR E                     WHERE P.TENANT_ID = E.TENANT_ID AND P.PACKAGE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 3 OR E.OBJ_TYPE = 5))                  THEN 'INVALID'             ELSE 'VALID' END AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_PACKAGE P        UNION ALL        SELECT       R.TENANT_ID       ,R.GMT_CREATE       ,R.GMT_MODIFIED       ,R.DATABASE_ID       ,R.ROUTINE_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,R.ROUTINE_ID OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,CASE WHEN ROUTINE_TYPE = 1 THEN 'PROCEDURE'             WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'             ELSE NULL END AS OBJECT_TYPE       ,CASE WHEN EXISTS                   (SELECT OBJ_ID FROM OCEANBASE.__ALL_VIRTUAL_ERROR E                     WHERE R.TENANT_ID = E.TENANT_ID AND R.ROUTINE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 9 OR E.OBJ_TYPE = 12))                  THEN 'INVALID'             ELSE 'VALID' END AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_ROUTINE R       WHERE (ROUTINE_TYPE = 1 OR ROUTINE_TYPE = 2)        UNION ALL        SELECT       TENANT_ID       ,GMT_CREATE       ,GMT_MODIFIED       ,DATABASE_ID       ,TYPE_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,TYPE_ID OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,'TYPE' AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_TYPE        UNION ALL        SELECT       TENANT_ID       ,GMT_CREATE       ,GMT_MODIFIED       ,DATABASE_ID       ,OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,OBJECT_TYPE_ID OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,'TYPE BODY' AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_OBJECT_TYPE       WHERE TYPE = 2        UNION ALL        SELECT       T.TENANT_ID       ,T.GMT_CREATE       ,T.GMT_MODIFIED       ,T.DATABASE_ID       ,T.TRIGGER_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,T.TRIGGER_ID OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,'TRIGGER' OBJECT_TYPE       ,CASE WHEN EXISTS                   (SELECT OBJ_ID FROM OCEANBASE.__ALL_VIRTUAL_ERROR E                     WHERE T.TENANT_ID = E.TENANT_ID AND T.TRIGGER_ID = E.OBJ_ID AND (E.OBJ_TYPE = 7))                  THEN 'INVALID'             ELSE 'VALID' END AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_TRIGGER T        UNION ALL        SELECT       TENANT_ID       ,GMT_CREATE       ,GMT_MODIFIED       ,DATABASE_ID       ,SEQUENCE_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,SEQUENCE_ID OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,'SEQUENCE' AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_SEQUENCE_OBJECT        UNION ALL        SELECT       TENANT_ID       ,GMT_CREATE       ,GMT_MODIFIED       ,DATABASE_ID       ,SYNONYM_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,SYNONYM_ID OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,'SYNONYM' AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_SYNONYM        UNION ALL        SELECT         TENANT_ID         ,GMT_CREATE         ,GMT_MODIFIED         ,CAST(201006 AS SIGNED) AS DATABASE_ID         ,NAMESPACE AS OBJECT_NAME         ,NULL AS SUBOBJECT_NAME         ,CONTEXT_ID OBJECT_ID         ,NULL AS DATA_OBJECT_ID         ,'CONTEXT' AS OBJECT_TYPE         ,'VALID' AS STATUS         ,'N' AS TEMPORARY         ,'N' AS "GENERATED"         ,'N' AS SECONDARY         ,21 AS NAMESPACE         ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_TENANT_CONTEXT         UNION ALL        SELECT         TENANT_ID,         GMT_CREATE,         GMT_MODIFIED,         DATABASE_ID,         DATABASE_NAME AS OBJECT_NAME,         NULL AS SUBOBJECT_NAME,         DATABASE_ID AS OBJECT_ID,         NULL AS DATA_OBJECT_ID,         'DATABASE' AS OBJECT_TYPE,         'VALID' AS STATUS,         'N' AS TEMPORARY,         'N' AS "GENERATED",         'N' AS SECONDARY,         0 AS NAMESPACE,         NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_DATABASE        UNION ALL        SELECT         TENANT_ID,         GMT_CREATE,         GMT_MODIFIED,         CAST(201001 AS SIGNED) AS DATABASE_ID,         TABLEGROUP_NAME AS OBJECT_NAME,         NULL AS SUBOBJECT_NAME,         TABLEGROUP_ID AS OBJECT_ID,         NULL AS DATA_OBJECT_ID,         'TABLEGROUP' AS OBJECT_TYPE,         'VALID' AS STATUS,         'N' AS TEMPORARY,         'N' AS "GENERATED",         'N' AS SECONDARY,         0 AS NAMESPACE,         NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_TABLEGROUP     ) A     JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE B     ON A.TENANT_ID = B.TENANT_ID     AND A.DATABASE_ID = B.DATABASE_ID )__"))) {
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

int ObInnerTableSchema::cdb_tables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_TABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_TABLES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(DB.TENANT_ID AS SIGNED) AS CON_ID,   CAST(DB.DATABASE_NAME AS CHAR(128)) AS OWNER,   CAST(T.TABLE_NAME AS CHAR(128)) AS TABLE_NAME,   CAST(TP.TABLESPACE_NAME AS CHAR(30)) AS TABLESPACE_NAME,   CAST(NULL AS CHAR(128)) AS CLUSTER_NAME,   CAST(NULL AS CHAR(128)) AS IOT_NAME,   CAST('VALID' AS CHAR(8)) AS STATUS,   CAST(T.PCTFREE AS SIGNED) AS PCT_FREE,   CAST(NULL AS SIGNED) AS PCT_USED,   CAST(NULL AS SIGNED) AS INI_TRANS,   CAST(NULL AS SIGNED) AS MAX_TRANS,   CAST(NULL AS SIGNED) AS INITIAL_EXTENT,   CAST(NULL AS SIGNED) AS NEXT_EXTENT,   CAST(NULL AS SIGNED) AS MIN_EXTENTS,   CAST(NULL AS SIGNED) AS MAX_EXTENTS,   CAST(NULL AS SIGNED) AS PCT_INCREASE,   CAST(NULL AS SIGNED) AS FREELISTS,   CAST(NULL AS SIGNED) AS FREELIST_GROUPS,   CAST(NULL AS CHAR(3)) AS LOGGING,   CAST(NULL AS CHAR(1)) AS BACKED_UP,   CAST(INFO.ROW_COUNT AS SIGNED) AS NUM_ROWS,   CAST(NULL AS SIGNED) AS BLOCKS,   CAST(NULL AS SIGNED) AS EMPTY_BLOCKS,   CAST(NULL AS SIGNED) AS AVG_SPACE,   CAST(NULL AS SIGNED) AS CHAIN_CNT,   CAST(NULL AS SIGNED) AS AVG_ROW_LEN,   CAST(NULL AS SIGNED) AS AVG_SPACE_FREELIST_BLOCKS,   CAST(NULL AS SIGNED) AS NUM_FREELIST_BLOCKS,   CAST(NULL AS CHAR(10)) AS DEGREE,   CAST(NULL AS CHAR(10)) AS INSTANCES,   CAST(NULL AS CHAR(5)) AS CACHE,   CAST(NULL AS CHAR(8)) AS TABLE_LOCK,   CAST(NULL AS SIGNED) AS SAMPLE_SIZE,   CAST(NULL AS DATE) AS LAST_ANALYZED,   CAST(   CASE     WHEN       T.PART_LEVEL = 0     THEN       'NO'     ELSE       'YES'   END   AS CHAR(3)) AS PARTITIONED,   CAST(NULL AS CHAR(12)) AS IOT_TYPE,   CAST(CASE WHEN T.TABLE_TYPE IN (6, 8, 9) THEN 'Y' ELSE 'N' END AS CHAR(1)) AS TEMPORARY,   CAST(NULL AS CHAR(1)) AS SECONDARY,   CAST('NO' AS CHAR(3)) AS NESTED,   CAST(NULL AS CHAR(7)) AS BUFFER_POOL,   CAST(NULL AS CHAR(7)) AS FLASH_CACHE,   CAST(NULL AS CHAR(7)) AS CELL_FLASH_CACHE,   CAST(NULL AS CHAR(8)) AS ROW_MOVEMENT,   CAST(NULL AS CHAR(3)) AS GLOBAL_STATS,   CAST(NULL AS CHAR(3)) AS USER_STATS,   CAST(CASE WHEN T.TABLE_TYPE IN (6, 8) THEN 'SYS$SESSION'             WHEN T.TABLE_TYPE IN (9) THEN 'SYS$TRANSACTION'             ELSE NULL END AS CHAR(15)) AS DURATION,   CAST(NULL AS CHAR(8)) AS SKIP_CORRUPT,   CAST(NULL AS CHAR(3)) AS MONITORING,   CAST(NULL AS CHAR(128)) AS CLUSTER_OWNER,   CAST(NULL AS CHAR(8)) AS DEPENDENCIES,   CAST(NULL AS CHAR(8)) AS COMPRESSION,   CAST(NULL AS CHAR(30)) AS COMPRESS_FOR,   CAST(CASE WHEN DB.IN_RECYCLEBIN = 1 THEN 'YES' ELSE 'NO' END AS CHAR(3)) AS DROPPED,   CAST(NULL AS CHAR(3)) AS READ_ONLY,   CAST(NULL AS CHAR(3)) AS SEGMENT_CREATED,   CAST(NULL AS CHAR(7)) AS RESULT_CACHE,   CAST(NULL AS CHAR(3)) AS CLUSTERING,   CAST(NULL AS CHAR(23)) AS ACTIVITY_TRACKING,   CAST(NULL AS CHAR(25)) AS DML_TIMESTAMP,   CAST(NULL AS CHAR(3)) AS HAS_IDENTITY,   CAST(NULL AS CHAR(3)) AS CONTAINER_DATA,   CAST(NULL AS CHAR(8)) AS INMEMORY,   CAST(NULL AS CHAR(8)) AS INMEMORY_PRIORITY,   CAST(NULL AS CHAR(15)) AS INMEMORY_DISTRIBUTE,   CAST(NULL AS CHAR(17)) AS INMEMORY_COMPRESSION,   CAST(NULL AS CHAR(13)) AS INMEMORY_DUPLICATE,   CAST(NULL AS CHAR(100)) AS DEFAULT_COLLATION,   CAST(NULL AS CHAR(1)) AS DUPLICATED,   CAST(NULL AS CHAR(1)) AS SHARDED,   CAST(NULL AS CHAR(1)) AS EXTERNALLY_SHARDED,   CAST(NULL AS CHAR(1)) AS EXTERNALLY_DUPLICATED,   CAST(CASE WHEN T.TABLE_TYPE IN (14) THEN 'YES' ELSE 'NO' END AS CHAR(3)) AS EXTERNAL,   CAST(NULL AS CHAR(3)) AS HYBRID,   CAST(NULL AS CHAR(24)) AS CELLMEMORY,   CAST(NULL AS CHAR(3)) AS CONTAINERS_DEFAULT,   CAST(NULL AS CHAR(3)) AS CONTAINER_MAP,   CAST(NULL AS CHAR(3)) AS EXTENDED_DATA_LINK,   CAST(NULL AS CHAR(3)) AS EXTENDED_DATA_LINK_MAP,   CAST(NULL AS CHAR(12)) AS INMEMORY_SERVICE,   CAST(NULL AS CHAR(1000)) AS INMEMORY_SERVICE_NAME,   CAST(NULL AS CHAR(3)) AS CONTAINER_MAP_OBJECT,   CAST(NULL AS CHAR(8)) AS MEMOPTIMIZE_READ,   CAST(NULL AS CHAR(8)) AS MEMOPTIMIZE_WRITE,   CAST(NULL AS CHAR(3)) AS HAS_SENSITIVE_COLUMN,   CAST(NULL AS CHAR(3)) AS ADMIT_NULL,   CAST(NULL AS CHAR(3)) AS DATA_LINK_DML_ENABLED,   CAST(NULL AS CHAR(8)) AS LOGICAL_REPLICATION FROM   (SELECT      TENANT_ID,      TABLE_ID,      SUM(ROW_CNT) AS ROW_COUNT    FROM      OCEANBASE.__ALL_VIRTUAL_TABLE_STAT TS    GROUP BY TENANT_ID, TABLE_ID   ) INFO    RIGHT JOIN   (SELECT      TENANT_ID,      TABLE_ID,      TABLE_NAME,      DATABASE_ID,      PCTFREE,      PART_LEVEL,      TABLE_TYPE,      TABLESPACE_ID    FROM      OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE     UNION ALL     SELECT      TENANT_ID,      TABLE_ID,      TABLE_NAME,      DATABASE_ID,      PCTFREE,      PART_LEVEL,      TABLE_TYPE,      TABLESPACE_ID    FROM OCEANBASE.__ALL_VIRTUAL_TABLE) T   ON     T.TENANT_ID = INFO.TENANT_ID     AND T.TABLE_ID = INFO.TABLE_ID    JOIN     OCEANBASE.__ALL_VIRTUAL_DATABASE DB   ON     DB.TENANT_ID = T.TENANT_ID     AND DB.DATABASE_ID = T.DATABASE_ID     AND T.TABLE_TYPE IN (0, 3, 6, 8, 9, 14)     AND DB.DATABASE_NAME != '__recyclebin'    LEFT JOIN     OCEANBASE.__ALL_VIRTUAL_TENANT_TABLESPACE TP   ON     TP.TABLESPACE_ID = T.TABLESPACE_ID     AND T.TENANT_ID = TP.TENANT_ID )__"))) {
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

int ObInnerTableSchema::cdb_tab_cols_v_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_TAB_COLS_V_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_TAB_COLS_V_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT/*+leading(DB,T,C,STAT)*/   CAST(DB.TENANT_ID AS SIGNED) AS CON_ID,   CAST(DB.DATABASE_NAME AS CHAR(128)) AS OWNER,   CAST(T.TABLE_NAME AS CHAR(128)) AS  TABLE_NAME,   CAST(C.COLUMN_NAME AS CHAR(128)) AS  COLUMN_NAME,   CAST(CASE C.DATA_TYPE         WHEN 0 THEN 'VARCHAR2'          WHEN 1 THEN 'NUMBER'         WHEN 2 THEN 'NUMBER'         WHEN 3 THEN 'NUMBER'         WHEN 4 THEN 'NUMBER'         WHEN 5 THEN 'NUMBER'          WHEN 6 THEN 'NUMBER'         WHEN 7 THEN 'NUMBER'         WHEN 8 THEN 'NUMBER'         WHEN 9 THEN 'NUMBER'         WHEN 10 THEN 'NUMBER'          WHEN 11 THEN 'BINARY_FLOAT'         WHEN 12 THEN 'BINARY_DOUBLE'          WHEN 13 THEN 'NUMBER'         WHEN 14 THEN 'NUMBER'          WHEN 15 THEN 'NUMBER'         WHEN 16 THEN 'NUMBER'          WHEN 17 THEN 'DATE'         WHEN 18 THEN 'TIMESTAMP'         WHEN 19 THEN 'DATE'         WHEN 20 THEN 'TIME'         WHEN 21 THEN 'YEAR'          WHEN 22 THEN 'VARCHAR2'         WHEN 23 THEN 'CHAR'         WHEN 24 THEN 'HEX_STRING'          WHEN 25 THEN 'UNDEFINED'         WHEN 26 THEN 'UNKNOWN'          WHEN 27 THEN 'TINYTEXT'         WHEN 28 THEN 'TEXT'         WHEN 29 THEN 'MEDIUMTEXT'         WHEN 30 THEN (CASE C.COLLATION_TYPE WHEN 63 THEN 'BLOB' ELSE 'CLOB' END)         WHEN 31 THEN 'BIT'         WHEN 32 THEN 'ENUM'         WHEN 33 THEN 'SET'         WHEN 34 THEN 'ENUM_INNER'         WHEN 35 THEN 'SET_INNER'         WHEN 36 THEN CONCAT('TIMESTAMP(', CONCAT(C.DATA_SCALE, ') WITH TIME ZONE'))         WHEN 37 THEN CONCAT('TIMESTAMP(', CONCAT(C.DATA_SCALE, ') WITH LOCAL TIME ZONE'))         WHEN 38 THEN CONCAT('TIMESTAMP(', CONCAT(C.DATA_SCALE, ')'))         WHEN 39 THEN 'RAW'         WHEN 40 THEN CONCAT('INTERVAL YEAR(', CONCAT(C.DATA_SCALE, ') TO MONTH'))         WHEN 41 THEN CONCAT('INTERVAL DAY(', CONCAT(FLOOR(C.DATA_SCALE/10), CONCAT(') TO SECOND(', CONCAT(MOD(C.DATA_SCALE, 10), ')'))))         WHEN 42 THEN 'FLOAT'         WHEN 43 THEN 'NVARCHAR2'         WHEN 44 THEN 'NCHAR'         WHEN 45 THEN 'UROWID'         WHEN 46 THEN ''         ELSE 'UNDEFINED' END AS CHAR(128)) AS  DATA_TYPE,   CAST(NULL AS CHAR(3)) AS  DATA_TYPE_MOD,   CAST(NULL AS CHAR(128)) AS  DATA_TYPE_OWNER,   CAST(C.DATA_LENGTH * CASE WHEN C.DATA_TYPE IN (22,23,30,43,44,46) AND C.DATA_PRECISION = 1                             THEN (CASE C.COLLATION_TYPE                                   WHEN 63  THEN 1                                   WHEN 249 THEN 4                                   WHEN 248 THEN 4                                   WHEN 87  THEN 2                                   WHEN 28  THEN 2                                   WHEN 55  THEN 4                                   WHEN 54  THEN 4                                   WHEN 101 THEN 2                                   WHEN 46  THEN 4                                   WHEN 45  THEN 4                                   WHEN 224 THEN 4                                   ELSE 1 END)                             ELSE 1 END                             AS SIGNED) AS  DATA_LENGTH,   CAST(CASE WHEN C.DATA_TYPE IN (0,11,12,17,18,19,22,23,27,28,29,30,36,37,38,43,44)             THEN NULL             ELSE CASE WHEN C.DATA_PRECISION < 0 THEN NULL ELSE C.DATA_PRECISION END        END AS SIGNED) AS  DATA_PRECISION,   CAST(CASE WHEN C.DATA_TYPE IN (0,11,12,17,19,22,23,27,28,29,30,42,43,44)             THEN NULL             ELSE CASE WHEN C.DATA_SCALE < -84 THEN NULL ELSE C.DATA_SCALE END        END AS SIGNED) AS  DATA_SCALE,   CAST((CASE         WHEN C.NULLABLE = 0 THEN 'N'         WHEN (C.COLUMN_FLAGS & (5 * POWER(2, 13))) = 5 * POWER(2, 13) THEN 'N'         ELSE 'Y' END) AS CHAR(1)) AS  NULLABLE,   CAST(CASE WHEN (C.COLUMN_FLAGS & 64) = 0 THEN C.COLUMN_ID ELSE NULL END AS SIGNED) AS  COLUMN_ID,   CAST(LENGTH(C.CUR_DEFAULT_VALUE_V2) AS SIGNED) AS  DEFAULT_LENGTH,   CAST(C.CUR_DEFAULT_VALUE_V2 AS /* TODO: LONG() */ CHAR(262144)) AS  DATA_DEFAULT,   CAST(STAT.DISTINCT_CNT AS SIGNED) AS  NUM_DISTINCT,   CAST(STAT.MIN_VALUE AS /* TODO: RAW */ CHAR(128)) AS  LOW_VALUE,   CAST(STAT.MAX_VALUE AS /* TODO: RAW */ CHAR(128)) AS  HIGH_VALUE,   CAST(STAT.DENSITY AS SIGNED) AS  DENSITY,   CAST(STAT.NULL_CNT AS SIGNED) AS  NUM_NULLS,   CAST(STAT.BUCKET_CNT AS SIGNED) AS  NUM_BUCKETS,   CAST(STAT.LAST_ANALYZED AS DATE) AS  LAST_ANALYZED,   CAST(STAT.SAMPLE_SIZE AS SIGNED) AS  SAMPLE_SIZE,   CAST(CASE C.DATA_TYPE        WHEN 22 THEN 'CHAR_CS'        WHEN 23 THEN 'CHAR_CS'        WHEN 30 THEN (CASE WHEN C.COLLATION_TYPE = 63 THEN 'NULL' ELSE 'CHAR_CS' END)        WHEN 43 THEN 'NCHAR_CS'        WHEN 44 THEN 'NCHAR_CS'        ELSE '' END AS CHAR(44)) AS  CHARACTER_SET_NAME,   CAST(NULL AS SIGNED) AS  CHAR_COL_DECL_LENGTH,   CAST(CASE STAT.GLOBAL_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END AS CHAR(3)) AS GLOBAL_STATS,   CAST(CASE STAT.USER_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END AS CHAR(3)) AS USER_STATS,   CAST(NULL AS CHAR(80)) AS  NOTES,   CAST(STAT.AVG_LEN AS SIGNED) AS  AVG_COL_LEN,   CAST(CASE WHEN C.DATA_TYPE IN (22,23,43,44) THEN C.DATA_LENGTH ELSE 0 END AS SIGNED) AS  CHAR_LENGTH,   CAST(CASE C.DATA_TYPE        WHEN 22 THEN (CASE WHEN C.DATA_PRECISION = 1 THEN 'C' ELSE 'B' END)        WHEN 23 THEN (CASE WHEN C.DATA_PRECISION = 1 THEN 'C' ELSE 'B' END)        WHEN 43 THEN (CASE WHEN C.DATA_PRECISION = 1 THEN 'C' ELSE 'B' END)        WHEN 44 THEN (CASE WHEN C.DATA_PRECISION = 1 THEN 'C' ELSE 'B' END)        ELSE NULL END AS CHAR(1)) AS  CHAR_USED,   CAST(NULL AS CHAR(3)) AS  V80_FMT_IMAGE,   CAST(NULL AS CHAR(3)) AS  DATA_UPGRADED,   CAST(CASE WHEN (C.COLUMN_FLAGS & 64) = 0 THEN 'NO'  ELSE 'YES' END AS CHAR(3)) AS HIDDEN_COLUMN,   CAST(CASE WHEN (C.COLUMN_FLAGS & 1)  = 1 THEN 'YES' ELSE 'NO'  END AS CHAR(3)) AS  VIRTUAL_COLUMN,   CAST(NULL AS SIGNED) AS  SEGMENT_COLUMN_ID,   CAST(NULL AS SIGNED) AS  INTERNAL_COLUMN_ID,   CAST((CASE WHEN STAT.HISTOGRAM_TYPE = 1 THEN 'FREQUENCY'         WHEN STAT.HISTOGRAM_TYPE = 3 THEN 'TOP-FREQUENCY'         WHEN STAT.HISTOGRAM_TYPE = 4 THEN 'HYBRID'         ELSE NULL END) AS CHAR(15)) AS HISTOGRAM,   CAST(C.COLUMN_NAME AS CHAR(4000)) AS  QUALIFIED_COL_NAME,   CAST('YES' AS CHAR(3)) AS  USER_GENERATED,   CAST(NULL AS CHAR(3)) AS  DEFAULT_ON_NULL,   CAST(NULL AS CHAR(3)) AS  IDENTITY_COLUMN,   CAST(NULL AS CHAR(128)) AS  EVALUATION_EDITION,   CAST(NULL AS CHAR(128)) AS  UNUSABLE_BEFORE,   CAST(NULL AS CHAR(128)) AS  UNUSABLE_BEGINNING,   CAST(NULL AS CHAR(100)) AS  COLLATION,   CAST(NULL AS SIGNED) AS  COLLATED_COLUMN_ID FROM     (SELECT TENANT_ID,             TABLE_ID,             DATABASE_ID,             TABLE_NAME,             TABLE_TYPE      FROM OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE      UNION ALL      SELECT TENANT_ID,             TABLE_ID,             DATABASE_ID,             TABLE_NAME,             TABLE_TYPE      FROM OCEANBASE.__ALL_VIRTUAL_TABLE) T   JOIN     OCEANBASE.__ALL_VIRTUAL_DATABASE DB   ON     DB.TENANT_ID = T.TENANT_ID     AND DB.DATABASE_ID = T.DATABASE_ID    JOIN      (SELECT TENANT_ID,              TABLE_ID,              COLUMN_ID,              COLUMN_NAME,              DATA_TYPE,              COLLATION_TYPE,              DATA_SCALE,              DATA_LENGTH,              DATA_PRECISION,              NULLABLE,              COLUMN_FLAGS,              CUR_DEFAULT_VALUE_V2,              IS_HIDDEN       FROM OCEANBASE.__ALL_VIRTUAL_CORE_COLUMN_TABLE        UNION ALL        SELECT TENANT_ID,              TABLE_ID,              COLUMN_ID,              COLUMN_NAME,              DATA_TYPE,              COLLATION_TYPE,              DATA_SCALE,              DATA_LENGTH,              DATA_PRECISION,              NULLABLE,              COLUMN_FLAGS,              CUR_DEFAULT_VALUE_V2,              IS_HIDDEN      FROM OCEANBASE.__ALL_VIRTUAL_COLUMN) C   ON     C.TENANT_ID = T.TENANT_ID     AND C.TABLE_ID = T.TABLE_ID     AND C.IS_HIDDEN = 0    LEFT JOIN     OCEANBASE.__ALL_VIRTUAL_COLUMN_STAT STAT    ON      C.TENANT_ID = STAT.TENANT_ID      AND C.TABLE_ID = STAT.TABLE_ID      AND C.COLUMN_ID = STAT.COLUMN_ID      AND STAT.OBJECT_TYPE = 1 WHERE   T.TABLE_TYPE IN (0,1,3,4,5,6,7,8,9,14) )__"))) {
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

int ObInnerTableSchema::cdb_tab_cols_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_TAB_COLS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_TAB_COLS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CON_ID,   OWNER,   TABLE_NAME,   COLUMN_NAME,   DATA_TYPE,   DATA_TYPE_MOD,   DATA_TYPE_OWNER,   DATA_LENGTH,   DATA_PRECISION,   DATA_SCALE,   NULLABLE,   COLUMN_ID,   DEFAULT_LENGTH,   DATA_DEFAULT,   NUM_DISTINCT,   LOW_VALUE,   HIGH_VALUE,   DENSITY,   NUM_NULLS,   NUM_BUCKETS,   LAST_ANALYZED,   SAMPLE_SIZE,   CHARACTER_SET_NAME,   CHAR_COL_DECL_LENGTH,   GLOBAL_STATS,   USER_STATS,   AVG_COL_LEN,   CHAR_LENGTH,   CHAR_USED,   V80_FMT_IMAGE,   DATA_UPGRADED,   HIDDEN_COLUMN,   VIRTUAL_COLUMN,   SEGMENT_COLUMN_ID,   INTERNAL_COLUMN_ID,   HISTOGRAM,   QUALIFIED_COL_NAME,   USER_GENERATED,   DEFAULT_ON_NULL,   IDENTITY_COLUMN,   EVALUATION_EDITION,   UNUSABLE_BEFORE,   UNUSABLE_BEGINNING,   COLLATION,   COLLATED_COLUMN_ID FROM OCEANBASE.CDB_TAB_COLS_V$ )__"))) {
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

int ObInnerTableSchema::cdb_indexes_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_INDEXES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_INDEXES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(C.TENANT_ID AS SIGNED) AS CON_ID,       CAST(INDEX_OWNER AS CHAR(128)) AS OWNER,       CAST(INDEX_NAME AS CHAR(128)) AS INDEX_NAME,       CAST(INDEX_TYPE_NAME AS CHAR(27)) AS INDEX_TYPE,       CAST(TABLE_OWNER AS CHAR(128)) AS TABLE_OWNER,       CAST(TABLE_NAME AS CHAR(128)) AS TABLE_NAME,       CAST('TABLE' AS CHAR(5)) AS TABLE_TYPE,       CAST(UNIQUENESS AS CHAR(9)) AS UNIQUENESS,       CAST(COMPRESSION AS CHAR(13)) AS COMPRESSION,       CAST(NULL AS NUMBER) AS PREFIX_LENGTH,       CAST(TABLESPACE_NAME AS CHAR(30)) AS TABLESPACE_NAME,       CAST(NULL AS NUMBER) AS INI_TRANS,       CAST(NULL AS NUMBER) AS MAX_TRANS,       CAST(NULL AS NUMBER) AS INITIAL_EXTENT,       CAST(NULL AS NUMBER) AS NEXT_EXTENT,       CAST(NULL AS NUMBER) AS MIN_EXTENTS,       CAST(NULL AS NUMBER) AS MAX_EXTENTS,       CAST(NULL AS NUMBER) AS PCT_INCREASE,       CAST(NULL AS NUMBER) AS PCT_THRESHOLD,       CAST(NULL AS NUMBER) AS INCLUDE_COLUMN,       CAST(NULL AS NUMBER) AS FREELISTS,       CAST(NULL AS NUMBER) AS FREELIST_GROUPS,       CAST(NULL AS NUMBER) AS PCT_FREE,       CAST(NULL AS CHAR(3)) AS LOGGING,       CAST(NULL AS NUMBER) AS BLEVEL,       CAST(NULL AS NUMBER) AS LEAF_BLOCKS,       CAST(NULL AS NUMBER) AS DISTINCT_KEYS,       CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,       CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,       CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,       CAST(STATUS AS CHAR(8)) AS STATUS,       CAST(NULL AS NUMBER) AS NUM_ROWS,       CAST(NULL AS NUMBER) AS SAMPLE_SIZE,       CAST(NULL AS DATE) AS LAST_ANALYZED,       CAST(DOP_DEGREE AS CHAR(40)) AS DEGREE,       CAST(NULL AS CHAR(40)) AS INSTANCES,       CAST(CASE WHEN A_PART_LEVEL = 0 THEN 'NO' ELSE 'YES' END AS CHAR(3)) AS PARTITIONED,       CAST(NULL AS CHAR(1)) AS TEMPORARY,       CAST(NULL AS CHAR(1)) AS "GENERATED",       CAST(NULL AS CHAR(1)) AS SECONDARY,       CAST(NULL AS CHAR(7)) AS BUFFER_POOL,       CAST(NULL AS CHAR(7)) AS FLASH_CACHE,       CAST(NULL AS CHAR(7)) AS CELL_FLASH_CACHE,       CAST(NULL AS CHAR(3)) AS USER_STATS,       CAST(NULL AS CHAR(15)) AS DURATION,       CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,       CAST(NULL AS CHAR(128)) AS ITYP_OWNER,       CAST(NULL AS CHAR(128)) AS ITYP_NAME,       CAST(NULL AS CHAR(1000)) AS PARAMETERS,       CAST(NULL AS CHAR(3)) AS GLOBAL_STATS,       CAST(NULL AS CHAR(12)) AS DOMIDX_STATUS,       CAST(NULL AS CHAR(6)) AS DOMIDX_OPSTATUS,       CAST(FUNCIDX_STATUS AS CHAR(8)) AS FUNCIDX_STATUS,       CAST('NO' AS CHAR(3)) AS JOIN_INDEX,       CAST(NULL AS CHAR(3)) AS IOT_REDUNDANT_PKEY_ELIM,       CAST(DROPPED AS CHAR(3)) AS DROPPED,       CAST(VISIBILITY AS CHAR(9)) AS VISIBILITY,       CAST(NULL AS CHAR(14)) AS DOMIDX_MANAGEMENT,       CAST(NULL AS CHAR(3)) AS SEGMENT_CREATED,       CAST(NULL AS CHAR(3)) AS ORPHANED_ENTRIES,       CAST(NULL AS CHAR(7)) AS INDEXING,       CAST(NULL AS CHAR(3)) AS AUTO       FROM         (SELECT         A.TENANT_ID AS TENANT_ID,         DATABASE_NAME AS INDEX_OWNER,          CASE WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME !=  '__recyclebin')              THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))              WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME =  '__recyclebin')              THEN TABLE_NAME              WHEN (TABLE_TYPE = 3 AND CONS_TAB.CONSTRAINT_NAME IS NULL)              THEN CONCAT('t_pk_obpk_', A.TABLE_ID)              ELSE (CONS_TAB.CONSTRAINT_NAME) END AS INDEX_NAME,          CASE           WHEN A.TABLE_TYPE = 5 AND EXISTS (             SELECT 1             FROM OCEANBASE.__ALL_VIRTUAL_COLUMN T_COL_INDEX,                  OCEANBASE.__ALL_VIRTUAL_COLUMN T_COL_BASE             WHERE T_COL_BASE.TABLE_ID = A.DATA_TABLE_ID               AND T_COL_BASE.COLUMN_NAME = T_COL_INDEX.COLUMN_NAME               AND T_COL_INDEX.TABLE_ID = A.TABLE_ID               AND T_COL_BASE.TENANT_ID = A.TENANT_ID               AND T_COL_INDEX.TENANT_ID = A.TENANT_ID               AND (T_COL_BASE.COLUMN_FLAGS & 3) > 0               AND T_COL_INDEX.INDEX_POSITION != 0           ) THEN 'FUNCTION-BASED NORMAL'           ELSE 'NORMAL'         END AS INDEX_TYPE_NAME,          DATABASE_NAME AS TABLE_OWNER,          CASE WHEN (TABLE_TYPE = 3) THEN A.TABLE_ID              ELSE A.DATA_TABLE_ID END AS TABLE_ID,          A.TABLE_ID AS INDEX_ID,          CASE WHEN TABLE_TYPE = 3 THEN 'UNIQUE'              WHEN A.INDEX_TYPE IN (2, 4, 8) THEN 'UNIQUE'              ELSE 'NONUNIQUE' END AS UNIQUENESS,          CASE WHEN A.COMPRESS_FUNC_NAME = NULL THEN 'DISABLED'              ELSE 'ENABLED' END AS COMPRESSION,          CASE WHEN TABLE_TYPE = 3 THEN 'VALID'              WHEN A.INDEX_STATUS = 2 THEN 'VALID'              WHEN A.INDEX_STATUS = 3 THEN 'CHECKING'              WHEN A.INDEX_STATUS = 4 THEN 'INELEGIBLE'              WHEN A.INDEX_STATUS = 5 THEN 'ERROR'              ELSE 'UNUSABLE' END AS STATUS,          A.INDEX_TYPE AS A_INDEX_TYPE,         A.PART_LEVEL AS A_PART_LEVEL,         A.TABLE_TYPE AS A_TABLE_TYPE,          CASE WHEN 0 = (SELECT COUNT(1) FROM OCEANBASE.__ALL_VIRTUAL_COLUMN                        WHERE TABLE_ID = A.TABLE_ID AND IS_HIDDEN = 0) THEN 'ENABLED'              ELSE 'NULL' END AS FUNCIDX_STATUS,          CASE WHEN B.IN_RECYCLEBIN = 1 THEN 'YES' ELSE 'NO' END AS DROPPED,          CASE WHEN (A.INDEX_ATTRIBUTES_SET & 1) = 0 THEN 'VISIBLE'              ELSE 'INVISIBLE' END AS VISIBILITY,          A.TABLESPACE_ID,         A.DOP AS DOP_DEGREE          FROM           OCEANBASE.__ALL_VIRTUAL_TABLE A           JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE B           ON A.DATABASE_ID = B.DATABASE_ID              AND A.TENANT_ID = B.TENANT_ID              AND B.DATABASE_NAME != '__recyclebin'            LEFT JOIN OCEANBASE.__ALL_VIRTUAL_CONSTRAINT CONS_TAB           ON CONS_TAB.TABLE_ID = A.TABLE_ID              AND CONS_TAB.TENANT_ID = A.TENANT_ID              AND CONS_TAB.CONSTRAINT_TYPE = 1         WHERE           (A.TABLE_TYPE = 3 AND A.TABLE_MODE & 66048 = 0) OR (A.TABLE_TYPE = 5)         ) C       JOIN OCEANBASE.__ALL_VIRTUAL_TABLE D         ON C.TABLE_ID = D.TABLE_ID            AND C.TENANT_ID = D.TENANT_ID        LEFT JOIN OCEANBASE.__ALL_VIRTUAL_TENANT_TABLESPACE TP       ON C.TABLESPACE_ID = TP.TABLESPACE_ID          AND TP.TENANT_ID = C.TENANT_ID )__"))) {
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

int ObInnerTableSchema::cdb_ind_columns_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_IND_COLUMNS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_IND_COLUMNS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT/*+leading(E,D,F)*/       CAST(E.TENANT_ID AS NUMBER) AS CON_ID,       CAST(INDEX_OWNER AS CHAR(128)) AS INDEX_OWNER,       CAST(INDEX_NAME AS CHAR(128)) AS INDEX_NAME,       CAST(TABLE_OWNER AS CHAR(128)) AS TABLE_OWNER,       CAST(TABLE_NAME AS CHAR(128)) AS TABLE_NAME,       CAST(COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,       CAST(ROWKEY_POSITION AS SIGNED) AS COLUMN_POSITION,        CASE WHEN DATA_TYPE >= 1 AND DATA_TYPE <= 16 THEN CAST(22 AS SIGNED)            WHEN DATA_TYPE = 17 THEN CAST(7 AS SIGNED)            WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 2 THEN CAST(DATA_LENGTH AS SIGNED)            WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 1 AND F.COLLATION_TYPE IN (45, 46, 224, 54, 55, 101) THEN CAST(DATA_LENGTH * 4 AS SIGNED)            WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 1 AND F.COLLATION_TYPE IN (28, 87) THEN CAST(DATA_LENGTH * 2 AS SIGNED)            WHEN DATA_TYPE = 36 THEN CAST(12 AS SIGNED)            WHEN DATA_TYPE IN (37, 38) THEN CAST(11 AS SIGNED)            WHEN DATA_TYPE = 39 THEN CAST(DATA_LENGTH AS SIGNED)            WHEN DATA_TYPE = 40 THEN CAST(5 AS SIGNED)            WHEN DATA_TYPE = 41 THEN CAST(11 AS SIGNED)            ELSE CAST(0 AS SIGNED) END AS COLUMN_LENGTH,        CASE WHEN DATA_TYPE IN (22, 23) THEN CAST(DATA_LENGTH AS SIGNED)            ELSE CAST(0 AS SIGNED) END AS CHAR_LENGTH,        CAST('ASC' AS CHAR(4)) AS DESCEND,       CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID        FROM         (SELECT             A.TENANT_ID,             DATABASE_NAME AS INDEX_OWNER,             CASE WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME !=  '__recyclebin')                  THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))                  WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME =  '__recyclebin')                  THEN TABLE_NAME                  WHEN (TABLE_TYPE = 3 AND CONS_TAB.CONSTRAINT_NAME IS NULL)                  THEN CONCAT('t_pk_obpk_', A.TABLE_ID)                  ELSE (CONS_TAB.CONSTRAINT_NAME) END AS INDEX_NAME,             DATABASE_NAME AS TABLE_OWNER,             CASE WHEN (TABLE_TYPE = 3) THEN A.TABLE_ID                  ELSE A.DATA_TABLE_ID END AS TABLE_ID,             A.TABLE_ID AS INDEX_ID,             TABLE_TYPE AS IDX_TYPE           FROM             OCEANBASE.__ALL_VIRTUAL_TABLE A             JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE B             ON A.DATABASE_ID = B.DATABASE_ID                AND A.TENANT_ID = B.TENANT_ID AND A.TENANT_ID = B.TENANT_ID              LEFT JOIN OCEANBASE.__ALL_VIRTUAL_CONSTRAINT CONS_TAB             ON CONS_TAB.TABLE_ID = A.TABLE_ID                AND A.TENANT_ID = CONS_TAB.TENANT_ID                AND CONS_TAB.CONSTRAINT_TYPE = 1            WHERE             (A.TABLE_TYPE = 3 AND A.TABLE_MODE & 66048 = 0) OR (A.TABLE_TYPE = 5)         ) E         JOIN OCEANBASE.__ALL_VIRTUAL_TABLE D           ON E.TENANT_ID = D.TENANT_ID              AND E.TABLE_ID = D.TABLE_ID          JOIN OCEANBASE.__ALL_VIRTUAL_COLUMN F           ON E.INDEX_ID = F.TABLE_ID              AND F.TENANT_ID = E.TENANT_ID       WHERE         F.ROWKEY_POSITION != 0         AND (CASE WHEN IDX_TYPE = 5 THEN INDEX_POSITION ELSE 1 END) != 0 )__"))) {
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

int ObInnerTableSchema::cdb_part_tables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_PART_TABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_PART_TABLES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(DB.TENANT_ID AS SIGNED) CON_ID,          CAST(DB.DATABASE_NAME AS CHAR(128)) OWNER,          CAST(TB.TABLE_NAME AS CHAR(128)) TABLE_NAME,          CAST((CASE TB.PART_FUNC_TYPE               WHEN 0 THEN 'HASH'               WHEN 1 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'HASH' ELSE 'KEY' END)               WHEN 2 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'HASH' ELSE 'KEY' END)               WHEN 3 THEN 'RANGE'               WHEN 4 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'RANGE' ELSE 'RANGE COLUMNS' END)               WHEN 5 THEN 'LIST'               WHEN 6 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'LIST' ELSE 'LIST COLUMNS' END)               WHEN 7 THEN 'RANGE' END)               AS CHAR(13)) PARTITIONING_TYPE,          CAST((CASE TB.PART_LEVEL                WHEN 1 THEN 'NONE'                WHEN 2 THEN                (CASE TB.SUB_PART_FUNC_TYPE                 WHEN 0 THEN 'HASH'                 WHEN 1 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'HASH' ELSE 'KEY' END)                 WHEN 2 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'HASH' ELSE 'KEY' END)                 WHEN 3 THEN 'RANGE'                 WHEN 4 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'RANGE' ELSE 'RANGE COLUMNS' END)                 WHEN 5 THEN 'LIST'                 WHEN 6 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'LIST' ELSE 'LIST COLUMNS' END)                 WHEN 7 THEN 'RANGE' END) END)               AS CHAR(13)) SUBPARTITIONING_TYPE,          CAST((CASE TB.PART_FUNC_TYPE                WHEN 7 THEN 1048575                ELSE TB.PART_NUM END) AS SIGNED) PARTITION_COUNT,          CAST ((CASE TB.PART_LEVEL                 WHEN 1 THEN 0                 WHEN 2 THEN (CASE WHEN TB.SUB_PART_TEMPLATE_FLAGS > 0 THEN TB.SUB_PART_NUM ELSE 1 END)                 END) AS SIGNED) DEF_SUBPARTITION_COUNT,          CAST(PART_INFO.PART_KEY_COUNT AS SIGNED) PARTITIONING_KEY_COUNT,          CAST((CASE TB.PART_LEVEL               WHEN 1 THEN 0               WHEN 2 THEN PART_INFO.SUBPART_KEY_COUNT END)               AS SIGNED) SUBPARTITIONING_KEY_COUNT,          CAST(NULL AS CHAR(8)) STATUS,          CAST(TP.TABLESPACE_NAME AS CHAR(30)) DEF_TABLESPACE_NAME,          CAST(NULL AS SIGNED) DEF_PCT_FREE,          CAST(NULL AS SIGNED) DEF_PCT_USED,          CAST(NULL AS SIGNED) DEF_INI_TRANS,          CAST(NULL AS SIGNED) DEF_MAX_TRANS,          CAST(NULL AS CHAR(40)) DEF_INITIAL_EXTENT,          CAST(NULL AS CHAR(40)) DEF_NEXT_EXTENT,          CAST(NULL AS CHAR(40)) DEF_MIN_EXTENTS,          CAST(NULL AS CHAR(40)) DEF_MAX_EXTENTS,          CAST(NULL AS CHAR(40)) DEF_MAX_SIZE,          CAST(NULL AS CHAR(40)) DEF_PCT_INCREASE,          CAST(NULL AS SIGNED) DEF_FREELISTS,          CAST(NULL AS SIGNED) DEF_FREELIST_GROUPS,          CAST(NULL AS CHAR(7)) DEF_LOGGING,          CAST(CASE WHEN TB.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED'               ELSE 'ENABLED' END AS CHAR(8)) DEF_COMPRESSION,          CAST(TB.COMPRESS_FUNC_NAME AS CHAR(12)) DEF_COMPRESS_FOR,          CAST(NULL AS CHAR(7)) DEF_BUFFER_POOL,          CAST(NULL AS CHAR(7)) DEF_FLASH_CACHE,          CAST(NULL AS CHAR(7)) DEF_CELL_FLASH_CACHE,          CAST(NULL AS CHAR(128)) REF_PTN_CONSTRAINT_NAME,          CAST(TB.INTERVAL_RANGE AS CHAR(1000)) "INTERVAL",          CAST('NO' AS CHAR(3)) AUTOLIST,          CAST(NULL AS CHAR(1000)) INTERVAL_SUBPARTITION,          CAST('NO' AS CHAR(3)) AUTOLIST_SUBPARTITION,          CAST(NULL AS CHAR(3)) IS_NESTED,          CAST(NULL AS CHAR(4)) DEF_SEGMENT_CREATED,          CAST(NULL AS CHAR(3)) DEF_INDEXING,          CAST(NULL AS CHAR(8)) DEF_INMEMORY,          CAST(NULL AS CHAR(8)) DEF_INMEMORY_PRIORITY,          CAST(NULL AS CHAR(15)) DEF_INMEMORY_DISTRIBUTE,          CAST(NULL AS CHAR(17)) DEF_INMEMORY_COMPRESSION,          CAST(NULL AS CHAR(13)) DEF_INMEMORY_DUPLICATE,          CAST(NULL AS CHAR(3)) DEF_READ_ONLY,          CAST(NULL AS CHAR(24)) DEF_CELLMEMORY,          CAST(NULL AS CHAR(12)) DEF_INMEMORY_SERVICE,          CAST(NULL AS CHAR(1000)) DEF_INMEMORY_SERVICE_NAME,          CAST('NO' AS CHAR(3)) AUTO       FROM OCEANBASE.__ALL_VIRTUAL_TABLE TB       JOIN OCEANBASE.__ALL_TENANT T       ON TB.TENANT_ID = T.TENANT_ID       JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE DB       ON TB.TENANT_ID = DB.TENANT_ID AND TB.DATABASE_ID = DB.DATABASE_ID       JOIN         (SELECT          TENANT_ID,          TABLE_ID,          SUM(CASE WHEN (PARTITION_KEY_POSITION & 255) > 0 THEN 1 ELSE 0 END) AS PART_KEY_COUNT,          SUM(CASE WHEN (PARTITION_KEY_POSITION & 65280) > 0 THEN 1 ELSE 0 END) AS SUBPART_KEY_COUNT          FROM OCEANBASE.__ALL_VIRTUAL_COLUMN          WHERE PARTITION_KEY_POSITION > 0          GROUP BY TENANT_ID, TABLE_ID) PART_INFO       ON TB.TENANT_ID = PART_INFO.TENANT_ID AND TB.TABLE_ID = PART_INFO.TABLE_ID       LEFT JOIN OCEANBASE.__ALL_VIRTUAL_TENANT_TABLESPACE TP       ON TB.TENANT_ID = TP.TENANT_ID AND TP.TABLESPACE_ID = TB.TABLESPACE_ID       WHERE TB.TABLE_TYPE IN (3, 6, 8, 9)         AND TB.PART_LEVEL != 0   )__"))) {
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

int ObInnerTableSchema::cdb_tab_partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_TAB_PARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_TAB_PARTITIONS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CAST(DB_TB.TENANT_ID AS SIGNED) CON_ID,       CAST(DB_TB.DATABASE_NAME AS CHAR(128)) TABLE_OWNER,       CAST(DB_TB.TABLE_NAME AS CHAR(128)) TABLE_NAME,        CAST(CASE DB_TB.PART_LEVEL            WHEN 2 THEN 'YES'            ELSE 'NO' END AS CHAR(3)) COMPOSITE,        CAST(PART.PART_NAME AS CHAR(128)) PARTITION_NAME,        CAST(CASE DB_TB.PART_LEVEL            WHEN 2 THEN PART.SUB_PART_NUM            ELSE 0 END AS SIGNED)  SUBPARTITION_COUNT,        CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL            ELSE PART.LIST_VAL END AS CHAR(262144)) HIGH_VALUE,        CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)            ELSE length(PART.LIST_VAL) END AS SIGNED) HIGH_VALUE_LENGTH,        CAST(PART.PARTITION_POSITION AS SIGNED) PARTITION_POSITION,       CAST(TP.TABLESPACE_NAME AS CHAR(30)) TABLESPACE_NAME,       CAST(NULL AS SIGNED) PCT_FREE,       CAST(NULL AS SIGNED) PCT_USED,       CAST(NULL AS SIGNED) INI_TRANS,       CAST(NULL AS SIGNED) MAX_TRANS,       CAST(NULL AS SIGNED) INITIAL_EXTENT,       CAST(NULL AS SIGNED) NEXT_EXTENT,       CAST(NULL AS SIGNED) MIN_EXTENT,       CAST(NULL AS SIGNED) MAX_EXTENT,       CAST(NULL AS SIGNED) MAX_SIZE,       CAST(NULL AS SIGNED) PCT_INCREASE,       CAST(NULL AS SIGNED) FREELISTS,       CAST(NULL AS SIGNED) FREELIST_GROUPS,       CAST(NULL AS CHAR(7)) LOGGING,        CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED'            ELSE 'ENABLED' END AS CHAR(8)) COMPRESSION,        CAST(PART.COMPRESS_FUNC_NAME AS CHAR(30)) COMPRESS_FOR,       CAST(NULL AS SIGNED) NUM_ROWS,       CAST(NULL AS SIGNED) BLOCKS,       CAST(NULL AS SIGNED) EMPTY_BLOCKS,       CAST(NULL AS SIGNED) AVG_SPACE,       CAST(NULL AS SIGNED) CHAIN_CNT,       CAST(NULL AS SIGNED) AVG_ROW_LEN,       CAST(NULL AS SIGNED) SAMPLE_SIZE,       CAST(NULL AS DATE) LAST_ANALYZED,       CAST(NULL AS CHAR(7)) BUFFER_POOL,       CAST(NULL AS CHAR(7)) FLASH_CACHE,       CAST(NULL AS CHAR(7)) CELL_FLASH_CACHE,       CAST(NULL AS CHAR(3)) GLOBAL_STATS,       CAST(NULL AS CHAR(3)) USER_STATS,       CAST(NULL AS CHAR(3)) IS_NESTED,       CAST(NULL AS CHAR(128)) PARENT_TABLE_PARTITION,        CAST (CASE WHEN PART.PARTITION_POSITION >             MAX (CASE WHEN PART.HIGH_BOUND_VAL = DB_TB.B_TRANSITION_POINT                  THEN PART.PARTITION_POSITION ELSE NULL END)             OVER(PARTITION BY DB_TB.TABLE_ID)             THEN 'YES' ELSE 'NO' END AS CHAR(3)) "INTERVAL",        CAST(NULL AS CHAR(4)) SEGMENT_CREATED,       CAST(NULL AS CHAR(4)) INDEXING,       CAST(NULL AS CHAR(4)) READ_ONLY,       CAST(NULL AS CHAR(8)) INMEMORY,       CAST(NULL AS CHAR(8)) INMEMORY_PRIORITY,       CAST(NULL AS CHAR(15)) INMEMORY_DISTRIBUTE,       CAST(NULL AS CHAR(17)) INMEMORY_COMPRESSION,       CAST(NULL AS CHAR(13)) INMEMORY_DUPLICATE,       CAST(NULL AS CHAR(24)) CELLMEMORY,       CAST(NULL AS CHAR(12)) INMEMORY_SERVICE,       CAST(NULL AS CHAR(100)) INMEMORY_SERVICE_NAME,       CAST(NULL AS CHAR(8)) MEMOPTIMIZE_READ,       CAST(NULL AS CHAR(8)) MEMOPTIMIZE_WRITE        FROM (SELECT DB.TENANT_ID,                    DB.DATABASE_NAME,                    DB.DATABASE_ID,                    TB.TABLE_ID,                    TB.TABLE_NAME,                    TB.B_TRANSITION_POINT,                    TB.PART_LEVEL             FROM OCEANBASE.__ALL_VIRTUAL_TABLE TB,                  OCEANBASE.__ALL_VIRTUAL_DATABASE DB             WHERE TB.DATABASE_ID = DB.DATABASE_ID               AND TB.TENANT_ID = DB.TENANT_ID               AND TB.TABLE_TYPE IN (3, 6, 8, 9)            ) DB_TB       JOIN (SELECT TENANT_ID,                    TABLE_ID,                    PART_NAME,                    SUB_PART_NUM,                    HIGH_BOUND_VAL,                    LIST_VAL,                    COMPRESS_FUNC_NAME,                    TABLESPACE_ID,                    ROW_NUMBER() OVER (                      PARTITION BY TENANT_ID, TABLE_ID                      ORDER BY PART_IDX, PART_ID ASC                    ) PARTITION_POSITION             FROM OCEANBASE.__ALL_VIRTUAL_PART) PART       ON DB_TB.TABLE_ID = PART.TABLE_ID AND PART.TENANT_ID = DB_TB.TENANT_ID        LEFT JOIN OCEANBASE.__ALL_VIRTUAL_TENANT_TABLESPACE TP       ON TP.TABLESPACE_ID = PART.TABLESPACE_ID AND TP.TENANT_ID = PART.TENANT_ID  )__"))) {
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

int ObInnerTableSchema::cdb_tab_subpartitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_TAB_SUBPARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_TAB_SUBPARTITIONS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CAST(DB_TB.TENANT_ID AS SIGNED) CON_ID,       CAST(DB_TB.DATABASE_NAME AS CHAR(128)) TABLE_OWNER,       CAST(DB_TB.TABLE_NAME AS CHAR(128)) TABLE_NAME,       CAST(PART.PART_NAME AS CHAR(128)) PARTITION_NAME,       CAST(PART.SUB_PART_NAME AS CHAR(128))  SUBPARTITION_NAME,       CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL            ELSE PART.LIST_VAL END AS CHAR(262144)) HIGH_VALUE,       CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)            ELSE length(PART.LIST_VAL) END AS SIGNED) HIGH_VALUE_LENGTH,       CAST(PART.PARTITION_POSITION AS SIGNED) PARTITION_POSITION,       CAST(PART.SUBPARTITION_POSITION AS SIGNED) SUBPARTITION_POSITION,       CAST(TP.TABLESPACE_NAME AS CHAR(30)) TABLESPACE_NAME,       CAST(NULL AS SIGNED) PCT_FREE,       CAST(NULL AS SIGNED) PCT_USED,       CAST(NULL AS SIGNED) INI_TRANS,       CAST(NULL AS SIGNED) MAX_TRANS,       CAST(NULL AS SIGNED) INITIAL_EXTENT,       CAST(NULL AS SIGNED) NEXT_EXTENT,       CAST(NULL AS SIGNED) MIN_EXTENT,       CAST(NULL AS SIGNED) MAX_EXTENT,       CAST(NULL AS SIGNED) MAX_SIZE,       CAST(NULL AS SIGNED) PCT_INCREASE,       CAST(NULL AS SIGNED) FREELISTS,       CAST(NULL AS SIGNED) FREELIST_GROUPS,       CAST(NULL AS CHAR(3)) LOGGING,       CAST(CASE WHEN       PART.COMPRESS_FUNC_NAME IS NULL THEN       'DISABLED' ELSE 'ENABLED' END AS CHAR(8)) COMPRESSION,       CAST(PART.COMPRESS_FUNC_NAME AS CHAR(30)) COMPRESS_FOR,       CAST(NULL AS SIGNED) NUM_ROWS,       CAST(NULL AS SIGNED) BLOCKS,       CAST(NULL AS SIGNED) EMPTY_BLOCKS,       CAST(NULL AS SIGNED) AVG_SPACE,       CAST(NULL AS SIGNED) CHAIN_CNT,       CAST(NULL AS SIGNED) AVG_ROW_LEN,       CAST(NULL AS SIGNED) SAMPLE_SIZE,       CAST(NULL AS DATE) LAST_ANALYZED,       CAST(NULL AS CHAR(7)) BUFFER_POOL,       CAST(NULL AS CHAR(7)) FLASH_CACHE,       CAST(NULL AS CHAR(7)) CELL_FLASH_CACHE,       CAST(NULL AS CHAR(3)) GLOBAL_STATS,       CAST(NULL AS CHAR(3)) USER_STATS,       CAST('NO' AS CHAR(3)) "INTERVAL",       CAST(NULL AS CHAR(3)) SEGMENT_CREATED,       CAST(NULL AS CHAR(3)) INDEXING,       CAST(NULL AS CHAR(3)) READ_ONLY,       CAST(NULL AS CHAR(8)) INMEMORY,       CAST(NULL AS CHAR(8)) INMEMORY_PRIORITY,       CAST(NULL AS CHAR(15)) INMEMORY_DISTRIBUTE,       CAST(NULL AS CHAR(17)) INMEMORY_COMPRESSION,       CAST(NULL AS CHAR(13)) INMEMORY_DUPLICATE,       CAST(NULL AS CHAR(12)) INMEMORY_SERVICE,       CAST(NULL AS CHAR(1000)) INMEMORY_SERVICE_NAME,       CAST(NULL AS CHAR(24)) CELLMEMORY,       CAST(NULL AS CHAR(8)) MEMOPTIMIZE_READ,       CAST(NULL AS CHAR(8)) MEMOPTIMIZE_WRITE       FROM       (SELECT DB.TENANT_ID,               DB.DATABASE_NAME,               DB.DATABASE_ID,               TB.TABLE_ID,               TB.TABLE_NAME        FROM  OCEANBASE.__ALL_VIRTUAL_TABLE TB,              OCEANBASE.__ALL_VIRTUAL_DATABASE DB        WHERE TB.DATABASE_ID = DB.DATABASE_ID          AND TB.TENANT_ID = DB.TENANT_ID          AND TB.TABLE_TYPE IN (3, 6, 8, 9)) DB_TB       JOIN       (SELECT P_PART.TENANT_ID,               P_PART.TABLE_ID,               P_PART.PART_NAME,               P_PART.PARTITION_POSITION,               S_PART.SUB_PART_NAME,               S_PART.HIGH_BOUND_VAL,               S_PART.LIST_VAL,               S_PART.COMPRESS_FUNC_NAME,               S_PART.TABLESPACE_ID,               S_PART.SUBPARTITION_POSITION        FROM (SELECT                TENANT_ID,                TABLE_ID,                PART_ID,                PART_NAME,                ROW_NUMBER() OVER (                  PARTITION BY TENANT_ID, TABLE_ID                  ORDER BY PART_IDX, PART_ID ASC                ) AS PARTITION_POSITION              FROM OCEANBASE.__ALL_VIRTUAL_PART) P_PART,             (SELECT                TENANT_ID,                TABLE_ID,                PART_ID,                SUB_PART_NAME,                HIGH_BOUND_VAL,                LIST_VAL,                COMPRESS_FUNC_NAME,                TABLESPACE_ID,                ROW_NUMBER() OVER (                  PARTITION BY TENANT_ID, TABLE_ID, PART_ID                  ORDER BY SUB_PART_IDX, SUB_PART_ID ASC                ) AS SUBPARTITION_POSITION              FROM OCEANBASE.__ALL_VIRTUAL_SUB_PART) S_PART        WHERE P_PART.PART_ID = S_PART.PART_ID              AND P_PART.TABLE_ID = S_PART.TABLE_ID              AND P_PART.TENANT_ID = S_PART.TENANT_ID) PART       ON DB_TB.TABLE_ID = PART.TABLE_ID AND DB_TB.TENANT_ID = PART.TENANT_ID        LEFT JOIN         OCEANBASE.__ALL_VIRTUAL_TENANT_TABLESPACE TP       ON TP.TABLESPACE_ID = PART.TABLESPACE_ID AND TP.TENANT_ID = PART.TENANT_ID  )__"))) {
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

int ObInnerTableSchema::cdb_subpartition_templates_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_SUBPARTITION_TEMPLATES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_SUBPARTITION_TEMPLATES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CAST(DB.TENANT_ID AS NUMBER) CON_ID,       CAST(DB.DATABASE_NAME AS CHAR(128)) USER_NAME,       CAST(TB.TABLE_NAME AS CHAR(128)) TABLE_NAME,       CAST(SP.SUB_PART_NAME AS CHAR(132)) SUBPARTITION_NAME,       CAST(SP.SUB_PART_ID + 1 AS SIGNED) SUBPARTITION_POSITION,       CAST(TP.TABLESPACE_NAME AS CHAR(30)) TABLESPACE_NAME,       CAST(CASE WHEN SP.HIGH_BOUND_VAL IS NULL THEN SP.LIST_VAL            ELSE SP.HIGH_BOUND_VAL END AS CHAR(262144)) HIGH_BOUND,       CAST(NULL AS CHAR(4)) COMPRESSION,       CAST(NULL AS CHAR(4)) INDEXING,       CAST(NULL AS CHAR(4)) READ_ONLY        FROM OCEANBASE.__ALL_VIRTUAL_DATABASE DB        JOIN OCEANBASE.__ALL_VIRTUAL_TABLE TB       ON DB.DATABASE_ID = TB.DATABASE_ID AND DB.TENANT_ID = TB.TENANT_ID          AND TB.TABLE_TYPE IN (3, 6, 8, 9)        JOIN OCEANBASE.__ALL_VIRTUAL_DEF_SUB_PART SP       ON TB.TABLE_ID = SP.TABLE_ID AND SP.TENANT_ID = TB.TENANT_ID        LEFT JOIN OCEANBASE.__ALL_VIRTUAL_TENANT_TABLESPACE TP       ON TP.TABLESPACE_ID = SP.TABLESPACE_ID AND TP.TENANT_ID = SP.TENANT_ID       )__"))) {
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

int ObInnerTableSchema::cdb_part_key_columns_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_PART_KEY_COLUMNS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_PART_KEY_COLUMNS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT  CAST(D.TENANT_ID AS SIGNED) AS CON_ID,             CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(T.TABLE_NAME AS CHAR(128)) AS NAME,             CAST('TABLE' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 255) AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_VIRTUAL_COLUMN C, OCEANBASE.__ALL_VIRTUAL_TABLE T, OCEANBASE.__ALL_VIRTUAL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND (C.PARTITION_KEY_POSITION & 255) > 0           AND T.TABLE_TYPE IN (3, 6, 8, 9)     UNION     SELECT  CAST(D.TENANT_ID AS SIGNED) AS CON_ID,             CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7))) END AS CHAR(128)) AS NAME,             CAST('INDEX' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 255) AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_VIRTUAL_COLUMN C, OCEANBASE.__ALL_VIRTUAL_TABLE T, OCEANBASE.__ALL_VIRTUAL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND (C.PARTITION_KEY_POSITION & 255) > 0     UNION     SELECT  CAST(D.TENANT_ID AS SIGNED) AS CON_ID,             CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7))) END AS CHAR(128)) AS NAME,             CAST('INDEX' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 255) AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_VIRTUAL_COLUMN C, OCEANBASE.__ALL_VIRTUAL_TABLE T, OCEANBASE.__ALL_VIRTUAL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2,10)           AND (C.PARTITION_KEY_POSITION & 255) > 0 )__"))) {
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

int ObInnerTableSchema::cdb_subpart_key_columns_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_SUBPART_KEY_COLUMNS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_SUBPART_KEY_COLUMNS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT  CAST(D.TENANT_ID AS SIGNED) AS CON_ID,             CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(T.TABLE_NAME AS CHAR(128)) AS NAME,             CAST('TABLE' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 65280)/256 AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_VIRTUAL_COLUMN C, OCEANBASE.__ALL_VIRTUAL_TABLE T, OCEANBASE.__ALL_VIRTUAL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND (C.PARTITION_KEY_POSITION & 65280) > 0           AND T.TABLE_TYPE IN (3, 6, 8, 9)     UNION     SELECT  CAST(D.TENANT_ID AS SIGNED) AS CON_ID,             CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7))) END AS CHAR(128)) AS NAME,             CAST('INDEX' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 65280)/256 AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_VIRTUAL_COLUMN C, OCEANBASE.__ALL_VIRTUAL_TABLE T, OCEANBASE.__ALL_VIRTUAL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND (C.PARTITION_KEY_POSITION & 65280) > 0     UNION     SELECT  CAST(D.TENANT_ID AS SIGNED) AS CON_ID,             CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7))) END AS CHAR(128)) AS NAME,             CAST('INDEX' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 65280)/256 AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_VIRTUAL_COLUMN C, OCEANBASE.__ALL_VIRTUAL_TABLE T, OCEANBASE.__ALL_VIRTUAL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2,10)           AND (C.PARTITION_KEY_POSITION & 65280) > 0 )__"))) {
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
