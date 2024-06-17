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

int ObInnerTableSchema::dba_ob_sys_variables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SYS_VARIABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SYS_VARIABLES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     a.GMT_CREATE AS CREATE_TIME,     a.GMT_MODIFIED AS MODIFY_TIME,     a.NAME as NAME,     a.VALUE as VALUE,     a.MIN_VAL as MIN_VALUE,     a.MAX_VAL as MAX_VALUE,     CASE a.FLAGS & 0x3         WHEN 1 THEN "GLOBAL_ONLY"         WHEN 2 THEN "SESSION_ONLY"         WHEN 3 THEN "GLOBAL | SESSION"         ELSE NULL     END as SCOPE,     a.INFO as INFO,     b.DEFAULT_VALUE as DEFAULT_VALUE,     CAST (CASE WHEN a.VALUE = b.DEFAULT_VALUE           THEN 'YES'           ELSE 'NO'           END AS CHAR(3)) AS ISDEFAULT   FROM oceanbase.__all_sys_variable a   join oceanbase.__all_virtual_sys_variable_default_value b   where a.name = b.variable_name;   )__"))) {
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

int ObInnerTableSchema::dba_ob_transfer_partition_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TRANSFER_PARTITION_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TRANSFER_PARTITION_TASKS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TASK_ID,          GMT_CREATE AS CREATE_TIME,          GMT_MODIFIED AS MODIFY_TIME,          TABLE_ID,          OBJECT_ID,          DEST_LS,          BALANCE_JOB_ID,          TRANSFER_TASK_ID,          STATUS,          COMMENT   FROM oceanbase.__all_transfer_partition_task   )__"))) {
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

int ObInnerTableSchema::cdb_ob_transfer_partition_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TRANSFER_PARTITION_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TRANSFER_PARTITION_TASKS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          TASK_ID,          GMT_CREATE AS CREATE_TIME,          GMT_MODIFIED AS MODIFY_TIME,          TABLE_ID,          OBJECT_ID,          DEST_LS,          BALANCE_JOB_ID,          TRANSFER_TASK_ID,          STATUS,          COMMENT   FROM oceanbase.__all_virtual_transfer_partition_task   )__"))) {
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

int ObInnerTableSchema::dba_ob_transfer_partition_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TASK_ID,          CREATE_TIME,          FINISH_TIME,          TABLE_ID,          OBJECT_ID,          DEST_LS,          BALANCE_JOB_ID,          TRANSFER_TASK_ID,          STATUS,          COMMENT   FROM oceanbase.__all_transfer_partition_task_history   )__"))) {
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

int ObInnerTableSchema::cdb_ob_transfer_partition_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TRANSFER_PARTITION_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TRANSFER_PARTITION_TASK_HISTORY_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          TASK_ID,          CREATE_TIME,          FINISH_TIME,          TABLE_ID,          OBJECT_ID,          DEST_LS,          BALANCE_JOB_ID,          TRANSFER_TASK_ID,          STATUS,          COMMENT   FROM oceanbase.__all_virtual_transfer_partition_task_history   )__"))) {
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

int ObInnerTableSchema::dba_ob_trusted_root_certificate_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TRUSTED_ROOT_CERTIFICATE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TRUSTED_ROOT_CERTIFICATE_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT    common_name AS COMMON_NAME,   description AS DESCRIPTION,   EXTRACT_CERT_EXPIRED_TIME(content) AS CERT_EXPIRED_TIME   FROM     oceanbase.__all_trusted_root_certificate   )__"))) {
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

int ObInnerTableSchema::dba_ob_clone_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_CLONE_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_CLONE_PROGRESS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT job_id AS CLONE_JOB_ID,        trace_id AS TRACE_ID,        source_tenant_id AS SOURCE_TENANT_ID,        source_tenant_name AS SOURCE_TENANT_NAME,        clone_tenant_id AS CLONE_TENANT_ID,        clone_tenant_name AS CLONE_TENANT_NAME,        tenant_snapshot_id AS TENANT_SNAPSHOT_ID,        tenant_snapshot_name AS TENANT_SNAPSHOT_NAME,        resource_pool_id AS RESOURCE_POOL_ID,        resource_pool_name AS RESOURCE_POOL_NAME,        unit_config_name AS UNIT_CONFIG_NAME,        restore_scn AS RESTORE_SCN,        status AS STATUS,        job_type AS CLONE_JOB_TYPE,        clone_start_time AS CLONE_START_TIME,        clone_finished_time AS CLONE_FINISHED_TIME,        ret_code AS RET_CODE,        error_msg AS ERROR_MESSAGE FROM oceanbase.__all_clone_job ORDER BY CLONE_START_TIME )__"))) {
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

int ObInnerTableSchema::role_edges_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_MYSQL_SCHEMA_ID);
  table_schema.set_table_id(OB_ROLE_EDGES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ROLE_EDGES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT cast(from_user.host AS char(255)) FROM_HOST,          cast(from_user.user_name AS char(128)) FROM_USER,          cast(to_user.host AS char(255)) TO_HOST,          cast(to_user.user_name AS char(128)) TO_USER,          cast(CASE role_map.admin_option WHEN 1 THEN 'Y' ELSE 'N' END AS char(1)) WITH_ADMIN_OPTION   FROM oceanbase.__all_tenant_role_grantee_map role_map,        oceanbase.__all_user from_user,        oceanbase.__all_user to_user   WHERE role_map.tenant_id = from_user.tenant_id     AND role_map.tenant_id = to_user.tenant_id     AND role_map.grantee_id = to_user.user_id     AND role_map.role_id = from_user.user_id; )__"))) {
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

int ObInnerTableSchema::default_roles_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_MYSQL_SCHEMA_ID);
  table_schema.set_table_id(OB_DEFAULT_ROLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DEFAULT_ROLES_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT cast(to_user.host AS char(255)) HOST,          cast(to_user.user_name AS char(128)) USER,          cast(from_user.host AS char(255)) DEFAULT_ROLE_HOST,          cast(from_user.user_name AS char(128)) DEFAULT_ROLE_USER   FROM oceanbase.__all_tenant_role_grantee_map role_map,        oceanbase.__all_user from_user,        oceanbase.__all_user to_user   WHERE role_map.tenant_id = from_user.tenant_id     AND role_map.tenant_id = to_user.tenant_id     AND role_map.grantee_id = to_user.user_id     AND role_map.role_id = from_user.user_id     AND role_map.disable_flag = 0; )__"))) {
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

int ObInnerTableSchema::cdb_index_usage_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_INDEX_USAGE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_INDEX_USAGE_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       IUT.TENANT_ID AS CON_ID,       CAST(IUT.OBJECT_ID AS SIGNED) AS OBJECT_ID,       CAST(T.TABLE_NAME AS CHAR(128)) AS NAME,       CAST(DB.DATABASE_NAME AS CHAR(128)) AS OWNER,       CAST(IUT.TOTAL_ACCESS_COUNT AS SIGNED) AS TOTAL_ACCESS_COUNT,       CAST(IUT.TOTAL_EXEC_COUNT AS SIGNED) AS TOTAL_EXEC_COUNT,       CAST(IUT.TOTAL_ROWS_RETURNED AS SIGNED) AS TOTAL_ROWS_RETURNED,       CAST(IUT.BUCKET_0_ACCESS_COUNT AS SIGNED) AS BUCKET_0_ACCESS_COUNT,       CAST(IUT.BUCKET_1_ACCESS_COUNT AS SIGNED) AS BUCKET_1_ACCESS_COUNT,       CAST(IUT.BUCKET_2_10_ACCESS_COUNT AS SIGNED) AS BUCKET_2_10_ACCESS_COUNT,       CAST(IUT.BUCKET_2_10_ROWS_RETURNED AS SIGNED) AS BUCKET_2_10_ROWS_RETURNED,       CAST(IUT.BUCKET_11_100_ACCESS_COUNT AS SIGNED) AS BUCKET_11_100_ACCESS_COUNT,       CAST(IUT.BUCKET_11_100_ROWS_RETURNED AS SIGNED) AS BUCKET_11_100_ROWS_RETURNED,       CAST(IUT.BUCKET_101_1000_ACCESS_COUNT AS SIGNED) AS BUCKET_101_1000_ACCESS_COUNT,       CAST(IUT.BUCKET_101_1000_ROWS_RETURNED AS SIGNED) AS BUCKET_101_1000_ROWS_RETURNED,       CAST(IUT.BUCKET_1000_PLUS_ACCESS_COUNT AS SIGNED) AS BUCKET_1000_PLUS_ACCESS_COUNT,       CAST(IUT.BUCKET_1000_PLUS_ROWS_RETURNED AS SIGNED) AS BUCKET_1000_PLUS_ROWS_RETURNED,       CAST(IUT.LAST_USED AS CHAR(128)) AS LAST_USED     FROM       oceanbase.__all_virtual_index_usage_info IUT       JOIN oceanbase.__all_virtual_table T       ON IUT.TENANT_ID = T.TENANT_ID AND IUT.OBJECT_ID = T.TABLE_ID       JOIN oceanbase.__all_virtual_database DB       ON IUT.TENANT_ID = DB.TENANT_ID AND t.DATABASE_ID = DB.DATABASE_ID     WHERE T.TABLE_ID = IUT.OBJECT_ID )__"))) {
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

int ObInnerTableSchema::columns_priv_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_MYSQL_SCHEMA_ID);
  table_schema.set_table_id(OB_COLUMNS_PRIV_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_COLUMNS_PRIV_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT cast(b.host as char(255)) as Host,            cast(a.database_name as char(128)) as Db,            cast(b.user_name as char(128)) as User,            cast(a.table_name as char(128)) as Table_name,            cast(a.column_name as char(128)) as Column_name,            substr(concat(case when (a.all_priv & 1) > 0 then ',Select' else '' end,                           case when (a.all_priv & 2) > 0 then ',Insert' else '' end,                           case when (a.all_priv & 4) > 0 then ',Update' else '' end,                           case when (a.all_priv & 8) > 0 then ',References' else '' end), 2) as Column_priv,            cast(a.gmt_modified as datetime) as Timestamp     FROM oceanbase.__all_column_privilege a, oceanbase.__all_user b     WHERE a.tenant_id = 0 and a.tenant_id = b.tenant_id AND a.user_id = b.user_id )__"))) {
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

int ObInnerTableSchema::gv_ob_ls_snapshots_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_LS_SNAPSHOTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_LS_SNAPSHOTS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       tenant_id AS TENANT_ID,       snapshot_id AS SNAPSHOT_ID,       ls_id AS LS_ID,       svr_ip AS SVR_IP,       svr_port AS SVR_PORT,       (CASE         WHEN meta_existed = 1 THEN 'YES'         ELSE 'NO'         END) AS META_EXISTED,       (CASE         WHEN build_status = 0 THEN 'BUILDING'         WHEN build_status = 1 THEN 'FAILED'         WHEN build_status = 2 THEN 'SUCCESSFUL'         ELSE 'UNKNOWN'         END) AS BUILD_STATUS,       rebuild_seq_start AS REBUILD_SEQ_START,       rebuild_seq_end AS REBUILD_SEQ_END,       end_interval_scn AS END_INTERVAL_SCN,       ls_meta_package AS LS_META_PACKAGE,       (CASE         WHEN tsnap_is_running = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_IS_RUNNING,       (CASE         WHEN tsnap_has_unfinished_create_dag = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_HAS_UNFINISHED_CREATE_DAG,       (CASE         WHEN tsnap_has_unfinished_gc_dag = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_HAS_UNFINISHED_GC_DAG,       tsnap_clone_ref AS TSNAP_CLONE_REF,       (CASE         WHEN tsnap_meta_existed = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_META_EXISTED     FROM oceanbase.__all_virtual_ls_snapshot )__"))) {
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

int ObInnerTableSchema::v_ob_ls_snapshots_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_LS_SNAPSHOTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_LS_SNAPSHOTS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT TENANT_ID,     SNAPSHOT_ID,     LS_ID,     SVR_IP,     SVR_PORT,     META_EXISTED,     BUILD_STATUS,     REBUILD_SEQ_START,     REBUILD_SEQ_END,     END_INTERVAL_SCN,     LS_META_PACKAGE,     TSNAP_IS_RUNNING,     TSNAP_HAS_UNFINISHED_CREATE_DAG,     TSNAP_HAS_UNFINISHED_GC_DAG,     TSNAP_CLONE_REF,     TSNAP_META_EXISTED     FROM oceanbase.GV$OB_LS_SNAPSHOTS     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::dba_ob_clone_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_CLONE_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_CLONE_HISTORY_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT job_id AS CLONE_JOB_ID,        trace_id AS TRACE_ID,        source_tenant_id AS SOURCE_TENANT_ID,        source_tenant_name AS SOURCE_TENANT_NAME,        clone_tenant_id AS CLONE_TENANT_ID,        clone_tenant_name AS CLONE_TENANT_NAME,        tenant_snapshot_id AS TENANT_SNAPSHOT_ID,        tenant_snapshot_name AS TENANT_SNAPSHOT_NAME,        resource_pool_id AS RESOURCE_POOL_ID,        resource_pool_name AS RESOURCE_POOL_NAME,        unit_config_name AS UNIT_CONFIG_NAME,        restore_scn AS RESTORE_SCN,        status AS STATUS,        job_type AS CLONE_JOB_TYPE,        clone_start_time AS CLONE_START_TIME,        clone_finished_time AS CLONE_FINISHED_TIME,        ret_code AS RET_CODE,        error_msg AS ERROR_MESSAGE FROM oceanbase.__all_clone_job_history ORDER BY CLONE_START_TIME )__"))) {
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

int ObInnerTableSchema::cdb_mview_logs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_MVIEW_LOGS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_MVIEW_LOGS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       B.TENANT_ID AS TENANT_ID,       CAST(A.DATABASE_NAME AS CHAR(128)) AS LOG_OWNER,       CAST(D.TABLE_NAME AS CHAR(128)) AS MASTER,       CAST(B.TABLE_NAME AS CHAR(128)) AS LOG_TABLE,       CAST(NULL AS CHAR(128)) AS LOG_TRIGGER,       CAST(IF(D.TABLE_MODE & 66048 = 66048, 'YES', 'NO') AS  CHAR(3)) AS ROWIDS,       CAST(IF(D.TABLE_MODE & 66048 = 0, 'YES', 'NO') AS  CHAR(3)) AS PRIMARY_KEY,       CAST('NO' AS CHAR(3)) AS OBJECT_ID,       CAST(         IF((           SELECT COUNT(*)             FROM oceanbase.__all_virtual_column C1,                  oceanbase.__all_virtual_column C2             WHERE B.TENANT_ID = C1.TENANT_ID               AND B.TABLE_ID = C1.TABLE_ID               AND C1.COLUMN_ID >= 16               AND C1.COLUMN_ID < 65520               AND D.TENANT_ID = C2.TENANT_ID               AND D.TABLE_ID = C2.TABLE_ID               AND C2.ROWKEY_POSITION != 0               AND C1.COLUMN_ID != C2.COLUMN_ID           ) = 0, 'NO', 'YES') AS CHAR(3)       ) AS FILTER_COLUMNS,       CAST('YES' AS CHAR(3)) AS SEQUENCE,       CAST('YES' AS CHAR(3)) AS INCLUDE_NEW_VALUES,       CAST(IF(C.PURGE_MODE = 1, 'YES', 'NO') AS CHAR(3)) AS PURGE_ASYNCHRONOUS,       CAST(IF(C.PURGE_MODE = 2, 'YES', 'NO') AS CHAR(3)) AS PURGE_DEFERRED,       CAST(C.PURGE_START AS DATETIME) AS PURGE_START,       CAST(C.PURGE_NEXT AS CHAR(200)) AS PURGE_INTERVAL,       CAST(C.LAST_PURGE_DATE AS DATETIME) AS LAST_PURGE_DATE,       CAST(0 AS SIGNED) AS LAST_PURGE_STATUS,       C.LAST_PURGE_ROWS AS NUM_ROWS_PURGED,       CAST('YES' AS CHAR(3)) AS COMMIT_SCN_BASED,       CAST('NO' AS CHAR(3)) AS STAGING_LOG     FROM       oceanbase.__all_virtual_database A,       oceanbase.__all_virtual_table B,       oceanbase.__all_virtual_mlog C,       oceanbase.__all_virtual_table D     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND B.TENANT_ID = D.TENANT_ID       AND C.TENANT_ID = D.TENANT_ID       AND B.TABLE_ID = C.MLOG_ID       AND B.TABLE_TYPE = 15       AND B.DATA_TABLE_ID = D.TABLE_ID )__"))) {
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

int ObInnerTableSchema::dba_mview_logs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVIEW_LOGS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVIEW_LOGS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS CHAR(128)) AS LOG_OWNER,       CAST(D.TABLE_NAME AS CHAR(128)) AS MASTER,       CAST(B.TABLE_NAME AS CHAR(128)) AS LOG_TABLE,       CAST(NULL AS CHAR(128)) AS LOG_TRIGGER,       CAST(IF(D.TABLE_MODE & 66048 = 66048, 'YES', 'NO') AS  CHAR(3)) AS ROWIDS,       CAST(IF(D.TABLE_MODE & 66048 = 0, 'YES', 'NO') AS  CHAR(3)) AS PRIMARY_KEY,       CAST('NO' AS CHAR(3)) AS OBJECT_ID,       CAST(         IF((           SELECT COUNT(*)             FROM oceanbase.__all_column C1,                  oceanbase.__all_column C2             WHERE B.TENANT_ID = C1.TENANT_ID               AND B.TABLE_ID = C1.TABLE_ID               AND C1.COLUMN_ID >= 16               AND C1.COLUMN_ID < 65520               AND D.TENANT_ID = C2.TENANT_ID               AND D.TABLE_ID = C2.TABLE_ID               AND C2.ROWKEY_POSITION != 0               AND C1.COLUMN_ID != C2.COLUMN_ID           ) = 0, 'NO', 'YES') AS CHAR(3)       ) AS FILTER_COLUMNS,       CAST('YES' AS CHAR(3)) AS SEQUENCE,       CAST('YES' AS CHAR(3)) AS INCLUDE_NEW_VALUES,       CAST(IF(C.PURGE_MODE = 1, 'YES', 'NO') AS CHAR(3)) AS PURGE_ASYNCHRONOUS,       CAST(IF(C.PURGE_MODE = 2, 'YES', 'NO') AS CHAR(3)) AS PURGE_DEFERRED,       CAST(C.PURGE_START AS DATETIME) AS PURGE_START,       CAST(C.PURGE_NEXT AS CHAR(200)) AS PURGE_INTERVAL,       CAST(C.LAST_PURGE_DATE AS DATETIME) AS LAST_PURGE_DATE,       CAST(0 AS SIGNED) AS LAST_PURGE_STATUS,       C.LAST_PURGE_ROWS AS NUM_ROWS_PURGED,       CAST('YES' AS CHAR(3)) AS COMMIT_SCN_BASED,       CAST('NO' AS CHAR(3)) AS STAGING_LOG     FROM       oceanbase.__all_database A,       oceanbase.__all_table B,       oceanbase.__all_mlog C,       oceanbase.__all_table D     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND B.TENANT_ID = D.TENANT_ID       AND C.TENANT_ID = D.TENANT_ID       AND B.TABLE_ID = C.MLOG_ID       AND B.TABLE_TYPE = 15       AND B.DATA_TABLE_ID = D.TABLE_ID )__"))) {
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

int ObInnerTableSchema::cdb_mviews_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_MVIEWS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_MVIEWS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       B.TENANT_ID AS TENANT_ID,       CAST(A.DATABASE_NAME AS CHAR(128)) AS OWNER,       CAST(B.TABLE_NAME AS CHAR(128)) AS MVIEW_NAME,       CAST(B.TABLE_NAME AS CHAR(128)) AS CONTAINER_NAME,       B.VIEW_DEFINITION AS QUERY,       CAST(LENGTH(B.VIEW_DEFINITION) AS SIGNED) AS QUERY_LEN,       CAST('N' AS CHAR(1)) AS UPDATABLE,       CAST(NULL AS CHAR(128)) AS UPDATE_LOG,       CAST(NULL AS CHAR(128)) AS MASTER_ROLLBACK_SEG,       CAST(NULL AS CHAR(128)) AS MASTER_LINK,       CAST(         CASE ((B.TABLE_MODE >> 27) & 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS REWRITE_ENABLED,       CAST(NULL AS CHAR(9)) AS REWRITE_CAPABILITY,       CAST(         CASE C.REFRESH_MODE           WHEN 0 THEN 'NEVER'           WHEN 1 THEN 'DEMAND'           WHEN 2 THEN 'COMMIT'           WHEN 3 THEN 'STATEMENT'           ELSE NULL         END AS CHAR(6)       ) AS REFRESH_MODE,       CAST(         CASE C.REFRESH_METHOD           WHEN 0 THEN 'NEVER'           WHEN 1 THEN 'COMPLETE'           WHEN 2 THEN 'FAST'           WHEN 3 THEN 'FORCE'           ELSE NULL         END AS CHAR(8)       ) AS REFRESH_METHOD,       CAST(         CASE C.BUILD_MODE           WHEN 0 THEN 'IMMEDIATE'           WHEN 1 THEN 'DEFERRED'           WHEN 2 THEN 'PERBUILT'           ELSE NULL         END AS CHAR(9)       ) AS BUILD_MODE,       CAST(NULL AS CHAR(18)) AS FAST_REFRESHABLE,       CAST(         CASE C.LAST_REFRESH_TYPE           WHEN 0 THEN 'COMPLETE'           WHEN 1 THEN 'FAST'           ELSE 'NA'         END AS CHAR(8)       ) AS LAST_REFRESH_TYPE,       CAST(C.LAST_REFRESH_DATE AS DATETIME) AS LAST_REFRESH_DATE,       CAST(DATE_ADD(C.LAST_REFRESH_DATE, INTERVAL C.LAST_REFRESH_TIME SECOND) AS DATETIME) AS LAST_REFRESH_END_TIME,       CAST(NULL AS CHAR(19)) AS STALENESS,       CAST(NULL AS CHAR(19)) AS AFTER_FAST_REFRESH,       CAST(IF(C.BUILD_MODE = 2, 'Y', 'N') AS CHAR(1)) AS UNKNOWN_PREBUILT,       CAST('N' AS CHAR(1)) AS UNKNOWN_PLSQL_FUNC,       CAST('N' AS CHAR(1)) AS UNKNOWN_EXTERNAL_TABLE,       CAST('N' AS CHAR(1)) AS UNKNOWN_CONSIDER_FRESH,       CAST('N' AS CHAR(1)) AS UNKNOWN_IMPORT,       CAST('N' AS CHAR(1)) AS UNKNOWN_TRUSTED_FD,       CAST(NULL AS CHAR(19)) AS COMPILE_STATE,       CAST('Y' AS CHAR(1)) AS USE_NO_INDEX,       CAST(NULL AS DATETIME) AS STALE_SINCE,       CAST(NULL AS SIGNED) AS NUM_PCT_TABLES,       CAST(NULL AS SIGNED) AS NUM_FRESH_PCT_REGIONS,       CAST(NULL AS SIGNED) AS NUM_STALE_PCT_REGIONS,       CAST('NO' AS CHAR(3)) AS SEGMENT_CREATED,       CAST(NULL AS CHAR(128)) AS EVALUATION_EDITION,       CAST(NULL AS CHAR(128)) AS UNUSABLE_BEFORE,       CAST(NULL AS CHAR(128)) AS UNUSABLE_BEGINNING,       CAST(NULL AS CHAR(100)) AS DEFAULT_COLLATION,       CAST(         CASE ((B.TABLE_MODE >> 28) & 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS ON_QUERY_COMPUTATION     FROM       oceanbase.__all_virtual_database A,       oceanbase.__all_virtual_table B,       oceanbase.__all_virtual_mview C     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND B.TENANT_ID = C.TENANT_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7 )__"))) {
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

int ObInnerTableSchema::dba_mviews_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVIEWS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVIEWS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS CHAR(128)) AS OWNER,       CAST(B.TABLE_NAME AS CHAR(128)) AS MVIEW_NAME,       CAST(B.TABLE_NAME AS CHAR(128)) AS CONTAINER_NAME,       B.VIEW_DEFINITION AS QUERY,       CAST(LENGTH(B.VIEW_DEFINITION) AS SIGNED) AS QUERY_LEN,       CAST('N' AS CHAR(1)) AS UPDATABLE,       CAST(NULL AS CHAR(128)) AS UPDATE_LOG,       CAST(NULL AS CHAR(128)) AS MASTER_ROLLBACK_SEG,       CAST(NULL AS CHAR(128)) AS MASTER_LINK,       CAST(         CASE ((B.TABLE_MODE >> 27) & 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS REWRITE_ENABLED,       CAST(NULL AS CHAR(9)) AS REWRITE_CAPABILITY,       CAST(         CASE C.REFRESH_MODE           WHEN 0 THEN 'NEVER'           WHEN 1 THEN 'DEMAND'           WHEN 2 THEN 'COMMIT'           WHEN 3 THEN 'STATEMENT'           ELSE NULL         END AS CHAR(6)       ) AS REFRESH_MODE,       CAST(         CASE C.REFRESH_METHOD           WHEN 0 THEN 'NEVER'           WHEN 1 THEN 'COMPLETE'           WHEN 2 THEN 'FAST'           WHEN 3 THEN 'FORCE'           ELSE NULL         END AS CHAR(8)       ) AS REFRESH_METHOD,       CAST(         CASE C.BUILD_MODE           WHEN 0 THEN 'IMMEDIATE'           WHEN 1 THEN 'DEFERRED'           WHEN 2 THEN 'PERBUILT'           ELSE NULL         END AS CHAR(9)       ) AS BUILD_MODE,       CAST(NULL AS CHAR(18)) AS FAST_REFRESHABLE,       CAST(         CASE C.LAST_REFRESH_TYPE           WHEN 0 THEN 'COMPLETE'           WHEN 1 THEN 'FAST'           ELSE 'NA'         END AS CHAR(8)       ) AS LAST_REFRESH_TYPE,       CAST(C.LAST_REFRESH_DATE AS DATETIME) AS LAST_REFRESH_DATE,       CAST(DATE_ADD(C.LAST_REFRESH_DATE, INTERVAL C.LAST_REFRESH_TIME SECOND) AS DATETIME) AS LAST_REFRESH_END_TIME,       CAST(NULL AS CHAR(19)) AS STALENESS,       CAST(NULL AS CHAR(19)) AS AFTER_FAST_REFRESH,       CAST(IF(C.BUILD_MODE = 2, 'Y', 'N') AS CHAR(1)) AS UNKNOWN_PREBUILT,       CAST('N' AS CHAR(1)) AS UNKNOWN_PLSQL_FUNC,       CAST('N' AS CHAR(1)) AS UNKNOWN_EXTERNAL_TABLE,       CAST('N' AS CHAR(1)) AS UNKNOWN_CONSIDER_FRESH,       CAST('N' AS CHAR(1)) AS UNKNOWN_IMPORT,       CAST('N' AS CHAR(1)) AS UNKNOWN_TRUSTED_FD,       CAST(NULL AS CHAR(19)) AS COMPILE_STATE,       CAST('Y' AS CHAR(1)) AS USE_NO_INDEX,       CAST(NULL AS DATETIME) AS STALE_SINCE,       CAST(NULL AS SIGNED) AS NUM_PCT_TABLES,       CAST(NULL AS SIGNED) AS NUM_FRESH_PCT_REGIONS,       CAST(NULL AS SIGNED) AS NUM_STALE_PCT_REGIONS,       CAST('NO' AS CHAR(3)) AS SEGMENT_CREATED,       CAST(NULL AS CHAR(128)) AS EVALUATION_EDITION,       CAST(NULL AS CHAR(128)) AS UNUSABLE_BEFORE,       CAST(NULL AS CHAR(128)) AS UNUSABLE_BEGINNING,       CAST(NULL AS CHAR(100)) AS DEFAULT_COLLATION,       CAST(         CASE ((B.TABLE_MODE >> 28) & 1)           WHEN 0 THEN 'N'           WHEN 1 THEN 'Y'           ELSE NULL         END AS CHAR(1)       ) AS ON_QUERY_COMPUTATION     FROM       oceanbase.__all_database A,       oceanbase.__all_table B,       oceanbase.__all_mview C     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND B.TENANT_ID = C.TENANT_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7 )__"))) {
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

int ObInnerTableSchema::cdb_mvref_stats_sys_defaults_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_MVREF_STATS_SYS_DEFAULTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_MVREF_STATS_SYS_DEFAULTS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TENANT_ID,       CAST(PARAMETER_NAME AS CHAR(16)) AS PARAMETER_NAME,       CAST(VALUE AS CHAR(40)) AS VALUE     FROM     (       /* COLLECTION_LEVEL */       SELECT         TENANT_ID,         'COLLECTION_LEVEL' PARAMETER_NAME,         CASE IFNULL(MAX(COLLECTION_LEVEL), 1)           WHEN 0 THEN 'NONE'           WHEN 1 THEN 'TYPICAL'           WHEN 2 THEN 'ADVANCED'           ELSE NULL         END VALUE       FROM         oceanbase.__all_virtual_mview_refresh_stats_sys_defaults       RIGHT OUTER JOIN         (SELECT TENANT_ID FROM oceanbase.__all_tenant WHERE TENANT_ID = 1 OR (TENANT_ID & 0x1) = 0)       USING (TENANT_ID)       GROUP BY TENANT_ID        UNION ALL        /* RETENTION_PERIOD */       SELECT         TENANT_ID,         'RETENTION_PERIOD' PARAMETER_NAME,         CAST(IFNULL(MAX(RETENTION_PERIOD), 31) AS CHAR) VALUE       FROM         oceanbase.__all_virtual_mview_refresh_stats_sys_defaults       RIGHT OUTER JOIN         (SELECT TENANT_ID FROM oceanbase.__all_tenant WHERE TENANT_ID = 1 OR (TENANT_ID & 0x1) = 0)       USING (TENANT_ID)       GROUP BY TENANT_ID     )     ORDER BY TENANT_ID )__"))) {
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

int ObInnerTableSchema::dba_mvref_stats_sys_defaults_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_STATS_SYS_DEFAULTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_STATS_SYS_DEFAULTS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(PARAMETER_NAME AS CHAR(16)) AS PARAMETER_NAME,       CAST(VALUE AS CHAR(40)) AS VALUE     FROM     (       /* COLLECTION_LEVEL */       SELECT         'COLLECTION_LEVEL' PARAMETER_NAME,         CASE IFNULL(MAX(COLLECTION_LEVEL), 1)           WHEN 0 THEN 'NONE'           WHEN 1 THEN 'TYPICAL'           WHEN 2 THEN 'ADVANCED'           ELSE NULL         END VALUE       FROM         oceanbase.__all_mview_refresh_stats_sys_defaults        UNION ALL        /* RETENTION_PERIOD */       SELECT         'RETENTION_PERIOD' PARAMETER_NAME,         CAST(IFNULL(MAX(RETENTION_PERIOD), 31) AS CHAR) VALUE       FROM         oceanbase.__all_mview_refresh_stats_sys_defaults     ) )__"))) {
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

int ObInnerTableSchema::cdb_mvref_stats_params_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_MVREF_STATS_PARAMS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_MVREF_STATS_PARAMS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TENANT_ID,       CAST(MV_OWNER AS CHAR(128)) AS MV_OWNER,       CAST(MV_NAME AS CHAR(128)) AS MV_NAME,       CAST(         CASE COLLECTION_LEVEL           WHEN 0 THEN 'NONE'           WHEN 1 THEN 'TYPICAL'           WHEN 2 THEN 'ADVANCED'           ELSE NULL         END AS CHAR(8)       ) AS COLLECTION_LEVEL,       RETENTION_PERIOD     FROM     (       WITH DEFVALS AS        (         SELECT           TENANT_ID,           IFNULL(MAX(COLLECTION_LEVEL), 1) AS COLLECTION_LEVEL,           IFNULL(MAX(RETENTION_PERIOD), 31) AS RETENTION_PERIOD         FROM           oceanbase.__all_virtual_mview_refresh_stats_sys_defaults         RIGHT OUTER JOIN           (SELECT TENANT_ID FROM oceanbase.__all_tenant WHERE TENANT_ID = 1 OR (TENANT_ID & 0x1) = 0)         USING (TENANT_ID)         GROUP BY TENANT_ID       )        SELECT         B.TENANT_ID TENANT_ID,         A.DATABASE_NAME MV_OWNER,         B.TABLE_NAME MV_NAME,         IFNULL(C.COLLECTION_LEVEL, D.COLLECTION_LEVEL) COLLECTION_LEVEL,         IFNULL(C.RETENTION_PERIOD, D.RETENTION_PERIOD) RETENTION_PERIOD       FROM         oceanbase.__all_virtual_database A,         oceanbase.__all_virtual_table B,         (           SELECT TENANT_ID, MVIEW_ID, COLLECTION_LEVEL, RETENTION_PERIOD FROM oceanbase.__all_virtual_mview_refresh_stats_params           RIGHT OUTER JOIN           (             SELECT TENANT_ID, MVIEW_ID FROM oceanbase.__all_virtual_mview           )           USING (TENANT_ID, MVIEW_ID)         ) C,         DEFVALS D       WHERE A.TENANT_ID = B.TENANT_ID         AND A.DATABASE_ID = B.DATABASE_ID         AND B.TENANT_ID = C.TENANT_ID         AND B.TABLE_ID = C.MVIEW_ID         AND B.TABLE_TYPE = 7         AND C.TENANT_ID = D.TENANT_ID     ) )__"))) {
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

int ObInnerTableSchema::dba_mvref_stats_params_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_STATS_PARAMS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_STATS_PARAMS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(MV_OWNER AS CHAR(128)) AS MV_OWNER,       CAST(MV_NAME AS CHAR(128)) AS MV_NAME,       CAST(         CASE COLLECTION_LEVEL           WHEN 0 THEN 'NONE'           WHEN 1 THEN 'TYPICAL'           WHEN 2 THEN 'ADVANCED'           ELSE NULL         END AS CHAR(8)       ) AS COLLECTION_LEVEL,       RETENTION_PERIOD     FROM     (       WITH DEFVALS AS        (         SELECT           IFNULL(MAX(COLLECTION_LEVEL), 1) AS COLLECTION_LEVEL,           IFNULL(MAX(RETENTION_PERIOD), 31) AS RETENTION_PERIOD         FROM           oceanbase.__all_mview_refresh_stats_sys_defaults       )        SELECT         A.DATABASE_NAME MV_OWNER,         B.TABLE_NAME MV_NAME,         IFNULL(C.COLLECTION_LEVEL, D.COLLECTION_LEVEL) COLLECTION_LEVEL,         IFNULL(C.RETENTION_PERIOD, D.RETENTION_PERIOD) RETENTION_PERIOD       FROM         oceanbase.__all_database A,         oceanbase.__all_table B,         (           SELECT TENANT_ID, MVIEW_ID, COLLECTION_LEVEL, RETENTION_PERIOD FROM oceanbase.__all_mview_refresh_stats_params           RIGHT OUTER JOIN           (             SELECT TENANT_ID, MVIEW_ID FROM oceanbase.__all_mview           )           USING (TENANT_ID, MVIEW_ID)         ) C,         DEFVALS D       WHERE A.TENANT_ID = B.TENANT_ID         AND A.DATABASE_ID = B.DATABASE_ID         AND B.TENANT_ID = C.TENANT_ID         AND B.TABLE_ID = C.MVIEW_ID         AND B.TABLE_TYPE = 7     ) )__"))) {
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

int ObInnerTableSchema::cdb_mvref_run_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_MVREF_RUN_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_MVREF_RUN_STATS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       B.TENANT_ID AS TENANT_ID,       CAST(A.USER_NAME AS CHAR(128)) AS RUN_OWNER,       B.REFRESH_ID AS REFRESH_ID,       B.NUM_MVS_TOTAL AS NUM_MVS,       CAST(B.MVIEWS AS CHAR(4000)) AS MVIEWS,       CAST(B.BASE_TABLES AS CHAR(4000)) AS BASE_TABLES,       CAST(B.METHOD AS CHAR(4000)) AS METHOD,       CAST(B.ROLLBACK_SEG AS CHAR(4000)) AS ROLLBACK_SEG,       CAST(IF(B.PUSH_DEFERRED_RPC = 1, 'Y', 'N') AS CHAR(1)) AS PUSH_DEFERRED_RPC,       CAST(IF(B.REFRESH_AFTER_ERRORS = 1, 'Y', 'N') AS CHAR(1)) AS REFRESH_AFTER_ERRORS,       B.PURGE_OPTION AS PURGE_OPTION,       B.PARALLELISM AS PARALLELISM,       B.HEAP_SIZE AS HEAP_SIZE,       CAST(IF(B.ATOMIC_REFRESH = 1, 'Y', 'N') AS CHAR(1)) AS ATOMIC_REFRESH,       CAST(IF(B.NESTED = 1, 'Y', 'N') AS CHAR(1)) AS NESTED,       CAST(IF(B.OUT_OF_PLACE = 1, 'Y', 'N') AS CHAR(1)) AS OUT_OF_PLACE,       B.NUMBER_OF_FAILURES AS NUMBER_OF_FAILURES,       CAST(B.START_TIME AS DATETIME) AS START_TIME,       CAST(B.END_TIME AS DATETIME) AS END_TIME,       B.ELAPSED_TIME AS ELAPSED_TIME,       CAST(0 AS SIGNED) AS LOG_SETUP_TIME,       B.LOG_PURGE_TIME AS LOG_PURGE_TIME,       CAST(IF(B.COMPLETE_STATS_AVALIABLE = 1, 'Y', 'N') AS CHAR(1)) AS COMPLETE_STATS_AVAILABLE     FROM       oceanbase.__all_virtual_user A,       oceanbase.__all_virtual_mview_refresh_run_stats B,       (         SELECT           C1.TENANT_ID AS TENANT_ID,           C1.REFRESH_ID AS REFRESH_ID         FROM           oceanbase.__all_virtual_mview_refresh_stats C1,           oceanbase.__all_virtual_table C2         WHERE C1.TENANT_ID = C2.TENANT_ID           AND C1.MVIEW_ID = C2.TABLE_ID         GROUP BY TENANT_ID, REFRESH_ID       ) C     WHERE A.TENANT_ID = B.TENANT_ID       AND A.USER_ID = B.RUN_USER_ID       AND B.TENANT_ID = C.TENANT_ID       AND B.REFRESH_ID = C.REFRESH_ID )__"))) {
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

int ObInnerTableSchema::dba_mvref_run_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_RUN_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_RUN_STATS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.USER_NAME AS CHAR(128)) AS RUN_OWNER,       B.REFRESH_ID AS REFRESH_ID,       B.NUM_MVS_TOTAL AS NUM_MVS,       CAST(B.MVIEWS AS CHAR(4000)) AS MVIEWS,       CAST(B.BASE_TABLES AS CHAR(4000)) AS BASE_TABLES,       CAST(B.METHOD AS CHAR(4000)) AS METHOD,       CAST(B.ROLLBACK_SEG AS CHAR(4000)) AS ROLLBACK_SEG,       CAST(IF(B.PUSH_DEFERRED_RPC = 1, 'Y', 'N') AS CHAR(1)) AS PUSH_DEFERRED_RPC,       CAST(IF(B.REFRESH_AFTER_ERRORS = 1, 'Y', 'N') AS CHAR(1)) AS REFRESH_AFTER_ERRORS,       B.PURGE_OPTION AS PURGE_OPTION,       B.PARALLELISM AS PARALLELISM,       B.HEAP_SIZE AS HEAP_SIZE,       CAST(IF(B.ATOMIC_REFRESH = 1, 'Y', 'N') AS CHAR(1)) AS ATOMIC_REFRESH,       CAST(IF(B.NESTED = 1, 'Y', 'N') AS CHAR(1)) AS NESTED,       CAST(IF(B.OUT_OF_PLACE = 1, 'Y', 'N') AS CHAR(1)) AS OUT_OF_PLACE,       B.NUMBER_OF_FAILURES AS NUMBER_OF_FAILURES,       CAST(B.START_TIME AS DATETIME) AS START_TIME,       CAST(B.END_TIME AS DATETIME) AS END_TIME,       B.ELAPSED_TIME AS ELAPSED_TIME,       CAST(0 AS SIGNED) AS LOG_SETUP_TIME,       B.LOG_PURGE_TIME AS LOG_PURGE_TIME,       CAST(IF(B.COMPLETE_STATS_AVALIABLE = 1, 'Y', 'N') AS CHAR(1)) AS COMPLETE_STATS_AVAILABLE     FROM       oceanbase.__all_user A,       oceanbase.__all_mview_refresh_run_stats B,       (         SELECT           C1.TENANT_ID AS TENANT_ID,           C1.REFRESH_ID AS REFRESH_ID         FROM           oceanbase.__all_mview_refresh_stats C1,           oceanbase.__all_table C2         WHERE C1.TENANT_ID = C2.TENANT_ID           AND C1.MVIEW_ID = C2.TABLE_ID         GROUP BY TENANT_ID, REFRESH_ID       ) C     WHERE A.TENANT_ID = B.TENANT_ID       AND A.USER_ID = B.RUN_USER_ID       AND B.TENANT_ID = C.TENANT_ID       AND B.REFRESH_ID = C.REFRESH_ID )__"))) {
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

int ObInnerTableSchema::cdb_mvref_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_MVREF_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_MVREF_STATS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       B.TENANT_ID AS TENANT_ID,       CAST(A.DATABASE_NAME AS CHAR(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS CHAR(128)) AS MV_NAME,       C.REFRESH_ID AS REFRESH_ID,       CAST(         CASE C.REFRESH_TYPE           WHEN 0 THEN 'COMPLETE'           WHEN 1 THEN 'FAST'           ELSE NULL         END AS CHAR(30)       ) AS REFRESH_METHOD,       CAST(NULL AS CHAR(4000)) AS REFRESH_OPTIMIZATIONS,       CAST(NULL AS CHAR(4000)) AS ADDITIONAL_EXECUTIONS,       CAST(C.START_TIME AS DATETIME) AS START_TIME,       CAST(C.END_TIME AS DATETIME) AS END_TIME,       C.ELAPSED_TIME AS ELAPSED_TIME,       CAST(0 AS SIGNED) AS LOG_SETUP_TIME,       C.LOG_PURGE_TIME AS LOG_PURGE_TIME,       C.INITIAL_NUM_ROWS AS INITIAL_NUM_ROWS,       C.FINAL_NUM_ROWS AS FINAL_NUM_ROWS     FROM       oceanbase.__all_virtual_database A,       oceanbase.__all_virtual_table B,       oceanbase.__all_virtual_mview_refresh_stats C     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND B.TENANT_ID = C.TENANT_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7 )__"))) {
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

int ObInnerTableSchema::dba_mvref_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_STATS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS CHAR(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS CHAR(128)) AS MV_NAME,       C.REFRESH_ID AS REFRESH_ID,       CAST(         CASE C.REFRESH_TYPE           WHEN 0 THEN 'COMPLETE'           WHEN 1 THEN 'FAST'           ELSE NULL         END AS CHAR(30)       ) AS REFRESH_METHOD,       CAST(NULL AS CHAR(4000)) AS REFRESH_OPTIMIZATIONS,       CAST(NULL AS CHAR(4000)) AS ADDITIONAL_EXECUTIONS,       CAST(C.START_TIME AS DATETIME) AS START_TIME,       CAST(C.END_TIME AS DATETIME) AS END_TIME,       C.ELAPSED_TIME AS ELAPSED_TIME,       CAST(0 AS SIGNED) AS LOG_SETUP_TIME,       C.LOG_PURGE_TIME AS LOG_PURGE_TIME,       C.INITIAL_NUM_ROWS AS INITIAL_NUM_ROWS,       C.FINAL_NUM_ROWS AS FINAL_NUM_ROWS     FROM       oceanbase.__all_database A,       oceanbase.__all_table B,       oceanbase.__all_mview_refresh_stats C     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND B.TENANT_ID = C.TENANT_ID       AND B.TABLE_ID = C.MVIEW_ID       AND B.TABLE_TYPE = 7 )__"))) {
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

int ObInnerTableSchema::cdb_mvref_change_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_MVREF_CHANGE_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_MVREF_CHANGE_STATS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       E.TENANT_ID AS TENANT_ID,       CAST(C.DATABASE_NAME AS CHAR(128)) AS TBL_OWNER,       CAST(D.TABLE_NAME AS CHAR(128)) AS TBL_NAME,       CAST(A.DATABASE_NAME AS CHAR(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS CHAR(128)) AS MV_NAME,       E.REFRESH_ID AS REFRESH_ID,       E.NUM_ROWS_INS AS NUM_ROWS_INS,       E.NUM_ROWS_UPD AS NUM_ROWS_UPD,       E.NUM_ROWS_DEL AS NUM_ROWS_DEL,       CAST(0 AS SIGNED) AS NUM_ROWS_DL_INS,       CAST('N' AS CHAR(1)) AS PMOPS_OCCURRED,       CAST(NULL AS CHAR(4000)) AS PMOP_DETAILS,       E.NUM_ROWS AS NUM_ROWS     FROM       oceanbase.__all_virtual_database A,       oceanbase.__all_virtual_table B,       oceanbase.__all_virtual_database C,       oceanbase.__all_virtual_table D,       oceanbase.__all_virtual_mview_refresh_change_stats E     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND C.TENANT_ID = D.TENANT_ID       AND C.DATABASE_ID = D.DATABASE_ID       AND E.TENANT_ID = B.TENANT_ID       AND E.MVIEW_ID = B.TABLE_ID       AND E.TENANT_ID = D.TENANT_ID       AND E.DETAIL_TABLE_ID = D.TABLE_ID )__"))) {
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

int ObInnerTableSchema::dba_mvref_change_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_CHANGE_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_CHANGE_STATS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(C.DATABASE_NAME AS CHAR(128)) AS TBL_OWNER,       CAST(D.TABLE_NAME AS CHAR(128)) AS TBL_NAME,       CAST(A.DATABASE_NAME AS CHAR(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS CHAR(128)) AS MV_NAME,       E.REFRESH_ID AS REFRESH_ID,       E.NUM_ROWS_INS AS NUM_ROWS_INS,       E.NUM_ROWS_UPD AS NUM_ROWS_UPD,       E.NUM_ROWS_DEL AS NUM_ROWS_DEL,       CAST(0 AS SIGNED) AS NUM_ROWS_DL_INS,       CAST('N' AS CHAR(1)) AS PMOPS_OCCURRED,       CAST(NULL AS CHAR(4000)) AS PMOP_DETAILS,       E.NUM_ROWS AS NUM_ROWS     FROM       oceanbase.__all_database A,       oceanbase.__all_table B,       oceanbase.__all_database C,       oceanbase.__all_table D,       oceanbase.__all_mview_refresh_change_stats E     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND C.TENANT_ID = D.TENANT_ID       AND C.DATABASE_ID = D.DATABASE_ID       AND E.TENANT_ID = B.TENANT_ID       AND E.MVIEW_ID = B.TABLE_ID       AND E.TENANT_ID = D.TENANT_ID       AND E.DETAIL_TABLE_ID = D.TABLE_ID )__"))) {
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

int ObInnerTableSchema::cdb_mvref_stmt_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_MVREF_STMT_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_MVREF_STMT_STATS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       C.TENANT_ID AS TENANT_ID,       CAST(A.DATABASE_NAME AS CHAR(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS CHAR(128)) AS MV_NAME,       C.REFRESH_ID AS REFRESH_ID,       C.STEP AS STEP,       CAST(C.SQLID AS CHAR(32)) AS SQLID,       C.STMT AS STMT,       C.EXECUTION_TIME AS EXECUTION_TIME,       C.EXECUTION_PLAN AS EXECUTION_PLAN     FROM       oceanbase.__all_virtual_database A,       oceanbase.__all_virtual_table B,       oceanbase.__all_virtual_mview_refresh_stmt_stats C     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND B.TENANT_ID = C.TENANT_ID       AND B.TABLE_ID = C.MVIEW_ID )__"))) {
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

int ObInnerTableSchema::dba_mvref_stmt_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_MVREF_STMT_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVREF_STMT_STATS_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS CHAR(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS CHAR(128)) AS MV_NAME,       C.REFRESH_ID AS REFRESH_ID,       C.STEP AS STEP,       CAST(C.SQLID AS CHAR(32)) AS SQLID,       C.STMT AS STMT,       C.EXECUTION_TIME AS EXECUTION_TIME,       C.EXECUTION_PLAN AS EXECUTION_PLAN     FROM       oceanbase.__all_database A,       oceanbase.__all_table B,       oceanbase.__all_mview_refresh_stmt_stats C     WHERE A.TENANT_ID = B.TENANT_ID       AND A.DATABASE_ID = B.DATABASE_ID       AND B.TENANT_ID = C.TENANT_ID       AND B.TABLE_ID = C.MVIEW_ID )__"))) {
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

int ObInnerTableSchema::gv_ob_tracepoint_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TRACEPOINT_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TRACEPOINT_INFO_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           SVR_IP,           SVR_PORT,           TP_NO,           TP_NAME,           TP_DESCRIBE,           TP_FREQUENCY,           TP_ERROR_CODE,           TP_OCCUR,           TP_MATCH         FROM oceanbase.__all_virtual_tracepoint_info )__"))) {
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

int ObInnerTableSchema::v_ob_tracepoint_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TRACEPOINT_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TRACEPOINT_INFO_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           SVR_IP,           SVR_PORT,           TP_NO,           TP_NAME,           TP_DESCRIBE,           TP_FREQUENCY,           TP_ERROR_CODE,           TP_OCCUR,           TP_MATCH     FROM OCEANBASE.GV$OB_TRACEPOINT_INFO     WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::v_ob_compatibility_control_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_COMPATIBILITY_CONTROL_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_COMPATIBILITY_CONTROL_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       name as NAME,       description as DESCRIPTION,       CASE is_enable WHEN 1 THEN 'TRUE' ELSE 'FALSE' END AS IS_ENABLE,       enable_versions as ENABLE_VERSIONS     FROM oceanbase.__all_virtual_compatibility_control )__"))) {
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
