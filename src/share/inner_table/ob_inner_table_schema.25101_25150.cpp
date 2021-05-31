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

namespace oceanbase {
using namespace share::schema;
using namespace common;
namespace share {

int ObInnerTableSchema::user_scheduler_job_args_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_SCHEDULER_JOB_ARGS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SCHEDULER_JOB_ARGS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS JOB_NAME,   CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,   CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,   CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,   CAST(NULL AS VARCHAR2(4000)) AS VALUE,   CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,   CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT FROM   DUAL WHERE   1 = 0 )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_errors_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ERRORS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ERRORS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(o.owner AS VARCHAR2(128)) AS OWNER,   CAST(o.object_name AS VARCHAR2(128)) AS NAME,   CAST(o.object_type AS VARCHAR2(19)) AS TYPE,   CAST(e.obj_seq AS NUMBER) AS SEQUENCE,   CAST(e.line AS NUMBER) AS LINE,   CAST(e.position AS NUMBER) AS POSITION,   CAST(e.text as VARCHAR2(4000)) AS TEXT,   CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,   CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER FROM   all_objects o,  (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,                                    3, 'PACKAGE',                                    4, 'TYPE',                                    5, 'PACKAGE BODY',                                    6, 'TYPE BODY',                                    7, 'TRIGGER',                                    8, 'VIEW',                                    9, 'FUNCTION',                                    12, 'PROCEDURE',                                    'MAXTYPE')) AS VARCHAR2(23)) object_type from sys.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT                       WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e WHERE   o.object_id = e.obj_id   AND o.object_type like e.object_type   AND o.object_type IN (UPPER('package'),                         UPPER('type'),                         UPPER('procedure'),                         UPPER('function'),                         UPPER('package body'),                         UPPER('view'),                         UPPER('trigger'),                         UPPER('type body'),                         UPPER('library'),                         UPPER('queue'),                         UPPER('java source'),                         UPPER('java class'),                         UPPER('dimension'),                         UPPER('assembly'),                         UPPER('hierarchy'),                         UPPER('arrtibute dimension'),                         UPPER('analytic view'))   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_errors_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_ERRORS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_ERRORS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(o.owner AS VARCHAR2(128)) AS OWNER,   CAST(o.object_name AS VARCHAR2(128)) AS NAME,   CAST(o.object_type AS VARCHAR2(19)) AS TYPE,   CAST(e.obj_seq AS NUMBER) AS SEQUENCE,   CAST(e.line AS NUMBER) AS LINE,   CAST(e.position AS NUMBER) AS POSITION,   CAST(e.text as VARCHAR2(4000)) AS TEXT,   CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,   CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER FROM   all_objects o,  (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,                                    3, 'PACKAGE',                                    4, 'TYPE',                                    5, 'PACKAGE BODY',                                    6, 'TYPE BODY',                                    7, 'TRIGGER',                                    8, 'VIEW',                                    9, 'FUNCTION',                                    12, 'PROCEDURE',                                    'MAXTYPE')) AS VARCHAR2(23)) object_type from sys.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT                             WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e WHERE   o.object_id = e.obj_id   AND o.object_type like e.object_type   AND o.object_type IN (UPPER('package'),                         UPPER('type'),                         UPPER('procedure'),                         UPPER('function'),                         UPPER('package body'),                         UPPER('view'),                         UPPER('trigger'),                         UPPER('type body'),                         UPPER('library'),                         UPPER('queue'),                         UPPER('java source'),                         UPPER('java class'),                         UPPER('dimension'),                         UPPER('assembly'),                         UPPER('hierarchy'),                         UPPER('arrtibute dimension'),                         UPPER('analytic view'))   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_errors_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_ERRORS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_ERRORS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(o.owner AS VARCHAR2(128)) AS OWNER,   CAST(o.object_name AS VARCHAR2(128)) AS NAME,   CAST(o.object_type AS VARCHAR2(19)) AS TYPE,   CAST(e.obj_seq AS NUMBER) AS SEQUENCE,   CAST(e.line AS NUMBER) AS LINE,   CAST(e.position AS NUMBER) AS POSITION,   CAST(e.text as VARCHAR2(4000)) AS TEXT,   CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,   CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER FROM   all_objects o,  (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,                                    3, 'PACKAGE',                                    4, 'TYPE',                                    5, 'PACKAGE BODY',                                    6, 'TYPE BODY',                                    7, 'TRIGGER',                                    8, 'VIEW',                                    9, 'FUNCTION',                                    12, 'PROCEDURE',                                    'MAXTYPE')) AS VARCHAR2(23)) object_type from sys.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT                           WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e,   all_users u WHERE   o.object_id = e.obj_id   AND o.object_type like e.object_type   AND o.object_type IN (UPPER('package'),                         UPPER('type'),                         UPPER('procedure'),                         UPPER('function'),                         UPPER('package body'),                         UPPER('view'),                         UPPER('trigger'),                         UPPER('type body'),                         UPPER('library'),                         UPPER('queue'),                         UPPER('java source'),                         UPPER('java class'),                         UPPER('dimension'),                         UPPER('assembly'),                         UPPER('hierarchy'),                         UPPER('arrtibute dimension'),                         UPPER('analytic view'))   AND u.username=o.owner   AND u.userid IN (USERENV('SCHEMAID'))   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_type_methods_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TYPE_METHODS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TYPE_METHODS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,   CAST(NULL AS NUMBER) AS PARAMETERS,   CAST(NULL AS NUMBER) AS RESULTS,   CAST(NULL AS VARCHAR2(3)) AS FINAL,   CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,   CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,   CAST(NULL AS VARCHAR2(3)) AS INHERITED FROM   DUAL WHERE   1 = 0 )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_type_methods_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_TYPE_METHODS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TYPE_METHODS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,   CAST(NULL AS NUMBER) AS PARAMETERS,   CAST(NULL AS NUMBER) AS RESULTS,   CAST(NULL AS VARCHAR2(3)) AS FINAL,   CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,   CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,   CAST(NULL AS VARCHAR2(3)) AS INHERITED FROM   DUAL WHERE   1 = 0 )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_type_methods_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_TYPE_METHODS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TYPE_METHODS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,   CAST(NULL AS NUMBER) AS PARAMETERS,   CAST(NULL AS NUMBER) AS RESULTS,   CAST(NULL AS VARCHAR2(3)) AS FINAL,   CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,   CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,   CAST(NULL AS VARCHAR2(3)) AS INHERITED FROM   DUAL WHERE   1 = 0 )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_method_params_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_METHOD_PARAMS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_METHOD_PARAMS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,   CAST(NULL AS NUMBER) AS PARAM_NO,   CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,   CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,   CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME FROM   DUAL WHERE   1 = 0 )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_method_params_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_METHOD_PARAMS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_METHOD_PARAMS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,   CAST(NULL AS NUMBER) AS PARAM_NO,   CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,   CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,   CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME FROM   DUAL WHERE   1 = 0 )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_method_params_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_METHOD_PARAMS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_METHOD_PARAMS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,   CAST(NULL AS NUMBER) AS PARAM_NO,   CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,   CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,   CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME FROM   DUAL WHERE   1 = 0 )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_tablespaces_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_TABLESPACES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TABLESPACES_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(SELECT       TABLESPACE_NAME,       CAST(NULL AS NUMBER) BLOCK_SIZE,       CAST(NULL AS NUMBER) INITIAL_EXTENT,       CAST(NULL AS NUMBER) NEXT_EXTENT,       CAST(NULL AS NUMBER) MIN_EXTENT,       CAST(NULL AS NUMBER) MAX_EXTENT,       CAST(NULL AS NUMBER) MAX_SIZE,       CAST(NULL AS NUMBER) PCT_INCREASE,       CAST(NULL AS NUMBER) MIN_EXTLEN,       CAST(NULL AS VARCHAR2(9)) STATUS,       CAST(NULL AS VARCHAR2(9)) CONTENTS,       CAST(NULL AS VARCHAR2(9)) LOGGING,       CAST(NULL AS VARCHAR2(3)) FORCE_LOGGING,       CAST(NULL AS VARCHAR2(10)) EXTENT_MANAGEMENT,       CAST(NULL AS VARCHAR2(9)) ALLOCATION_TYPE,       CAST(NULL AS VARCHAR2(3)) PLUGGED_IN,       CAST(NULL AS VARCHAR2(6)) SEGMENT_SPACE_MANAGEMENT,       CAST(NULL AS VARCHAR2(8)) DEF_TAB_COMPRESSION,       CAST(NULL AS VARCHAR2(11)) RETENTION,       CAST(NULL AS VARCHAR2(3)) BIGFILE,       CAST(NULL AS VARCHAR2(7)) PREDICATE_EVALUATION,       CAST(NULL AS VARCHAR2(3)) ENCRYPTED,       CAST(NULL AS VARCHAR2(12)) COMPRESS_FOR       FROM       SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT       WHERE TENANT_ID = EFFECTIVE_TENANT_ID()       )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_tablespaces_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_TABLESPACES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TABLESPACES_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(SELECT       TABLESPACE_NAME,       CAST(NULL AS NUMBER) BLOCK_SIZE,       CAST(NULL AS NUMBER) INITIAL_EXTENT,       CAST(NULL AS NUMBER) NEXT_EXTENT,       CAST(NULL AS NUMBER) MIN_EXTENT,       CAST(NULL AS NUMBER) MAX_EXTENT,       CAST(NULL AS NUMBER) MAX_SIZE,       CAST(NULL AS NUMBER) PCT_INCREASE,       CAST(NULL AS NUMBER) MIN_EXTLEN,       CAST(NULL AS VARCHAR2(9)) STATUS,       CAST(NULL AS VARCHAR2(9)) CONTENTS,       CAST(NULL AS VARCHAR2(9)) LOGGING,       CAST(NULL AS VARCHAR2(3)) FORCE_LOGGING,       CAST(NULL AS VARCHAR2(10)) EXTENT_MANAGEMENT,       CAST(NULL AS VARCHAR2(9)) ALLOCATION_TYPE,       CAST(NULL AS VARCHAR2(6)) SEGMENT_SPACE_MANAGEMENT,       CAST(NULL AS VARCHAR2(8)) DEF_TAB_COMPRESSION,       CAST(NULL AS VARCHAR2(11)) RETENTION,       CAST(NULL AS VARCHAR2(3)) BIGFILE,       CAST(NULL AS VARCHAR2(7)) PREDICATE_EVALUATION,       CAST(NULL AS VARCHAR2(3)) ENCRYPTED,       CAST(NULL AS VARCHAR2(12)) COMPRESS_FOR       FROM       SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT       WHERE TENANT_ID = EFFECTIVE_TENANT_ID()       )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_ind_expressions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_IND_EXPRESSIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_IND_EXPRESSIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema
                    .set_view_definition(
                        R"__(   SELECT CAST(INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,     CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,     CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,     COLUMN_POSITION   FROM   (   SELECT INDEX_OWNER,     INDEX_NAME,     TABLE_OWNER,     F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,     E.INDEX_POSITION AS  COLUMN_POSITION,     E.TABLE_ID AS TABLE_ID     FROM       (SELECT INDEX_OWNER,               INDEX_NAME,               TABLE_OWNER,               C.TABLE_ID AS TABLE_ID,               C.INDEX_ID AS INDEX_ID,               D.COLUMN_ID AS COLUMN_ID,               D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION       FROM         (SELECT DATABASE_NAME AS INDEX_OWNER,                 SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 DATABASE_NAME AS TABLE_OWNER,                 A.DATA_TABLE_ID AS TABLE_ID,                 A.TABLE_ID AS INDEX_ID           FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A           JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID           AND B.DATABASE_NAME != '__recyclebin'           AND A.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           WHERE TABLE_TYPE=5 ) C       JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()) E     JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID         AND F.TENANT_ID = EFFECTIVE_TENANT_ID()     AND F.COLUMN_ID=E.COLUMN_ID     AND F.COLUMN_FLAGS=1) G   JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID       AND H.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_ind_expressions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_IND_EXPRESSIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_IND_EXPRESSIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(   SELECT     CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,     CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,     COLUMN_POSITION   FROM   (   SELECT     INDEX_NAME,     F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,     E.INDEX_POSITION AS  COLUMN_POSITION,     E.TABLE_ID AS TABLE_ID     FROM       (SELECT               INDEX_NAME,               C.TABLE_ID AS TABLE_ID,               C.INDEX_ID AS INDEX_ID,               D.COLUMN_ID AS COLUMN_ID,               D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION       FROM         (SELECT                 SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 A.DATA_TABLE_ID AS TABLE_ID,                 A.TABLE_ID AS INDEX_ID           FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A           JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID           AND B.DATABASE_NAME != '__recyclebin' AND A.DATABASE_ID = USERENV('SCHEMAID')           AND A.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           WHERE TABLE_TYPE=5 ) C       JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()) E     JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID         AND F.TENANT_ID = EFFECTIVE_TENANT_ID()     AND F.COLUMN_ID=E.COLUMN_ID     AND F.COLUMN_FLAGS=1) G   JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID       AND H.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_ind_expressions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_IND_EXPRESSIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_IND_EXPRESSIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(   SELECT CAST(INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,     CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,     CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,     COLUMN_POSITION   FROM   (   SELECT INDEX_OWNER,     INDEX_NAME,     TABLE_OWNER,     F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,     E.INDEX_POSITION AS  COLUMN_POSITION,     E.TABLE_ID AS TABLE_ID     FROM       (SELECT INDEX_OWNER,               INDEX_NAME,               TABLE_OWNER,               C.TABLE_ID AS TABLE_ID,               C.INDEX_ID AS INDEX_ID,               D.COLUMN_ID AS COLUMN_ID,               D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION       FROM         (SELECT DATABASE_NAME AS INDEX_OWNER,                         SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 DATABASE_NAME AS TABLE_OWNER,                 A.DATA_TABLE_ID AS TABLE_ID,                 A.TABLE_ID AS INDEX_ID           FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A           JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID           AND A.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (A.DATABASE_ID = USERENV('SCHEMAID')                OR USER_CAN_ACCESS_OBJ(1, A.DATA_TABLE_ID, A.DATABASE_ID) = 1)           AND B.DATABASE_NAME != '__recyclebin'           WHERE TABLE_TYPE=5 ) C       JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()) E     JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID         AND F.TENANT_ID = EFFECTIVE_TENANT_ID()     AND F.COLUMN_ID=E.COLUMN_ID     AND F.COLUMN_FLAGS=1) G   JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID       AND H.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_ind_partitions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_IND_PARTITIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_IND_PARTITIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(   SELECT     CAST(B.INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(CASE WHEN PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) AS COMPOSITE,     CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,     PART.SUB_PART_NUM AS SUBPARTITION_COUNT,     CAST(PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,     CAST(LENGTH(PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,     PART.PART_ID + 1 AS PARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST(NULL AS VARCHAR2(3)) AS INTERVAL,     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED     FROM(       SELECT       A.INDEX_OWNER AS INDEX_OWNER,       A.INDEX_NAME AS INDEX_NAME,       A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID*/       A.DATA_TABLE_ID AS DATA_TABLE_ID,       CAST(CASE WHEN A.PART_LEVEL=1 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL       FROM(         SELECT           DB.DATABASE_NAME AS INDEX_OWNER,           SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,           TB1.TABLE_ID AS INDEX_TABLE_ID,           TB1.DATA_TABLE_ID AS DATA_TABLE_ID,           TB1.INDEX_TYPE AS INDEX_TYPE,           TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/         FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         WHERE DB.DATABASE_NAME!='__recyclebin'          AND TB1.DATABASE_ID=DB.DATABASE_ID          AND TB1.TABLE_TYPE=5          AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()          AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()          AND (TB1.DATABASE_ID = USERENV('SCHEMAID')              OR USER_CAN_ACCESS_OBJ(1, TB1.DATA_TABLE_ID, TB1.DATABASE_ID) = 1)       ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID           AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()       WHERE A.PART_LEVEL=1 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=1)     ) B JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART ON     ((B.TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='FALSE') OR (B.DATA_TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='TRUE'))     AND PART.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_ind_partitions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_IND_PARTITIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_IND_PARTITIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(   SELECT     CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(CASE WHEN PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) AS COMPOSITE,     CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,     PART.SUB_PART_NUM AS SUBPARTITION_COUNT,     CAST(PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,     CAST(LENGTH(PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,     PART.PART_ID + 1 AS PARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST(NULL AS VARCHAR2(3)) AS INTERVAL,     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED     FROM(       SELECT       A.INDEX_OWNER AS INDEX_OWNER,       A.INDEX_NAME AS INDEX_NAME,       A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID */       A.DATA_TABLE_ID AS DATA_TABLE_ID,       CAST(CASE WHEN A.PART_LEVEL=1 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL       FROM(         SELECT           DB.DATABASE_NAME AS INDEX_OWNER,           SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,           TB1.TABLE_ID AS INDEX_TABLE_ID,           TB1.DATA_TABLE_ID AS DATA_TABLE_ID,           TB1.INDEX_TYPE AS INDEX_TYPE,           TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/         FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         WHERE DB.DATABASE_NAME!='__recyclebin' AND TB1.DATABASE_ID=DB.DATABASE_ID AND TB1.TABLE_TYPE=5               AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()               AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()       ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID           AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()       WHERE A.PART_LEVEL=1 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=1)     ) B JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART ON     ((B.TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='FALSE') OR (B.DATA_TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='TRUE'))     AND PART.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_ind_partitions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_IND_PARTITIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_IND_PARTITIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(   SELECT     CAST(B.INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(CASE WHEN PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) AS COMPOSITE,     CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,     PART.SUB_PART_NUM AS SUBPARTITION_COUNT,     CAST(PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,     CAST(LENGTH(PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,     PART.PART_ID + 1 AS PARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST(NULL AS VARCHAR2(3)) AS INTERVAL,     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED     FROM(       SELECT       A.INDEX_OWNER AS INDEX_OWNER,       A.INDEX_NAME AS INDEX_NAME,       A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID*/       A.DATA_TABLE_ID AS DATA_TABLE_ID,       CAST(CASE WHEN A.PART_LEVEL=1 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL       FROM(         SELECT           DB.DATABASE_NAME AS INDEX_OWNER,           SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,           TB1.TABLE_ID AS INDEX_TABLE_ID,           TB1.DATA_TABLE_ID AS DATA_TABLE_ID,           TB1.INDEX_TYPE AS INDEX_TYPE,           TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/         FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         WHERE DB.DATABASE_NAME!='__recyclebin' AND TB1.DATABASE_ID=DB.DATABASE_ID AND TB1.TABLE_TYPE=5             AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()             AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()       ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID           AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()       WHERE A.PART_LEVEL=1 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=1)     ) B JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART ON     ((B.TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='FALSE') OR (B.DATA_TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='TRUE'))     AND PART.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_ind_subpartitions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_IND_SUBPARTITIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_IND_SUBPARTITIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(   SELECT     CAST(B.INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,     CAST(SUB_PART.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(SUB_PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,     CAST(LENGTH(SUB_PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,     SUB_PART.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST(NULL AS VARCHAR2(3)) AS INTERVAL,     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED     FROM(       SELECT       A.INDEX_OWNER AS INDEX_OWNER,       A.INDEX_NAME AS INDEX_NAME,       A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID*/       A.DATA_TABLE_ID AS DATA_TABLE_ID,       A.PART_LEVEL AS INDEX_TABLE_PART_LEVEL,       TB2.PART_LEVEL AS DATA_TABLE_PART_LEVEL,       /* CAST(CASE WHEN A.PART_LEVEL=2 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL */       CAST('TRUE' AS VARCHAR2(5)) AS IS_LOCAL       FROM(         SELECT           DB.DATABASE_NAME AS INDEX_OWNER,           SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,           TB1.TABLE_ID AS INDEX_TABLE_ID,           TB1.DATA_TABLE_ID AS DATA_TABLE_ID,           TB1.INDEX_TYPE AS INDEX_TYPE,           TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/         FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         WHERE DB.DATABASE_NAME!='__recyclebin' AND TB1.DATABASE_ID=DB.DATABASE_ID AND TB1.TABLE_TYPE=5             AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()             AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()       ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID             AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()       WHERE A.PART_LEVEL=2 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=2)       ) B, SYS.ALL_VIRTUAL_PART_REAL_AGENT PART, SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUB_PART       WHERE ((B.IS_LOCAL='FALSE' AND B.TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID)       OR (B.IS_LOCAL='TRUE' AND B.DATA_TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID))       AND SUB_PART.TENANT_ID = EFFECTIVE_TENANT_ID()       AND PART.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_ind_subpartitions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_IND_SUBPARTITIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_IND_SUBPARTITIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(   SELECT     CAST(B.INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,     CAST(SUB_PART.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(SUB_PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,     CAST(LENGTH(SUB_PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,     SUB_PART.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST(NULL AS VARCHAR2(3)) AS INTERVAL,     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED     FROM(       SELECT       A.INDEX_OWNER AS INDEX_OWNER,       A.INDEX_NAME AS INDEX_NAME,       A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID*/       A.DATA_TABLE_ID AS DATA_TABLE_ID,       A.PART_LEVEL AS INDEX_TABLE_PART_LEVEL,       TB2.PART_LEVEL AS DATA_TABLE_PART_LEVEL,       /* CAST(CASE WHEN A.PART_LEVEL=2 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL */       CAST('TRUE' AS VARCHAR2(5)) AS IS_LOCAL       FROM(         SELECT           DB.DATABASE_NAME AS INDEX_OWNER,           SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,           TB1.TABLE_ID AS INDEX_TABLE_ID,           TB1.DATA_TABLE_ID AS DATA_TABLE_ID,           TB1.INDEX_TYPE AS INDEX_TYPE,           TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/         FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         WHERE DB.DATABASE_NAME!='__recyclebin'           AND TB1.DATABASE_ID=DB.DATABASE_ID           AND TB1.TABLE_TYPE=5           AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()           AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (TB1.DATABASE_ID = USERENV('SCHEMAID')                  OR USER_CAN_ACCESS_OBJ(1, TB1.DATA_TABLE_ID, TB1.DATABASE_ID) = 1)        ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID             AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()       WHERE A.PART_LEVEL=2 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=2)       ) B, SYS.ALL_VIRTUAL_PART_REAL_AGENT PART, SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUB_PART       WHERE ((B.IS_LOCAL='FALSE' AND B.TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID)               OR (B.IS_LOCAL='TRUE' AND B.DATA_TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID))           AND SUB_PART.TENANT_ID = EFFECTIVE_TENANT_ID()           AND PART.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_ind_subpartitions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_IND_SUBPARTITIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_IND_SUBPARTITIONS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(   SELECT     CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(PART.PART_NAME AS VARCHAR2(30)) AS PARTITION_NAME,     CAST(SUB_PART.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(SUB_PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,     CAST(LENGTH(SUB_PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,     SUB_PART.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST(NULL AS VARCHAR2(3)) AS INTERVAL,     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED     FROM(       SELECT       A.INDEX_OWNER AS INDEX_OWNER,       A.INDEX_NAME AS INDEX_NAME,       A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID */       A.DATA_TABLE_ID AS DATA_TABLE_ID,       A.PART_LEVEL AS INDEX_TABLE_PART_LEVEL,       TB2.PART_LEVEL AS DATA_TABLE_PART_LEVEL,       /* CAST(CASE WHEN A.PART_LEVEL=2 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL */       CAST('TRUE' AS VARCHAR2(5)) AS IS_LOCAL       FROM(         SELECT           DB.DATABASE_NAME AS INDEX_OWNER,           SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,           TB1.TABLE_ID AS INDEX_TABLE_ID,           TB1.DATA_TABLE_ID AS DATA_TABLE_ID,           TB1.INDEX_TYPE AS INDEX_TYPE,           TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/         FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         WHERE DB.DATABASE_NAME!='__recyclebin' AND TB1.DATABASE_ID=DB.DATABASE_ID AND TB1.TABLE_TYPE=5             AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()             AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()       ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID             AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()       WHERE A.PART_LEVEL=2 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=2)       ) B, SYS.ALL_VIRTUAL_PART_REAL_AGENT PART, SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUB_PART       WHERE ((B.IS_LOCAL='FALSE' AND B.TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID)               OR (B.IS_LOCAL='TRUE' AND B.DATA_TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID))           AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()           AND SUB_PART.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_roles_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_ROLES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_ROLES_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(SELECT CAST(USER_NAME AS VARCHAR2(30)) ROLE,      CAST('NO' AS VARCHAR2(8)) PASSWORD_REQUIRED,      CAST('NONE' AS VARCHAR2(11)) AUTHENTICATION_TYPE      FROM   SYS.ALL_VIRTUAL_USER_REAL_AGENT U      WHERE             U.TYPE = 1 AND U.TENANT_ID = EFFECTIVE_TENANT_ID())__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_role_privs_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_ROLE_PRIVS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_ROLE_PRIVS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(SELECT CAST(A.USER_NAME AS VARCHAR2(30)) GRANTEE,      CAST(B.USER_NAME AS VARCHAR2(30)) GRANTED_ROLE,      DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') AS ADMIN_OPTION ,      DECODE(R.DISABLE_FLAG, 0, 'YES', 1, 'NO', '') AS DEFAULT_ROLE      FROM SYS.ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_AGENT R,      SYS.ALL_VIRTUAL_USER_REAL_AGENT A,      SYS.ALL_VIRTUAL_USER_REAL_AGENT B      WHERE R.GRANTEE_ID = A.USER_ID      AND R.ROLE_ID = B.USER_ID      AND B.TYPE = 1      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()      AND B.TENANT_ID = EFFECTIVE_TENANT_ID())__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_role_privs_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_ROLE_PRIVS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_ROLE_PRIVS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(SELECT CAST(A.USER_NAME AS VARCHAR2(30)) GRANTEE,      CAST(B.USER_NAME AS VARCHAR2(30)) GRANTED_ROLE,      DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') AS ADMIN_OPTION ,      DECODE(R.DISABLE_FLAG, 0, 'YES', 1, 'NO', '') AS DEFAULT_ROLE      FROM SYS.ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_AGENT R,      SYS.ALL_VIRTUAL_USER_REAL_AGENT A,      SYS.ALL_VIRTUAL_USER_REAL_AGENT B      WHERE R.GRANTEE_ID = A.USER_ID      AND R.ROLE_ID = B.USER_ID      AND B.TYPE = 1      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()      AND A.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER'))__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_tab_privs_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_TAB_PRIVS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_PRIVS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(     SELECT C.USER_NAME AS GRANTEE,        E.DATABASE_NAME AS OWNER,        CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),                         D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,        B.USER_NAME AS GRANTOR,        CAST (DECODE(A.PRIV_ID, 1, 'ALTER',                          2, 'AUDIT',                          3, 'COMMENT',                          4, 'DELETE',                          5, 'GRANT',                          6, 'INDEX',                          7, 'INSERT',                          8, 'LOCK',                          9, 'RENAME',                          10, 'SELECT',                          11, 'UPDATE',                          12, 'REFERENCES',                          13, 'EXECUTE',                          14, 'CREATE',                          15, 'FLASHBACK',                          16, 'READ',                          17, 'WRITE',                          'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,        DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,        CAST('NO' AS VARCHAR(10)) AS  HIERARCHY       FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,               SYS.ALL_VIRTUAL_USER_REAL_AGENT B,               SYS.ALL_VIRTUAL_USER_REAL_AGENT C,               (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID                  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()               ) D,               SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E       WHERE A.GRANTOR_ID = B.USER_ID         AND A.GRANTEE_ID = C.USER_ID         AND A.COL_ID = 65535         AND A.OBJ_ID = D.TABLE_ID         AND  D.DATABASE_ID=E.DATABASE_ID         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.DATABASE_NAME != '__recyclebin' )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_tab_privs_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TAB_PRIVS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TAB_PRIVS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(     SELECT B.USER_NAME AS GRANTOR,        C.USER_NAME AS GRANTEE,        E.DATABASE_NAME AS TABLE_SCHEMA,        CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),                         D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,        CAST (DECODE(A.PRIV_ID, 1, 'ALTER',                          2, 'AUDIT',                          3, 'COMMENT',                          4, 'DELETE',                          5, 'GRANT',                          6, 'INDEX',                          7, 'INSERT',                          8, 'LOCK',                          9, 'RENAME',                          10, 'SELECT',                          11, 'UPDATE',                          12, 'REFERENCES',                          13, 'EXECUTE',                          14, 'CREATE',                          15, 'FLASHBACK',                          16, 'READ',                          17, 'WRITE',                          'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,        DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,        CAST('NO' AS VARCHAR(10)) AS  HIERARCHY       FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,               SYS.ALL_VIRTUAL_USER_REAL_AGENT B,               SYS.ALL_VIRTUAL_USER_REAL_AGENT C,               (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID                  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()               ) D,               SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E       WHERE A.GRANTOR_ID = B.USER_ID         AND A.GRANTEE_ID = C.USER_ID         AND A.COL_ID = 65535         AND A.OBJ_ID = D.TABLE_ID         AND D.DATABASE_ID=E.DATABASE_ID         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.DATABASE_NAME != '__recyclebin'         AND (C.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')              OR E.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')              OR B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')) )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_tab_privs_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_TAB_PRIVS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TAB_PRIVS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(     SELECT C.USER_NAME AS GRANTEE,        E.DATABASE_NAME AS OWNER,        CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),                         D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,        B.USER_NAME AS GRANTOR,        CAST (DECODE(A.PRIV_ID, 1, 'ALTER',                          2, 'AUDIT',                          3, 'COMMENT',                          4, 'DELETE',                          5, 'GRANT',                          6, 'INDEX',                          7, 'INSERT',                          8, 'LOCK',                          9, 'RENAME',                          10, 'SELECT',                          11, 'UPDATE',                          12, 'REFERENCES',                          13, 'EXECUTE',                          14, 'CREATE',                          15, 'FLASHBACK',                          16, 'READ',                          17, 'WRITE',                          'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,        DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,        CAST('NO' AS VARCHAR(10)) AS  HIERARCHY       FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,               SYS.ALL_VIRTUAL_USER_REAL_AGENT B,               SYS.ALL_VIRTUAL_USER_REAL_AGENT C,               (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID                  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT                WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID                   FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT                WHERE TENANT_ID = EFFECTIVE_TENANT_ID()               ) D,               SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E       WHERE A.GRANTOR_ID = B.USER_ID         AND A.GRANTEE_ID = C.USER_ID         AND A.COL_ID = 65535         AND A.OBJ_ID = D.TABLE_ID         AND D.DATABASE_ID=E.DATABASE_ID         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.DATABASE_NAME != '__recyclebin'         AND (SYS_CONTEXT('USERENV','CURRENT_USER') IN (C.USER_NAME, B.USER_NAME)              OR E.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')) )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_sys_privs_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_SYS_PRIVS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SYS_PRIVS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema
                    .set_view_definition(R"__(     SELECT B.USER_NAME AS GRANTEE,            CAST (DECODE(A.PRIV_ID,                 1, 'CREATE SESSION',                 2, 'EXEMPT REDACT POLICY',                 3, 'SYSDBA',                 4, 'SYSOPER',                 5, 'SYSBACKUP',                 6, 'CREATE TABLE',                 7, 'CREATE ANY TABLE',                 8, 'ALTER ANY TABLE',                 9, 'BACKUP ANY TABLE',                 10, 'DROP ANY TABLE',                 11, 'LOCK ANY TABLE',                 12, 'COMMENT ANY TABLE',                 13, 'SELECT ANY TABLE',                 14, 'INSERT ANY TABLE',                 15, 'UPDATE ANY TABLE',                 16, 'DELETE ANY TABLE',                 17, 'FLASHBACK ANY TABLE',                 18, 'CREATE ROLE',                 19, 'DROP ANY ROLE',                 20, 'GRANT ANY ROLE',                 21, 'ALTER ANY ROLE',                 22, 'AUDIT ANY',                 23, 'GRANT ANY PRIVILEGE',                 24, 'GRANT ANY OBJECT PRIVILEGE',                 25, 'CREATE ANY INDEX',                 26, 'ALTER ANY INDEX',                 27, 'DROP ANY INDEX',                 28, 'CREATE ANY VIEW',                 29, 'DROP ANY VIEW',                 30, 'CREATE VIEW',                 31, 'SELECT ANY DICTIONARY',                 32, 'CREATE PROCEDURE',                 33, 'CREATE ANY PROCEDURE',                 34, 'ALTER ANY PROCEDURE',                 35, 'DROP ANY PROCEDURE',                 36, 'EXECUTE ANY PROCEDURE',                 37, 'CREATE SYNONYM',                 38, 'CREATE ANY SYNONYM',                 39, 'DROP ANY SYNONYM',                 40, 'CREATE PUBLIC SYNONYM',                 41, 'DROP PUBLIC SYNONYM',                 42, 'CREATE SEQUENCE',                 43, 'CREATE ANY SEQUENCE',                 44, 'ALTER ANY SEQUENCE',                 45, 'DROP ANY SEQUENCE',                 46, 'SELECT ANY SEQUENCE',                 47, 'CREATE TRIGGER',                 48, 'CREATE ANY TRIGGER',                 49, 'ALTER ANY TRIGGER',                 50, 'DROP ANY TRIGGER',                 51, 'CREATE PROFILE',                 52, 'ALTER PROFILE',                 53, 'DROP PROFILE',                 54, 'CREATE USER',                 55, 'BECOME USER',                 56, 'ALTER USER',                 57, 'DROP USER',                 58, 'CREATE TYPE',                 59, 'CREATE ANY TYPE',                 60, 'ALTER ANY TYPE',                 61, 'DROP ANY TYPE',                 62, 'EXECUTE ANY TYPE',                 63, 'UNDER ANY TYPE',                 64, 'PURGE DBA_RECYCLEBIN',                 65, 'CREATE ANY OUTLINE',                 66, 'ALTER ANY OUTLINE',                 67, 'DROP ANY OUTLINE',                 68, 'SYSKM',                 69, 'CREATE TABLESPACE',                 70, 'ALTER TABLESPACE',                 71, 'DROP TABLESPACE',                 72, 'SHOW PROCESS',                 73, 'ALTER SYSTEM',                 74, 'CREATE DATABASE LINK',                 75, 'CREATE PUBLIC DATABASE LINK',                 76, 'DROP DATABASE LINK',                 77, 'ALTER SESSION',                 78, 'ALTER DATABASE',                 'OTHER') AS VARCHAR(40)) AS PRIVILEGE,         CASE PRIV_OPTION           WHEN 0 THEN 'NO'           ELSE 'YES'           END AS ADMIN_OPTION     FROM SYS.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT A,           SYS.ALL_VIRTUAL_USER_REAL_AGENT B     WHERE A.GRANTEE_ID = B.USER_ID     AND A.TENANT_ID = EFFECTIVE_TENANT_ID()     AND B.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_sys_privs_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_SYS_PRIVS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SYS_PRIVS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(     SELECT B.USER_NAME AS USERNAME,            CAST (DECODE(A.PRIV_ID,                 1, 'CREATE SESSION',                 2, 'EXEMPT REDACT POLICY',                 3, 'SYSDBA',                 4, 'SYSOPER',                 5, 'SYSBACKUP',                 6, 'CREATE TABLE',                 7, 'CREATE ANY TABLE',                 8, 'ALTER ANY TABLE',                 9, 'BACKUP ANY TABLE',                 10, 'DROP ANY TABLE',                 11, 'LOCK ANY TABLE',                 12, 'COMMENT ANY TABLE',                 13, 'SELECT ANY TABLE',                 14, 'INSERT ANY TABLE',                 15, 'UPDATE ANY TABLE',                 16, 'DELETE ANY TABLE',                 17, 'FLASHBACK ANY TABLE',                 18, 'CREATE ROLE',                 19, 'DROP ANY ROLE',                 20, 'GRANT ANY ROLE',                 21, 'ALTER ANY ROLE',                 22, 'AUDIT ANY',                 23, 'GRANT ANY PRIVILEGE',                 24, 'GRANT ANY OBJECT PRIVILEGE',                 25, 'CREATE ANY INDEX',                 26, 'ALTER ANY INDEX',                 27, 'DROP ANY INDEX',                 28, 'CREATE ANY VIEW',                 29, 'DROP ANY VIEW',                 30, 'CREATE VIEW',                 31, 'SELECT ANY DICTIONARY',                 32, 'CREATE PROCEDURE',                 33, 'CREATE ANY PROCEDURE',                 34, 'ALTER ANY PROCEDURE',                 35, 'DROP ANY PROCEDURE',                 36, 'EXECUTE ANY PROCEDURE',                 37, 'CREATE SYNONYM',                 38, 'CREATE ANY SYNONYM',                 39, 'DROP ANY SYNONYM',                 40, 'CREATE PUBLIC SYNONYM',                 41, 'DROP PUBLIC SYNONYM',                 42, 'CREATE SEQUENCE',                 43, 'CREATE ANY SEQUENCE',                 44, 'ALTER ANY SEQUENCE',                 45, 'DROP ANY SEQUENCE',                 46, 'SELECT ANY SEQUENCE',                 47, 'CREATE TRIGGER',                 48, 'CREATE ANY TRIGGER',                 49, 'ALTER ANY TRIGGER',                 50, 'DROP ANY TRIGGER',                 51, 'CREATE PROFILE',                 52, 'ALTER PROFILE',                 53, 'DROP PROFILE',                 54, 'CREATE USER',                 55, 'BECOME USER',                 56, 'ALTER USER',                 57, 'DROP USER',                 58, 'CREATE TYPE',                 59, 'CREATE ANY TYPE',                 60, 'ALTER ANY TYPE',                 61, 'DROP ANY TYPE',                 62, 'EXECUTE ANY TYPE',                 63, 'UNDER ANY TYPE',                 64, 'PURGE DBA_RECYCLEBIN',                 65, 'CREATE ANY OUTLINE',                 66, 'ALTER ANY OUTLINE',                 67, 'DROP ANY OUTLINE',                 68, 'SYSKM',                 69, 'CREATE TABLESPACE',                 70, 'ALTER TABLESPACE',                 71, 'DROP TABLESPACE',                 72, 'SHOW PROCESS',                 73, 'ALTER SYSTEM',                 74, 'CREATE DATABASE LINK',                 75, 'CREATE PUBLIC DATABASE LINK',                 76, 'DROP DATABASE LINK',                 77, 'ALTER SESSION',                 78, 'ALTER DATABASE',                 'OTHER') AS VARCHAR(40)) AS PRIVILEGE,         CASE PRIV_OPTION           WHEN 0 THEN 'NO'           ELSE 'YES'           END AS ADMIN_OPTION     FROM SYS.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT A,           SYS.ALL_VIRTUAL_USER_REAL_AGENT B     WHERE B.TYPE = 0 AND           A.GRANTEE_ID =B.USER_ID           AND A.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dba_col_privs_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_COL_PRIVS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_COL_PRIVS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(       SELECT         u_grantee.user_name as GRANTEE,         db.database_name as OWNER,         decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,         c.column_name as COLUMN_NAME,         u_grantor.user_name as GRANTOR,         cast (decode(auth.priv_id, 1, 'ALTER',                              2, 'AUDIT',                              3, 'COMMENT',                              4, 'DELETE',                              5, 'GRANT',                              6, 'INDEX',                              7, 'INSERT',                              8, 'LOCK',                              9, 'RENAME',                              10, 'SELECT',                              11, 'UPDATE',                              12, 'REFERENCES',                              13, 'EXECUTE',                              14, 'CREATE',                              15, 'FLASHBACK',                              16, 'READ',                              17, 'WRITE',                              'OTHERS') as varchar(40)) as PRIVILEGE,         decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE       FROM         sys.all_virtual_objauth_agent auth,         sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c,         sys.ALL_VIRTUAL_TABLE_REAL_AGENT t,         sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db,         sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,         sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantee       WHERE         auth.col_id = c.column_id and         auth.obj_id = t.table_id and         auth.objtype = 1 and         auth.obj_id = c.table_id and         db.database_id = t.database_id and         u_grantor.user_id = auth.grantor_id and         u_grantee.user_id = auth.grantee_id         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID() 	      AND c.column_id != 65535   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::user_col_privs_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_COL_PRIVS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_COL_PRIVS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(       SELECT         u_grantee.user_name as GRANTEE,         db.database_name as OWNER,         decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,         c.column_name as COLUMN_NAME,         u_grantor.user_name as GRANTOR,         cast (decode(auth.priv_id, 1, 'ALTER',                              2, 'AUDIT',                              3, 'COMMENT',                              4, 'DELETE',                              5, 'GRANT',                              6, 'INDEX',                              7, 'INSERT',                              8, 'LOCK',                              9, 'RENAME',                              10, 'SELECT',                              11, 'UPDATE',                              12, 'REFERENCES',                              13, 'EXECUTE',                              14, 'CREATE',                              15, 'FLASHBACK',                              16, 'READ',                              17, 'WRITE',                              'OTHERS') as varchar(40)) as PRIVILEGE,         decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE       FROM         sys.all_virtual_objauth_agent auth,         sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c,         sys.ALL_VIRTUAL_TABLE_REAL_AGENT t,         sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db,         sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,         sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantee       WHERE         auth.col_id = c.column_id and         auth.obj_id = t.table_id and         auth.objtype = 1 and         auth.obj_id = c.table_id and         db.database_id = t.database_id and         u_grantor.user_id = auth.grantor_id and         u_grantee.user_id = auth.grantee_id         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()         AND c.column_id != 65535 and 	      (db.database_name = SYS_CONTEXT('USERENV','CURRENT_USER') or          u_grantee.user_name = SYS_CONTEXT('USERENV','CURRENT_USER') or          u_grantor.user_name = SYS_CONTEXT('USERENV','CURRENT_USER')         )   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_col_privs_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_COL_PRIVS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COL_PRIVS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(       SELECT 	u_grantor.user_name as GRANTOR,         u_grantee.user_name as GRANTEE, 	db.database_name as OWNER, 	decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,         c.column_name as COLUMN_NAME,         cast (decode(auth.priv_id, 1, 'ALTER',                              2, 'AUDIT',                              3, 'COMMENT',                              4, 'DELETE',                              5, 'GRANT',                              6, 'INDEX',                              7, 'INSERT',                              8, 'LOCK',                              9, 'RENAME',                              10, 'SELECT',                              11, 'UPDATE',                              12, 'REFERENCES',                              13, 'EXECUTE',                              14, 'CREATE',                              15, 'FLASHBACK',                              16, 'READ',                              17, 'WRITE',                              'OTHERS') as varchar(40)) as PRIVILEGE,         decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE       FROM         sys.all_virtual_objauth_agent auth,         sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c,         sys.ALL_VIRTUAL_TABLE_REAL_AGENT t,         sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db,         sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,         sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantee       WHERE         auth.col_id = c.column_id and         auth.obj_id = t.table_id and         auth.objtype = 1 and         auth.obj_id = c.table_id and         db.database_id = t.database_id and         u_grantor.user_id = auth.grantor_id and         u_grantee.user_id = auth.grantee_id         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()         AND c.column_id != 65535   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::role_tab_privs_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ROLE_TAB_PRIVS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ROLE_TAB_PRIVS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(       SELECT         U_GRANTEE.USER_NAME AS ROLE,         DB.DATABASE_NAME AS OWNER,           CAST (DECODE(AUTH.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),                         T.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,         C.COLUMN_NAME AS COLUMN_NAME,         CAST (DECODE(AUTH.PRIV_ID, 1, 'ALTER',                              2, 'AUDIT',                              3, 'COMMENT',                              4, 'DELETE',                              5, 'GRANT',                              6, 'INDEX',                              7, 'INSERT',                              8, 'LOCK',                              9, 'RENAME',                              10, 'SELECT',                              11, 'UPDATE',                              12, 'REFERENCES',                              13, 'EXECUTE',                              14, 'CREATE',                              15, 'FLASHBACK',                              16, 'READ',                              17, 'WRITE',                              'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,         DECODE(AUTH.PRIV_OPTION, 0, 'NO', 1, 'YES', '') AS GRANTABLE       FROM         (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) AUTH         LEFT JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C           ON AUTH.COL_ID = C.COLUMN_ID AND AUTH.OBJ_ID = C.TABLE_ID             AND C.TENANT_ID = EFFECTIVE_TENANT_ID(),         (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID             FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT             WHERE TENANT_ID = EFFECTIVE_TENANT_ID()          UNION ALL          SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID            FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT            WHERE TENANT_ID = EFFECTIVE_TENANT_ID()          UNION ALL          SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID            FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT            WHERE TENANT_ID = EFFECTIVE_TENANT_ID()          UNION ALL          SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID            FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT            WHERE TENANT_ID = EFFECTIVE_TENANT_ID()         ) T,         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB,         SYS.ALL_VIRTUAL_USER_REAL_AGENT U_GRANTEE       WHERE         AUTH.OBJ_ID = T.TABLE_ID          AND DB.DATABASE_ID = T.DATABASE_ID          AND U_GRANTEE.USER_ID = AUTH.GRANTEE_ID          AND U_GRANTEE.TYPE = 1          AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()          AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::role_sys_privs_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ROLE_SYS_PRIVS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ROLE_SYS_PRIVS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema
                    .set_view_definition(R"__(       select 	u.user_name as ROLE,         CAST (DECODE(AUTH.PRIV_ID,                 1, 'CREATE SESSION',                 2, 'EXEMPT REDACT POLICY',                 3, 'SYSDBA',                 4, 'SYSOPER',                 5, 'SYSBACKUP',                 6, 'CREATE TABLE',                 7, 'CREATE ANY TABLE',                 8, 'ALTER ANY TABLE',                 9, 'BACKUP ANY TABLE',                 10, 'DROP ANY TABLE',                 11, 'LOCK ANY TABLE',                 12, 'COMMENT ANY TABLE',                 13, 'SELECT ANY TABLE',                 14, 'INSERT ANY TABLE',                 15, 'UPDATE ANY TABLE',                 16, 'DELETE ANY TABLE',                 17, 'FLASHBACK ANY TABLE',                 18, 'CREATE ROLE',                 19, 'DROP ANY ROLE',                 20, 'GRANT ANY ROLE',                 21, 'ALTER ANY ROLE',                 22, 'AUDIT ANY',                 23, 'GRANT ANY PRIVILEGE',                 24, 'GRANT ANY OBJECT PRIVILEGE',                 25, 'CREATE ANY INDEX',                 26, 'ALTER ANY INDEX',                 27, 'DROP ANY INDEX',                 28, 'CREATE ANY VIEW',                 29, 'DROP ANY VIEW',                 30, 'CREATE VIEW',                 31, 'SELECT ANY DICTIONARY',                 32, 'CREATE PROCEDURE',                 33, 'CREATE ANY PROCEDURE',                 34, 'ALTER ANY PROCEDURE',                 35, 'DROP ANY PROCEDURE',                 36, 'EXECUTE ANY PROCEDURE',                 37, 'CREATE SYNONYM',                 38, 'CREATE ANY SYNONYM',                 39, 'DROP ANY SYNONYM',                 40, 'CREATE PUBLIC SYNONYM',                 41, 'DROP PUBLIC SYNONYM',                 42, 'CREATE SEQUENCE',                 43, 'CREATE ANY SEQUENCE',                 44, 'ALTER ANY SEQUENCE',                 45, 'DROP ANY SEQUENCE',                 46, 'SELECT ANY SEQUENCE',                 47, 'CREATE TRIGGER',                 48, 'CREATE ANY TRIGGER',                 49, 'ALTER ANY TRIGGER',                 50, 'DROP ANY TRIGGER',                 51, 'CREATE PROFILE',                 52, 'ALTER PROFILE',                 53, 'DROP PROFILE',                 54, 'CREATE USER',                 55, 'BECOME USER',                 56, 'ALTER USER',                 57, 'DROP USER',                 58, 'CREATE TYPE',                 59, 'CREATE ANY TYPE',                 60, 'ALTER ANY TYPE',                 61, 'DROP ANY TYPE',                 62, 'EXECUTE ANY TYPE',                 63, 'UNDER ANY TYPE',                 64, 'PURGE DBA_RECYCLEBIN',                 65, 'CREATE ANY OUTLINE',                 66, 'ALTER ANY OUTLINE',                 67, 'DROP ANY OUTLINE',                 68, 'SYSKM',                 69, 'CREATE TABLESPACE',                 70, 'ALTER TABLESPACE',                 71, 'DROP TABLESPACE',                 72, 'SHOW PROCESS',                 73, 'ALTER SYSTEM',                 74, 'CREATE DATABASE LINK',                 75, 'CREATE PUBLIC DATABASE LINK',                 76, 'DROP DATABASE LINK',                 77, 'ALTER SESSION',                 78, 'ALTER DATABASE',                 'OTHER') AS VARCHAR(40)) AS PRIVILEGE ,        	decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as ADMIN_OPTION       FROM 	sys.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT auth, 	sys.ALL_VIRTUAL_USER_REAL_AGENT u       WHERE 	auth.grantee_id = u.user_id and   u.type = 1   AND U.TENANT_ID = EFFECTIVE_TENANT_ID()   AND AUTH.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::role_role_privs_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ROLE_ROLE_PRIVS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ROLE_ROLE_PRIVS_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(      select 	u_role.user_name as ROLE,         u_grantee.user_name as GRANTED_ROLE,         DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') as ADMIN_OPTION      FROM 	sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantee, 	sys.ALL_VIRTUAL_USER_REAL_AGENT u_role, 	(select * from sys.all_virtual_tenant_role_grantee_map_agent 	 connect by prior role_id = grantee_id                    and bitand(role_id, power(2,40) - 1) != 9 	 start with grantee_id = uid               and bitand(role_id, power(2,40) - 1) != 9 ) r      WHERE 	r.grantee_id = u_role.user_id and 	r.role_id = u_grantee.user_id and 	u_role.type = 1 and   u_grantee.type = 1   AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()   AND U_ROLE.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::dictionary_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DICTIONARY_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DICTIONARY_ORA_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(
            R"__(     SELECT       CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,       CAST(CASE TABLE_NAME         WHEN 'DBA_COL_PRIVS' THEN 'All grants on columns in the database'         WHEN 'USER_COL_PRIVS' THEN 'Grants on columns for which the user is the owner, grantor or grantee'         WHEN 'ALL_COL_PRIVS' THEN 'Grants on columns for which the user is the grantor, grantee, owner, or an enabled role or PUBLIC is the grantee'         WHEN 'ROLE_TAB_PRIVS' THEN 'Table privileges granted to roles'         WHEN 'ROLE_SYS_PRIVS' THEN 'System privileges granted to roles'         WHEN 'ROLE_ROLE_PRIVS' THEN 'Roles which are granted to roles'         WHEN 'DBA_SYNONYMS' THEN 'All synonyms in the database'         WHEN 'DBA_OBJECTS' THEN 'All objects in the database'         WHEN 'ALL_OBJECTS' THEN 'Objects accessible to the user'         WHEN 'USER_OBJECTS' THEN 'Objects owned by the user'         WHEN 'DBA_SEQUENCES' THEN 'Description of all SEQUENCEs in the database'         WHEN 'ALL_SEQUENCES' THEN 'Description of SEQUENCEs accessible to the user'         WHEN 'USER_SEQUENCES' THEN 'Description of the user''s own SEQUENCEs'         WHEN 'DBA_USERS' THEN 'Information about all users of the database'         WHEN 'ALL_USERS' THEN 'Information about all users of the database'         WHEN 'ALL_SYNONYMS' THEN 'All synonyms for base objects accessible to the user and session'         WHEN 'USER_SYNONYMS' THEN 'The user''s private synonyms'         WHEN 'DBA_IND_COLUMNS' THEN 'COLUMNs comprising INDEXes on all TABLEs and CLUSTERs'         WHEN 'ALL_IND_COLUMNS' THEN 'COLUMNs comprising INDEXes on accessible TABLES'         WHEN 'USER_IND_COLUMNS' THEN 'COLUMNs comprising user''s INDEXes and INDEXes on user''s TABLES'         WHEN 'DBA_CONSTRAINTS' THEN 'Constraint definitions on all tables'         WHEN 'ALL_CONSTRAINTS' THEN 'Constraint definitions on accessible tables'         WHEN 'USER_CONSTRAINTS' THEN 'Constraint definitions on user''s own tables'         WHEN 'ALL_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'DBA_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'USER_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'ALL_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'DBA_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'USER_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'ALL_TABLES' THEN 'Description of relational tables accessible to the user'         WHEN 'DBA_TABLES' THEN 'Description of all relational tables in the database'         WHEN 'USER_TABLES' THEN 'Description of the user''s own relational tables'         WHEN 'DBA_TAB_COMMENTS' THEN 'Comments on all tables and views in the database'         WHEN 'ALL_TAB_COMMENTS' THEN 'Comments on tables and views accessible to the user'         WHEN 'USER_TAB_COMMENTS' THEN 'Comments on the tables and views owned by the user'         WHEN 'DBA_COL_COMMENTS' THEN 'Comments on columns of all tables and views'         WHEN 'ALL_COL_COMMENTS' THEN 'Comments on columns of accessible tables and views'         WHEN 'USER_COL_COMMENTS' THEN 'Comments on columns of user''s tables and views'         WHEN 'DBA_INDEXES' THEN 'Description for all indexes in the database'         WHEN 'ALL_INDEXES' THEN 'Descriptions of indexes on tables accessible to the user'         WHEN 'USER_INDEXES' THEN 'Description of the user''s own indexes'         WHEN 'DBA_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'         WHEN 'ALL_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'         WHEN 'USER_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'         WHEN 'USER_SEGMENTS' THEN 'Storage allocated for all database segments'         WHEN 'DBA_SEGMENTS' THEN 'Storage allocated for all database segments'         WHEN 'DBA_TYPES' THEN 'Description of all types in the database'         WHEN 'ALL_TYPES' THEN 'Description of types accessible to the user'         WHEN 'USER_TYPES' THEN 'Description of the user''s own types'         WHEN 'DBA_TYPE_ATTRS' THEN 'Description of attributes of all types in the database'         WHEN 'ALL_TYPE_ATTRS' THEN 'Description of attributes of types accessible to the user'         WHEN 'USER_TYPE_ATTRS' THEN 'Description of attributes of the user''s own types'         WHEN 'DBA_COLL_TYPES' THEN 'Description of all named collection types in the database'         WHEN 'ALL_COLL_TYPES' THEN 'Description of named collection types accessible to the user'         WHEN 'USER_COLL_TYPES' THEN 'Description of the user''s own named collection types'         WHEN 'DBA_PROCEDURES' THEN 'Description of the dba functions/procedures/packages/types/triggers'         WHEN 'DBA_ARGUMENTS' THEN 'All arguments for objects in the database'         WHEN 'DBA_SOURCE' THEN 'Source of all stored objects in the database'         WHEN 'ALL_PROCEDURES' THEN 'Functions/procedures/packages/types/triggers available to the user'         WHEN 'ALL_ARGUMENTS' THEN 'Arguments in object accessible to the user'         WHEN 'ALL_SOURCE' THEN 'Current source on stored objects that user is allowed to create'         WHEN 'USER_PROCEDURES' THEN 'Description of the user functions/procedures/packages/types/triggers'         WHEN 'USER_ARGUMENTS' THEN 'Arguments in object accessible to the user'         WHEN 'USER_SOURCE' THEN 'Source of stored objects accessible to the user'         WHEN 'ALL_ALL_TABLES' THEN 'Description of all object and relational tables accessible to the user'         WHEN 'DBA_ALL_TABLES' THEN 'Description of all object and relational tables in the database'         WHEN 'USER_ALL_TABLES' THEN 'Description of all object and relational tables owned by the user''s'         WHEN 'DBA_PROFILES' THEN 'Display all profiles and their limits'         WHEN 'ALL_MVIEW_COMMENTS' THEN 'Comments on materialized views accessible to the user'         WHEN 'USER_MVIEW_COMMENTS' THEN 'Comments on materialized views owned by the user'         WHEN 'DBA_MVIEW_COMMENTS' THEN 'Comments on all materialized views in the database'         WHEN 'ALL_SCHEDULER_PROGRAM_ARGS' THEN 'All arguments of all scheduler programs visible to the user'         WHEN 'ALL_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'         WHEN 'DBA_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'         WHEN 'USER_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'         WHEN 'DBA_VIEWS' THEN 'Description of all views in the database'         WHEN 'ALL_VIEWS' THEN 'Description of views accessible to the user'         WHEN 'USER_VIEWS' THEN 'Description of the user''s own views'         WHEN 'ALL_ERRORS' THEN 'Current errors on stored objects that user is allowed to create'         WHEN 'USER_ERRORS' THEN 'Current errors on stored objects owned by the user'         WHEN 'ALL_TYPE_METHODS' THEN 'Description of methods of types accessible to the user'         WHEN 'DBA_TYPE_METHODS' THEN 'Description of methods of all types in the database'         WHEN 'USER_TYPE_METHODS' THEN 'Description of methods of the user''s own types'         WHEN 'ALL_METHOD_PARAMS' THEN 'Description of method parameters of types accessible to the user'         WHEN 'DBA_METHOD_PARAMS' THEN 'Description of method parameters of all types in the database'         WHEN 'USER_TABLESPACES' THEN 'Description of accessible tablespaces'         WHEN 'DBA_IND_EXPRESSIONS' THEN 'FUNCTIONAL INDEX EXPRESSIONs on all TABLES and CLUSTERS'         WHEN 'ALL_IND_EXPRESSIONS' THEN 'FUNCTIONAL INDEX EXPRESSIONs on accessible TABLES'         WHEN 'DBA_ROLE_PRIVS' THEN 'Roles granted to users and roles'         WHEN 'USER_ROLE_PRIVS' THEN 'Roles granted to current user'         WHEN 'DBA_TAB_PRIVS' THEN 'All grants on objects in the database'         WHEN 'ALL_TAB_PRIVS' THEN 'Grants on objects for which the user is the grantor, grantee, owner,'         WHEN 'DBA_SYS_PRIVS' THEN 'System privileges granted to users and roles'         WHEN 'USER_SYS_PRIVS' THEN 'System privileges granted to current user'         WHEN 'AUDIT_ACTIONS' THEN 'Description table for audit trail action type codes.  Maps action type numbers to action type names'         WHEN 'ALL_DEF_AUDIT_OPTS' THEN 'Auditing options for newly created objects'         WHEN 'DBA_STMT_AUDIT_OPTS' THEN 'Describes current system auditing options across the system and by user'         WHEN 'DBA_OBJ_AUDIT_OPTS' THEN 'Auditing options for all tables and views with atleast one option set'         WHEN 'DBA_AUDIT_TRAIL' THEN 'All audit trail entries'         WHEN 'USER_AUDIT_TRAIL' THEN 'Audit trail entries relevant to the user'         WHEN 'DBA_AUDIT_EXISTS' THEN 'Lists audit trail entries produced by AUDIT NOT EXISTS and AUDIT EXISTS'         WHEN 'DBA_AUDIT_STATEMENT' THEN 'Audit trail records concerning grant, revoke, audit, noaudit and alter system'         WHEN 'USER_AUDIT_STATEMENT' THEN 'Audit trail records concerning  grant, revoke, audit, noaudit and alter system'         WHEN 'DBA_AUDIT_OBJECT' THEN 'Audit trail records for statements concerning objects, specifically: table, cluster, view, index, sequence,  [public] database link, [public] synonym, procedure, trigger, rollback segment, tablespace, role, user'         WHEN 'USER_AUDIT_OBJECT' THEN 'Audit trail records for statements concerning objects, specifically: table, cluster, view, index, sequence,  [public] database link, [public] synonym, procedure, trigger, rollback segment, tablespace, role, user'         WHEN 'ALL_DEPENDENCIES' THEN 'Describes dependencies between procedures, packages,functions, package bodies, and triggers accessible to the current user,including dependencies on views created without any database links'         WHEN 'DBA_DEPENDENCIES' THEN 'Describes all dependencies in the database between procedures,packages, functions, package bodies, and triggers, including dependencies on views created without any database links'         WHEN 'USER_DEPENDENCIES' THEN 'Describes dependencies between procedures, packages, functions, package bodies, and triggers owned by the current user, including dependencies on views created without any database links'         WHEN 'GV$INSTANCE' THEN 'Synonym for GV_$INSTANCE'         WHEN 'V$INSTANCE' THEN 'Synonym for V_$INSTANCE'         WHEN 'GV$SESSION_WAIT' THEN 'Synonym for GV_$SESSION_WAIT'         WHEN 'V$SESSION_WAIT' THEN 'Synonym for V_$SESSION_WAIT'         WHEN 'GV$SESSION_WAIT_HISTORY' THEN 'Synonym for GV_$SESSION_WAIT_HISTORY'         WHEN 'V$SESSION_WAIT_HISTORY' THEN 'Synonym for V_$SESSION_WAIT_HISTORY'         WHEN 'GV$SESSTAT' THEN 'Synonym for GV_$SESSTAT'         WHEN 'V$SESSTAT' THEN 'Synonym for V_$SESSTAT'         WHEN 'GV$SYSSTAT' THEN 'Synonym for GV_$SYSSTAT'         WHEN 'V$SYSSTAT' THEN 'Synonym for V_$SYSSTAT'         WHEN 'GV$SYSTEM_EVENT' THEN 'Synonym for GV_$SYSTEM_EVENT'         WHEN 'V$SYSTEM_EVENT' THEN 'Synonym for V_$SYSTEM_EVENT'         WHEN 'NLS_SESSION_PARAMETERS' THEN 'NLS parameters of the user session'         WHEN 'NLS_DATABASE_PARAMETERS' THEN 'Permanent NLS parameters of the database'         WHEN 'V$NLS_PARAMETERS' THEN 'Synonym for V_$NLS_PARAMETERS'         WHEN 'V$VERSION' THEN 'Synonym for V_$VERSION'         WHEN 'GV$SQL_WORKAREA' THEN 'Synonym for GV_$SQL_WORKAREA'         WHEN 'V$SQL_WORKAREA' THEN 'Synonym for V_$SQL_WORKAREA'         WHEN 'GV$SQL_WORKAREA_ACTIVE' THEN 'Synonym for GV_$SQL_WORKAREA_ACTIVE'         WHEN 'V$SQL_WORKAREA_ACTIVE' THEN 'Synonym for V_$SQL_WORKAREA_ACTIVE'         WHEN 'GV$SQL_WORKAREA_HISTOGRAM' THEN 'Synonym for GV_$SQL_WORKAREA_HISTOGRAM'         WHEN 'V$SQL_WORKAREA_HISTOGRAM' THEN 'Synonym for V_$SQL_WORKAREA_HISTOGRAM'         WHEN 'DICT' THEN 'Synonym for DICTIONARY'         WHEN 'DICTIONARY' THEN 'Description of data dictionary tables and views'         WHEN 'DBA_RECYCLEBIN' THEN 'Description of the Recyclebin view accessible to the user'         WHEN 'USER_RECYCLEBIN' THEN 'User view of his recyclebin'         WHEN 'V$TENANT_PX_WORKER_STAT' THEN ''         WHEN 'GV$PS_STAT' THEN ''         WHEN 'V$PS_STAT' THEN ''         WHEN 'GV$PS_ITEM_INFO' THEN ''         WHEN 'V$PS_ITEM_INFO' THEN ''         WHEN 'GV$OB_SQL_WORKAREA_MEMORY_INFO' THEN ''         WHEN 'V$OB_SQL_WORKAREA_MEMORY_INFO' THEN ''         WHEN 'DBA_PART_KEY_COLUMNS' THEN ''         WHEN 'ALL_PART_KEY_COLUMNS' THEN ''         WHEN 'USER_PART_KEY_COLUMNS' THEN ''         WHEN 'DBA_SUBPART_KEY_COLUMNS' THEN ''         WHEN 'ALL_SUBPART_KEY_COLUMNS' THEN ''         WHEN 'USER_SUBPART_KEY_COLUMNS' THEN ''         WHEN 'ALL_TAB_PARTITIONS' THEN ''         WHEN 'ALL_TAB_SUBPARTITIONS' THEN ''         WHEN 'ALL_PART_TABLES' THEN ''         WHEN 'DBA_PART_TABLES' THEN ''         WHEN 'USER_PART_TABLES' THEN ''         WHEN 'DBA_TAB_PARTITIONS' THEN ''         WHEN 'USER_TAB_PARTITIONS' THEN ''         WHEN 'DBA_TAB_SUBPARTITIONS' THEN ''         WHEN 'USER_TAB_SUBPARTITIONS' THEN ''         WHEN 'DBA_SUBPARTITION_TEMPLATES' THEN ''         WHEN 'ALL_SUBPARTITION_TEMPLATES' THEN ''         WHEN 'USER_SUBPARTITION_TEMPLATES' THEN ''         WHEN 'DBA_PART_INDEXES' THEN ''         WHEN 'ALL_PART_INDEXES' THEN ''         WHEN 'USER_PART_INDEXES' THEN ''         WHEN 'ALL_TAB_COLS_V$' THEN ''         WHEN 'DBA_TAB_COLS_V$' THEN ''         WHEN 'USER_TAB_COLS_V$' THEN ''         WHEN 'USER_PROFILES' THEN ''         WHEN 'ALL_PROFILES' THEN ''         WHEN 'DBA_SCHEDULER_PROGRAM_ARGS' THEN ''         WHEN 'USER_SCHEDULER_PROGRAM_ARGS' THEN ''         WHEN 'USER_IND_EXPRESSIONS' THEN ''         WHEN 'DBA_ERRORS' THEN ''         WHEN 'USER_METHOD_PARAMS' THEN ''         WHEN 'DBA_TABLESPACES' THEN ''         WHEN 'ALL_IND_PARTITIONS' THEN ''         WHEN 'USER_IND_PARTITIONS' THEN ''         WHEN 'DBA_IND_PARTITIONS' THEN ''         WHEN 'DBA_IND_SUBPARTITIONS' THEN ''         WHEN 'ALL_IND_SUBPARTITIONS' THEN ''         WHEN 'USER_IND_SUBPARTITIONS' THEN ''         WHEN 'DBA_ROLES' THEN ''         WHEN 'USER_TAB_PRIVS' THEN ''         WHEN 'STMT_AUDIT_OPTION_MAP' THEN ''         WHEN 'GV$OUTLINE' THEN ''         WHEN 'GV$SQL_AUDIT' THEN ''         WHEN 'V$SQL_AUDIT' THEN ''         WHEN 'DBA_AUDIT_SESSION' THEN ''         WHEN 'USER_AUDIT_SESSION' THEN ''         WHEN 'GV$PLAN_CACHE_PLAN_STAT' THEN ''         WHEN 'V$PLAN_CACHE_PLAN_STAT' THEN ''         WHEN 'GV$PLAN_CACHE_PLAN_EXPLAIN' THEN ''         WHEN 'V$PLAN_CACHE_PLAN_EXPLAIN' THEN ''         WHEN 'GV$MEMSTORE' THEN ''         WHEN 'V$MEMSTORE' THEN ''         WHEN 'GV$MEMSTORE_INFO' THEN ''         WHEN 'V$MEMSTORE_INFO' THEN ''         WHEN 'GV$MEMORY' THEN ''         WHEN 'V$MEMORY' THEN ''         WHEN 'GV$SERVER_MEMSTORE' THEN ''         WHEN 'GV$TENANT_MEMSTORE_ALLOCATOR_INFO' THEN ''         WHEN 'V$TENANT_MEMSTORE_ALLOCATOR_INFO' THEN ''         WHEN 'GV$PLAN_CACHE_STAT' THEN ''         WHEN 'V$PLAN_CACHE_STAT' THEN ''         WHEN 'GV$CONCURRENT_LIMIT_SQL' THEN ''         WHEN 'NLS_INSTANCE_PARAMETERS' THEN ''         WHEN 'GV$TENANT_PX_WORKER_STAT' THEN ''         ELSE NULL END AS VARCHAR2(4000)) AS COMMENTS     FROM SYS.ALL_VIRTUAL_TABLE_SYS_AGENT     WHERE BITAND(TABLE_ID,  1099511627775) > 25000         AND BITAND(TABLE_ID, 1099511627775) <= 30000         AND TABLE_TYPE = 1 )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
