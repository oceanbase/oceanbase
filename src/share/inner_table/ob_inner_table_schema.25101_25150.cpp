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

int ObInnerTableSchema::user_scheduler_job_args_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_SCHEDULER_JOB_ARGS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS JOB_NAME,   CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,   CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,   CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,   CAST(NULL AS VARCHAR2(4000)) AS VALUE,   CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,   CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::all_errors_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ERRORS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(o.owner AS VARCHAR2(128)) AS OWNER,   CAST(o.object_name AS VARCHAR2(128)) AS NAME,   CAST(o.object_type AS VARCHAR2(19)) AS TYPE,   CAST(e.obj_seq AS NUMBER) AS SEQUENCE,   CAST(e.line AS NUMBER) AS LINE,   CAST(e.position AS NUMBER) AS POSITION,   CAST(e.text as VARCHAR2(4000)) AS TEXT,   CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,   CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER FROM   SYS.ALL_OBJECTS o,  (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,                                    3, 'PACKAGE',                                    4, 'TYPE',                                    5, 'PACKAGE BODY',                                    6, 'TYPE BODY',                                    7, 'TRIGGER',                                    8, 'VIEW',                                    9, 'FUNCTION',                                    12, 'PROCEDURE',                                    'MAXTYPE')) AS VARCHAR2(23)) object_type from SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT                       WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e WHERE   o.object_id = e.obj_id   AND o.object_type like e.object_type   AND o.object_type IN (UPPER('package'),                         UPPER('type'),                         UPPER('procedure'),                         UPPER('function'),                         UPPER('package body'),                         UPPER('view'),                         UPPER('trigger'),                         UPPER('type body'),                         UPPER('library'),                         UPPER('queue'),                         UPPER('java source'),                         UPPER('java class'),                         UPPER('dimension'),                         UPPER('assembly'),                         UPPER('hierarchy'),                         UPPER('arrtibute dimension'),                         UPPER('analytic view'))   )__"))) {
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

int ObInnerTableSchema::dba_errors_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_ERRORS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(o.owner AS VARCHAR2(128)) AS OWNER,   CAST(o.object_name AS VARCHAR2(128)) AS NAME,   CAST(o.object_type AS VARCHAR2(19)) AS TYPE,   CAST(e.obj_seq AS NUMBER) AS SEQUENCE,   CAST(e.line AS NUMBER) AS LINE,   CAST(e.position AS NUMBER) AS POSITION,   CAST(e.text as VARCHAR2(4000)) AS TEXT,   CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,   CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER FROM   SYS.ALL_OBJECTS o,  (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,                                    3, 'PACKAGE',                                    4, 'TYPE',                                    5, 'PACKAGE BODY',                                    6, 'TYPE BODY',                                    7, 'TRIGGER',                                    8, 'VIEW',                                    9, 'FUNCTION',                                    12, 'PROCEDURE',                                    'MAXTYPE')) AS VARCHAR2(23)) object_type from SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT                             WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e WHERE   o.object_id = e.obj_id   AND o.object_type like e.object_type   AND o.object_type IN (UPPER('package'),                         UPPER('type'),                         UPPER('procedure'),                         UPPER('function'),                         UPPER('package body'),                         UPPER('view'),                         UPPER('trigger'),                         UPPER('type body'),                         UPPER('library'),                         UPPER('queue'),                         UPPER('java source'),                         UPPER('java class'),                         UPPER('dimension'),                         UPPER('assembly'),                         UPPER('hierarchy'),                         UPPER('arrtibute dimension'),                         UPPER('analytic view'))   )__"))) {
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

int ObInnerTableSchema::user_errors_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_ERRORS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(SYS_CONTEXT('USERENV','CURRENT_USER') AS VARCHAR2(128)) AS OWNER,   CAST(o.object_name AS VARCHAR2(128)) AS NAME,   CAST(o.object_type AS VARCHAR2(19)) AS TYPE,   CAST(e.obj_seq AS NUMBER) AS SEQUENCE,   CAST(e.line AS NUMBER) AS LINE,   CAST(e.position AS NUMBER) AS POSITION,   CAST(e.text as VARCHAR2(4000)) AS TEXT,   CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,   CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER FROM   SYS.USER_OBJECTS o,  (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,                                    3, 'PACKAGE',                                    4, 'TYPE',                                    5, 'PACKAGE BODY',                                    6, 'TYPE BODY',                                    7, 'TRIGGER',                                    8, 'VIEW',                                    9, 'FUNCTION',                                    12, 'PROCEDURE',                                    'MAXTYPE')) AS VARCHAR2(23)) object_type from SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT                           WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e WHERE   o.object_id = e.obj_id   AND o.object_type like e.object_type   AND o.object_type IN (UPPER('package'),                         UPPER('type'),                         UPPER('procedure'),                         UPPER('function'),                         UPPER('package body'),                         UPPER('view'),                         UPPER('trigger'),                         UPPER('type body'),                         UPPER('library'),                         UPPER('queue'),                         UPPER('java source'),                         UPPER('java class'),                         UPPER('dimension'),                         UPPER('assembly'),                         UPPER('hierarchy'),                         UPPER('arrtibute dimension'),                         UPPER('analytic view'))   )__"))) {
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

int ObInnerTableSchema::all_type_methods_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TYPE_METHODS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,   CAST(NULL AS NUMBER) AS PARAMETERS,   CAST(NULL AS NUMBER) AS RESULTS,   CAST(NULL AS VARCHAR2(3)) AS FINAL,   CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,   CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,   CAST(NULL AS VARCHAR2(3)) AS INHERITED FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::dba_type_methods_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TYPE_METHODS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,   CAST(NULL AS NUMBER) AS PARAMETERS,   CAST(NULL AS NUMBER) AS RESULTS,   CAST(NULL AS VARCHAR2(3)) AS FINAL,   CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,   CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,   CAST(NULL AS VARCHAR2(3)) AS INHERITED FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::user_type_methods_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TYPE_METHODS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,   CAST(NULL AS NUMBER) AS PARAMETERS,   CAST(NULL AS NUMBER) AS RESULTS,   CAST(NULL AS VARCHAR2(3)) AS FINAL,   CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,   CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,   CAST(NULL AS VARCHAR2(3)) AS INHERITED FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::all_method_params_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_METHOD_PARAMS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,   CAST(NULL AS NUMBER) AS PARAM_NO,   CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,   CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,   CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::dba_method_params_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_METHOD_PARAMS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,   CAST(NULL AS NUMBER) AS PARAM_NO,   CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,   CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,   CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::user_method_params_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_METHOD_PARAMS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,   CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,   CAST(NULL AS NUMBER) AS METHOD_NO,   CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,   CAST(NULL AS NUMBER) AS PARAM_NO,   CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,   CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,   CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,   CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::dba_tablespaces_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TABLESPACES_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       TABLESPACE_NAME,       CAST(NULL AS NUMBER) BLOCK_SIZE,       CAST(NULL AS NUMBER) INITIAL_EXTENT,       CAST(NULL AS NUMBER) NEXT_EXTENT,       CAST(NULL AS NUMBER) MIN_EXTENT,       CAST(NULL AS NUMBER) MAX_EXTENT,       CAST(NULL AS NUMBER) MAX_SIZE,       CAST(NULL AS NUMBER) PCT_INCREASE,       CAST(NULL AS NUMBER) MIN_EXTLEN,       CAST(NULL AS VARCHAR2(9)) STATUS,       CAST(NULL AS VARCHAR2(9)) CONTENTS,       CAST(NULL AS VARCHAR2(9)) LOGGING,       CAST(NULL AS VARCHAR2(3)) FORCE_LOGGING,       CAST(NULL AS VARCHAR2(10)) EXTENT_MANAGEMENT,       CAST(NULL AS VARCHAR2(9)) ALLOCATION_TYPE,       CAST(NULL AS VARCHAR2(3)) PLUGGED_IN,       CAST(NULL AS VARCHAR2(6)) SEGMENT_SPACE_MANAGEMENT,       CAST(NULL AS VARCHAR2(8)) DEF_TAB_COMPRESSION,       CAST(NULL AS VARCHAR2(11)) RETENTION,       CAST(NULL AS VARCHAR2(3)) BIGFILE,       CAST(NULL AS VARCHAR2(7)) PREDICATE_EVALUATION,       CAST(CASE WHEN ENCRYPTION_NAME IS NULL THEN 'NO' ELSE 'YES' END AS VARCHAR2(3)) AS ENCRYPTED,       CAST(NULL AS VARCHAR2(12)) COMPRESS_FOR       FROM       SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT       WHERE TENANT_ID = EFFECTIVE_TENANT_ID()       )__"))) {
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

int ObInnerTableSchema::user_tablespaces_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TABLESPACES_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       TABLESPACE_NAME,       CAST(NULL AS NUMBER) BLOCK_SIZE,       CAST(NULL AS NUMBER) INITIAL_EXTENT,       CAST(NULL AS NUMBER) NEXT_EXTENT,       CAST(NULL AS NUMBER) MIN_EXTENT,       CAST(NULL AS NUMBER) MAX_EXTENT,       CAST(NULL AS NUMBER) MAX_SIZE,       CAST(NULL AS NUMBER) PCT_INCREASE,       CAST(NULL AS NUMBER) MIN_EXTLEN,       CAST(NULL AS VARCHAR2(9)) STATUS,       CAST(NULL AS VARCHAR2(9)) CONTENTS,       CAST(NULL AS VARCHAR2(9)) LOGGING,       CAST(NULL AS VARCHAR2(3)) FORCE_LOGGING,       CAST(NULL AS VARCHAR2(10)) EXTENT_MANAGEMENT,       CAST(NULL AS VARCHAR2(9)) ALLOCATION_TYPE,       CAST(NULL AS VARCHAR2(6)) SEGMENT_SPACE_MANAGEMENT,       CAST(NULL AS VARCHAR2(8)) DEF_TAB_COMPRESSION,       CAST(NULL AS VARCHAR2(11)) RETENTION,       CAST(NULL AS VARCHAR2(3)) BIGFILE,       CAST(NULL AS VARCHAR2(7)) PREDICATE_EVALUATION,       CAST(CASE WHEN ENCRYPTION_NAME IS NULL THEN 'NO' ELSE 'YES' END AS VARCHAR2(3)) AS ENCRYPTED,       CAST(NULL AS VARCHAR2(12)) COMPRESS_FOR       FROM       SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT       WHERE TENANT_ID = EFFECTIVE_TENANT_ID()       )__"))) {
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

int ObInnerTableSchema::dba_ind_expressions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_IND_EXPRESSIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,     CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,     CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,     COLUMN_POSITION   FROM   (   SELECT INDEX_OWNER,     INDEX_NAME,     TABLE_OWNER,     F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,     E.INDEX_POSITION AS  COLUMN_POSITION,     E.TABLE_ID AS TABLE_ID     FROM       (SELECT INDEX_OWNER,               INDEX_NAME,               TABLE_OWNER,               C.TABLE_ID AS TABLE_ID,               C.INDEX_ID AS INDEX_ID,               D.COLUMN_ID AS COLUMN_ID,               D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION       FROM         (SELECT DATABASE_NAME AS INDEX_OWNER,                 SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 DATABASE_NAME AS TABLE_OWNER,                 A.DATA_TABLE_ID AS TABLE_ID,                 A.TABLE_ID AS INDEX_ID           FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A           JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID           AND B.DATABASE_NAME != '__recyclebin'           AND A.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           WHERE TABLE_TYPE=5 ) C       JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.INDEX_POSITION != 0) E     JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID         AND F.TENANT_ID = EFFECTIVE_TENANT_ID()     AND F.COLUMN_ID=E.COLUMN_ID     AND BITAND(F.COLUMN_FLAGS,3) > 0) G   JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID       AND H.TENANT_ID = EFFECTIVE_TENANT_ID() AND H.TABLE_TYPE != 12 AND H.TABLE_TYPE != 13 )__"))) {
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

int ObInnerTableSchema::user_ind_expressions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_IND_EXPRESSIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,     CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,     COLUMN_POSITION   FROM   (   SELECT     INDEX_NAME,     F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,     E.INDEX_POSITION AS  COLUMN_POSITION,     E.TABLE_ID AS TABLE_ID     FROM       (SELECT               INDEX_NAME,               C.TABLE_ID AS TABLE_ID,               C.INDEX_ID AS INDEX_ID,               D.COLUMN_ID AS COLUMN_ID,               D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION       FROM         (SELECT                 SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 A.DATA_TABLE_ID AS TABLE_ID,                 A.TABLE_ID AS INDEX_ID           FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A           JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID           AND B.DATABASE_NAME != '__recyclebin' AND A.DATABASE_ID = USERENV('SCHEMAID')           AND A.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           WHERE TABLE_TYPE=5 ) C       JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()         AND D.INDEX_POSITION != 0) E     JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID         AND F.TENANT_ID = EFFECTIVE_TENANT_ID()     AND F.COLUMN_ID=E.COLUMN_ID     AND BITAND(F.COLUMN_FLAGS,3) > 0) G   JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID       AND H.TENANT_ID = EFFECTIVE_TENANT_ID() AND H.TABLE_TYPE != 12 AND H.TABLE_TYPE != 13 )__"))) {
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

int ObInnerTableSchema::all_ind_expressions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_IND_EXPRESSIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,     CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,     CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,     CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,     COLUMN_POSITION   FROM   (   SELECT INDEX_OWNER,     INDEX_NAME,     TABLE_OWNER,     F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,     E.INDEX_POSITION AS  COLUMN_POSITION,     E.TABLE_ID AS TABLE_ID     FROM       (SELECT INDEX_OWNER,               INDEX_NAME,               TABLE_OWNER,               C.TABLE_ID AS TABLE_ID,               C.INDEX_ID AS INDEX_ID,               D.COLUMN_ID AS COLUMN_ID,               D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION       FROM         (SELECT DATABASE_NAME AS INDEX_OWNER,                         SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 DATABASE_NAME AS TABLE_OWNER,                 A.DATA_TABLE_ID AS TABLE_ID,                 A.TABLE_ID AS INDEX_ID           FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A           JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID           AND A.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (A.DATABASE_ID = USERENV('SCHEMAID')                OR USER_CAN_ACCESS_OBJ(1, A.DATA_TABLE_ID, A.DATABASE_ID) = 1)           AND B.DATABASE_NAME != '__recyclebin'           WHERE TABLE_TYPE=5 ) C       JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()         AND D.INDEX_POSITION != 0) E     JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID         AND F.TENANT_ID = EFFECTIVE_TENANT_ID()     AND F.COLUMN_ID=E.COLUMN_ID     AND BITAND(F.COLUMN_FLAGS,3) > 0) G   JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID       AND H.TENANT_ID = EFFECTIVE_TENANT_ID() AND H.TABLE_TYPE != 12 AND H.TABLE_TYPE != 13 )__"))) {
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

int ObInnerTableSchema::all_ind_partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_IND_PARTITIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(D.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))         END AS VARCHAR2(128)) AS INDEX_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN 'YES'          ELSE 'NO' END AS CHAR(3)) COMPOSITE,      CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN PART.SUB_PART_NUM          ELSE 0 END AS NUMBER)  SUBPARTITION_COUNT,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS VARCHAR(32767)) HIGH_VALUE,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.PARTITION_POSITION AS NUMBER) PARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(13)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST('NO' AS VARCHAR2(3)) AS "INTERVAL",     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,     CAST(NULL AS VARCHAR2(3)) AS ORPHANED_ENTRIES     FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I     JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5        AND (I.DATABASE_ID = USERENV('SCHEMAID')             OR USER_CAN_ACCESS_OBJ(1, I.DATA_TABLE_ID, I.DATABASE_ID) = 1)      JOIN (SELECT TENANT_ID,                  TABLE_ID,                  PART_NAME,                  SUB_PART_NUM,                  HIGH_BOUND_VAL,                  LIST_VAL,                  COMPRESS_FUNC_NAME,                  ROW_NUMBER() OVER (                    PARTITION BY TENANT_ID, TABLE_ID                    ORDER BY PART_IDX, PART_ID ASC                  ) PARTITION_POSITION           FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT) PART     ON I.TENANT_ID = PART.TENANT_ID        AND I.TABLE_ID = PART.TABLE_ID      WHERE I.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::user_ind_partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_IND_PARTITIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))         END AS VARCHAR2(128)) AS INDEX_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN 'YES'          ELSE 'NO' END AS CHAR(3)) COMPOSITE,      CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN PART.SUB_PART_NUM          ELSE 0 END AS NUMBER)  SUBPARTITION_COUNT,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS VARCHAR(32767)) HIGH_VALUE,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.PARTITION_POSITION AS NUMBER) PARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(13)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST('NO' AS VARCHAR2(3)) AS "INTERVAL",     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,     CAST(NULL AS VARCHAR2(3)) AS ORPHANED_ENTRIES     FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I     JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5        AND I.DATABASE_ID = USERENV('SCHEMAID')      JOIN (SELECT TENANT_ID,                  TABLE_ID,                  PART_NAME,                  SUB_PART_NUM,                  HIGH_BOUND_VAL,                  LIST_VAL,                  COMPRESS_FUNC_NAME,                  ROW_NUMBER() OVER (                    PARTITION BY TENANT_ID, TABLE_ID                    ORDER BY PART_IDX, PART_ID ASC                  ) PARTITION_POSITION           FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT) PART     ON I.TENANT_ID = PART.TENANT_ID        AND I.TABLE_ID = PART.TABLE_ID      WHERE I.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ind_partitions_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_IND_PARTITIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_IND_PARTITIONS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(D.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))         END AS VARCHAR2(128)) AS INDEX_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN 'YES'          ELSE 'NO' END AS CHAR(3)) COMPOSITE,      CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN PART.SUB_PART_NUM          ELSE 0 END AS NUMBER)  SUBPARTITION_COUNT,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS VARCHAR(32767)) HIGH_VALUE,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.PARTITION_POSITION AS NUMBER) PARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(13)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,     CAST('NO' AS VARCHAR2(3)) AS "INTERVAL",     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,     CAST(NULL AS VARCHAR2(3)) AS ORPHANED_ENTRIES     FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I     JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5      JOIN (SELECT TENANT_ID,                  TABLE_ID,                  PART_NAME,                  SUB_PART_NUM,                  HIGH_BOUND_VAL,                  LIST_VAL,                  COMPRESS_FUNC_NAME,                  ROW_NUMBER() OVER (                    PARTITION BY TENANT_ID, TABLE_ID                    ORDER BY PART_IDX, PART_ID ASC                  ) PARTITION_POSITION           FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT) PART     ON I.TENANT_ID = PART.TENANT_ID        AND I.TABLE_ID = PART.TABLE_ID      WHERE I.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ind_subpartitions_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_IND_SUBPARTITIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_IND_SUBPARTITIONS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(D.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))         END AS VARCHAR2(128)) AS INDEX_NAME,     CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,     CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS VARCHAR(32767)) HIGH_VALUE,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,     CAST(PART.PARTITION_POSITION AS NUMBER) PARTITION_POSITION,     CAST(PART.SUBPARTITION_POSITION AS NUMBER) SUBPARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(3)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(13)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST('NO' AS VARCHAR2(3)) AS "INTERVAL",     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS     FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I     JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5     JOIN     (SELECT P_PART.TENANT_ID,             P_PART.TABLE_ID,             P_PART.PART_NAME,             P_PART.PARTITION_POSITION,             S_PART.SUB_PART_NAME,             S_PART.HIGH_BOUND_VAL,             S_PART.LIST_VAL,             S_PART.COMPRESS_FUNC_NAME,             S_PART.SUBPARTITION_POSITION      FROM (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              PART_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID                ORDER BY PART_IDX, PART_ID ASC              ) AS PARTITION_POSITION            FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT) P_PART,           (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              SUB_PART_NAME,              HIGH_BOUND_VAL,              LIST_VAL,              COMPRESS_FUNC_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID, PART_ID                ORDER BY SUB_PART_IDX, SUB_PART_ID ASC              ) AS SUBPARTITION_POSITION            FROM SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT) S_PART      WHERE P_PART.PART_ID = S_PART.PART_ID AND            P_PART.TABLE_ID = S_PART.TABLE_ID            AND P_PART.TENANT_ID = S_PART.TENANT_ID) PART     ON I.TABLE_ID = PART.TABLE_ID AND I.TENANT_ID = PART.TENANT_ID     WHERE I.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_ind_subpartitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_IND_SUBPARTITIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(D.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))         END AS VARCHAR2(128)) AS INDEX_NAME,     CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,     CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS VARCHAR(32767)) HIGH_VALUE,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,     CAST(PART.PARTITION_POSITION AS NUMBER) PARTITION_POSITION,     CAST(PART.SUBPARTITION_POSITION AS NUMBER) SUBPARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(3)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(13)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST('NO' AS VARCHAR2(3)) AS "INTERVAL",     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS     FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I     JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5        AND (I.DATABASE_ID = USERENV('SCHEMAID')             OR USER_CAN_ACCESS_OBJ(1, I.DATA_TABLE_ID, I.DATABASE_ID) = 1)     JOIN     (SELECT P_PART.TENANT_ID,             P_PART.TABLE_ID,             P_PART.PART_NAME,             P_PART.PARTITION_POSITION,             S_PART.SUB_PART_NAME,             S_PART.HIGH_BOUND_VAL,             S_PART.LIST_VAL,             S_PART.COMPRESS_FUNC_NAME,             S_PART.SUBPARTITION_POSITION      FROM (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              PART_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID                ORDER BY PART_IDX, PART_ID ASC              ) AS PARTITION_POSITION            FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT) P_PART,           (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              SUB_PART_NAME,              HIGH_BOUND_VAL,              LIST_VAL,              COMPRESS_FUNC_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID, PART_ID                ORDER BY SUB_PART_IDX, SUB_PART_ID ASC              ) AS SUBPARTITION_POSITION            FROM SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT) S_PART      WHERE P_PART.PART_ID = S_PART.PART_ID AND            P_PART.TABLE_ID = S_PART.TABLE_ID            AND P_PART.TENANT_ID = S_PART.TENANT_ID) PART     ON I.TABLE_ID = PART.TABLE_ID AND I.TENANT_ID = PART.TENANT_ID     WHERE I.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::user_ind_subpartitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_IND_SUBPARTITIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))         END AS VARCHAR2(128)) AS INDEX_NAME,     CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,     CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS VARCHAR(32767)) HIGH_VALUE,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,     CAST(PART.PARTITION_POSITION AS NUMBER) PARTITION_POSITION,     CAST(PART.SUBPARTITION_POSITION AS NUMBER) SUBPARTITION_POSITION,     CAST(NULL AS VARCHAR2(8)) AS STATUS,     CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,     CAST(NULL AS NUMBER) AS PCT_FREE,     CAST(NULL AS NUMBER) AS INI_TRANS,     CAST(NULL AS NUMBER) AS MAX_TRANS,     CAST(NULL AS NUMBER) AS INITIAL_EXTENT,     CAST(NULL AS NUMBER) AS NEXT_EXTENT,     CAST(NULL AS NUMBER) AS MIN_EXTENT,     CAST(NULL AS NUMBER) AS MAX_EXTENT,     CAST(NULL AS NUMBER) AS MAX_SIZE,     CAST(NULL AS NUMBER) AS PCT_INCREASE,     CAST(NULL AS NUMBER) AS FREELISTS,     CAST(NULL AS NUMBER) AS FREELIST_GROUPS,     CAST(NULL AS VARCHAR2(3)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(13)) AS COMPRESSION,     CAST(NULL AS NUMBER) AS BLEVEL,     CAST(NULL AS NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,     CAST(NULL AS NUMBER) AS NUM_ROWS,     CAST(NULL AS NUMBER) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,     CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,     CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS VARCHAR2(3)) AS USER_STATS,     CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,     CAST('NO' AS VARCHAR2(3)) AS "INTERVAL",     CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,     CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS     FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I     JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5        AND I.DATABASE_ID = USERENV('SCHEMAID')     JOIN     (SELECT P_PART.TENANT_ID,             P_PART.TABLE_ID,             P_PART.PART_NAME,             P_PART.PARTITION_POSITION,             S_PART.SUB_PART_NAME,             S_PART.HIGH_BOUND_VAL,             S_PART.LIST_VAL,             S_PART.COMPRESS_FUNC_NAME,             S_PART.SUBPARTITION_POSITION      FROM (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              PART_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID                ORDER BY PART_IDX, PART_ID ASC              ) AS PARTITION_POSITION            FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT) P_PART,           (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              SUB_PART_NAME,              HIGH_BOUND_VAL,              LIST_VAL,              COMPRESS_FUNC_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID, PART_ID                ORDER BY SUB_PART_IDX, SUB_PART_ID ASC              ) AS SUBPARTITION_POSITION            FROM SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT) S_PART      WHERE P_PART.PART_ID = S_PART.PART_ID AND            P_PART.TABLE_ID = S_PART.TABLE_ID            AND P_PART.TENANT_ID = S_PART.TENANT_ID) PART     ON I.TABLE_ID = PART.TABLE_ID AND I.TENANT_ID = PART.TENANT_ID     WHERE I.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_roles_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_ROLES_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT CAST(USER_NAME AS VARCHAR2(30)) ROLE,      CAST('NO' AS VARCHAR2(8)) PASSWORD_REQUIRED,      CAST('NONE' AS VARCHAR2(11)) AUTHENTICATION_TYPE      FROM   SYS.ALL_VIRTUAL_USER_REAL_AGENT U      WHERE             U.TYPE = 1 AND U.TENANT_ID = EFFECTIVE_TENANT_ID())__"))) {
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

int ObInnerTableSchema::dba_role_privs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_ROLE_PRIVS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT CAST(A.USER_NAME AS VARCHAR2(30)) GRANTEE,      CAST(B.USER_NAME AS VARCHAR2(30)) GRANTED_ROLE,      DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') AS ADMIN_OPTION ,      DECODE(R.DISABLE_FLAG, 0, 'YES', 1, 'NO', '') AS DEFAULT_ROLE      FROM SYS.ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT R,      SYS.ALL_VIRTUAL_USER_REAL_AGENT A,      SYS.ALL_VIRTUAL_USER_REAL_AGENT B      WHERE R.GRANTEE_ID = A.USER_ID      AND R.ROLE_ID = B.USER_ID      AND B.TYPE = 1      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       )__"))) {
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

int ObInnerTableSchema::user_role_privs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_ROLE_PRIVS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT CAST(A.USER_NAME AS VARCHAR2(30)) GRANTEE,      CAST(B.USER_NAME AS VARCHAR2(30)) GRANTED_ROLE,      DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') AS ADMIN_OPTION ,      DECODE(R.DISABLE_FLAG, 0, 'YES', 1, 'NO', '') AS DEFAULT_ROLE      FROM SYS.ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT R,      SYS.ALL_VIRTUAL_USER_REAL_AGENT A,      SYS.ALL_VIRTUAL_USER_REAL_AGENT B      WHERE R.GRANTEE_ID = A.USER_ID      AND R.ROLE_ID = B.USER_ID      AND B.TYPE = 1      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()      AND A.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER'))__"))) {
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

int ObInnerTableSchema::dba_tab_privs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_PRIVS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT C.USER_NAME AS GRANTEE,        E.DATABASE_NAME AS OWNER,        CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),                         D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,        B.USER_NAME AS GRANTOR,        CAST (DECODE(A.PRIV_ID, 1, 'ALTER',                          2, 'AUDIT',                          3, 'COMMENT',                          4, 'DELETE',                          5, 'GRANT',                          6, 'INDEX',                          7, 'INSERT',                          8, 'LOCK',                          9, 'RENAME',                          10, 'SELECT',                          11, 'UPDATE',                          12, 'REFERENCES',                          13, 'EXECUTE',                          14, 'CREATE',                          15, 'FLASHBACK',                          16, 'READ',                          17, 'WRITE',                          'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,        DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,        CAST('NO' AS VARCHAR(10)) AS  HIERARCHY       FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,               SYS.ALL_VIRTUAL_USER_REAL_AGENT B,               SYS.ALL_VIRTUAL_USER_REAL_AGENT C,               (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID, decode(table_type, 5,11,1) AS OBJ_TYPE                  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID() AND TABLE_TYPE != 12 AND TABLE_TYPE != 13                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID, 3 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID,                       PACKAGE_NAME AS TABLE_NAME,                       DATABASE_ID AS DATABASE_ID,                       14 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT                UNION ALL                SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID,                       DECODE(ROUTINE_TYPE, 1, 12, 2, 9, 0) AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID, 2 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT TYPE_ID AS TABLE_ID, TYPE_NAME AS TABLE_NAME, DATABASE_ID, 4 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_TYPE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT DIRECTORY_ID AS TABLE_ID, DIRECTORY_NAME AS TABLE_NAME,                   (SELECT DATABASE_ID FROM SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT WHERE DATABASE_NAME = 'SYS'),                   10 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()               ) D,               SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E       WHERE A.GRANTOR_ID = B.USER_ID         AND A.GRANTEE_ID = C.USER_ID         AND A.COL_ID = 65535         AND A.OBJ_ID = D.TABLE_ID         AND A.OBJTYPE = D.OBJ_TYPE         AND  D.DATABASE_ID = E.DATABASE_ID         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.DATABASE_NAME != '__recyclebin' )__"))) {
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

int ObInnerTableSchema::all_tab_privs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TAB_PRIVS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT B.USER_NAME AS GRANTOR,        C.USER_NAME AS GRANTEE,        E.DATABASE_NAME AS TABLE_SCHEMA,        CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),                         D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,        CAST (DECODE(A.PRIV_ID, 1, 'ALTER',                          2, 'AUDIT',                          3, 'COMMENT',                          4, 'DELETE',                          5, 'GRANT',                          6, 'INDEX',                          7, 'INSERT',                          8, 'LOCK',                          9, 'RENAME',                          10, 'SELECT',                          11, 'UPDATE',                          12, 'REFERENCES',                          13, 'EXECUTE',                          14, 'CREATE',                          15, 'FLASHBACK',                          16, 'READ',                          17, 'WRITE',                          'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,        DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,        CAST('NO' AS VARCHAR(10)) AS  HIERARCHY       FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,               SYS.ALL_VIRTUAL_USER_REAL_AGENT B,               SYS.ALL_VIRTUAL_USER_REAL_AGENT C,               (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID, decode(table_type, 5,11,1) AS OBJ_TYPE                  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID() AND TABLE_TYPE != 12 AND TABLE_TYPE != 13                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID, 3 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID,                       PACKAGE_NAME AS TABLE_NAME,                       DATABASE_ID AS DATABASE_ID,                       14 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT                UNION ALL                SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID,                       DECODE(ROUTINE_TYPE, 1, 12, 2, 9, 0) AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID, 2 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT TYPE_ID AS TABLE_ID, TYPE_NAME AS TABLE_NAME, DATABASE_ID, 4 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_TYPE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT DIRECTORY_ID AS TABLE_ID, DIRECTORY_NAME AS TABLE_NAME,                   (SELECT DATABASE_ID FROM SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT WHERE DATABASE_NAME = 'SYS'),                   10 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()               ) D,               SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E       WHERE A.GRANTOR_ID = B.USER_ID         AND A.GRANTEE_ID = C.USER_ID         AND A.COL_ID = 65535         AND A.OBJ_ID = D.TABLE_ID         AND A.OBJTYPE = D.OBJ_TYPE         AND D.DATABASE_ID = E.DATABASE_ID         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.DATABASE_NAME != '__recyclebin'         AND (C.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')              OR E.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')              OR B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')) )__"))) {
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

int ObInnerTableSchema::user_tab_privs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TAB_PRIVS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT C.USER_NAME AS GRANTEE,        E.DATABASE_NAME AS OWNER,        CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),                         D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,        B.USER_NAME AS GRANTOR,        CAST (DECODE(A.PRIV_ID, 1, 'ALTER',                          2, 'AUDIT',                          3, 'COMMENT',                          4, 'DELETE',                          5, 'GRANT',                          6, 'INDEX',                          7, 'INSERT',                          8, 'LOCK',                          9, 'RENAME',                          10, 'SELECT',                          11, 'UPDATE',                          12, 'REFERENCES',                          13, 'EXECUTE',                          14, 'CREATE',                          15, 'FLASHBACK',                          16, 'READ',                          17, 'WRITE',                          'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,        DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,        CAST('NO' AS VARCHAR(10)) AS  HIERARCHY       FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,               SYS.ALL_VIRTUAL_USER_REAL_AGENT B,               SYS.ALL_VIRTUAL_USER_REAL_AGENT C,               (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID, decode(table_type, 5,11,1) AS OBJ_TYPE                  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID() AND TABLE_TYPE != 12 AND TABLE_TYPE != 13                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID, 3 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT PACKAGE_ID AS TABLE_ID,                       PACKAGE_NAME AS TABLE_NAME,                       DATABASE_ID AS DATABASE_ID,                       14 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT                UNION ALL                SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID,                       DECODE(ROUTINE_TYPE, 1, 12, 2, 9, 0) AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID, 2 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT TYPE_ID AS TABLE_ID, TYPE_NAME AS TABLE_NAME, DATABASE_ID, 4 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_TYPE_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()                UNION ALL                SELECT DIRECTORY_ID AS TABLE_ID, DIRECTORY_NAME AS TABLE_NAME,                   (SELECT DATABASE_ID FROM SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT WHERE DATABASE_NAME = 'SYS'),                   10 AS OBJ_TYPE                   FROM SYS.ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT                   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()               ) D,               SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E       WHERE A.GRANTOR_ID = B.USER_ID         AND A.GRANTEE_ID = C.USER_ID         AND A.COL_ID = 65535         AND A.OBJ_ID = D.TABLE_ID         AND A.OBJTYPE = D.OBJ_TYPE         AND D.DATABASE_ID = E.DATABASE_ID         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.TENANT_ID = EFFECTIVE_TENANT_ID()         AND E.DATABASE_NAME != '__recyclebin'         AND (SYS_CONTEXT('USERENV','CURRENT_USER') IN (C.USER_NAME, B.USER_NAME)              OR E.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')) )__"))) {
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

int ObInnerTableSchema::dba_sys_privs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SYS_PRIVS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT B.USER_NAME AS GRANTEE,            CAST (DECODE(A.PRIV_ID,                 1, 'CREATE SESSION',                 2, 'EXEMPT REDACTION POLICY',                 3, 'SYSDBA',                 4, 'SYSOPER',                 5, 'SYSBACKUP',                 6, 'CREATE TABLE',                 7, 'CREATE ANY TABLE',                 8, 'ALTER ANY TABLE',                 9, 'BACKUP ANY TABLE',                 10, 'DROP ANY TABLE',                 11, 'LOCK ANY TABLE',                 12, 'COMMENT ANY TABLE',                 13, 'SELECT ANY TABLE',                 14, 'INSERT ANY TABLE',                 15, 'UPDATE ANY TABLE',                 16, 'DELETE ANY TABLE',                 17, 'FLASHBACK ANY TABLE',                 18, 'CREATE ROLE',                 19, 'DROP ANY ROLE',                 20, 'GRANT ANY ROLE',                 21, 'ALTER ANY ROLE',                 22, 'AUDIT ANY',                 23, 'GRANT ANY PRIVILEGE',                 24, 'GRANT ANY OBJECT PRIVILEGE',                 25, 'CREATE ANY INDEX',                 26, 'ALTER ANY INDEX',                 27, 'DROP ANY INDEX',                 28, 'CREATE ANY VIEW',                 29, 'DROP ANY VIEW',                 30, 'CREATE VIEW',                 31, 'SELECT ANY DICTIONARY',                 32, 'CREATE PROCEDURE',                 33, 'CREATE ANY PROCEDURE',                 34, 'ALTER ANY PROCEDURE',                 35, 'DROP ANY PROCEDURE',                 36, 'EXECUTE ANY PROCEDURE',                 37, 'CREATE SYNONYM',                 38, 'CREATE ANY SYNONYM',                 39, 'DROP ANY SYNONYM',                 40, 'CREATE PUBLIC SYNONYM',                 41, 'DROP PUBLIC SYNONYM',                 42, 'CREATE SEQUENCE',                 43, 'CREATE ANY SEQUENCE',                 44, 'ALTER ANY SEQUENCE',                 45, 'DROP ANY SEQUENCE',                 46, 'SELECT ANY SEQUENCE',                 47, 'CREATE TRIGGER',                 48, 'CREATE ANY TRIGGER',                 49, 'ALTER ANY TRIGGER',                 50, 'DROP ANY TRIGGER',                 51, 'CREATE PROFILE',                 52, 'ALTER PROFILE',                 53, 'DROP PROFILE',                 54, 'CREATE USER',                 55, 'BECOME USER',                 56, 'ALTER USER',                 57, 'DROP USER',                 58, 'CREATE TYPE',                 59, 'CREATE ANY TYPE',                 60, 'ALTER ANY TYPE',                 61, 'DROP ANY TYPE',                 62, 'EXECUTE ANY TYPE',                 63, 'UNDER ANY TYPE',                 64, 'PURGE DBA_RECYCLEBIN',                 65, 'CREATE ANY OUTLINE',                 66, 'ALTER ANY OUTLINE',                 67, 'DROP ANY OUTLINE',                 68, 'SYSKM',                 69, 'CREATE TABLESPACE',                 70, 'ALTER TABLESPACE',                 71, 'DROP TABLESPACE',                 72, 'SHOW PROCESS',                 73, 'ALTER SYSTEM',                 74, 'CREATE DATABASE LINK',                 75, 'CREATE PUBLIC DATABASE LINK',                 76, 'DROP DATABASE LINK',                 77, 'ALTER SESSION',                 78, 'ALTER DATABASE',                 79, 'CREATE ANY DIRECTORY',                 80, 'DROP ANY DIRECTORY',                 81, 'DEBUG CONNECT SESSION',                 82, 'DEBUG ANY PROCEDURE',                 83, 'CREATE ANY CONTEXT',                 84, 'DROP ANY CONTEXT',                 'OTHER') AS VARCHAR(40)) AS PRIVILEGE,         CASE PRIV_OPTION           WHEN 0 THEN 'NO'           ELSE 'YES'           END AS ADMIN_OPTION     FROM SYS.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT A,           SYS.ALL_VIRTUAL_USER_REAL_AGENT B     WHERE A.GRANTEE_ID = B.USER_ID     AND A.TENANT_ID = EFFECTIVE_TENANT_ID()     AND B.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::user_sys_privs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_SYS_PRIVS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT B.USER_NAME AS USERNAME,            CAST (DECODE(A.PRIV_ID,                 1, 'CREATE SESSION',                 2, 'EXEMPT REDACTION POLICY',                 3, 'SYSDBA',                 4, 'SYSOPER',                 5, 'SYSBACKUP',                 6, 'CREATE TABLE',                 7, 'CREATE ANY TABLE',                 8, 'ALTER ANY TABLE',                 9, 'BACKUP ANY TABLE',                 10, 'DROP ANY TABLE',                 11, 'LOCK ANY TABLE',                 12, 'COMMENT ANY TABLE',                 13, 'SELECT ANY TABLE',                 14, 'INSERT ANY TABLE',                 15, 'UPDATE ANY TABLE',                 16, 'DELETE ANY TABLE',                 17, 'FLASHBACK ANY TABLE',                 18, 'CREATE ROLE',                 19, 'DROP ANY ROLE',                 20, 'GRANT ANY ROLE',                 21, 'ALTER ANY ROLE',                 22, 'AUDIT ANY',                 23, 'GRANT ANY PRIVILEGE',                 24, 'GRANT ANY OBJECT PRIVILEGE',                 25, 'CREATE ANY INDEX',                 26, 'ALTER ANY INDEX',                 27, 'DROP ANY INDEX',                 28, 'CREATE ANY VIEW',                 29, 'DROP ANY VIEW',                 30, 'CREATE VIEW',                 31, 'SELECT ANY DICTIONARY',                 32, 'CREATE PROCEDURE',                 33, 'CREATE ANY PROCEDURE',                 34, 'ALTER ANY PROCEDURE',                 35, 'DROP ANY PROCEDURE',                 36, 'EXECUTE ANY PROCEDURE',                 37, 'CREATE SYNONYM',                 38, 'CREATE ANY SYNONYM',                 39, 'DROP ANY SYNONYM',                 40, 'CREATE PUBLIC SYNONYM',                 41, 'DROP PUBLIC SYNONYM',                 42, 'CREATE SEQUENCE',                 43, 'CREATE ANY SEQUENCE',                 44, 'ALTER ANY SEQUENCE',                 45, 'DROP ANY SEQUENCE',                 46, 'SELECT ANY SEQUENCE',                 47, 'CREATE TRIGGER',                 48, 'CREATE ANY TRIGGER',                 49, 'ALTER ANY TRIGGER',                 50, 'DROP ANY TRIGGER',                 51, 'CREATE PROFILE',                 52, 'ALTER PROFILE',                 53, 'DROP PROFILE',                 54, 'CREATE USER',                 55, 'BECOME USER',                 56, 'ALTER USER',                 57, 'DROP USER',                 58, 'CREATE TYPE',                 59, 'CREATE ANY TYPE',                 60, 'ALTER ANY TYPE',                 61, 'DROP ANY TYPE',                 62, 'EXECUTE ANY TYPE',                 63, 'UNDER ANY TYPE',                 64, 'PURGE DBA_RECYCLEBIN',                 65, 'CREATE ANY OUTLINE',                 66, 'ALTER ANY OUTLINE',                 67, 'DROP ANY OUTLINE',                 68, 'SYSKM',                 69, 'CREATE TABLESPACE',                 70, 'ALTER TABLESPACE',                 71, 'DROP TABLESPACE',                 72, 'SHOW PROCESS',                 73, 'ALTER SYSTEM',                 74, 'CREATE DATABASE LINK',                 75, 'CREATE PUBLIC DATABASE LINK',                 76, 'DROP DATABASE LINK',                 77, 'ALTER SESSION',                 78, 'ALTER DATABASE',                 79, 'CREATE ANY DIRECTORY',                 80, 'DROP ANY DIRECTORY',                 81, 'DEBUG CONNECT SESSION',                 82, 'DEBUG ANY PROCEDURE',                 83, 'CREATE ANY CONTEXT',                 84, 'DROP ANY CONTEXT',                 'OTHER') AS VARCHAR(40)) AS PRIVILEGE,         CASE PRIV_OPTION           WHEN 0 THEN 'NO'           ELSE 'YES'           END AS ADMIN_OPTION     FROM SYS.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT A,           SYS.ALL_VIRTUAL_USER_REAL_AGENT B     WHERE B.TYPE = 0 AND           A.GRANTEE_ID =B.USER_ID           AND A.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           AND B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::audit_actions_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_AUDIT_ACTIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_AUDIT_ACTIONS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT ACTION_ID AS ACTION,       ACTION_NAME AS NAME       FROM SYS.ALL_VIRTUAL_AUDIT_ACTION   )__"))) {
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

int ObInnerTableSchema::stmt_audit_option_map_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_STMT_AUDIT_OPTION_MAP_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_STMT_AUDIT_OPTION_MAP_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT OPERATION_TYPE AS OPTION#,       OPERATION_NAME AS NAME,       CAST(NULL AS NUMBER) AS PROPERTY       FROM SYS.ALL_VIRTUAL_AUDIT_OPERATION   )__"))) {
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

int ObInnerTableSchema::all_def_audit_opts_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DEF_AUDIT_OPTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DEF_AUDIT_OPTS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         MAX(CASE A.OPERATION_TYPE WHEN 39 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS ALT,         MAX(CASE A.OPERATION_TYPE WHEN 40 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS AUD,         MAX(CASE A.OPERATION_TYPE WHEN 41 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS COM,         MAX(CASE A.OPERATION_TYPE WHEN 42 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS DEL,         MAX(CASE A.OPERATION_TYPE WHEN 43 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS GRA,         MAX(CASE A.OPERATION_TYPE WHEN 44 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS IND,         MAX(CASE A.OPERATION_TYPE WHEN 45 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS INS,         MAX(CASE A.OPERATION_TYPE WHEN 46 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS LOC,         MAX(CASE A.OPERATION_TYPE WHEN 47 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REN,         MAX(CASE A.OPERATION_TYPE WHEN 48 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS SEL,         MAX(CASE A.OPERATION_TYPE WHEN 49 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS UPD,         MAX(CASE A.OPERATION_TYPE WHEN 50 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REF,         MAX(CASE A.OPERATION_TYPE WHEN 51 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS EXE,         MAX(CASE A.OPERATION_TYPE WHEN 55 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS FBK,         MAX(CASE A.OPERATION_TYPE WHEN 53 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REA       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT A       WHERE A.AUDIT_TYPE = 3   )__"))) {
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

int ObInnerTableSchema::dba_stmt_audit_opts_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_STMT_AUDIT_OPTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_STMT_AUDIT_OPTS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT  B.USER_NAME AS USER_NAME,               CAST(NULL AS VARCHAR2(128)) PROXY_NAME,               C.OPERATION_NAME AS AUDIT_OPTION,               DECODE(A.IN_SUCCESS, 2, 'BY SESSION', 3, 'BY ACCESS', 'NOT SET') AS SUCCESS,               DECODE(A.IN_FAILURE, 2, 'BY SESSION', 3, 'BY ACCESS', 'NOT SET') AS FAILURE       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT A,            SYS.ALL_VIRTUAL_USER_REAL_AGENT B,            SYS.ALL_VIRTUAL_AUDIT_OPERATION C       WHERE A.AUDIT_TYPE = 2         AND A.OWNER_ID = B.USER_ID         AND A.OPERATION_TYPE = C.OPERATION_TYPE         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       UNION ALL       SELECT  CAST(NULL AS VARCHAR2(128)) AS USER_NAME,               CAST(NULL AS VARCHAR2(128)) PROXY_NAME,               C.OPERATION_NAME AS AUDIT_OPTION,               DECODE(A.IN_SUCCESS, 2, 'BY SESSION', 3, 'BY ACCESS', 'NOT SET') AS SUCCESS,               DECODE(A.IN_FAILURE, 2, 'BY SESSION', 3, 'BY ACCESS', 'NOT SET') AS FAILURE       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_OPERATION C       WHERE A.AUDIT_TYPE = 1         AND A.OPERATION_TYPE = C.OPERATION_TYPE   )__"))) {
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

int ObInnerTableSchema::dba_obj_audit_opts_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OBJ_AUDIT_OPTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OBJ_AUDIT_OPTS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT C.DATABASE_NAME AS OWNER,         CAST(B.TABLE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,         CAST('TABLE' AS VARCHAR2(23)) AS OBJECT_TYPE,         MAX(CASE A.OPERATION_TYPE WHEN 39 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS ALT,         MAX(CASE A.OPERATION_TYPE WHEN 40 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS AUD,         MAX(CASE A.OPERATION_TYPE WHEN 41 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS COM,         MAX(CASE A.OPERATION_TYPE WHEN 42 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS DEL,         MAX(CASE A.OPERATION_TYPE WHEN 43 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS GRA,         MAX(CASE A.OPERATION_TYPE WHEN 44 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS IND,         MAX(CASE A.OPERATION_TYPE WHEN 45 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS INS,         MAX(CASE A.OPERATION_TYPE WHEN 46 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS LOC,         MAX(CASE A.OPERATION_TYPE WHEN 47 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REN,         MAX(CASE A.OPERATION_TYPE WHEN 48 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS SEL,         MAX(CASE A.OPERATION_TYPE WHEN 49 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS UPD,         MAX(CASE A.OPERATION_TYPE WHEN 50 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REF,         MAX(CASE A.OPERATION_TYPE WHEN 51 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS EXE,         MAX(CASE A.OPERATION_TYPE WHEN 52 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS CRE,         MAX(CASE A.OPERATION_TYPE WHEN 53 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REA,         MAX(CASE A.OPERATION_TYPE WHEN 54 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS WRI,         MAX(CASE A.OPERATION_TYPE WHEN 55 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS FBK       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT A,            SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,            SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C       WHERE A.AUDIT_TYPE = 4         AND A.OWNER_ID = B.TABLE_ID         AND B.DATABASE_ID = C.DATABASE_ID         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TABLE_TYPE != 12         AND B.TABLE_TYPE != 13       GROUP BY C.DATABASE_NAME,         B.TABLE_NAME,         CAST('TABLE' AS VARCHAR2(23))       UNION ALL       SELECT C.DATABASE_NAME AS OWNER,           CAST(B.SEQUENCE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,           CAST('SEQUENCE' AS VARCHAR2(23)) AS OBJECT_TYPE,           MAX(CASE A.OPERATION_TYPE WHEN 39 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS ALT,           MAX(CASE A.OPERATION_TYPE WHEN 40 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS AUD,           MAX(CASE A.OPERATION_TYPE WHEN 41 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS COM,           MAX(CASE A.OPERATION_TYPE WHEN 42 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS DEL,           MAX(CASE A.OPERATION_TYPE WHEN 43 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS GRA,           MAX(CASE A.OPERATION_TYPE WHEN 44 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS IND,           MAX(CASE A.OPERATION_TYPE WHEN 45 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS INS,           MAX(CASE A.OPERATION_TYPE WHEN 46 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS LOC,           MAX(CASE A.OPERATION_TYPE WHEN 47 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REN,           MAX(CASE A.OPERATION_TYPE WHEN 48 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS SEL,           MAX(CASE A.OPERATION_TYPE WHEN 49 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS UPD,           MAX(CASE A.OPERATION_TYPE WHEN 50 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REF,           MAX(CASE A.OPERATION_TYPE WHEN 51 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS EXE,           MAX(CASE A.OPERATION_TYPE WHEN 52 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS CRE,           MAX(CASE A.OPERATION_TYPE WHEN 53 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REA,           MAX(CASE A.OPERATION_TYPE WHEN 54 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS WRI,           MAX(CASE A.OPERATION_TYPE WHEN 55 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS FBK         FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT A,              SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT B,              SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C         WHERE A.AUDIT_TYPE = 5           AND A.OWNER_ID = B.SEQUENCE_ID           AND B.DATABASE_ID = C.DATABASE_ID           AND B.TENANT_ID = EFFECTIVE_TENANT_ID()           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         GROUP BY C.DATABASE_NAME,           B.SEQUENCE_NAME,           CAST('SEQUENCE' AS VARCHAR2(23))         UNION ALL         SELECT C.DATABASE_NAME AS OWNER,             CAST(B.PACKAGE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,             CAST('PROCEDURE' AS VARCHAR2(23)) AS OBJECT_TYPE,             MAX(CASE A.OPERATION_TYPE WHEN 39 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS ALT,             MAX(CASE A.OPERATION_TYPE WHEN 40 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS AUD,             MAX(CASE A.OPERATION_TYPE WHEN 41 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS COM,             MAX(CASE A.OPERATION_TYPE WHEN 42 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS DEL,             MAX(CASE A.OPERATION_TYPE WHEN 43 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS GRA,             MAX(CASE A.OPERATION_TYPE WHEN 44 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS IND,             MAX(CASE A.OPERATION_TYPE WHEN 45 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS INS,             MAX(CASE A.OPERATION_TYPE WHEN 46 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS LOC,             MAX(CASE A.OPERATION_TYPE WHEN 47 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REN,             MAX(CASE A.OPERATION_TYPE WHEN 48 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS SEL,             MAX(CASE A.OPERATION_TYPE WHEN 49 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS UPD,             MAX(CASE A.OPERATION_TYPE WHEN 50 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REF,             MAX(CASE A.OPERATION_TYPE WHEN 51 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS EXE,             MAX(CASE A.OPERATION_TYPE WHEN 52 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS CRE,             MAX(CASE A.OPERATION_TYPE WHEN 53 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REA,             MAX(CASE A.OPERATION_TYPE WHEN 54 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS WRI,             MAX(CASE A.OPERATION_TYPE WHEN 55 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS FBK           FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT A,                SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT B,                SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C           WHERE A.AUDIT_TYPE = 6             AND A.OWNER_ID = B.PACKAGE_ID             AND B.DATABASE_ID = C.DATABASE_ID             AND B.TENANT_ID = EFFECTIVE_TENANT_ID()             AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           GROUP BY C.DATABASE_NAME,             B.PACKAGE_NAME,             CAST('PROCEDURE' AS VARCHAR2(23))         UNION ALL         SELECT C.DATABASE_NAME AS OWNER,             CAST(B.ROUTINE_NAME AS VARCHAR2(128)) AS OBJECT_NAME,             CAST('PROCEDURE' AS VARCHAR2(23)) AS OBJECT_TYPE,             MAX(CASE A.OPERATION_TYPE WHEN 39 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS ALT,             MAX(CASE A.OPERATION_TYPE WHEN 40 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS AUD,             MAX(CASE A.OPERATION_TYPE WHEN 41 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS COM,             MAX(CASE A.OPERATION_TYPE WHEN 42 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS DEL,             MAX(CASE A.OPERATION_TYPE WHEN 43 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS GRA,             MAX(CASE A.OPERATION_TYPE WHEN 44 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS IND,             MAX(CASE A.OPERATION_TYPE WHEN 45 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS INS,             MAX(CASE A.OPERATION_TYPE WHEN 46 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS LOC,             MAX(CASE A.OPERATION_TYPE WHEN 47 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REN,             MAX(CASE A.OPERATION_TYPE WHEN 48 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS SEL,             MAX(CASE A.OPERATION_TYPE WHEN 49 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS UPD,             MAX(CASE A.OPERATION_TYPE WHEN 50 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REF,             MAX(CASE A.OPERATION_TYPE WHEN 51 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS EXE,             MAX(CASE A.OPERATION_TYPE WHEN 52 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS CRE,             MAX(CASE A.OPERATION_TYPE WHEN 53 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS REA,             MAX(CASE A.OPERATION_TYPE WHEN 54 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS WRI,             MAX(CASE A.OPERATION_TYPE WHEN 55 THEN (DECODE(A.IN_SUCCESS, 2, 'S', 3, 'A', '-') || '/' || DECODE(A.IN_FAILURE, 2, 'S', 3, 'A', '-')) ELSE '-/-' END) AS FBK           FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT A,                SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT B,                SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C           WHERE A.AUDIT_TYPE = 7             AND A.OWNER_ID = B.ROUTINE_ID             AND B.DATABASE_ID = C.DATABASE_ID           GROUP BY C.DATABASE_NAME,             B.ROUTINE_NAME,             CAST('PROCEDURE' AS VARCHAR2(23))    )__"))) {
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

int ObInnerTableSchema::dba_audit_trail_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_AUDIT_TRAIL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_AUDIT_TRAIL_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         A.OBJ_OWNER_NAME AS OWNER,         A.OBJ_NAME AS OBJ_NAME,         A.ACTION_ID AS ACTION,         B.ACTION_NAME AS ACTION_NAME,         A.NEW_OBJ_OWNER_NAME AS NEW_OWNER,         A.NEW_OBJ_NAME AS NEW_NAME,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, NULL,             109 /* revoke sys_priv */, NULL,             114 /* grant  role */, NULL,             115 /* revoke role */, NULL,             A.AUTH_PRIVILEGES) AS OBJ_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.LOGOFF_DEAD_LOCK,             109 /* revoke sys_priv */, A.LOGOFF_DEAD_LOCK,             NULL) AS SYS_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.AUTH_PRIVILEGES,             109 /* revoke sys_priv */, A.AUTH_PRIVILEGES,             114 /* grant  role */, A.AUTH_PRIVILEGES,             115 /* revoke role */, A.AUTH_PRIVILEGES,             NULL) AS ADMIN_OPTION,         A.AUTH_GRANTEE AS GRANTEE,         DECODE(A.ACTION_ID,             104 /* audit   */, A.LOGOFF_DEAD_LOCK,             105 /* noaudit */, A.LOGOFF_DEAD_LOCK,             NULL) AS AUDIT_OPTION,         CAST(NULL AS VARCHAR2(19)) AS SES_ACTIONS,         DECODE(A.ACTION_ID, 201, CAST(A.RECORD_TIMESTAMP_US AS DATE), CAST(NULL AS DATE)) AS LOGOFF_TIME,         A.LOGOFF_LOGICAL_READ AS LOGOFF_LREAD,         CAST(NULL AS NUMBER) AS LOGOFF_PREAD,         CAST(NULL AS NUMBER) AS LOGOFF_LWRITE,         CAST(NULL AS VARCHAR2(40)) AS LOGOFF_DLOCK,         A.COMMENT_TEXT AS COMMENT_TEXT,         A.SESSION_ID AS SESSIONID,         A.ENTRY_ID AS ENTRYID,         A.STATEMENT_ID AS STATEMENTID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS PRIV_USED,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         CAST(NULL AS VARCHAR2(40)) AS ECONTEXT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS,         CAST(A.TRANS_ID AS VARCHAR2(128)) AS TRANSACTIONID,         A.COMMIT_VERSION AS SCN,         CAST(A.SQL_BIND AS VARCHAR2(2000)) AS SQL_BIND,         CAST(A.SQL_TEXT AS VARCHAR2(2000)) AS SQL_TEXT,         CAST(NULL AS VARCHAR2(128)) AS OBJ_EDITION_NAME,         A.DB_ID AS DBID       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::user_audit_trail_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_AUDIT_TRAIL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_AUDIT_TRAIL_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         A.OBJ_OWNER_NAME AS OWNER,         A.OBJ_NAME AS OBJ_NAME,         A.ACTION_ID AS ACTION,         B.ACTION_NAME AS ACTION_NAME,         A.NEW_OBJ_OWNER_NAME AS NEW_OWNER,         A.NEW_OBJ_NAME AS NEW_NAME,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, NULL,             109 /* revoke sys_priv */, NULL,             114 /* grant  role */, NULL,             115 /* revoke role */, NULL,             A.AUTH_PRIVILEGES) AS OBJ_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.LOGOFF_DEAD_LOCK,             109 /* revoke sys_priv */, A.LOGOFF_DEAD_LOCK,             NULL) AS SYS_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.AUTH_PRIVILEGES,             109 /* revoke sys_priv */, A.AUTH_PRIVILEGES,             114 /* grant  role */, A.AUTH_PRIVILEGES,             115 /* revoke role */, A.AUTH_PRIVILEGES,             NULL) AS ADMIN_OPTION,         A.AUTH_GRANTEE AS GRANTEE,         DECODE(A.ACTION_ID,             104 /* audit   */, A.LOGOFF_DEAD_LOCK,             105 /* noaudit */, A.LOGOFF_DEAD_LOCK,             NULL) AS AUDIT_OPTION,         CAST(NULL AS VARCHAR2(19)) AS SES_ACTIONS,         DECODE(A.ACTION_ID, 201, CAST(A.RECORD_TIMESTAMP_US AS DATE), CAST(NULL AS DATE)) AS LOGOFF_TIME,         A.LOGOFF_LOGICAL_READ AS LOGOFF_LREAD,         CAST(NULL AS NUMBER) AS LOGOFF_PREAD,         CAST(NULL AS NUMBER) AS LOGOFF_LWRITE,         CAST(NULL AS VARCHAR2(40)) AS LOGOFF_DLOCK,         A.COMMENT_TEXT AS COMMENT_TEXT,         A.SESSION_ID AS SESSIONID,         A.ENTRY_ID AS ENTRYID,         A.STATEMENT_ID AS STATEMENTID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS PRIV_USED,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         CAST(NULL AS VARCHAR2(40)) AS ECONTEXT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS,         CAST(A.TRANS_ID AS VARCHAR2(128)) AS TRANSACTIONID,         A.COMMIT_VERSION AS SCN,         CAST(A.SQL_BIND AS VARCHAR2(2000)) AS SQL_BIND,         CAST(A.SQL_TEXT AS VARCHAR2(2000)) AS SQL_TEXT,         CAST(NULL AS VARCHAR2(128)) AS OBJ_EDITION_NAME,         A.DB_ID AS DBID       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID             AND A.DB_ID=USERENV('SCHEMAID')       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::dba_audit_exists_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_AUDIT_EXISTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_AUDIT_EXISTS_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         A.OBJ_OWNER_NAME AS OWNER,         A.OBJ_NAME AS OBJ_NAME,         B.ACTION_NAME AS ACTION_NAME,         A.NEW_OBJ_OWNER_NAME AS NEW_OWNER,         A.NEW_OBJ_NAME AS NEW_NAME,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, NULL,             109 /* revoke sys_priv */, NULL,             114 /* grant  role */, NULL,             115 /* revoke role */, NULL,             A.AUTH_PRIVILEGES) AS OBJ_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.LOGOFF_DEAD_LOCK,             109 /* revoke sys_priv */, A.LOGOFF_DEAD_LOCK,             NULL) AS SYS_PRIVILEGE,         A.AUTH_GRANTEE AS GRANTEE,         A.SESSION_ID AS SESSIONID,         A.ENTRY_ID AS ENTRYID,         A.STATEMENT_ID AS STATEMENTID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         CAST(NULL AS VARCHAR2(40)) AS ECONTEXT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS,         CAST(A.TRANS_ID AS VARCHAR2(128)) AS TRANSACTIONID,         A.COMMIT_VERSION AS SCN,         CAST(A.SQL_BIND AS VARCHAR2(2000)) AS SQL_BIND,         CAST(A.SQL_TEXT AS VARCHAR2(2000)) AS SQL_TEXT,         CAST(NULL AS VARCHAR2(128)) AS OBJ_EDITION_NAME       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID          AND A.RETURN_CODE in (942, 943, 959, 1418, 1432, 1434, 1435, 1534, 1917, 1918, 1919,          2019, 2024, 2289, 4042, 4043, 4080, 1, 951, 955, 957, 1430, 1433, 1452, 1471, 1535,          1543, 1758, 1920, 1921, 1922, 2239, 2264, 2266, 2273, 2292, 2297, 2378, 2379, 2382,          4081, 12006, 12325)       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::dba_audit_session_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_AUDIT_SESSION_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_AUDIT_SESSION_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         B.ACTION_NAME AS ACTION_NAME,         DECODE(A.ACTION_ID, 201, CAST(A.RECORD_TIMESTAMP_US AS DATE), CAST(NULL AS DATE)) AS LOGOFF_TIME,         A.LOGOFF_LOGICAL_READ AS LOGOFF_LREAD,         CAST(NULL AS NUMBER) AS LOGOFF_PREAD,         CAST(NULL AS NUMBER) AS LOGOFF_LWRITE,         CAST(NULL AS VARCHAR2(40)) AS LOGOFF_DLOCK,         A.SESSION_ID AS SESSIONID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID         AND A.ACTION_ID BETWEEN 100 and 102       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::user_audit_session_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_AUDIT_SESSION_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_AUDIT_SESSION_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         B.ACTION_NAME AS ACTION_NAME,         DECODE(A.ACTION_ID, 201, CAST(A.RECORD_TIMESTAMP_US AS DATE), CAST(NULL AS DATE)) AS LOGOFF_TIME,         A.LOGOFF_LOGICAL_READ AS LOGOFF_LREAD,         CAST(NULL AS NUMBER) AS LOGOFF_PREAD,         CAST(NULL AS NUMBER) AS LOGOFF_LWRITE,         CAST(NULL AS VARCHAR2(40)) AS LOGOFF_DLOCK,         A.SESSION_ID AS SESSIONID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID         AND A.ACTION_ID BETWEEN 100 AND 102         AND A.DB_ID=USERENV('SCHEMAID')       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::dba_audit_statement_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_AUDIT_STATEMENT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_AUDIT_STATEMENT_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         A.OBJ_OWNER_NAME AS OWNER,         A.OBJ_NAME AS OBJ_NAME,         B.ACTION_NAME AS ACTION_NAME,         A.NEW_OBJ_NAME AS NEW_NAME,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, NULL,             109 /* revoke sys_priv */, NULL,             114 /* grant  role */, NULL,             115 /* revoke role */, NULL,             A.AUTH_PRIVILEGES) AS OBJ_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.LOGOFF_DEAD_LOCK,             109 /* revoke sys_priv */, A.LOGOFF_DEAD_LOCK,             NULL) AS SYS_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.AUTH_PRIVILEGES,             109 /* revoke sys_priv */, A.AUTH_PRIVILEGES,             114 /* grant  role */, A.AUTH_PRIVILEGES,             115 /* revoke role */, A.AUTH_PRIVILEGES,             NULL) AS ADMIN_OPTION,         A.AUTH_GRANTEE AS GRANTEE,         DECODE(A.ACTION_ID,             104 /* audit   */, A.LOGOFF_DEAD_LOCK,             105 /* noaudit */, A.LOGOFF_DEAD_LOCK,             NULL) AS AUDIT_OPTION,         CAST(NULL AS VARCHAR2(19)) AS SES_ACTIONS,         A.COMMENT_TEXT AS COMMENT_TEXT,         A.SESSION_ID AS SESSIONID,         A.ENTRY_ID AS ENTRYID,         A.STATEMENT_ID AS STATEMENTID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS PRIV_USED,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         CAST(NULL AS VARCHAR2(40)) AS ECONTEXT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS,         CAST(A.TRANS_ID AS VARCHAR2(128)) AS TRANSACTIONID,         A.COMMIT_VERSION AS SCN,         CAST(A.SQL_BIND AS VARCHAR2(2000)) AS SQL_BIND,         CAST(A.SQL_TEXT AS VARCHAR2(2000)) AS SQL_TEXT,         CAST(NULL AS VARCHAR2(128)) AS OBJ_EDITION_NAME       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID          AND A.ACTION_ID IN ( 17 /* GRANT OBJECT  */,                               18 /* REVOKE OBJECT */,                               30 /* AUDIT OBJECT */,                               31 /* NOAUDIT OBJECT */,                               49 /* ALTER SYSTEM */,                               104 /* SYSTEM AUDIT */,                               105 /* SYSTEM NOAUDIT */,                               106 /* AUDIT DEFAULT */,                               107 /* NOAUDIT DEFAULT */,                               108 /* SYSTEM GRANT */,                               109 /* SYSTEM REVOKE */,                               114 /* GRANT ROLE */,                               115 /* REVOKE ROLE */ )       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::user_audit_statement_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_AUDIT_STATEMENT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_AUDIT_STATEMENT_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         A.OBJ_OWNER_NAME AS OWNER,         A.OBJ_NAME AS OBJ_NAME,         B.ACTION_NAME AS ACTION_NAME,         A.NEW_OBJ_NAME AS NEW_NAME,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, NULL,             109 /* revoke sys_priv */, NULL,             114 /* grant  role */, NULL,             115 /* revoke role */, NULL,             A.AUTH_PRIVILEGES) AS OBJ_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.LOGOFF_DEAD_LOCK,             109 /* revoke sys_priv */, A.LOGOFF_DEAD_LOCK,             NULL) AS SYS_PRIVILEGE,         DECODE(A.ACTION_ID,             108 /* grant  sys_priv */, A.AUTH_PRIVILEGES,             109 /* revoke sys_priv */, A.AUTH_PRIVILEGES,             114 /* grant  role */, A.AUTH_PRIVILEGES,             115 /* revoke role */, A.AUTH_PRIVILEGES,             NULL) AS ADMIN_OPTION,         A.AUTH_GRANTEE AS GRANTEE,         DECODE(A.ACTION_ID,             104 /* audit   */, A.LOGOFF_DEAD_LOCK,             105 /* noaudit */, A.LOGOFF_DEAD_LOCK,             NULL) AS AUDIT_OPTION,         CAST(NULL AS VARCHAR2(19)) AS SES_ACTIONS,         A.COMMENT_TEXT AS COMMENT_TEXT,         A.SESSION_ID AS SESSIONID,         A.ENTRY_ID AS ENTRYID,         A.STATEMENT_ID AS STATEMENTID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS PRIV_USED,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         CAST(NULL AS VARCHAR2(40)) AS ECONTEXT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS,         CAST(A.TRANS_ID AS VARCHAR2(128)) AS TRANSACTIONID,         A.COMMIT_VERSION AS SCN,         CAST(A.SQL_BIND AS VARCHAR2(2000)) AS SQL_BIND,         CAST(A.SQL_TEXT AS VARCHAR2(2000)) AS SQL_TEXT,         CAST(NULL AS VARCHAR2(128)) AS OBJ_EDITION_NAME       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID          AND A.ACTION_ID IN ( 17 /* GRANT OBJECT  */,                               18 /* REVOKE OBJECT */,                               30 /* AUDIT OBJECT */,                               31 /* NOAUDIT OBJECT */,                               49 /* ALTER SYSTEM */,                               104 /* SYSTEM AUDIT */,                               105 /* SYSTEM NOAUDIT */,                               106 /* AUDIT DEFAULT */,                               107 /* NOAUDIT DEFAULT */,                               108 /* SYSTEM GRANT */,                               109 /* SYSTEM REVOKE */,                               114 /* GRANT ROLE */,                               115 /* REVOKE ROLE */ )          AND A.DB_ID=USERENV('SCHEMAID')       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::dba_audit_object_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_AUDIT_OBJECT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_AUDIT_OBJECT_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         A.OBJ_OWNER_NAME AS OWNER,         A.OBJ_NAME AS OBJ_NAME,         B.ACTION_NAME AS ACTION_NAME,         A.NEW_OBJ_OWNER_NAME AS NEW_OWNER,         A.NEW_OBJ_NAME AS NEW_NAME,         CAST(NULL AS VARCHAR2(19)) AS SES_ACTIONS,         A.COMMENT_TEXT AS COMMENT_TEXT,         A.SESSION_ID AS SESSIONID,         A.ENTRY_ID AS ENTRYID,         A.STATEMENT_ID AS STATEMENTID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS PRIV_USED,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         CAST(NULL AS VARCHAR2(40)) AS ECONTEXT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS,         CAST(A.TRANS_ID AS VARCHAR2(128)) AS TRANSACTIONID,         A.COMMIT_VERSION AS SCN,         CAST(A.SQL_BIND AS VARCHAR2(2000)) AS SQL_BIND,         CAST(A.SQL_TEXT AS VARCHAR2(2000)) AS SQL_TEXT,         CAST(NULL AS VARCHAR2(128)) AS OBJ_EDITION_NAME       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID          AND ((A.ACTION_ID BETWEEN 1 AND 16)           OR (A.ACTION_ID BETWEEN 19 AND 29)           OR (A.ACTION_ID BETWEEN 32 AND 41)           OR (A.ACTION_ID = 43)           OR (A.ACTION_ID BETWEEN 51 AND 99)           OR (A.ACTION_ID = 103)           OR (A.ACTION_ID BETWEEN 110 AND 113)           OR (A.ACTION_ID BETWEEN 116 AND 121)           OR (A.ACTION_ID BETWEEN 123 AND 128)           OR (A.ACTION_ID BETWEEN 160 AND 162))       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::user_audit_object_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_AUDIT_OBJECT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_AUDIT_OBJECT_ORA_TNAME))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         CAST(NULL AS VARCHAR2(255)) AS OS_USERNAME,         A.USER_NAME AS USERNAME,         A.CLIENT_IP AS USERHOST,         CAST(NULL AS VARCHAR2(255)) AS TERMINAL,         CAST(A.RECORD_TIMESTAMP_US AS DATE) AS TIMESTAMP,         A.OBJ_OWNER_NAME AS OWNER,         A.OBJ_NAME AS OBJ_NAME,         B.ACTION_NAME AS ACTION_NAME,         A.NEW_OBJ_OWNER_NAME AS NEW_OWNER,         A.NEW_OBJ_NAME AS NEW_NAME,         CAST(NULL AS VARCHAR2(19)) AS SES_ACTIONS,         A.COMMENT_TEXT AS COMMENT_TEXT,         A.SESSION_ID AS SESSIONID,         A.ENTRY_ID AS ENTRYID,         A.STATEMENT_ID AS STATEMENTID,         A.RETURN_CODE AS RETURNCODE,         CAST(NULL AS VARCHAR2(40)) AS PRIV_USED,         CAST(NULL AS VARCHAR2(40)) AS CLIENT_ID,         CAST(NULL AS VARCHAR2(40)) AS ECONTEXT_ID,         A.LOGOFF_CPU_TIME_US AS SESSION_CPU,         CAST(A.RECORD_TIMESTAMP_US AS TIMESTAMP(6) WITH TIME ZONE) AS EXTENDED_TIMESTAMP,         A.PROXY_SESSION_ID AS PROXY_SESSIONID,         A.USER_ID AS GLOBAL_UID,         CAST(NULL AS VARCHAR2(40)) AS INSTANCE_NUMBER,         CAST(NULL AS VARCHAR2(40)) AS OS_PROCESS,         CAST(A.TRANS_ID AS VARCHAR2(128)) AS TRANSACTIONID,         A.COMMIT_VERSION AS SCN,         CAST(A.SQL_BIND AS VARCHAR2(2000)) AS SQL_BIND,         CAST(A.SQL_TEXT AS VARCHAR2(2000)) AS SQL_TEXT,         CAST(NULL AS VARCHAR2(128)) AS OBJ_EDITION_NAME       FROM SYS.ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT A,            SYS.ALL_VIRTUAL_AUDIT_ACTION B       WHERE A.ACTION_ID = B.ACTION_ID          AND ((A.ACTION_ID BETWEEN 1 AND 16)           OR (A.ACTION_ID BETWEEN 19 AND 29)           OR (A.ACTION_ID BETWEEN 32 AND 41)           OR (A.ACTION_ID = 43)           OR (A.ACTION_ID BETWEEN 51 AND 99)           OR (A.ACTION_ID = 103)           OR (A.ACTION_ID BETWEEN 110 AND 113)           OR (A.ACTION_ID BETWEEN 116 AND 121)           OR (A.ACTION_ID BETWEEN 123 AND 128)           OR (A.ACTION_ID BETWEEN 160 AND 162))          AND A.DB_ID=USERENV('SCHEMAID')       ORDER BY A.RECORD_TIMESTAMP_US   )__"))) {
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

int ObInnerTableSchema::dba_col_privs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_COL_PRIVS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         u_grantee.user_name as GRANTEE,         db.database_name as OWNER,         decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,         c.column_name as COLUMN_NAME,         u_grantor.user_name as GRANTOR,         cast (decode(auth.priv_id, 1, 'ALTER',                              2, 'AUDIT',                              3, 'COMMENT',                              4, 'DELETE',                              5, 'GRANT',                              6, 'INDEX',                              7, 'INSERT',                              8, 'LOCK',                              9, 'RENAME',                              10, 'SELECT',                              11, 'UPDATE',                              12, 'REFERENCES',                              13, 'EXECUTE',                              14, 'CREATE',                              15, 'FLASHBACK',                              16, 'READ',                              17, 'WRITE',                              'OTHERS') as varchar(40)) as PRIVILEGE,         decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE       FROM         SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT auth,         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c,         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t,         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,         SYS.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,         SYS.ALL_VIRTUAL_USER_REAL_AGENT u_grantee       WHERE         auth.col_id = c.column_id and         auth.obj_id = t.table_id and         auth.objtype = 1 and         auth.obj_id = c.table_id and         db.database_id = t.database_id and         u_grantor.user_id = auth.grantor_id and         u_grantee.user_id = auth.grantee_id         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID() 	      AND c.column_id != 65535         AND t.table_type != 12         AND t.table_type != 13   )__"))) {
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

int ObInnerTableSchema::user_col_privs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_COL_PRIVS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         u_grantee.user_name as GRANTEE,         db.database_name as OWNER,         decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,         c.column_name as COLUMN_NAME,         u_grantor.user_name as GRANTOR,         cast (decode(auth.priv_id, 1, 'ALTER',                              2, 'AUDIT',                              3, 'COMMENT',                              4, 'DELETE',                              5, 'GRANT',                              6, 'INDEX',                              7, 'INSERT',                              8, 'LOCK',                              9, 'RENAME',                              10, 'SELECT',                              11, 'UPDATE',                              12, 'REFERENCES',                              13, 'EXECUTE',                              14, 'CREATE',                              15, 'FLASHBACK',                              16, 'READ',                              17, 'WRITE',                              'OTHERS') as varchar(40)) as PRIVILEGE,         decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE       FROM         SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT auth,         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c,         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t,         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,         SYS.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,         SYS.ALL_VIRTUAL_USER_REAL_AGENT u_grantee       WHERE         auth.col_id = c.column_id and         auth.obj_id = t.table_id and         auth.objtype = 1 and         auth.obj_id = c.table_id and         db.database_id = t.database_id and         u_grantor.user_id = auth.grantor_id and         u_grantee.user_id = auth.grantee_id         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()         AND c.column_id != 65535 and 	      (db.database_name = SYS_CONTEXT('USERENV','CURRENT_USER') or          u_grantee.user_name = SYS_CONTEXT('USERENV','CURRENT_USER') or          u_grantor.user_name = SYS_CONTEXT('USERENV','CURRENT_USER')         )         AND t.table_type != 12         AND t.table_type != 13   )__"))) {
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

int ObInnerTableSchema::all_col_privs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_COL_PRIVS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT 	u_grantor.user_name as GRANTOR,         u_grantee.user_name as GRANTEE, 	db.database_name as OWNER, 	decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,         c.column_name as COLUMN_NAME,         cast (decode(auth.priv_id, 1, 'ALTER',                              2, 'AUDIT',                              3, 'COMMENT',                              4, 'DELETE',                              5, 'GRANT',                              6, 'INDEX',                              7, 'INSERT',                              8, 'LOCK',                              9, 'RENAME',                              10, 'SELECT',                              11, 'UPDATE',                              12, 'REFERENCES',                              13, 'EXECUTE',                              14, 'CREATE',                              15, 'FLASHBACK',                              16, 'READ',                              17, 'WRITE',                              'OTHERS') as varchar(40)) as PRIVILEGE,         decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE       FROM         SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT auth,         SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c,         SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t,         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,         SYS.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,         SYS.ALL_VIRTUAL_USER_REAL_AGENT u_grantee       WHERE         auth.col_id = c.column_id and         auth.obj_id = t.table_id and         auth.objtype = 1 and         auth.obj_id = c.table_id and         db.database_id = t.database_id and         u_grantor.user_id = auth.grantor_id and         u_grantee.user_id = auth.grantee_id         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()         AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()         AND c.column_id != 65535         AND t.table_type != 12         AND t.table_type != 13   )__"))) {
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

int ObInnerTableSchema::role_tab_privs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ROLE_TAB_PRIVS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT         U_GRANTEE.USER_NAME AS ROLE,         DB.DATABASE_NAME AS OWNER,           CAST (DECODE(AUTH.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),                         T.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,         C.COLUMN_NAME AS COLUMN_NAME,         CAST (DECODE(AUTH.PRIV_ID, 1, 'ALTER',                              2, 'AUDIT',                              3, 'COMMENT',                              4, 'DELETE',                              5, 'GRANT',                              6, 'INDEX',                              7, 'INSERT',                              8, 'LOCK',                              9, 'RENAME',                              10, 'SELECT',                              11, 'UPDATE',                              12, 'REFERENCES',                              13, 'EXECUTE',                              14, 'CREATE',                              15, 'FLASHBACK',                              16, 'READ',                              17, 'WRITE',                              'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,         DECODE(AUTH.PRIV_OPTION, 0, 'NO', 1, 'YES', '') AS GRANTABLE       FROM         (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) AUTH         LEFT JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C           ON AUTH.COL_ID = C.COLUMN_ID AND AUTH.OBJ_ID = C.TABLE_ID             AND C.TENANT_ID = EFFECTIVE_TENANT_ID(),         (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID, decode(table_type, 5,11,1) AS OBJ_TYPE                  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID() AND TABLE_TYPE != 12 AND TABLE_TYPE != 13         UNION ALL         SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID, 3 AS OBJ_TYPE           FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()         UNION ALL         SELECT PACKAGE_ID AS TABLE_ID,                PACKAGE_NAME AS TABLE_NAME,                DATABASE_ID AS DATABASE_ID,                14 AS OBJ_TYPE           FROM SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT         UNION ALL         SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID,               DECODE(ROUTINE_TYPE, 1, 12, 2, 9, 0) AS OBJ_TYPE           FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()         UNION ALL         SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID, 2 AS OBJ_TYPE           FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()         UNION ALL         SELECT TYPE_ID AS TABLE_ID, TYPE_NAME AS TABLE_NAME, DATABASE_ID, 4 AS OBJ_TYPE           FROM SYS.ALL_VIRTUAL_TYPE_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()         UNION ALL         SELECT DIRECTORY_ID AS TABLE_ID, DIRECTORY_NAME AS TABLE_NAME,           (SELECT DATABASE_ID FROM SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT WHERE DATABASE_NAME = 'SYS'),           10 AS OBJ_TYPE           FROM SYS.ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()         ) T,         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB,         SYS.ALL_VIRTUAL_USER_REAL_AGENT U_GRANTEE       WHERE         AUTH.OBJ_ID = T.TABLE_ID          AND AUTH.OBJTYPE = T.OBJ_TYPE          AND DB.DATABASE_ID = T.DATABASE_ID          AND U_GRANTEE.USER_ID = AUTH.GRANTEE_ID          AND U_GRANTEE.TYPE = 1          AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()          AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::role_sys_privs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ROLE_SYS_PRIVS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(       select 	u.user_name as ROLE,         CAST (DECODE(AUTH.PRIV_ID,                 1, 'CREATE SESSION',                 2, 'EXEMPT REDACTION POLICY',                 3, 'SYSDBA',                 4, 'SYSOPER',                 5, 'SYSBACKUP',                 6, 'CREATE TABLE',                 7, 'CREATE ANY TABLE',                 8, 'ALTER ANY TABLE',                 9, 'BACKUP ANY TABLE',                 10, 'DROP ANY TABLE',                 11, 'LOCK ANY TABLE',                 12, 'COMMENT ANY TABLE',                 13, 'SELECT ANY TABLE',                 14, 'INSERT ANY TABLE',                 15, 'UPDATE ANY TABLE',                 16, 'DELETE ANY TABLE',                 17, 'FLASHBACK ANY TABLE',                 18, 'CREATE ROLE',                 19, 'DROP ANY ROLE',                 20, 'GRANT ANY ROLE',                 21, 'ALTER ANY ROLE',                 22, 'AUDIT ANY',                 23, 'GRANT ANY PRIVILEGE',                 24, 'GRANT ANY OBJECT PRIVILEGE',                 25, 'CREATE ANY INDEX',                 26, 'ALTER ANY INDEX',                 27, 'DROP ANY INDEX',                 28, 'CREATE ANY VIEW',                 29, 'DROP ANY VIEW',                 30, 'CREATE VIEW',                 31, 'SELECT ANY DICTIONARY',                 32, 'CREATE PROCEDURE',                 33, 'CREATE ANY PROCEDURE',                 34, 'ALTER ANY PROCEDURE',                 35, 'DROP ANY PROCEDURE',                 36, 'EXECUTE ANY PROCEDURE',                 37, 'CREATE SYNONYM',                 38, 'CREATE ANY SYNONYM',                 39, 'DROP ANY SYNONYM',                 40, 'CREATE PUBLIC SYNONYM',                 41, 'DROP PUBLIC SYNONYM',                 42, 'CREATE SEQUENCE',                 43, 'CREATE ANY SEQUENCE',                 44, 'ALTER ANY SEQUENCE',                 45, 'DROP ANY SEQUENCE',                 46, 'SELECT ANY SEQUENCE',                 47, 'CREATE TRIGGER',                 48, 'CREATE ANY TRIGGER',                 49, 'ALTER ANY TRIGGER',                 50, 'DROP ANY TRIGGER',                 51, 'CREATE PROFILE',                 52, 'ALTER PROFILE',                 53, 'DROP PROFILE',                 54, 'CREATE USER',                 55, 'BECOME USER',                 56, 'ALTER USER',                 57, 'DROP USER',                 58, 'CREATE TYPE',                 59, 'CREATE ANY TYPE',                 60, 'ALTER ANY TYPE',                 61, 'DROP ANY TYPE',                 62, 'EXECUTE ANY TYPE',                 63, 'UNDER ANY TYPE',                 64, 'PURGE DBA_RECYCLEBIN',                 65, 'CREATE ANY OUTLINE',                 66, 'ALTER ANY OUTLINE',                 67, 'DROP ANY OUTLINE',                 68, 'SYSKM',                 69, 'CREATE TABLESPACE',                 70, 'ALTER TABLESPACE',                 71, 'DROP TABLESPACE',                 72, 'SHOW PROCESS',                 73, 'ALTER SYSTEM',                 74, 'CREATE DATABASE LINK',                 75, 'CREATE PUBLIC DATABASE LINK',                 76, 'DROP DATABASE LINK',                 77, 'ALTER SESSION',                 78, 'ALTER DATABASE',                 79, 'CREATE ANY DIRECTORY',                 80, 'DROP ANY DIRECTORY',                 81, 'DEBUG CONNECT SESSION',                 82, 'DEBUG ANY PROCEDURE',                 83, 'CREATE ANY CONTEXT',                 84, 'DROP ANY CONTEXT',                 'OTHER') AS VARCHAR(40)) AS PRIVILEGE ,        	decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as ADMIN_OPTION       FROM 	SYS.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT auth, 	SYS.ALL_VIRTUAL_USER_REAL_AGENT u       WHERE 	auth.grantee_id = u.user_id and   u.type = 1   AND U.TENANT_ID = EFFECTIVE_TENANT_ID()   AND AUTH.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::role_role_privs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ROLE_ROLE_PRIVS_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     U_ROLE.USER_NAME AS ROLE,     U_GRANTEE.USER_NAME AS GRANTED_ROLE,     DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') AS ADMIN_OPTION   FROM 	SYS.ALL_VIRTUAL_USER_REAL_AGENT U_GRANTEE, 	SYS.ALL_VIRTUAL_USER_REAL_AGENT U_ROLE, 	(SELECT * FROM SYS.ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT    CONNECT BY PRIOR ROLE_ID = GRANTEE_ID AND ROLE_ID != 200009    START WITH GRANTEE_ID = UID AND ROLE_ID != 200009) R   WHERE 	R.GRANTEE_ID = U_ROLE.USER_ID   AND R.ROLE_ID = U_GRANTEE.USER_ID   AND U_ROLE.TYPE = 1   AND U_GRANTEE.TYPE = 1   AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()   AND U_ROLE.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::dictionary_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DICTIONARY_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,       CAST(CASE TABLE_NAME         WHEN 'DBA_COL_PRIVS' THEN 'All grants on columns in the database'         WHEN 'USER_COL_PRIVS' THEN 'Grants on columns for which the user is the owner, grantor or grantee'         WHEN 'ALL_COL_PRIVS' THEN 'Grants on columns for which the user is the grantor, grantee, owner, or an enabled role or PUBLIC is the grantee'         WHEN 'ROLE_TAB_PRIVS' THEN 'Table privileges granted to roles'         WHEN 'ROLE_SYS_PRIVS' THEN 'System privileges granted to roles'         WHEN 'ROLE_ROLE_PRIVS' THEN 'Roles which are granted to roles'         WHEN 'DBA_SYNONYMS' THEN 'All synonyms in the database'         WHEN 'DBA_OBJECTS' THEN 'All objects in the database'         WHEN 'ALL_OBJECTS' THEN 'Objects accessible to the user'         WHEN 'USER_OBJECTS' THEN 'Objects owned by the user'         WHEN 'DBA_SEQUENCES' THEN 'Description of all SEQUENCEs in the database'         WHEN 'ALL_SEQUENCES' THEN 'Description of SEQUENCEs accessible to the user'         WHEN 'USER_SEQUENCES' THEN 'Description of the user''s own SEQUENCEs'         WHEN 'DBA_USERS' THEN 'Information about all users of the database'         WHEN 'ALL_USERS' THEN 'Information about all users of the database'         WHEN 'ALL_SYNONYMS' THEN 'All synonyms for base objects accessible to the user and session'         WHEN 'USER_SYNONYMS' THEN 'The user''s private synonyms'         WHEN 'DBA_IND_COLUMNS' THEN 'COLUMNs comprising INDEXes on all TABLEs and CLUSTERs'         WHEN 'ALL_IND_COLUMNS' THEN 'COLUMNs comprising INDEXes on accessible TABLES'         WHEN 'USER_IND_COLUMNS' THEN 'COLUMNs comprising user''s INDEXes and INDEXes on user''s TABLES'         WHEN 'DBA_CONSTRAINTS' THEN 'Constraint definitions on all tables'         WHEN 'ALL_CONSTRAINTS' THEN 'Constraint definitions on accessible tables'         WHEN 'USER_CONSTRAINTS' THEN 'Constraint definitions on user''s own tables'         WHEN 'ALL_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'DBA_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'USER_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'ALL_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'DBA_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'USER_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'         WHEN 'ALL_TABLES' THEN 'Description of relational tables accessible to the user'         WHEN 'DBA_TABLES' THEN 'Description of all relational tables in the database'         WHEN 'USER_TABLES' THEN 'Description of the user''s own relational tables'         WHEN 'DBA_TAB_COMMENTS' THEN 'Comments on all tables and views in the database'         WHEN 'ALL_TAB_COMMENTS' THEN 'Comments on tables and views accessible to the user'         WHEN 'USER_TAB_COMMENTS' THEN 'Comments on the tables and views owned by the user'         WHEN 'DBA_COL_COMMENTS' THEN 'Comments on columns of all tables and views'         WHEN 'ALL_COL_COMMENTS' THEN 'Comments on columns of accessible tables and views'         WHEN 'USER_COL_COMMENTS' THEN 'Comments on columns of user''s tables and views'         WHEN 'DBA_INDEXES' THEN 'Description for all indexes in the database'         WHEN 'ALL_INDEXES' THEN 'Descriptions of indexes on tables accessible to the user'         WHEN 'USER_INDEXES' THEN 'Description of the user''s own indexes'         WHEN 'DBA_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'         WHEN 'ALL_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'         WHEN 'USER_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'         WHEN 'USER_SEGMENTS' THEN 'Storage allocated for all database segments'         WHEN 'DBA_SEGMENTS' THEN 'Storage allocated for all database segments'         WHEN 'DBA_TYPES' THEN 'Description of all types in the database'         WHEN 'ALL_TYPES' THEN 'Description of types accessible to the user'         WHEN 'USER_TYPES' THEN 'Description of the user''s own types'         WHEN 'DBA_TYPE_ATTRS' THEN 'Description of attributes of all types in the database'         WHEN 'ALL_TYPE_ATTRS' THEN 'Description of attributes of types accessible to the user'         WHEN 'USER_TYPE_ATTRS' THEN 'Description of attributes of the user''s own types'         WHEN 'DBA_COLL_TYPES' THEN 'Description of all named collection types in the database'         WHEN 'ALL_COLL_TYPES' THEN 'Description of named collection types accessible to the user'         WHEN 'USER_COLL_TYPES' THEN 'Description of the user''s own named collection types'         WHEN 'DBA_PROCEDURES' THEN 'Description of the dba functions/procedures/packages/types/triggers'         WHEN 'DBA_ARGUMENTS' THEN 'All arguments for objects in the database'         WHEN 'DBA_SOURCE' THEN 'Source of all stored objects in the database'         WHEN 'ALL_PROCEDURES' THEN 'Functions/procedures/packages/types/triggers available to the user'         WHEN 'ALL_ARGUMENTS' THEN 'Arguments in object accessible to the user'         WHEN 'ALL_SOURCE' THEN 'Current source on stored objects that user is allowed to create'         WHEN 'USER_PROCEDURES' THEN 'Description of the user functions/procedures/packages/types/triggers'         WHEN 'USER_ARGUMENTS' THEN 'Arguments in object accessible to the user'         WHEN 'USER_SOURCE' THEN 'Source of stored objects accessible to the user'         WHEN 'ALL_ALL_TABLES' THEN 'Description of all object and relational tables accessible to the user'         WHEN 'DBA_ALL_TABLES' THEN 'Description of all object and relational tables in the database'         WHEN 'USER_ALL_TABLES' THEN 'Description of all object and relational tables owned by the user''s'         WHEN 'DBA_PROFILES' THEN 'Display all profiles and their limits'         WHEN 'ALL_MVIEW_COMMENTS' THEN 'Comments on materialized views accessible to the user'         WHEN 'USER_MVIEW_COMMENTS' THEN 'Comments on materialized views owned by the user'         WHEN 'DBA_MVIEW_COMMENTS' THEN 'Comments on all materialized views in the database'         WHEN 'ALL_SCHEDULER_PROGRAM_ARGS' THEN 'All arguments of all scheduler programs visible to the user'         WHEN 'ALL_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'         WHEN 'DBA_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'         WHEN 'USER_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'         WHEN 'DBA_VIEWS' THEN 'Description of all views in the database'         WHEN 'ALL_VIEWS' THEN 'Description of views accessible to the user'         WHEN 'USER_VIEWS' THEN 'Description of the user''s own views'         WHEN 'ALL_ERRORS' THEN 'Current errors on stored objects that user is allowed to create'         WHEN 'USER_ERRORS' THEN 'Current errors on stored objects owned by the user'         WHEN 'ALL_TYPE_METHODS' THEN 'Description of methods of types accessible to the user'         WHEN 'DBA_TYPE_METHODS' THEN 'Description of methods of all types in the database'         WHEN 'USER_TYPE_METHODS' THEN 'Description of methods of the user''s own types'         WHEN 'ALL_METHOD_PARAMS' THEN 'Description of method parameters of types accessible to the user'         WHEN 'DBA_METHOD_PARAMS' THEN 'Description of method parameters of all types in the database'         WHEN 'USER_TABLESPACES' THEN 'Description of accessible tablespaces'         WHEN 'DBA_IND_EXPRESSIONS' THEN 'FUNCTIONAL INDEX EXPRESSIONs on all TABLES and CLUSTERS'         WHEN 'ALL_IND_EXPRESSIONS' THEN 'FUNCTIONAL INDEX EXPRESSIONs on accessible TABLES'         WHEN 'DBA_ROLE_PRIVS' THEN 'Roles granted to users and roles'         WHEN 'USER_ROLE_PRIVS' THEN 'Roles granted to current user'         WHEN 'DBA_TAB_PRIVS' THEN 'All grants on objects in the database'         WHEN 'ALL_TAB_PRIVS' THEN 'Grants on objects for which the user is the grantor, grantee, owner,'         WHEN 'DBA_SYS_PRIVS' THEN 'System privileges granted to users and roles'         WHEN 'USER_SYS_PRIVS' THEN 'System privileges granted to current user'         WHEN 'AUDIT_ACTIONS' THEN 'Description table for audit trail action type codes.  Maps action type numbers to action type names'         WHEN 'ALL_DEF_AUDIT_OPTS' THEN 'Auditing options for newly created objects'         WHEN 'DBA_STMT_AUDIT_OPTS' THEN 'Describes current system auditing options across the system and by user'         WHEN 'DBA_OBJ_AUDIT_OPTS' THEN 'Auditing options for all tables and views with atleast one option set'         WHEN 'DBA_AUDIT_TRAIL' THEN 'All audit trail entries'         WHEN 'USER_AUDIT_TRAIL' THEN 'Audit trail entries relevant to the user'         WHEN 'DBA_AUDIT_EXISTS' THEN 'Lists audit trail entries produced by AUDIT NOT EXISTS and AUDIT EXISTS'         WHEN 'DBA_AUDIT_STATEMENT' THEN 'Audit trail records concerning grant, revoke, audit, noaudit and alter system'         WHEN 'USER_AUDIT_STATEMENT' THEN 'Audit trail records concerning  grant, revoke, audit, noaudit and alter system'         WHEN 'DBA_AUDIT_OBJECT' THEN 'Audit trail records for statements concerning objects, specifically: table, cluster, view, index, sequence,  [public] database link, [public] synonym, procedure, trigger, rollback segment, tablespace, role, user'         WHEN 'USER_AUDIT_OBJECT' THEN 'Audit trail records for statements concerning objects, specifically: table, cluster, view, index, sequence,  [public] database link, [public] synonym, procedure, trigger, rollback segment, tablespace, role, user'         WHEN 'ALL_DEPENDENCIES' THEN 'Describes dependencies between procedures, packages,functions, package bodies, and triggers accessible to the current user,including dependencies on views created without any database links'         WHEN 'DBA_DEPENDENCIES' THEN 'Describes all dependencies in the database between procedures,packages, functions, package bodies, and triggers, including dependencies on views created without any database links'         WHEN 'USER_DEPENDENCIES' THEN 'Describes dependencies between procedures, packages, functions, package bodies, and triggers owned by the current user, including dependencies on views created without any database links'         WHEN 'GV$INSTANCE' THEN 'Synonym for GV_$INSTANCE'         WHEN 'V$INSTANCE' THEN 'Synonym for V_$INSTANCE'         WHEN 'GV$SESSION_WAIT' THEN 'Synonym for GV_$SESSION_WAIT'         WHEN 'V$SESSION_WAIT' THEN 'Synonym for V_$SESSION_WAIT'         WHEN 'GV$SESSION_WAIT_HISTORY' THEN 'Synonym for GV_$SESSION_WAIT_HISTORY'         WHEN 'V$SESSION_WAIT_HISTORY' THEN 'Synonym for V_$SESSION_WAIT_HISTORY'         WHEN 'GV$SESSTAT' THEN 'Synonym for GV_$SESSTAT'         WHEN 'V$SESSTAT' THEN 'Synonym for V_$SESSTAT'         WHEN 'GV$SYSSTAT' THEN 'Synonym for GV_$SYSSTAT'         WHEN 'V$SYSSTAT' THEN 'Synonym for V_$SYSSTAT'         WHEN 'GV$SYSTEM_EVENT' THEN 'Synonym for GV_$SYSTEM_EVENT'         WHEN 'V$SYSTEM_EVENT' THEN 'Synonym for V_$SYSTEM_EVENT'         WHEN 'NLS_SESSION_PARAMETERS' THEN 'NLS parameters of the user session'         WHEN 'NLS_DATABASE_PARAMETERS' THEN 'Permanent NLS parameters of the database'         WHEN 'V$NLS_PARAMETERS' THEN 'Synonym for V_$NLS_PARAMETERS'         WHEN 'V$VERSION' THEN 'Synonym for V_$VERSION'         WHEN 'GV$SQL_WORKAREA' THEN 'Synonym for GV_$SQL_WORKAREA'         WHEN 'V$SQL_WORKAREA' THEN 'Synonym for V_$SQL_WORKAREA'         WHEN 'GV$SQL_WORKAREA_ACTIVE' THEN 'Synonym for GV_$SQL_WORKAREA_ACTIVE'         WHEN 'V$SQL_WORKAREA_ACTIVE' THEN 'Synonym for V_$SQL_WORKAREA_ACTIVE'         WHEN 'GV$SQL_WORKAREA_HISTOGRAM' THEN 'Synonym for GV_$SQL_WORKAREA_HISTOGRAM'         WHEN 'V$SQL_WORKAREA_HISTOGRAM' THEN 'Synonym for V_$SQL_WORKAREA_HISTOGRAM'         WHEN 'DICT' THEN 'Synonym for DICTIONARY'         WHEN 'DICTIONARY' THEN 'Description of data dictionary tables and views'         WHEN 'DBA_RECYCLEBIN' THEN 'Description of the Recyclebin view accessible to the user'         WHEN 'USER_RECYCLEBIN' THEN 'User view of his recyclebin'         WHEN 'V$OB_PX_WORKER_STAT' THEN ''         WHEN 'GV$OB_PS_STAT' THEN ''         WHEN 'V$OB_PS_STAT' THEN ''         WHEN 'GV$OB_PS_ITEM_INFO' THEN ''         WHEN 'V$OB_PS_ITEM_INFO' THEN ''         WHEN 'GV$OB_SQL_WORKAREA_MEMORY_INFO' THEN ''         WHEN 'V$OB_SQL_WORKAREA_MEMORY_INFO' THEN ''         WHEN 'DBA_PART_KEY_COLUMNS' THEN ''         WHEN 'ALL_PART_KEY_COLUMNS' THEN ''         WHEN 'USER_PART_KEY_COLUMNS' THEN ''         WHEN 'DBA_SUBPART_KEY_COLUMNS' THEN ''         WHEN 'ALL_SUBPART_KEY_COLUMNS' THEN ''         WHEN 'USER_SUBPART_KEY_COLUMNS' THEN ''         WHEN 'ALL_TAB_PARTITIONS' THEN ''         WHEN 'ALL_TAB_SUBPARTITIONS' THEN ''         WHEN 'ALL_PART_TABLES' THEN ''         WHEN 'DBA_PART_TABLES' THEN ''         WHEN 'USER_PART_TABLES' THEN ''         WHEN 'DBA_TAB_PARTITIONS' THEN ''         WHEN 'USER_TAB_PARTITIONS' THEN ''         WHEN 'DBA_TAB_SUBPARTITIONS' THEN ''         WHEN 'USER_TAB_SUBPARTITIONS' THEN ''         WHEN 'DBA_SUBPARTITION_TEMPLATES' THEN ''         WHEN 'ALL_SUBPARTITION_TEMPLATES' THEN ''         WHEN 'USER_SUBPARTITION_TEMPLATES' THEN ''         WHEN 'DBA_PART_INDEXES' THEN ''         WHEN 'ALL_PART_INDEXES' THEN ''         WHEN 'USER_PART_INDEXES' THEN ''         WHEN 'ALL_TAB_COLS_V$' THEN ''         WHEN 'DBA_TAB_COLS_V$' THEN ''         WHEN 'USER_TAB_COLS_V$' THEN ''         WHEN 'USER_PROFILES' THEN ''         WHEN 'ALL_PROFILES' THEN ''         WHEN 'DBA_SCHEDULER_PROGRAM_ARGS' THEN ''         WHEN 'USER_SCHEDULER_PROGRAM_ARGS' THEN ''         WHEN 'USER_IND_EXPRESSIONS' THEN ''         WHEN 'DBA_ERRORS' THEN ''         WHEN 'USER_METHOD_PARAMS' THEN ''         WHEN 'DBA_TABLESPACES' THEN ''         WHEN 'ALL_IND_PARTITIONS' THEN ''         WHEN 'USER_IND_PARTITIONS' THEN ''         WHEN 'DBA_IND_PARTITIONS' THEN ''         WHEN 'DBA_IND_SUBPARTITIONS' THEN ''         WHEN 'ALL_IND_SUBPARTITIONS' THEN ''         WHEN 'USER_IND_SUBPARTITIONS' THEN ''         WHEN 'DBA_ROLES' THEN ''         WHEN 'USER_TAB_PRIVS' THEN ''         WHEN 'STMT_AUDIT_OPTION_MAP' THEN ''         WHEN 'DBA_OB_OUTLINES' THEN ''         WHEN 'GV$OB_SQL_AUDIT' THEN ''         WHEN 'V$OB_SQL_AUDIT' THEN ''         WHEN 'DBA_AUDIT_SESSION' THEN ''         WHEN 'USER_AUDIT_SESSION' THEN ''         WHEN 'GV$OB_PLAN_CACHE_PLAN_STAT' THEN ''         WHEN 'V$OB_PLAN_CACHE_PLAN_STAT' THEN ''         WHEN 'GV$OB_PLAN_CACHE_PLAN_EXPLAIN' THEN ''         WHEN 'V$OB_PLAN_CACHE_PLAN_EXPLAIN' THEN ''         WHEN 'GV$OB_MEMSTORE' THEN ''         WHEN 'V$OB_MEMSTORE' THEN ''         WHEN 'GV$OB_MEMSTORE_INFO' THEN 'Dynamic performance view about memtables'         WHEN 'V$OB_MEMSTORE_INFO' THEN 'Dynamic performance view about memtables'         WHEN 'GV$OB_MEMORY' THEN ''         WHEN 'V$OB_MEMORY' THEN ''         WHEN 'GV$OB_PLAN_CACHE_STAT' THEN ''         WHEN 'V$OB_PLAN_CACHE_STAT' THEN ''         WHEN 'DBA_OB_CONCURRENT_LIMIT_SQL' THEN ''         WHEN 'NLS_INSTANCE_PARAMETERS' THEN ''         WHEN 'GV$OB_PX_WORKER_STAT' THEN ''         ELSE NULL END AS VARCHAR2(4000)) AS COMMENTS     FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT     WHERE TABLE_ID > 25000 AND TABLE_ID <= 30000         AND TABLE_TYPE = 1 )__"))) {
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
