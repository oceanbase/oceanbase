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

int ObInnerTableSchema::dict_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DICT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DICT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT * FROM SYS.DICTIONARY )__"))) {
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

int ObInnerTableSchema::all_triggers_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TRIGGERS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TRIGGERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT DB1.DATABASE_NAME AS OWNER,        TRG.TRIGGER_NAME AS TRIGGER_NAME,        CAST(DECODE(BITAND(TRG.TIMING_POINTS, 30),                    4, 'BEFORE EACH ROW',                    8, 'AFTER EACH ROW')             AS VARCHAR2(16)) AS TRIGGER_TYPE,        CAST(DECODE(TRG.TRIGGER_EVENTS,                    2, 'INSERT',                    4, 'UPDATE',                    8, 'DELETE',                    2 + 4, 'INSERT OR UPDATE',                    2 + 8, 'INSERT OR DELETE',                    4 + 8, 'UPDATE OR DELETE',                    2 + 4 + 8, 'INSERT OR UPDATE OR DELETE')             AS VARCHAR2(246)) AS TRIGGERING_EVENT,        DB2.DATABASE_NAME AS TABLE_OWNER,        CAST(DECODE(TRG.BASE_OBJECT_TYPE,                    5, 'TABLE')             AS VARCHAR2(18)) AS BASE_OBJECT_TYPE,        TBL.TABLE_NAME AS TABLE_NAME,        CAST(NULL AS VARCHAR2(4000)) AS COLUMN_NAME,        CAST(CONCAT('REFERENCING', CONCAT(CONCAT(' NEW AS ', REF_NEW_NAME), CONCAT(' OLD AS ', REF_OLD_NAME)))             AS VARCHAR2(422)) AS REFERENCING_NAMES,        WHEN_CONDITION AS WHEN_CLAUSE,        CAST(decode(BITAND(TRG.trigger_flags, 1), 1, 'ENABLED', 'DISABLED') AS VARCHAR2(8)) AS STATUS,        TRIGGER_BODY AS DESCRIPTION,        CAST('PL/SQL' AS VARCHAR2(11)) AS ACTION_TYPE,        TRIGGER_BODY AS TRIGGER_BODY,        CAST('NO' AS VARCHAR2(7)) AS CROSSEDITION,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS INSTEAD_OF_ROW,        CAST('YES' AS VARCHAR2(3)) AS FIRE_ONCE,        CAST('NO' AS VARCHAR2(3)) AS APPLY_SERVER_ONLY   FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT TRG        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB1        ON TRG.DATABASE_ID = DB1.DATABASE_ID           AND TRG.TENANT_ID = EFFECTIVE_TENANT_ID()           AND DB1.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (TRG.DATABASE_ID = USERENV('SCHEMAID')               OR USER_CAN_ACCESS_OBJ(1, abs(nvl(TRG.BASE_OBJECT_ID,0)), TRG.DATABASE_ID) = 1)        LEFT JOIN        SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TBL        ON TRG.BASE_OBJECT_ID = TBL.TABLE_ID         AND TBL.TENANT_ID = EFFECTIVE_TENANT_ID()        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB2        ON TBL.DATABASE_ID = DB2.DATABASE_ID         AND DB2.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_triggers_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_TRIGGERS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TRIGGERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT * FROM ALL_TRIGGERS )__"))) {
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

int ObInnerTableSchema::user_triggers_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_TRIGGERS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TRIGGERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT TRG.TRIGGER_NAME AS TRIGGER_NAME,        CAST(DECODE(BITAND(TRG.TIMING_POINTS, 30),                    4, 'BEFORE EACH ROW',                    8, 'AFTER EACH ROW')             AS VARCHAR2(16)) AS TRIGGER_TYPE,        CAST(DECODE(TRG.TRIGGER_EVENTS,                    2, 'INSERT',                    4, 'UPDATE',                    8, 'DELETE',                    2 + 4, 'INSERT OR UPDATE',                    2 + 8, 'INSERT OR DELETE',                    4 + 8, 'UPDATE OR DELETE',                    2 + 4 + 8, 'INSERT OR UPDATE OR DELETE')             AS VARCHAR2(246)) AS TRIGGERING_EVENT,        DB2.DATABASE_NAME AS TABLE_OWNER,        CAST(DECODE(TRG.BASE_OBJECT_TYPE,                    5, 'TABLE')             AS VARCHAR2(18)) AS BASE_OBJECT_TYPE,        TBL.TABLE_NAME AS TABLE_NAME,        CAST(NULL AS VARCHAR2(4000)) AS COLUMN_NAME,        CAST(CONCAT('REFERENCING', CONCAT(CONCAT(' NEW AS ', REF_NEW_NAME), CONCAT(' OLD AS ', REF_OLD_NAME)))             AS VARCHAR2(422)) AS REFERENCING_NAMES,        WHEN_CONDITION AS WHEN_CLAUSE,        CAST(decode(BITAND(TRG.trigger_flags, 1), 1, 'ENABLED', 'DISABLED') AS VARCHAR2(8)) AS STATUS,        TRIGGER_BODY AS DESCRIPTION,        CAST('PL/SQL' AS VARCHAR2(11)) AS ACTION_TYPE,        TRIGGER_BODY AS TRIGGER_BODY,        CAST('NO' AS VARCHAR2(7)) AS CROSSEDITION,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS INSTEAD_OF_ROW,        CAST('YES' AS VARCHAR2(3)) AS FIRE_ONCE,        CAST('NO' AS VARCHAR2(3)) AS APPLY_SERVER_ONLY   FROM (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID())TRG        LEFT JOIN        SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TBL        ON TRG.BASE_OBJECT_ID = TBL.TABLE_ID         AND TBL.TENANT_ID = EFFECTIVE_TENANT_ID()        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB2        ON TBL.DATABASE_ID = DB2.DATABASE_ID         AND DB2.TENANT_ID = EFFECTIVE_TENANT_ID()  WHERE TRG.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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

int ObInnerTableSchema::all_dependencies_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DEPENDENCIES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DEPENDENCIES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT      o.OWNER AS OWNER,      o.OBJECT_NAME AS NAME,      o.OBJECT_TYPE AS TYPE,      ro.REFERENCED_OWNER AS REFERENCED_OWNER,      ro.REFERENCED_NAME AS REFERENCED_NAME,      DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,      CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,      CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE      FROM (select             OWNER,             OBJECT_NAME,             OBJECT_TYPE,             REF_OBJ_NAME,             ref_obj_type,             dep_obj_id,             dep_obj_type,             dep_order,             property             from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d             WHERE CAST(UPPER(decode(d.dep_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                 'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o                 LEFT OUTER JOIN                 (SELECT DISTINCT                   CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,                   CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,                   CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,                   dep_obj_id,                   dep_obj_type,                   dep_order FROM                   SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d                   WHERE CAST(UPPER(decode(d.ref_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                       'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro                     on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type                       AND ro.dep_order = o.dep_order )__"))) {
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

int ObInnerTableSchema::dba_dependencies_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_DEPENDENCIES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_DEPENDENCIES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT      o.OWNER AS OWNER,      o.OBJECT_NAME AS NAME,      o.OBJECT_TYPE AS TYPE,      ro.REFERENCED_OWNER AS REFERENCED_OWNER,      ro.REFERENCED_NAME AS REFERENCED_NAME,      DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,      CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,      CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE      FROM (select             OWNER,             OBJECT_NAME,             OBJECT_TYPE,             REF_OBJ_NAME,             ref_obj_type,             dep_obj_id,             dep_obj_type,             dep_order,             property             from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d             WHERE CAST(UPPER(decode(d.dep_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                 'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o                 LEFT OUTER JOIN                 (SELECT DISTINCT                   CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,                   CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,                   CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,                   dep_obj_id,                   dep_obj_type,                   dep_order FROM                   SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d                   WHERE CAST(UPPER(decode(d.ref_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                       'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro                     on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type                       AND ro.dep_order = o.dep_order )__"))) {
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

int ObInnerTableSchema::user_dependencies_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_DEPENDENCIES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_DEPENDENCIES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT      o.OBJECT_NAME AS NAME,      o.OBJECT_TYPE AS TYPE,      ro.REFERENCED_OWNER AS REFERENCED_OWNER,      ro.REFERENCED_NAME AS REFERENCED_NAME,      DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,      CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,      CAST(USERENV('SCHEMAID') AS NUMBER) AS SCHEMAID,      CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE      FROM (select             OWNER,             OBJECT_NAME,             OBJECT_TYPE,             REF_OBJ_NAME,             ref_obj_type,             dep_obj_id,             dep_obj_type,             dep_order,             property             from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d             WHERE CAST(UPPER(decode(d.dep_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                 'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o                 LEFT OUTER JOIN                 (SELECT DISTINCT                   CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,                   CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,                   CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,                   dep_obj_id,                   dep_obj_type,                   dep_order FROM                   SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d                   WHERE CAST(UPPER(decode(d.ref_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                       'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro                     on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type                       AND ro.dep_order = o.dep_order )__"))) {
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

int ObInnerTableSchema::dba_rsrc_plans_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_RSRC_PLANS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RSRC_PLANS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT /*+ READ_CONSISTENCY(WEAK) */       CAST(NULL AS NUMBER) AS PLAN_ID,       PLAN,       CAST(NULL AS NUMBER) AS NUM_PLAN_DIRECTIVES,       CAST(NULL AS VARCHAR2(128)) AS CPU_METHOD,       CAST(NULL AS VARCHAR2(128)) AS MGMT_METHOD,       CAST(NULL AS VARCHAR2(128)) AS ACTIVE_SESS_POOL_MTH,       CAST(NULL AS VARCHAR2(128)) AS PARALLEL_DEGREE_LIMIT_MTH,       CAST(NULL AS VARCHAR2(128)) AS QUEUING_MTH,       CAST(NULL AS VARCHAR2(3)) AS SUB_PLAN,       COMMENTS,       CAST(NULL AS VARCHAR2(128)) AS STATUS,       CAST(NULL AS VARCHAR2(3)) AS MANDATORY     FROM        SYS.ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::dba_rsrc_plan_directives_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_RSRC_PLAN_DIRECTIVES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RSRC_PLAN_DIRECTIVES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT /*+ READ_CONSISTENCY(WEAK) */       PLAN,       GROUP_OR_SUBPLAN,       CAST(NULL AS VARCHAR2(14)) AS TYPE,       CAST(NULL AS NUMBER) AS CPU_P1,       CAST(NULL AS NUMBER) AS CPU_P2,       CAST(NULL AS NUMBER) AS CPU_P3,       CAST(NULL AS NUMBER) AS CPU_P4,       CAST(NULL AS NUMBER) AS CPU_P5,       CAST(NULL AS NUMBER) AS CPU_P6,       CAST(NULL AS NUMBER) AS CPU_P7,       CAST(NULL AS NUMBER) AS CPU_P8,       MGMT_P1,       CAST(NULL AS NUMBER) AS MGMT_P2,       CAST(NULL AS NUMBER) AS MGMT_P3,       CAST(NULL AS NUMBER) AS MGMT_P4,       CAST(NULL AS NUMBER) AS MGMT_P5,       CAST(NULL AS NUMBER) AS MGMT_P6,       CAST(NULL AS NUMBER) AS MGMT_P7,       CAST(NULL AS NUMBER) AS MGMT_P8,       CAST(NULL AS NUMBER) AS ACTIVE_SESS_POOL_P1,       CAST(NULL AS NUMBER) AS QUEUEING_P1,       CAST(NULL AS NUMBER) AS PARALLEL_TARGET_PERCENTAGE,       CAST(NULL AS NUMBER) AS PARALLEL_DEGREE_LIMIT_P1,       CAST(NULL AS VARCHAR2(128)) AS SWITCH_GROUP,       CAST(NULL AS VARCHAR2(5)) AS SWITCH_FOR_CALL,       CAST(NULL AS NUMBER) AS SWITCH_TIME,       CAST(NULL AS NUMBER) AS SWITCH_IO_MEGABYTES,       CAST(NULL AS NUMBER) AS SWITCH_IO_REQS,       CAST(NULL AS VARCHAR2(5)) AS SWITCH_ESTIMATE,       CAST(NULL AS NUMBER) AS MAX_EST_EXEC_TIME,       CAST(NULL AS NUMBER) AS UNDO_POOL,       CAST(NULL AS NUMBER) AS MAX_IDLE_TIME,       CAST(NULL AS NUMBER) AS MAX_IDLE_BLOCKER_TIME,       CAST(NULL AS NUMBER) AS MAX_UTILIZATION_LIMIT,       CAST(NULL AS NUMBER) AS PARALLEL_QUEUE_TIMEOUT,       CAST(NULL AS NUMBER) AS SWITCH_TIME_IN_CALL,       CAST(NULL AS NUMBER) AS SWITCH_IO_LOGICAL,       CAST(NULL AS NUMBER) AS SWITCH_ELAPSED_TIME,       CAST(NULL AS NUMBER) AS PARALLEL_SERVER_LIMIT,       UTILIZATION_LIMIT,       CAST(NULL AS VARCHAR2(12)) AS PARALLEL_STMT_CRITICAL,       CAST(NULL AS NUMBER) AS SESSION_PGA_LIMIT,       CAST(NULL AS VARCHAR2(6)) AS PQ_TIMEOUT_ACTION,       COMMENTS,       CAST(NULL AS VARCHAR2(128)) AS STATUS,       CAST('YES' AS VARCHAR2(3)) AS MANDATORY     FROM        SYS.ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::dba_rsrc_group_mappings_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_RSRC_GROUP_MAPPINGS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RSRC_GROUP_MAPPINGS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT /*+ READ_CONSISTENCY(WEAK) */       ATTRIBUTE,       VALUE,       CONSUMER_GROUP,       CAST(NULL AS VARCHAR2(128)) AS STATUS     FROM        SYS.ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::dba_recyclebin_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_RECYCLEBIN_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RECYCLEBIN_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT OWNER, OBJECT_NAME, ORIGINAL_NAME, OPERATION, TYPE, CAST(TABLESPACE_NAME AS VARCHAR2(30)) AS TS_NAME, CREATETIME, DROPTIME, DROPSCN, PARTITION_NAME, CAN_UNDROP, CAN_PURGE, RELATED, BASE_OBJECT, PURGE_OBJECT, SPACE     FROM       (SELECT         CAST(B.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,         CAST(A.OBJECT_NAME AS VARCHAR2(128)) AS OBJECT_NAME,         CAST(A.ORIGINAL_NAME AS VARCHAR2(128)) AS ORIGINAL_NAME,         CAST(NULL AS VARCHAR2(9)) AS OPERATION,         CAST(CASE A.TYPE              WHEN 1 THEN 'TABLE'              WHEN 2 THEN 'NORMAL INDEX'              WHEN 3 THEN 'VIEW'              WHEN 4 THEN 'DATABASE'              WHEN 5 THEN 'AUX_VP'              WHEN 6 THEN 'TRIGGER'              ELSE NULL END AS VARCHAR2(25)) AS TYPE,         CAST(NULL AS VARCHAR2(30)) AS TS_NAME,         CAST(C.GMT_CREATE AS VARCHAR(30)) AS CREATETIME,         CAST(C.GMT_MODIFIED AS VARCHAR(30)) AS DROPTIME,         CAST(NULL AS NUMBER) AS DROPSCN,         CAST(NULL AS VARCHAR2(128)) AS PARTITION_NAME,         CAST('YES' AS VARCHAR2(3)) AS CAN_UNDROP,         CAST('YES' AS VARCHAR2(3)) AS CAN_PURGE,         CAST(NULL AS NUMBER) AS RELATED,         CAST(NULL AS NUMBER) AS BASE_OBJECT,         CAST(NULL AS NUMBER) AS PURGE_OBJECT,         CAST(NULL AS NUMBER) AS SPACE,         C.TABLE_ID AS TABLE_ID,         C.TABLESPACE_ID AS TABLESPACE_ID         FROM SYS.ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT A, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C         WHERE A.DATABASE_ID = B.DATABASE_ID AND A.TABLE_ID = C.TABLE_ID AND A.TENANT_ID = EFFECTIVE_TENANT_ID() AND B.TENANT_ID = EFFECTIVE_TENANT_ID() AND C.TENANT_ID = EFFECTIVE_TENANT_ID()) LEFT_TABLE       LEFT JOIN         (SELECT TABLESPACE_NAME, TABLESPACE_ID FROM SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT) RIGHT_TABLE       ON LEFT_TABLE.TABLESPACE_ID = RIGHT_TABLE.TABLESPACE_ID )__"))) {
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

int ObInnerTableSchema::user_recyclebin_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_RECYCLEBIN_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_RECYCLEBIN_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(
            table_schema.set_view_definition(R"__(     SELECT OBJECT_NAME, ORIGINAL_NAME, OPERATION, TYPE, CAST(TABLESPACE_NAME AS VARCHAR2(30)) AS TS_NAME, CREATETIME, DROPTIME, DROPSCN, PARTITION_NAME, CAN_UNDROP, CAN_PURGE, RELATED, BASE_OBJECT, PURGE_OBJECT, SPACE     FROM       (SELECT         CAST(A.OBJECT_NAME AS VARCHAR2(128)) AS OBJECT_NAME,         CAST(A.ORIGINAL_NAME AS VARCHAR2(128)) AS ORIGINAL_NAME,         CAST(NULL AS VARCHAR2(9)) AS OPERATION,         CAST(CASE A.TYPE              WHEN 1 THEN 'TABLE'              WHEN 2 THEN 'NORMAL INDEX'              WHEN 3 THEN 'VIEW'              WHEN 4 THEN 'DATABASE'              WHEN 5 THEN 'AUX_VP'              WHEN 6 THEN 'TRIGGER'              ELSE NULL END AS VARCHAR2(25)) AS TYPE,         CAST(NULL AS VARCHAR2(30)) AS TS_NAME,         CAST(C.GMT_CREATE AS VARCHAR(30)) AS CREATETIME,         CAST(C.GMT_MODIFIED AS VARCHAR(30)) AS DROPTIME,         CAST(NULL AS NUMBER) AS DROPSCN,         CAST(NULL AS VARCHAR2(128)) AS PARTITION_NAME,         CAST('YES' AS VARCHAR2(3)) AS CAN_UNDROP,         CAST('YES' AS VARCHAR2(3)) AS CAN_PURGE,         CAST(NULL AS NUMBER) AS RELATED,         CAST(NULL AS NUMBER) AS BASE_OBJECT,         CAST(NULL AS NUMBER) AS PURGE_OBJECT,         CAST(NULL AS NUMBER) AS SPACE,         C.TABLE_ID AS TABLE_ID,         C.TABLESPACE_ID AS TABLESPACE_ID         FROM SYS.ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C WHERE A.TABLE_ID = C.TABLE_ID AND A.DATABASE_ID = USERENV('SCHEMAID') AND A.TENANT_ID = EFFECTIVE_TENANT_ID() AND C.TENANT_ID = EFFECTIVE_TENANT_ID()) LEFT_TABLE       LEFT JOIN         (SELECT TABLESPACE_NAME, TABLESPACE_ID FROM SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) RIGHT_TABLE       ON LEFT_TABLE.TABLESPACE_ID = RIGHT_TABLE.TABLESPACE_ID )__"))) {
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

int ObInnerTableSchema::dba_rsrc_consumer_groups_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_RSRC_CONSUMER_GROUPS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RSRC_CONSUMER_GROUPS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT /*+ READ_CONSISTENCY(WEAK) */       CONSUMER_GROUP_ID,       CONSUMER_GROUP,       CAST(NULL AS VARCHAR2(128)) AS CPU_METHOD,       CAST(NULL AS VARCHAR2(128)) AS MGMT_METHOD,       CAST(NULL AS VARCHAR2(3)) AS INTERNAL_USE,       COMMENTS,       CAST(NULL AS VARCHAR2(128)) AS CATEGORY,       CAST(NULL AS VARCHAR2(128)) AS STATUS,       CAST(NULL AS VARCHAR2(3)) AS MANDATORY     FROM        SYS.ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT )__"))) {
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
