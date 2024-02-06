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

int ObInnerTableSchema::dict_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DICT_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT * FROM SYS.DICTIONARY )__"))) {
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

int ObInnerTableSchema::all_triggers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TRIGGERS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT DB1.DATABASE_NAME AS OWNER,        TRG.TRIGGER_NAME AS TRIGGER_NAME,        CAST((case when TRG.TRIGGER_TYPE=1 then DECODE(BITAND(TRG.TIMING_POINTS, 30),                                                       2, 'BEFORE STATEMENT',                                                       4, 'BEFORE EACH ROW',                                                       8, 'AFTER EACH ROW',                                                       16, 'AFTER STATEMENT')             when TRG.TRIGGER_TYPE=2 then 'COMPOUND'             when TRG.TRIGGER_TYPE=3 then 'INSTEAD OF' END)             AS VARCHAR2(16)) AS TRIGGER_TYPE,        CAST(DECODE(TRG.TRIGGER_EVENTS,                    1, 'INSERT',                    2, 'UPDATE',                    4, 'DELETE',                    1 + 2, 'INSERT OR UPDATE',                    1 + 4, 'INSERT OR DELETE',                    2 + 4, 'UPDATE OR DELETE',                    1 + 2 + 4, 'INSERT OR UPDATE OR DELETE')             AS VARCHAR2(246)) AS TRIGGERING_EVENT,        DB2.DATABASE_NAME AS TABLE_OWNER,        CAST(DECODE(TRG.BASE_OBJECT_TYPE,                    5, 'TABLE',                    34, 'VIEW')             AS VARCHAR2(18)) AS BASE_OBJECT_TYPE,        TBL.TABLE_NAME AS TABLE_NAME,        CAST(NULL AS VARCHAR2(4000)) AS COLUMN_NAME,        CAST(CONCAT('REFERENCING', CONCAT(CONCAT(' NEW AS ', REF_NEW_NAME), CONCAT(' OLD AS ', REF_OLD_NAME)))             AS VARCHAR2(422)) AS REFERENCING_NAMES,        WHEN_CONDITION AS WHEN_CLAUSE,        CAST(decode(BITAND(TRG.trigger_flags, 1), 1, 'ENABLED', 'DISABLED') AS VARCHAR2(8)) AS STATUS,        TRIGGER_BODY AS DESCRIPTION,        CAST('PL/SQL' AS VARCHAR2(11)) AS ACTION_TYPE,        TRIGGER_BODY AS TRIGGER_BODY,        CAST('NO' AS VARCHAR2(7)) AS CROSSEDITION,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS INSTEAD_OF_ROW,        CAST('YES' AS VARCHAR2(3)) AS FIRE_ONCE,        CAST('NO' AS VARCHAR2(3)) AS APPLY_SERVER_ONLY   FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT TRG        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB1        ON TRG.DATABASE_ID = DB1.DATABASE_ID           AND TRG.TENANT_ID = EFFECTIVE_TENANT_ID()           AND DB1.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (TRG.DATABASE_ID = USERENV('SCHEMAID')               OR USER_CAN_ACCESS_OBJ(1, abs(nvl(TRG.BASE_OBJECT_ID,0)), TRG.DATABASE_ID) = 1)        LEFT JOIN        SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TBL        ON TRG.BASE_OBJECT_ID = TBL.TABLE_ID         AND TBL.TENANT_ID = EFFECTIVE_TENANT_ID()        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB2        ON TBL.DATABASE_ID = DB2.DATABASE_ID         AND DB2.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_triggers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TRIGGERS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT DB1.DATABASE_NAME AS OWNER,        TRG.TRIGGER_NAME AS TRIGGER_NAME,        CAST((case when TRG.TRIGGER_TYPE=1 then DECODE(BITAND(TRG.TIMING_POINTS, 30),                                                       2, 'BEFORE STATEMENT',                                                       4, 'BEFORE EACH ROW',                                                       8, 'AFTER EACH ROW',                                                       16, 'AFTER STATEMENT')             when TRG.TRIGGER_TYPE=2 then 'COMPOUND'             when TRG.TRIGGER_TYPE=3 then 'INSTEAD OF' END)             AS VARCHAR2(16)) AS TRIGGER_TYPE,        CAST(DECODE(TRG.TRIGGER_EVENTS,                    1, 'INSERT',                    2, 'UPDATE',                    4, 'DELETE',                    1 + 2, 'INSERT OR UPDATE',                    1 + 4, 'INSERT OR DELETE',                    2 + 4, 'UPDATE OR DELETE',                    1 + 2 + 4, 'INSERT OR UPDATE OR DELETE')             AS VARCHAR2(246)) AS TRIGGERING_EVENT,        DB2.DATABASE_NAME AS TABLE_OWNER,        CAST(DECODE(TRG.BASE_OBJECT_TYPE,                    5, 'TABLE')             AS VARCHAR2(18)) AS BASE_OBJECT_TYPE,        TBL.TABLE_NAME AS TABLE_NAME,        CAST(NULL AS VARCHAR2(4000)) AS COLUMN_NAME,        CAST(CONCAT('REFERENCING', CONCAT(CONCAT(' NEW AS ', REF_NEW_NAME), CONCAT(' OLD AS ', REF_OLD_NAME)))             AS VARCHAR2(422)) AS REFERENCING_NAMES,        WHEN_CONDITION AS WHEN_CLAUSE,        CAST(decode(BITAND(TRG.trigger_flags, 1), 1, 'ENABLED', 'DISABLED') AS VARCHAR2(8)) AS STATUS,        TRIGGER_BODY AS DESCRIPTION,        CAST('PL/SQL' AS VARCHAR2(11)) AS ACTION_TYPE,        TRIGGER_BODY AS TRIGGER_BODY,        CAST('NO' AS VARCHAR2(7)) AS CROSSEDITION,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS INSTEAD_OF_ROW,        CAST('YES' AS VARCHAR2(3)) AS FIRE_ONCE,        CAST('NO' AS VARCHAR2(3)) AS APPLY_SERVER_ONLY   FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT TRG        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB1        ON TRG.DATABASE_ID = DB1.DATABASE_ID           AND TRG.TENANT_ID = EFFECTIVE_TENANT_ID()           AND DB1.TENANT_ID = EFFECTIVE_TENANT_ID()        LEFT JOIN        SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TBL        ON TRG.BASE_OBJECT_ID = TBL.TABLE_ID         AND TBL.TENANT_ID = EFFECTIVE_TENANT_ID()        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB2        ON TBL.DATABASE_ID = DB2.DATABASE_ID         AND DB2.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::user_triggers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TRIGGERS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT TRG.TRIGGER_NAME AS TRIGGER_NAME,        CAST((case when TRG.TRIGGER_TYPE=1 then DECODE(BITAND(TRG.TIMING_POINTS, 30),                                                       2, 'BEFORE STATEMENT',                                                       4, 'BEFORE EACH ROW',                                                       8, 'AFTER EACH ROW',                                                       16, 'AFTER STATEMENT')             when TRG.TRIGGER_TYPE=2 then 'COMPOUND'             when TRG.TRIGGER_TYPE=3 then 'INSTEAD OF' END)             AS VARCHAR2(16)) AS TRIGGER_TYPE,        CAST(DECODE(TRG.TRIGGER_EVENTS,                    1, 'INSERT',                    2, 'UPDATE',                    4, 'DELETE',                    1 + 2, 'INSERT OR UPDATE',                    1 + 4, 'INSERT OR DELETE',                    2 + 4, 'UPDATE OR DELETE',                    1 + 2 + 4, 'INSERT OR UPDATE OR DELETE')             AS VARCHAR2(246)) AS TRIGGERING_EVENT,        DB2.DATABASE_NAME AS TABLE_OWNER,        CAST(DECODE(TRG.BASE_OBJECT_TYPE,                    5, 'TABLE',                    34, 'VIEW')             AS VARCHAR2(18)) AS BASE_OBJECT_TYPE,        TBL.TABLE_NAME AS TABLE_NAME,        CAST(NULL AS VARCHAR2(4000)) AS COLUMN_NAME,        CAST(CONCAT('REFERENCING', CONCAT(CONCAT(' NEW AS ', REF_NEW_NAME), CONCAT(' OLD AS ', REF_OLD_NAME)))             AS VARCHAR2(422)) AS REFERENCING_NAMES,        WHEN_CONDITION AS WHEN_CLAUSE,        CAST(decode(BITAND(TRG.trigger_flags, 1), 1, 'ENABLED', 'DISABLED') AS VARCHAR2(8)) AS STATUS,        TRIGGER_BODY AS DESCRIPTION,        CAST('PL/SQL' AS VARCHAR2(11)) AS ACTION_TYPE,        TRIGGER_BODY AS TRIGGER_BODY,        CAST('NO' AS VARCHAR2(7)) AS CROSSEDITION,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS BEFORE_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_ROW,        CAST('NO' AS VARCHAR2(3)) AS AFTER_STATEMENT,        CAST('NO' AS VARCHAR2(3)) AS INSTEAD_OF_ROW,        CAST('YES' AS VARCHAR2(3)) AS FIRE_ONCE,        CAST('NO' AS VARCHAR2(3)) AS APPLY_SERVER_ONLY   FROM (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID())TRG        LEFT JOIN        SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TBL        ON TRG.BASE_OBJECT_ID = TBL.TABLE_ID         AND TBL.TENANT_ID = EFFECTIVE_TENANT_ID()        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB2        ON TBL.DATABASE_ID = DB2.DATABASE_ID         AND DB2.TENANT_ID = EFFECTIVE_TENANT_ID()  WHERE TRG.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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

int ObInnerTableSchema::all_dependencies_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DEPENDENCIES_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT      o.OWNER AS OWNER,      o.OBJECT_NAME AS NAME,      o.OBJECT_TYPE AS TYPE,      ro.REFERENCED_OWNER AS REFERENCED_OWNER,      ro.REFERENCED_NAME AS REFERENCED_NAME,      DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,      CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,      CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE      FROM (select             OWNER,             OBJECT_NAME,             OBJECT_TYPE,             REF_OBJ_NAME,             ref_obj_type,             dep_obj_id,             dep_obj_type,             dep_order,             property             from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT d             WHERE CAST(UPPER(decode(d.dep_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                 'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o                 LEFT OUTER JOIN                 (SELECT DISTINCT                   CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,                   CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,                   CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,                   dep_obj_id,                   dep_obj_type,                   dep_order FROM                   SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT d                   WHERE CAST(UPPER(decode(d.ref_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                       'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro                     on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type                       AND ro.dep_order = o.dep_order )__"))) {
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

int ObInnerTableSchema::dba_dependencies_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_DEPENDENCIES_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT      o.OWNER AS OWNER,      o.OBJECT_NAME AS NAME,      o.OBJECT_TYPE AS TYPE,      ro.REFERENCED_OWNER AS REFERENCED_OWNER,      ro.REFERENCED_NAME AS REFERENCED_NAME,      DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,      CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,      CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE      FROM (select             OWNER,             OBJECT_NAME,             OBJECT_TYPE,             REF_OBJ_NAME,             ref_obj_type,             dep_obj_id,             dep_obj_type,             dep_order,             property             from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT d             WHERE CAST(UPPER(decode(d.dep_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                 'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o                 LEFT OUTER JOIN                 (SELECT DISTINCT                   CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,                   CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,                   CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,                   dep_obj_id,                   dep_obj_type,                   dep_order FROM                   SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT d                   WHERE CAST(UPPER(decode(d.ref_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                       'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro                     on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type                       AND ro.dep_order = o.dep_order )__"))) {
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

int ObInnerTableSchema::user_dependencies_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_DEPENDENCIES_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT      o.OBJECT_NAME AS NAME,      o.OBJECT_TYPE AS TYPE,      ro.REFERENCED_OWNER AS REFERENCED_OWNER,      ro.REFERENCED_NAME AS REFERENCED_NAME,      DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,      CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,      CAST(USERENV('SCHEMAID') AS NUMBER) AS SCHEMAID,      CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE      FROM (select             OWNER,             OBJECT_NAME,             OBJECT_TYPE,             REF_OBJ_NAME,             ref_obj_type,             dep_obj_id,             dep_obj_type,             dep_order,             property             from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT d             WHERE CAST(UPPER(decode(d.dep_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                 'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o                 LEFT OUTER JOIN                 (SELECT DISTINCT                   CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,                   CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,                   CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,                   dep_obj_id,                   dep_obj_type,                   dep_order FROM                   SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT d                   WHERE CAST(UPPER(decode(d.ref_obj_type,                       1, 'TABLE',                       2, 'SEQUENCE',                       3, 'PACKAGE',                       4, 'TYPE',                       5, 'PACKAGE BODY',                       6, 'TYPE BODY',                       7, 'TRIGGER',                       8, 'VIEW',                       9, 'FUNCTION',                       10, 'DIRECTORY',                       11, 'INDEX',                       12, 'PROCEDURE',                       13, 'SYNONYM',                       'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro                     on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type                       AND ro.dep_order = o.dep_order )__"))) {
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

int ObInnerTableSchema::dba_rsrc_plans_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RSRC_PLANS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RSRC_PLANS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(NULL AS NUMBER) AS PLAN_ID,       PLAN,       CAST(NULL AS NUMBER) AS NUM_PLAN_DIRECTIVES,       CAST(NULL AS VARCHAR2(128)) AS CPU_METHOD,       CAST(NULL AS VARCHAR2(128)) AS MGMT_METHOD,       CAST(NULL AS VARCHAR2(128)) AS ACTIVE_SESS_POOL_MTH,       CAST(NULL AS VARCHAR2(128)) AS PARALLEL_DEGREE_LIMIT_MTH,       CAST(NULL AS VARCHAR2(128)) AS QUEUING_MTH,       CAST(NULL AS VARCHAR2(3)) AS SUB_PLAN,       COMMENTS,       CAST(NULL AS VARCHAR2(128)) AS STATUS,       CAST(NULL AS VARCHAR2(3)) AS MANDATORY     FROM        SYS.ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::dba_rsrc_plan_directives_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RSRC_PLAN_DIRECTIVES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RSRC_PLAN_DIRECTIVES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       PLAN,       GROUP_OR_SUBPLAN,       CAST(NULL AS VARCHAR2(14)) AS TYPE,       CAST(NULL AS NUMBER) AS CPU_P1,       CAST(NULL AS NUMBER) AS CPU_P2,       CAST(NULL AS NUMBER) AS CPU_P3,       CAST(NULL AS NUMBER) AS CPU_P4,       CAST(NULL AS NUMBER) AS CPU_P5,       CAST(NULL AS NUMBER) AS CPU_P6,       CAST(NULL AS NUMBER) AS CPU_P7,       CAST(NULL AS NUMBER) AS CPU_P8,       MGMT_P1,       CAST(NULL AS NUMBER) AS MGMT_P2,       CAST(NULL AS NUMBER) AS MGMT_P3,       CAST(NULL AS NUMBER) AS MGMT_P4,       CAST(NULL AS NUMBER) AS MGMT_P5,       CAST(NULL AS NUMBER) AS MGMT_P6,       CAST(NULL AS NUMBER) AS MGMT_P7,       CAST(NULL AS NUMBER) AS MGMT_P8,       CAST(NULL AS NUMBER) AS ACTIVE_SESS_POOL_P1,       CAST(NULL AS NUMBER) AS QUEUEING_P1,       CAST(NULL AS NUMBER) AS PARALLEL_TARGET_PERCENTAGE,       CAST(NULL AS NUMBER) AS PARALLEL_DEGREE_LIMIT_P1,       CAST(NULL AS VARCHAR2(128)) AS SWITCH_GROUP,       CAST(NULL AS VARCHAR2(5)) AS SWITCH_FOR_CALL,       CAST(NULL AS NUMBER) AS SWITCH_TIME,       CAST(NULL AS NUMBER) AS SWITCH_IO_MEGABYTES,       CAST(NULL AS NUMBER) AS SWITCH_IO_REQS,       CAST(NULL AS VARCHAR2(5)) AS SWITCH_ESTIMATE,       CAST(NULL AS NUMBER) AS MAX_EST_EXEC_TIME,       CAST(NULL AS NUMBER) AS UNDO_POOL,       CAST(NULL AS NUMBER) AS MAX_IDLE_TIME,       CAST(NULL AS NUMBER) AS MAX_IDLE_BLOCKER_TIME,       CAST(NULL AS NUMBER) AS MAX_UTILIZATION_LIMIT,       CAST(NULL AS NUMBER) AS PARALLEL_QUEUE_TIMEOUT,       CAST(NULL AS NUMBER) AS SWITCH_TIME_IN_CALL,       CAST(NULL AS NUMBER) AS SWITCH_IO_LOGICAL,       CAST(NULL AS NUMBER) AS SWITCH_ELAPSED_TIME,       CAST(NULL AS NUMBER) AS PARALLEL_SERVER_LIMIT,       UTILIZATION_LIMIT,       CAST(NULL AS VARCHAR2(12)) AS PARALLEL_STMT_CRITICAL,       CAST(NULL AS NUMBER) AS SESSION_PGA_LIMIT,       CAST(NULL AS VARCHAR2(6)) AS PQ_TIMEOUT_ACTION,       COMMENTS,       CAST(NULL AS VARCHAR2(128)) AS STATUS,       CAST('YES' AS VARCHAR2(3)) AS MANDATORY     FROM        SYS.ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::dba_rsrc_group_mappings_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RSRC_GROUP_MAPPINGS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RSRC_GROUP_MAPPINGS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       ATTRIBUTE,       VALUE,       CONSUMER_GROUP,       CAST(NULL AS VARCHAR2(128)) AS STATUS     FROM        SYS.ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::dba_recyclebin_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RECYCLEBIN_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT   CAST(B.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,   CAST(A.OBJECT_NAME AS VARCHAR2(128)) AS OBJECT_NAME,   CAST(A.ORIGINAL_NAME AS VARCHAR2(128)) AS ORIGINAL_NAME,   CAST(NULL AS VARCHAR2(9)) AS OPERATION,   CAST(CASE A.TYPE        WHEN 1 THEN 'TABLE'        WHEN 2 THEN 'NORMAL INDEX'        WHEN 3 THEN 'VIEW'        ELSE NULL END AS VARCHAR2(25)) AS TYPE,   CAST(CASE WHEN TP.TABLESPACE_ID IS NULL THEN NULL ELSE TP.TABLESPACE_NAME END AS VARCHAR2(30)) AS TS_NAME,   CAST(TO_CHAR(C.GMT_CREATE) AS VARCHAR2(19)) AS CREATETIME,   CAST(TO_CHAR(C.GMT_MODIFIED) AS VARCHAR2(19)) AS DROPTIME,   CAST(NULL AS NUMBER) AS DROPSCN,   CAST(NULL AS VARCHAR2(128)) AS PARTITION_NAME,   CAST('YES' AS VARCHAR2(3)) AS CAN_UNDROP,   CAST('YES' AS VARCHAR2(3)) AS CAN_PURGE,   CAST(NULL AS NUMBER) AS RELATED,   CAST(NULL AS NUMBER) AS BASE_OBJECT,   CAST(NULL AS NUMBER) AS PURGE_OBJECT,   CAST(NULL AS NUMBER) AS SPACE   FROM SYS.ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT A   JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B     ON A.TENANT_ID = B.TENANT_ID        AND A.DATABASE_ID = B.DATABASE_ID   JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C     ON A.TENANT_ID = C.TENANT_ID        AND A.TABLE_ID = C.TABLE_ID   LEFT JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP     ON C.TENANT_ID = TP.TENANT_ID        AND C.TABLESPACE_ID = TP.TABLESPACE_ID   WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()     AND A.TYPE IN (1, 2, 3)    UNION ALL    SELECT   CAST(B.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,   CAST(A.OBJECT_NAME AS VARCHAR2(128)) AS OBJECT_NAME,   CAST(A.ORIGINAL_NAME AS VARCHAR2(128)) AS ORIGINAL_NAME,   CAST(NULL AS VARCHAR2(9)) AS OPERATION,   CAST('TRIGGER' AS VARCHAR2(25)) AS TYPE,   CAST(NULL AS VARCHAR2(30)) AS TS_NAME,   CAST(TO_CHAR(C.GMT_CREATE) AS VARCHAR2(19)) AS CREATETIME,   CAST(TO_CHAR(C.GMT_MODIFIED) AS VARCHAR2(19)) AS DROPTIME,   CAST(NULL AS NUMBER) AS DROPSCN,   CAST(NULL AS VARCHAR2(128)) AS PARTITION_NAME,   CAST('YES' AS VARCHAR2(3)) AS CAN_UNDROP,   CAST('YES' AS VARCHAR2(3)) AS CAN_PURGE,   CAST(NULL AS NUMBER) AS RELATED,   CAST(NULL AS NUMBER) AS BASE_OBJECT,   CAST(NULL AS NUMBER) AS PURGE_OBJECT,   CAST(NULL AS NUMBER) AS SPACE   FROM SYS.ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT A   JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B     ON A.TENANT_ID = B.TENANT_ID        AND A.DATABASE_ID = B.DATABASE_ID   JOIN SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT C     ON A.TENANT_ID = C.TENANT_ID        AND A.TABLE_ID = C.TRIGGER_ID   WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()     AND A.TYPE = 6 )__"))) {
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

int ObInnerTableSchema::user_recyclebin_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_RECYCLEBIN_ORA_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT   CAST(A.OBJECT_NAME AS VARCHAR2(128)) AS OBJECT_NAME,   CAST(A.ORIGINAL_NAME AS VARCHAR2(128)) AS ORIGINAL_NAME,   CAST(NULL AS VARCHAR2(9)) AS OPERATION,   CAST(CASE A.TYPE        WHEN 1 THEN 'TABLE'        WHEN 2 THEN 'NORMAL INDEX'        WHEN 3 THEN 'VIEW'        ELSE NULL END AS VARCHAR2(25)) AS TYPE,   CAST(CASE WHEN TP.TABLESPACE_ID IS NULL THEN NULL ELSE TP.TABLESPACE_NAME END AS VARCHAR2(30)) AS TS_NAME,   CAST(TO_CHAR(C.GMT_CREATE) AS VARCHAR2(19)) AS CREATETIME,   CAST(TO_CHAR(C.GMT_MODIFIED) AS VARCHAR2(19)) AS DROPTIME,   CAST(NULL AS NUMBER) AS DROPSCN,   CAST(NULL AS VARCHAR2(128)) AS PARTITION_NAME,   CAST('YES' AS VARCHAR2(3)) AS CAN_UNDROP,   CAST('YES' AS VARCHAR2(3)) AS CAN_PURGE,   CAST(NULL AS NUMBER) AS RELATED,   CAST(NULL AS NUMBER) AS BASE_OBJECT,   CAST(NULL AS NUMBER) AS PURGE_OBJECT,   CAST(NULL AS NUMBER) AS SPACE   FROM SYS.ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT A   JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B     ON A.TENANT_ID = B.TENANT_ID        AND A.DATABASE_ID = B.DATABASE_ID   JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C     ON A.TENANT_ID = C.TENANT_ID        AND A.TABLE_ID = C.TABLE_ID   LEFT JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP     ON C.TENANT_ID = TP.TENANT_ID        AND C.TABLESPACE_ID = TP.TABLESPACE_ID   WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()     AND A.TYPE IN (1, 2, 3)     AND A.DATABASE_ID = USERENV('SCHEMAID')    UNION ALL    SELECT   CAST(A.OBJECT_NAME AS VARCHAR2(128)) AS OBJECT_NAME,   CAST(A.ORIGINAL_NAME AS VARCHAR2(128)) AS ORIGINAL_NAME,   CAST(NULL AS VARCHAR2(9)) AS OPERATION,   CAST('TRIGGER' AS VARCHAR2(25)) AS TYPE,   CAST(NULL AS VARCHAR2(30)) AS TS_NAME,   CAST(TO_CHAR(C.GMT_CREATE) AS VARCHAR2(19)) AS CREATETIME,   CAST(TO_CHAR(C.GMT_MODIFIED) AS VARCHAR2(19)) AS DROPTIME,   CAST(NULL AS NUMBER) AS DROPSCN,   CAST(NULL AS VARCHAR2(128)) AS PARTITION_NAME,   CAST('YES' AS VARCHAR2(3)) AS CAN_UNDROP,   CAST('YES' AS VARCHAR2(3)) AS CAN_PURGE,   CAST(NULL AS NUMBER) AS RELATED,   CAST(NULL AS NUMBER) AS BASE_OBJECT,   CAST(NULL AS NUMBER) AS PURGE_OBJECT,   CAST(NULL AS NUMBER) AS SPACE   FROM SYS.ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT A   JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B     ON A.TENANT_ID = B.TENANT_ID        AND A.DATABASE_ID = B.DATABASE_ID   JOIN SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT C     ON A.TENANT_ID = C.TENANT_ID        AND A.TABLE_ID = C.TRIGGER_ID   WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()     AND A.TYPE = 6     AND A.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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

int ObInnerTableSchema::dba_rsrc_consumer_groups_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RSRC_CONSUMER_GROUPS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_RSRC_CONSUMER_GROUPS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CONSUMER_GROUP_ID,       CONSUMER_GROUP,       CAST(NULL AS VARCHAR2(128)) AS CPU_METHOD,       CAST(NULL AS VARCHAR2(128)) AS MGMT_METHOD,       CAST(NULL AS VARCHAR2(3)) AS INTERNAL_USE,       COMMENTS,       CAST(NULL AS VARCHAR2(128)) AS CATEGORY,       CAST(NULL AS VARCHAR2(128)) AS STATUS,       CAST(NULL AS VARCHAR2(3)) AS MANDATORY     FROM        SYS.ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_locations_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_LOCATIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_LOCATIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT          CAST(TO_CHAR(GMT_CREATE) AS VARCHAR2(19)) AS CREATE_TIME,          CAST(TO_CHAR(GMT_MODIFIED) AS VARCHAR2(19)) AS MODIFY_TIME,          CAST(LS_ID AS NUMBER) AS LS_ID,          SVR_IP,          CAST(SVR_PORT AS NUMBER) AS SVR_PORT,          CAST(SQL_PORT AS NUMBER) AS SQL_PORT,          ZONE,          (CASE ROLE WHEN 1 THEN 'LEADER' ELSE 'FOLLOWER' END) AS ROLE,          (CASE ROLE WHEN 1 THEN MEMBER_LIST ELSE NULL END) AS MEMBER_LIST,          CAST((CASE ROLE WHEN 1 THEN PAXOS_REPLICA_NUMBER ELSE NULL END) AS NUMBER) AS PAXOS_REPLICA_NUMBER,          (CASE REPLICA_TYPE           WHEN 0   THEN 'FULL'           WHEN 5   THEN 'LOGONLY'           WHEN 16  THEN 'READONLY'           WHEN 261 THEN 'ENCRYPTION LOGONLY'           ELSE NULL END) AS REPLICA_TYPE,          (CASE ROLE WHEN 1 THEN LEARNER_LIST ELSE NULL END) AS LEARNER_LIST,          (CASE REBUILD            WHEN 0  THEN 'FALSE'            ELSE 'TRUE' END) AS REBUILD   FROM SYS.ALL_VIRTUAL_LS_META_TABLE   WHERE     TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablet_to_ls_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLET_TO_LS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLET_TO_LS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT CAST(TABLE_ID AS NUMBER) AS TABLET_ID,          CAST(1 AS NUMBER) AS LS_ID   FROM SYS.ALL_VIRTUAL_CORE_ALL_TABLE   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )   UNION ALL   (   SELECT CAST(TABLE_ID AS NUMBER) AS TABLET_ID,          CAST(1 AS NUMBER) AS LS_ID   FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   AND ((TABLE_ID > 0 AND TABLE_ID < 10000)        OR (TABLE_ID > 50000 AND TABLE_ID < 70000)        OR (TABLE_ID > 100000 AND TABLE_ID < 200000))   )   UNION ALL   (   SELECT CAST(TABLET_ID AS NUMBER) AS TABLET_ID,          CAST(LS_ID AS NUMBER) AS LS_ID   FROM SYS.ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT   )   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablet_replicas_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLET_REPLICAS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLET_REPLICAS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT gmt_create AS CREATE_TIME,          gmt_modified AS MODIFY_TIME,          CAST(TABLET_ID AS NUMBER) AS TABLET_ID,          SVR_IP,          CAST(SVR_PORT AS NUMBER) AS SVR_PORT,          CAST(LS_ID AS NUMBER) AS LS_ID,          CAST(COMPACTION_SCN AS NUMBER) AS COMPACTION_SCN,          CAST(DATA_SIZE AS NUMBER) AS DATA_SIZE,          CAST(REQUIRED_SIZE AS NUMBER) AS REQUIRED_SIZE   FROM SYS.ALL_VIRTUAL_TABLET_META_TABLE   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablegroups_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLEGROUPS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLEGROUPS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TABLEGROUP_NAME,                    CAST('NONE' AS VARCHAR2(13)) AS PARTITIONING_TYPE,           CAST('NONE' AS VARCHAR2(13)) AS SUBPARTITIONING_TYPE,           CAST(NULL AS NUMBER) AS PARTITION_COUNT,           CAST(NULL AS NUMBER) AS DEF_SUBPARTITION_COUNT,           CAST(NULL AS NUMBER) AS PARTITIONING_KEY_COUNT,           CAST(NULL AS NUMBER) AS SUBPARTITIONING_KEY_COUNT,           SHARDING   FROM SYS.ALL_VIRTUAL_TABLEGROUP_REAL_AGENT   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablegroup_partitions_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLEGROUP_PARTITIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLEGROUP_PARTITIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST('' AS VARCHAR(128)) AS TABLEGROUP_NAME,           CAST('NO' AS VARCHAR(3)) AS COMPOSITE,           CAST('' AS VARCHAR(64)) AS PARTITION_NAME,           CAST(NULL AS NUMBER) AS SUBPARTITION_COUNT,           CAST(NULL AS VARCHAR(4096)) AS HIGH_VALUE,           CAST(NULL AS NUMBER) AS HIGH_VALUE_LENGTH,           CAST(NULL AS NUMBER) AS PARTITION_POSITION   FROM      DUAL   WHERE      0 = 1   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablegroup_subpartitions_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST('' AS VARCHAR(128)) AS TABLEGROUP_NAME,           CAST('' AS VARCHAR(64)) AS PARTITION_NAME,           CAST('' AS VARCHAR(64)) AS SUBPARTITION_NAME,           CAST(NULL AS VARCHAR(4096)) AS HIGH_VALUE,           CAST(NULL AS NUMBER) AS HIGH_VALUE_LENGTH,           CAST(NULL AS NUMBER) AS PARTITION_POSITION,           CAST(NULL AS NUMBER) AS SUBPARTITION_POSITION    FROM        DUAL    WHERE       0 = 1   )__"))) {
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

int ObInnerTableSchema::dba_ob_databases_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_DATABASES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_DATABASES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT D.DATABASE_NAME AS DATABASE_NAME,          (CASE D.IN_RECYCLEBIN WHEN 0 THEN 'NO' ELSE 'YES' END) AS IN_RECYCLEBIN,          C.COLLATION AS COLLATION,          (CASE D.READ_ONLY WHEN 0 THEN 'NO' ELSE 'YES' END) AS READ_ONLY,          D."COMMENT" AS "COMMENT"   FROM SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D   LEFT JOIN SYS.TENANT_VIRTUAL_COLLATION C   ON D.COLLATION_TYPE = C.COLLATION_TYPE   )__"))) {
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

int ObInnerTableSchema::dba_ob_tablegroup_tables_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLEGROUP_TABLES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLEGROUP_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TG.TABLEGROUP_NAME AS TABLEGROUP_NAME,          D.DATABASE_NAME AS OWNER,          T.TABLE_NAME AS TABLE_NAME,          TG.SHARDING AS SHARDING   FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T   JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D   ON T.TENANT_ID = D.TENANT_ID AND T.DATABASE_ID = D.DATABASE_ID   JOIN SYS.ALL_VIRTUAL_TABLEGROUP_REAL_AGENT TG   ON T.TENANT_ID = TG.TENANT_ID AND T.TABLEGROUP_ID = TG.TABLEGROUP_ID   WHERE T.TABLE_TYPE in (0, 3, 6)   )__"))) {
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

int ObInnerTableSchema::dba_ob_zone_major_compaction_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ZONE_MAJOR_COMPACTION_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ZONE_MAJOR_COMPACTION_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT ZONE,          BROADCAST_SCN,          LAST_MERGED_SCN AS LAST_SCN,          LAST_MERGED_TIME AS LAST_FINISH_TIME,          MERGE_START_TIME AS START_TIME,          (CASE MERGE_STATUS                 WHEN 0 THEN 'IDLE'                 WHEN 1 THEN 'COMPACTING'                 ELSE 'UNKNOWN' END) AS STATUS   FROM SYS.ALL_VIRTUAL_ZONE_MERGE_INFO   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::dba_ob_major_compaction_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_MAJOR_COMPACTION_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_MAJOR_COMPACTION_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT FROZEN_SCN,          GLOBAL_BROADCAST_SCN,          LAST_MERGED_SCN AS LAST_SCN,          LAST_MERGED_TIME AS LAST_FINISH_TIME,          MERGE_START_TIME AS START_TIME,          (CASE MERGE_STATUS                 WHEN 0 THEN 'IDLE'                 WHEN 1 THEN 'COMPACTING'                 WHEN 2 THEN 'CHECKSUM'                 ELSE 'UNKNOWN' END) AS STATUS,          (CASE IS_MERGE_ERROR WHEN 0 THEN 'NO' ELSE 'YES' END) AS IS_ERROR,          (CASE SUSPEND_MERGING WHEN 0 THEN 'NO' ELSE 'YES' END) AS IS_SUSPENDED,          (CASE ERROR_TYPE                 WHEN 0 THEN ''                 WHEN 1 THEN 'CHECKSUM_ERROR'                 ELSE 'UNKNOWN' END) AS INFO   FROM SYS.ALL_VIRTUAL_MERGE_INFO   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::all_ind_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_IND_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_IND_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS OWNER,     CAST(V.INDEX_NAME AS     VARCHAR2(128)) AS INDEX_NAME,     CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS TABLE_OWNER,     CAST(T.TABLE_NAME       AS  VARCHAR2(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  VARCHAR2(128)) AS PARTITION_NAME,     CAST(V.PARTITION_POSITION AS    NUMBER) AS PARTITION_POSITION,     CAST(V.SUBPARTITION_NAME  AS    VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(V.SUBPARTITION_POSITION AS NUMBER) AS SUBPARTITION_POSITION,     CAST(V.OBJECT_TYPE AS VARCHAR2(12)) AS OBJECT_TYPE,     CAST(NULL AS    NUMBER) AS BLEVEL,     CAST(NULL AS    NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS    NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS    NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS    NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS    NUMBER) AS CLUSTERING_FACTOR,     CAST(STAT.ROW_CNT AS    NUMBER) AS NUM_ROWS,     CAST(NULL AS    NUMBER) AS AVG_CACHED_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHE_HIT_RATIO,     CAST(NULL AS    NUMBER) AS SAMPLE_SIZE,     CAST(STAT.LAST_ANALYZED AS  DATE) AS LAST_ANALYZED,     CAST(decode(STAT.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,     CAST(decode(STAT.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,     CAST(decode(bitand(STAT.STATTYPE_LOCKED, 15), NULL, NULL, 0, NULL, 1, 'DATA', 2, 'CACHE', 'ALL') AS    VARCHAR2(5)) AS STATTYPE_LOCKED,     CAST(decode(STAT.STALE_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS STALE_STATS,     CAST(NULL AS    VARCHAR2(7)) AS SCOPE     FROM     (       (SELECT TENANT_ID,               DATABASE_ID,               TABLE_ID,               DATA_TABLE_ID,               TABLE_ID AS PARTITION_ID,               SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,               NULL AS PARTITION_NAME,               NULL AS SUBPARTITION_NAME,               NULL AS PARTITION_POSITION,               NULL AS SUBPARTITION_POSITION,               'INDEX' AS OBJECT_TYPE           FROM              SYS.ALL_VIRTUAL_CORE_ALL_TABLE T           WHERE T.TABLE_TYPE = 5       UNION ALL        SELECT TENANT_ID,               DATABASE_ID,               TABLE_ID,               DATA_TABLE_ID,               CASE WHEN PART_LEVEL = 0 THEN TABLE_ID ELSE -1 END AS PARTITION_ID,               SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,               NULL AS PARTITION_NAME,               NULL AS SUBPARTITION_NAME,               NULL AS PARTITION_POSITION,               NULL AS SUBPARTITION_POSITION,               'INDEX' AS OBJECT_TYPE         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         WHERE T.TABLE_TYPE = 5)     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 T.DATA_TABLE_ID,                 P.PART_ID,                 SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) AS INDEX_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T           JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         WHERE T.TABLE_TYPE = 5     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                T.DATA_TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) AS INDEX_NAME,                P.PART_NAME,                SP.SUB_PART_NAME,                P.PART_IDX + 1,                SP.SUB_PART_IDX + 1,               'SUBPARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         JOIN             SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE = 5     ) V     JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T          ON T.TABLE_ID = V.DATA_TABLE_ID          AND T.TENANT_ID = V.TENANT_ID          AND T.DATABASE_ID = V.DATABASE_ID          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = V.DATABASE_ID         AND (V.DATABASE_ID = USERENV('SCHEMAID')              OR USER_CAN_ACCESS_OBJ(1, V.TABLE_ID, V.DATABASE_ID) = 1)         AND V.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN         SYS.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND V.PARTITION_ID = STAT.PARTITION_ID         AND STAT.INDEX_TYPE = 1; )__"))) {
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

int ObInnerTableSchema::dba_ind_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_IND_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_IND_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS OWNER,     CAST(V.INDEX_NAME AS     VARCHAR2(128)) AS INDEX_NAME,     CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS TABLE_OWNER,     CAST(T.TABLE_NAME       AS  VARCHAR2(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  VARCHAR2(128)) AS PARTITION_NAME,     CAST(V.PARTITION_POSITION AS    NUMBER) AS PARTITION_POSITION,     CAST(V.SUBPARTITION_NAME  AS    VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(V.SUBPARTITION_POSITION AS NUMBER) AS SUBPARTITION_POSITION,     CAST(V.OBJECT_TYPE AS VARCHAR2(12)) AS OBJECT_TYPE,     CAST(NULL AS    NUMBER) AS BLEVEL,     CAST(NULL AS    NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS    NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS    NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS    NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS    NUMBER) AS CLUSTERING_FACTOR,     CAST(STAT.ROW_CNT AS    NUMBER) AS NUM_ROWS,     CAST(NULL AS    NUMBER) AS AVG_CACHED_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHE_HIT_RATIO,     CAST(NULL AS    NUMBER) AS SAMPLE_SIZE,     CAST(STAT.LAST_ANALYZED AS  DATE) AS LAST_ANALYZED,     CAST(decode(STAT.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,     CAST(decode(STAT.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,     CAST(decode(bitand(STAT.STATTYPE_LOCKED, 15), NULL, NULL, 0, NULL, 1, 'DATA', 2, 'CACHE', 'ALL') AS    VARCHAR2(5)) AS STATTYPE_LOCKED,     CAST(decode(STAT.STALE_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS STALE_STATS,     CAST(NULL AS    VARCHAR2(7)) AS SCOPE     FROM     (       (SELECT TENANT_ID,               DATABASE_ID,               TABLE_ID,               DATA_TABLE_ID,               TABLE_ID AS PARTITION_ID,               SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,               NULL AS PARTITION_NAME,               NULL AS SUBPARTITION_NAME,               NULL AS PARTITION_POSITION,               NULL AS SUBPARTITION_POSITION,               'INDEX' AS OBJECT_TYPE           FROM              SYS.ALL_VIRTUAL_CORE_ALL_TABLE T           WHERE T.TABLE_TYPE = 5       UNION ALL        SELECT TENANT_ID,               DATABASE_ID,               TABLE_ID,               DATA_TABLE_ID,               CASE WHEN PART_LEVEL = 0 THEN TABLE_ID ELSE -1 END AS PARTITION_ID,               SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,               NULL AS PARTITION_NAME,               NULL AS SUBPARTITION_NAME,               NULL AS PARTITION_POSITION,               NULL AS SUBPARTITION_POSITION,               'INDEX' AS OBJECT_TYPE         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         WHERE T.TABLE_TYPE = 5)     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 T.DATA_TABLE_ID,                 P.PART_ID,                 SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) AS INDEX_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T           JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         WHERE T.TABLE_TYPE = 5     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                T.DATA_TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) AS INDEX_NAME,                P.PART_NAME,                SP.SUB_PART_NAME,                P.PART_IDX + 1,                SP.SUB_PART_IDX + 1,               'SUBPARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         JOIN             SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE = 5     ) V     JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T          ON T.TABLE_ID = V.DATA_TABLE_ID          AND T.TENANT_ID = V.TENANT_ID          AND T.DATABASE_ID = V.DATABASE_ID          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = V.DATABASE_ID     LEFT JOIN         SYS.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND V.PARTITION_ID = STAT.PARTITION_ID         AND STAT.INDEX_TYPE = 1; )__"))) {
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

int ObInnerTableSchema::user_ind_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_IND_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_IND_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(V.INDEX_NAME AS     VARCHAR2(128)) AS INDEX_NAME,     CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS TABLE_OWNER,     CAST(T.TABLE_NAME       AS  VARCHAR2(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  VARCHAR2(128)) AS PARTITION_NAME,     CAST(V.PARTITION_POSITION AS    NUMBER) AS PARTITION_POSITION,     CAST(V.SUBPARTITION_NAME  AS    VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(V.SUBPARTITION_POSITION AS NUMBER) AS SUBPARTITION_POSITION,     CAST(V.OBJECT_TYPE AS VARCHAR2(12)) AS OBJECT_TYPE,     CAST(NULL AS    NUMBER) AS BLEVEL,     CAST(NULL AS    NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS    NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS    NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS    NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS    NUMBER) AS CLUSTERING_FACTOR,     CAST(STAT.ROW_CNT AS    NUMBER) AS NUM_ROWS,     CAST(NULL AS    NUMBER) AS AVG_CACHED_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHE_HIT_RATIO,     CAST(NULL AS    NUMBER) AS SAMPLE_SIZE,     CAST(STAT.LAST_ANALYZED AS  DATE) AS LAST_ANALYZED,     CAST(decode(STAT.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,     CAST(decode(STAT.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,     CAST(decode(bitand(STAT.STATTYPE_LOCKED, 15), NULL, NULL, 0, NULL, 1, 'DATA', 2, 'CACHE', 'ALL') AS    VARCHAR2(5)) AS STATTYPE_LOCKED,     CAST(decode(STAT.STALE_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS STALE_STATS,     CAST(NULL AS    VARCHAR2(7)) AS SCOPE     FROM     (         SELECT TENANT_ID,                 DATABASE_ID,                 TABLE_ID,                 DATA_TABLE_ID,                 CASE WHEN PART_LEVEL = 0 THEN TABLE_ID ELSE -1 END AS PARTITION_ID,                 SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 NULL AS PARTITION_NAME,                 NULL AS SUBPARTITION_NAME,                 NULL AS PARTITION_POSITION,                 NULL AS SUBPARTITION_POSITION,                 'INDEX' AS OBJECT_TYPE         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         WHERE T.TABLE_TYPE = 5 AND T.DATABASE_ID = USERENV('SCHEMAID')     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 T.DATA_TABLE_ID,                 P.PART_ID,                 SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) AS INDEX_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T           JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         WHERE T.TABLE_TYPE = 5 AND T.DATABASE_ID = USERENV('SCHEMAID')     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                T.DATA_TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) AS INDEX_NAME,                P.PART_NAME,                SP.SUB_PART_NAME,                P.PART_IDX + 1,                SP.SUB_PART_IDX + 1,               'SUBPARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         JOIN             SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE = 5 AND T.DATABASE_ID = USERENV('SCHEMAID')     ) V     JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T          ON T.TABLE_ID = V.DATA_TABLE_ID          AND T.TENANT_ID = V.TENANT_ID          AND T.DATABASE_ID = V.DATABASE_ID          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = V.DATABASE_ID     LEFT JOIN         SYS.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND V.PARTITION_ID = STAT.PARTITION_ID         AND STAT.INDEX_TYPE = 1;     WHERE T.TABLE_TYPE != 12       AND T.TABLE_TYPE != 13 )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_jobs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_JOBS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_JOBS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     EXECUTOR_TENANT_ID,     PLUS_ARCHIVELOG,     BACKUP_TYPE,     JOB_LEVEL,     ENCRYPTION_MODE,     PASSWD,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')       END AS END_TIMESTAMP,     STATUS,     RESULT,     "COMMENT",     DESCRIPTION,     PATH     FROM SYS.ALL_VIRTUAL_BACKUP_JOB     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_job_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_JOB_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_JOB_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     EXECUTOR_TENANT_ID,     PLUS_ARCHIVELOG,     BACKUP_TYPE,     JOB_LEVEL,     ENCRYPTION_MODE,     PASSWD,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')       END AS END_TIMESTAMP,     STATUS,     RESULT,     "COMMENT",     DESCRIPTION,     PATH     FROM SYS.ALL_VIRTUAL_BACKUP_JOB_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_TASKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TASK_ID,     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')       END AS END_TIMESTAMP,     STATUS,     START_SCN,     END_SCN,     USER_LS_START_SCN,     ENCRYPTION_MODE,     PASSWD,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     "COMMENT",     PATH,     MINOR_TURN_ID,     MAJOR_TURN_ID     FROM SYS.ALL_VIRTUAL_BACKUP_TASK     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_task_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_TASK_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_TASK_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TASK_ID,     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')       END AS END_TIMESTAMP,     STATUS,     START_SCN,     END_SCN,     USER_LS_START_SCN,     ENCRYPTION_MODE,     PASSWD,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     "COMMENT",     PATH,     MINOR_TURN_ID,     MAJOR_TURN_ID     FROM SYS.ALL_VIRTUAL_BACKUP_TASK_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_set_files_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_SET_FILES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_SET_FILES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     BACKUP_SET_ID,     DEST_ID,     INCARNATION,     BACKUP_TYPE,     PREV_FULL_BACKUP_SET_ID,     PREV_INC_BACKUP_SET_ID,     TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')       END AS END_TIMESTAMP,     STATUS,     FILE_STATUS,     CASE       WHEN END_TS = 0         THEN 0       ELSE         ROUND((END_TS - START_TS)/1000/1000,0)       END AS ELAPSED_SECONDES,     PLUS_ARCHIVELOG,     START_REPLAY_SCN,     SCN_TO_TIMESTAMP(START_REPLAY_SCN) AS START_REPLAY_SCN_DISPLAY,     MIN_RESTORE_SCN,     CASE       WHEN MIN_RESTORE_SCN_DISPLAY != ''         THEN MIN_RESTORE_SCN_DISPLAY       WHEN MIN_RESTORE_SCN = 0          THEN NULL       ELSE         TO_CHAR(SCN_TO_TIMESTAMP(MIN_RESTORE_SCN),'YYYY-MM-DDHH24:MI:SS.FF9')       END AS MIN_RESTORE_SCN_DISPLAY,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     "COMMENT",     ENCRYPTION_MODE,     PASSWD,     TENANT_COMPATIBLE,     BACKUP_COMPATIBLE,     PATH,     CLUSTER_VERSION,     CONSISTENT_SCN,     MINOR_TURN_ID,     MAJOR_TURN_ID     FROM SYS.ALL_VIRTUAL_BACKUP_SET_FILES     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_tab_modifications_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TAB_MODIFICATIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TAB_MODIFICATIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT   CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS TABLE_OWNER,   CAST(T.TABLE_NAME AS         VARCHAR2(128)) AS TABLE_NAME,   CAST(P.PART_NAME AS     VARCHAR2(128)) AS PARTITION_NAME,   CAST(SP.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,   CAST(V.INSERTS AS     NUMBER) AS INSERTS,   CAST(V.UPDATES AS     NUMBER) AS UPDATES,   CAST(V.DELETES AS     NUMBER) AS DELETES,   CAST(V.MODIFIED_TIME AS     DATE) AS TIMESTAMP,   CAST(NULL AS     VARCHAR2(3)) AS TRUNCATED,   CAST(NULL AS     NUMBER) AS DROP_SEGMENTS   FROM     (SELECT      CASE WHEN T.TENANT_ID IS NOT NULL THEN T.TENANT_ID ELSE VT.TENANT_ID END AS TENANT_ID,      CASE WHEN T.TABLE_ID IS NOT NULL THEN T.TABLE_ID ELSE VT.TABLE_ID END AS TABLE_ID,      CASE WHEN T.TABLET_ID IS NOT NULL THEN T.TABLET_ID ELSE VT.TABLET_ID END AS TABLET_ID,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.INSERTS + VT.INSERT_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.INSERTS ELSE VT.INSERT_ROW_COUNT END) END AS INSERTS,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.UPDATES + VT.UPDATE_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.UPDATES ELSE VT.UPDATE_ROW_COUNT END) END AS UPDATES,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.DELETES + VT.DELETE_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.DELETES ELSE VT.DELETE_ROW_COUNT END) END AS DELETES,      CASE WHEN T.GMT_MODIFIED IS NOT NULL THEN T.GMT_MODIFIED ELSE NULL END AS MODIFIED_TIME      FROM      SYS.ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT T      FULL JOIN      SYS.ALL_VIRTUAL_DML_STATS VT      ON T.TENANT_ID = VT.TENANT_ID AND T.TABLE_ID = VT.TABLE_ID AND T.TABLET_ID = VT.TABLET_ID     )V     JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T          ON T.TABLE_ID = V.TABLE_ID          AND T.TENANT_ID = V.TENANT_ID          AND T.TABLE_TYPE IN (0, 3, 8, 9)          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = T.DATABASE_ID         AND (DB.DATABASE_ID = USERENV('SCHEMAID')              OR USER_CAN_ACCESS_OBJ(1, V.TABLE_ID, DB.DATABASE_ID) = 1)     LEFT JOIN         SYS.ALL_VIRTUAL_PART_REAL_AGENT P         ON V.TENANT_ID = P.TENANT_ID         AND V.TABLE_ID = P.TABLE_ID         AND V.TABLET_ID = P.TABLET_ID     LEFT JOIN         SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP         ON V.TENANT_ID = SP.TENANT_ID         AND V.TABLE_ID = SP.TABLE_ID         AND V.TABLET_ID = SP.TABLET_ID   )__"))) {
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

int ObInnerTableSchema::dba_tab_modifications_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_MODIFICATIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_MODIFICATIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT   CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS TABLE_OWNER,   CAST(T.TABLE_NAME AS         VARCHAR2(128)) AS TABLE_NAME,   CAST(P.PART_NAME AS     VARCHAR2(128)) AS PARTITION_NAME,   CAST(SP.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,   CAST(V.INSERTS AS     NUMBER) AS INSERTS,   CAST(V.UPDATES AS     NUMBER) AS UPDATES,   CAST(V.DELETES AS     NUMBER) AS DELETES,   CAST(V.MODIFIED_TIME AS     DATE) AS TIMESTAMP,   CAST(NULL AS     VARCHAR2(3)) AS TRUNCATED,   CAST(NULL AS     NUMBER) AS DROP_SEGMENTS   FROM     (SELECT      CASE WHEN T.TENANT_ID IS NOT NULL THEN T.TENANT_ID ELSE VT.TENANT_ID END AS TENANT_ID,      CASE WHEN T.TABLE_ID IS NOT NULL THEN T.TABLE_ID ELSE VT.TABLE_ID END AS TABLE_ID,      CASE WHEN T.TABLET_ID IS NOT NULL THEN T.TABLET_ID ELSE VT.TABLET_ID END AS TABLET_ID,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.INSERTS + VT.INSERT_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.INSERTS ELSE VT.INSERT_ROW_COUNT END) END AS INSERTS,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.UPDATES + VT.UPDATE_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.UPDATES ELSE VT.UPDATE_ROW_COUNT END) END AS UPDATES,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.DELETES + VT.DELETE_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.DELETES ELSE VT.DELETE_ROW_COUNT END) END AS DELETES,      CASE WHEN T.GMT_MODIFIED IS NOT NULL THEN T.GMT_MODIFIED ELSE NULL END AS MODIFIED_TIME      FROM      SYS.ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT T      FULL JOIN      SYS.ALL_VIRTUAL_DML_STATS VT      ON T.TENANT_ID = VT.TENANT_ID AND T.TABLE_ID = VT.TABLE_ID AND T.TABLET_ID = VT.TABLET_ID     )V     JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T          ON T.TABLE_ID = V.TABLE_ID          AND T.TENANT_ID = V.TENANT_ID          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()          AND T.TABLE_TYPE IN (0, 3, 8, 9)     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = T.DATABASE_ID     LEFT JOIN         SYS.ALL_VIRTUAL_PART_REAL_AGENT P         ON V.TENANT_ID = P.TENANT_ID         AND V.TABLE_ID = P.TABLE_ID         AND V.TABLET_ID = P.TABLET_ID     LEFT JOIN         SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP         ON V.TENANT_ID = SP.TENANT_ID         AND V.TABLE_ID = SP.TABLE_ID         AND V.TABLET_ID = SP.TABLET_ID   )__"))) {
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

int ObInnerTableSchema::user_tab_modifications_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TAB_MODIFICATIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TAB_MODIFICATIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT   CAST(T.TABLE_NAME AS         VARCHAR2(128)) AS TABLE_NAME,   CAST(P.PART_NAME AS     VARCHAR2(128)) AS PARTITION_NAME,   CAST(SP.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,   CAST(V.INSERTS AS     NUMBER) AS INSERTS,   CAST(V.UPDATES AS     NUMBER) AS UPDATES,   CAST(V.DELETES AS     NUMBER) AS DELETES,   CAST(V.MODIFIED_TIME AS     DATE) AS TIMESTAMP,   CAST(NULL AS     VARCHAR2(3)) AS TRUNCATED,   CAST(NULL AS     NUMBER) AS DROP_SEGMENTS   FROM     (SELECT      CASE WHEN T.TENANT_ID IS NOT NULL THEN T.TENANT_ID ELSE VT.TENANT_ID END AS TENANT_ID,      CASE WHEN T.TABLE_ID IS NOT NULL THEN T.TABLE_ID ELSE VT.TABLE_ID END AS TABLE_ID,      CASE WHEN T.TABLET_ID IS NOT NULL THEN T.TABLET_ID ELSE VT.TABLET_ID END AS TABLET_ID,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.INSERTS + VT.INSERT_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.INSERTS ELSE VT.INSERT_ROW_COUNT END) END AS INSERTS,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.UPDATES + VT.UPDATE_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.UPDATES ELSE VT.UPDATE_ROW_COUNT END) END AS UPDATES,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.DELETES + VT.DELETE_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.DELETES ELSE VT.DELETE_ROW_COUNT END) END AS DELETES,      CASE WHEN T.GMT_MODIFIED IS NOT NULL THEN T.GMT_MODIFIED ELSE NULL END AS MODIFIED_TIME      FROM      SYS.ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT T      FULL JOIN      SYS.ALL_VIRTUAL_DML_STATS VT      ON T.TENANT_ID = VT.TENANT_ID AND T.TABLE_ID = VT.TABLE_ID AND T.TABLET_ID = VT.TABLET_ID     )V     JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T          ON T.TABLE_ID = V.TABLE_ID          AND T.TENANT_ID = V.TENANT_ID          AND T.TABLE_TYPE IN (0, 3, 8, 9)          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()          AND T.DATABASE_ID = USERENV('SCHEMAID')     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = T.DATABASE_ID     LEFT JOIN         SYS.ALL_VIRTUAL_PART_REAL_AGENT P         ON V.TENANT_ID = P.TENANT_ID         AND V.TABLE_ID = P.TABLE_ID         AND V.TABLET_ID = P.TABLET_ID     LEFT JOIN         SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP         ON V.TENANT_ID = SP.TENANT_ID         AND V.TABLE_ID = SP.TABLE_ID         AND V.TABLET_ID = SP.TABLET_ID   )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_storage_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_STORAGE_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_STORAGE_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     PATH,     ENDPOINT,     DEST_ID,     DEST_TYPE,     AUTHORIZATION,     EXTENSION,     CHECK_FILE_NAME,     TO_CHAR(LAST_CHECK_TIME / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS LAST_CHECK_TIMESTAMP     FROM SYS.ALL_VIRTUAL_BACKUP_STORAGE_INFO     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_storage_info_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     PATH,     ENDPOINT,     DEST_ID,     DEST_TYPE,     AUTHORIZATION,     EXTENSION,     CHECK_FILE_NAME,     TO_CHAR(LAST_CHECK_TIME / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS LAST_CHECK_TIMESTAMP     FROM SYS.ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_policy_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_POLICY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_POLICY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       POLICY_NAME,       RECOVERY_WINDOW     FROM SYS.ALL_VIRTUAL_BACKUP_DELETE_POLICY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_jobs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_JOBS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_JOBS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       JOB_ID,       INCARNATION,       INITIATOR_TENANT_ID,       INITIATOR_JOB_ID,       EXECUTOR_TENANT_ID,       TYPE,       TO_CHAR(PARAMETER / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS PARAMETER,       JOB_LEVEL,       TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')         END AS END_TIMESTAMP,       STATUS,       TASK_COUNT,       SUCCESS_TASK_COUNT,       RESULT,       "COMMENT"     FROM SYS.ALL_VIRTUAL_BACKUP_DELETE_JOB     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_job_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       JOB_ID,       INCARNATION,       INITIATOR_TENANT_ID,       INITIATOR_JOB_ID,       EXECUTOR_TENANT_ID,       TYPE,       TO_CHAR(PARAMETER / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS PARAMETER,       JOB_LEVEL,       TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')         END AS END_TIMESTAMP,       STATUS,       TASK_COUNT,       SUCCESS_TASK_COUNT,       RESULT,       "COMMENT"     FROM SYS.ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_TASKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TASK_ID,       INCARNATION,       JOB_ID,       TASK_TYPE,       ID,       ROUND_ID,       DEST_ID,       TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')         END AS END_TIMESTAMP,       STATUS,       TOTAL_LS_COUNT,       FINISH_LS_COUNT,       RESULT,       "COMMENT",       PATH     FROM SYS.ALL_VIRTUAL_BACKUP_DELETE_TASK     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_task_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TASK_ID,       INCARNATION,       JOB_ID,       TASK_TYPE,       ID,       ROUND_ID,       DEST_ID,       TO_CHAR(START_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           TO_CHAR(END_TS / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')         END AS END_TIMESTAMP,       STATUS,       TOTAL_LS_COUNT,       FINISH_LS_COUNT,       RESULT,       "COMMENT",       PATH     FROM SYS.ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_restore_progress_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RESTORE_PROGRESS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RESTORE_PROGRESS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     P.JOB_ID AS JOB_ID,     RESTORE_TENANT_NAME,     RESTORE_TENANT_ID,     BACKUP_TENANT_NAME,     BACKUP_TENANT_ID,     BACKUP_CLUSTER_NAME,     BACKUP_DEST,     RESTORE_OPTION,     RESTORE_SCN,     CASE       WHEN RESTORE_SCN IS NULL         THEN NULL       ELSE         SCN_TO_TIMESTAMP(RESTORE_SCN)       END AS RESTORE_SCN_DISPLAY,     CASE       WHEN STATUS = 'RESTORE_PRE'         THEN 'RESTORING'       WHEN STATUS = 'RESTORE_CREATE_INIT_LS'         THEN 'RESTORING'       WHEN STATUS = 'RESTORE_WAIT_LS'         THEN 'RESTORING'       WHEN STATUS = 'POST_CHECK'         THEN 'RESTORING'       ELSE STATUS       END AS STATUS,     CASE       WHEN START_TIMESTAMP IS NULL         THEN NULL       WHEN START_TIMESTAMP=''         THEN NULL       WHEN START_TIMESTAMP='0'         THEN NULL       ELSE         TO_CHAR(TO_NUMBER(START_TIMESTAMP) / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss')       END AS START_TIMESTAMP,     BACKUP_SET_LIST,     BACKUP_PIECE_LIST,     TOTAL_BYTES,     CASE       WHEN TOTAL_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN TOTAL_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN TOTAL_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(TOTAL_BYTES/1024/1024,2), 'MB')       END AS TOTAL_BYTES_DISPLAY,     FINISH_BYTES,     CASE       WHEN FINISH_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN FINISH_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN FINISH_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(FINISH_BYTES/1024/1024,2), 'MB')       END AS FINISH_BYTES_DISPLAY,     DESCRIPTION     FROM   (       SELECT       TENANT_ID,       JOB_ID,       MAX(CASE NAME WHEN 'tenant_name' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS RESTORE_TENANT_NAME,       MAX(CASE NAME WHEN 'tenant_id' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS RESTORE_TENANT_ID,       MAX(CASE NAME WHEN 'backup_tenant_name' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS BACKUP_TENANT_NAME,       MAX(CASE NAME WHEN 'backup_tenant_id' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS BACKUP_TENANT_ID,       MAX(CASE NAME WHEN 'backup_cluster_name' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS BACKUP_CLUSTER_NAME,       MAX(CASE NAME WHEN 'target_tenant_role' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS TENANT_ROLE,       MAX(CASE NAME WHEN 'backup_dest' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS BACKUP_DEST,       MAX(CASE NAME WHEN 'restore_option' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS RESTORE_OPTION,       MAX(CASE NAME WHEN 'status' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS STATUS,       MAX(CASE NAME WHEN 'restore_scn' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS RESTORE_SCN,       MAX(CASE NAME WHEN 'restore_start_ts' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS START_TIMESTAMP,       MAX(CASE NAME WHEN 'backup_set_list' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS BACKUP_SET_LIST,       MAX(CASE NAME WHEN 'backup_piece_list' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS BACKUP_PIECE_LIST,       MAX(CASE NAME WHEN 'description' THEN CAST(VALUE AS VARCHAR2(4096)) ELSE '' END) AS DESCRIPTION       FROM SYS.ALL_VIRTUAL_RESTORE_JOB GROUP BY TENANT_ID, JOB_ID   ) P LEFT JOIN   (       SELECT       TENANT_ID,       JOB_ID,       TOTAL_BYTES,       FINISH_BYTES       FROM SYS.ALL_VIRTUAL_RESTORE_PROGRESS   ) J     ON P.TENANT_ID=J.TENANT_ID AND P.JOB_ID=J.JOB_ID     WHERE P.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_restore_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RESTORE_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RESTORE_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     RESTORE_TENANT_NAME,     RESTORE_TENANT_ID,     BACKUP_TENANT_NAME,     BACKUP_TENANT_ID,     BACKUP_CLUSTER_NAME,     BACKUP_DEST,     RESTORE_SCN,     SCN_TO_TIMESTAMP(RESTORE_SCN) AS RESTORE_SCN_DISPLAY,     RESTORE_OPTION,     START_TIME AS START_TIMESTAMP,     FINISH_TIME AS FINISH_TIMESTAMP,     STATUS,     BACKUP_PIECE_LIST,     BACKUP_SET_LIST,     BACKUP_CLUSTER_VERSION,     LS_COUNT,     FINISH_LS_COUNT,     TABLET_COUNT,     FINISH_TABLET_COUNT,     TOTAL_BYTES,     CASE       WHEN TOTAL_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN TOTAL_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN TOTAL_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(TOTAL_BYTES/1024/1024,2), 'MB')       END AS TOTAL_BYTES_DISPLAY,     FINISH_BYTES,     CASE       WHEN FINISH_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN FINISH_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN FINISH_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(FINISH_BYTES/1024/1024,2), 'MB')       END AS FINISH_BYTES_DISPLAY,     DESCRIPTION,     "COMMENT"     FROM SYS.ALL_VIRTUAL_RESTORE_JOB_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_archive_dest_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARCHIVE_DEST_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARCHIVE_DEST_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     DEST_NO,     NAME,     VALUE     FROM SYS.ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_archivelog_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARCHIVELOG_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARCHIVELOG_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     DEST_ID,     ROUND_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     SCN_TO_TIMESTAMP(START_SCN) AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     SCN_TO_TIMESTAMP(CHECKPOINT_SCN) AS CHECKPOINT_SCN_DISPLAY,     COMPATIBLE,     BASE_PIECE_ID,     USED_PIECE_ID,     PIECE_SWITCH_INTERVAL,     UNIT_SIZE,     COMPRESSION,     (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) AS INPUT_BYTES,     CASE       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024/1024/1024,2), 'PB')       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024/1024,2), 'TB')       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) AS OUTPUT_BYTES,     CASE       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024/1024/1024,2), 'PB')       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024/1024,2), 'TB')       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) = 0         THEN 0       ELSE         ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) / (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES), 2)       END AS COMPRESSION_RATIO,     DELETED_INPUT_BYTES,     CASE       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_INPUT_BYTES_DISPLAY,     DELETED_OUTPUT_BYTES,     CASE       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_OUTPUT_BYTES_DISPLAY,     "COMMENT",     PATH     FROM SYS.ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_archivelog_summary_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARCHIVELOG_SUMMARY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARCHIVELOG_SUMMARY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     DEST_ID,     ROUND_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     SCN_TO_TIMESTAMP(START_SCN) AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     SCN_TO_TIMESTAMP(CHECKPOINT_SCN) AS CHECKPOINT_SCN_DISPLAY,     COMPATIBLE,     BASE_PIECE_ID,     USED_PIECE_ID,     PIECE_SWITCH_INTERVAL,     UNIT_SIZE,     COMPRESSION,     INPUT_BYTES,     CASE       WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     OUTPUT_BYTES,     CASE       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN INPUT_BYTES = 0         THEN 0       ELSE         ROUND(OUTPUT_BYTES / INPUT_BYTES, 2)       END AS COMPRESSION_RATIO,     DELETED_INPUT_BYTES,     CASE       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_INPUT_BYTES_DISPLAY,     DELETED_OUTPUT_BYTES,     CASE       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_OUTPUT_BYTES_DISPLAY,     PATH,     "COMMENT"     FROM ( SELECT DEST_ID,        ROUND_ID,        INCARNATION,        DEST_NO,        STATUS,        START_SCN,        CHECKPOINT_SCN,        COMPATIBLE,        BASE_PIECE_ID,        USED_PIECE_ID,        PIECE_SWITCH_INTERVAL,        UNIT_SIZE,        COMPRESSION,        (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) AS INPUT_BYTES,        (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) AS OUTPUT_BYTES,        DELETED_INPUT_BYTES,        DELETED_OUTPUT_BYTES,        PATH,        "COMMENT"        FROM SYS.ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS        WHERE TENANT_ID = EFFECTIVE_TENANT_ID() AND STATUS != 'STOP' UNION SELECT DEST_ID,        ROUND_ID,        INCARNATION,        DEST_NO,        'STOP' AS STATUS,        START_SCN,        CHECKPOINT_SCN,        COMPATIBLE,        BASE_PIECE_ID,        USED_PIECE_ID,        PIECE_SWITCH_INTERVAL,        UNIT_SIZE,        COMPRESSION,        INPUT_BYTES,        OUTPUT_BYTES,        DELETED_INPUT_BYTES,        DELETED_OUTPUT_BYTES,        PATH,        "COMMENT"        FROM SYS.ALL_VIRTUAL_LOG_ARCHIVE_HISTORY        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) )__"))) {
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

int ObInnerTableSchema::dba_ob_archivelog_piece_files_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARCHIVELOG_PIECE_FILES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARCHIVELOG_PIECE_FILES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     DEST_ID,     ROUND_ID,     PIECE_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     SCN_TO_TIMESTAMP(START_SCN) AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     SCN_TO_TIMESTAMP(CHECKPOINT_SCN) AS CHECKPOINT_SCN_DISPLAY,     MAX_SCN,     END_SCN,     SCN_TO_TIMESTAMP(END_SCN) AS END_SCN_DISPLAY,     COMPATIBLE,     UNIT_SIZE,     COMPRESSION,     INPUT_BYTES,     CASE       WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     OUTPUT_BYTES,     CASE       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN INPUT_BYTES = 0         THEN 0       ELSE         ROUND(OUTPUT_BYTES / INPUT_BYTES, 2)       END AS COMPRESSION_RATIO,     FILE_STATUS,     PATH     FROM SYS.ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_parameter_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_PARAMETER_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_PARAMETER_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     NAME,     VALUE     FROM SYS.ALL_VIRTUAL_BACKUP_PARAMETER     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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
