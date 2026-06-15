/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

int ObInnerTableSchema::all_plsql_type_attrs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PLSQL_TYPE_ATTRS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PLSQL_TYPE_ATTRS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       d.database_name as owner,       cast(pt.type_name as varchar2(136)) as type_name,       p.package_name as package_name,       cast(a.name as varchar2(128)) as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       cast(null as varchar2(128)) as attr_type_owner,       CAST(             CASE BITAND(A.PROPERTIES, 15)             WHEN 3               THEN DECODE (A.TYPE_ATTR_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(A.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(A.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(A.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(A.LENGTH, ')')),                 46, DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 50, 'NUMBER',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS attr_type_name,       NULL as attr_type_package,       a.length as length,       a.number_precision as PRECISION,       a.scale as scale,       cast('CHAR_CS' as varchar2(44)) as character_set_name,       a.attribute as attr_no,       cast(decode(a.number_precision, 1, 'C', 'B') AS varchar2(1)) as char_used     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id            and bitand(a.properties, 15) = 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)     UNION ALL     select           d.database_name as owner,           cast(pt.type_name as varchar2(136)) as type_name,           p.package_name as package_name,           cast(a.name as varchar2(128)) as attr_name,           cast(null as varchar2(7)) as attr_type_mod,           cast(d1.database_name as varchar2(128)) as attr_type_owner,           CAST(t.TYPE_NAME AS VARCHAR2(136)) AS attr_type_name,           NULL as attr_type_package,           a.length as length,           a.number_precision as PRECISION,           a.scale as scale,           cast('CHAR_CS' as varchar2(44)) as character_set_name,           a.attribute as attr_no,           cast(decode(a.number_precision, 1, 'C', 'B') AS varchar2(1)) as char_used         from sys.all_virtual_pkg_type_real_agent pt           join sys.all_virtual_package_real_agent p             on pt.package_id = p.package_id           join sys.all_virtual_pkg_type_attr_real_agent A             on pt.type_id = a.type_id               and bitand(a.properties, 15) != 3           join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d             on pt.database_id = d.database_id             and (pt.database_id = USERENV('SCHEMAID')                   or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)           join sys.all_virtual_type_real_agent t             on t.type_id = a.type_attr_id           join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1             on t.database_id = d1.database_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       d1.database_name as attr_type_owner,       cast(pt1.type_name as varchar2(136)) as attr_type_name,       p1.package_name as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')             or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent p1         on A.attr_package_id = p1.package_id       join sys.all_virtual_pkg_type_real_agent pt1         on A.attr_package_id = pt1.package_id         and pt1.type_id = a.type_attr_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on p1.database_id = d1.database_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       'SYS' as attr_type_owner,       cast(t.TYPE_NAME AS VARCHAR2(136)) as attr_type_name,       NULL as attr_type_package,       NULL as length,       NULL as PRECISION,       NULL as scale,       NULL as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')             or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = A.type_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_type_sys_agent T         on t.type_id = a.type_attr_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       'SYS' as attr_type_owner,       cast(pts.type_name as varchar2(136)) as attr_type_name,       ps.package_name as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')             or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id       join sys.all_virtual_package_sys_agent ps         on a.attr_package_id = ps.package_id       join sys.all_virtual_pkg_type_sys_agent pts         on a.attr_package_id = pts.package_id         and pts.type_id = a.type_attr_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       d1.database_name as attr_type_owner,       cast(case bitand(a.properties, 15)           when 9 then tbl.table_name || '%ROWTYPE'           else 'NOT SUPPORT' end as varchar2(136)) as attr_type_name,       NULL as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = A.type_id         and (bitand(a.properties, 15) = 9             or bitand(a.properties, 15) = 10)       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')             or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.ALL_VIRTUAL_TABLE_REAL_AGENT tbl         on a.type_attr_id = tbl.table_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on tbl.database_id = d1.database_id     UNION ALL     select       'SYS' as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       cast(NULL as varchar2(128)) as attr_type_owner,       CAST(             CASE BITAND(a.PROPERTIES, 15)             WHEN 3               THEN DECODE (a.TYPE_attr_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(a.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(a.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(a.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(a.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(a.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(a.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(a.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(a.LENGTH, ')')),                 46, DECODE(a.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 50, 'NUMBER',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS attr_TYPE_NAME,       NULL as attr_type_package,       a.length as length,       a.number_precision as PRECISION,       a.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       a.attribute as attr_no,       CAST(DECODE(a.number_precision, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED     from sys.all_virtual_pkg_type_sys_agent pt       join sys.all_virtual_package_sys_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_sys_agent a         on pt.type_id = a.type_id            and bitand(a.properties, 15) = 3     UNION ALL     select           'SYS' as owner,           pt.type_name as type_name,           p.package_name as package_name,           a.name as attr_name,           cast(null as varchar2(7)) as attr_type_mod,           cast('SYS' as varchar2(128)) as attr_type_owner,           CAST(t.TYPE_NAME AS VARCHAR2(136)) AS attr_TYPE_NAME,           NULL as attr_type_package,           a.length as length,           a.number_precision as PRECISION,           a.scale as scale,           CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,           a.attribute as attr_no,           CAST(DECODE(a.length, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED         from sys.all_virtual_pkg_type_sys_agent pt           join sys.all_virtual_package_sys_agent P             on pt.package_id = p.package_id           join sys.all_virtual_pkg_type_attr_sys_agent a             on pt.type_id = a.type_id               and bitand(a.properties, 15) != 3           join sys.all_virtual_type_sys_agent t             on t.type_id = a.type_attr_id )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_plsql_type_attrs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PLSQL_TYPE_ATTRS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PLSQL_TYPE_ATTRS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       d.database_name as owner,       cast(pt.type_name as varchar2(136)) as type_name,       p.package_name as package_name,       cast(a.name as varchar2(128)) as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       cast(null as varchar2(128)) as attr_type_owner,       CAST(             CASE BITAND(A.PROPERTIES, 15)             WHEN 3               THEN DECODE (A.TYPE_ATTR_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(A.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(A.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(A.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(A.LENGTH, ')')),                 46, DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 50, 'NUMBER',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS attr_type_name,       NULL as attr_type_package,       a.length as length,       a.number_precision as PRECISION,       a.scale as scale,       cast('CHAR_CS' as varchar2(44)) as character_set_name,       a.attribute as attr_no,       cast(decode(a.number_precision, 1, 'C', 'B') AS varchar2(1)) as char_used     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id            and bitand(a.properties, 15) = 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id     UNION ALL     select           d.database_name as owner,           cast(pt.type_name as varchar2(136)) as type_name,           p.package_name as package_name,           cast(a.name as varchar2(128)) as attr_name,           cast(null as varchar2(7)) as attr_type_mod,           cast(d1.database_name as varchar2(128)) as attr_type_owner,           CAST(t.TYPE_NAME AS VARCHAR2(136)) AS attr_type_name,           NULL as attr_type_package,           a.length as length,           a.number_precision as PRECISION,           a.scale as scale,           cast('CHAR_CS' as varchar2(44)) as character_set_name,           a.attribute as attr_no,           cast(decode(a.number_precision, 1, 'C', 'B') AS varchar2(1)) as char_used         from sys.all_virtual_pkg_type_real_agent pt           join sys.all_virtual_package_real_agent p             on pt.package_id = p.package_id           join sys.all_virtual_pkg_type_attr_real_agent A             on pt.type_id = a.type_id               and bitand(a.properties, 15) != 3           join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d             on pt.database_id = d.database_id           join sys.all_virtual_type_real_agent t             on t.type_id = a.type_attr_id           join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1             on t.database_id = d1.database_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       d1.database_name as attr_type_owner,       cast(pt1.type_name as varchar2(136)) as attr_type_name,       p1.package_name as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id       join sys.all_virtual_package_real_agent p1         on A.attr_package_id = p1.package_id       join sys.all_virtual_pkg_type_real_agent pt1         on A.attr_package_id = pt1.package_id         and pt1.type_id = a.type_attr_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on p1.database_id = d1.database_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       'SYS' as attr_type_owner,       cast(t.TYPE_NAME AS VARCHAR2(136)) as attr_type_name,       NULL as attr_type_package,       NULL as length,       NULL as PRECISION,       NULL as scale,       NULL as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = A.type_id       join sys.all_virtual_type_sys_agent T         on t.type_id = a.type_attr_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       'SYS' as attr_type_owner,       cast(pts.type_name as varchar2(136)) as attr_type_name,       ps.package_name as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id       join sys.all_virtual_package_sys_agent ps         on a.attr_package_id = ps.package_id       join sys.all_virtual_pkg_type_sys_agent pts         on a.attr_package_id = pts.package_id         and pts.type_id = a.type_attr_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       d1.database_name as attr_type_owner,       cast(case bitand(a.properties, 15)           when 9 then tbl.table_name || '%ROWTYPE'           else 'NOT SUPPORT' end as varchar2(136)) as attr_type_name,       NULL as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = A.type_id         and (bitand(a.properties, 15) = 9             or bitand(a.properties, 15) = 10)       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id       join sys.ALL_VIRTUAL_TABLE_REAL_AGENT tbl         on a.type_attr_id = tbl.table_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on tbl.database_id = d1.database_id     UNION ALL     select       'SYS' as owner,       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       cast(NULL as varchar2(128)) as attr_type_owner,       CAST(             CASE BITAND(a.PROPERTIES, 15)             WHEN 3               THEN DECODE (a.TYPE_attr_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(a.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(a.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(a.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(a.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(a.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(a.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(a.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(a.LENGTH, ')')),                 46, DECODE(a.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 50, 'NUMBER',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS attr_TYPE_NAME,       NULL as attr_type_package,       a.length as length,       a.number_precision as PRECISION,       a.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       a.attribute as attr_no,       CAST(DECODE(a.number_precision, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED     from sys.all_virtual_pkg_type_sys_agent pt       join sys.all_virtual_package_sys_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_sys_agent a         on pt.type_id = a.type_id            and bitand(a.properties, 15) = 3     UNION ALL     select           'SYS' as owner,           pt.type_name as type_name,           p.package_name as package_name,           a.name as attr_name,           cast(null as varchar2(7)) as attr_type_mod,           cast('SYS' as varchar2(128)) as attr_type_owner,           CAST(t.TYPE_NAME AS VARCHAR2(136)) AS attr_TYPE_NAME,           NULL as attr_type_package,           a.length as length,           a.number_precision as PRECISION,           a.scale as scale,           CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,           a.attribute as attr_no,           CAST(DECODE(a.number_precision, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED         from sys.all_virtual_pkg_type_sys_agent pt           join sys.all_virtual_package_sys_agent P             on pt.package_id = p.package_id           join sys.all_virtual_pkg_type_attr_sys_agent a             on pt.type_id = a.type_id               and bitand(a.properties, 15) != 3           join sys.all_virtual_type_sys_agent t             on t.type_id = a.type_attr_id )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::user_plsql_type_attrs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_PLSQL_TYPE_ATTRS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PLSQL_TYPE_ATTRS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       cast(pt.type_name as varchar2(136)) as type_name,       p.package_name as package_name,       cast(a.name as varchar2(128)) as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       cast(null as varchar2(128)) as attr_type_owner,       CAST(             CASE BITAND(A.PROPERTIES, 15)             WHEN 3               THEN DECODE (A.TYPE_ATTR_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(A.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(A.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(A.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(A.LENGTH, ')')),                 46, DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 50, 'NUMBER',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS attr_type_name,       NULL as attr_type_package,       a.length as length,       a.number_precision as PRECISION,       a.scale as scale,       cast('CHAR_CS' as varchar2(44)) as character_set_name,       a.attribute as attr_no,       cast(decode(a.number_precision, 1, 'C', 'B') AS varchar2(1)) as char_used     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id            and bitand(a.properties, 15) = 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')     UNION ALL     select           cast(pt.type_name as varchar2(136)) as type_name,           p.package_name as package_name,           cast(a.name as varchar2(128)) as attr_name,           cast(null as varchar2(7)) as attr_type_mod,           cast(d1.database_name as varchar2(128)) as attr_type_owner,           CAST(t.TYPE_NAME AS VARCHAR2(136)) AS attr_type_name,           NULL as attr_type_package,           a.length as length,           a.number_precision as PRECISION,           a.scale as scale,           cast('CHAR_CS' as varchar2(44)) as character_set_name,           a.attribute as attr_no,           cast(decode(a.number_precision, 1, 'C', 'B') AS varchar2(1)) as char_used         from sys.all_virtual_pkg_type_real_agent pt           join sys.all_virtual_package_real_agent p             on pt.package_id = p.package_id           join sys.all_virtual_pkg_type_attr_real_agent A             on pt.type_id = a.type_id               and bitand(a.properties, 15) != 3           join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d             on pt.database_id = d.database_id             and pt.database_id = USERENV('SCHEMAID')           join sys.all_virtual_type_real_agent t             on t.type_id = a.type_attr_id           join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1             on t.database_id = d1.database_id     UNION ALL     select       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       d1.database_name as attr_type_owner,       cast(pt1.type_name as varchar2(136)) as attr_type_name,       p1.package_name as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')       join sys.all_virtual_package_real_agent p1         on A.attr_package_id = p1.package_id       join sys.all_virtual_pkg_type_real_agent pt1         on A.attr_package_id = pt1.package_id         and pt1.type_id = a.type_attr_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on p1.database_id = d1.database_id     UNION ALL     select       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       'SYS' as attr_type_owner,       cast(t.TYPE_NAME AS VARCHAR2(136)) as attr_type_name,       NULL as attr_type_package,       NULL as length,       NULL as PRECISION,       NULL as scale,       NULL as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = A.type_id         and pt.database_id = USERENV('SCHEMAID')       join sys.all_virtual_type_sys_agent T         on t.type_id = a.type_attr_id     UNION ALL     select       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       'SYS' as attr_type_owner,       cast(pts.type_name as varchar2(136)) as attr_type_name,       ps.package_name as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = a.type_id       join sys.all_virtual_package_sys_agent ps         on a.attr_package_id = ps.package_id       join sys.all_virtual_pkg_type_sys_agent pts         on a.attr_package_id = pts.package_id         and pts.type_id = a.type_attr_id     UNION ALL     select       pt.type_name as type_name,       p.package_name as package_name,       a.name as attr_name,       cast(null as varchar2(7)) as attr_type_mod,       d1.database_name as attr_type_owner,       cast(case bitand(a.properties, 15)           when 9 then tbl.table_name || '%ROWTYPE'           else 'NOT SUPPORT' end as varchar2(136)) as attr_type_name,       NULL as attr_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       a.attribute as attr_no,       'B' as CHAR_USED     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_type_attr_real_agent A         on pt.type_id = A.type_id         and (bitand(a.properties, 15) = 9             or bitand(a.properties, 15) = 10)       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')       join sys.ALL_VIRTUAL_TABLE_REAL_AGENT tbl         on a.type_attr_id = tbl.table_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on tbl.database_id = d1.database_id )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_ob_res_mgr_sysstat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_RES_MGR_SYSSTAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_RES_MGR_SYSSTAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT CAST(TENANT_ID AS NUMBER) as CON_ID,            CAST(GROUP_ID AS NUMBER) as GROUP_ID,            SVR_IP as SVR_IP,            SVR_PORT as SVR_PORT,            CAST("STATISTIC#" AS NUMBER) as "STATISTIC#",            CAST(NAME AS VARCHAR2(64)) as NAME,            CAST(CLASS AS NUMBER) as CLASS,            CAST(VALUE AS NUMBER) as VALUE,            CAST(VALUE_TYPE AS VARCHAR2(16)) as VALUE_TYPE,            CAST(STAT_ID AS NUMBER) as STAT_ID     FROM SYS.ALL_VIRTUAL_RES_MGR_SYSSTAT )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_res_mgr_sysstat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_RES_MGR_SYSSTAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_RES_MGR_SYSSTAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT CON_ID,           GROUP_ID,           SVR_IP,           SVR_PORT,           "STATISTIC#",           NAME,           CLASS,           VALUE,           VALUE_TYPE,           STAT_ID     FROM SYS.GV$OB_RES_MGR_SYSSTAT     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_wr_sql_plan_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_WR_SQL_PLAN_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_WR_SQL_PLAN_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       SQLPLAN.TENANT_ID AS TENANT_ID,       SQLPLAN.CLUSTER_ID AS CLUSTER_ID,       SQLPLAN.SNAP_ID AS SNAP_ID,       SQLPLAN.SVR_IP AS SVR_IP,       SQLPLAN.SVR_PORT AS SVR_PORT,       SQLPLAN.SQL_ID AS SQL_ID,       SQLPLAN.PLAN_HASH AS PLAN_HASH,       SQLPLAN.PLAN_ID AS PLAN_ID,       SQLPLAN.ID AS ID,       SQLPLAN.DB_ID AS DB_ID,       SQLPLAN.GMT_CREATE AS GMT_CREATE,       SQLPLAN.OPERATOR AS OPERATOR,       SQLPLAN.OPTIONS AS OPTIONS,       SQLPLAN.OBJECT_NODE AS OBJECT_NODE,       SQLPLAN.OBJECT_ID AS OBJECT_ID,       SQLPLAN.OBJECT_OWNER AS OBJECT_OWNER,       SQLPLAN.OBJECT_NAME AS OBJECT_NAME,       SQLPLAN.OBJECT_ALIAS AS OBJECT_ALIAS,       SQLPLAN.OBJECT_TYPE AS OBJECT_TYPE,       SQLPLAN.OPTIMIZER AS OPTIMIZER,       SQLPLAN.PARENT_ID AS PARENT_ID,       SQLPLAN.DEPTH AS DEPTH,       SQLPLAN.POSITION AS POSITION,       SQLPLAN.IS_LAST_CHILD AS IS_LAST_CHILD,       SQLPLAN.COST AS COST,       SQLPLAN.REAL_COST AS REAL_COST,       SQLPLAN.CARDINALITY AS CARDINALITY,       SQLPLAN.REAL_CARDINALITY AS REAL_CARDINALITY,       SQLPLAN.BYTES AS BYTES,       SQLPLAN.ROWSET AS ROWSET,       SQLPLAN.OTHER_TAG AS OTHER_TAG,       SQLPLAN.PARTITION_START AS PARTITION_START,       SQLPLAN.other AS OTHER,       SQLPLAN.CPU_COST AS CPU_COST,       SQLPLAN.IO_COST AS IO_COST,       SQLPLAN.ACCESS_PREDICATES AS ACCESS_PREDICATES,       SQLPLAN.FILTER_PREDICATES AS FILTER_PREDICATES,       SQLPLAN.STARTUP_PREDICATES AS STARTUP_PREDICATES,       SQLPLAN.PROJECTION AS PROJECTION,       SQLPLAN.SPECIAL_PREDICATES AS SPECIAL_PREDICATES,       SQLPLAN.QBLOCK_NAME AS QBLOCK_NAME,       SQLPLAN.REMARKS AS REMARKS,       SQLPLAN.OTHER_XML AS OTHER_XML   FROM     SYS.ALL_VIRTUAL_WR_SQL_PLAN SQLPLAN   WHERE     SQLPLAN.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_wr_res_mgr_sysstat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_WR_RES_MGR_SYSSTAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_WR_RES_MGR_SYSSTAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     STAT.CLUSTER_ID AS CLUSTER_ID,     STAT.TENANT_ID AS TENANT_ID,     STAT.GROUP_ID AS GROUP_ID,     STAT.SNAP_ID AS SNAP_ID,     STAT.SVR_IP AS SVR_IP,     STAT.SVR_PORT AS SVR_PORT,     STAT.STAT_ID AS STAT_ID,     STAT.VALUE AS VALUE   FROM     SYS.ALL_VIRTUAL_WR_RES_MGR_SYSSTAT STAT,     SYS.ALL_VIRTUAL_WR_SNAPSHOT SNAP   WHERE     STAT.TENANT_ID = EFFECTIVE_TENANT_ID()     AND STAT.CLUSTER_ID = SNAP.CLUSTER_ID     AND STAT.TENANT_ID = SNAP.TENANT_ID     AND STAT.SNAP_ID = SNAP.SNAP_ID     AND SNAP.STATUS = 0;   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_spm_evo_result_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SPM_EVO_RESULT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SPM_EVO_RESULT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT D.DATABASE_NAME AS OWNER,          RECORD_TIME,          SVR_IP,          SVR_PORT,          SQL_ID,          CAST(CASE WHEN TYPE = 0 THEN 'OnlineEvolve'                    WHEN TYPE = 1 THEN 'FirstBaseline'                    WHEN TYPE = 2 THEN 'UnReproducible'                    WHEN TYPE = 3 THEN 'BaselineFirst'                    WHEN TYPE = 4 THEN 'BestBaseline'                    WHEN TYPE = 5 THEN 'FixedBaseline'                    ELSE NULL END AS VARCHAR(32)) AS TYPE,          START_TIME,          END_TIME,          STATUS,          NEW_PLAN_BETTER,          EVO_PLAN_EXEC_COUNT,          EVO_PLAN_CPU_TIME,          BASELINE_EXEC_COUNT,          BASELINE_CPU_TIME,          EVO_PLAN_HASH,          BASELINE_PLAN_HASH   FROM SYS.ALL_VIRTUAL_SPM_EVO_RESULT R,        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D   WHERE R.TENANT_ID = EFFECTIVE_TENANT_ID()     AND D.TENANT_ID = EFFECTIVE_TENANT_ID()     AND R.DATABASE_ID = D.DATABASE_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_ob_function_io_stat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_FUNCTION_IO_STAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_FUNCTION_IO_STAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     A.SVR_IP AS SVR_IP,     A.SVR_PORT AS SVR_PORT,     A.TENANT_ID AS TENANT_ID,     A.FUNCTION_NAME AS FUNCTION_NAME,     A."MODE" AS "MODE",     A."SIZE" AS "SIZE",     A.REAL_IOPS AS REAL_IOPS,     A.REAL_MBPS AS REAL_MBPS,     A.SCHEDULE_US AS SCHEDULE_US,     A.IO_DELAY_US AS IO_DELAY_US,     A.TOTAL_US AS TOTAL_US   FROM     SYS.ALL_VIRTUAL_FUNCTION_IO_STAT A )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_function_io_stat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_FUNCTION_IO_STAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_FUNCTION_IO_STAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     A.SVR_IP AS SVR_IP,     A.SVR_PORT AS SVR_PORT,     A.TENANT_ID AS TENANT_ID,     A.FUNCTION_NAME AS FUNCTION_NAME,     A."MODE" AS "MODE",     A."SIZE" AS "SIZE",     A.REAL_IOPS AS REAL_IOPS,     A.REAL_MBPS AS REAL_MBPS,     A.SCHEDULE_US AS SCHEDULE_US,     A.IO_DELAY_US AS IO_DELAY_US,     A.TOTAL_US AS TOTAL_US   FROM     SYS.GV$OB_FUNCTION_IO_STAT A   WHERE     SVR_IP=HOST_IP()     AND     SVR_PORT=RPC_PORT() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_temp_files_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TEMP_FILES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TEMP_FILES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     SVR_IP,     SVR_PORT,     FILE_ID,     TRACE_ID,     DIR_ID,     DATA_BYTES,     START_OFFSET,     TOTAL_WRITES,     UNALIGNED_WRITES,     TOTAL_READS,     UNALIGNED_READS,     TOTAL_READ_BYTES,     LAST_ACCESS_TIME,     LAST_MODIFY_TIME,     BIRTH_TIME   FROM SYS.ALL_VIRTUAL_TEMP_FILE   WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_cs_replica_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_CS_REPLICA_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_CS_REPLICA_STATS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SVR_IP,     SVR_PORT,     LS_ID,     COUNT(*) AS TOTAL_TABLET_CNT,     SUM(CASE WHEN available = 1 THEN 1 ELSE 0 END) AS AVAILABLE_TABLET_CNT,     SUM(macro_block_cnt) AS TOTAL_MACRO_BLOCK_CNT,     SUM(CASE WHEN available = 1 THEN macro_block_cnt ELSE 0 END) AS AVAILABLE_MACRO_BLOCK_CNT,     CASE       WHEN SUM(CASE WHEN available = 0 THEN 1 ELSE 0 END) > 0 THEN 'FALSE'       ELSE 'TRUE'     END AS AVAILABLE   FROM SYS.ALL_VIRTUAL_CS_REPLICA_TABLET_STATS   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   GROUP BY SVR_IP, SVR_PORT, LS_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_ob_sql_ccl_status_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SQL_CCL_STATUS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SQL_CCL_STATUS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(           SELECT           TENANT_ID as CON_ID,           SVR_IP,           SVR_PORT,           CCL_RULE_ID,           FORMAT_SQLID,           CURRENT_CONCURRENCY,           MAX_CONCURRENCY         FROM SYS.ALL_VIRTUAL_CCL_STATUS )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_sql_ccl_status_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SQL_CCL_STATUS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SQL_CCL_STATUS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(           SELECT           TENANT_ID as CON_ID,           SVR_IP,           SVR_PORT,           CCL_RULE_ID,           FORMAT_SQLID,           CURRENT_CONCURRENCY,           MAX_CONCURRENCY         FROM SYS.ALL_VIRTUAL_CCL_STATUS         WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_dynamic_partition_tables_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_DYNAMIC_PARTITION_TABLES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_DYNAMIC_PARTITION_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     TENANT_ID,     TENANT_SCHEMA_VERSION,     DATABASE_NAME,     TABLE_NAME,     TABLE_ID,     MAX_HIGH_BOUND_VAL,     ENABLE,     TIME_UNIT,     PRECREATE_TIME,     EXPIRE_TIME,     TIME_ZONE,     BIGINT_PRECISION   FROM SYS.ALL_VIRTUAL_DYNAMIC_PARTITION_TABLE; )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_ob_result_cache_objects_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_RESULT_CACHE_OBJECTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_RESULT_CACHE_OBJECTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT TENANT_ID AS TENANT_ID,            SVR_IP AS SVR_IP,            SVR_PORT AS SVR_PORT,            PLAN_ID AS CACHE_OBJECT_ID,            STATEMENT AS UDF_NAME,            FIRST_LOAD_TIME AS FIRST_LOAD_TIME,            LAST_ACTIVE_TIME AS LAST_ACTIVE_TIME,            HIT_COUNT AS HIT_COUNT,            PLAN_SIZE AS CACHE_OBJ_SIZE,            ELAPSED_TIME AS BUILD_TIME,            OBJECT_TYPE AS OBJECT_TYPE,            PL_SCHEMA_ID AS OBJECT_ID,            DB_ID AS DB_ID,            PLAN_HASH AS HASH,            SYS_VARS AS SYS_VARS,            CONFIGS AS CONFIGS     FROM SYS.ALL_VIRTUAL_PLAN_STAT WHERE OBJECT_STATUS = 0 AND TYPE = 11 AND is_in_pc='1' )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_result_cache_objects_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_RESULT_CACHE_OBJECTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_RESULT_CACHE_OBJECTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT TENANT_ID AS TENANT_ID,            SVR_IP AS SVR_IP,            SVR_PORT AS SVR_PORT,            CACHE_OBJECT_ID AS CACHE_OBJECT_ID,            UDF_NAME AS UDF_NAME,            FIRST_LOAD_TIME AS FIRST_LOAD_TIME,            LAST_ACTIVE_TIME AS LAST_ACTIVE_TIME,            HIT_COUNT AS HIT_COUNT,            CACHE_OBJ_SIZE AS CACHE_OBJ_SIZE,            BUILD_TIME AS BUILD_TIME,            OBJECT_TYPE AS OBJECT_TYPE,            OBJECT_ID AS OBJECT_ID,            DB_ID AS DB_ID,            HASH AS HASH,            SYS_VARS AS SYS_VARS,            CONFIGS AS CONFIGS     FROM SYS.GV$OB_RESULT_CACHE_OBJECTS WHERE SVR_IP =HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_locations_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_LOCATIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_LOCATIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST('SYS' AS VARCHAR2(128)) AS OWNER,       t.LOCATION_NAME AS LOCATION_NAME,       t.LOCATION_URL AS LOCATION_URL,       t.TENANT_ID AS ORIGIN_CON_ID,       t.LOCATION_ACCESS_INFO AS ACCESS_INFO     FROM       SYS.ALL_VIRTUAL_TENANT_LOCATION_REAL_AGENT t       WHERE t.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_ss_sstables_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SS_SSTABLES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SS_SSTABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT  M.TENANT_ID,  M.LS_ID,  M.TABLET_ID,  M.TRANSFER_SCN,  (case M.TABLE_TYPE     when 10 then 'MAJOR' when 11 then 'MINOR'     when 12 then 'MINI' when 13 then 'META'     when 14 then 'DDL_DUMP'     when 17 then 'CO_MAJOR' when 18 then 'NORMAL_CG' when 19 then 'ROWKEY_CG' when 20 then 'COL_ORIENTED_META'     when 21 then 'DDL_MERGE_CO' when 22 then 'DDL_MERGE_CG' when 23 then 'DDL_MEM_CO'     when 24 then 'DDL_MEM_CG' when 25 then 'DDL_MEM_MINI_SSTABLE'     when 26 then 'MDS_MINI' when 27 then 'MDS_MINOR'     when 29 then 'INC_MAJOR' when 30 then 'INC_CO_MAJOR' when 31 then 'INC_NORMAL_CG' when 32 then 'INC_ROWKEY_CG'     when 33 then 'INC_DDL_DUMP' when 34 then 'INC_DDL_MERGE_CO' when 35 then 'INC_DDL_MERGE_CG'     when 36 then 'INC_DDL_MEM_CO' when 37 then 'INC_DDL_MEM_CG' when 38 then 'INC_DDL_MEM'     else 'INVALID'   end) as TABLE_TYPE,  M.CG_IDX,  M.START_LOG_SCN,  M.END_LOG_SCN,  M.DATA_CHECKSUM,  M."SIZE",  M.REC_SCN,  M.UPPER_TRANS_VERSION,  M.CONTAIN_UNCOMMITTED_ROW FROM  SYS.ALL_VIRTUAL_SS_SSTABLE_MGR M )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_pl_obj_cache_status_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_PL_OBJ_CACHE_STATUS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_PL_OBJ_CACHE_STATUS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     OBJECT_NAME,     OBJECT_ID,     OBJECT_TYPE,     STATUS,     COMPILE_DB_ID,     CASE       WHEN IS_INVOKER_RIGHT = 'TRUE' THEN         CASE           WHEN PL_CACHE_STATUS = 'VALID PL CACHE OBJ' THEN 'VALID DISK CACHE OBJ'           ELSE 'UNKNOWN'         END       ELSE         CASE           WHEN COMPILE_DB_ID IS NULL THEN 'NOT IN DISK CACHE'           ELSE DBMS_UTILITY.CHECK_DISK_CACHE_OBJ_EXPIRED(OBJECT_ID, MERGE_VERSION, EXTRA_INFO)         END     END AS DISK_CACHE_STATUS,     PL_CACHE_STATUS   FROM   (   SELECT     ALLT.OBJECT_NAME AS OBJECT_NAME,     ALLT.OBJECT_ID AS OBJECT_ID,     ALLT.OBJECT_TYPE AS OBJECT_TYPE,     ALLT.STATUS AS STATUS,     ALLT.COMPILE_DB_ID AS COMPILE_DB_ID,     ALLT.IS_INVOKER_RIGHT AS IS_INVOKER_RIGHT,     ALLT.MERGE_VERSION AS MERGE_VERSION,     ALLT.EXTRA_INFO AS EXTRA_INFO,     DECODE(ALLT.COMPILE_DB_ID,             NULL, DBMS_UTILITY.CHECK_PL_CACHE_OBJ_EXPIRED(ALLT.OBJECT_ID, ALLT.OBJECT_TYPE,               ALLT.SCHEMA_DB_ID, 'X86_0'),             DBMS_UTILITY.CHECK_PL_CACHE_OBJ_EXPIRED(ALLT.OBJECT_ID, ALLT.OBJECT_TYPE,               ALLT.COMPILE_DB_ID, ALLT.ARCH_TYPE)) AS PL_CACHE_STATUS     FROM     (       SELECT             PACKAGE_NAME AS OBJECT_NAME,             PACKAGE_ID AS OBJECT_ID,             'PACKAGE BODY' AS OBJECT_TYPE,             'VALID' AS STATUS,             PS.DATABASE_ID AS SCHEMA_DB_ID,             D.COMPILE_DB_ID AS COMPILE_DB_ID,             D.MERGE_VERSION AS MERGE_VERSION,             CASE WHEN BITAND(PS.FLAG, 4) = 4               THEN 'TRUE' ELSE 'FALSE' END AS IS_INVOKER_RIGHT,             D.ARCH_TYPE AS ARCH_TYPE,             D.EXTRA_INFO AS EXTRA_INFO       FROM SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS       LEFT OUTER JOIN SYS.ALL_VIRTUAL_NCOMP_DLL_V2_REAL_AGENT D       ON D.KEY_ID = PS.PACKAGE_ID       WHERE PS.TYPE = 2        UNION ALL        SELECT             P.PACKAGE_NAME AS OBJECT_NAME,             P.PACKAGE_ID OBJECT_ID,             'PACKAGE BODY' AS OBJECT_TYPE,             CASE WHEN EXISTS                         (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E                         WHERE P.TENANT_ID = E.TENANT_ID AND P.PACKAGE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 3 OR E.OBJ_TYPE = 5))                       THEN 'INVALID'                 WHEN TYPE = 2 AND EXISTS                         (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT EB                         WHERE OBJ_ID IN                                   (SELECT PACKAGE_ID FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT PB                                   WHERE PB.PACKAGE_NAME = P.PACKAGE_NAME AND PB.DATABASE_ID = P.DATABASE_ID AND PB.TENANT_ID = P.TENANT_ID AND TYPE = 1)                         AND EB.OBJ_TYPE = 3)                     THEN 'INVALID'                 ELSE 'VALID' END AS STATUS,             P.DATABASE_ID AS SCHEMA_DB_ID,             D.COMPILE_DB_ID AS COMPILE_DB_ID,             D.MERGE_VERSION AS MERGE_VERSION,             CASE WHEN BITAND(P.FLAG, 4) = 4               THEN 'TRUE' ELSE 'FALSE' END AS IS_INVOKER_RIGHT,             D.ARCH_TYPE AS ARCH_TYPE,             D.EXTRA_INFO AS EXTRA_INFO       FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P       LEFT OUTER JOIN SYS.ALL_VIRTUAL_NCOMP_DLL_V2_REAL_AGENT D       ON D.KEY_ID = P.PACKAGE_ID       WHERE P.TENANT_ID = EFFECTIVE_TENANT_ID()       AND P.TYPE = 2        UNION ALL        SELECT             R.ROUTINE_NAME AS OBJECT_NAME,             R.ROUTINE_ID OBJECT_ID,             CASE WHEN ROUTINE_TYPE = 1 THEN 'PROCEDURE'                 WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'                 ELSE NULL END AS OBJECT_TYPE,             CASE WHEN EXISTS                         (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E                         WHERE R.TENANT_ID = E.TENANT_ID AND R.ROUTINE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 9 OR E.OBJ_TYPE = 12))                       THEN 'INVALID'                 ELSE 'VALID' END AS STATUS,             R.DATABASE_ID AS SCHEMA_DB_ID,             D.COMPILE_DB_ID AS COMPILE_DB_ID,             D.MERGE_VERSION AS MERGE_VERSION,             CASE WHEN BITAND(R.FLAG, 16) = 16               THEN 'TRUE' ELSE 'FALSE' END AS IS_INVOKER_RIGHT,             D.ARCH_TYPE AS ARCH_TYPE,             D.EXTRA_INFO AS EXTRA_INFO       FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R       LEFT OUTER JOIN SYS.ALL_VIRTUAL_NCOMP_DLL_V2_REAL_AGENT D       ON D.KEY_ID = R.ROUTINE_ID       WHERE (ROUTINE_TYPE = 1 OR ROUTINE_TYPE = 2) AND R.TENANT_ID = EFFECTIVE_TENANT_ID()        UNION ALL        SELECT             OBJECT_NAME,             OBJECT_TYPE_ID AS OBJECT_ID,             'TYPE BODY' AS OBJECT_TYPE,             CASE WHEN EXISTS                         (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E                         WHERE TY.TENANT_ID = E.TENANT_ID AND TY.OBJECT_TYPE_ID = E.OBJ_ID AND E.OBJ_TYPE = 6)                       THEN 'INVALID'                 ELSE 'VALID' END AS STATUS,             TY.DATABASE_ID AS SCHEMA_DB_ID,             D.COMPILE_DB_ID AS COMPILE_DB_ID,             D.MERGE_VERSION AS MERGE_VERSION,             CASE WHEN BITAND(TY.FLAG, 4) = 4               THEN 'TRUE' ELSE 'FALSE' END AS IS_INVOKER_RIGHT,             D.ARCH_TYPE AS ARCH_TYPE,             D.EXTRA_INFO AS EXTRA_INFO       FROM SYS.ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT TY       LEFT OUTER JOIN SYS.ALL_VIRTUAL_NCOMP_DLL_V2_REAL_AGENT D       ON BITAND(D.KEY_ID, -2305843009213693953) = TY.COLL_TYPE       WHERE TY.TENANT_ID = EFFECTIVE_TENANT_ID() and TY.TYPE = 2        UNION ALL        SELECT           T.TRIGGER_NAME AS OBJECT_NAME,           T.TRIGGER_ID AS OBJECT_ID,           'TRIGGER' AS OBJECT_TYPE,           CASE WHEN EXISTS                       (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E                       WHERE T.TENANT_ID = E.TENANT_ID AND T.TRIGGER_ID = E.OBJ_ID AND (E.OBJ_TYPE = 7))                     THEN 'INVALID'                 ELSE 'VALID' END AS STATUS,           T.DATABASE_ID AS SCHEMA_DB_ID,           D.COMPILE_DB_ID AS COMPILE_DB_ID,           D.MERGE_VERSION AS MERGE_VERSION,           CASE WHEN BITAND(T.package_flag, 4) = 4             THEN 'TRUE' ELSE 'FALSE' END AS IS_INVOKER_RIGHT,             D.ARCH_TYPE AS ARCH_TYPE,             D.EXTRA_INFO AS EXTRA_INFO       FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT T       LEFT OUTER JOIN SYS.ALL_VIRTUAL_NCOMP_DLL_V2_REAL_AGENT D       ON BITAND(BITAND(D.KEY_ID, -4611686018427387905), 9223372036854775807) = T.TRIGGER_ID       WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TRIGGER_NAME NOT LIKE 'RECYCLE_%'         AND D.KEY_ID < 0     ) ALLT   ) )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_ob_hms_client_pool_stat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_HMS_CLIENT_POOL_STAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_HMS_CLIENT_POOL_STAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT       SVR_IP,       SVR_PORT,       TENANT_ID,       CATALOG_ID,       TOTAL_CLIENTS,       IN_USE_CLIENTS,       IDLE_CLIENTS       FROM SYS.ALL_VIRTUAL_HMS_CLIENT_POOL_STAT )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_hms_client_pool_stat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_HMS_CLIENT_POOL_STAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_HMS_CLIENT_POOL_STAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(       SELECT SVR_IP,       SVR_PORT,       TENANT_ID,       CATALOG_ID,       TOTAL_CLIENTS,       IN_USE_CLIENTS,       IDLE_CLIENTS       FROM SYS.GV$OB_HMS_CLIENT_POOL_STAT WHERE       SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_lob_check_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LOB_CHECK_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LOB_CHECK_TASKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CASE A.ls_id         WHEN -1 THEN 'TENANT'         ELSE 'LS' END AS TASK_SCOPE,       A.ls_id AS LS_ID,       CASE A.table_id         WHEN 3 THEN '__all_table'         ELSE B.table_name END AS TABLE_NAME,       A.table_id AS TABLE_ID,       A.tablet_id AS TABLET_ID,       A.task_id AS TASK_ID,       TO_CHAR(A.task_start_time / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS START_TIME,       TO_CHAR(A.task_update_time / (1000 * 60 * 60 * 24 * 1000) + TO_DATE('1970-01-01 08:00:00', 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') AS END_TIME,       CASE A.trigger_type         WHEN 0 THEN 'PERIODIC'         WHEN 1 THEN 'USER'         ELSE 'INVALID' END AS TRIGGER_TYPE,       CASE A.status         when 0 then 'PREPARED'         when 1 then 'RUNNING'         when 2 then 'PENDING'         when 3 then 'CANCELED'         when 4 then 'FINISHED'         when 15 then 'TRIGGERING'         when 16 then 'SUSPENDING'         when 17 then 'CANCELING'         when 18 then 'CLEANING'         else 'INVALID' END AS STATUS,       CASE A.task_type         WHEN 2 THEN A.ttl_del_cnt         ELSE 0 END AS MISS_CNT,       CASE A.task_type         WHEN 2 THEN A.max_version_del_cnt         ELSE 0 END AS MISMATCH_LEN_CNT,       CASE A.task_type         WHEN 2 THEN A.scan_cnt         ELSE 0 END AS ORPHAN_CNT,       CASE A.task_type         WHEN 3 THEN A.scan_cnt         ELSE 0 END AS CORRECT_CNT,       substr(A.ret_code, 1, instr(A.ret_code, '|') - 1) AS RET_CODE,       CASE A.task_type         WHEN 2 THEN 'LOB_CHECK'         WHEN 3 THEN 'LOB_REPAIR'         ELSE 'INVALID' END AS TASK_TYPE,       A.scan_index AS SCAN_INDEX       FROM SYS.ALL_VIRTUAL_KV_TTL_TASK A LEFT OUTER JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B ON           A.table_id = B.table_id AND A.tenant_id = B.tenant_id       WHERE A.task_type IN (2, 3) )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_lob_check_exception_result_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LOB_CHECK_EXCEPTION_RESULT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LOB_CHECK_EXCEPTION_RESULT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       A.tenant_id AS TENANT_ID,       CASE A.ls_id         WHEN -1 THEN 'TENANT'         ELSE 'LS' END AS TASK_SCOPE,       A.ls_id AS LS_ID,       A.gmt_create AS CREATE_TIME,       A.gmt_modified AS UPDATE_TIME,       CASE A.table_id         WHEN 3 THEN '__all_table'         ELSE B.table_name END AS TABLE_NAME,       A.table_id AS TABLE_ID,       CASE A.inconsistency_type         WHEN 0 THEN 'LOB_NOT_FOUND'         WHEN 1 THEN 'LOB_LEN_MISMATCH'         WHEN 2 THEN 'LOB_ORPHANED'         ELSE 'INVALID' END AS INCONSISTENCY_TYPE,       A.tablet_ids AS TABLET_IDS       FROM SYS.ALL_VIRTUAL_LOB_CHECK_EXCEPTION_RESULT A LEFT OUTER JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B ON           A.table_id = B.table_id AND A.tenant_id = B.tenant_id )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_sensitive_rules_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SENSITIVE_RULES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SENSITIVE_RULES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SR.SENSITIVE_RULE_NAME AS RULE_NAME,     DECODE(SR.PROTECTION_POLICY, 1, 'NONE', 2, 'ENCRYPTION', 3, 'MASKING', NULL) AS PROTECTION_POLICY,     SR.METHOD AS METHOD,     DECODE(SR.ENABLED, 1, 'YES', 'NO') AS ENABLED   FROM SYS.ALL_VIRTUAL_SENSITIVE_RULE_REAL_AGENT SR   ORDER BY SR.SENSITIVE_RULE_ID; )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_sensitive_columns_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SENSITIVE_COLUMNS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SENSITIVE_COLUMNS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SR.SENSITIVE_RULE_NAME AS RULE_NAME,     D.DATABASE_NAME AS DATABASE_NAME,     T.TABLE_NAME AS TABLE_NAME,     C.COLUMN_NAME AS COLUMN_NAME   FROM     SYS.ALL_VIRTUAL_SENSITIVE_COLUMN_REAL_AGENT SC     JOIN SYS.ALL_VIRTUAL_SENSITIVE_RULE_REAL_AGENT SR       ON  SC.TENANT_ID = SR.TENANT_ID       AND SC.SENSITIVE_RULE_ID = SR.SENSITIVE_RULE_ID     JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C       ON  SC.TENANT_ID = C.TENANT_ID       AND SC.TABLE_ID  = C.TABLE_ID       AND SC.COLUMN_ID = C.COLUMN_ID     JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T       ON  SC.TENANT_ID = T.TENANT_ID       AND C.TABLE_ID = T.TABLE_ID     JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D       ON  SC.TENANT_ID = D.TENANT_ID       AND T.DATABASE_ID = D.DATABASE_ID   WHERE D.IN_RECYCLEBIN = 0     AND D.DATABASE_NAME != '__RECYCLEBIN'     AND BITAND((T.TABLE_MODE / 4096), 15) IN (0,1)   ORDER BY SR.SENSITIVE_RULE_ID, D.DATABASE_ID, T.TABLE_ID, C.COLUMN_ID   ; )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_sensitive_rule_plainaccess_users_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SENSITIVE_RULE_PLAINACCESS_USERS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SENSITIVE_RULE_PLAINACCESS_USERS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     TP.TABLE_NAME AS RULE_NAME,     TP.GRANTEE AS USER_NAME,     DECODE(U.TYPE, 0, 'USER', 1, 'ROLE', NULL) AS USER_TYPE   FROM SYS.DBA_TAB_PRIVS TP   JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U     ON TP.GRANTEE = U.USER_NAME   WHERE TP.PRIVILEGE = 'PLAINACCESS'   ORDER BY TP.TABLE_NAME, TP.GRANTEE; )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_ttl_tasks_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TTL_TASKS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TTL_TASKS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       b.table_name as TABLE_NAME,       a.table_id as TABLE_ID,       a.task_id as TASK_ID,       a.task_start_time as START_TIME,       a.task_update_time as MODIFIED_TIME,       (case a.trigger_type         when 0 then 'PERIODIC'         when 1 then 'USER'         else 'INVALID' END) AS TRIGGER_TYPE,       (case a.status         when 2 then 'PENDING'         when 3 then 'CANCELED'         when 4 then 'FINISHED'         when 5 then 'MOVED'         when 6 then 'SKIP'         when 7 then 'FAILED'         when 15 then 'TRIGGERING'         when 16 then 'SUSPENDING'         when 17 then 'CANCELING'         when 18 then 'MOVING'         else 'INVALID' END) AS STATUS,       a.ret_code as RET_CODE,       (case a.task_type         when 4 then 'COMPACTION'         else 'INVALID' END) AS TASK_TYPE       FROM SYS.ALL_VIRTUAL_KV_TTL_TASK a left outer JOIN SYS.ALL_VIRTUAL_CORE_ALL_TABLE b on           a.table_id = b.table_id and a.tenant_id = b.tenant_id and a.tenant_id = effective_tenant_id()           where a.task_type=4 and a.tenant_id = effective_tenant_id() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_ttl_task_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TTL_TASK_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TTL_TASK_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       b.table_name as TABLE_NAME,       a.table_id as TABLE_ID,       a.task_id as TASK_ID,       a.task_start_time as START_TIME,       a.task_update_time as MODIFIED_TIME,       (case a.trigger_type         when 0 then 'PERIODIC'         when 1 then 'USER'         else 'INVALID' END) AS TRIGGER_TYPE,       (case a.status         when 3 then 'CANCELED'         when 4 then 'FINISHED'         when 6 then 'SKIP'         else 'INVALID' END) AS STATUS,       a.ret_code as RET_CODE,       (case a.task_type         when 4 then 'COMPACTION'         else 'INVALID' END) AS TASK_TYPE       FROM SYS.ALL_VIRTUAL_KV_TTL_TASK_HISTORY a left outer JOIN SYS.ALL_VIRTUAL_CORE_ALL_TABLE b on           a.table_id = b.table_id and a.tenant_id = b.tenant_id and a.tenant_id = effective_tenant_id()           where a.task_type=4 and a.tenant_id = effective_tenant_id() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_routine_load_jobs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ROUTINE_LOAD_JOBS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ROUTINE_LOAD_JOBS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT A.job_id AS JOB_ID,        A.job_name AS JOB_NAME,        A.create_time AS CREATE_TIME,        A.pause_time AS PAUSE_TIME,        A.end_time AS END_TIME,        B.database_name AS DATABASE_NAME,        A.database_id AS DATABASE_ID,        C.table_name AS TABLE_NAME,        A.table_id AS TABLE_ID,        A.state AS STATE,        A.job_properties AS JOB_PROPERTIES,        A.progress AS PROGRESS,        A.lag AS LAG,        A.tmp_progress AS TMP_PROGRESS,        A.tmp_lag AS TMP_LAG,        A.last_trace_id AS LAST_TRACE_ID,        A.last_ret_code AS LAST_RET_CODE,        A.last_error_msg AS LAST_ERROR_MSG FROM SYS.ALL_VIRTUAL_ROUTINE_LOAD_JOB_REAL_AGENT A LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.database_id = B.database_id LEFT JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C ON A.table_id = C.table_id ORDER BY A.create_time )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_ob_tenant_worker_groups_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TENANT_WORKER_GROUPS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TENANT_WORKER_GROUPS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT     SVR_IP,     SVR_PORT,     TENANT_ID,     GROUP_ID,     WORKERS_SIZE,     REQ_QUEUE_SIZE,     RECV_REQ_CNT,     DELETED,     TOKEN_CHANGE_TS,     THROTTLED_TIME_US,     IDLE_CNT,     LAST_NOT_EMPTY_TS FROM     SYS.ALL_VIRTUAL_TENANT_WORKER_GROUP )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);
  table_schema.set_micro_index_clustered(false);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}


} // end namespace share
} // end namespace oceanbase
