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

int ObInnerTableSchema::dba_coll_types_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_COLL_TYPES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_COLL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT /*+ USE_MERGE(T, C, D, T1, D1) */       D.DATABASE_NAME AS OWNER,       T.TYPE_NAME AS TYPE_NAME,       CAST(         CASE C.UPPER_BOUND         WHEN 0 THEN 'COLLECTION'         ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,       C.UPPER_BOUND AS UPPER_BOUND,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,       CAST(         CASE C.PROPERTIES         WHEN 3 THEN NULL         ELSE D1.DATABASE_NAME END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,       CAST(         CASE C.PROPERTIES         WHEN 3           THEN DECODE (BITAND(C.ELEM_TYPE_ID, 1099511627775),             0,  'NULL',             1,  'NUMBER',             2,  'NUMBER',             3,  'NUMBER',             4,  'NUMBER',             5,  'NUMBER',             6,  'NUMBER',             7,  'NUMBER',             8,  'NUMBER',             9,  'NUMBER',             10, 'NUMBER',             11, 'BINARY_FLOAT',             12, 'BINARY_DOUBLE',             13, 'NUMBER',             14, 'NUMBER',             15, 'NUMBER',             16, 'NUMBER',             17, 'DATE',             18, 'TIMESTAMP',             19, 'DATE',             20, 'TIME',             21, 'YEAR',             22, 'VARCHAR2',             23, 'CHAR',             24, 'HEX_STRING',             25, 'EXT',             26, 'UNKNOWN',             27, 'TINYTEXT',             28, 'TEXT',             29, 'MEDIUMTEXT',             30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),             31, 'BIT',             32, 'ENUM',             33, 'SET',             34, 'ENUM_INNER',             35, 'SET_INNER',             36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),             37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),             38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),             39, 'RAW',             40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),             41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),             42, 'FLOAT',             43, 'NVARCHAR2',             44, 'NCHAR',             45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),             46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),             'NOT_SUPPORT')         ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,       C.LENGTH AS LENGTH,       C.NUMBER_PRECISION AS NUMBER_PRECISION,       C.SCALE AS SCALE,       CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,       CAST('YES' AS CHAR(3)) AS ELEM_STORAGE,       CAST('B' AS CHAR(1)) AS NULLS_STORED     FROM       SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_COLL_TYPE_REAL_AGENT C         ON T.TYPE_ID = C.COLL_TYPE_ID         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D         ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')           AND T.DATABASE_ID = D.DATABASE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1         ON T1.TYPE_ID = C.ELEM_TYPE_ID         AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1         ON T1.DATABASE_ID = D1.DATABASE_ID         AND D1.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION ALL     SELECT /*+ USE_MERGE(TS, CS, TS1) */       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       TS.TYPE_NAME AS TYPE_NAME,       CAST(         CASE CS.UPPER_BOUND         WHEN 0 THEN 'COLLECTION'         ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,       CS.UPPER_BOUND AS UPPER_BOUND,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,       CAST(         CASE CS.PROPERTIES         WHEN 3 THEN NULL         ELSE 'SYS' END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,       CAST(         CASE CS.PROPERTIES         WHEN 3           THEN DECODE (BITAND(CS.ELEM_TYPE_ID, 1099511627775),             0,  'NULL',             1,  'NUMBER',             2,  'NUMBER',             3,  'NUMBER',             4,  'NUMBER',             5,  'NUMBER',             6,  'NUMBER',             7,  'NUMBER',             8,  'NUMBER',             9,  'NUMBER',             10, 'NUMBER',             11, 'BINARY_FLOAT',             12, 'BINARY_DOUBLE',             13, 'NUMBER',             14, 'NUMBER',             15, 'NUMBER',             16, 'NUMBER',             17, 'DATE',             18, 'TIMESTAMP',             19, 'DATE',             20, 'TIME',             21, 'YEAR',             22, 'VARCHAR2',             23, 'CHAR',             24, 'HEX_STRING',             25, 'EXT',             26, 'UNKNOWN',             27, 'TINYTEXT',             28, 'TEXT',             29, 'MEDIUMTEXT',             30,  DECODE(CS.COLL_TYPE, 63, 'BLOB', 'CLOB'),             31, 'BIT',             32, 'ENUM',             33, 'SET',             34, 'ENUM_INNER',             35, 'SET_INNER',             36, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ') WITH TIME ZONE')),             37, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ') WITH LOCAL TIME ZONE')),             38, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ')')),             39, 'RAW',             40, CONCAT('INTERVAL YEAR(', CONCAT(CS.SCALE, ') TO MONTH')),             41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(CS.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(CS.SCALE, 10), ')')))),             42, 'FLOAT',             43, 'NVARCHAR2',             44, 'NCHAR',             45, CONCAT('UROWID(', CONCAT(CS.LENGTH, ')')),             46, DECODE(CS.COLL_TYPE, 63, 'BLOB', 'CLOB'),             'NOT_SUPPORT')         ELSE TS1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,       CS.LENGTH AS LENGTH,       CS.NUMBER_PRECISION AS NUMBER_PRECISION,       CS.SCALE AS SCALE,       CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,       CAST('YES' AS CHAR(3)) AS ELEM_STORAGE,       CAST('B' AS CHAR(1)) AS NULLS_STORED     FROM       SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS JOIN SYS.ALL_VIRTUAL_COLL_TYPE_SYS_AGENT CS         ON TS.TYPE_ID = CS.COLL_TYPE_ID       LEFT JOIN SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS1         ON TS1.TYPE_ID = CS.ELEM_TYPE_ID )__"))) {
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

int ObInnerTableSchema::all_coll_types_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_COLL_TYPES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            table_schema.set_view_definition(R"__(     SELECT /*+ USE_MERGE(T, C, D, T1, D1) */       D.DATABASE_NAME AS OWNER,       T.TYPE_NAME AS TYPE_NAME,       CAST(         CASE C.UPPER_BOUND         WHEN 0 THEN 'COLLECTION'         ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,       C.UPPER_BOUND AS UPPER_BOUND,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,       CAST(         CASE C.PROPERTIES         WHEN 3 THEN NULL         ELSE D1.DATABASE_NAME END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,       CAST(         CASE C.PROPERTIES         WHEN 3           THEN DECODE (BITAND(C.ELEM_TYPE_ID, 1099511627775),             0,  'NULL',             1,  'NUMBER',             2,  'NUMBER',             3,  'NUMBER',             4,  'NUMBER',             5,  'NUMBER',             6,  'NUMBER',             7,  'NUMBER',             8,  'NUMBER',             9,  'NUMBER',             10, 'NUMBER',             11, 'BINARY_FLOAT',             12, 'BINARY_DOUBLE',             13, 'NUMBER',             14, 'NUMBER',             15, 'NUMBER',             16, 'NUMBER',             17, 'DATE',             18, 'TIMESTAMP',             19, 'DATE',             20, 'TIME',             21, 'YEAR',             22, 'VARCHAR2',             23, 'CHAR',             24, 'HEX_STRING',             25, 'EXT',             26, 'UNKNOWN',             27, 'TINYTEXT',             28, 'TEXT',             29, 'MEDIUMTEXT',             30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),             31, 'BIT',             32, 'ENUM',             33, 'SET',             34, 'ENUM_INNER',             35, 'SET_INNER',             36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),             37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),             38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),             39, 'RAW',             40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),             41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),             42, 'FLOAT',             43, 'NVARCHAR2',             44, 'NCHAR',             45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),             46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),             'NOT_SUPPORT')         ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,       C.LENGTH AS LENGTH,       C.NUMBER_PRECISION AS NUMBER_PRECISION,       C.SCALE AS SCALE,       CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,       CAST('YES' AS CHAR(3)) AS ELEM_STORAGE,       CAST('B' AS CHAR(1)) AS NULLS_STORED     FROM       SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_COLL_TYPE_REAL_AGENT C         ON T.TYPE_ID = C.COLL_TYPE_ID         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D         ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')           AND T.DATABASE_ID = D.DATABASE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (T.DATABASE_ID = USERENV('SCHEMAID')              or USER_CAN_ACCESS_OBJ(4, T.TYPE_ID, 1) = 1)       LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1         ON T1.TYPE_ID = C.ELEM_TYPE_ID         AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1         ON T1.DATABASE_ID = D1.DATABASE_ID         AND D1.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION ALL     SELECT /*+ USE_MERGE(TS, CS, TS1) */       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       TS.TYPE_NAME AS TYPE_NAME,       CAST(         CASE CS.UPPER_BOUND         WHEN 0 THEN 'COLLECTION'         ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,       CS.UPPER_BOUND AS UPPER_BOUND,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,       CAST(         CASE CS.PROPERTIES         WHEN 3 THEN NULL         ELSE 'SYS' END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,       CAST(         CASE CS.PROPERTIES         WHEN 3           THEN DECODE (BITAND(CS.ELEM_TYPE_ID, 1099511627775),             0,  'NULL',             1,  'NUMBER',             2,  'NUMBER',             3,  'NUMBER',             4,  'NUMBER',             5,  'NUMBER',             6,  'NUMBER',             7,  'NUMBER',             8,  'NUMBER',             9,  'NUMBER',             10, 'NUMBER',             11, 'BINARY_FLOAT',             12, 'BINARY_DOUBLE',             13, 'NUMBER',             14, 'NUMBER',             15, 'NUMBER',             16, 'NUMBER',             17, 'DATE',             18, 'TIMESTAMP',             19, 'DATE',             20, 'TIME',             21, 'YEAR',             22, 'VARCHAR2',             23, 'CHAR',             24, 'HEX_STRING',             25, 'EXT',             26, 'UNKNOWN',             27, 'TINYTEXT',             28, 'TEXT',             29, 'MEDIUMTEXT',             30,  DECODE(CS.COLL_TYPE, 63, 'BLOB', 'CLOB'),             31, 'BIT',             32, 'ENUM',             33, 'SET',             34, 'ENUM_INNER',             35, 'SET_INNER',             36, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ') WITH TIME ZONE')),             37, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ') WITH LOCAL TIME ZONE')),             38, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ')')),             39, 'RAW',             40, CONCAT('INTERVAL YEAR(', CONCAT(CS.SCALE, ') TO MONTH')),             41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(CS.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(CS.SCALE, 10), ')')))),             42, 'FLOAT',             43, 'NVARCHAR2',             44, 'NCHAR',             45, CONCAT('UROWID(', CONCAT(CS.LENGTH, ')')),             46, '',             'NOT_SUPPORT')         ELSE TS1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,       CS.LENGTH AS LENGTH,       CS.NUMBER_PRECISION AS NUMBER_PRECISION,       CS.SCALE AS SCALE,       CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,       CAST('YES' AS CHAR(3)) AS ELEM_STORAGE,       CAST('B' AS CHAR(1)) AS NULLS_STORED     FROM       SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS JOIN SYS.ALL_VIRTUAL_COLL_TYPE_SYS_AGENT CS         ON TS.TYPE_ID = CS.COLL_TYPE_ID       LEFT JOIN SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS1         ON TS1.TYPE_ID = CS.ELEM_TYPE_ID )__"))) {
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

int ObInnerTableSchema::user_coll_types_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_COLL_TYPES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_COLL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT /*+ USE_MERGE(T, C, D, T1, D1) */       T.TYPE_NAME AS TYPE_NAME,       CAST(         CASE C.UPPER_BOUND         WHEN 0 THEN 'COLLECTION'         ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,       C.UPPER_BOUND AS UPPER_BOUND,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,       CAST(         CASE C.PROPERTIES         WHEN 3 THEN NULL         ELSE d1.DATABASE_NAME END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,       CAST(         CASE C.PROPERTIES         WHEN 3           THEN DECODE (BITAND(C.ELEM_TYPE_ID, 1099511627775),             0,  'NULL',             1,  'NUMBER',             2,  'NUMBER',             3,  'NUMBER',             4,  'NUMBER',             5,  'NUMBER',             6,  'NUMBER',             7,  'NUMBER',             8,  'NUMBER',             9,  'NUMBER',             10, 'NUMBER',             11, 'BINARY_FLOAT',             12, 'BINARY_DOUBLE',             13, 'NUMBER',             14, 'NUMBER',             15, 'NUMBER',             16, 'NUMBER',             17, 'DATE',             18, 'TIMESTAMP',             19, 'DATE',             20, 'TIME',             21, 'YEAR',             22, 'VARCHAR2',             23, 'CHAR',             24, 'HEX_STRING',             25, 'EXT',             26, 'UNKNOWN',             27, 'TINYTEXT',             28, 'TEXT',             29, 'MEDIUMTEXT',             30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),             31, 'BIT',             32, 'ENUM',             33, 'SET',             34, 'ENUM_INNER',             35, 'SET_INNER',             36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),             37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),             38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),             39, 'RAW',             40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),             41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),             42, 'FLOAT',             43, 'NVARCHAR2',             44, 'NCHAR',             45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),             46, '',             'NOT_SUPPORT')         ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,       C.LENGTH AS LENGTH,       C.NUMBER_PRECISION AS NUMBER_PRECISION,       C.SCALE AS SCALE,       CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,       CAST('YES' AS CHAR(7)) AS ELEM_STORAGE,       CAST('B' AS CHAR(7)) AS NULLS_STORED     FROM       SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_COLL_TYPE_REAL_AGENT C         ON T.TYPE_ID = C.COLL_TYPE_ID         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D         ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')            AND T.DATABASE_ID = D.DATABASE_ID            AND D.TENANT_ID = EFFECTIVE_TENANT_ID()            AND D.DATABASE_ID = USERENV('SCHEMAID')       LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1         ON T1.TYPE_ID = C.ELEM_TYPE_ID         AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1         ON T1.DATABASE_ID = D1.DATABASE_ID         AND D1.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_procedures_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_PROCEDURES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PROCEDURES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT       U.USER_NAME AS OWNER,       CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_NAME ELSE P.PACKAGE_NAME END AS OBJECT_NAME,       CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE R.ROUTINE_NAME END AS PROCEDURE_NAME,       CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,       CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,       CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,       CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,       CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,       CAST(DECODE(BITAND(R.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,       CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,       CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,       CAST(DECODE(BITAND(R.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,       CAST('NO' AS VARCHAR2(3)) AS INTERFACE,       CAST(DECODE(BITAND(R.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,       CAST(DECODE(BITAND(R.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,       R.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID())R       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID           AND U.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID           AND P.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0     UNION ALL     SELECT       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_NAME ELSE PS.PACKAGE_NAME END AS OBJECT_NAME,       CASE RS.PACKAGE_ID WHEN -1 THEN NULL ELSE RS.ROUTINE_NAME END AS PROCEDURE_NAME,       CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_ID ELSE RS.PACKAGE_ID END AS OBJECT_ID,       CASE RS.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE RS.SUBPROGRAM_ID END AS SUBPROGRAM_ID,       CASE RS.OVERLOAD WHEN 0 THEN NULL ELSE RS.OVERLOAD END AS OVERLOAD,       CASE RS.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,       CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,       CAST(DECODE(BITAND(RS.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,       CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,       CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,       CAST(DECODE(BITAND(RS.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,       CAST('NO' AS VARCHAR2(3)) AS INTERFACE,       CAST(DECODE(BITAND(RS.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,       CAST(DECODE(BITAND(RS.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,       RS.TENANT_ID AS ORIGIN_CON_ID     FROM       SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS       LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS ON RS.PACKAGE_ID = PS.PACKAGE_ID )__"))) {
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

int ObInnerTableSchema::dba_arguments_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_ARGUMENTS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_ARGUMENTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT     U.USER_NAME AS OWNER,     R.ROUTINE_NAME AS OBJECT_NAME,     CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE P.PACKAGE_NAME END AS PACKAGE_NAME,     CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,     CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,     CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,     RP.PARAM_NAME AS ARGUMENT_NAME,     RP.PARAM_POSITION AS POSITION,     RP.SEQUENCE AS SEQUENCE,     RP.PARAM_LEVEL AS DATA_LEVEL,     V.DATA_TYPE_STR AS DATA_TYPE,     'NO' AS DEFAULTED,     RP.PARAM_LENGTH AS DATA_LENGTH,     DECODE(BITAND(RP.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT', 0, 'OUT') AS IN_OUT,     RP.PARAM_PRECISION AS DATA_PRECISION,     RP.PARAM_SCALE AS DATA_SCALE,     CASE RP.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,     CASE RP.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,     RP.TYPE_OWNER AS TYPE_OWNER,     RP.TYPE_NAME AS TYPE_NAME,     RP.TYPE_SUBNAME AS TYPE_SUBNAME,     RP.TENANT_ID AS ORIGIN_CON_ID   FROM     (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) RP     LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R ON RP.ROUTINE_ID = R.ROUTINE_ID         AND R.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE V ON RP.PARAM_TYPE = V.DATA_TYPE   WHERE     (SYS_CONTEXT('USERENV', 'CON_ID') = RP.TENANT_ID OR RP.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0   UNION ALL   SELECT     CAST('SYS' AS VARCHAR2(30)) AS OWNER,     RS.ROUTINE_NAME AS OBJECT_NAME,     CASE RS.PACKAGE_ID WHEN -1 THEN NULL ELSE PS.PACKAGE_NAME END AS PACKAGE_NAME,     CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_ID ELSE RS.PACKAGE_ID END AS OBJECT_ID,     CASE RS.OVERLOAD WHEN 0 THEN NULL ELSE RS.OVERLOAD END AS OVERLOAD,     CASE RS.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE RS.SUBPROGRAM_ID END AS SUBPROGRAM_ID,     RPS.PARAM_NAME AS ARGUMENT_NAME,     RPS.PARAM_POSITION AS POSITION,     RPS.SEQUENCE AS SEQUENCE,     RPS.PARAM_LEVEL AS DATA_LEVEL,     VV.DATA_TYPE_STR AS DATA_TYPE,     'NO' AS DEFAULTED,     RPS.PARAM_LENGTH AS DATA_LENGTH,     DECODE(BITAND(RPS.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT') AS IN_OUT,     RPS.PARAM_PRECISION AS DATA_PRECISION,     RPS.PARAM_SCALE AS DATA_SCALE,     CASE RPS.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,     CASE RPS.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,     RPS.TYPE_OWNER AS TYPE_OWNER,     RPS.TYPE_NAME AS TYPE_NAME,     RPS.TYPE_SUBNAME AS TYPE_SUBNAME,     RPS.TENANT_ID AS ORIGIN_CON_ID   FROM     SYS.ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT RPS     LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS ON RPS.ROUTINE_ID = RS.ROUTINE_ID     LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS ON RS.PACKAGE_ID = PS.PACKAGE_ID     LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE VV ON RPS.PARAM_TYPE = VV.DATA_TYPE )__"))) {
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

int ObInnerTableSchema::dba_source_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_SOURCE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SOURCE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT       CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,       CAST(P.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE P.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(P.SOURCE) AS TEXT,       P.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) P       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON P.DATABASE_ID = D.DATABASE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON P.OWNER_ID = U.USER_ID           AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = P.TENANT_ID OR P.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0     UNION ALL     SELECT       CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,       CAST(R.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(R.ROUTINE_BODY) AS TEXT,       R.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID           AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0 AND R.PACKAGE_ID = -1     UNION ALL     SELECT       CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,       CAST(T.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,       CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(T.TRIGGER_BODY) AS TEXT,       T.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) T       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON T.DATABASE_ID = D.DATABASE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON T.OWNER_ID = U.USER_ID           AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = T.TENANT_ID OR T.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0     UNION ALL     SELECT       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       CAST(PS.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE PS.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(PS.SOURCE) AS TEXT,       PS.TENANT_ID AS ORIGIN_CON_ID     FROM       SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS     UNION ALL     SELECT       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       CAST(RS.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE RS.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(RS.ROUTINE_BODY) AS TEXT,       RS.TENANT_ID AS ORIGIN_CON_ID     FROM       SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS WHERE RS.ROUTINE_TYPE != 3     UNION ALL     SELECT       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       CAST(TS.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,       CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(TS.TRIGGER_BODY) AS TEXT,       TS.TENANT_ID AS ORIGIN_CON_ID     FROM       SYS.ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT TS )__"))) {
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

int ObInnerTableSchema::all_procedures_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PROCEDURES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PROCEDURES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT       U.USER_NAME AS OWNER,       CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_NAME ELSE P.PACKAGE_NAME END AS OBJECT_NAME,       CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE R.ROUTINE_NAME END AS PROCEDURE_NAME,       CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,       CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,       CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,       CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,       CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,       CAST(DECODE(BITAND(R.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,       CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,       CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,       CAST(DECODE(BITAND(R.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,       CAST('NO' AS VARCHAR2(3)) AS INTERFACE,       CAST(DECODE(BITAND(R.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,       CAST(DECODE(BITAND(R.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,       R.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID           AND U.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID           AND P.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (R.DATABASE_ID = USERENV('SCHEMAID')         OR USER_CAN_ACCESS_OBJ(12, R.ROUTINE_ID, R.DATABASE_ID) = 1)       AND (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0   UNION ALL   SELECT     CAST('SYS' AS VARCHAR2(30)) AS OWNER,     CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_NAME ELSE PS.PACKAGE_NAME END AS OBJECT_NAME,     CASE RS.PACKAGE_ID WHEN -1 THEN NULL ELSE RS.ROUTINE_NAME END AS PROCEDURE_NAME,     CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_ID ELSE RS.PACKAGE_ID END AS OBJECT_ID,     CASE RS.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE RS.SUBPROGRAM_ID END AS SUBPROGRAM_ID,     CASE RS.OVERLOAD WHEN 0 THEN NULL ELSE RS.OVERLOAD END AS OVERLOAD,     CASE RS.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,     CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,     CAST(DECODE(BITAND(RS.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,     CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,     CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,     CAST(DECODE(BITAND(RS.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,     CAST('NO' AS VARCHAR2(3)) AS INTERFACE,     CAST(DECODE(BITAND(RS.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,     CAST(DECODE(BITAND(RS.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,       RS.TENANT_ID AS ORIGIN_CON_ID   FROM     SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS     LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS ON RS.PACKAGE_ID = PS.PACKAGE_ID )__"))) {
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

int ObInnerTableSchema::all_arguments_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ARGUMENTS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ARGUMENTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT     U.USER_NAME AS OWNER,     R.ROUTINE_NAME AS OBJECT_NAME,     CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE P.PACKAGE_NAME END AS PACKAGE_NAME,     CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,     CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,     CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,     RP.PARAM_NAME AS ARGUMENT_NAME,     RP.PARAM_POSITION AS POSITION,     RP.SEQUENCE AS SEQUENCE,     RP.PARAM_LEVEL AS DATA_LEVEL,     V.DATA_TYPE_STR AS DATA_TYPE,     'NO' AS DEFAULTED,     RP.PARAM_LENGTH AS DATA_LENGTH,     DECODE(BITAND(RP.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT', 0, 'OUT') AS IN_OUT,     RP.PARAM_PRECISION AS DATA_PRECISION,     RP.PARAM_SCALE AS DATA_SCALE,     CASE RP.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,     CASE RP.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,     RP.TYPE_OWNER AS TYPE_OWNER,     RP.TYPE_NAME AS TYPE_NAME,     RP.TYPE_SUBNAME AS TYPE_SUBNAME,     RP.TENANT_ID AS ORIGIN_CON_ID   FROM     (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT       WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) RP     LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R ON RP.ROUTINE_ID = R.ROUTINE_ID       AND R.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID       AND P.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID       AND D.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID       AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE V ON RP.PARAM_TYPE = V.DATA_TYPE   WHERE     (SYS_CONTEXT('USERENV', 'CON_ID') = RP.TENANT_ID OR RP.TENANT_ID = 1)     AND D.IN_RECYCLEBIN = 0     AND (R.DATABASE_ID = USERENV('SCHEMAID')       OR USER_CAN_ACCESS_OBJ(12, RP.ROUTINE_ID, 1) = 1)   UNION ALL   SELECT     CAST('SYS' AS VARCHAR2(30)) AS OWNER,     RS.ROUTINE_NAME AS OBJECT_NAME,     CASE RS.PACKAGE_ID WHEN -1 THEN NULL ELSE PS.PACKAGE_NAME END AS PACKAGE_NAME,     CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_ID ELSE RS.PACKAGE_ID END AS OBJECT_ID,     CASE RS.OVERLOAD WHEN 0 THEN NULL ELSE RS.OVERLOAD END AS OVERLOAD,     CASE RS.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE RS.SUBPROGRAM_ID END AS SUBPROGRAM_ID,     RPS.PARAM_NAME AS ARGUMENT_NAME,     RPS.PARAM_POSITION AS POSITION,     RPS.SEQUENCE AS SEQUENCE,     RPS.PARAM_LEVEL AS DATA_LEVEL,     VV.DATA_TYPE_STR AS DATA_TYPE,     'NO' AS DEFAULTED,     RPS.PARAM_LENGTH AS DATA_LENGTH,     DECODE(BITAND(RPS.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT') AS IN_OUT,     RPS.PARAM_PRECISION AS DATA_PRECISION,     RPS.PARAM_SCALE AS DATA_SCALE,     CASE RPS.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,     CASE RPS.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,     RPS.TYPE_OWNER AS TYPE_OWNER,     RPS.TYPE_NAME AS TYPE_NAME,     RPS.TYPE_SUBNAME AS TYPE_SUBNAME,     RPS.TENANT_ID AS ORIGIN_CON_ID   FROM     SYS.ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT RPS     LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS ON RPS.ROUTINE_ID = RS.ROUTINE_ID     LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS ON RS.PACKAGE_ID = PS.PACKAGE_ID     LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE VV ON RPS.PARAM_TYPE = VV.DATA_TYPE )__"))) {
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

int ObInnerTableSchema::all_source_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SOURCE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SOURCE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT       CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,       CAST(P.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE P.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(P.SOURCE) AS TEXT,       P.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) P       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON P.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON P.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = P.TENANT_ID OR P.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0       AND (P.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(12, P.PACKAGE_ID, P.DATABASE_ID) = 1)     UNION ALL     SELECT       CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,       CAST(R.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(R.ROUTINE_BODY) AS TEXT,       R.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0       AND R.PACKAGE_ID = -1       AND (R.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(12, R.ROUTINE_ID, R.DATABASE_ID) = 1)     UNION ALL     SELECT       CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,       CAST(T.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,       CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(T.TRIGGER_BODY) AS TEXT,       T.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID())T       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON T.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON T.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = T.TENANT_ID OR T.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0       AND (T.DATABASE_ID = USERENV('SCHEMAID')           OR USER_CAN_ACCESS_OBJ(12, T.TRIGGER_ID, T.DATABASE_ID) = 1)     UNION ALL     SELECT       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       CAST(PS.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE PS.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(PS.SOURCE) AS TEXT,       PS.TENANT_ID AS ORIGIN_CON_ID     FROM       SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS     UNION ALL     SELECT       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       CAST(RS.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE RS.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(RS.ROUTINE_BODY) AS TEXT,       RS.TENANT_ID AS ORIGIN_CON_ID     FROM       SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS WHERE RS.ROUTINE_TYPE != 3     UNION ALL     SELECT       CAST('SYS' AS VARCHAR2(30)) AS OWNER,       CAST(TS.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,       CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(TS.TRIGGER_BODY) AS TEXT,       TS.TENANT_ID AS ORIGIN_CON_ID     FROM       SYS.ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT TS )__"))) {
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

int ObInnerTableSchema::user_procedures_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_PROCEDURES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PROCEDURES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
                    .set_view_definition(R"__(     SELECT       CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_NAME ELSE P.PACKAGE_NAME END AS OBJECT_NAME,       CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE R.ROUTINE_NAME END AS PROCEDURE_NAME,       CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,       CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,       CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,       CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,       CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,       CAST(DECODE(BITAND(R.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,       CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,       CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,       CAST(DECODE(BITAND(R.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,       CAST('NO' AS VARCHAR2(3)) AS INTERFACE,       CAST(DECODE(BITAND(R.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,       CAST(DECODE(BITAND(R.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,       R.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0       AND R.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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

int ObInnerTableSchema::user_arguments_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_ARGUMENTS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_ARGUMENTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT       R.ROUTINE_NAME AS OBJECT_NAME,       CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE P.PACKAGE_NAME END AS PACKAGE_NAME,       CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,       CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,       CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,       RP.PARAM_NAME AS ARGUMENT_NAME,       RP.PARAM_POSITION AS POSITION,       RP.SEQUENCE AS SEQUENCE,       RP.PARAM_LEVEL AS DATA_LEVEL,       V.DATA_TYPE_STR AS DATA_TYPE,       'NO' AS DEFAULTED,       RP.PARAM_LENGTH AS DATA_LENGTH,       DECODE(BITAND(RP.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT', 0, 'OUT') AS IN_OUT,       RP.PARAM_PRECISION AS DATA_PRECISION,       RP.PARAM_SCALE AS DATA_SCALE,       CASE RP.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,       CASE RP.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,       RP.TYPE_OWNER AS TYPE_OWNER,       RP.TYPE_NAME AS TYPE_NAME,       RP.TYPE_SUBNAME AS TYPE_SUBNAME,       RP.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID())RP       LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R ON RP.ROUTINE_ID = R.ROUTINE_ID         AND R.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID         AND P.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE V ON RP.PARAM_TYPE = V.DATA_TYPE     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = RP.TENANT_ID OR RP.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0       AND R.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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

int ObInnerTableSchema::user_source_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_SOURCE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SOURCE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT       CAST(P.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE P.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(P.SOURCE) AS TEXT,       P.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) P       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON P.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON P.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = P.TENANT_ID OR P.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0     UNION ALL     SELECT       CAST(R.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,       CAST(CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(R.ROUTINE_BODY) AS TEXT,       R.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0       AND R.PACKAGE_ID = -1       AND R.DATABASE_ID = USERENV('SCHEMAID')     UNION ALL     SELECT       CAST(T.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,       CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,       CAST(1 AS NUMBER) AS LINE,       TO_CLOB(T.TRIGGER_BODY) AS TEXT,       T.TENANT_ID AS ORIGIN_CON_ID     FROM       (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) T       LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON T.DATABASE_ID = D.DATABASE_ID         AND D.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON T.OWNER_ID = U.USER_ID         AND U.TENANT_ID = EFFECTIVE_TENANT_ID()     WHERE       (SYS_CONTEXT('USERENV', 'CON_ID') = T.TENANT_ID OR T.TENANT_ID = 1)       AND D.IN_RECYCLEBIN = 0       AND T.DATABASE_ID = USERENV('SCHEMAID'); )__"))) {
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

int ObInnerTableSchema::dba_part_key_columns_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_PART_KEY_COLUMNS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PART_KEY_COLUMNS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT  D.DATABASE_NAME AS OWNER,             CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,             'TABLE' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION     SELECT  D.DATABASE_NAME AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION     SELECT  D.DATABASE_NAME AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(-1 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2)           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_part_key_columns_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PART_KEY_COLUMNS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_KEY_COLUMNS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT  D.DATABASE_NAME AS OWNER,             CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,             'TABLE' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (T.DATABASE_ID = USERENV('SCHEMAID')            OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)     UNION     SELECT  D.DATABASE_NAME AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (T.DATABASE_ID = USERENV('SCHEMAID')            OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)     UNION     SELECT  D.DATABASE_NAME AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(-1 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2)           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND (T.DATABASE_ID = USERENV('SCHEMAID')            OR USER_CAN_ACCESS_OBJ(1, T.DATA_TABLE_ID, T.DATABASE_ID) = 1) )__"))) {
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

int ObInnerTableSchema::user_part_key_columns_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_PART_KEY_COLUMNS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PART_KEY_COLUMNS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT  CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,             'TABLE' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T     WHERE C.TENANT_ID = T.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)           AND T.DATABASE_ID = USERENV('SCHEMAID')           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION     SELECT  CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND T.DATABASE_ID = USERENV('SCHEMAID')           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION     SELECT  CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(-1 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2)           AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0           AND T.DATABASE_ID = USERENV('SCHEMAID')           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_subpart_key_columns_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_SUBPART_KEY_COLUMNS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SUBPART_KEY_COLUMNS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT  D.DATABASE_NAME AS OWNER,             CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,             'TABLE' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION     SELECT  D.DATABASE_NAME AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION     SELECT  D.DATABASE_NAME AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(-1 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2)           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_subpart_key_columns_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SUBPART_KEY_COLUMNS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SUBPART_KEY_COLUMNS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(      SELECT  D.DATABASE_NAME AS OWNER,             CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,             'TABLE' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND (T.DATABASE_ID = USERENV('SCHEMAID')                OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)     UNION     SELECT  D.DATABASE_NAME AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND (T.DATABASE_ID = USERENV('SCHEMAID')                OR USER_CAN_ACCESS_OBJ(1, T.DATA_TABLE_ID, T.DATABASE_ID) = 1)     UNION     SELECT  D.DATABASE_NAME AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(-1 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2)           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND (T.DATABASE_ID = USERENV('SCHEMAID')                OR USER_CAN_ACCESS_OBJ(1, T.DATA_TABLE_ID, T.DATABASE_ID) = 1) )__"))) {
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

int ObInnerTableSchema::user_subpart_key_columns_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_SUBPART_KEY_COLUMNS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SUBPART_KEY_COLUMNS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(     SELECT  CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,             'TABLE' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T     WHERE C.TENANT_ID = T.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)           AND T.DATABASE_ID = USERENV('SCHEMAID')           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION     SELECT  CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND T.DATABASE_ID = USERENV('SCHEMAID')     UNION     SELECT  CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,             'INDEX' AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,             CAST(-1 AS NUMBER) AS COLUMN_POSITION,             CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID     FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2)           AND C.TENANT_ID = EFFECTIVE_TENANT_ID()           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()           AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0           AND T.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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

int ObInnerTableSchema::dba_views_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_VIEWS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_VIEWS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(   SELECT CAST('SYS' AS VARCHAR2(128)) OWNER,       CAST(TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,       CAST(LENGTH(VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,       TO_CLOB(VIEW_DEFINITION) TEXT,       CAST(NULL AS NUMBER) OID_TEXT_LENGTH,       CAST(NULL AS VARCHAR2(4000)) OID_TEXT,       CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,       CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,       CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,       CAST(NULL AS VARCHAR2(1)) READ_ONLY  FROM SYS.ALL_VIRTUAL_TABLE_SYS_AGENT  WHERE BITAND(TABLE_ID,  1099511627775) > 25000      AND BITAND(TABLE_ID, 1099511627775) <= 28000      AND TABLE_TYPE = 1  UNION ALL  SELECT CAST(B.DATABASE_NAME AS VARCHAR2(128)) OWNER,         CAST(A.TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,         CAST(LENGTH(A.VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,         TO_CLOB(VIEW_DEFINITION) TEXT,         CAST(NULL AS NUMBER) OID_TEXT_LENGTH,         CAST(NULL AS VARCHAR2(4000)) OID_TEXT,         CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,         CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,         CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,         CAST(NULL AS VARCHAR2(1)) READ_ONLY  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A,       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B  WHERE A.TABLE_TYPE = 4        AND A.DATABASE_ID = B.DATABASE_ID        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()        AND B.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_views_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIEWS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIEWS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(   SELECT CAST('SYS' AS VARCHAR2(128)) OWNER,       CAST(TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,       CAST(LENGTH(VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,       TO_CLOB(VIEW_DEFINITION) TEXT,       CAST(NULL AS NUMBER) OID_TEXT_LENGTH,       CAST(NULL AS VARCHAR2(4000)) OID_TEXT,       CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,       CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,       CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,       CAST(NULL AS VARCHAR2(1)) READ_ONLY  FROM SYS.ALL_VIRTUAL_TABLE_SYS_AGENT  WHERE BITAND(TABLE_ID, 1099511627775) > 25000       AND BITAND(TABLE_ID, 1099511627775) <= 28000       AND TABLE_TYPE = 1       AND ((SUBSTR(TABLE_NAME,1,3) = 'DBA' AND USER_CAN_ACCESS_OBJ(1, TABLE_ID, DATABASE_ID) =1)            OR SUBSTR(TABLE_NAME,1,3) != 'DBA')   UNION ALL  SELECT CAST(B.DATABASE_NAME AS VARCHAR2(128)) OWNER,         CAST(A.TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,         CAST(LENGTH(A.VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,         TO_CLOB(VIEW_DEFINITION) TEXT,         CAST(NULL AS NUMBER) OID_TEXT_LENGTH,         CAST(NULL AS VARCHAR2(4000)) OID_TEXT,         CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,         CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,         CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,         CAST(NULL AS VARCHAR2(1)) READ_ONLY  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A,       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B  WHERE A.TABLE_TYPE = 4         AND A.DATABASE_ID = B.DATABASE_ID         AND A.TENANT_ID = EFFECTIVE_TENANT_ID()         AND B.TENANT_ID = EFFECTIVE_TENANT_ID()         AND (A.DATABASE_ID = USERENV('SCHEMAID')              OR USER_CAN_ACCESS_OBJ(1, A.TABLE_ID, A.DATABASE_ID) =1) )__"))) {
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

int ObInnerTableSchema::user_views_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_VIEWS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_VIEWS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(   SELECT CAST(A.TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,         CAST(LENGTH(A.VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,         TO_CLOB(VIEW_DEFINITION) TEXT,         CAST(NULL AS NUMBER) OID_TEXT_LENGTH,         CAST(NULL AS VARCHAR2(4000)) OID_TEXT,         CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,         CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,         CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,         CAST(NULL AS VARCHAR2(1)) READ_ONLY  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A,        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B   WHERE A.TABLE_TYPE = 4         AND A.DATABASE_ID = B.DATABASE_ID         AND B.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()        AND B.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_tab_partitions_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TAB_PARTITIONS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TAB_PARTITIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            table_schema.set_view_definition(
                R"__(SELECT CAST(DB_TB.DATABASE_NAME AS VARCHAR2(128)) TABLE_OWNER,      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(CASE WHEN      PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) COMPOSITE,      CAST(PART.PART_NAME AS VARCHAR(128)) PARTITION_NAME,      CAST(PART.SUB_PART_NUM AS NUMBER)  SUBPARTITION_COUNT,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.PART_ID + 1 AS NUMBER) PARTITION_POSITION,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,      CAST(NULL AS NUMBER) PCT_FREE,      CAST(NULL AS NUMBER) PCT_USED,      CAST(NULL AS NUMBER) INI_TRANS,      CAST(NULL AS NUMBER) MAX_TRANS,      CAST(NULL AS NUMBER) INITIAL_EXTENT,      CAST(NULL AS NUMBER) NEXT_EXTENT,      CAST(NULL AS NUMBER) MIN_EXTENT,      CAST(NULL AS NUMBER) MAX_EXTENT,      CAST(NULL AS NUMBER) MAX_SIZE,      CAST(NULL AS NUMBER) PCT_INCREASE,      CAST(NULL AS NUMBER) FREELISTS,      CAST(NULL AS NUMBER) FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(7)) LOGGING,      CAST(CASE WHEN      PART.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS NUMBER) NUM_ROWS,      CAST(NULL AS NUMBER) BLOCKS,      CAST(NULL AS NUMBER) EMPTY_BLOCKS,      CAST(NULL AS NUMBER) AVG_SPACE,      CAST(NULL AS NUMBER) CHAIN_CNT,      CAST(NULL AS NUMBER) AVG_ROW_LEN,      CAST(NULL AS NUMBER) SAMPLE_SIZE,      CAST(NULL AS DATE) LAST_ANALYZED,      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,      CAST(NULL AS VARCHAR2(3)) USER_STATS,      CAST(NULL AS VARCHAR2(3)) IS_NESTED,      CAST(NULL AS VARCHAR2(30)) PARENT_TABLE_PARTITION,      CAST(NULL AS VARCHAR2(3)) "INTERVAL",      CAST(NULL AS VARCHAR2(4)) SEGMENT_CREATED      FROM      (SELECT DB.DATABASE_NAME,              DB.DATABASE_ID,              TB.TABLE_ID,              TB.TABLE_NAME       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB       WHERE TB.DATABASE_ID = DB.DATABASE_ID             AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()             AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()             AND (TB.DATABASE_ID = USERENV('SCHEMAID')                 OR USER_CAN_ACCESS_OBJ(1, TB.TABLE_ID, TB.DATABASE_ID) = 1)) DB_TB      JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART      ON   DB_TB.TABLE_ID = PART.TABLE_ID        AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()      LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_tab_subpartitions_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TAB_SUBPARTITIONS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TAB_SUBPARTITIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT CAST(DB_TB.DATABASE_NAME AS VARCHAR2(128)) TABLE_OWNER,      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,      CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.SUB_PART_ID + 1 AS NUMBER) SUBPARTITION_POSITION,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,      CAST(NULL AS NUMBER) PCT_FREE,      CAST(NULL AS NUMBER) PCT_USED,      CAST(NULL AS NUMBER) INI_TRANS,      CAST(NULL AS NUMBER) MAX_TRANS,      CAST(NULL AS NUMBER) INITIAL_EXTENT,      CAST(NULL AS NUMBER) NEXT_EXTENT,      CAST(NULL AS NUMBER) MIN_EXTENT,      CAST(NULL AS NUMBER) MAX_EXTENT,      CAST(NULL AS NUMBER) MAX_SIZE,      CAST(NULL AS NUMBER) PCT_INCREASE,      CAST(NULL AS NUMBER) FREELISTS,      CAST(NULL AS NUMBER) FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(3)) LOGGING,      CAST(CASE WHEN      PART.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS NUMBER) NUM_ROWS,      CAST(NULL AS NUMBER) BLOCKS,      CAST(NULL AS NUMBER) EMPTY_BLOCKS,      CAST(NULL AS NUMBER) AVG_SPACE,      CAST(NULL AS NUMBER) CHAIN_CNT,      CAST(NULL AS NUMBER) AVG_ROW_LEN,      CAST(NULL AS NUMBER) SAMPLE_SIZE,      CAST(NULL AS DATE) LAST_ANALYZED,      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,      CAST(NULL AS VARCHAR2(3)) USER_STATS,      CAST(NULL AS VARCHAR2(3)) "INTERVAL",      CAST(NULL AS VARCHAR2(3)) SEGMENT_CREATED      FROM      (SELECT DB.DATABASE_NAME,              DB.DATABASE_ID,              TB.TABLE_ID,              TB.TABLE_NAME       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB       WHERE TB.DATABASE_ID = DB.DATABASE_ID             AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()             AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()             AND (TB.DATABASE_ID = USERENV('SCHEMAID')                 OR USER_CAN_ACCESS_OBJ(1, TB.TABLE_ID, TB.DATABASE_ID) = 1)) DB_TB      JOIN      (SELECT P_PART.PART_NAME,              P_PART.SUB_PART_NUM,              P_PART.TABLE_ID,              S_PART.SUB_PART_NAME,              S_PART.HIGH_BOUND_VAL,              S_PART.LIST_VAL,              S_PART.COMPRESS_FUNC_NAME,              S_PART.SUB_PART_ID,              S_PART.TABLESPACE_ID        FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT P_PART,            SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT S_PART       WHERE P_PART.PART_ID = S_PART.PART_ID AND             P_PART.TABLE_ID = S_PART.TABLE_ID              AND P_PART.TENANT_ID = EFFECTIVE_TENANT_ID()             AND S_PART.TENANT_ID = EFFECTIVE_TENANT_ID()) PART      ON      DB_TB.TABLE_ID = PART.TABLE_ID       LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID       AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()  )__"))) {
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

int ObInnerTableSchema::all_part_tables_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PART_TABLES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT CAST(DB.DATABASE_NAME AS VARCHAR2(128)) OWNER,      CAST(TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(CASE TB.PART_FUNC_TYPE WHEN 0 THEN      'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'      WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'      WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'      WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END      AS VARCHAR2(9)) PARTITIONING_TYPE,      CAST (DECODE(TB.PART_LEVEL, 1, 'NONE',                                  2, DECODE(TB.SUB_PART_FUNC_TYPE, 0, 'HASH',                                                                   1, 'KEY',                                                                   2, 'KEY',                                                                   3, 'RANGE',                                                                   4, 'RANGE',                                                                   5, 'LIST',                                                                   6, 'KEY',                                                                   7, 'LIST',                                                                   8, 'HASH',                                                                   9, 'KEY',                                                                   10, 'KEY'))           AS VARCHAR2(9)) SUBPARTITIONING_TYPE,      CAST(TB.PART_NUM AS NUMBER) PARTITION_COUNT,      CAST (DECODE (TB.PART_LEVEL, 1, 0,                                   2, TB.SUB_PART_NUM) AS NUMBER) DEF_SUBPARTITION_COUNT,      CAST(PART_INFO.PART_KEY_COUNT AS NUMBER) PARTITIONING_KEY_COUNT,      CAST (DECODE (TB.PART_LEVEL, 1, 0,                                   2, PART_INFO.SUBPART_KEY_COUNT) AS NUMBER) SUBPARTITIONING_KEY_COUNT,      CAST(NULL AS VARCHAR2(8)) STATUS,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) DEF_TABLESPACE_NAME,      CAST(NULL AS NUMBER) DEF_PCT_FREE,      CAST(NULL AS NUMBER) DEF_PCT_USED,      CAST(NULL AS NUMBER) DEF_INI_TRANS,      CAST(NULL AS NUMBER) DEF_MAX_TRANS,      CAST(NULL AS VARCHAR2(40)) DEF_INITIAL_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_NEXT_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_MIN_EXTENT,      CAST(NULL AS VARCHAR2(40)) MAX_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_MAX_SIZE,      CAST(NULL AS VARCHAR2(40)) DEF_PCT_INCREASE,      CAST(NULL AS NUMBER) DEF_FREELISTS,      CAST(NULL AS NUMBER) DEF_FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(7)) DEF_LOGGING,      CAST(CASE WHEN      TB.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(TB.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS VARCHAR2(7)) DEF_BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) DEF_FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) DEF_CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(30)) REF_PTN_CONSTRAINT_NAME,      CAST(NULL AS VARCHAR2(1000)) "INTERVAL",      CAST(NULL AS VARCHAR2(3)) IS_NESTED,      CAST(NULL AS VARCHAR2(4)) DEF_SEGMENT_CREATED,      CAST(CASE TB.AUTO_PART WHEN 1 THEN 'AUTO_PART'      WHEN 0 THEN '' END AS VARCHAR(10)) AUTO_PART,      CAST(TB.AUTO_PART_SIZE AS NUMBER) AUTO_PART_SIZE      FROM   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB      JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB      ON        TB.DATABASE_ID = DB.DATABASE_ID         AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()        AND (TB.DATABASE_ID = USERENV('SCHEMAID')             OR USER_CAN_ACCESS_OBJ(1, TB.TABLE_ID, TB.DATABASE_ID) = 1)      JOIN        (select table_id,                sum(case when BITAND(PARTITION_KEY_POSITION, 255) > 0 then 1 else 0 end) as PART_KEY_COUNT,                sum(case when BITAND(PARTITION_KEY_POSITION, 65280) > 0 then 0 else 1 end) as SUBPART_KEY_COUNT         from SYS.ALL_VIRTUAL_COLUMN_AGENT         where PARTITION_KEY_POSITION > 0         group by table_id) PART_INFO      ON        TB.TABLE_ID = PART_INFO.TABLE_ID      LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = TB.TABLESPACE_ID      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()      WHERE TB.TABLE_TYPE != 5       AND TB.PART_LEVEL != 0)__"))) {
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

int ObInnerTableSchema::dba_part_tables_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_PART_TABLES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PART_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT CAST(DB.DATABASE_NAME AS VARCHAR2(128)) OWNER,      CAST(TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(CASE TB.PART_FUNC_TYPE WHEN 0 THEN      'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'      WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'      WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'      WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END      AS VARCHAR2(9)) PARTITIONING_TYPE,      CAST (DECODE(TB.PART_LEVEL, 1, 'NONE',                                  2, DECODE(TB.SUB_PART_FUNC_TYPE, 0, 'HASH',                                                                   1, 'KEY',                                                                   2, 'KEY',                                                                   3, 'RANGE',                                                                   4, 'RANGE',                                                                   5, 'LIST',                                                                   6, 'KEY',                                                                   7, 'LIST',                                                                   8, 'HASH',                                                                   9, 'KEY',                                                                   10, 'KEY'))           AS VARCHAR2(9)) SUBPARTITIONING_TYPE,      CAST (TB.PART_NUM AS NUMBER) PARTITION_COUNT,      CAST (DECODE (TB.PART_LEVEL, 1, 0,                                   2, TB.SUB_PART_NUM) AS NUMBER) DEF_SUBPARTITION_COUNT,      CAST(PART_INFO.PART_KEY_COUNT AS NUMBER) PARTITIONING_KEY_COUNT,      CAST (DECODE (TB.PART_LEVEL, 1, 0,                                   2, PART_INFO.SUBPART_KEY_COUNT) AS NUMBER) SUBPARTITIONING_KEY_COUNT,      CAST(NULL AS VARCHAR2(8)) STATUS,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) DEF_TABLESPACE_NAME,      CAST(NULL AS NUMBER) DEF_PCT_FREE,      CAST(NULL AS NUMBER) DEF_PCT_USED,      CAST(NULL AS NUMBER) DEF_INI_TRANS,      CAST(NULL AS NUMBER) DEF_MAX_TRANS,      CAST(NULL AS VARCHAR2(40)) DEF_INITIAL_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_NEXT_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_MIN_EXTENT,      CAST(NULL AS VARCHAR2(40)) MAX_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_MAX_SIZE,      CAST(NULL AS VARCHAR2(40)) DEF_PCT_INCREASE,      CAST(NULL AS NUMBER) DEF_FREELISTS,      CAST(NULL AS NUMBER) DEF_FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(7)) DEF_LOGGING,      CAST(CASE WHEN      TB.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(TB.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS VARCHAR2(7)) DEF_BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) DEF_FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) DEF_CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(30)) REF_PTN_CONSTRAINT_NAME,      CAST(NULL AS VARCHAR2(1000)) "INTERVAL",      CAST(NULL AS VARCHAR2(3)) IS_NESTED,      CAST(NULL AS VARCHAR2(4)) DEF_SEGMENT_CREATED      FROM   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB      JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB      ON        TB.DATABASE_ID = DB.DATABASE_ID        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()      JOIN        (select table_id,                sum(case when BITAND(PARTITION_KEY_POSITION, 255) > 0 then 1 else 0 end) as PART_KEY_COUNT,                sum(case when BITAND(PARTITION_KEY_POSITION, 65280) > 0 then 0 else 1 end) as SUBPART_KEY_COUNT         from SYS.ALL_VIRTUAL_COLUMN_AGENT         where PARTITION_KEY_POSITION > 0         group by table_id) PART_INFO      ON        TB.TABLE_ID = PART_INFO.TABLE_ID      LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = TB.TABLESPACE_ID       AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()      WHERE TB.TABLE_TYPE != 5       AND TB.PART_LEVEL != 0)__"))) {
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

int ObInnerTableSchema::user_part_tables_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_PART_TABLES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PART_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT CAST(TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(CASE TB.PART_FUNC_TYPE WHEN 0 THEN      'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'      WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'      WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'      WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END      AS VARCHAR2(9)) PARTITIONING_TYPE,      CAST (DECODE(TB.PART_LEVEL, 1, 'NONE',                                  2, DECODE(TB.SUB_PART_FUNC_TYPE, 0, 'HASH',                                                                   1, 'KEY',                                                                   2, 'KEY',                                                                   3, 'RANGE',                                                                   4, 'RANGE',                                                                   5, 'LIST',                                                                   6, 'KEY',                                                                   7, 'LIST',                                                                   8, 'HASH',                                                                   9, 'KEY',                                                                   10, 'KEY'))           AS VARCHAR2(9)) SUBPARTITIONING_TYPE,      CAST(TB.PART_NUM AS NUMBER) PARTITION_COUNT,      CAST (DECODE (TB.PART_LEVEL, 1, 0,                                   2, TB.SUB_PART_NUM) AS NUMBER) DEF_SUBPARTITION_COUNT,      CAST(PART_INFO.PART_KEY_COUNT AS NUMBER) PARTITIONING_KEY_COUNT,      CAST (DECODE (TB.PART_LEVEL, 1, 0,                                   2, PART_INFO.SUBPART_KEY_COUNT) AS NUMBER) SUBPARTITIONING_KEY_COUNT,      CAST(NULL AS VARCHAR2(8)) STATUS,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) DEF_TABLESPACE_NAME,      CAST(NULL AS NUMBER) DEF_PCT_FREE,      CAST(NULL AS NUMBER) DEF_PCT_USED,      CAST(NULL AS NUMBER) DEF_INI_TRANS,      CAST(NULL AS NUMBER) DEF_MAX_TRANS,      CAST(NULL AS VARCHAR2(40)) DEF_INITIAL_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_NEXT_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_MIN_EXTENT,      CAST(NULL AS VARCHAR2(40)) MAX_EXTENT,      CAST(NULL AS VARCHAR2(40)) DEF_MAX_SIZE,      CAST(NULL AS VARCHAR2(40)) DEF_PCT_INCREASE,      CAST(NULL AS NUMBER) DEF_FREELISTS,      CAST(NULL AS NUMBER) DEF_FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(7)) DEF_LOGGING,      CAST(CASE WHEN      TB.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(TB.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS VARCHAR2(7)) DEF_BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) DEF_FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) DEF_CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(30)) REF_PTN_CONSTRAINT_NAME,      CAST(NULL AS VARCHAR2(1000)) "INTERVAL",      CAST(NULL AS VARCHAR2(3)) IS_NESTED,      CAST(NULL AS VARCHAR2(4)) DEF_SEGMENT_CREATED      FROM   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB      JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB      ON        TB.DATABASE_ID = DB.DATABASE_ID        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()        AND DB.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')       JOIN        (select table_id,                sum(case when BITAND(PARTITION_KEY_POSITION, 255) > 0 then 1 else 0 end) as PART_KEY_COUNT,                sum(case when BITAND(PARTITION_KEY_POSITION, 65280) > 0 then 0 else 1 end) as SUBPART_KEY_COUNT         from SYS.ALL_VIRTUAL_COLUMN_AGENT         where PARTITION_KEY_POSITION > 0         group by table_id) PART_INFO      ON        TB.TABLE_ID = PART_INFO.TABLE_ID      LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = TB.TABLESPACE_ID       AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()      WHERE TB.TABLE_TYPE != 5       AND TB.PART_LEVEL != 0)__"))) {
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

int ObInnerTableSchema::dba_tab_partitions_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_TAB_PARTITIONS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_PARTITIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT CAST(DB_TB.DATABASE_NAME AS VARCHAR2(128)) TABLE_OWNER,      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(CASE WHEN      PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) COMPOSITE,      CAST(PART.PART_NAME AS VARCHAR(128)) PARTITION_NAME,      CAST(PART.SUB_PART_NUM AS NUMBER)  SUBPARTITION_COUNT,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.PART_ID + 1 AS NUMBER) PARTITION_POSITION,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,      CAST(NULL AS NUMBER) PCT_FREE,      CAST(NULL AS NUMBER) PCT_USED,      CAST(NULL AS NUMBER) INI_TRANS,      CAST(NULL AS NUMBER) MAX_TRANS,      CAST(NULL AS NUMBER) INITIAL_EXTENT,      CAST(NULL AS NUMBER) NEXT_EXTENT,      CAST(NULL AS NUMBER) MIN_EXTENT,      CAST(NULL AS NUMBER) MAX_EXTENT,      CAST(NULL AS NUMBER) MAX_SIZE,      CAST(NULL AS NUMBER) PCT_INCREASE,      CAST(NULL AS NUMBER) FREELISTS,      CAST(NULL AS NUMBER) FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(7)) LOGGING,      CAST(CASE WHEN      PART.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS NUMBER) NUM_ROWS,      CAST(NULL AS NUMBER) BLOCKS,      CAST(NULL AS NUMBER) EMPTY_BLOCKS,      CAST(NULL AS NUMBER) AVG_SPACE,      CAST(NULL AS NUMBER) CHAIN_CNT,      CAST(NULL AS NUMBER) AVG_ROW_LEN,      CAST(NULL AS NUMBER) SAMPLE_SIZE,      CAST(NULL AS DATE) LAST_ANALYZED,      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,      CAST(NULL AS VARCHAR2(3)) USER_STATS,      CAST(NULL AS VARCHAR2(3)) IS_NESTED,      CAST(NULL AS VARCHAR2(30)) PARENT_TABLE_PARTITION,      CAST(NULL AS VARCHAR2(3)) "INTERVAL",      CAST(NULL AS VARCHAR2(4)) SEGMENT_CREATED      FROM      (SELECT DB.DATABASE_NAME,              DB.DATABASE_ID,              TB.TABLE_ID,              TB.TABLE_NAME       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB       WHERE TB.DATABASE_ID = DB.DATABASE_ID         AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()) DB_TB      JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART      ON   DB_TB.TABLE_ID = PART.TABLE_ID      AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()       LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID       AND TP.TENANT_ID = EFFECTIVE_TENANT_ID())__"))) {
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

int ObInnerTableSchema::user_tab_partitions_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_TAB_PARTITIONS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TAB_PARTITIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT       CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(CASE WHEN      PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) COMPOSITE,      CAST(PART.PART_NAME AS VARCHAR(128)) PARTITION_NAME,      CAST(PART.SUB_PART_NUM AS NUMBER)  SUBPARTITION_COUNT,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.PART_ID + 1 AS NUMBER) PARTITION_POSITION,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,      CAST(NULL AS NUMBER) PCT_FREE,      CAST(NULL AS NUMBER) PCT_USED,      CAST(NULL AS NUMBER) INI_TRANS,      CAST(NULL AS NUMBER) MAX_TRANS,      CAST(NULL AS NUMBER) INITIAL_EXTENT,      CAST(NULL AS NUMBER) NEXT_EXTENT,      CAST(NULL AS NUMBER) MIN_EXTENT,      CAST(NULL AS NUMBER) MAX_EXTENT,      CAST(NULL AS NUMBER) MAX_SIZE,      CAST(NULL AS NUMBER) PCT_INCREASE,      CAST(NULL AS NUMBER) FREELISTS,      CAST(NULL AS NUMBER) FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(7)) LOGGING,      CAST(CASE WHEN      PART.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS NUMBER) NUM_ROWS,      CAST(NULL AS NUMBER) BLOCKS,      CAST(NULL AS NUMBER) EMPTY_BLOCKS,      CAST(NULL AS NUMBER) AVG_SPACE,      CAST(NULL AS NUMBER) CHAIN_CNT,      CAST(NULL AS NUMBER) AVG_ROW_LEN,      CAST(NULL AS NUMBER) SAMPLE_SIZE,      CAST(NULL AS DATE) LAST_ANALYZED,      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,      CAST(NULL AS VARCHAR2(3)) USER_STATS,      CAST(NULL AS VARCHAR2(3)) IS_NESTED,      CAST(NULL AS VARCHAR2(30)) PARENT_TABLE_PARTITION,      CAST(NULL AS VARCHAR2(3)) "INTERVAL",      CAST(NULL AS VARCHAR2(4)) SEGMENT_CREATED      FROM      (SELECT DB.DATABASE_NAME,              DB.DATABASE_ID,              TB.TABLE_ID,              TB.TABLE_NAME       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB       WHERE TB.DATABASE_ID = DB.DATABASE_ID         AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()) DB_TB      JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART      ON   DB_TB.TABLE_ID = PART.TABLE_ID          AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()          AND DB_TB.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')      LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID       AND TP.TENANT_ID = EFFECTIVE_TENANT_ID())__"))) {
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

int ObInnerTableSchema::dba_tab_subpartitions_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_TAB_SUBPARTITIONS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_SUBPARTITIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT CAST(DB_TB.DATABASE_NAME AS VARCHAR2(128)) TABLE_OWNER,      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,      CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.SUB_PART_ID + 1 AS NUMBER) SUBPARTITION_POSITION,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,      CAST(NULL AS NUMBER) PCT_FREE,      CAST(NULL AS NUMBER) PCT_USED,      CAST(NULL AS NUMBER) INI_TRANS,      CAST(NULL AS NUMBER) MAX_TRANS,      CAST(NULL AS NUMBER) INITIAL_EXTENT,      CAST(NULL AS NUMBER) NEXT_EXTENT,      CAST(NULL AS NUMBER) MIN_EXTENT,      CAST(NULL AS NUMBER) MAX_EXTENT,      CAST(NULL AS NUMBER) MAX_SIZE,      CAST(NULL AS NUMBER) PCT_INCREASE,      CAST(NULL AS NUMBER) FREELISTS,      CAST(NULL AS NUMBER) FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(3)) LOGGING,      CAST(CASE WHEN      PART.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS NUMBER) NUM_ROWS,      CAST(NULL AS NUMBER) BLOCKS,      CAST(NULL AS NUMBER) EMPTY_BLOCKS,      CAST(NULL AS NUMBER) AVG_SPACE,      CAST(NULL AS NUMBER) CHAIN_CNT,      CAST(NULL AS NUMBER) AVG_ROW_LEN,      CAST(NULL AS NUMBER) SAMPLE_SIZE,      CAST(NULL AS DATE) LAST_ANALYZED,      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,      CAST(NULL AS VARCHAR2(3)) USER_STATS,      CAST(NULL AS VARCHAR2(3)) "INTERVAL",      CAST(NULL AS VARCHAR2(3)) SEGMENT_CREATED      FROM      (SELECT DB.DATABASE_NAME,              DB.DATABASE_ID,              TB.TABLE_ID,              TB.TABLE_NAME       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB       WHERE TB.DATABASE_ID = DB.DATABASE_ID        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()) DB_TB      JOIN      (SELECT P_PART.PART_NAME,              P_PART.SUB_PART_NUM,              P_PART.TABLE_ID,              S_PART.SUB_PART_NAME,              S_PART.HIGH_BOUND_VAL,              S_PART.LIST_VAL,              S_PART.COMPRESS_FUNC_NAME,              S_PART.SUB_PART_ID,              S_PART.TABLESPACE_ID        FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT P_PART,            SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT S_PART       WHERE P_PART.PART_ID = S_PART.PART_ID AND             P_PART.TABLE_ID = S_PART.TABLE_ID              AND P_PART.TENANT_ID = EFFECTIVE_TENANT_ID()              AND S_PART.TENANT_ID = EFFECTIVE_TENANT_ID()) PART      ON      DB_TB.TABLE_ID = PART.TABLE_ID      LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID())__"))) {
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

int ObInnerTableSchema::user_tab_subpartitions_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_TAB_SUBPARTITIONS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TAB_SUBPARTITIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT       CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,      CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,      CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,      CAST(CASE WHEN      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,      CAST(PART.SUB_PART_ID + 1 AS NUMBER) SUBPARTITION_POSITION,      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,      CAST(NULL AS NUMBER) PCT_FREE,      CAST(NULL AS NUMBER) PCT_USED,      CAST(NULL AS NUMBER) INI_TRANS,      CAST(NULL AS NUMBER) MAX_TRANS,      CAST(NULL AS NUMBER) INITIAL_EXTENT,      CAST(NULL AS NUMBER) NEXT_EXTENT,      CAST(NULL AS NUMBER) MIN_EXTENT,      CAST(NULL AS NUMBER) MAX_EXTENT,      CAST(NULL AS NUMBER) MAX_SIZE,      CAST(NULL AS NUMBER) PCT_INCREASE,      CAST(NULL AS NUMBER) FREELISTS,      CAST(NULL AS NUMBER) FREELIST_GROUPS,      CAST(NULL AS VARCHAR2(3)) LOGGING,      CAST(CASE WHEN      PART.COMPRESS_FUNC_NAME IS NULL THEN      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,      CAST(NULL AS NUMBER) NUM_ROWS,      CAST(NULL AS NUMBER) BLOCKS,      CAST(NULL AS NUMBER) EMPTY_BLOCKS,      CAST(NULL AS NUMBER) AVG_SPACE,      CAST(NULL AS NUMBER) CHAIN_CNT,      CAST(NULL AS NUMBER) AVG_ROW_LEN,      CAST(NULL AS NUMBER) SAMPLE_SIZE,      CAST(NULL AS DATE) LAST_ANALYZED,      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,      CAST(NULL AS VARCHAR2(3)) USER_STATS,      CAST(NULL AS VARCHAR2(3)) "INTERVAL",      CAST(NULL AS VARCHAR2(3)) SEGMENT_CREATED      FROM      (SELECT DB.DATABASE_NAME,              DB.DATABASE_ID,              TB.TABLE_ID,              TB.TABLE_NAME       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB       WHERE TB.DATABASE_ID = DB.DATABASE_ID       AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()       AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()) DB_TB      JOIN      (SELECT P_PART.PART_NAME,              P_PART.SUB_PART_NUM,              P_PART.TABLE_ID,              S_PART.SUB_PART_NAME,              S_PART.HIGH_BOUND_VAL,              S_PART.LIST_VAL,              S_PART.COMPRESS_FUNC_NAME,              S_PART.SUB_PART_ID,              S_PART.TABLESPACE_ID        FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT P_PART,            SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT S_PART       WHERE P_PART.PART_ID = S_PART.PART_ID AND             P_PART.TABLE_ID = S_PART.TABLE_ID             AND P_PART.TENANT_ID = EFFECTIVE_TENANT_ID()             AND S_PART.TENANT_ID = EFFECTIVE_TENANT_ID()) PART      ON      DB_TB.TABLE_ID = PART.TABLE_ID AND      DB_TB.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')      LEFT JOIN        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID())__"))) {
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

int ObInnerTableSchema::dba_subpartition_templates_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_SUBPARTITION_TEMPLATES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SUBPARTITION_TEMPLATES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            table_schema
                .set_view_definition(R"__(SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) USER_NAME,       TB.TABLE_NAME AS TABLE_NAME,       SP.SUB_PART_NAME AS SUBPARTITION_NAME,       SP.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,       TP.TABLESPACE_NAME AS TABLESPACE_NAME,       CAST(CASE WHEN SP.HIGH_BOUND_VAL is NULL THEN SP.LIST_VAL ELSE SP.HIGH_BOUND_VAL END AS VARCHAR2(1024)) HIGH_BOUND       FROM (SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB join SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB             on DB.database_id = TB.database_id               AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()               AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()             join SYS.ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT SP on tb.table_id = sp.table_id                 AND SP.TENANT_ID = EFFECTIVE_TENANT_ID())             left join SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP               ON TP.TABLESPACE_ID = SP.TABLESPACE_ID               AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()       )__"))) {
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

int ObInnerTableSchema::all_subpartition_templates_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SUBPARTITION_TEMPLATES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SUBPARTITION_TEMPLATES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__(SELECT       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) USER_NAME,       TB.TABLE_NAME AS TABLE_NAME,       SP.SUB_PART_NAME AS SUBPARTITION_NAME,       SP.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,       TP.TABLESPACE_NAME AS TABLESPACE_NAME,       CAST(CASE WHEN SP.HIGH_BOUND_VAL is NULL THEN SP.LIST_VAL ELSE SP.HIGH_BOUND_VAL END AS VARCHAR2(1024)) HIGH_BOUND       FROM (SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB join SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB             on DB.database_id = TB.database_id             AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()             AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()             AND (TB.DATABASE_ID = USERENV('SCHEMAID')                  OR USER_CAN_ACCESS_OBJ(1, TB.TABLE_ID, TB.DATABASE_ID) = 1)             join SYS.ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT SP on tb.table_id = sp.table_id                 AND SP.TENANT_ID = EFFECTIVE_TENANT_ID())             left join SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP               ON TP.TABLESPACE_ID = SP.TABLESPACE_ID               AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()       )__"))) {
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

int ObInnerTableSchema::user_subpartition_templates_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_SUBPARTITION_TEMPLATES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SUBPARTITION_TEMPLATES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            table_schema
                .set_view_definition(R"__(SELECT       TB.TABLE_NAME AS USER_NAME,       SP.SUB_PART_NAME AS SUBPARTITION_NAME,       SP.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,       TP.TABLESPACE_NAME AS TABLESPACE_NAME,       CAST(CASE WHEN SP.HIGH_BOUND_VAL is NULL THEN SP.LIST_VAL ELSE SP.HIGH_BOUND_VAL END AS VARCHAR2(1024)) HIGH_BOUND       FROM (SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB join SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB             on DB.database_id = TB.database_id               AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()               AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()             join SYS.ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT SP on tb.table_id = sp.table_id               AND SP.TENANT_ID = EFFECTIVE_TENANT_ID())             left join SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP               ON TP.TABLESPACE_ID = SP.TABLESPACE_ID                 AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()       WHERE DB.database_id = USERENV('SCHEMAID')       )__"))) {
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

int ObInnerTableSchema::dba_part_indexes_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_PART_INDEXES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PART_INDEXES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( with PARTITIONED_INDEXES as ( SELECT CASE when I.INDEX_TYPE IN (1, 2) then 1 else 0 end as IS_LOCAL, DB.DATABASE_NAME AS I_OWNER, CASE WHEN DB.DATABASE_NAME !=  '__recyclebin' THEN SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_')) ELSE I.TABLE_NAME END AS I_NAME, T.TABLE_NAME AS T_NAME, I.DATA_TABLE_ID AS T_ID, I.TABLE_ID AS I_ID, TP.TABLESPACE_NAME, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN I.DATA_TABLE_ID ELSE I.TABLE_ID END AS PART_INFO_T_ID, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_LEVEL ELSE I.PART_LEVEL END AS I_PART_LEVEL, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_FUNC_TYPE ELSE I.PART_FUNC_TYPE END AS I_PART_FUNC_TYPE, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_FUNC_TYPE ELSE I.SUB_PART_FUNC_TYPE END AS I_SUB_PART_FUNC_TYPE, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_NUM ELSE I.PART_NUM END AS I_PART_NUM, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_NUM ELSE I.SUB_PART_NUM END AS I_SUB_PART_NUM FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB ON I.DATABASE_ID = DB.DATABASE_ID   AND I.TENANT_ID = EFFECTIVE_TENANT_ID()   AND DB.TENANT_ID = EFFECTIVE_TENANT_ID() JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON I.DATA_TABLE_ID = T.TABLE_ID   AND T.TENANT_ID = EFFECTIVE_TENANT_ID() LEFT JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP ON  TP.TABLESPACE_ID = I.TABLESPACE_ID   AND TP.TENANT_ID = EFFECTIVE_TENANT_ID() WHERE I.TABLE_TYPE = 5 AND (I.INDEX_TYPE NOT IN (1, 2) and I.PART_LEVEL != 0  or  I.INDEX_TYPE IN (1, 2) and T.PART_LEVEL != 0) ), PART_KEY_COUNT as ( select PI.I_ID, SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 255) != 0 THEN 1 ELSE 0 END) AS PARTITIONING_KEY_COUNT, SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 65280)/256 != 0 THEN 1 ELSE 0 END) AS SUBPARTITIONING_KEY_COUNT from PARTITIONED_INDEXES PI join SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C on PI.PART_INFO_T_ID = C.TABLE_ID AND C.TENANT_ID = EFFECTIVE_TENANT_ID() group by I_ID ), LOCAL_INDEX_PREFIXED as ( select I.TABLE_ID AS I_ID, 1 AS IS_PREFIXED from SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I where I.TABLE_TYPE = 5 AND I.INDEX_TYPE in (1,2)   AND I.TENANT_ID = EFFECTIVE_TENANT_ID() and not exists (select * from ( select * from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C where C.TABLE_ID = I.DATA_TABLE_ID AND C.PARTITION_KEY_POSITION != 0 AND C.TENANT_ID = EFFECTIVE_TENANT_ID() ) PART_COLUMNS left join ( select * from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C WHERE C.TABLE_ID = I.TABLE_ID AND C.TENANT_ID = EFFECTIVE_TENANT_ID() AND C.INDEX_POSITION != 0 ) INDEX_COLUMNS ON PART_COLUMNS.COLUMN_ID = INDEX_COLUMNS.COLUMN_ID where (BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != 0 AND (INDEX_COLUMNS.INDEX_POSITION is null  or BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != INDEX_COLUMNS.INDEX_POSITION)) or (BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 65280)/256 != 0  AND (INDEX_COLUMNS.INDEX_POSITION is null)) ) ) SELECT CAST(PI.I_OWNER AS VARCHAR2(128)) AS OWNER, CAST(PI.I_NAME AS VARCHAR2(128)) AS INDEX_NAME, CAST(PI.T_NAME AS VARCHAR2(128)) AS TABLE_NAME,  CAST(CASE PI.I_PART_FUNC_TYPE WHEN 0 THEN 'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY' WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE' WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST' WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END AS VARCHAR2(9)) AS PARTITIONING_TYPE, CAST( CASE WHEN PI.I_PART_LEVEL < 2 THEN 'NONE' ELSE CASE PI.I_SUB_PART_FUNC_TYPE WHEN 0 THEN 'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY' WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE' WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST' WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END END AS VARCHAR2(9)) AS SUBPARTITIONING_TYPE,  CAST(PI.I_PART_NUM AS NUMBER) AS PARTITION_COUNT, CAST(CASE WHEN PI.I_PART_LEVEL < 2 THEN 0 ELSE PI.I_SUB_PART_NUM END AS NUMBER) AS DEF_SUBPARTITION_COUNT, CAST(PKC.PARTITIONING_KEY_COUNT AS NUMBER) AS PARTITIONING_KEY_COUNT, CAST(PKC.SUBPARTITIONING_KEY_COUNT AS NUMBER) AS SUBPARTITIONING_KEY_COUNT,  CAST(CASE WHEN PI.IS_LOCAL = 1 THEN 'LOCAL' ELSE 'GLOBAL' END AS VARCHAR2(6)) AS LOCALITY,  CAST(CASE WHEN (PI.IS_LOCAL = 0 or (PI.IS_LOCAL = 1 and LIP.IS_PREFIXED = 1)) THEN 'PREFIXED' ELSE 'NON_PREFIXED' END AS VARCHAR2(12)) AS ALIGNMENT,  CAST(PI.TABLESPACE_NAME AS VARCHAR2(30)) AS DEF_TABLESPACE_NAME, CAST(0 AS NUMBER) AS DEF_PCT_FREE, CAST(0 AS NUMBER) AS DEF_INI_TRANS, CAST(0 AS NUMBER) AS DEF_MAX_TRANS, CAST(NULL AS VARCHAR2(40)) AS DEF_INITIAL_EXTENT, CAST(NULL AS VARCHAR2(40)) AS DEF_NEXT_EXTENT, CAST(NULL AS VARCHAR2(40)) AS DEF_MIN_EXTENTS, CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_EXTENTS, CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_SIZE, CAST(NULL AS VARCHAR2(40)) AS DEF_PCT_INCREASE, CAST(0 AS NUMBER) AS DEF_FREELISTS, CAST(0 AS NUMBER) AS DEF_FREELIST_GROUPS, CAST(NULL AS VARCHAR2(7)) AS DEF_LOGGING, CAST(NULL AS VARCHAR2(7)) AS DEF_BUFFER_POOL, CAST(NULL AS VARCHAR2(7)) AS DEF_FLASH_CACHE, CAST(NULL AS VARCHAR2(7)) AS DEF_CELL_FLASH_CACHE, CAST(NULL AS VARCHAR2(1000)) AS DEF_PARAMETERS, CAST(NULL AS VARCHAR2(1000)) AS INTERVAL  from PARTITIONED_INDEXES PI join PART_KEY_COUNT PKC on PI.I_ID = PKC.I_ID left join LOCAL_INDEX_PREFIXED LIP on PI.I_ID = LIP.I_ID     )__"))) {
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

int ObInnerTableSchema::all_part_indexes_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PART_INDEXES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_INDEXES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            table_schema
                .set_view_definition(R"__( with PARTITIONED_INDEXES as ( SELECT CASE when I.INDEX_TYPE IN (1, 2) then 1 else 0 end as IS_LOCAL, DB.DATABASE_NAME AS I_OWNER, CASE WHEN DB.DATABASE_NAME !=  '__recyclebin' THEN SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_')) ELSE I.TABLE_NAME END AS I_NAME, T.TABLE_NAME AS T_NAME, I.DATA_TABLE_ID AS T_ID, I.TABLE_ID AS I_ID, TP.TABLESPACE_NAME, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN I.DATA_TABLE_ID ELSE I.TABLE_ID END AS PART_INFO_T_ID, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_LEVEL ELSE I.PART_LEVEL END AS I_PART_LEVEL, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_FUNC_TYPE ELSE I.PART_FUNC_TYPE END AS I_PART_FUNC_TYPE, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_FUNC_TYPE ELSE I.SUB_PART_FUNC_TYPE END AS I_SUB_PART_FUNC_TYPE, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_NUM ELSE I.PART_NUM END AS I_PART_NUM, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_NUM ELSE I.SUB_PART_NUM END AS I_SUB_PART_NUM FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB ON I.DATABASE_ID = DB.DATABASE_ID   AND I.TENANT_ID = EFFECTIVE_TENANT_ID()   AND DB.TENANT_ID = EFFECTIVE_TENANT_ID() JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON I.DATA_TABLE_ID = T.TABLE_ID                                      AND T.TENANT_ID = EFFECTIVE_TENANT_ID()                                      AND (I.DATABASE_ID = USERENV('SCHEMAID')                                           OR USER_CAN_ACCESS_OBJ(1, I.DATA_TABLE_ID, 1) = 1) LEFT JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP ON  TP.TABLESPACE_ID = I.TABLESPACE_ID AND TP.TENANT_ID = EFFECTIVE_TENANT_ID() WHERE I.TABLE_TYPE = 5 AND (I.INDEX_TYPE NOT IN (1, 2) and I.PART_LEVEL != 0  or  I.INDEX_TYPE IN (1, 2) and T.PART_LEVEL != 0) ), PART_KEY_COUNT as ( select PI.I_ID, SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 255) != 0 THEN 1 ELSE 0 END) AS PARTITIONING_KEY_COUNT, SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 65280)/256 != 0 THEN 1 ELSE 0 END) AS SUBPARTITIONING_KEY_COUNT from PARTITIONED_INDEXES PI join SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C on PI.PART_INFO_T_ID = C.TABLE_ID AND C.TENANT_ID = EFFECTIVE_TENANT_ID() group by I_ID ), LOCAL_INDEX_PREFIXED as ( select I.TABLE_ID AS I_ID, 1 AS IS_PREFIXED from SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I where I.TABLE_TYPE = 5 AND I.INDEX_TYPE in (1,2) AND I.TENANT_ID = EFFECTIVE_TENANT_ID() AND (I.DATABASE_ID = USERENV('SCHEMAID')     OR USER_CAN_ACCESS_OBJ(1, I.DATA_TABLE_ID, I.DATABASE_ID) = 1) and not exists (select * from ( select * from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C where C.TABLE_ID = I.DATA_TABLE_ID AND C.PARTITION_KEY_POSITION != 0 AND C.TENANT_ID = EFFECTIVE_TENANT_ID() ) PART_COLUMNS left join ( select * from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C WHERE C.TABLE_ID = I.TABLE_ID AND C.INDEX_POSITION != 0 AND C.TENANT_ID = EFFECTIVE_TENANT_ID() ) INDEX_COLUMNS ON PART_COLUMNS.COLUMN_ID = INDEX_COLUMNS.COLUMN_ID where (BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != 0 AND (INDEX_COLUMNS.INDEX_POSITION is null  or BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != INDEX_COLUMNS.INDEX_POSITION)) or (BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 65280)/256 != 0  AND (INDEX_COLUMNS.INDEX_POSITION is null)) ) ) SELECT CAST(PI.I_OWNER AS VARCHAR2(128)) AS OWNER, CAST(PI.I_NAME AS VARCHAR2(128)) AS INDEX_NAME, CAST(PI.T_NAME AS VARCHAR2(128)) AS TABLE_NAME,  CAST(CASE PI.I_PART_FUNC_TYPE WHEN 0 THEN 'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY' WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE' WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST' WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END AS VARCHAR2(9)) AS PARTITIONING_TYPE,  CAST( CASE WHEN PI.I_PART_LEVEL < 2 THEN 'NONE' ELSE CASE PI.I_SUB_PART_FUNC_TYPE WHEN 0 THEN 'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY' WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE' WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST' WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END END AS VARCHAR2(9)) AS SUBPARTITIONING_TYPE,  CAST(PI.I_PART_NUM AS NUMBER) AS PARTITION_COUNT, CAST(CASE WHEN PI.I_PART_LEVEL < 2 THEN 0 ELSE PI.I_SUB_PART_NUM END AS NUMBER) AS DEF_SUBPARTITION_COUNT, CAST(PKC.PARTITIONING_KEY_COUNT AS NUMBER) AS PARTITIONING_KEY_COUNT, CAST(PKC.SUBPARTITIONING_KEY_COUNT AS NUMBER) AS SUBPARTITIONING_KEY_COUNT,  CAST(CASE WHEN PI.IS_LOCAL = 1 THEN 'LOCAL' ELSE 'GLOBAL' END AS VARCHAR2(6)) AS LOCALITY,  CAST(CASE WHEN (PI.IS_LOCAL = 0 or (PI.IS_LOCAL = 1 and LIP.IS_PREFIXED = 1)) THEN 'PREFIXED' ELSE 'NON_PREFIXED' END AS VARCHAR2(12)) AS ALIGNMENT,  CAST(PI.TABLESPACE_NAME AS VARCHAR2(30)) AS DEF_TABLESPACE_NAME, CAST(0 AS NUMBER) AS DEF_PCT_FREE, CAST(0 AS NUMBER) AS DEF_INI_TRANS, CAST(0 AS NUMBER) AS DEF_MAX_TRANS, CAST(NULL AS VARCHAR2(40)) AS DEF_INITIAL_EXTENT, CAST(NULL AS VARCHAR2(40)) AS DEF_NEXT_EXTENT, CAST(NULL AS VARCHAR2(40)) AS DEF_MIN_EXTENTS, CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_EXTENTS, CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_SIZE, CAST(NULL AS VARCHAR2(40)) AS DEF_PCT_INCREASE, CAST(0 AS NUMBER) AS DEF_FREELISTS, CAST(0 AS NUMBER) AS DEF_FREELIST_GROUPS, CAST(NULL AS VARCHAR2(7)) AS DEF_LOGGING, CAST(NULL AS VARCHAR2(7)) AS DEF_BUFFER_POOL, CAST(NULL AS VARCHAR2(7)) AS DEF_FLASH_CACHE, CAST(NULL AS VARCHAR2(7)) AS DEF_CELL_FLASH_CACHE, CAST(NULL AS VARCHAR2(1000)) AS DEF_PARAMETERS, CAST(NULL AS VARCHAR2(1000)) AS INTERVAL  from PARTITIONED_INDEXES PI join PART_KEY_COUNT PKC on PI.I_ID = PKC.I_ID left join LOCAL_INDEX_PREFIXED LIP on PI.I_ID = LIP.I_ID     )__"))) {
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

int ObInnerTableSchema::user_part_indexes_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_PART_INDEXES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PART_INDEXES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( with PARTITIONED_INDEXES as ( SELECT CASE when I.INDEX_TYPE IN (1, 2) then 1 else 0 end as IS_LOCAL, DB.DATABASE_NAME AS I_OWNER, CASE WHEN DB.DATABASE_NAME !=  '__recyclebin' THEN SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_')) ELSE I.TABLE_NAME END AS I_NAME, T.TABLE_NAME AS T_NAME, I.DATA_TABLE_ID AS T_ID, I.TABLE_ID AS I_ID, TP.TABLESPACE_NAME, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN I.DATA_TABLE_ID ELSE I.TABLE_ID END AS PART_INFO_T_ID, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_LEVEL ELSE I.PART_LEVEL END AS I_PART_LEVEL, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_FUNC_TYPE ELSE I.PART_FUNC_TYPE END AS I_PART_FUNC_TYPE, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_FUNC_TYPE ELSE I.SUB_PART_FUNC_TYPE END AS I_SUB_PART_FUNC_TYPE, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_NUM ELSE I.PART_NUM END AS I_PART_NUM, CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_NUM ELSE I.SUB_PART_NUM END AS I_SUB_PART_NUM FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB ON I.DATABASE_ID = DB.DATABASE_ID   AND I.TENANT_ID = EFFECTIVE_TENANT_ID()   AND DB.TENANT_ID = EFFECTIVE_TENANT_ID() AND I.DATABASE_ID = USERENV('SCHEMAID') JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON I.DATA_TABLE_ID = T.TABLE_ID   AND T.TENANT_ID = EFFECTIVE_TENANT_ID() LEFT JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP ON  TP.TABLESPACE_ID = I.TABLESPACE_ID AND TP.TENANT_ID = EFFECTIVE_TENANT_ID() WHERE I.TABLE_TYPE = 5 AND (I.INDEX_TYPE NOT IN (1, 2) and I.PART_LEVEL != 0  or  I.INDEX_TYPE IN (1, 2) and T.PART_LEVEL != 0) ), PART_KEY_COUNT as ( select PI.I_ID, SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 255) != 0 THEN 1 ELSE 0 END) AS PARTITIONING_KEY_COUNT, SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 65280)/256 != 0 THEN 1 ELSE 0 END) AS SUBPARTITIONING_KEY_COUNT from PARTITIONED_INDEXES PI join SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C on PI.PART_INFO_T_ID = C.TABLE_ID AND C.TENANT_ID = EFFECTIVE_TENANT_ID() group by I_ID ), LOCAL_INDEX_PREFIXED as ( select I.TABLE_ID AS I_ID, 1 AS IS_PREFIXED from SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I where I.TABLE_TYPE = 5 AND I.INDEX_TYPE in (1,2) AND I.TENANT_ID = EFFECTIVE_TENANT_ID() and not exists (select * from ( select * from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C where C.TABLE_ID = I.DATA_TABLE_ID AND C.PARTITION_KEY_POSITION != 0 AND C.TENANT_ID = EFFECTIVE_TENANT_ID() ) PART_COLUMNS left join ( select * from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C WHERE C.TABLE_ID = I.TABLE_ID AND C.INDEX_POSITION != 0 AND C.TENANT_ID = EFFECTIVE_TENANT_ID() ) INDEX_COLUMNS ON PART_COLUMNS.COLUMN_ID = INDEX_COLUMNS.COLUMN_ID where (BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != 0 AND (INDEX_COLUMNS.INDEX_POSITION is null  or BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != INDEX_COLUMNS.INDEX_POSITION)) or (BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 65280)/256 != 0  AND (INDEX_COLUMNS.INDEX_POSITION is null)) ) ) SELECT CAST(PI.I_NAME AS VARCHAR2(128)) AS INDEX_NAME, CAST(PI.T_NAME AS VARCHAR2(128)) AS TABLE_NAME,  CAST(CASE PI.I_PART_FUNC_TYPE WHEN 0 THEN 'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY' WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE' WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST' WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END AS VARCHAR2(9)) AS PARTITIONING_TYPE,  CAST( CASE WHEN PI.I_PART_LEVEL < 2 THEN 'NONE' ELSE CASE PI.I_SUB_PART_FUNC_TYPE WHEN 0 THEN 'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY' WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE' WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST' WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END END AS VARCHAR2(9)) AS SUBPARTITIONING_TYPE,  CAST(PI.I_PART_NUM AS NUMBER) AS PARTITION_COUNT, CAST(CASE WHEN PI.I_PART_LEVEL < 2 THEN 0 ELSE PI.I_SUB_PART_NUM END AS NUMBER) AS DEF_SUBPARTITION_COUNT, CAST(PKC.PARTITIONING_KEY_COUNT AS NUMBER) AS PARTITIONING_KEY_COUNT, CAST(PKC.SUBPARTITIONING_KEY_COUNT AS NUMBER) AS SUBPARTITIONING_KEY_COUNT,  CAST(CASE WHEN PI.IS_LOCAL = 1 THEN 'LOCAL' ELSE 'GLOBAL' END AS VARCHAR2(6)) AS LOCALITY,  CAST(CASE WHEN (PI.IS_LOCAL = 0 or (PI.IS_LOCAL = 1 and LIP.IS_PREFIXED = 1)) THEN 'PREFIXED' ELSE 'NON_PREFIXED' END AS VARCHAR2(12)) AS ALIGNMENT,  CAST(PI.TABLESPACE_NAME AS VARCHAR2(30)) AS DEF_TABLESPACE_NAME, CAST(0 AS NUMBER) AS DEF_PCT_FREE, CAST(0 AS NUMBER) AS DEF_INI_TRANS, CAST(0 AS NUMBER) AS DEF_MAX_TRANS, CAST(NULL AS VARCHAR2(40)) AS DEF_INITIAL_EXTENT, CAST(NULL AS VARCHAR2(40)) AS DEF_NEXT_EXTENT, CAST(NULL AS VARCHAR2(40)) AS DEF_MIN_EXTENTS, CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_EXTENTS, CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_SIZE, CAST(NULL AS VARCHAR2(40)) AS DEF_PCT_INCREASE, CAST(0 AS NUMBER) AS DEF_FREELISTS, CAST(0 AS NUMBER) AS DEF_FREELIST_GROUPS, CAST(NULL AS VARCHAR2(7)) AS DEF_LOGGING, CAST(NULL AS VARCHAR2(7)) AS DEF_BUFFER_POOL, CAST(NULL AS VARCHAR2(7)) AS DEF_FLASH_CACHE, CAST(NULL AS VARCHAR2(7)) AS DEF_CELL_FLASH_CACHE, CAST(NULL AS VARCHAR2(1000)) AS DEF_PARAMETERS, CAST(NULL AS VARCHAR2(1000)) AS INTERVAL  from PARTITIONED_INDEXES PI join PART_KEY_COUNT PKC on PI.I_ID = PKC.I_ID left join LOCAL_INDEX_PREFIXED LIP on PI.I_ID = LIP.I_ID     )__"))) {
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

int ObInnerTableSchema::all_all_tables_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ALL_TABLES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ALL_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(db.database_name AS VARCHAR2(128)) AS OWNER,   CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,   CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,   CAST(NULL AS VARCHAR2(30)) AS CLUSTER_NAME,   CAST(NULL AS VARCHAR2(30)) AS IOT_NAME,   CAST('VALID' AS VARCHAR2(8)) AS STATUS,   CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,   CAST(NULL AS NUMBER) AS PCT_USED,   CAST(NULL AS NUMBER) AS INI_TRANS,   CAST(NULL AS NUMBER) AS MAX_TRANS,   CAST(NULL AS NUMBER) AS INITIAL_EXTENT,   CAST(NULL AS NUMBER) AS NEXT_EXTENT,   CAST(NULL AS NUMBER) AS MIN_EXTENTS,   CAST(NULL AS NUMBER) AS MAX_EXTENTS,   CAST(NULL AS NUMBER) AS PCT_INCREASE,   CAST(NULL AS NUMBER) AS FREELISTS,   CAST(NULL AS NUMBER) AS FREELIST_GROUPS,   CAST(NULL AS VARCHAR2(3)) AS LOGGING,   CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,   CAST(info.row_count AS NUMBER) AS NUM_ROWS,   CAST(NULL AS NUMBER) AS BLOCKS,   CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,   CAST(NULL AS NUMBER) AS AVG_SPACE,   CAST(NULL AS NUMBER) AS CHAIN_CNT,   CAST(NULL AS NUMBER) AS AVG_ROW_LEN,   CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,   CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,   CAST(NULL AS VARCHAR2(40)) AS DEGREE,   CAST(NULL AS VARCHAR2(40)) AS INSTANCES,   CAST(NULL AS VARCHAR2(20)) AS CACHE,   CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,   CAST(NULL AS NUMBER) AS SAMPLE_SIZE,   CAST(NULL AS DATE) AS LAST_ANALYZED,   CAST(   CASE     WHEN       t.part_level = 0     THEN       'NO'     ELSE       'YES'   END   AS VARCHAR2(3)) AS PARTITIONED,   CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,   CAST(NULL AS VARCHAR2(16)) AS OBJECT_ID_TYPE,   CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE_OWNER,   CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE,   CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,   CAST(NULL AS VARCHAR2(1)) AS SECONDARY,   CAST(NULL AS VARCHAR2(3)) AS NESTED,   CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,   CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,   CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,   CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,   CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,   CAST(NULL AS VARCHAR2(3)) AS USER_STATS,   CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,   CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,   CAST(NULL AS VARCHAR2(3)) AS MONITORING,   CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,   CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,   CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,   CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,   CAST(   CASE     WHEN       db.database_name =  '__recyclebin'     THEN 'YES'     ELSE       'NO'   END   AS VARCHAR2(3)) AS DROPPED,   CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,   CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,   CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE FROM   (     SELECT       tenant_id,       table_id,       SUM(row_count) AS row_count     FROM       sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p     WHERE       p.role = 1       AND P.TENANT_ID = EFFECTIVE_TENANT_ID()     GROUP BY       tenant_id,       table_id   )   info   RIGHT JOIN     sys.ALL_VIRTUAL_TABLE_REAL_AGENT t     ON t.tenant_id = info.tenant_id     AND t.table_id = info.table_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID(),     sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db  WHERE     db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND db.database_name != '__recyclebin'     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     AND t.table_type != 5     AND (t.database_id = USERENV('SCHEMAID')          or user_can_access_obj(1, t.table_id, t.database_id) =1     ) )__"))) {
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

int ObInnerTableSchema::dba_all_tables_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_ALL_TABLES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_ALL_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(db.database_name AS VARCHAR2(128)) AS OWNER,   CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,   CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,   CAST(NULL AS VARCHAR2(30)) AS CLUSTER_NAME,   CAST(NULL AS VARCHAR2(30)) AS IOT_NAME,   CAST('VALID' AS VARCHAR2(8)) AS STATUS,   CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,   CAST(NULL AS NUMBER) AS PCT_USED,   CAST(NULL AS NUMBER) AS INI_TRANS,   CAST(NULL AS NUMBER) AS MAX_TRANS,   CAST(NULL AS NUMBER) AS INITIAL_EXTENT,   CAST(NULL AS NUMBER) AS NEXT_EXTENT,   CAST(NULL AS NUMBER) AS MIN_EXTENTS,   CAST(NULL AS NUMBER) AS MAX_EXTENTS,   CAST(NULL AS NUMBER) AS PCT_INCREASE,   CAST(NULL AS NUMBER) AS FREELISTS,   CAST(NULL AS NUMBER) AS FREELIST_GROUPS,   CAST(NULL AS VARCHAR2(3)) AS LOGGING,   CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,   CAST(info.row_count AS NUMBER) AS NUM_ROWS,   CAST(NULL AS NUMBER) AS BLOCKS,   CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,   CAST(NULL AS NUMBER) AS AVG_SPACE,   CAST(NULL AS NUMBER) AS CHAIN_CNT,   CAST(NULL AS NUMBER) AS AVG_ROW_LEN,   CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,   CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,   CAST(NULL AS VARCHAR2(40)) AS DEGREE,   CAST(NULL AS VARCHAR2(40)) AS INSTANCES,   CAST(NULL AS VARCHAR2(20)) AS CACHE,   CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,   CAST(NULL AS NUMBER) AS SAMPLE_SIZE,   CAST(NULL AS DATE) AS LAST_ANALYZED,   CAST(   CASE     WHEN       t.part_level = 0     THEN       'NO'     ELSE       'YES'   END   AS VARCHAR2(3)) AS PARTITIONED,   CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,   CAST(NULL AS VARCHAR2(16)) AS OBJECT_ID_TYPE,   CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE_OWNER,   CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE,   CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,   CAST(NULL AS VARCHAR2(1)) AS SECONDARY,   CAST(NULL AS VARCHAR2(3)) AS NESTED,   CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,   CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,   CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,   CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,   CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,   CAST(NULL AS VARCHAR2(3)) AS USER_STATS,   CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,   CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,   CAST(NULL AS VARCHAR2(3)) AS MONITORING,   CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,   CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,   CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,   CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,   CAST(   CASE     WHEN       db.database_name =  '__recyclebin'     THEN 'YES'     ELSE       'NO'   END   AS VARCHAR2(3)) AS DROPPED,   CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,   CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,   CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE FROM   (     SELECT       tenant_id,       table_id,       SUM(row_count) AS row_count     FROM       sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p     WHERE       p.role = 1       AND P.TENANT_ID = EFFECTIVE_TENANT_ID()     GROUP BY       tenant_id,       table_id   )   info   RIGHT JOIN     sys.ALL_VIRTUAL_TABLE_REAL_AGENT t     ON t.tenant_id = info.tenant_id     AND t.table_id = info.table_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID(),     sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db  WHERE     db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND db.database_name != '__recyclebin'     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     AND t.table_type != 5 )__"))) {
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

int ObInnerTableSchema::user_all_tables_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_ALL_TABLES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_ALL_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,   CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,   CAST(NULL AS VARCHAR2(30)) AS CLUSTER_NAME,   CAST(NULL AS VARCHAR2(30)) AS IOT_NAME,   CAST('VALID' AS VARCHAR2(8)) AS STATUS,   CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,   CAST(NULL AS NUMBER) AS PCT_USED,   CAST(NULL AS NUMBER) AS INI_TRANS,   CAST(NULL AS NUMBER) AS MAX_TRANS,   CAST(NULL AS NUMBER) AS INITIAL_EXTENT,   CAST(NULL AS NUMBER) AS NEXT_EXTENT,   CAST(NULL AS NUMBER) AS MIN_EXTENTS,   CAST(NULL AS NUMBER) AS MAX_EXTENTS,   CAST(NULL AS NUMBER) AS PCT_INCREASE,   CAST(NULL AS NUMBER) AS FREELISTS,   CAST(NULL AS NUMBER) AS FREELIST_GROUPS,   CAST(NULL AS VARCHAR2(3)) AS LOGGING,   CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,   CAST(info.row_count AS NUMBER) AS NUM_ROWS,   CAST(NULL AS NUMBER) AS BLOCKS,   CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,   CAST(NULL AS NUMBER) AS AVG_SPACE,   CAST(NULL AS NUMBER) AS CHAIN_CNT,   CAST(NULL AS NUMBER) AS AVG_ROW_LEN,   CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,   CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,   CAST(NULL AS VARCHAR2(40)) AS DEGREE,   CAST(NULL AS VARCHAR2(40)) AS INSTANCES,   CAST(NULL AS VARCHAR2(20)) AS CACHE,   CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,   CAST(NULL AS NUMBER) AS SAMPLE_SIZE,   CAST(NULL AS DATE) AS LAST_ANALYZED,   CAST(   CASE     WHEN       t.part_level = 0     THEN       'NO'     ELSE       'YES'   END   AS VARCHAR2(3)) AS PARTITIONED,   CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,   CAST(NULL AS VARCHAR2(16)) AS OBJECT_ID_TYPE,   CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE_OWNER,   CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE,   CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,   CAST(NULL AS VARCHAR2(1)) AS SECONDARY,   CAST(NULL AS VARCHAR2(3)) AS NESTED,   CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,   CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,   CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,   CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,   CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,   CAST(NULL AS VARCHAR2(3)) AS USER_STATS,   CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,   CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,   CAST(NULL AS VARCHAR2(3)) AS MONITORING,   CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,   CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,   CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,   CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,   CAST(   CASE     WHEN       db.database_name =  '__recyclebin'     THEN 'YES'     ELSE       'NO'   END   AS VARCHAR2(3)) AS DROPPED,   CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,   CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,   CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE FROM   (     SELECT       tenant_id,       table_id,       SUM(row_count) AS row_count     FROM       sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p     WHERE       p.role = 1       AND P.TENANT_ID = EFFECTIVE_TENANT_ID()     GROUP BY       tenant_id,       table_id   )   info   RIGHT JOIN     sys.ALL_VIRTUAL_TABLE_REAL_AGENT t     ON t.tenant_id = info.tenant_id     AND t.table_id = info.table_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID(),     sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db WHERE     db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND t.database_id = USERENV('SCHEMAID')     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     AND t.table_type != 5 )__"))) {
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

int ObInnerTableSchema::dba_profiles_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_PROFILES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PROFILES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   PROFILE,   RESOURCE_NAME,   RESOURCE_TYPE,   LIMIT FROM   (SELECT     PROFILE_NAME AS PROFILE,     CAST('FAILED_LOGIN_ATTEMPTS' AS VARCHAR2(32)) AS RESOURCE_NAME,     CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,     CAST(DECODE(FAILED_LOGIN_ATTEMPTS, -1, 'UNLIMITED',       9223372036854775807, 'UNLIMITED',       10, 'DEFAULT',       FAILED_LOGIN_ATTEMPTS) AS VARCHAR2(128)) AS LIMIT   FROM     sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT   UNION ALL   SELECT     PROFILE_NAME AS PROFILE,     CAST('PASSWORD_LOCK_TIME' AS VARCHAR2(32)) AS RESOURCE_NAME,     CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,     CAST(DECODE(PASSWORD_LOCK_TIME, -1, 'UNLIMITED',       9223372036854775807, 'UNLIMITED',       86400000000, 'DEFAULT',       PASSWORD_LOCK_TIME) AS VARCHAR2(128)) AS LIMIT   FROM     sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT   UNION ALL   SELECT     PROFILE_NAME AS PROFILE,     CAST('PASSWORD_VERIFY_FUNCTION' AS VARCHAR2(32)) AS RESOURCE_NAME,     CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,     CAST(DECODE(PASSWORD_VERIFY_FUNCTION, NULL, 'NULL',       PASSWORD_VERIFY_FUNCTION) AS VARCHAR2(128)) AS LIMIT   FROM     sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT   UNION ALL   SELECT     PROFILE_NAME AS PROFILE,     CAST('PASSWORD_LIFE_TIME' AS VARCHAR2(32)) AS RESOURCE_NAME,     CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,     CAST(DECODE(PASSWORD_LIFE_TIME, -1, 'UNLIMITED',       9223372036854775807, 'UNLIMITED',       15552000000000, 'DEFAULT',       PASSWORD_LIFE_TIME) AS VARCHAR2(128)) AS LIMIT   FROM     sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT   UNION ALL   SELECT     PROFILE_NAME AS PROFILE,     CAST('PASSWORD_GRACE_TIME' AS VARCHAR2(32)) AS RESOURCE_NAME,     CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,     CAST(DECODE(PASSWORD_GRACE_TIME, -1, 'UNLIMITED',       9223372036854775807, 'UNLIMITED',       604800000000, 'DEFAULT',       PASSWORD_GRACE_TIME) AS VARCHAR2(128)) AS LIMIT   FROM     sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT) ORDER BY PROFILE, RESOURCE_NAME )__"))) {
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

int ObInnerTableSchema::user_profiles_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_PROFILES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PROFILES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(t.profile_name AS VARCHAR2(30)) AS PROFILE,   CAST(NULL AS VARCHAR2(32)) AS RESOURCE_NAME,   CAST(NULL AS VARCHAR2(8)) AS RESOURCE_TYPE,   CAST(NULL AS VARCHAR2(40)) AS LIMIT_ON_RESOURCE FROM   SYS.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT t   WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_profiles_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PROFILES_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PROFILES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(t.profile_name AS VARCHAR2(30)) AS PROFILE,   CAST(NULL AS VARCHAR2(32)) AS RESOURCE_NAME,   CAST(NULL AS VARCHAR2(8)) AS RESOURCE_TYPE,   CAST(NULL AS VARCHAR2(40)) AS LIMIT_ON_RESOURCE FROM   SYS.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT t   WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_mview_comments_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_MVIEW_COMMENTS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_MVIEW_COMMENTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   db.DATABASE_NAME AS OWNER,   CAST(t.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,   CAST(t."COMMENT" AS VARCHAR(4000)) AS COMMENTS FROM   SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t WHERE     db.DATABASE_ID = t.DATABASE_ID     AND t.TABLE_TYPE = 7     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     AND T.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::user_mview_comments_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_MVIEW_COMMENTS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVIEW_COMMENTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   db.DATABASE_NAME AS OWNER,   CAST(t.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,   CAST(t."COMMENT" AS VARCHAR(4000)) AS COMMENTS FROM   SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t WHERE     db.DATABASE_ID = t.DATABASE_ID     AND t.TABLE_TYPE = 7     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND db.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::dba_mview_comments_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_MVIEW_COMMENTS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_MVIEW_COMMENTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   db.DATABASE_NAME AS OWNER,   CAST(t.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,   CAST(t."COMMENT" AS VARCHAR(4000)) AS COMMENTS FROM   SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t WHERE     db.DATABASE_ID = t.DATABASE_ID     AND t.TABLE_TYPE = 7     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     AND T.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::all_scheduler_program_args_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SCHEDULER_PROGRAM_ARGS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SCHEDULER_PROGRAM_ARGS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS PROGRAM_NAME,   CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,   CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,   CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,   CAST(NULL AS VARCHAR2(19)) AS METADATA_ATTRIBUTE,   CAST(NULL AS VARCHAR2(4000)) AS DEFAULT_VALUE,   CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,   CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::dba_scheduler_program_args_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_SCHEDULER_PROGRAM_ARGS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SCHEDULER_PROGRAM_ARGS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS PROGRAM_NAME,   CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,   CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,   CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,   CAST(NULL AS VARCHAR2(19)) AS METADATA_ATTRIBUTE,   CAST(NULL AS VARCHAR2(4000)) AS DEFAULT_VALUE,   CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,   CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::user_scheduler_program_args_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_SCHEDULER_PROGRAM_ARGS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SCHEDULER_PROGRAM_ARGS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS PROGRAM_NAME,   CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,   CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,   CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,   CAST(NULL AS VARCHAR2(19)) AS METADATA_ATTRIBUTE,   CAST(NULL AS VARCHAR2(4000)) AS DEFAULT_VALUE,   CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,   CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::all_scheduler_job_args_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SCHEDULER_JOB_ARGS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SCHEDULER_JOB_ARGS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS JOB_NAME,   CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,   CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,   CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,   CAST(NULL AS VARCHAR2(4000)) AS VALUE,   CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,   CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT FROM   DUAL WHERE   1 = 0 )__"))) {
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

int ObInnerTableSchema::dba_scheduler_job_args_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DBA_SCHEDULER_JOB_ARGS_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SCHEDULER_JOB_ARGS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
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
            R"__( SELECT   CAST(NULL AS VARCHAR2(30)) AS OWNER,   CAST(NULL AS VARCHAR2(30)) AS JOB_NAME,   CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,   CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,   CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,   CAST(NULL AS VARCHAR2(4000)) AS VALUE,   CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,   CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT FROM   DUAL WHERE   1 = 0 )__"))) {
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
