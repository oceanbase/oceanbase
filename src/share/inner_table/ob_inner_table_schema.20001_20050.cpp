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

int ObInnerTableSchema::gv_ob_plan_cache_stat_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_PLAN_CACHE_STAT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_PLAN_CACHE_STAT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,SVR_IP,SVR_PORT,SQL_NUM,MEM_USED,MEM_HOLD,ACCESS_COUNT,   HIT_COUNT,HIT_RATE,PLAN_NUM,MEM_LIMIT,HASH_BUCKET,STMTKEY_NUM   FROM oceanbase.__all_virtual_plan_cache_stat )__"))) {
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

int ObInnerTableSchema::gv_ob_plan_cache_plan_stat_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_PLAN_CACHE_PLAN_STAT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_PLAN_CACHE_PLAN_STAT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT TENANT_ID,SVR_IP,SVR_PORT,PLAN_ID,SQL_ID,TYPE,IS_BIND_SENSITIVE,IS_BIND_AWARE,     DB_ID,STATEMENT,QUERY_SQL,SPECIAL_PARAMS,PARAM_INFOS, SYS_VARS, CONFIGS, PLAN_HASH,     FIRST_LOAD_TIME,SCHEMA_VERSION,LAST_ACTIVE_TIME,AVG_EXE_USEC,SLOWEST_EXE_TIME,SLOWEST_EXE_USEC,     SLOW_COUNT,HIT_COUNT,PLAN_SIZE,EXECUTIONS,DISK_READS,DIRECT_WRITES,BUFFER_GETS,APPLICATION_WAIT_TIME,     CONCURRENCY_WAIT_TIME,USER_IO_WAIT_TIME,ROWS_PROCESSED,ELAPSED_TIME,CPU_TIME,LARGE_QUERYS,     DELAYED_LARGE_QUERYS,DELAYED_PX_QUERYS,OUTLINE_VERSION,OUTLINE_ID,OUTLINE_DATA,ACS_SEL_INFO,     TABLE_SCAN,EVOLUTION, EVO_EXECUTIONS, EVO_CPU_TIME, TIMEOUT_COUNT, PS_STMT_ID, SESSID,     TEMP_TABLES, IS_USE_JIT,OBJECT_TYPE,HINTS_INFO,HINTS_ALL_WORKED, PL_SCHEMA_ID,     IS_BATCHED_MULTI_STMT, RULE_NAME     FROM oceanbase.__all_virtual_plan_stat WHERE OBJECT_STATUS = 0 AND is_in_pc=true )__"))) {
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

int ObInnerTableSchema::schemata_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_SCHEMATA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_SCHEMATA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT 'def' AS CATALOG_NAME,          DATABASE_NAME collate utf8mb4_name_case AS SCHEMA_NAME,          b.charset AS DEFAULT_CHARACTER_SET_NAME,          b.collation AS DEFAULT_COLLATION_NAME,          CAST(NULL AS CHAR(512)) as SQL_PATH,          'NO' as DEFAULT_ENCRYPTION   FROM oceanbase.__all_database a inner join oceanbase.__tenant_virtual_collation b ON a.collation_type = b.collation_type   WHERE a.tenant_id = 0     and in_recyclebin = 0     and a.database_name not in ('__recyclebin', '__public')     and 0 = sys_privilege_check('db_acc', 0, a.database_name, '')   ORDER BY a.database_id )__"))) {
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

int ObInnerTableSchema::character_sets_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_CHARACTER_SETS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CHARACTER_SETS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CHARSET AS CHARACTER_SET_NAME, DEFAULT_COLLATION AS DEFAULT_COLLATE_NAME, DESCRIPTION, max_length AS MAXLEN FROM oceanbase.__tenant_virtual_charset )__"))) {
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

int ObInnerTableSchema::global_variables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_GLOBAL_VARIABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GLOBAL_VARIABLES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT `variable_name` as VARIABLE_NAME, `value` as VARIABLE_VALUE  FROM oceanbase.__tenant_virtual_global_variable )__"))) {
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

int ObInnerTableSchema::statistics_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_STATISTICS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_STATISTICS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST('def' AS             CHAR(512))    AS TABLE_CATALOG,          V.TABLE_SCHEMA collate utf8mb4_name_case AS TABLE_SCHEMA,          V.TABLE_NAME collate utf8mb4_name_case  AS TABLE_NAME,          CAST(V.NON_UNIQUE AS      SIGNED)       AS NON_UNIQUE,          V.INDEX_SCHEMA collate utf8mb4_name_case AS INDEX_SCHEMA,          V.INDEX_NAME collate utf8mb4_name_case  AS INDEX_NAME,          CAST(V.SEQ_IN_INDEX AS    UNSIGNED)     AS SEQ_IN_INDEX,          V.COLUMN_NAME                           AS COLUMN_NAME,          CAST('A' AS               CHAR(1))      AS COLLATION,          CAST(NULL AS              SIGNED)       AS CARDINALITY,          CAST(V.SUB_PART AS        SIGNED)       AS SUB_PART,          CAST(NULL AS              CHAR(10))     AS PACKED,          CAST(V.NULLABLE AS        CHAR(3))      AS NULLABLE,          CAST(V.INDEX_TYPE AS      CHAR(16))     AS INDEX_TYPE,          CAST(V.COMMENT AS         CHAR(16))     AS COMMENT,          CAST(V.INDEX_COMMENT AS   CHAR(1024))   AS INDEX_COMMENT,          CAST(V.IS_VISIBLE AS      CHAR(3))      AS IS_VISIBLE,          V.EXPRESSION                            AS EXPRESSION   FROM   (SELECT db.database_name                                              AS TABLE_SCHEMA,                  t.table_name                                                  AS TABLE_NAME,                  CASE WHEN i.index_type IN (2,4,8) THEN 0 ELSE 1 END           AS NON_UNIQUE,                  db.database_name                                              AS INDEX_SCHEMA,                  substr(i.table_name, 7 + instr(substr(i.table_name, 7), '_')) AS INDEX_NAME,                  c.index_position                                              AS SEQ_IN_INDEX,                  CASE WHEN d_col.column_name IS NOT NULL THEN d_col.column_name ELSE c.column_name END AS COLUMN_NAME,                  CASE WHEN d_col.column_name IS NOT NULL THEN c.data_length ELSE NULL END AS SUB_PART,                  CASE WHEN c.nullable = 1 THEN 'YES' ELSE '' END               AS NULLABLE,                  CASE WHEN i.index_type in (15, 18, 21) THEN 'FULLTEXT'                       WHEN i.index_using_type = 0 THEN 'BTREE'                       WHEN i.index_using_type = 1 THEN 'HASH'                       ELSE 'UNKOWN' END      AS INDEX_TYPE,                  CASE i.index_status                  WHEN 2 THEN 'VALID'                  WHEN 3 THEN 'CHECKING'                  WHEN 4 THEN 'INELEGIBLE'                  WHEN 5 THEN 'ERROR'                  ELSE 'UNUSABLE' END                                                  AS COMMENT,                  i.comment                                                     AS INDEX_COMMENT,                  CASE WHEN (i.index_attributes_set & 1) THEN 'NO' ELSE 'YES' END AS IS_VISIBLE,                  d_col2.cur_default_value_v2                                     AS EXPRESSION           FROM   oceanbase.__all_table i           JOIN   oceanbase.__all_table t           ON     i.data_table_id=t.table_id           AND    i.tenant_id = t.tenant_id           AND    i.database_id = t.database_id           AND    i.table_type = 5           AND    i.index_type NOT IN (13, 14, 16, 17, 19, 20, 22)           AND    i.table_mode >> 12 & 15 in (0,1)           AND    t.table_type in (0,3)           JOIN   oceanbase.__all_column c           ON     i.table_id=c.table_id           AND    i.tenant_id = c.tenant_id           AND    c.index_position > 0           JOIN   oceanbase.__all_database db           ON     i.tenant_id = db.tenant_id           AND    i.database_id = db.database_id           AND    db.in_recyclebin = 0           AND    db.database_name != '__recyclebin'           LEFT JOIN oceanbase.__all_column d_col           ON    i.data_table_id = d_col.table_id           AND   i.tenant_id = d_col.tenant_id           AND   (case when (c.is_hidden = 1 and substr(c.column_name, 1, 8) = '__substr') then                    substr(c.column_name, 8 + instr(substr(c.column_name, 8), '_')) else 0 end) = d_col.column_id           LEFT JOIN oceanbase.__all_column d_col2           ON    i.data_table_id = d_col2.table_id           AND   i.tenant_id = d_col2.tenant_id           AND   c.column_id = d_col2.column_id           AND   d_col2.cur_default_value_v2 is not null           AND   d_col2.is_hidden = 1           AND   (d_col2.column_flags & (0x1 << 0) = 1 or d_col2.column_flags & (0x1 << 1) = 1)           AND   substr(d_col2.column_name, 1, 6) = 'SYS_NC'         UNION ALL           SELECT  db.database_name  AS TABLE_SCHEMA,                   t.table_name      AS TABLE_NAME,                   0                 AS NON_UNIQUE,                   db.database_name  AS INDEX_SCHEMA,                   'PRIMARY'         AS INDEX_NAME,                   c.rowkey_position AS SEQ_IN_INDEX,                   c.column_name     AS COLUMN_NAME,                   NULL              AS SUB_PART,                   ''                AS NULLABLE,                   CASE WHEN t.index_using_type = 0 THEN 'BTREE' ELSE (                     CASE WHEN t.index_using_type = 1 THEN 'HASH' ELSE 'UNKOWN' END) END AS INDEX_TYPE,                   'VALID'          AS COMMENT,                   t.comment        AS INDEX_COMMENT,                   'YES'            AS IS_VISIBLE,                   NULL             AS EXPRESSION           FROM   oceanbase.__all_table t           JOIN   oceanbase.__all_column c           ON     t.table_id=c.table_id           AND    t.tenant_id = c.tenant_id           AND    c.rowkey_position > 0           AND    c.is_hidden = 0           AND    t.table_type in (0,3)           JOIN   oceanbase.__all_database db           ON     t.tenant_id = db.tenant_id           AND    t.database_id = db.database_id           AND    db.in_recyclebin = 0           AND    db.database_name != '__recyclebin'         UNION ALL           SELECT db.database_name                                           AS TABLE_SCHEMA,               t.table_name                                                  AS TABLE_NAME,               CASE WHEN i.index_type IN (2,4,8) THEN 0 ELSE 1 END           AS NON_UNIQUE,               db.database_name                                              AS INDEX_SCHEMA,               substr(i.table_name, 7 + instr(substr(i.table_name, 7), '_')) AS INDEX_NAME,               c.index_position                                              AS SEQ_IN_INDEX,               CASE WHEN d_col.column_name IS NOT NULL THEN d_col.column_name ELSE c.column_name END AS COLUMN_NAME,               CASE WHEN d_col.column_name IS NOT NULL THEN c.data_length ELSE NULL END AS SUB_PART,               CASE WHEN c.nullable = 1 THEN 'YES' ELSE '' END               AS NULLABLE,               CASE WHEN i.index_type in (15, 18, 21) THEN 'FULLTEXT'                    WHEN i.index_using_type = 0 THEN 'BTREE'                    WHEN i.index_using_type = 1 THEN 'HASH'                    ELSE 'UNKOWN' END      AS INDEX_TYPE,               CASE i.index_status               WHEN 2 THEN 'VALID'               WHEN 3 THEN 'CHECKING'               WHEN 4 THEN 'INELEGIBLE'               WHEN 5 THEN 'ERROR'               ELSE 'UNUSABLE' END                                           AS COMMENT,               i.comment                                                     AS INDEX_COMMENT,               CASE WHEN (i.index_attributes_set & 1) THEN 'NO' ELSE 'YES' END AS IS_VISIBLE,               d_col2.cur_default_value_v2                                   AS EXPRESSION           FROM   oceanbase.__ALL_VIRTUAL_CORE_ALL_TABLE i           JOIN   oceanbase.__ALL_VIRTUAL_CORE_ALL_TABLE t           ON     i.data_table_id=t.table_id           AND    i.tenant_id = t.tenant_id           AND    i.database_id = t.database_id           AND    i.table_type = 5           AND    i.index_type NOT IN (13, 14, 16, 17, 19, 20, 22)           AND    t.table_type in (0,3)           AND    t.tenant_id = EFFECTIVE_TENANT_ID()           JOIN   oceanbase.__ALL_VIRTUAL_CORE_COLUMN_TABLE c           ON     i.table_id=c.table_id           AND    i.tenant_id = c.tenant_id           AND    c.index_position > 0           JOIN   oceanbase.__all_database db           ON     i.database_id = db.database_id           LEFT JOIN oceanbase.__ALL_VIRTUAL_CORE_COLUMN_TABLE d_col           ON    i.data_table_id = d_col.table_id           AND   i.tenant_id = d_col.tenant_id           AND   (case when (c.is_hidden = 1 and substr(c.column_name, 1, 8) = '__substr') then                    substr(c.column_name, 8 + instr(substr(c.column_name, 8), '_')) else 0 end) = d_col.column_id           LEFT JOIN oceanbase.__ALL_VIRTUAL_CORE_COLUMN_TABLE d_col2           ON    i.data_table_id = d_col2.table_id           AND   i.tenant_id = d_col2.tenant_id           AND   c.column_id = d_col2.column_id           AND   d_col2.cur_default_value_v2 is not null           AND   d_col2.is_hidden = 1           AND   (d_col2.column_flags & (0x1 << 0) = 1 or d_col2.column_flags & (0x1 << 1) = 1)           AND   substr(d_col2.column_name, 1, 6) = 'SYS_NC'         UNION ALL           SELECT db.database_name  AS TABLE_SCHEMA,                   t.table_name      AS TABLE_NAME,                   0                 AS NON_UNIQUE,                   db.database_name  AS INDEX_SCHEMA,                   'PRIMARY'         AS INDEX_NAME,                   c.rowkey_position AS SEQ_IN_INDEX,                   c.column_name     AS COLUMN_NAME,                   NULL              AS SUB_PART,                   ''                AS NULLABLE,                   CASE WHEN t.index_using_type = 0 THEN 'BTREE' ELSE (                     CASE WHEN t.index_using_type = 1 THEN 'HASH' ELSE 'UNKOWN' END) END AS INDEX_TYPE,                   'VALID'          AS COMMENT,                   t.comment        AS INDEX_COMMENT,                   'YES'            AS IS_VISIBLE,                   NULL             AS EXPRESSION           FROM   oceanbase.__ALL_VIRTUAL_CORE_ALL_TABLE t           JOIN   oceanbase.__ALL_VIRTUAL_CORE_COLUMN_TABLE c           ON     t.table_id=c.table_id           AND    t.tenant_id = c.tenant_id           AND    t.tenant_id = EFFECTIVE_TENANT_ID()           AND    c.rowkey_position > 0           AND    c.is_hidden = 0           AND    t.table_type in (0,3)           JOIN   oceanbase.__all_database db           ON     t.database_id = db.database_id)V )__"))) {
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

int ObInnerTableSchema::views_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_VIEWS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_VIEWS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select                    cast('def' as CHAR(64)) AS TABLE_CATALOG,                    d.database_name collate utf8mb4_name_case as TABLE_SCHEMA,                    t.table_name collate utf8mb4_name_case as TABLE_NAME,                    t.view_definition as VIEW_DEFINITION,                    case t.view_check_option when 1 then 'LOCAL' when 2 then 'CASCADED' else 'NONE' end as CHECK_OPTION,                    case t.view_is_updatable when 1 then 'YES' else 'NO' end as IS_UPDATABLE,                    cast((case t.define_user_id                          when -1 then 'NONE'                          else concat(u.user_name, '@', u.host) end) as CHAR(288)) as DEFINER,                    cast('NONE' as CHAR(7)) AS SECURITY_TYPE,                    cast((case t.collation_type                          when 45 then 'utf8mb4'                          else 'NONE' end) as CHAR(64)) AS CHARACTER_SET_CLIENT,                    cast((case t.collation_type                          when 45 then 'utf8mb4_general_ci'                          else 'NONE' end) as CHAR(64)) AS COLLATION_CONNECTION                    from oceanbase.__all_table as t                    join oceanbase.__all_database as d                      on t.tenant_id = d.tenant_id and t.database_id = d.database_id                    left join oceanbase.__all_user as u                      on t.tenant_id = u.tenant_id and t.define_user_id = u.user_id and t.define_user_id != -1                    where t.tenant_id = 0                      and t.table_type in (1, 4)                      and t.table_mode >> 12 & 15 in (0,1)                      and d.in_recyclebin = 0                      and d.database_name != '__recyclebin'                      and d.database_name != 'information_schema'                      and d.database_name != 'oceanbase'                      and 0 = sys_privilege_check('table_acc', effective_tenant_id(), d.database_name, t.table_name) )__"))) {
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

int ObInnerTableSchema::tables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_TABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TABLES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(                     select /*+ leading(a) no_use_nl(ts)*/                     cast('def' as char(512)) as TABLE_CATALOG,                     cast(b.database_name as char(64)) collate utf8mb4_name_case as TABLE_SCHEMA,                     cast(a.table_name as char(64)) collate utf8mb4_name_case as TABLE_NAME,                     cast(case when (a.database_id = 201002 or a.table_type = 1) then 'SYSTEM VIEW'                          when a.table_type in (0, 2) then 'SYSTEM TABLE'                          when a.table_type = 4 then 'VIEW'                          when a.table_type = 14 then 'EXTERNAL TABLE'                          else 'BASE TABLE' end as char(64)) as TABLE_TYPE,                     cast(case when a.table_type in (0,3,5,6,7,11,12,13,15) then 'InnoDB'                         else 'MEMORY' end as char(64)) as ENGINE,                     cast(NULL as unsigned) as VERSION,                     cast(a.store_format as char(10)) as ROW_FORMAT,                     cast( coalesce(ts.row_cnt,0) as unsigned) as TABLE_ROWS,                     cast( coalesce(ts.avg_row_len,0) as unsigned) as AVG_ROW_LENGTH,                     cast( coalesce(ts.data_size,0) as unsigned) as DATA_LENGTH,                     cast(NULL as unsigned) as MAX_DATA_LENGTH,                     cast(NULL as unsigned) as INDEX_LENGTH,                     cast(NULL as unsigned) as DATA_FREE,                     cast(NULL as unsigned) as AUTO_INCREMENT,                     cast(a.gmt_create as datetime) as CREATE_TIME,                     cast(a.gmt_modified as datetime) as UPDATE_TIME,                     cast(NULL as datetime) as CHECK_TIME,                     cast(d.collation as char(32)) as TABLE_COLLATION,                     cast(NULL as unsigned) as CHECKSUM,                     cast(NULL as char(255)) as CREATE_OPTIONS,                     cast(case when a.table_type = 4 then 'VIEW'                              else a.comment end as char(2048)) as TABLE_COMMENT                     from                     (                     select cast(0 as signed) as tenant_id,                            c.database_id,                            c.table_id,                            c.table_name,                            c.collation_type,                            c.table_type,                            usec_to_time(d.schema_version) as gmt_create,                            usec_to_time(c.schema_version) as gmt_modified,                            c.comment,                            c.store_format                     from oceanbase.__all_virtual_core_all_table c                     join oceanbase.__all_virtual_core_all_table d                       on c.tenant_id = d.tenant_id and d.table_name = '__all_core_table'                     where c.tenant_id = effective_tenant_id()                     union all                     select tenant_id,                            database_id,                            table_id,                            table_name,                            collation_type,                            table_type,                            gmt_create,                            gmt_modified,                            comment,                            store_format                     from oceanbase.__all_table where table_mode >> 12 & 15 in (0,1)) a                     join oceanbase.__all_database b                     on a.database_id = b.database_id                     and a.tenant_id = b.tenant_id                     join oceanbase.__tenant_virtual_collation d                     on a.collation_type = d.collation_type                     left join (                       select tenant_id,                              table_id,                              row_cnt,                              avg_row_len,                              (macro_blk_cnt * 2 * 1024 * 1024) as data_size                       from oceanbase.__all_table_stat                       where partition_id = -1 or partition_id = table_id) ts                     on a.table_id = ts.table_id                     and a.tenant_id = ts.tenant_id                     where a.tenant_id = 0                     and a.table_type in (0, 1, 2, 3, 4, 14, 15)                     and b.database_name != '__recyclebin'                     and b.in_recyclebin = 0                     and 0 = sys_privilege_check('table_acc', effective_tenant_id(), b.database_name, a.table_name) )__"))) {
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

int ObInnerTableSchema::collations_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_COLLATIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_COLLATIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select collation as COLLATION_NAME, charset as CHARACTER_SET_NAME, id as ID, `is_default` as IS_DEFAULT, is_compiled as IS_COMPILED, sortlen as SORTLEN from oceanbase.__tenant_virtual_collation )__"))) {
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

int ObInnerTableSchema::collation_character_set_applicability_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_COLLATION_CHARACTER_SET_APPLICABILITY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_COLLATION_CHARACTER_SET_APPLICABILITY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select collation as COLLATION_NAME, charset as CHARACTER_SET_NAME from oceanbase.__tenant_virtual_collation )__"))) {
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

int ObInnerTableSchema::processlist_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_PROCESSLIST_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_PROCESSLIST_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT id AS ID, user AS USER, concat(user_client_ip, ':', user_client_port) AS HOST, db AS DB, command AS COMMAND, cast(time as SIGNED) AS TIME, state AS STATE, info AS INFO FROM oceanbase.__all_virtual_processlist WHERE  is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) )__"))) {
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

int ObInnerTableSchema::key_column_usage_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_KEY_COLUMN_USAGE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_KEY_COLUMN_USAGE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(                     (select 'def' as CONSTRAINT_CATALOG,                     c.database_name collate utf8mb4_name_case as  CONSTRAINT_SCHEMA,                     'PRIMARY' as CONSTRAINT_NAME, 'def' as TABLE_CATALOG,                     c.database_name collate utf8mb4_name_case as TABLE_SCHEMA,                     a.table_name collate utf8mb4_name_case as TABLE_NAME,                     b.column_name as COLUMN_NAME,                     b.rowkey_position as ORDINAL_POSITION,                     CAST(NULL AS UNSIGNED) as POSITION_IN_UNIQUE_CONSTRAINT,                     CAST(NULL AS CHAR(64)) as REFERENCED_TABLE_SCHEMA,                     CAST(NULL AS CHAR(64)) as REFERENCED_TABLE_NAME,                     CAST(NULL AS CHAR(64)) as REFERENCED_COLUMN_NAME                     from oceanbase.__all_table a                     join oceanbase.__all_column b                       on a.tenant_id = b.tenant_id and a.table_id = b.table_id                     join oceanbase.__all_database c                       on a.tenant_id = c.tenant_id and a.database_id = c.database_id                     where a.tenant_id = 0                       and a.table_mode >> 12 & 15 in (0,1)                       and c.in_recyclebin = 0                       and c.database_name != '__recyclebin'                       and b.rowkey_position > 0                       and b.column_id >= 16                       and a.table_type != 5 and a.table_type != 12 and a.table_type != 13                       and b.column_flags & (0x1 << 8) = 0)                      union all                     (select 'def' as CONSTRAINT_CATALOG,                     d.database_name collate utf8mb4_name_case as CONSTRAINT_SCHEMA,                     substr(a.table_name, 2 + length(substring_index(a.table_name,'_',4))) as CONSTRAINT_NAME,                     'def' as TABLE_CATALOG,                     d.database_name collate utf8mb4_name_case as TABLE_SCHEMA,                     c.table_name collate utf8mb4_name_case as TABLE_NAME,                     b.column_name as COLUMN_NAME,                     b.index_position as ORDINAL_POSITION,                     CAST(NULL AS UNSIGNED) as POSITION_IN_UNIQUE_CONSTRAINT,                     CAST(NULL AS CHAR(64)) as REFERENCED_TABLE_SCHEMA,                     CAST(NULL AS CHAR(64)) as REFERENCED_TABLE_NAME,                     CAST(NULL AS CHAR(64)) as REFERENCED_COLUMN_NAME                     from oceanbase.__all_table a                     join oceanbase.__all_column b                       on a.tenant_id = b.tenant_id and a.table_id = b.table_id                     join oceanbase.__all_table c                       on a.tenant_id = c.tenant_id and a.data_table_id = c.table_id                     join oceanbase.__all_database d                       on a.tenant_id = d.tenant_id and c.database_id = d.database_id                     where a.tenant_id = 0                       and d.in_recyclebin = 0                       and d.database_name != '__recyclebin'                       and a.table_type = 5                       and a.index_type in (2, 4, 8)                       and b.index_position > 0)                      union all                     (select 'def' as CONSTRAINT_CATALOG,                     d.database_name collate utf8mb4_name_case as CONSTRAINT_SCHEMA,                     f.foreign_key_name as CONSTRAINT_NAME,                     'def' as TABLE_CATALOG,                     d.database_name collate utf8mb4_name_case as TABLE_SCHEMA,                     t.table_name collate utf8mb4_name_case as TABLE_NAME,                     c.column_name as COLUMN_NAME,                     fc.position as ORDINAL_POSITION,                     CAST(fc.position AS UNSIGNED) as POSITION_IN_UNIQUE_CONSTRAINT,                     d2.database_name as REFERENCED_TABLE_SCHEMA,                     t2.table_name as REFERENCED_TABLE_NAME,                     c2.column_name as REFERENCED_COLUMN_NAME                     from                     oceanbase.__all_foreign_key f                     join oceanbase.__all_table t                       on f.tenant_id = t.tenant_id and f.child_table_id = t.table_id                     join oceanbase.__all_database d                       on f.tenant_id = d.tenant_id and t.database_id = d.database_id                     join oceanbase.__all_foreign_key_column fc                       on f.tenant_id = fc.tenant_id and f.foreign_key_id = fc.foreign_key_id                     join oceanbase.__all_column c                       on f.tenant_id = c.tenant_id and fc.child_column_id = c.column_id and t.table_id = c.table_id                     join oceanbase.__all_table t2                       on f.tenant_id = t2.tenant_id and f.parent_table_id = t2.table_id                     join oceanbase.__all_database d2                       on f.tenant_id = d2.tenant_id and t2.database_id = d2.database_id                     join oceanbase.__all_column c2                       on f.tenant_id = c2.tenant_id and fc.parent_column_id = c2.column_id and t2.table_id = c2.table_id                     where f.tenant_id = 0)                      union all                     (select 'def' as CONSTRAINT_CATALOG,                     d.database_name collate utf8mb4_name_case as CONSTRAINT_SCHEMA,                     f.foreign_key_name as CONSTRAINT_NAME,                     'def' as TABLE_CATALOG,                     d.database_name collate utf8mb4_name_case as TABLE_SCHEMA,                     t.table_name collate utf8mb4_name_case as TABLE_NAME,                     c.column_name as COLUMN_NAME,                     fc.position as ORDINAL_POSITION,                     CAST(fc.position AS UNSIGNED) as POSITION_IN_UNIQUE_CONSTRAINT,                     d.database_name as REFERENCED_TABLE_SCHEMA,                     t2.mock_fk_parent_table_name as REFERENCED_TABLE_NAME,                     c2.parent_column_name as REFERENCED_COLUMN_NAME                     from oceanbase.__all_foreign_key f                     join oceanbase.__all_table t                       on f.tenant_id = t.tenant_id and f.child_table_id = t.table_id                     join oceanbase.__all_database d                       on f.tenant_id = d.tenant_id and t.database_id = d.database_id                     join oceanbase.__all_foreign_key_column fc                       on f.tenant_id = fc.tenant_id and f.foreign_key_id = fc.foreign_key_id                     join oceanbase.__all_column c                       on f.tenant_id = c.tenant_id and fc.child_column_id = c.column_id and t.table_id = c.table_id                     join oceanbase.__all_mock_fk_parent_table t2                       on f.tenant_id = t2.tenant_id and f.parent_table_id = t2.mock_fk_parent_table_id                     join oceanbase.__all_mock_fk_parent_table_column c2                       on f.tenant_id = c2.tenant_id and fc.parent_column_id = c2.parent_column_id and t2.mock_fk_parent_table_id = c2.mock_fk_parent_table_id                     where f.tenant_id = 0)                     )__"))) {
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

int ObInnerTableSchema::engines_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_ENGINES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ENGINES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT CAST('InnoDB' as CHAR(64)) as ENGINE,            CAST('YES' AS CHAR(8)) as SUPPORT,            CAST('Supports transactions' as CHAR(80)) as COMMENT,            CAST('YES' as CHAR(3)) as TRANSACTIONS,            CAST('NO' as CHAR(3)) as XA,            CAST('YES' as CHAR(3)) as SAVEPOINTS     FROM DUAL; )__"))) {
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

int ObInnerTableSchema::routines_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_ROUTINES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ROUTINES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select                       CAST(mp.specific_name AS CHAR(64)) AS SPECIFIC_NAME,                       CAST('def' AS CHAR(512)) as ROUTINE_CATALOG,                       CAST(mp.db AS CHAR(64)) collate utf8mb4_name_case as ROUTINE_SCHEMA,                       CAST(mp.name AS CHAR(64)) as ROUTINE_NAME,                       CAST(mp.type AS CHAR(9)) as ROUTINE_TYPE,                       CAST(lower(v.data_type_str) AS CHAR(64)) AS DATA_TYPE,                       CAST(                         CASE                         WHEN mp.type = 'FUNCTION' THEN CASE                         WHEN rp.param_type IN (22, 23, 27, 28, 29, 30) THEN rp.param_length                         ELSE NULL                         END                           ELSE NULL                         END                           AS SIGNED                       ) as CHARACTER_MAXIMUM_LENGTH,                       CASE                       WHEN rp.param_type IN (22, 23, 27, 28, 29, 30, 43, 44, 46) THEN CAST(                         rp.param_length * CASE rp.param_coll_type                         WHEN 63 THEN 1                         WHEN 249 THEN 4                         WHEN 248 THEN 4                         WHEN 87 THEN 2                         WHEN 28 THEN 2                         WHEN 55 THEN 4                         WHEN 54 THEN 4                         WHEN 101 THEN 2                         WHEN 46 THEN 4                         WHEN 45 THEN 4                         WHEN 224 THEN 4                         ELSE 1                         END                           AS SIGNED                       )                       ELSE CAST(NULL AS SIGNED)                     END                       AS CHARACTER_OCTET_LENGTH,                       CASE                       WHEN rp.param_type IN (1, 2, 3, 4, 5, 15, 16, 50) THEN CAST(rp.param_precision AS UNSIGNED)                       ELSE CAST(NULL AS UNSIGNED)                     END                       AS NUMERIC_PRECISION,                       CASE                       WHEN rp.param_type IN (15, 16, 50) THEN CAST(rp.param_scale AS SIGNED)                       WHEN rp.param_type IN (1, 2, 3, 4, 5, 11, 12, 13, 14) THEN CAST(0 AS SIGNED)                       ELSE CAST(NULL AS SIGNED)                     END                       AS NUMERIC_SCALE,                       CASE                       WHEN rp.param_type IN (17, 18, 20) THEN CAST(rp.param_scale AS UNSIGNED)                       ELSE CAST(NULL AS UNSIGNED)                     END                       AS DATETIME_PRECISION,                       CAST(                         CASE rp.param_charset                         WHEN 1 THEN 'binary'                         WHEN 2 THEN 'utf8mb4'                         WHEN 3 THEN 'gbk'                         WHEN 4 THEN 'utf16'                         WHEN 5 THEN 'gb18030'                         WHEN 6 THEN 'latin1'                         WHEN 7 THEN 'gb18030_2022'                         ELSE NULL                         END                           AS CHAR(64)                       ) AS CHARACTER_SET_NAME,                       CAST(                         CASE rp.param_coll_type                         WHEN 45 THEN 'utf8mb4_general_ci'                         WHEN 46 THEN 'utf8mb4_bin'                         WHEN 63 THEN 'binary'                         ELSE NULL                         END                           AS CHAR(64)                       ) AS COLLATION_NAME,                       CAST(                         CASE                         WHEN rp.param_type IN (1, 2, 3, 4, 5) THEN CONCAT(                           lower(v.data_type_str),                           '(',                           rp.param_precision,                           ')'                         )                         WHEN rp.param_type IN (15, 16, 50) THEN CONCAT(                           lower(v.data_type_str),                           '(',                           rp.param_precision,                           ',',                           rp.param_scale,                           ')'                         )                         WHEN rp.param_type IN (18, 20) THEN CONCAT(lower(v.data_type_str), '(', rp.param_scale, ')')                         WHEN rp.param_type IN (22, 23) and rp.param_length > 0 THEN CONCAT(lower(v.data_type_str), '(', rp.param_length, ')')                         ELSE lower(v.data_type_str)                         END                           AS CHAR(4194304)                       ) AS DTD_IDENTIFIER,                       CAST('SQL' AS CHAR(8)) as ROUTINE_BODY,                       CAST(mp.body AS CHAR(4194304)) as ROUTINE_DEFINITION,                       CAST(NULL AS CHAR(64)) as EXTERNAL_NAME,                       CAST(NULL AS CHAR(64)) as EXTERNAL_LANGUAGE,                       CAST('SQL' AS CHAR(8)) as PARAMETER_STYLE,                       CAST(mp.IS_DETERMINISTIC AS CHAR(3)) AS IS_DETERMINISTIC,                       CAST(mp.SQL_DATA_ACCESS AS CHAR(64)) AS SQL_DATA_ACCESS,                       CAST(NULL AS CHAR(64)) as SQL_PATH,                       CAST(mp.SECURITY_TYPE AS CHAR(7)) as SECURITY_TYPE,                       CAST(r.gmt_create AS datetime) as CREATED,                       CAST(r.gmt_modified AS datetime) as LAST_ALTERED,                       CAST(mp.SQL_MODE AS CHAR(8192)) as SQL_MODE,                       CAST(mp.comment AS CHAR(4194304)) as ROUTINE_COMMENT,                       CAST(mp.DEFINER AS CHAR(93)) as DEFINER,                       CAST(mp.CHARACTER_SET_CLIENT AS CHAR(32)) as CHARACTER_SET_CLIENT,                       CAST(mp.COLLATION_CONNECTION AS CHAR(32)) as COLLATION_CONNECTION,                       CAST(mp.db_collation AS CHAR(32)) as DATABASE_COLLATION                     from                       mysql.proc as mp                       join oceanbase.__all_database a                        on mp.DB = a.DATABASE_NAME                       and  a.in_recyclebin = 0                       join oceanbase.__all_routine as r on mp.specific_name = r.routine_name                        and r.DATABASE_ID = a.DATABASE_ID                       and                       CAST(                         CASE r.routine_type                         WHEN 1 THEN 'PROCEDURE'                         WHEN 2 THEN 'FUNCTION'                         ELSE NULL                         END                           AS CHAR(9)                       ) = mp.type                       left join oceanbase.__all_routine_param as rp on rp.subprogram_id = r.subprogram_id                       and rp.tenant_id = r.tenant_id                       and rp.routine_id = r.routine_id                       and rp.param_position = 0                       left join oceanbase.__all_virtual_data_type v on rp.param_type = v.data_type                     )__"))) {
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

int ObInnerTableSchema::profiling_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_PROFILING_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_PROFILING_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT  CAST(00000000000000000000 as SIGNED) as QUERY_ID,             CAST(00000000000000000000 as SIGNED) as SEQ,             CAST('' as CHAR(30)) as STATE,             CAST(0.000000 as DECIMAL(9, 6)) as DURATION,             CAST(NULL as DECIMAL(9, 6)) as CPU_USER,             CAST(NULL as DECIMAL(9, 6)) as CPU_SYSTEM,             CAST(00000000000000000000 as SIGNED) as CONTEXT_VOLUNTARY,             CAST(00000000000000000000 as SIGNED) as CONTEXT_INVOLUNTARY,             CAST(00000000000000000000 as SIGNED) as BLOCK_OPS_IN,             CAST(00000000000000000000 as SIGNED) as BLOCK_OPS_OUT,             CAST(00000000000000000000 as SIGNED) as MESSAGES_SENT,             CAST(00000000000000000000 as SIGNED) as MESSAGES_RECEIVED,             CAST(00000000000000000000 as SIGNED) as PAGE_FAULTS_MAJOR,             CAST(00000000000000000000 as SIGNED) as PAGE_FAULTS_MINOR,             CAST(00000000000000000000 as SIGNED) as SWAPS,             CAST(NULL as CHAR(30)) as SOURCE_FUNCTION,             CAST(NULL as CHAR(20)) as SOURCE_FILE,             CAST(00000000000000000000 as SIGNED) as SOURCE_LINE     FROM DUAL limit 0; )__"))) {
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
