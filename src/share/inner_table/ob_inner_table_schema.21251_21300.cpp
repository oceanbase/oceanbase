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

int ObInnerTableSchema::cdb_ob_backup_storage_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_STORAGE_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_STORAGE_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     PATH,     ENDPOINT,     DEST_ID,     DEST_TYPE,     AUTHORIZATION,     EXTENSION,     CHECK_FILE_NAME,     USEC_TO_TIME(LAST_CHECK_TIME) AS LAST_CHECK_TIMESTAMP     FROM oceanbase.__all_virtual_backup_storage_info )__"))) {
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

int ObInnerTableSchema::dba_tab_statistics_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_STATISTICS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_STATISTICS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(DB.DATABASE_NAME AS     CHAR(128)) AS OWNER,     CAST(V.TABLE_NAME       AS  CHAR(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  CHAR(128)) AS PARTITION_NAME,     CAST(V.PARTITION_POSITION AS    NUMBER) AS PARTITION_POSITION,     CAST(V.SUBPARTITION_NAME  AS    CHAR(128)) AS SUBPARTITION_NAME,     CAST(V.SUBPARTITION_POSITION AS NUMBER) AS SUBPARTITION_POSITION,     CAST(V.OBJECT_TYPE AS   CHAR(12)) AS OBJECT_TYPE,     CAST(STAT.ROW_CNT AS    NUMBER) AS NUM_ROWS,     CAST(NULL AS    NUMBER) AS BLOCKS,     CAST(NULL AS    NUMBER) AS EMPTY_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_SPACE,     CAST(NULL AS    NUMBER) AS CHAIN_CNT,     CAST(STAT.AVG_ROW_LEN AS    NUMBER) AS AVG_ROW_LEN,     CAST(NULL AS    NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,     CAST(NULL AS    NUMBER) AS NUM_FREELIST_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHED_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHE_HIT_RATIO,     CAST(NULL AS    NUMBER) AS IM_IMCU_COUNT,     CAST(NULL AS    NUMBER) AS IM_BLOCK_COUNT,     CAST(NULL AS    DATETIME) AS IM_STAT_UPDATE_TIME,     CAST(NULL AS    NUMBER) AS SCAN_RATE,     CAST(STAT.SPARE1 AS    NUMBER) AS SAMPLE_SIZE,     CAST(STAT.LAST_ANALYZED AS DATETIME(6)) AS LAST_ANALYZED,     CAST((CASE STAT.GLOBAL_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS GLOBAL_STATS,     CAST((CASE STAT.USER_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS USER_STATS,     CAST((CASE WHEN STAT.STATTYPE_LOCKED & 15 IS NULL THEN NULL ELSE (CASE STAT.STATTYPE_LOCKED & 15 WHEN 0 THEN NULL WHEN 1 THEN 'DATA' WHEN 2 THEN 'CACHE' ELSE 'ALL' END) END) AS CHAR(5)) AS STATTYPE_LOCKED,     CAST((CASE STAT.STALE_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS STALE_STATS,     CAST(NULL AS    CHAR(7)) AS SCOPE     FROM     (       (SELECT CAST(0 AS SIGNED) AS TENANT_ID,               DATABASE_ID,               TABLE_ID,               -2 AS PARTITION_ID,               TABLE_NAME,               NULL AS PARTITION_NAME,               NULL AS SUBPARTITION_NAME,               NULL AS PARTITION_POSITION,               NULL AS SUBPARTITION_POSITION,               'TABLE' AS OBJECT_TYPE           FROM             OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()           AND TABLE_TYPE IN (0,2,3,6,14,15)         UNION ALL         SELECT TENANT_ID,                DATABASE_ID,                TABLE_ID,                CASE WHEN PART_LEVEL = 0 THEN -2 ELSE -1 END AS PARTITION_ID,                TABLE_NAME,                NULL AS PARTITION_NAME,                NULL AS SUBPARTITION_NAME,                NULL AS PARTITION_POSITION,                NULL AS SUBPARTITION_POSITION,                'TABLE' AS OBJECT_TYPE         FROM             oceanbase.__all_table T         WHERE T.TABLE_TYPE IN (0,2,3,6,14,15)         AND T.TABLE_MODE >> 12 & 15 in (0,1))     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 P.PART_ID,                 T.TABLE_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             oceanbase.__all_table T           JOIN             oceanbase.__all_part P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         WHERE T.TABLE_TYPE IN (0,2,3,6,14,15)         AND T.TABLE_MODE >> 12 & 15 in (0,1)     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                T.TABLE_NAME,                  P.PART_NAME,                  SP.SUB_PART_NAME,                  P.PART_IDX + 1,                  SP.SUB_PART_IDX + 1,                  'SUBPARTITION'         FROM             oceanbase.__all_table T         JOIN             oceanbase.__all_part P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         JOIN             oceanbase.__all_sub_part SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE IN (0,2,3,6,14,15)         AND T.TABLE_MODE >> 12 & 15 in (0,1)     ) V     JOIN         oceanbase.__all_database DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = V.DATABASE_ID         AND V.TENANT_ID = 0     LEFT JOIN         oceanbase.__all_table_stat STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND (V.PARTITION_ID = STAT.PARTITION_ID OR V.PARTITION_ID = -2)         AND STAT.INDEX_TYPE = 0 )__"))) {
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

int ObInnerTableSchema::dba_tab_col_statistics_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_COL_STATISTICS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_COL_STATISTICS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT   cast(db.database_name as CHAR(128)) as OWNER,   cast(tc.table_name as CHAR(128)) as  TABLE_NAME,   cast(tc.column_name as CHAR(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as CHAR(128)) as  LOW_VALUE,   cast(stat.max_value as CHAR(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.last_analyzed as DATETIME(6)) as  LAST_ANALYZED,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   CAST((CASE stat.GLOBAL_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS GLOBAL_STATS,   CAST((CASE stat.USER_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS USER_STATS,   cast(NULL as CHAR(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as CHAR(15)) as HISTOGRAM,   cast(NULL as CHAR(7)) SCOPE     FROM     (SELECT CAST(0 AS SIGNED) AS TENANT_ID,             t.DATABASE_ID,             t.TABLE_ID,             t.TABLE_NAME,             c.COLUMN_ID,             c.COLUMN_NAME,             c.IS_HIDDEN           FROM             oceanbase.__all_virtual_core_all_table t,             oceanbase.__all_virtual_core_column_table c           WHERE t.TENANT_ID = EFFECTIVE_TENANT_ID()             and c.TENANT_ID = EFFECTIVE_TENANT_ID()             and c.tenant_id = t.tenant_id             AND c.table_id = t.table_id         UNION ALL      SELECT t.TENANT_ID,             t.database_id,             t.table_id,             t.table_name,             c.COLUMN_ID,             c.COLUMN_NAME,             c.IS_HIDDEN       FROM oceanbase.__all_table t,            oceanbase.__all_column c       where t.table_type in (0,2,3,6,14)         and t.table_mode >> 12 & 15 in (0,1)         and c.tenant_id = t.tenant_id         and c.table_id = t.table_id) tc   JOIN     oceanbase.__all_database db     ON db.tenant_id = tc.tenant_id     AND db.database_id = tc.database_id     and tc.tenant_id = 0   left join     oceanbase.__all_column_stat stat     ON tc.table_id = stat.table_id     AND tc.column_id = stat.column_id     AND stat.object_type = 1 WHERE   tc.is_hidden = 0 )__"))) {
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

int ObInnerTableSchema::dba_part_col_statistics_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PART_COL_STATISTICS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PART_COL_STATISTICS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT   cast(db.database_name as CHAR(128)) as OWNER,   cast(t.table_name as CHAR(128)) as  TABLE_NAME,   cast (part.part_name as CHAR(128)) as PARTITION_NAME,   cast(c.column_name as CHAR(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as CHAR(128)) as  LOW_VALUE,   cast(stat.max_value as CHAR(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.last_analyzed as DATETIME(6)) as  LAST_ANALYZED,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   CAST((CASE stat.GLOBAL_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS GLOBAL_STATS,   CAST((CASE stat.USER_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS USER_STATS,   cast(NULL as CHAR(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as CHAR(15)) as HISTOGRAM     FROM     oceanbase.__all_table t   JOIN     oceanbase.__all_database db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     and t.tenant_id = 0   JOIN     oceanbase.__all_column c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id   JOIN     oceanbase.__all_part part     on t.tenant_id = part.tenant_id     and t.table_id = part.table_id   left join     oceanbase.__all_column_stat stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND part.part_id = stat.partition_id     AND stat.object_type = 2 WHERE   c.is_hidden = 0   AND t.table_type in (0,3,6,14)   AND t.table_mode >> 12 & 15 in (0,1) )__"))) {
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

int ObInnerTableSchema::dba_subpart_col_statistics_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SUBPART_COL_STATISTICS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SUBPART_COL_STATISTICS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT   cast(db.database_name as CHAR(128)) as OWNER,   cast(t.table_name as CHAR(128)) as  TABLE_NAME,   cast (subpart.sub_part_name as CHAR(128)) as SUBPARTITION_NAME,   cast(c.column_name as CHAR(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as CHAR(128)) as  LOW_VALUE,   cast(stat.max_value as CHAR(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.last_analyzed as DATETIME(6)) as  LAST_ANALYZED,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   CAST((CASE stat.GLOBAL_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS GLOBAL_STATS,   CAST((CASE stat.USER_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS USER_STATS,   cast(NULL as CHAR(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as CHAR(15)) as HISTOGRAM     FROM     oceanbase.__all_table t   JOIN     oceanbase.__all_database db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     and t.tenant_id = 0   JOIN     oceanbase.__all_column c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id   JOIN     oceanbase.__all_sub_part subpart     on t.tenant_id = subpart.tenant_id     and t.table_id = subpart.table_id   left join     oceanbase.__all_column_stat stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND stat.partition_id = subpart.sub_part_id     AND stat.object_type = 3 WHERE   c.is_hidden = 0   AND t.table_type in (0,3,6,14)   AND t.table_mode >> 12 & 15 in (0,1) )__"))) {
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

int ObInnerTableSchema::dba_tab_histograms_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_HISTOGRAMS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_HISTOGRAMS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as CHAR(128)) as OWNER,   cast(t.table_name as CHAR(128)) as  TABLE_NAME,   cast(c.column_name as CHAR(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as CHAR(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.b_endpoint_value as CHAR(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT,   cast(NULL as CHAR(7)) as SCOPE     FROM     (SELECT CAST(0 AS SIGNED) AS TENANT_ID,             DATABASE_ID,             TABLE_ID,             TABLE_NAME           FROM             oceanbase.__all_virtual_core_all_table      UNION ALL      SELECT TENANT_ID,             database_id,             table_id,             table_name       FROM oceanbase.__all_table where table_type in (0,3,6,14)       and table_mode >> 12 & 15 in (0,1)) t   JOIN     oceanbase.__all_database db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     and t.tenant_id = 0   JOIN     oceanbase.__all_column c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id   JOIN     oceanbase.__all_histogram_stat hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND hist.object_type = 1 WHERE   c.is_hidden = 0 )__"))) {
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

int ObInnerTableSchema::dba_part_histograms_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PART_HISTOGRAMS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PART_HISTOGRAMS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select     cast(db.database_name as CHAR(128)) as OWNER,     cast(t.table_name as CHAR(128)) as  TABLE_NAME,     cast(part.part_name as CHAR(128)) as PARTITION_NAME,     cast(c.column_name as CHAR(128)) as  COLUMN_NAME,     cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,     cast(NULL as NUMBER) as  ENDPOINT_VALUE,     cast(hist.endpoint_value as CHAR(4000)) as ENDPOINT_ACTUAL_VALUE,     cast(hist.b_endpoint_value as CHAR(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,     cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT     FROM       oceanbase.__all_table t     JOIN       oceanbase.__all_database db       ON db.tenant_id = t.tenant_id       AND db.database_id = t.database_id       and t.tenant_id = 0     JOIN       oceanbase.__all_column c       ON c.tenant_id = t.tenant_id       AND c.table_id = t.table_id     JOIN       oceanbase.__all_part part       on t.tenant_id = part.tenant_id       and t.table_id = part.table_id     JOIN       oceanbase.__all_histogram_stat hist       ON c.table_id = hist.table_id       AND c.column_id = hist.column_id       AND part.part_id = hist.partition_id       AND hist.object_type = 2   WHERE     c.is_hidden = 0     AND t.table_type in (0,3,6,14)     AND t.table_mode >> 12 & 15 in (0,1)   )__"))) {
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

int ObInnerTableSchema::dba_subpart_histograms_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SUBPART_HISTOGRAMS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SUBPART_HISTOGRAMS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select     cast(db.database_name as CHAR(128)) as OWNER,     cast(t.table_name as CHAR(128)) as  TABLE_NAME,     cast(subpart.sub_part_name as CHAR(128)) as SUBPARTITION_NAME,     cast(c.column_name as CHAR(128)) as  COLUMN_NAME,     cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,     cast(NULL as NUMBER) as  ENDPOINT_VALUE,     cast(hist.endpoint_value as CHAR(4000)) as ENDPOINT_ACTUAL_VALUE,     cast(hist.b_endpoint_value as CHAR(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,     cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT     FROM       oceanbase.__all_table t     JOIN       oceanbase.__all_database db       ON db.tenant_id = t.tenant_id       AND db.database_id = t.database_id       and t.tenant_id = 0     JOIN       oceanbase.__all_column c       ON c.tenant_id = t.tenant_id       AND c.table_id = t.table_id     JOIN       oceanbase.__all_sub_part subpart       on t.tenant_id = subpart.tenant_id       and t.table_id = subpart.table_id     JOIN       oceanbase.__all_histogram_stat hist       ON c.table_id = hist.table_id       AND c.column_id = hist.column_id       AND hist.partition_id = subpart.sub_part_id       AND hist.object_type = 3   WHERE     c.is_hidden = 0     AND t.table_type in (0,3,6,14)     AND t.table_mode >> 12 & 15 in (0,1)   )__"))) {
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

int ObInnerTableSchema::dba_tab_stats_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_STATS_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_STATS_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(DB.DATABASE_NAME AS     CHAR(128)) AS OWNER,     CAST(V.TABLE_NAME       AS  CHAR(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  CHAR(128)) AS PARTITION_NAME,     CAST(V.SUBPARTITION_NAME  AS    CHAR(128)) AS SUBPARTITION_NAME,     CAST(STAT.SAVTIME AS DATETIME(6)) AS STATS_UPDATE_TIME     FROM     (       (SELECT CAST(0 AS SIGNED) AS TENANT_ID,               DATABASE_ID,               TABLE_ID,               -2 AS PARTITION_ID,               TABLE_NAME,               NULL AS PARTITION_NAME,               NULL AS SUBPARTITION_NAME,               NULL AS PARTITION_POSITION,               NULL AS SUBPARTITION_POSITION,               'TABLE' AS OBJECT_TYPE           FROM             OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()       UNION ALL         SELECT TENANT_ID,                DATABASE_ID,                TABLE_ID,                CASE WHEN PART_LEVEL = 0 THEN -2 ELSE -1 END AS PARTITION_ID,                TABLE_NAME,                NULL AS PARTITION_NAME,                NULL AS SUBPARTITION_NAME,                NULL AS PARTITION_POSITION,                 NULL AS SUBPARTITION_POSITION,                'TABLE' AS OBJECT_TYPE         FROM             oceanbase.__all_table T         WHERE T.TABLE_TYPE IN (0,3,6,14)         AND T.TABLE_MODE >> 12 & 15 in (0,1))     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 P.PART_ID,                 T.TABLE_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             oceanbase.__all_table T           JOIN             oceanbase.__all_part P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID             AND T.TABLE_MODE >> 12 & 15 in (0,1)         WHERE T.TABLE_TYPE IN (0,3,6,14)     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                T.TABLE_NAME,                  P.PART_NAME,                  SP.SUB_PART_NAME,                  P.PART_IDX + 1,                  SP.SUB_PART_IDX + 1,                  'SUBPARTITION'         FROM             oceanbase.__all_table T         JOIN             oceanbase.__all_part P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID             AND T.TABLE_MODE >> 12 & 15 in (0,1)         JOIN             oceanbase.__all_sub_part SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE IN (0,3,6,14)     ) V     JOIN         oceanbase.__all_database DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = V.DATABASE_ID         AND V.TENANT_ID = 0     LEFT JOIN         oceanbase.__all_table_stat_history STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND (V.PARTITION_ID = STAT.PARTITION_ID OR V.PARTITION_ID = -2)         AND STAT.INDEX_TYPE = 0 )__"))) {
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

int ObInnerTableSchema::dba_ind_statistics_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_IND_STATISTICS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_IND_STATISTICS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(DB.DATABASE_NAME AS     CHAR(128)) AS OWNER,     CAST(V.INDEX_NAME AS     CHAR(128)) AS INDEX_NAME,     CAST(DB.DATABASE_NAME AS     CHAR(128)) AS TABLE_OWNER,     CAST(T.TABLE_NAME       AS  CHAR(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  CHAR(128)) AS PARTITION_NAME,     CAST(V.PARTITION_POSITION AS    NUMBER) AS PARTITION_POSITION,     CAST(V.SUBPARTITION_NAME  AS    CHAR(128)) AS SUBPARTITION_NAME,     CAST(V.SUBPARTITION_POSITION AS NUMBER) AS SUBPARTITION_POSITION,     CAST(V.OBJECT_TYPE AS   CHAR(12)) AS OBJECT_TYPE,     CAST(NULL AS    NUMBER) AS BLEVEL,     CAST(NULL AS    NUMBER) AS LEAF_BLOCKS,     CAST(NULL AS    NUMBER) AS DISTINCT_KEYS,     CAST(NULL AS    NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS    NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS    NUMBER) AS CLUSTERING_FACTOR,     CAST(STAT.ROW_CNT AS    NUMBER) AS NUM_ROWS,     CAST(NULL AS    NUMBER) AS AVG_CACHED_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHE_HIT_RATIO,     CAST(NULL AS    NUMBER) AS SAMPLE_SIZE,     CAST(STAT.LAST_ANALYZED AS DATETIME(6)) AS LAST_ANALYZED,     CAST((CASE STAT.GLOBAL_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS GLOBAL_STATS,     CAST((CASE STAT.USER_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS USER_STATS,     CAST((CASE WHEN STAT.STATTYPE_LOCKED & 15 IS NULL THEN NULL ELSE (CASE STAT.STATTYPE_LOCKED & 15 WHEN 0 THEN NULL WHEN 1 THEN 'DATA' WHEN 2 THEN 'CACHE' ELSE 'ALL' END) END) AS CHAR(5)) AS STATTYPE_LOCKED,     CAST((CASE STAT.STALE_STATS WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' ELSE NULL END) AS CHAR(3)) AS STALE_STATS,     CAST(NULL AS    CHAR(7)) AS SCOPE     FROM     (         (SELECT CAST(0 AS SIGNED) AS TENANT_ID,                 DATABASE_ID,                 TABLE_ID,                 DATA_TABLE_ID,                 -2 AS PARTITION_ID,                 SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 NULL AS PARTITION_NAME,                 NULL AS SUBPARTITION_NAME,                 NULL AS PARTITION_POSITION,                 NULL AS SUBPARTITION_POSITION,                 'INDEX' AS OBJECT_TYPE           FROM             OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE T           WHERE T.TABLE_TYPE = 5 AND T.INDEX_TYPE NOT IN (13, 14, 16, 17, 19, 20, 22) AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         UNION ALL          SELECT TENANT_ID,                 DATABASE_ID,                 TABLE_ID,                 DATA_TABLE_ID,                 CASE WHEN PART_LEVEL = 0 THEN -2 ELSE -1 END AS PARTITION_ID,                 SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,                 NULL AS PARTITION_NAME,                 NULL AS SUBPARTITION_NAME,                 NULL AS PARTITION_POSITION,                 NULL AS SUBPARTITION_POSITION,                 'INDEX' AS OBJECT_TYPE         FROM             oceanbase.__all_table T         WHERE T.TABLE_TYPE = 5 AND T.INDEX_TYPE NOT IN (13, 14, 16, 17, 19, 20, 22)         AND T.TABLE_MODE >> 12 & 15 in (0,1))     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 T.DATA_TABLE_ID,                 P.PART_ID,                 SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) AS INDEX_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             oceanbase.__all_table T           JOIN             oceanbase.__all_part P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         WHERE T.TABLE_TYPE = 5 AND T.INDEX_TYPE NOT IN (13, 14, 16, 17, 19, 20, 22)     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                T.DATA_TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) AS INDEX_NAME,                P.PART_NAME,                SP.SUB_PART_NAME,                P.PART_IDX + 1,                SP.SUB_PART_IDX + 1,                'SUBPARTITION'         FROM             oceanbase.__all_table T         JOIN             oceanbase.__all_part P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         JOIN             oceanbase.__all_sub_part SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE = 5 AND T.INDEX_TYPE NOT IN (13, 14, 16, 17, 19, 20, 22)     ) V     JOIN oceanbase.__all_table T          ON T.TABLE_ID = V.DATA_TABLE_ID          AND T.TENANT_ID = V.TENANT_ID          AND T.DATABASE_ID = V.DATABASE_ID     JOIN         oceanbase.__all_database DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = V.DATABASE_ID         AND V.TENANT_ID = 0     LEFT JOIN         oceanbase.__all_table_stat STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND (V.PARTITION_ID = STAT.PARTITION_ID OR V.PARTITION_ID = -2)         AND STAT.INDEX_TYPE = 1 )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     EXECUTOR_TENANT_ID,     PLUS_ARCHIVELOG,     BACKUP_TYPE,     JOB_LEVEL,     ENCRYPTION_MODE,     PASSWD,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     RESULT,     COMMENT,     DESCRIPTION,     PATH     FROM OCEANBASE.__all_virtual_backup_job     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_job_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_JOB_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_JOB_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     INITIATOR_TENANT_ID,     INITIATOR_JOB_ID,     EXECUTOR_TENANT_ID,     PLUS_ARCHIVELOG,     BACKUP_TYPE,     JOB_LEVEL,     ENCRYPTION_MODE,     PASSWD,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     RESULT,     COMMENT,     DESCRIPTION,     PATH     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_JOB_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TASK_ID,     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     START_SCN,     END_SCN,     USER_LS_START_SCN,     ENCRYPTION_MODE,     PASSWD,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     COMMENT,     PATH,     MINOR_TURN_ID,     MAJOR_TURN_ID,     CASE          WHEN MACRO_BLOCK_COUNT = 0 THEN 0.00         WHEN FINISH_MACRO_BLOCK_COUNT > MACRO_BLOCK_COUNT THEN 99.99         ELSE ROUND((FINISH_MACRO_BLOCK_COUNT / MACRO_BLOCK_COUNT) * 100, 2)     END AS DATA_PROGRESS,     LOG_FILE_COUNT,     FINISH_LOG_FILE_COUNT,     CASE          WHEN LOG_FILE_COUNT = 0 THEN 0.00         WHEN FINISH_LOG_FILE_COUNT > LOG_FILE_COUNT THEN 99.99         ELSE ROUND((FINISH_LOG_FILE_COUNT / LOG_FILE_COUNT) * 100, 2)     END AS LOG_PROGRESS     FROM OCEANBASE.__all_virtual_backup_task     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_TASK_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TASK_ID,     JOB_ID,     INCARNATION,     BACKUP_SET_ID,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0         THEN NULL       ELSE         USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     START_SCN,     END_SCN,     USER_LS_START_SCN,     ENCRYPTION_MODE,     PASSWD,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     COMMENT,     PATH,     MINOR_TURN_ID,     MAJOR_TURN_ID     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_TASK_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_set_files_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_SET_FILES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_SET_FILES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     BACKUP_SET_ID,     DEST_ID,     INCARNATION,     BACKUP_TYPE,     PREV_FULL_BACKUP_SET_ID,     PREV_INC_BACKUP_SET_ID,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0           THEN NULL       ELSE           USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     FILE_STATUS,     CASE       WHEN END_TS = 0         THEN 0       ELSE         ROUND((END_TS - START_TS)/1000/1000,0)       END AS ELAPSED_SECONDES,     PLUS_ARCHIVELOG,     START_REPLAY_SCN,     CASE       WHEN START_REPLAY_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(START_REPLAY_SCN)       END AS START_REPLAY_SCN_DISPLAY,     MIN_RESTORE_SCN,     CASE       WHEN MIN_RESTORE_SCN_DISPLAY != ''         THEN MIN_RESTORE_SCN_DISPLAY       WHEN MIN_RESTORE_SCN = 0          THEN NULL       ELSE         SCN_TO_TIMESTAMP(MIN_RESTORE_SCN)       END AS MIN_RESTORE_SCN_DISPLAY,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     COMMENT,     ENCRYPTION_MODE,     PASSWD,     TENANT_COMPATIBLE,     BACKUP_COMPATIBLE,     PATH,     CLUSTER_VERSION,     CONSISTENT_SCN,     MINOR_TURN_ID,     MAJOR_TURN_ID     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_SET_FILES     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_sql_plan_baselines_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SQL_PLAN_BASELINES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SQL_PLAN_BASELINES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(ITEM.PLAN_HASH_VALUE     AS UNSIGNED) AS SIGNATURE,     CAST(ITEM.SQL_ID              AS CHAR(32)) AS SQL_HANDLE,     ITEM.ORIGIN_SQL AS SQL_TEXT,     CAST(ITEM.PLAN_HASH_VALUE     AS CHAR(128)) AS PLAN_NAME,     CAST(DB.DATABASE_NAME         AS CHAR(128)) AS CREATOR,     CAST((CASE ITEM.ORIGIN WHEN 1 THEN 'AUTO-CAPTURE' WHEN 2 THEN 'MANUAL-LOAD' ELSE NULL END) AS CHAR(29)) AS ORIGIN,     CAST(NULL                     AS CHAR(128)) AS PARSING_SCHEMA_NAME,     CAST(ITEM.DESCRIPTION         AS CHAR(500)) AS DESCRIPTION,     CAST(ITEM.DB_VERSION          AS CHAR(256)) AS VERSION,     ITEM.GMT_CREATE AS CREATED,     ITEM.GMT_MODIFIED AS LAST_MODIFIED,     CAST(ITEM.LAST_EXECUTED       AS SIGNED) AS LAST_EXECUTED,     CAST(ITEM.LAST_VERIFIED       AS SIGNED) AS LAST_VERIFIED,     CAST(CASE WHEN (ITEM.FLAGS & 1) > 0 THEN 'YES' ELSE 'NO' END  AS CHAR(3)) AS ENABLED,     CAST(CASE WHEN (ITEM.FLAGS & 2) > 0 THEN 'YES' ELSE 'NO' END  AS CHAR(3)) AS ACCEPTED,     CAST(CASE WHEN (ITEM.FLAGS & 4) > 0 THEN 'YES' ELSE 'NO' END  AS CHAR(3)) AS FIXED,     CAST(CASE WHEN (ITEM.FLAGS & 16) > 0 THEN 'YES' ELSE 'NO' END  AS CHAR(3)) AS REPRODUCED,     CAST(CASE WHEN (ITEM.FLAGS & 8) > 0 THEN 'YES' ELSE 'NO' END  AS CHAR(3)) AS AUTOPURGE,     CAST('NO'                     AS CHAR(3)) AS ADAPTIVE,     CAST(ITEM.OPTIMIZER_COST      AS SIGNED) AS OPTIMIZER_COST,     CAST(NULL                     AS CHAR(64)) AS MODULE,     CAST(NULL                     AS CHAR(64)) AS ACTION,     CAST(ITEM.EXECUTIONS          AS SIGNED) AS EXECUTIONS,     CAST(ITEM.ELAPSED_TIME        AS SIGNED) AS ELAPSED_TIME,     CAST(ITEM.CPU_TIME            AS SIGNED) AS CPU_TIME,     CAST(NULL                     AS SIGNED) AS BUFFER_GETS,     CAST(NULL                     AS SIGNED) AS DISK_READS,     CAST(NULL                     AS SIGNED) AS DIRECT_WRITES,     CAST(NULL                     AS SIGNED) AS ROWS_PROCESSED,     CAST(NULL                     AS SIGNED) AS FETCHES,     CAST(NULL                     AS SIGNED) AS END_OF_FETCH_COUNT     FROM       oceanbase.__all_plan_baseline_item ITEM,       oceanbase.__all_database DB     WHERE ITEM.DATABASE_ID = DB.DATABASE_ID )__"))) {
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

int ObInnerTableSchema::dba_sql_management_config_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SQL_MANAGEMENT_CONFIG_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SQL_MANAGEMENT_CONFIG_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(CONFIG.NAME          AS CHAR(128)) AS PARAMETER_NAME,     CAST(CONFIG.VALUE         AS CHAR(4000)) AS PARAMETER_VALUE,     CONFIG.GMT_MODIFIED  AS LAST_MODIFIED,     CAST(DB.DATABASE_NAME     AS CHAR(128)) AS MODIFIED_BY     FROM       oceanbase.__all_spm_config CONFIG       LEFT JOIN oceanbase.__all_database DB       ON CONFIG.MODIFIED_BY = DB.DATABASE_ID )__"))) {
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

int ObInnerTableSchema::gv_active_session_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_ACTIVE_SESSION_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_ACTIVE_SESSION_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       CAST(SVR_IP AS CHAR(46)) AS SVR_IP,       CAST(SVR_PORT AS SIGNED) AS SVR_PORT,       CAST(SAMPLE_ID AS SIGNED) AS SAMPLE_ID,       CAST(SAMPLE_TIME AS DATETIME) AS SAMPLE_TIME,       CAST(TENANT_ID AS SIGNED) AS CON_ID,       CAST(USER_ID AS SIGNED) AS USER_ID,       CAST(SESSION_ID AS SIGNED) AS SESSION_ID,       CAST(IF (SESSION_TYPE = 0, 'FOREGROUND', 'BACKGROUND') AS CHAR(10)) AS SESSION_TYPE,       CAST(IF (EVENT_NO = 0, 'ON CPU', 'WAITING') AS CHAR(7)) AS SESSION_STATE,       CAST(SQL_ID AS CHAR(32)) AS SQL_ID,       CAST(PLAN_ID AS SIGNED) AS PLAN_ID,       CAST(TRACE_ID AS CHAR(64)) AS TRACE_ID,       CAST(NAME AS CHAR(64)) AS EVENT,       CAST(EVENT_NO AS SIGNED) AS EVENT_NO,       CAST(oceanbase.__all_virtual_ash.EVENT_ID AS SIGNED) AS EVENT_ID,       CAST(PARAMETER1 AS CHAR(64)) AS P1TEXT,       CAST(P1 AS SIGNED) AS P1,       CAST(PARAMETER2 AS CHAR(64)) AS P2TEXT,       CAST(P2 AS SIGNED) AS P2,       CAST(PARAMETER3 AS CHAR(64)) AS P3TEXT,       CAST(P3 AS SIGNED) AS P3,       CAST(WAIT_CLASS AS CHAR(64)) AS WAIT_CLASS,       CAST(WAIT_CLASS_ID AS SIGNED) AS WAIT_CLASS_ID,       CAST(TIME_WAITED AS SIGNED) AS TIME_WAITED,       CAST(SQL_PLAN_LINE_ID AS SIGNED) SQL_PLAN_LINE_ID,       CAST(GROUP_ID AS SIGNED) GROUP_ID,       CAST(TX_ID AS SIGNED) TX_ID,       CAST(BLOCKING_SESSION_ID AS SIGNED) BLOCKING_SESSION_ID,       CAST(IF (IN_PARSE = 1, 'Y', 'N') AS CHAR(1)) AS IN_PARSE,       CAST(IF (IN_PL_PARSE = 1, 'Y', 'N') AS CHAR(1)) AS IN_PL_PARSE,       CAST(IF (IN_PLAN_CACHE = 1, 'Y', 'N') AS CHAR(1)) AS IN_PLAN_CACHE,       CAST(IF (IN_SQL_OPTIMIZE = 1, 'Y', 'N') AS CHAR(1)) AS IN_SQL_OPTIMIZE,       CAST(IF (IN_SQL_EXECUTION = 1, 'Y', 'N') AS CHAR(1)) AS IN_SQL_EXECUTION,       CAST(IF (IN_PX_EXECUTION = 1, 'Y', 'N') AS CHAR(1)) AS IN_PX_EXECUTION,       CAST(IF (IN_SEQUENCE_LOAD = 1, 'Y', 'N') AS CHAR(1)) AS IN_SEQUENCE_LOAD,       CAST(IF (IN_COMMITTING = 1, 'Y', 'N') AS CHAR(1)) AS IN_COMMITTING,       CAST(IF (IN_STORAGE_READ = 1, 'Y', 'N') AS CHAR(1)) AS IN_STORAGE_READ,       CAST(IF (IN_STORAGE_WRITE = 1, 'Y', 'N') AS CHAR(1)) AS IN_STORAGE_WRITE,       CAST(IF (IN_REMOTE_DAS_EXECUTION = 1, 'Y', 'N') AS CHAR(1)) AS IN_REMOTE_DAS_EXECUTION,       CAST(IF (IN_FILTER_ROWS = 1, 'Y', 'N') AS CHAR(1)) AS IN_FILTER_ROWS,       CAST(PROGRAM AS CHAR(64)) AS PROGRAM,       CAST(MODULE AS CHAR(64)) AS MODULE,       CAST(ACTION AS CHAR(64)) AS ACTION,       CAST(CLIENT_ID AS CHAR(64)) AS CLIENT_ID,       CAST(BACKTRACE AS CHAR(512)) AS BACKTRACE,       CAST(TM_DELTA_TIME AS SIGNED) AS TM_DELTA_TIME,       CAST(TM_DELTA_CPU_TIME AS SIGNED) AS TM_DELTA_CPU_TIME,       CAST(TM_DELTA_DB_TIME AS SIGNED) AS TM_DELTA_DB_TIME,       CAST(TOP_LEVEL_SQL_ID AS CHAR(32)) AS TOP_LEVEL_SQL_ID,       CAST(IF (IN_PLSQL_COMPILATION = 1, 'Y', 'N') AS CHAR(1)) AS IN_PLSQL_COMPILATION,       CAST(IF (IN_PLSQL_EXECUTION = 1, 'Y', 'N') AS CHAR(1)) AS IN_PLSQL_EXECUTION,       CAST(PLSQL_ENTRY_OBJECT_ID AS SIGNED) AS PLSQL_ENTRY_OBJECT_ID,       CAST(PLSQL_ENTRY_SUBPROGRAM_ID AS SIGNED) AS PLSQL_ENTRY_SUBPROGRAM_ID,       CAST(PLSQL_ENTRY_SUBPROGRAM_NAME AS CHAR(32)) AS PLSQL_ENTRY_SUBPROGRAM_NAME,       CAST(PLSQL_OBJECT_ID AS SIGNED) AS PLSQL_OBJECT_ID,       CAST(PLSQL_SUBPROGRAM_ID AS SIGNED) AS PLSQL_SUBPROGRAM_ID,       CAST(PLSQL_SUBPROGRAM_NAME AS CHAR(32)) AS PLSQL_SUBPROGRAM_NAME   FROM oceanbase.__all_virtual_ash LEFT JOIN oceanbase.v$event_name on EVENT_NO = `event#` )__"))) {
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

int ObInnerTableSchema::v_active_session_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_ACTIVE_SESSION_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_ACTIVE_SESSION_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT SVR_IP, SVR_PORT, SAMPLE_ID, SAMPLE_TIME, CON_ID, USER_ID, SESSION_ID, SESSION_TYPE, SESSION_STATE, SQL_ID, PLAN_ID, TRACE_ID, EVENT, EVENT_NO, EVENT_ID, P1TEXT, P1, P2TEXT, P2, P3TEXT, P3, WAIT_CLASS, WAIT_CLASS_ID, TIME_WAITED, SQL_PLAN_LINE_ID, GROUP_ID, TX_ID, BLOCKING_SESSION_ID, IN_PARSE, IN_PL_PARSE, IN_PLAN_CACHE, IN_SQL_OPTIMIZE, IN_SQL_EXECUTION, IN_PX_EXECUTION, IN_SEQUENCE_LOAD, IN_COMMITTING, IN_STORAGE_READ, IN_STORAGE_WRITE, IN_REMOTE_DAS_EXECUTION, IN_FILTER_ROWS, PROGRAM, MODULE, ACTION, CLIENT_ID, BACKTRACE, TM_DELTA_TIME, TM_DELTA_CPU_TIME, TM_DELTA_DB_TIME, TOP_LEVEL_SQL_ID, IN_PLSQL_COMPILATION, IN_PLSQL_EXECUTION, PLSQL_ENTRY_OBJECT_ID, PLSQL_ENTRY_SUBPROGRAM_ID, PLSQL_ENTRY_SUBPROGRAM_NAME, PLSQL_OBJECT_ID, PLSQL_SUBPROGRAM_ID, PLSQL_SUBPROGRAM_NAME FROM oceanbase.gv$active_session_history WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_dml_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_DML_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_DML_STATS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           CAST(SVR_IP AS CHAR(46)) AS SVR_IP,           CAST(SVR_PORT AS SIGNED) AS SVR_PORT,           CAST(TENANT_ID AS SIGNED) AS INST_ID,           CAST(TABLE_ID AS SIGNED) AS OBJN,           CAST(INSERT_ROW_COUNT AS SIGNED) AS INS,           CAST(UPDATE_ROW_COUNT AS SIGNED) AS UPD,           CAST(DELETE_ROW_COUNT AS SIGNED) AS DEL,           CAST(NULL AS SIGNED) AS DROPSEG,           CAST(NULL AS SIGNED) AS CURROWS,           CAST(TABLET_ID AS SIGNED) AS PAROBJN,           CAST(NULL AS SIGNED) AS LASTUSED,           CAST(NULL AS SIGNED) AS FLAGS,           CAST(NULL AS SIGNED) AS CON_ID           FROM oceanbase.__all_virtual_dml_stats )__"))) {
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

int ObInnerTableSchema::v_dml_stats_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_DML_STATS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_DML_STATS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,     SVR_PORT,     INST_ID,     OBJN,     INS,     UPD,     DEL,     DROPSEG,     CURROWS,     PAROBJN,     LASTUSED,     FLAGS,     CON_ID FROM oceanbase.GV$DML_STATS WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::dba_tab_modifications_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_MODIFICATIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_MODIFICATIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT   CAST(DB.DATABASE_NAME AS     CHAR(128)) AS TABLE_OWNER,   CAST(T.TABLE_NAME AS         CHAR(128)) AS TABLE_NAME,   CAST(P.PART_NAME AS     CHAR(128)) AS PARTITION_NAME,   CAST(SP.SUB_PART_NAME AS CHAR(128)) AS SUBPARTITION_NAME,   CAST(V.INSERTS AS     SIGNED) AS INSERTS,   CAST(V.UPDATES AS     SIGNED) AS UPDATES,   CAST(V.DELETES AS     SIGNED) AS DELETES,   CAST(V.MODIFIED_TIME AS DATE) AS TIMESTAMP,   CAST(NULL AS     CHAR(3)) AS TRUNCATED,   CAST(NULL AS     SIGNED) AS DROP_SEGMENTS   FROM     (SELECT      CASE WHEN T.TENANT_ID IS NOT NULL THEN T.TENANT_ID ELSE 0 END AS TENANT_ID,      CASE WHEN T.TABLE_ID IS NOT NULL THEN T.TABLE_ID ELSE VT.TABLE_ID END AS TABLE_ID,      CASE WHEN T.TABLET_ID IS NOT NULL THEN T.TABLET_ID ELSE VT.TABLET_ID END AS TABLET_ID,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.INSERTS + VT.INSERT_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.INSERTS ELSE VT.INSERT_ROW_COUNT END) END AS INSERTS,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.UPDATES + VT.UPDATE_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.UPDATES ELSE VT.UPDATE_ROW_COUNT END) END AS UPDATES,      CASE WHEN T.TABLET_ID IS NOT NULL AND VT.TABLET_ID IS NOT NULL THEN T.DELETES + VT.DELETE_ROW_COUNT ELSE        (CASE WHEN T.TABLET_ID IS NOT NULL THEN T.DELETES ELSE VT.DELETE_ROW_COUNT END) END AS DELETES,      CASE WHEN T.GMT_MODIFIED IS NOT NULL THEN T.GMT_MODIFIED ELSE NULL END AS MODIFIED_TIME      FROM      OCEANBASE.__ALL_MONITOR_MODIFIED T      FULL JOIN      OCEANBASE.__ALL_VIRTUAL_DML_STATS VT      ON T.TABLET_ID = VT.TABLET_ID AND VT.TENANT_ID = EFFECTIVE_TENANT_ID()     )V     JOIN OCEANBASE.__ALL_TABLE T          ON V.TENANT_ID = T.TENANT_ID          AND V.TABLE_ID = T.TABLE_ID          AND T.TABLE_TYPE in (0, 3, 6)          AND T.TABLE_MODE >> 12 & 15 in (0,1)     JOIN         OCEANBASE.__ALL_DATABASE DB         ON T.TENANT_ID = DB.TENANT_ID         AND DB.DATABASE_ID = T.DATABASE_ID     LEFT JOIN         OCEANBASE.__ALL_PART P         ON V.TENANT_ID = P.TENANT_ID         AND V.TABLE_ID = P.TABLE_ID         AND V.TABLET_ID = P.TABLET_ID     LEFT JOIN         OCEANBASE.__ALL_SUB_PART SP         ON V.TENANT_ID = SP.TENANT_ID         AND V.TABLE_ID = SP.TABLE_ID         AND V.TABLET_ID = SP.TABLET_ID   )__"))) {
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

int ObInnerTableSchema::dba_scheduler_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SCHEDULER_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SCHEDULER_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(T.POWNER AS CHAR(128)) AS OWNER,     CAST(T.JOB_NAME AS CHAR(128)) AS JOB_NAME,     CAST(NULL AS CHAR(128)) AS JOB_SUBNAME,     CAST(T.JOB_STYLE AS CHAR(17)) AS JOB_STYLE,     CAST(NULL AS CHAR(128)) AS JOB_CREATOR,     CAST(NULL AS CHAR(65)) AS CLIENT_ID,     CAST(NULL AS CHAR(33)) AS GLOBAL_UID,     CAST(T.POWNER AS CHAR(4000)) AS PROGRAM_OWNER,     CAST(T.PROGRAM_NAME AS CHAR(4000)) AS PROGRAM_NAME,     CAST(T.JOB_TYPE AS CHAR(16)) AS JOB_TYPE,     CAST(T.JOB_ACTION AS CHAR(4000)) AS JOB_ACTION,     CAST(T.NUMBER_OF_ARGUMENT AS SIGNED) AS NUMBER_OF_ARGUMENTS,     CAST(NULL AS CHAR(4000)) AS SCHEDULE_OWNER,     CAST(NULL AS CHAR(4000)) AS SCHEDULE_NAME,     CAST(NULL AS CHAR(12)) AS SCHEDULE_TYPE,     CAST(T.START_DATE AS DATETIME(6)) AS START_DATE,     CAST(T.REPEAT_INTERVAL AS CHAR(4000)) AS REPEAT_INTERVAL,     CAST(NULL AS CHAR(128)) AS EVENT_QUEUE_OWNER,     CAST(NULL AS CHAR(128)) AS EVENT_QUEUE_NAME,     CAST(NULL AS CHAR(523)) AS EVENT_QUEUE_AGENT,     CAST(NULL AS CHAR(4000)) AS EVENT_CONDITION,     CAST(NULL AS CHAR(261)) AS EVENT_RULE,     CAST(NULL AS CHAR(261)) AS FILE_WATCHER_OWNER,     CAST(NULL AS CHAR(261)) AS FILE_WATCHER_NAME,     CAST(T.END_DATE AS DATETIME(6)) AS END_DATE,     CAST(T.JOB_CLASS AS CHAR(128)) AS JOB_CLASS,     CAST(T.ENABLED AS CHAR(5)) AS ENABLED,     CAST(T.AUTO_DROP AS CHAR(5)) AS AUTO_DROP,     CAST(NULL AS CHAR(5)) AS RESTART_ON_RECOVERY,     CAST(NULL AS CHAR(5)) AS RESTART_ON_FAILURE,     CAST(T.STATE AS CHAR(15)) AS STATE,     CAST(NULL AS SIGNED) AS JOB_PRIORITY,     CAST(T.RUN_COUNT AS SIGNED) AS RUN_COUNT,     CAST(NULL AS SIGNED) AS MAX_RUNS,     CAST(T.FAILURES AS SIGNED) AS FAILURE_COUNT,     CAST(NULL AS SIGNED) AS MAX_FAILURES,     CAST(T.RETRY_COUNT AS SIGNED) AS RETRY_COUNT,     CAST(T.LAST_DATE AS DATETIME(6)) AS LAST_START_DATE,     CAST(T.LAST_RUN_DURATION AS SIGNED) AS LAST_RUN_DURATION,     CAST(T.NEXT_DATE AS DATETIME(6)) AS NEXT_RUN_DATE,     CAST(NULL AS SIGNED) AS SCHEDULE_LIMIT,     CAST(T.MAX_RUN_DURATION AS SIGNED) AS MAX_RUN_DURATION,     CAST(NULL AS CHAR(11)) AS LOGGING_LEVEL,     CAST(NULL AS CHAR(5)) AS STORE_OUTPUT,     CAST(NULL AS CHAR(5)) AS STOP_ON_WINDOW_CLOSE,     CAST(NULL AS CHAR(5)) AS INSTANCE_STICKINESS,     CAST(NULL AS CHAR(4000)) AS RAISE_EVENTS,     CAST(NULL AS CHAR(5)) AS SYSTEM,     CAST(NULL AS SIGNED) AS JOB_WEIGHT,     CAST(T.NLSENV AS CHAR(4000)) AS NLS_ENV,     CAST(NULL AS CHAR(128)) AS SOURCE,     CAST(NULL AS SIGNED) AS NUMBER_OF_DESTINATIONS,     CAST(NULL AS CHAR(261)) AS DESTINATION_OWNER,     CAST(NULL AS CHAR(261)) AS DESTINATION,     CAST(NULL AS CHAR(128)) AS CREDENTIAL_OWNER,     CAST(NULL AS CHAR(128)) AS CREDENTIAL_NAME,     CAST(T.FIELD1 AS CHAR(128)) AS INSTANCE_ID,     CAST(NULL AS CHAR(5)) AS DEFERRED_DROP,     CAST(NULL AS CHAR(5)) AS ALLOW_RUNS_IN_RESTRICTED_MODE,     CAST(T.COMMENTS AS CHAR(4000)) AS COMMENTS,     CAST(T.FLAG AS SIGNED) AS FLAGS,     CAST(NULL AS CHAR(5)) AS RESTARTABLE,     CAST(NULL AS CHAR(128)) AS CONNECT_CREDENTIAL_OWNER,     CAST(NULL AS CHAR(128)) AS CONNECT_CREDENTIAL_NAME   FROM oceanbase.__all_tenant_scheduler_job T WHERE T.JOB_NAME != '__dummy_guard' and T.JOB > 0 )__"))) {
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

int ObInnerTableSchema::dba_ob_outline_concurrent_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       GMT_CREATE AS CREATE_TIME,       GMT_MODIFIED AS MODIFY_TIME,       CAST(EFFECTIVE_TENANT_ID() AS SIGNED) AS TENANT_ID,       DATABASE_ID,       OUTLINE_ID,       NAME AS OUTLINE_NAME,       SQL_TEXT,       OUTLINE_PARAMS,       OUTLINE_TARGET,       CAST(SQL_ID AS CHAR(32)) AS SQL_ID,       OUTLINE_CONTENT,       CASE WHEN IS_DELETED = 1 THEN 'YES' ELSE 'NO' END AS IS_DELETED,       CASE WHEN ENABLED = 1 THEN 'YES' ELSE 'NO' END AS ENABLED     FROM oceanbase.__all_outline_history )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_storage_info_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_STORAGE_INFO_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_STORAGE_INFO_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     PATH,     ENDPOINT,     DEST_ID,     DEST_TYPE,     AUTHORIZATION,     EXTENSION,     CHECK_FILE_NAME,     USEC_TO_TIME(LAST_CHECK_TIME) AS LAST_CHECK_TIMESTAMP     FROM oceanbase.__all_virtual_backup_storage_info_history )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_storage_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_STORAGE_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_STORAGE_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     PATH,     ENDPOINT,     DEST_ID,     DEST_TYPE,     AUTHORIZATION,     EXTENSION,     CHECK_FILE_NAME,     USEC_TO_TIME(LAST_CHECK_TIME) AS LAST_CHECK_TIMESTAMP     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_STORAGE_INFO     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_storage_info_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     PATH,     ENDPOINT,     DEST_ID,     DEST_TYPE,     AUTHORIZATION,     EXTENSION,     CHECK_FILE_NAME,     USEC_TO_TIME(LAST_CHECK_TIME) AS LAST_CHECK_TIMESTAMP     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_policy_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_POLICY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_POLICY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       POLICY_NAME,       RECOVERY_WINDOW     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_DELETE_POLICY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       JOB_ID,       INCARNATION,       INITIATOR_TENANT_ID,       INITIATOR_JOB_ID,       EXECUTOR_TENANT_ID,       TYPE,       USEC_TO_TIME(PARAMETER) AS PARAMETER,       JOB_LEVEL,       USEC_TO_TIME(START_TS) AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           USEC_TO_TIME(END_TS)         END AS END_TIMESTAMP,       STATUS,       TASK_COUNT,       SUCCESS_TASK_COUNT,       RESULT,       COMMENT     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_DELETE_JOB     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_job_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       JOB_ID,       INCARNATION,       INITIATOR_TENANT_ID,       INITIATOR_JOB_ID,       EXECUTOR_TENANT_ID,       TYPE,       USEC_TO_TIME(PARAMETER) AS PARAMETER,       JOB_LEVEL,       USEC_TO_TIME(START_TS) AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           USEC_TO_TIME(END_TS)         END AS END_TIMESTAMP,       STATUS,       TASK_COUNT,       SUCCESS_TASK_COUNT,       RESULT,       COMMENT     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TASK_ID,       INCARNATION,       JOB_ID,       TASK_TYPE,       ID,       ROUND_ID,       DEST_ID,       USEC_TO_TIME(START_TS) AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           USEC_TO_TIME(END_TS)         END AS END_TIMESTAMP,       STATUS,       TOTAL_LS_COUNT,       FINISH_LS_COUNT,       RESULT,       COMMENT,       PATH     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_DELETE_TASK     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_delete_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TASK_ID,       INCARNATION,       JOB_ID,       TASK_TYPE,       ID,       ROUND_ID,       DEST_ID,       USEC_TO_TIME(START_TS) AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           USEC_TO_TIME(END_TS)         END AS END_TIMESTAMP,       STATUS,       TOTAL_LS_COUNT,       FINISH_LS_COUNT,       RESULT,       COMMENT,       PATH     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_outlines_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_OUTLINES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_OUTLINES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       B.GMT_CREATE AS CREATE_TIME,       B.GMT_MODIFIED AS MODIFY_TIME,       A.TENANT_ID,       A.DATABASE_ID,       A.OUTLINE_ID,       A.DATABASE_NAME,       A.OUTLINE_NAME,       A.VISIBLE_SIGNATURE,       A.SQL_TEXT,       A.OUTLINE_TARGET,       A.OUTLINE_SQL,       A.SQL_ID,       A.OUTLINE_CONTENT     FROM oceanbase.__tenant_virtual_outline A, oceanbase.__all_outline B     WHERE A.OUTLINE_ID = B.OUTLINE_ID )__"))) {
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

int ObInnerTableSchema::dba_ob_concurrent_limit_sql_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_CONCURRENT_LIMIT_SQL_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_CONCURRENT_LIMIT_SQL_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       B.GMT_CREATE AS CREATE_TIME,       B.GMT_MODIFIED AS MODIFY_TIME,       A.TENANT_ID,       A.DATABASE_ID,       A.OUTLINE_ID,       A.DATABASE_NAME,       A.OUTLINE_NAME,       A.OUTLINE_CONTENT,       A.VISIBLE_SIGNATURE,       A.SQL_TEXT,       A.CONCURRENT_NUM,       A.LIMIT_TARGET     FROM oceanbase.__tenant_virtual_concurrent_limit_sql A, oceanbase.__all_outline B     WHERE A.OUTLINE_ID = B.OUTLINE_ID )__"))) {
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

int ObInnerTableSchema::dba_ob_restore_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RESTORE_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RESTORE_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     P.JOB_ID AS JOB_ID,     RESTORE_TENANT_NAME,     RESTORE_TENANT_ID,     BACKUP_TENANT_NAME,     BACKUP_TENANT_ID,     BACKUP_CLUSTER_NAME,     BACKUP_DEST,     RESTORE_OPTION,     RESTORE_TYPE,     RESTORE_SCN,     CASE       WHEN RESTORE_SCN IS NULL         THEN NULL       WHEN RESTORE_SCN=0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(RESTORE_SCN)       END AS RESTORE_SCN_DISPLAY,     CASE       WHEN STATUS = 'RESTORE_PRE'         THEN 'RESTORING'       WHEN STATUS = 'RESTORE_CREATE_INIT_LS'         THEN 'RESTORING'       WHEN STATUS = 'RESTORE_WAIT_LS'         THEN 'RESTORING'       WHEN STATUS = 'POST_CHECK'         THEN 'RESTORING'       ELSE STATUS       END AS STATUS,     CASE       WHEN START_TIMESTAMP IS NULL         THEN NULL       WHEN START_TIMESTAMP=''         THEN NULL       WHEN START_TIMESTAMP='0'         THEN NULL       ELSE         USEC_TO_TIME(START_TIMESTAMP)       END AS START_TIMESTAMP,     BACKUP_SET_LIST,     BACKUP_PIECE_LIST,     TOTAL_BYTES,     CASE       WHEN TOTAL_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN TOTAL_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN TOTAL_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(TOTAL_BYTES/1024/1024,2), 'MB')       END AS TOTAL_BYTES_DISPLAY,     FINISH_BYTES,     CASE       WHEN FINISH_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN FINISH_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN FINISH_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(FINISH_BYTES/1024/1024,2), 'MB')       END AS FINISH_BYTES_DISPLAY,     DESCRIPTION     FROM   (       SELECT       TENANT_ID,       JOB_ID,       MAX(CASE NAME WHEN 'tenant_name' THEN VALUE ELSE '' END) AS RESTORE_TENANT_NAME,       MAX(CASE NAME WHEN 'tenant_id' THEN VALUE ELSE '' END) AS RESTORE_TENANT_ID,       MAX(CASE NAME WHEN 'backup_tenant_name' THEN VALUE ELSE '' END) AS BACKUP_TENANT_NAME,       MAX(CASE NAME WHEN 'backup_tenant_id' THEN VALUE ELSE '' END) AS BACKUP_TENANT_ID,       MAX(CASE NAME WHEN 'backup_cluster_name' THEN VALUE ELSE '' END) AS BACKUP_CLUSTER_NAME,       MAX(CASE NAME WHEN 'target_tenant_role' THEN VALUE ELSE '' END) AS TENANT_ROLE,       MAX(CASE NAME WHEN 'backup_dest' THEN VALUE ELSE '' END) AS BACKUP_DEST,       MAX(CASE NAME WHEN 'restore_option' THEN VALUE ELSE '' END) AS RESTORE_OPTION,       MAX(CASE NAME WHEN 'status' THEN VALUE ELSE '' END) AS STATUS,       MAX(CASE NAME WHEN 'restore_scn' THEN VALUE ELSE '' END) AS RESTORE_SCN,       MAX(CASE NAME WHEN 'restore_start_ts' THEN VALUE ELSE '' END) AS START_TIMESTAMP,       MAX(CASE NAME WHEN 'backup_set_list' THEN VALUE ELSE '' END) AS BACKUP_SET_LIST,       MAX(CASE NAME WHEN 'backup_piece_list' THEN VALUE ELSE '' END) AS BACKUP_PIECE_LIST,       MAX(CASE NAME WHEN 'description' THEN VALUE ELSE '' END) AS DESCRIPTION,       MAX(CASE NAME WHEN 'restore_type' THEN VALUE ELSE '' END) AS RESTORE_TYPE       FROM OCEANBASE.__ALL_VIRTUAL_RESTORE_JOB GROUP BY TENANT_ID, JOB_ID   ) P LEFT JOIN   (       SELECT       TENANT_ID,       JOB_ID,       TOTAL_BYTES,       FINISH_BYTES       FROM OCEANBASE.__ALL_VIRTUAL_RESTORE_PROGRESS   ) J     ON P.TENANT_ID=J.TENANT_ID AND P.JOB_ID=J.JOB_ID     WHERE P.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_restore_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RESTORE_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RESTORE_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     JOB_ID,     RESTORE_TENANT_NAME,     RESTORE_TENANT_ID,     BACKUP_TENANT_NAME,     BACKUP_TENANT_ID,     BACKUP_CLUSTER_NAME,     BACKUP_DEST,     RESTORE_SCN,     CASE       WHEN RESTORE_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(RESTORE_SCN)       END AS RESTORE_SCN_DISPLAY,     RESTORE_OPTION,     RESTORE_TYPE,     START_TIME AS START_TIMESTAMP,     FINISH_TIME AS FINISH_TIMESTAMP,     STATUS,     BACKUP_PIECE_LIST,     BACKUP_SET_LIST,     BACKUP_CLUSTER_VERSION,     LS_COUNT,     FINISH_LS_COUNT,     TABLET_COUNT,     FINISH_TABLET_COUNT,     TOTAL_BYTES,     CASE       WHEN TOTAL_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN TOTAL_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN TOTAL_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(TOTAL_BYTES/1024/1024,2), 'MB')       END AS TOTAL_BYTES_DISPLAY,     FINISH_BYTES,     CASE       WHEN FINISH_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN FINISH_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN FINISH_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(FINISH_BYTES/1024/1024,2), 'MB')       END AS FINISH_BYTES_DISPLAY,     DESCRIPTION,     COMMENT     FROM OCEANBASE.__ALL_VIRTUAL_RESTORE_JOB_HISTORY     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_archive_dest_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARCHIVE_DEST_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARCHIVE_DEST_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     DEST_NO,     NAME,     VALUE     FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_archivelog_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARCHIVELOG_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARCHIVELOG_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     DEST_ID,     ROUND_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     CASE       WHEN START_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(START_SCN)       END AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     CASE       WHEN CHECKPOINT_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(CHECKPOINT_SCN)       END AS CHECKPOINT_SCN_DISPLAY,     COMPATIBLE,     BASE_PIECE_ID,     USED_PIECE_ID,     PIECE_SWITCH_INTERVAL,     UNIT_SIZE,     COMPRESSION,     (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) AS INPUT_BYTES,     CASE       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024/1024/1024,2), 'PB')       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024/1024,2), 'TB')       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) AS OUTPUT_BYTES,     CASE       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024/1024/1024,2), 'PB')       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024/1024,2), 'TB')       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) = 0         THEN 0       ELSE         ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) / (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES), 2)       END AS COMPRESSION_RATIO,     DELETED_INPUT_BYTES,     CASE       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_INPUT_BYTES_DISPLAY,     DELETED_OUTPUT_BYTES,     CASE       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_OUTPUT_BYTES_DISPLAY,     COMMENT,     PATH     FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_archivelog_summary_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARCHIVELOG_SUMMARY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARCHIVELOG_SUMMARY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     DEST_ID,     ROUND_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     CASE       WHEN START_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(START_SCN)       END AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     CASE       WHEN CHECKPOINT_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(CHECKPOINT_SCN)       END AS CHECKPOINT_SCN_DISPLAY,     COMPATIBLE,     BASE_PIECE_ID,     USED_PIECE_ID,     PIECE_SWITCH_INTERVAL,     UNIT_SIZE,     COMPRESSION,     INPUT_BYTES,     CASE       WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     OUTPUT_BYTES,     CASE       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN INPUT_BYTES = 0         THEN 0       ELSE         ROUND(OUTPUT_BYTES / INPUT_BYTES, 2)       END AS COMPRESSION_RATIO,     DELETED_INPUT_BYTES,     CASE       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_INPUT_BYTES_DISPLAY,     DELETED_OUTPUT_BYTES,     CASE       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_OUTPUT_BYTES_DISPLAY,     PATH,     COMMENT     FROM ( SELECT DEST_ID,        ROUND_ID,        INCARNATION,        DEST_NO,        STATUS,        START_SCN,        CHECKPOINT_SCN,        COMPATIBLE,        BASE_PIECE_ID,        USED_PIECE_ID,        PIECE_SWITCH_INTERVAL,        UNIT_SIZE,        COMPRESSION,        (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) AS INPUT_BYTES,        (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) AS OUTPUT_BYTES,        DELETED_INPUT_BYTES,        DELETED_OUTPUT_BYTES,        PATH,        COMMENT        FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS        WHERE TENANT_ID = EFFECTIVE_TENANT_ID() AND STATUS != 'STOP' UNION SELECT DEST_ID,        ROUND_ID,        INCARNATION,        DEST_NO,        'STOP' AS STATUS,        START_SCN,        CHECKPOINT_SCN,        COMPATIBLE,        BASE_PIECE_ID,        USED_PIECE_ID,        PIECE_SWITCH_INTERVAL,        UNIT_SIZE,        COMPRESSION,        INPUT_BYTES,        OUTPUT_BYTES,        DELETED_INPUT_BYTES,        DELETED_OUTPUT_BYTES,        PATH,        COMMENT        FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_HISTORY        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) )__"))) {
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

int ObInnerTableSchema::dba_ob_archivelog_piece_files_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARCHIVELOG_PIECE_FILES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARCHIVELOG_PIECE_FILES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     DEST_ID,     ROUND_ID,     PIECE_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     CASE       WHEN START_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(START_SCN)       END AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     CASE       WHEN CHECKPOINT_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(CHECKPOINT_SCN)       END AS CHECKPOINT_SCN_DISPLAY,     MAX_SCN,     END_SCN,     CASE       WHEN END_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(END_SCN)       END AS END_SCN_DISPLAY,     COMPATIBLE,     UNIT_SIZE,     COMPRESSION,     INPUT_BYTES,     CASE       WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     OUTPUT_BYTES,     CASE       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN INPUT_BYTES = 0         THEN 0       ELSE         ROUND(OUTPUT_BYTES / INPUT_BYTES, 2)       END AS COMPRESSION_RATIO,     FILE_STATUS,     PATH     FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_backup_parameter_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_BACKUP_PARAMETER_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_BACKUP_PARAMETER_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     NAME,     VALUE     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_PARAMETER     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::cdb_ob_archive_dest_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_ARCHIVE_DEST_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_ARCHIVE_DEST_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     DEST_NO,     NAME,     VALUE     FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER )__"))) {
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

int ObInnerTableSchema::cdb_ob_archivelog_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_ARCHIVELOG_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_ARCHIVELOG_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     DEST_ID,     ROUND_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     CASE       WHEN START_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(START_SCN)       END AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     CASE       WHEN CHECKPOINT_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(CHECKPOINT_SCN)       END AS CHECKPOINT_SCN_DISPLAY,     COMPATIBLE,     BASE_PIECE_ID,     USED_PIECE_ID,     PIECE_SWITCH_INTERVAL,     UNIT_SIZE,     COMPRESSION,     (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) AS INPUT_BYTES,     CASE       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024/1024/1024,2), 'PB')       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024/1024,2), 'TB')       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) >= 1024*1024*1024         THEN CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND((FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES)/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) AS OUTPUT_BYTES,     CASE       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024/1024/1024,2), 'PB')       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024/1024,2), 'TB')       WHEN (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) >= 1024*1024*1024         THEN CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES)/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) = 0         THEN 0       ELSE         ROUND((FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) / (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES), 2)       END AS COMPRESSION_RATIO,     DELETED_INPUT_BYTES,     CASE       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_INPUT_BYTES_DISPLAY,     DELETED_OUTPUT_BYTES,     CASE       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_OUTPUT_BYTES_DISPLAY,     COMMENT,     PATH     FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS )__"))) {
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

int ObInnerTableSchema::cdb_ob_archivelog_summary_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_ARCHIVELOG_SUMMARY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_ARCHIVELOG_SUMMARY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     DEST_ID,     ROUND_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     CASE       WHEN START_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(START_SCN)       END AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     CASE       WHEN CHECKPOINT_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(CHECKPOINT_SCN)       END AS CHECKPOINT_SCN_DISPLAY,     COMPATIBLE,     BASE_PIECE_ID,     USED_PIECE_ID,     PIECE_SWITCH_INTERVAL,     UNIT_SIZE,     COMPRESSION,     INPUT_BYTES,     CASE       WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     OUTPUT_BYTES,     CASE       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN INPUT_BYTES = 0         THEN 0       ELSE         ROUND(OUTPUT_BYTES / INPUT_BYTES, 2)       END AS COMPRESSION_RATIO,     DELETED_INPUT_BYTES,     CASE       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_INPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_INPUT_BYTES_DISPLAY,     DELETED_OUTPUT_BYTES,     CASE       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN DELETED_OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(DELETED_OUTPUT_BYTES/1024/1024,2), 'MB')       END AS DELETED_OUTPUT_BYTES_DISPLAY,     PATH,     COMMENT     FROM ( SELECT TENANT_ID,        DEST_ID,        ROUND_ID,        INCARNATION,        DEST_NO,        STATUS,        START_SCN,        CHECKPOINT_SCN,        COMPATIBLE,        BASE_PIECE_ID,        USED_PIECE_ID,        PIECE_SWITCH_INTERVAL,        UNIT_SIZE,        COMPRESSION,        (FROZEN_INPUT_BYTES + ACTIVE_INPUT_BYTES) AS INPUT_BYTES,        (FROZEN_OUTPUT_BYTES + ACTIVE_OUTPUT_BYTES) AS OUTPUT_BYTES,        DELETED_INPUT_BYTES,        DELETED_OUTPUT_BYTES,        PATH,        COMMENT        FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS        WHERE STATUS != 'STOP' UNION SELECT TENANT_ID,        DEST_ID,        ROUND_ID,        INCARNATION,        DEST_NO,        'STOP' AS STATUS,        START_SCN,        CHECKPOINT_SCN,        COMPATIBLE,        BASE_PIECE_ID,        USED_PIECE_ID,        PIECE_SWITCH_INTERVAL,        UNIT_SIZE,        COMPRESSION,        INPUT_BYTES,        OUTPUT_BYTES,        DELETED_INPUT_BYTES,        DELETED_OUTPUT_BYTES,        PATH,        COMMENT        FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_HISTORY) )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_parameter_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_PARAMETER_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_PARAMETER_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     NAME,     VALUE     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_PARAMETER )__"))) {
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

int ObInnerTableSchema::dba_ob_deadlock_event_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_DEADLOCK_EVENT_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_DEADLOCK_EVENT_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT EVENT_ID,          SVR_IP,          SVR_PORT,          DETECTOR_ID,          REPORT_TIME,          CYCLE_IDX,          CYCLE_SIZE,          ROLE,          PRIORITY_LEVEL,          PRIORITY,          CREATE_TIME,          START_DELAY AS START_DELAY_US,          MODULE,          VISITOR,          OBJECT,          EXTRA_NAME1,          EXTRA_VALUE1,          EXTRA_NAME2,          EXTRA_VALUE2,          EXTRA_NAME3,          EXTRA_VALUE3   FROM OCEANBASE.__ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY   WHERE TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::cdb_ob_deadlock_event_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_DEADLOCK_EVENT_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_DEADLOCK_EVENT_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          EVENT_ID,          SVR_IP,          SVR_PORT,          DETECTOR_ID,          REPORT_TIME,          CYCLE_IDX,          CYCLE_SIZE,          ROLE,          PRIORITY_LEVEL,          PRIORITY,          CREATE_TIME,          START_DELAY AS START_DELAY_US,          MODULE,          VISITOR,          OBJECT,          EXTRA_NAME1,          EXTRA_VALUE1,          EXTRA_NAME2,          EXTRA_VALUE2,          EXTRA_NAME3,          EXTRA_VALUE3   FROM OCEANBASE.__ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY   )__"))) {
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

int ObInnerTableSchema::cdb_ob_sys_variables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_SYS_VARIABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_SYS_VARIABLES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     a.GMT_CREATE AS CREATE_TIME,     a.GMT_MODIFIED AS MODIFY_TIME,     a.TENANT_ID as TENANT_ID,     a.NAME as NAME,     a.VALUE as VALUE,     a.MIN_VAL as MIN_VALUE,     a.MAX_VAL as MAX_VALUE,     CASE a.FLAGS & 0x3         WHEN 1 THEN "GLOBAL_ONLY"         WHEN 2 THEN "SESSION_ONLY"         WHEN 3 THEN "GLOBAL | SESSION"         ELSE NULL     END as SCOPE,     a.INFO as INFO,     b.DEFAULT_VALUE as DEFAULT_VALUE,     CAST (CASE WHEN a.VALUE = b.DEFAULT_VALUE           THEN 'YES'           ELSE 'NO'           END AS CHAR(3)) AS ISDEFAULT   FROM oceanbase.__all_virtual_sys_variable a   join oceanbase.__all_virtual_sys_variable_default_value b   where a.name = b.variable_name;   )__"))) {
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
