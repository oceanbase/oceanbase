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

int ObInnerTableSchema::cdb_part_indexes_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_PART_INDEXES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_PART_INDEXES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT /*+NO_USE_NL(I_T PKC)*/ CAST(I_T.TENANT_ID AS NUMBER) AS CON_ID, CAST(I_T.OWNER AS CHAR(128)) AS OWNER, CAST(I_T.INDEX_NAME AS CHAR(128)) AS INDEX_NAME, CAST(I_T.TABLE_NAME AS CHAR(128)) AS TABLE_NAME,  CAST(CASE I_T.PART_FUNC_TYPE      WHEN 0 THEN 'HASH'      WHEN 1 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'HASH' ELSE 'KEY' END)      WHEN 2 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'HASH' ELSE 'KEY' END)      WHEN 3 THEN 'RANGE'      WHEN 4 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'RANGE' ELSE 'RANGE COLUMNS' END)      WHEN 5 THEN 'LIST'      WHEN 6 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'LIST' ELSE 'LIST COLUMNS' END)      WHEN 7 THEN 'RANGE' END AS CHAR(13)) AS PARTITIONING_TYPE,  CAST(CASE WHEN I_T.PART_LEVEL < 2 THEN 'NONE'      ELSE (CASE I_T.SUB_PART_FUNC_TYPE            WHEN 0 THEN 'HASH'            WHEN 1 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'HASH' ELSE 'KEY' END)            WHEN 2 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'HASH' ELSE 'KEY' END)            WHEN 3 THEN 'RANGE'            WHEN 4 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'RANGE' ELSE 'RANGE COLUMNS' END)            WHEN 5 THEN 'LIST'            WHEN 6 THEN (CASE COMPATIBILITY_MODE WHEN 1 THEN 'LIST' ELSE 'LIST COLUMNS' END)            WHEN 7 THEN 'RANGE' END)      END AS CHAR(13)) AS SUBPARTITIONING_TYPE,  CAST(I_T.PART_NUM AS SIGNED) AS PARTITION_COUNT,  CAST(CASE WHEN (I_T.PART_LEVEL < 2 OR I_T.SUB_PART_TEMPLATE_FLAGS = 0) THEN 0      ELSE I_T.SUB_PART_NUM END AS SIGNED) AS DEF_SUBPARTITION_COUNT,  CAST(PKC.PARTITIONING_KEY_COUNT AS SIGNED) AS PARTITIONING_KEY_COUNT, CAST(PKC.SUBPARTITIONING_KEY_COUNT AS SIGNED) AS SUBPARTITIONING_KEY_COUNT,  CAST(CASE I_T.IS_LOCAL WHEN 1 THEN 'LOCAL'      ELSE 'GLOBAL' END AS CHAR(6)) AS LOCALITY,  CAST(CASE WHEN I_T.IS_LOCAL = 0 THEN 'PREFIXED'           WHEN (I_T.IS_LOCAL = 1 AND LOCAL_PARTITIONED_PREFIX_INDEX.IS_PREFIXED = 1) THEN 'PREFIXED'                     ELSE 'NON_PREFIXED' END AS CHAR(12)) AS ALIGNMENT,  CAST(TP.TABLESPACE_NAME AS CHAR(30)) AS DEF_TABLESPACE_NAME, CAST(0 AS SIGNED) AS DEF_PCT_FREE, CAST(0 AS SIGNED) AS DEF_INI_TRANS, CAST(0 AS SIGNED) AS DEF_MAX_TRANS, CAST(NULL AS CHAR(40)) AS DEF_INITIAL_EXTENT, CAST(NULL AS CHAR(40)) AS DEF_NEXT_EXTENT, CAST(NULL AS CHAR(40)) AS DEF_MIN_EXTENTS, CAST(NULL AS CHAR(40)) AS DEF_MAX_EXTENTS, CAST(NULL AS CHAR(40)) AS DEF_MAX_SIZE, CAST(NULL AS CHAR(40)) AS DEF_PCT_INCREASE, CAST(0 AS SIGNED) AS DEF_FREELISTS, CAST(0 AS SIGNED) AS DEF_FREELIST_GROUPS, CAST(NULL AS CHAR(7)) AS DEF_LOGGING, CAST(NULL AS CHAR(7)) AS DEF_BUFFER_POOL, CAST(NULL AS CHAR(7)) AS DEF_FLASH_CACHE, CAST(NULL AS CHAR(7)) AS DEF_CELL_FLASH_CACHE, CAST(NULL AS CHAR(1000)) AS DEF_PARAMETERS, CAST('NO' AS CHAR(1000)) AS "INTERVAL", CAST('NO' AS CHAR(3)) AS AUTOLIST, CAST(NULL AS CHAR(1000)) AS INTERVAL_SUBPARTITION, CAST(NULL AS CHAR(1000)) AS AUTOLIST_SUBPARTITION  FROM (SELECT D.TENANT_ID,         D.DATABASE_NAME AS OWNER,         I.TABLE_ID AS INDEX_ID,         CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME             ELSE SUBSTR(I.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(I.TABLE_NAME, 7)))             END AS CHAR(128)) AS INDEX_NAME,         I.PART_LEVEL,         I.PART_FUNC_TYPE,         I.PART_NUM,         I.SUB_PART_FUNC_TYPE,         T.TABLE_NAME AS TABLE_NAME,         T.SUB_PART_NUM,         T.SUB_PART_TEMPLATE_FLAGS,         T.TABLESPACE_ID,         (CASE I.INDEX_TYPE          WHEN 1 THEN 1          WHEN 2 THEN 1          WHEN 10 THEN 1          WHEN 15 THEN 1          WHEN 23 THEN 1          WHEN 24 THEN 1          ELSE 0 END) AS IS_LOCAL,         (CASE I.INDEX_TYPE          WHEN 1 THEN T.TABLE_ID          WHEN 2 THEN T.TABLE_ID          WHEN 10 THEN T.TABLE_ID          WHEN 15 THEN T.TABLE_ID          WHEN 23 THEN T.TABLE_ID          WHEN 24 THEN T.TABLE_ID          ELSE I.TABLE_ID END) AS JOIN_TABLE_ID  FROM OCEANBASE.__ALL_VIRTUAL_TABLE I  JOIN OCEANBASE.__ALL_VIRTUAL_TABLE T  ON I.TENANT_ID = T.TENANT_ID AND I.DATA_TABLE_ID = T.TABLE_ID  JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE D  ON T.TENANT_ID = D.TENANT_ID AND T.DATABASE_ID = D.DATABASE_ID  WHERE I.TABLE_TYPE = 5 AND I.INDEX_TYPE NOT IN (13, 14, 16, 17, 19, 20, 22) AND I.PART_LEVEL != 0  AND T.TABLE_MODE >> 12 & 15 in (0,1) ) I_T  JOIN OCEANBASE.__ALL_TENANT T ON I_T.TENANT_ID = T.TENANT_ID  LEFT JOIN (  SELECT I.TENANT_ID,                 I.TABLE_ID AS INDEX_ID,                 1 AS IS_PREFIXED  FROM OCEANBASE.__ALL_VIRTUAL_TABLE I  WHERE I.TABLE_TYPE = 5    AND I.INDEX_TYPE IN (1, 2, 10, 15, 23, 24)    AND I.PART_LEVEL != 0  AND NOT EXISTS  (SELECT /*+NO_USE_NL(PART_COLUMNS INDEX_COLUMNS)*/ *   FROM    (SELECT *     FROM OCEANBASE.__ALL_VIRTUAL_COLUMN C     WHERE C.TABLE_ID = I.DATA_TABLE_ID           AND C.TENANT_ID = I.TENANT_ID           AND C.PARTITION_KEY_POSITION != 0    ) PART_COLUMNS    LEFT JOIN    (SELECT *     FROM OCEANBASE.__ALL_VIRTUAL_COLUMN C     WHERE C.TABLE_ID = I.TABLE_ID           AND C.TENANT_ID = I.TENANT_ID           AND C.INDEX_POSITION != 0    ) INDEX_COLUMNS    ON PART_COLUMNS.COLUMN_ID = INDEX_COLUMNS.COLUMN_ID    WHERE    ((PART_COLUMNS.PARTITION_KEY_POSITION & 255) != 0     AND     (INDEX_COLUMNS.INDEX_POSITION IS NULL      OR (PART_COLUMNS.PARTITION_KEY_POSITION & 255) != INDEX_COLUMNS.INDEX_POSITION)    )    OR    ((PART_COLUMNS.PARTITION_KEY_POSITION & 65280)/256 != 0     AND (INDEX_COLUMNS.INDEX_POSITION IS NULL)    )  ) ) LOCAL_PARTITIONED_PREFIX_INDEX ON I_T.TENANT_ID = LOCAL_PARTITIONED_PREFIX_INDEX.TENANT_ID    AND I_T.INDEX_ID = LOCAL_PARTITIONED_PREFIX_INDEX.INDEX_ID  JOIN (SELECT    TENANT_ID,    TABLE_ID,    SUM(CASE WHEN (PARTITION_KEY_POSITION & 255) != 0 THEN 1 ELSE 0 END) AS PARTITIONING_KEY_COUNT,    SUM(CASE WHEN (PARTITION_KEY_POSITION & 65280)/256 != 0 THEN 1 ELSE 0 END) AS SUBPARTITIONING_KEY_COUNT    FROM OCEANBASE.__ALL_VIRTUAL_COLUMN    GROUP BY TENANT_ID, TABLE_ID) PKC ON I_T.TENANT_ID = PKC.TENANT_ID AND I_T.JOIN_TABLE_ID = PKC.TABLE_ID  LEFT JOIN OCEANBASE.__ALL_VIRTUAL_TENANT_TABLESPACE TP ON I_T.TENANT_ID = TP.TENANT_ID AND I_T.TABLESPACE_ID = TP.TABLESPACE_ID      )__"))) {
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

int ObInnerTableSchema::cdb_ind_partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_IND_PARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_IND_PARTITIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(D.TENANT_ID AS SIGNED) AS CON_ID,     CAST(D.DATABASE_NAME AS CHAR(128)) AS INDEX_OWNER,     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(I.TABLE_NAME, 7)))         END AS CHAR(128)) AS INDEX_NAME,     CAST(DT.TABLE_NAME AS CHAR(128)) AS TABLE_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN 'YES'          ELSE 'NO' END AS CHAR(3)) COMPOSITE,      CAST(PART.PART_NAME AS CHAR(128)) AS PARTITION_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN PART.SUB_PART_NUM          ELSE 0 END AS SIGNED)  SUBPARTITION_COUNT,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS CHAR(262144)) HIGH_VALUE,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS SIGNED) HIGH_VALUE_LENGTH,      CAST(PART.PARTITION_POSITION AS SIGNED) PARTITION_POSITION,     CAST(NULL AS CHAR(8)) AS STATUS,     CAST(NULL AS CHAR(30)) AS TABLESPACE_NAME,     CAST(NULL AS SIGNED) AS PCT_FREE,     CAST(NULL AS SIGNED) AS INI_TRANS,     CAST(NULL AS SIGNED) AS MAX_TRANS,     CAST(NULL AS SIGNED) AS INITIAL_EXTENT,     CAST(NULL AS SIGNED) AS NEXT_EXTENT,     CAST(NULL AS SIGNED) AS MIN_EXTENT,     CAST(NULL AS SIGNED) AS MAX_EXTENT,     CAST(NULL AS SIGNED) AS MAX_SIZE,     CAST(NULL AS SIGNED) AS PCT_INCREASE,     CAST(NULL AS SIGNED) AS FREELISTS,     CAST(NULL AS SIGNED) AS FREELIST_GROUPS,     CAST(NULL AS CHAR(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS CHAR(13)) AS COMPRESSION,     CAST(NULL AS SIGNED) AS BLEVEL,     CAST(NULL AS SIGNED) AS LEAF_BLOCKS,     CAST(NULL AS SIGNED) AS DISTINCT_KEYS,     CAST(NULL AS SIGNED) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS SIGNED) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS SIGNED) AS CLUSTERING_FACTOR,     CAST(NULL AS SIGNED) AS NUM_ROWS,     CAST(NULL AS SIGNED) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS CHAR(7)) AS BUFFER_POOL,     CAST(NULL AS CHAR(7)) AS FLASH_CACHE,     CAST(NULL AS CHAR(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS CHAR(3)) AS USER_STATS,     CAST(NULL AS SIGNED) AS PCT_DIRECT_ACCESS,     CAST(NULL AS CHAR(3)) AS GLOBAL_STATS,     CAST(NULL AS CHAR(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS CHAR(1000)) AS PARAMETERS,     CAST('NO' AS CHAR(3)) AS "INTERVAL",     CAST(NULL AS CHAR(3)) AS SEGMENT_CREATED,     CAST(NULL AS CHAR(3)) AS ORPHANED_ENTRIES     FROM     OCEANBASE.__ALL_VIRTUAL_TABLE I     JOIN OCEANBASE.__ALL_VIRTUAL_TABLE DT     ON I.TENANT_ID = DT.TENANT_ID AND I.DATA_TABLE_ID = DT.TABLE_ID     JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5        AND I.TABLE_MODE >> 12 & 15 in (0,1)      JOIN (SELECT TENANT_ID,                  TABLE_ID,                  PART_NAME,                  SUB_PART_NUM,                  HIGH_BOUND_VAL,                  LIST_VAL,                  COMPRESS_FUNC_NAME,                  ROW_NUMBER() OVER (                    PARTITION BY TENANT_ID, TABLE_ID                    ORDER BY PART_IDX, PART_ID ASC                  ) PARTITION_POSITION           FROM OCEANBASE.__ALL_VIRTUAL_PART) PART     ON I.TENANT_ID = PART.TENANT_ID        AND I.TABLE_ID = PART.TABLE_ID )__"))) {
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

int ObInnerTableSchema::cdb_ind_subpartitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_IND_SUBPARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_IND_SUBPARTITIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(D.TENANT_ID AS NUMBER) AS CON_ID,     CAST(D.DATABASE_NAME AS CHAR(128)) AS INDEX_OWNER,     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(I.TABLE_NAME, 7)))         END AS CHAR(128)) AS INDEX_NAME,     CAST(DT.TABLE_NAME AS CHAR(128)) AS TABLE_NAME,     CAST(PART.PART_NAME AS CHAR(128)) PARTITION_NAME,     CAST(PART.SUB_PART_NAME AS CHAR(128))  SUBPARTITION_NAME,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS CHAR(262144)) HIGH_VALUE,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS SIGNED) HIGH_VALUE_LENGTH,     CAST(PART.PARTITION_POSITION AS SIGNED) PARTITION_POSITION,     CAST(PART.SUBPARTITION_POSITION AS SIGNED) SUBPARTITION_POSITION,     CAST(NULL AS CHAR(8)) AS STATUS,     CAST(NULL AS CHAR(30)) AS TABLESPACE_NAME,     CAST(NULL AS SIGNED) AS PCT_FREE,     CAST(NULL AS SIGNED) AS INI_TRANS,     CAST(NULL AS SIGNED) AS MAX_TRANS,     CAST(NULL AS SIGNED) AS INITIAL_EXTENT,     CAST(NULL AS SIGNED) AS NEXT_EXTENT,     CAST(NULL AS SIGNED) AS MIN_EXTENT,     CAST(NULL AS SIGNED) AS MAX_EXTENT,     CAST(NULL AS SIGNED) AS MAX_SIZE,     CAST(NULL AS SIGNED) AS PCT_INCREASE,     CAST(NULL AS SIGNED) AS FREELISTS,     CAST(NULL AS SIGNED) AS FREELIST_GROUPS,     CAST(NULL AS CHAR(3)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS CHAR(13)) AS COMPRESSION,     CAST(NULL AS SIGNED) AS BLEVEL,     CAST(NULL AS SIGNED) AS LEAF_BLOCKS,     CAST(NULL AS SIGNED) AS DISTINCT_KEYS,     CAST(NULL AS SIGNED) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS SIGNED) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS SIGNED) AS CLUSTERING_FACTOR,     CAST(NULL AS SIGNED) AS NUM_ROWS,     CAST(NULL AS SIGNED) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS CHAR(7)) AS BUFFER_POOL,     CAST(NULL AS CHAR(7)) AS FLASH_CACHE,     CAST(NULL AS CHAR(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS CHAR(3)) AS USER_STATS,     CAST(NULL AS CHAR(3)) AS GLOBAL_STATS,     CAST('NO' AS CHAR(3)) AS "INTERVAL",     CAST(NULL AS CHAR(3)) AS SEGMENT_CREATED,     CAST(NULL AS CHAR(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS CHAR(1000)) AS PARAMETERS     FROM OCEANBASE.__ALL_VIRTUAL_TABLE I     JOIN OCEANBASE.__ALL_VIRTUAL_TABLE DT     ON I.TENANT_ID = DT.TENANT_ID AND I.DATA_TABLE_ID = DT.TABLE_ID     JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5        AND I.TABLE_MODE >> 12 & 15 in (0,1)     JOIN     (SELECT P_PART.TENANT_ID,             P_PART.TABLE_ID,             P_PART.PART_NAME,             P_PART.PARTITION_POSITION,             S_PART.SUB_PART_NAME,             S_PART.HIGH_BOUND_VAL,             S_PART.LIST_VAL,             S_PART.COMPRESS_FUNC_NAME,             S_PART.SUBPARTITION_POSITION      FROM (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              PART_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID                ORDER BY PART_IDX, PART_ID ASC              ) AS PARTITION_POSITION            FROM OCEANBASE.__ALL_VIRTUAL_PART) P_PART,           (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              SUB_PART_NAME,              HIGH_BOUND_VAL,              LIST_VAL,              COMPRESS_FUNC_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID, PART_ID                ORDER BY SUB_PART_IDX, SUB_PART_ID ASC              ) AS SUBPARTITION_POSITION            FROM OCEANBASE.__ALL_VIRTUAL_SUB_PART) S_PART      WHERE P_PART.PART_ID = S_PART.PART_ID AND            P_PART.TABLE_ID = S_PART.TABLE_ID            AND P_PART.TENANT_ID = S_PART.TENANT_ID) PART     ON I.TABLE_ID = PART.TABLE_ID AND I.TENANT_ID = PART.TENANT_ID )__"))) {
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

int ObInnerTableSchema::cdb_tab_col_statistics_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_TAB_COL_STATISTICS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_TAB_COL_STATISTICS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   CON_ID,   OWNER,   TABLE_NAME,   COLUMN_NAME,   NUM_DISTINCT,   LOW_VALUE,   HIGH_VALUE,   DENSITY,   NUM_NULLS,   NUM_BUCKETS,   LAST_ANALYZED,   SAMPLE_SIZE,   GLOBAL_STATS,   USER_STATS,   NOTES,   AVG_COL_LEN,   HISTOGRAM,   CAST(NULL AS CHAR(7)) SCOPE FROM OCEANBASE.CDB_TAB_COLS_V$ )__"))) {
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

int ObInnerTableSchema::dba_objects_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OBJECTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OBJECTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(B.DATABASE_NAME AS CHAR(128)) AS OWNER,       CAST(A.OBJECT_NAME AS CHAR(128)) AS OBJECT_NAME,       CAST(A.SUBOBJECT_NAME AS CHAR(128)) AS SUBOBJECT_NAME,       CAST(A.OBJECT_ID AS SIGNED) AS OBJECT_ID,       CAST(A.DATA_OBJECT_ID AS SIGNED) AS DATA_OBJECT_ID,       CAST(A.OBJECT_TYPE AS CHAR(23)) AS OBJECT_TYPE,       CAST(A.GMT_CREATE AS DATETIME) AS CREATED,       CAST(A.GMT_MODIFIED AS DATETIME) AS LAST_DDL_TIME,       CAST(A.GMT_CREATE AS DATETIME) AS TIMESTAMP,       CAST(A.STATUS AS CHAR(7)) AS STATUS,       CAST(A.TEMPORARY AS CHAR(1)) AS TEMPORARY,       CAST(A.`GENERATED` AS CHAR(1)) AS "GENERATED",       CAST(A.SECONDARY AS CHAR(1)) AS SECONDARY,       CAST(A.NAMESPACE AS SIGNED) AS NAMESPACE,       CAST(A.EDITION_NAME AS CHAR(128)) AS EDITION_NAME,       CAST(NULL AS CHAR(18)) AS SHARING,       CAST(NULL AS CHAR(1)) AS EDITIONABLE,       CAST(NULL AS CHAR(1)) AS ORACLE_MAINTAINED,       CAST(NULL AS CHAR(1)) AS APPLICATION,       CAST(NULL AS CHAR(1)) AS DEFAULT_COLLATION,       CAST(NULL AS CHAR(1)) AS DUPLICATED,       CAST(NULL AS CHAR(1)) AS SHARDED,       CAST(NULL AS CHAR(1)) AS IMPORTED_OBJECT,       CAST(NULL AS SIGNED) AS CREATED_APPID,       CAST(NULL AS SIGNED) AS CREATED_VSNID,       CAST(NULL AS SIGNED) AS MODIFIED_APPID,       CAST(NULL AS SIGNED) AS MODIFIED_VSNID     FROM (       SELECT CAST(0 AS SIGNED) AS TENANT_ID,              USEC_TO_TIME(B.SCHEMA_VERSION) AS GMT_CREATE,              USEC_TO_TIME(A.SCHEMA_VERSION) AS GMT_MODIFIED,              A.DATABASE_ID,              A.TABLE_NAME AS OBJECT_NAME,              NULL AS SUBOBJECT_NAME,              CAST(A.TABLE_ID AS SIGNED) AS OBJECT_ID,              A.TABLET_ID AS DATA_OBJECT_ID,              'TABLE' AS OBJECT_TYPE,              'VALID' AS STATUS,              'N' AS TEMPORARY,              'N' AS "GENERATED",              'N' AS SECONDARY,              0 AS NAMESPACE,              NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE A       JOIN OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE B         ON A.TENANT_ID = B.TENANT_ID AND B.TABLE_NAME = '__all_core_table'       WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()        UNION ALL        SELECT       TENANT_ID       ,GMT_CREATE       ,GMT_MODIFIED       ,DATABASE_ID       ,CAST((CASE              WHEN DATABASE_ID = 201004 THEN TABLE_NAME              WHEN TABLE_TYPE = 5 THEN SUBSTR(TABLE_NAME, 7 + POSITION('_' IN SUBSTR(TABLE_NAME, 7)))              ELSE TABLE_NAME END) AS CHAR(128)) AS OBJECT_NAME       ,NULL SUBOBJECT_NAME       ,CAST(TABLE_ID AS SIGNED) AS OBJECT_ID       ,(CASE WHEN TABLET_ID != 0 THEN TABLET_ID ELSE NULL END) DATA_OBJECT_ID       ,CASE WHEN TABLE_TYPE IN (0,3,6,8,9,14) THEN 'TABLE'             WHEN TABLE_TYPE IN (2) THEN 'VIRTUAL TABLE'             WHEN TABLE_TYPE IN (1,4) THEN 'VIEW'             WHEN TABLE_TYPE IN (5) THEN 'INDEX'             WHEN TABLE_TYPE IN (7) THEN 'MATERIALIZED VIEW'             WHEN TABLE_TYPE IN (15) THEN 'MATERIALIZED VIEW LOG'             ELSE NULL END AS OBJECT_TYPE       ,CAST(CASE WHEN TABLE_TYPE IN (5,15) THEN CASE WHEN INDEX_STATUS = 2 THEN 'VALID'               WHEN INDEX_STATUS = 3 THEN 'CHECKING'               WHEN INDEX_STATUS = 4 THEN 'INELEGIBLE'               WHEN INDEX_STATUS = 5 THEN 'ERROR'               ELSE 'UNUSABLE' END             ELSE  CASE WHEN OBJECT_STATUS = 1 THEN 'VALID' ELSE 'INVALID' END END AS CHAR(10)) AS STATUS       ,CASE WHEN TABLE_TYPE IN (6,8,9) THEN 'Y'           ELSE 'N' END AS TEMPORARY       ,CASE WHEN TABLE_TYPE IN (0,1) THEN 'Y'           ELSE 'N' END AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM       OCEANBASE.__ALL_TABLE       WHERE TENANT_ID = 0 AND TABLE_TYPE != 12 AND TABLE_TYPE != 13 AND TABLE_MODE >> 12 & 15 in (0,1)        UNION ALL        SELECT           CST.TENANT_ID          ,CST.GMT_CREATE          ,CST.GMT_MODIFIED          ,DB.DATABASE_ID          ,CST.constraint_name AS OBJECT_NAME          ,NULL AS SUBOBJECT_NAME          ,CAST(TBL.TABLE_ID AS SIGNED) AS OBJECT_ID          ,NULL AS DATA_OBJECT_ID          ,'INDEX' AS OBJECT_TYPE          ,'VALID' AS STATUS          ,'N' AS TEMPORARY          ,'N' AS "GENERATED"          ,'N' AS SECONDARY          ,0 AS NAMESPACE          ,NULL AS EDITION_NAME          FROM OCEANBASE.__ALL_CONSTRAINT CST, OCEANBASE.__ALL_TABLE TBL, OCEANBASE.__ALL_DATABASE DB          WHERE CST.TENANT_ID = 0 AND DB.DATABASE_ID = TBL.DATABASE_ID AND TBL.TABLE_ID = CST.TABLE_ID and CST.CONSTRAINT_TYPE = 1 and TBL.TABLE_MODE >> 12 & 15 in (0,1)        UNION ALL        SELECT       P.TENANT_ID       ,P.GMT_CREATE       ,P.GMT_MODIFIED       ,T.DATABASE_ID       ,CAST((CASE              WHEN T.DATABASE_ID = 201004 THEN T.TABLE_NAME              WHEN T.TABLE_TYPE = 5 THEN SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7)))              ELSE T.TABLE_NAME END) AS CHAR(128)) AS OBJECT_NAME       ,P.PART_NAME SUBOBJECT_NAME       ,P.PART_ID OBJECT_ID       ,CASE WHEN P.TABLET_ID != 0 THEN P.TABLET_ID ELSE NULL END AS DATA_OBJECT_ID       ,(CASE WHEN T.TABLE_TYPE = 5 THEN 'INDEX PARTITION' ELSE 'TABLE PARTITION' END) AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       , NULL AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_TABLE T JOIN OCEANBASE.__ALL_PART P ON T.TABLE_ID = P.TABLE_ID       WHERE T.TENANT_ID = 0 AND T.TENANT_ID = P.TENANT_ID AND T.TABLE_MODE >> 12 & 15 in (0,1)        UNION ALL        SELECT       SUBP.TENANT_ID       ,SUBP.GMT_CREATE       ,SUBP.GMT_MODIFIED       ,T.DATABASE_ID       ,CAST((CASE              WHEN T.DATABASE_ID = 201004 THEN T.TABLE_NAME              WHEN T.TABLE_TYPE = 5 THEN SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7)))              ELSE T.TABLE_NAME END) AS CHAR(128)) AS OBJECT_NAME       ,SUBP.SUB_PART_NAME SUBOBJECT_NAME       ,SUBP.SUB_PART_ID OBJECT_ID       ,SUBP.TABLET_ID AS DATA_OBJECT_ID       ,(CASE WHEN T.TABLE_TYPE = 5 THEN 'INDEX SUBPARTITION' ELSE 'TABLE SUBPARTITION' END) AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_TABLE T, OCEANBASE.__ALL_PART P,OCEANBASE.__ALL_SUB_PART SUBP       WHERE T.TABLE_ID =P.TABLE_ID AND P.TABLE_ID=SUBP.TABLE_ID AND P.PART_ID =SUBP.PART_ID       AND T.TENANT_ID = 0 AND T.TENANT_ID = P.TENANT_ID AND P.TENANT_ID = SUBP.TENANT_ID AND T.TABLE_MODE >> 12 & 15 in (0,1)        UNION ALL        SELECT       P.TENANT_ID       ,P.GMT_CREATE       ,P.GMT_MODIFIED       ,P.DATABASE_ID       ,P.PACKAGE_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,CAST(P.PACKAGE_ID AS SIGNED) AS OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,CASE WHEN TYPE = 1 THEN 'PACKAGE'             WHEN TYPE = 2 THEN 'PACKAGE BODY'             ELSE NULL END AS OBJECT_TYPE       ,CASE WHEN EXISTS                   (SELECT OBJ_ID FROM OCEANBASE.__ALL_TENANT_ERROR E                     WHERE P.TENANT_ID = E.TENANT_ID AND P.PACKAGE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 3 OR E.OBJ_TYPE = 5))                  THEN 'INVALID'             ELSE 'VALID' END AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_PACKAGE P       WHERE P.TENANT_ID = 0        UNION ALL        SELECT       R.TENANT_ID       ,R.GMT_CREATE       ,R.GMT_MODIFIED       ,R.DATABASE_ID       ,R.ROUTINE_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,CAST(R.ROUTINE_ID AS SIGNED) AS OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,CASE WHEN ROUTINE_TYPE = 1 THEN 'PROCEDURE'             WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'             ELSE NULL END AS OBJECT_TYPE       ,CASE WHEN EXISTS                   (SELECT OBJ_ID FROM OCEANBASE.__ALL_TENANT_ERROR E                     WHERE R.TENANT_ID = E.TENANT_ID AND R.ROUTINE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 9 OR E.OBJ_TYPE = 12))                  THEN 'INVALID'             ELSE 'VALID' END AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_ROUTINE R       WHERE (ROUTINE_TYPE = 1 OR ROUTINE_TYPE = 2) AND R.TENANT_ID = 0        UNION ALL        SELECT       TENANT_ID       ,GMT_CREATE       ,GMT_MODIFIED       ,DATABASE_ID       ,TYPE_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,CAST(TYPE_ID AS SIGNED) AS OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,'TYPE' AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_TYPE       WHERE TENANT_ID = 0        UNION ALL        SELECT       TENANT_ID       ,GMT_CREATE       ,GMT_MODIFIED       ,DATABASE_ID       ,OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,CAST(OBJECT_TYPE_ID AS SIGNED) AS OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,'TYPE BODY' AS OBJECT_TYPE       ,'VALID' AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_TENANT_OBJECT_TYPE       WHERE TENANT_ID = 0 AND TYPE = 2        UNION ALL        SELECT       TENANT_ID       ,T.GMT_CREATE       ,T.GMT_MODIFIED       ,T.DATABASE_ID       ,T.TRIGGER_NAME AS OBJECT_NAME       ,NULL AS SUBOBJECT_NAME       ,CAST(T.TRIGGER_ID AS SIGNED) AS OBJECT_ID       ,NULL AS DATA_OBJECT_ID       ,'TRIGGER' OBJECT_TYPE       ,CASE WHEN EXISTS                   (SELECT OBJ_ID FROM OCEANBASE.__ALL_TENANT_ERROR E                     WHERE T.TENANT_ID = E.TENANT_ID AND T.TRIGGER_ID = E.OBJ_ID AND (E.OBJ_TYPE = 7))                  THEN 'INVALID'             ELSE 'VALID' END AS STATUS       ,'N' AS TEMPORARY       ,'N' AS "GENERATED"       ,'N' AS SECONDARY       , 0 AS NAMESPACE       ,NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_TENANT_TRIGGER T       WHERE T.TENANT_ID = 0        UNION ALL        SELECT         TENANT_ID,         GMT_CREATE,         GMT_MODIFIED,         DATABASE_ID,         DATABASE_NAME AS OBJECT_NAME,         NULL AS SUBOBJECT_NAME,         CAST(DATABASE_ID AS SIGNED) AS OBJECT_ID,         NULL AS DATA_OBJECT_ID,         'DATABASE' AS OBJECT_TYPE,         'VALID' AS STATUS,         'N' AS TEMPORARY,         'N' AS "GENERATED",         'N' AS SECONDARY,         0 AS NAMESPACE,         NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_DATABASE       WHERE TENANT_ID = 0        UNION ALL        SELECT         TENANT_ID,         GMT_CREATE,         GMT_MODIFIED,         CAST(201001 AS SIGNED) AS DATABASE_ID,         TABLEGROUP_NAME AS OBJECT_NAME,         NULL AS SUBOBJECT_NAME,         CAST(TABLEGROUP_ID AS SIGNED) AS OBJECT_ID,         NULL AS DATA_OBJECT_ID,         'TABLEGROUP' AS OBJECT_TYPE,         'VALID' AS STATUS,         'N' AS TEMPORARY,         'N' AS "GENERATED",         'N' AS SECONDARY,         0 AS NAMESPACE,         NULL AS EDITION_NAME       FROM OCEANBASE.__ALL_TABLEGROUP       WHERE TENANT_ID = 0     ) A     JOIN OCEANBASE.__ALL_DATABASE B     ON A.TENANT_ID = B.TENANT_ID        AND A.DATABASE_ID = B.DATABASE_ID        AND B.TENANT_ID = 0 )__"))) {
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

int ObInnerTableSchema::dba_part_tables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PART_TABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PART_TABLES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(DB.DATABASE_NAME AS CHAR(128)) OWNER,          CAST(TB.TABLE_NAME AS CHAR(128)) TABLE_NAME,          CAST((CASE TB.PART_FUNC_TYPE               WHEN 0 THEN 'HASH'               WHEN 1 THEN 'KEY'               WHEN 2 THEN 'KEY'               WHEN 3 THEN 'RANGE'               WHEN 4 THEN 'RANGE COLUMNS'               WHEN 5 THEN 'LIST'               WHEN 6 THEN 'LIST COLUMNS'               WHEN 7 THEN 'RANGE' END)               AS CHAR(13)) PARTITIONING_TYPE,          CAST((CASE TB.PART_LEVEL                WHEN 1 THEN 'NONE'                WHEN 2 THEN                (CASE TB.SUB_PART_FUNC_TYPE                 WHEN 0 THEN 'HASH'                 WHEN 1 THEN 'KEY'                 WHEN 2 THEN 'KEY'                 WHEN 3 THEN 'RANGE'                 WHEN 4 THEN 'RANGE COLUMNS'                 WHEN 5 THEN 'LIST'                 WHEN 6 THEN 'LIST COLUMNS'                 WHEN 7 THEN 'RANGE' END) END)               AS CHAR(13)) SUBPARTITIONING_TYPE,          CAST((CASE TB.PART_FUNC_TYPE                WHEN 7 THEN 1048575                ELSE TB.PART_NUM END) AS SIGNED) PARTITION_COUNT,          CAST ((CASE TB.PART_LEVEL                 WHEN 1 THEN 0                 WHEN 2 THEN (CASE WHEN TB.SUB_PART_TEMPLATE_FLAGS > 0 THEN TB.SUB_PART_NUM ELSE 1 END)                 END) AS SIGNED) DEF_SUBPARTITION_COUNT,          CAST(PART_INFO.PART_KEY_COUNT AS SIGNED) PARTITIONING_KEY_COUNT,          CAST((CASE TB.PART_LEVEL               WHEN 1 THEN 0               WHEN 2 THEN PART_INFO.SUBPART_KEY_COUNT END)               AS SIGNED) SUBPARTITIONING_KEY_COUNT,          CAST(NULL AS CHAR(8)) STATUS,          CAST(TP.TABLESPACE_NAME AS CHAR(30)) DEF_TABLESPACE_NAME,          CAST(NULL AS SIGNED) DEF_PCT_FREE,          CAST(NULL AS SIGNED) DEF_PCT_USED,          CAST(NULL AS SIGNED) DEF_INI_TRANS,          CAST(NULL AS SIGNED) DEF_MAX_TRANS,          CAST(NULL AS CHAR(40)) DEF_INITIAL_EXTENT,          CAST(NULL AS CHAR(40)) DEF_NEXT_EXTENT,          CAST(NULL AS CHAR(40)) DEF_MIN_EXTENTS,          CAST(NULL AS CHAR(40)) DEF_MAX_EXTENTS,          CAST(NULL AS CHAR(40)) DEF_MAX_SIZE,          CAST(NULL AS CHAR(40)) DEF_PCT_INCREASE,          CAST(NULL AS SIGNED) DEF_FREELISTS,          CAST(NULL AS SIGNED) DEF_FREELIST_GROUPS,          CAST(NULL AS CHAR(7)) DEF_LOGGING,          CAST(CASE WHEN TB.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED'               ELSE 'ENABLED' END AS CHAR(8)) DEF_COMPRESSION,          CAST(TB.COMPRESS_FUNC_NAME AS CHAR(12)) DEF_COMPRESS_FOR,          CAST(NULL AS CHAR(7)) DEF_BUFFER_POOL,          CAST(NULL AS CHAR(7)) DEF_FLASH_CACHE,          CAST(NULL AS CHAR(7)) DEF_CELL_FLASH_CACHE,          CAST(NULL AS CHAR(30)) REF_PTN_CONSTRAINT_NAME,          CAST(TB.INTERVAL_RANGE AS CHAR(1000)) "INTERVAL",          CAST('NO' AS CHAR(3)) AUTOLIST,          CAST(NULL AS CHAR(1000)) INTERVAL_SUBPARTITION,          CAST('NO' AS CHAR(3)) AUTOLIST_SUBPARTITION,          CAST(NULL AS CHAR(3)) IS_NESTED,          CAST(NULL AS CHAR(4)) DEF_SEGMENT_CREATED,          CAST(NULL AS CHAR(3)) DEF_INDEXING,          CAST(NULL AS CHAR(8)) DEF_INMEMORY,          CAST(NULL AS CHAR(8)) DEF_INMEMORY_PRIORITY,          CAST(NULL AS CHAR(15)) DEF_INMEMORY_DISTRIBUTE,          CAST(NULL AS CHAR(17)) DEF_INMEMORY_COMPRESSION,          CAST(NULL AS CHAR(13)) DEF_INMEMORY_DUPLICATE,          CAST(NULL AS CHAR(3)) DEF_READ_ONLY,          CAST(NULL AS CHAR(24)) DEF_CELLMEMORY,          CAST(NULL AS CHAR(12)) DEF_INMEMORY_SERVICE,          CAST(NULL AS CHAR(1000)) DEF_INMEMORY_SERVICE_NAME,          CAST('NO' AS CHAR(3)) AUTO       FROM OCEANBASE.__ALL_TABLE TB       JOIN OCEANBASE.__ALL_DATABASE DB       ON TB.TENANT_ID = DB.TENANT_ID AND TB.DATABASE_ID = DB.DATABASE_ID       JOIN         (SELECT          TENANT_ID,          TABLE_ID,          SUM(CASE WHEN (PARTITION_KEY_POSITION & 255) > 0 THEN 1 ELSE 0 END) AS PART_KEY_COUNT,          SUM(CASE WHEN (PARTITION_KEY_POSITION & 65280) > 0 THEN 1 ELSE 0 END) AS SUBPART_KEY_COUNT          FROM OCEANBASE.__ALL_COLUMN          WHERE PARTITION_KEY_POSITION > 0          GROUP BY TENANT_ID, TABLE_ID) PART_INFO       ON TB.TENANT_ID = PART_INFO.TENANT_ID AND TB.TABLE_ID = PART_INFO.TABLE_ID       LEFT JOIN OCEANBASE.__ALL_TENANT_TABLESPACE TP       ON TB.TENANT_ID = TP.TENANT_ID AND TP.TABLESPACE_ID = TB.TABLESPACE_ID       WHERE TB.TENANT_ID = 0             AND TB.TABLE_TYPE IN (3, 6)             AND TB.PART_LEVEL != 0             AND TB.TABLE_MODE >> 12 & 15 in (0,1)   )__"))) {
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

int ObInnerTableSchema::dba_part_key_columns_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PART_KEY_COLUMNS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT  CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(T.TABLE_NAME AS CHAR(128)) AS NAME,             CAST('TABLE' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 255) AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_COLUMN C, OCEANBASE.__ALL_TABLE T, OCEANBASE.__ALL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND (C.PARTITION_KEY_POSITION & 255) > 0           AND T.TABLE_TYPE IN (3, 6)           AND T.TABLE_MODE >> 12 & 15 in (0,1)           AND C.TENANT_ID = 0     UNION     SELECT  CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7))) END AS CHAR(128)) AS NAME,             CAST('INDEX' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 255) AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_COLUMN C, OCEANBASE.__ALL_TABLE T, OCEANBASE.__ALL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE NOT IN (17,19,20,22)           AND (C.PARTITION_KEY_POSITION & 255) > 0           AND C.TENANT_ID = 0     UNION     SELECT  CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7))) END AS CHAR(128)) AS NAME,             CAST('INDEX' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 255) AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_COLUMN C, OCEANBASE.__ALL_TABLE T, OCEANBASE.__ALL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2,10,15,23,24)           AND (C.PARTITION_KEY_POSITION & 255) > 0           AND C.TENANT_ID = 0 )__"))) {
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

int ObInnerTableSchema::dba_subpart_key_columns_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SUBPART_KEY_COLUMNS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT  CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(T.TABLE_NAME AS CHAR(128)) AS NAME,             CAST('TABLE' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 65280)/256 AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_COLUMN C, OCEANBASE.__ALL_TABLE T, OCEANBASE.__ALL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND (C.PARTITION_KEY_POSITION & 65280) > 0           AND T.TABLE_TYPE IN (3, 6)           AND T.TABLE_MODE >> 12 & 15 in (0,1)           AND C.TENANT_ID = 0     UNION     SELECT  CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7))) END AS CHAR(128)) AS NAME,             CAST('INDEX' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 65280)/256 AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_COLUMN C, OCEANBASE.__ALL_TABLE T, OCEANBASE.__ALL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE NOT IN (17,19,20,22)           AND (C.PARTITION_KEY_POSITION & 65280) > 0           AND C.TENANT_ID = 0     UNION     SELECT  CAST(D.DATABASE_NAME AS CHAR(128)) AS OWNER,             CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME                 ELSE SUBSTR(T.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(T.TABLE_NAME, 7))) END AS CHAR(128)) AS NAME,             CAST('INDEX' AS CHAR(5)) AS OBJECT_TYPE,             CAST(C.COLUMN_NAME AS CHAR(4000)) AS COLUMN_NAME,             CAST((C.PARTITION_KEY_POSITION & 65280)/256 AS SIGNED) AS COLUMN_POSITION,             CAST(NULL AS SIGNED) AS COLLATED_COLUMN_ID     FROM OCEANBASE.__ALL_COLUMN C, OCEANBASE.__ALL_TABLE T, OCEANBASE.__ALL_DATABASE D     WHERE C.TENANT_ID = T.TENANT_ID           AND T.TENANT_ID = D.TENANT_ID           AND T.DATABASE_ID = D.DATABASE_ID           AND C.TABLE_ID = T.DATA_TABLE_ID           AND T.TABLE_TYPE = 5           AND T.INDEX_TYPE IN (1,2,10,15,23,24)           AND (C.PARTITION_KEY_POSITION & 65280) > 0           AND C.TENANT_ID = 0 )__"))) {
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

int ObInnerTableSchema::dba_tab_partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_PARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_PARTITIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CAST(DB_TB.DATABASE_NAME AS CHAR(128)) TABLE_OWNER,       CAST(DB_TB.TABLE_NAME AS CHAR(128)) TABLE_NAME,        CAST(CASE DB_TB.PART_LEVEL            WHEN 2 THEN 'YES'            ELSE 'NO' END AS CHAR(3)) COMPOSITE,        CAST(PART.PART_NAME AS CHAR(128)) PARTITION_NAME,        CAST(CASE DB_TB.PART_LEVEL            WHEN 2 THEN PART.SUB_PART_NUM            ELSE 0 END AS SIGNED)  SUBPARTITION_COUNT,        CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL            ELSE PART.LIST_VAL END AS CHAR(262144)) HIGH_VALUE,        CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)            ELSE length(PART.LIST_VAL) END AS SIGNED) HIGH_VALUE_LENGTH,        CAST(PART.PARTITION_POSITION AS SIGNED) PARTITION_POSITION,       CAST(TP.TABLESPACE_NAME AS CHAR(30)) TABLESPACE_NAME,       CAST(NULL AS SIGNED) PCT_FREE,       CAST(NULL AS SIGNED) PCT_USED,       CAST(NULL AS SIGNED) INI_TRANS,       CAST(NULL AS SIGNED) MAX_TRANS,       CAST(NULL AS SIGNED) INITIAL_EXTENT,       CAST(NULL AS SIGNED) NEXT_EXTENT,       CAST(NULL AS SIGNED) MIN_EXTENT,       CAST(NULL AS SIGNED) MAX_EXTENT,       CAST(NULL AS SIGNED) MAX_SIZE,       CAST(NULL AS SIGNED) PCT_INCREASE,       CAST(NULL AS SIGNED) FREELISTS,       CAST(NULL AS SIGNED) FREELIST_GROUPS,       CAST(NULL AS CHAR(7)) LOGGING,        CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED'            ELSE 'ENABLED' END AS CHAR(8)) COMPRESSION,        CAST(PART.COMPRESS_FUNC_NAME AS CHAR(30)) COMPRESS_FOR,       CAST(NULL AS SIGNED) NUM_ROWS,       CAST(NULL AS SIGNED) BLOCKS,       CAST(NULL AS SIGNED) EMPTY_BLOCKS,       CAST(NULL AS SIGNED) AVG_SPACE,       CAST(NULL AS SIGNED) CHAIN_CNT,       CAST(NULL AS SIGNED) AVG_ROW_LEN,       CAST(NULL AS SIGNED) SAMPLE_SIZE,       CAST(NULL AS DATE) LAST_ANALYZED,       CAST(NULL AS CHAR(7)) BUFFER_POOL,       CAST(NULL AS CHAR(7)) FLASH_CACHE,       CAST(NULL AS CHAR(7)) CELL_FLASH_CACHE,       CAST(NULL AS CHAR(3)) GLOBAL_STATS,       CAST(NULL AS CHAR(3)) USER_STATS,       CAST(NULL AS CHAR(3)) IS_NESTED,       CAST(NULL AS CHAR(128)) PARENT_TABLE_PARTITION,        CAST (CASE WHEN PART.PARTITION_POSITION >             MAX (CASE WHEN PART.HIGH_BOUND_VAL = DB_TB.B_TRANSITION_POINT                  THEN PART.PARTITION_POSITION ELSE NULL END)             OVER(PARTITION BY DB_TB.TABLE_ID)             THEN 'YES' ELSE 'NO' END AS CHAR(3)) "INTERVAL",        CAST(NULL AS CHAR(4)) SEGMENT_CREATED,       CAST(NULL AS CHAR(4)) INDEXING,       CAST(NULL AS CHAR(4)) READ_ONLY,       CAST(NULL AS CHAR(8)) INMEMORY,       CAST(NULL AS CHAR(8)) INMEMORY_PRIORITY,       CAST(NULL AS CHAR(15)) INMEMORY_DISTRIBUTE,       CAST(NULL AS CHAR(17)) INMEMORY_COMPRESSION,       CAST(NULL AS CHAR(13)) INMEMORY_DUPLICATE,       CAST(NULL AS CHAR(24)) CELLMEMORY,       CAST(NULL AS CHAR(12)) INMEMORY_SERVICE,       CAST(NULL AS CHAR(100)) INMEMORY_SERVICE_NAME,       CAST(NULL AS CHAR(8)) MEMOPTIMIZE_READ,       CAST(NULL AS CHAR(8)) MEMOPTIMIZE_WRITE        FROM (SELECT DB.TENANT_ID,                    DB.DATABASE_NAME,                    DB.DATABASE_ID,                    TB.TABLE_ID,                    TB.TABLE_NAME,                    TB.B_TRANSITION_POINT,                    TB.PART_LEVEL             FROM OCEANBASE.__ALL_TABLE TB,                  OCEANBASE.__ALL_DATABASE DB             WHERE TB.DATABASE_ID = DB.DATABASE_ID               AND TB.TENANT_ID = DB.TENANT_ID               AND TB.TABLE_TYPE in (3, 6)               AND TB.TABLE_MODE >> 12 & 15 in (0,1)            ) DB_TB       JOIN (SELECT TENANT_ID,                    TABLE_ID,                    PART_NAME,                    SUB_PART_NUM,                    HIGH_BOUND_VAL,                    LIST_VAL,                    COMPRESS_FUNC_NAME,                    TABLESPACE_ID,                    ROW_NUMBER() OVER (                      PARTITION BY TENANT_ID, TABLE_ID                      ORDER BY PART_IDX, PART_ID ASC                    ) PARTITION_POSITION             FROM OCEANBASE.__ALL_PART) PART       ON DB_TB.TABLE_ID = PART.TABLE_ID AND PART.TENANT_ID = DB_TB.TENANT_ID        LEFT JOIN OCEANBASE.__ALL_TENANT_TABLESPACE TP       ON TP.TABLESPACE_ID = PART.TABLESPACE_ID AND TP.TENANT_ID = PART.TENANT_ID        WHERE DB_TB.TENANT_ID = 0 )__"))) {
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

int ObInnerTableSchema::dba_tab_subpartitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_SUBPARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_SUBPARTITIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CAST(DB_TB.DATABASE_NAME AS CHAR(128)) TABLE_OWNER,       CAST(DB_TB.TABLE_NAME AS CHAR(128)) TABLE_NAME,       CAST(PART.PART_NAME AS CHAR(128)) PARTITION_NAME,       CAST(PART.SUB_PART_NAME AS CHAR(128))  SUBPARTITION_NAME,       CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL            ELSE PART.LIST_VAL END AS CHAR(262144)) HIGH_VALUE,       CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)            ELSE length(PART.LIST_VAL) END AS SIGNED) HIGH_VALUE_LENGTH,       CAST(PART.PARTITION_POSITION AS SIGNED) PARTITION_POSITION,       CAST(PART.SUBPARTITION_POSITION AS SIGNED) SUBPARTITION_POSITION,       CAST(TP.TABLESPACE_NAME AS CHAR(30)) TABLESPACE_NAME,       CAST(NULL AS SIGNED) PCT_FREE,       CAST(NULL AS SIGNED) PCT_USED,       CAST(NULL AS SIGNED) INI_TRANS,       CAST(NULL AS SIGNED) MAX_TRANS,       CAST(NULL AS SIGNED) INITIAL_EXTENT,       CAST(NULL AS SIGNED) NEXT_EXTENT,       CAST(NULL AS SIGNED) MIN_EXTENT,       CAST(NULL AS SIGNED) MAX_EXTENT,       CAST(NULL AS SIGNED) MAX_SIZE,       CAST(NULL AS SIGNED) PCT_INCREASE,       CAST(NULL AS SIGNED) FREELISTS,       CAST(NULL AS SIGNED) FREELIST_GROUPS,       CAST(NULL AS CHAR(3)) LOGGING,       CAST(CASE WHEN       PART.COMPRESS_FUNC_NAME IS NULL THEN       'DISABLED' ELSE 'ENABLED' END AS CHAR(8)) COMPRESSION,       CAST(PART.COMPRESS_FUNC_NAME AS CHAR(30)) COMPRESS_FOR,       CAST(NULL AS SIGNED) NUM_ROWS,       CAST(NULL AS SIGNED) BLOCKS,       CAST(NULL AS SIGNED) EMPTY_BLOCKS,       CAST(NULL AS SIGNED) AVG_SPACE,       CAST(NULL AS SIGNED) CHAIN_CNT,       CAST(NULL AS SIGNED) AVG_ROW_LEN,       CAST(NULL AS SIGNED) SAMPLE_SIZE,       CAST(NULL AS DATE) LAST_ANALYZED,       CAST(NULL AS CHAR(7)) BUFFER_POOL,       CAST(NULL AS CHAR(7)) FLASH_CACHE,       CAST(NULL AS CHAR(7)) CELL_FLASH_CACHE,       CAST(NULL AS CHAR(3)) GLOBAL_STATS,       CAST(NULL AS CHAR(3)) USER_STATS,       CAST('NO' AS CHAR(3)) "INTERVAL",       CAST(NULL AS CHAR(3)) SEGMENT_CREATED,       CAST(NULL AS CHAR(3)) INDEXING,       CAST(NULL AS CHAR(3)) READ_ONLY,       CAST(NULL AS CHAR(8)) INMEMORY,       CAST(NULL AS CHAR(8)) INMEMORY_PRIORITY,       CAST(NULL AS CHAR(15)) INMEMORY_DISTRIBUTE,       CAST(NULL AS CHAR(17)) INMEMORY_COMPRESSION,       CAST(NULL AS CHAR(13)) INMEMORY_DUPLICATE,       CAST(NULL AS CHAR(12)) INMEMORY_SERVICE,       CAST(NULL AS CHAR(1000)) INMEMORY_SERVICE_NAME,       CAST(NULL AS CHAR(24)) CELLMEMORY,       CAST(NULL AS CHAR(8)) MEMOPTIMIZE_READ,       CAST(NULL AS CHAR(8)) MEMOPTIMIZE_WRITE       FROM       (SELECT DB.TENANT_ID,               DB.DATABASE_NAME,               DB.DATABASE_ID,               TB.TABLE_ID,               TB.TABLE_NAME        FROM  OCEANBASE.__ALL_TABLE TB,              OCEANBASE.__ALL_DATABASE DB        WHERE TB.DATABASE_ID = DB.DATABASE_ID          AND TB.TABLE_MODE >> 12 & 15 in (0,1)          AND TB.TENANT_ID = DB.TENANT_ID          AND TB.TABLE_TYPE IN (3, 6)) DB_TB       JOIN       (SELECT P_PART.TENANT_ID,               P_PART.TABLE_ID,               P_PART.PART_NAME,               P_PART.PARTITION_POSITION,               S_PART.SUB_PART_NAME,               S_PART.HIGH_BOUND_VAL,               S_PART.LIST_VAL,               S_PART.COMPRESS_FUNC_NAME,               S_PART.TABLESPACE_ID,               S_PART.SUBPARTITION_POSITION        FROM (SELECT                TENANT_ID,                TABLE_ID,                PART_ID,                PART_NAME,                ROW_NUMBER() OVER (                  PARTITION BY TENANT_ID, TABLE_ID                  ORDER BY PART_IDX, PART_ID ASC                ) AS PARTITION_POSITION              FROM OCEANBASE.__ALL_PART) P_PART,             (SELECT                TENANT_ID,                TABLE_ID,                PART_ID,                SUB_PART_NAME,                HIGH_BOUND_VAL,                LIST_VAL,                COMPRESS_FUNC_NAME,                TABLESPACE_ID,                ROW_NUMBER() OVER (                  PARTITION BY TENANT_ID, TABLE_ID, PART_ID                  ORDER BY SUB_PART_IDX, SUB_PART_ID ASC                ) AS SUBPARTITION_POSITION              FROM OCEANBASE.__ALL_SUB_PART) S_PART        WHERE P_PART.PART_ID = S_PART.PART_ID AND              P_PART.TABLE_ID = S_PART.TABLE_ID              AND P_PART.TENANT_ID = S_PART.TENANT_ID) PART       ON DB_TB.TABLE_ID = PART.TABLE_ID AND DB_TB.TENANT_ID = PART.TENANT_ID        LEFT JOIN         OCEANBASE.__ALL_TENANT_TABLESPACE TP       ON TP.TABLESPACE_ID = PART.TABLESPACE_ID AND TP.TENANT_ID = PART.TENANT_ID        WHERE DB_TB.TENANT_ID = 0 )__"))) {
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

int ObInnerTableSchema::dba_subpartition_templates_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SUBPARTITION_TEMPLATES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SUBPARTITION_TEMPLATES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CAST(DB.DATABASE_NAME AS CHAR(128)) USER_NAME,       CAST(TB.TABLE_NAME AS CHAR(128)) TABLE_NAME,       CAST(SP.SUB_PART_NAME AS CHAR(132)) SUBPARTITION_NAME,       CAST(SP.SUB_PART_ID + 1 AS SIGNED) SUBPARTITION_POSITION,       CAST(TP.TABLESPACE_NAME AS CHAR(30)) TABLESPACE_NAME,       CAST(CASE WHEN SP.HIGH_BOUND_VAL IS NULL THEN SP.LIST_VAL            ELSE SP.HIGH_BOUND_VAL END AS CHAR(262144)) HIGH_BOUND,       CAST(NULL AS CHAR(4)) COMPRESSION,       CAST(NULL AS CHAR(4)) INDEXING,       CAST(NULL AS CHAR(4)) READ_ONLY        FROM OCEANBASE.__ALL_DATABASE DB        JOIN OCEANBASE.__ALL_TABLE TB       ON DB.DATABASE_ID = TB.DATABASE_ID AND DB.TENANT_ID = TB.TENANT_ID          AND TB.TABLE_TYPE IN (3, 6)          AND TB.TABLE_MODE >> 12 & 15 in (0,1)        JOIN OCEANBASE.__ALL_DEF_SUB_PART SP       ON TB.TABLE_ID = SP.TABLE_ID AND SP.TENANT_ID = TB.TENANT_ID        LEFT JOIN OCEANBASE.__ALL_TENANT_TABLESPACE TP       ON TP.TABLESPACE_ID = SP.TABLESPACE_ID AND TP.TENANT_ID = SP.TENANT_ID        WHERE DB.TENANT_ID = 0       )__"))) {
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

int ObInnerTableSchema::dba_part_indexes_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PART_INDEXES_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT CAST(I_T.OWNER AS CHAR(128)) AS OWNER, CAST(I_T.INDEX_NAME AS CHAR(128)) AS INDEX_NAME, CAST(I_T.TABLE_NAME AS CHAR(128)) AS TABLE_NAME,  CAST(CASE I_T.PART_FUNC_TYPE      WHEN 0 THEN 'HASH'      WHEN 1 THEN 'KEY'      WHEN 2 THEN 'KEY'      WHEN 3 THEN 'RANGE'      WHEN 4 THEN 'RANGE COLUMNS'      WHEN 5 THEN 'LIST'      WHEN 6 THEN 'LIST COLUMNS'      WHEN 7 THEN 'RANGE' END AS CHAR(13)) AS PARTITIONING_TYPE,  CAST(CASE WHEN I_T.PART_LEVEL < 2 THEN 'NONE'      ELSE (CASE I_T.SUB_PART_FUNC_TYPE            WHEN 0 THEN 'HASH'            WHEN 1 THEN 'KEY'            WHEN 2 THEN 'KEY'            WHEN 3 THEN 'RANGE'            WHEN 4 THEN 'RANGE COLUMNS'            WHEN 5 THEN 'LIST'            WHEN 6 THEN 'LIST COLUMNS'            WHEN 7 THEN 'RANGE' END)      END AS CHAR(13)) AS SUBPARTITIONING_TYPE,  CAST(I_T.PART_NUM AS SIGNED) AS PARTITION_COUNT,  CAST(CASE WHEN (I_T.PART_LEVEL < 2 OR I_T.SUB_PART_TEMPLATE_FLAGS = 0) THEN 0      ELSE I_T.SUB_PART_NUM END AS SIGNED) AS DEF_SUBPARTITION_COUNT,  CAST(PKC.PARTITIONING_KEY_COUNT AS SIGNED) AS PARTITIONING_KEY_COUNT, CAST(PKC.SUBPARTITIONING_KEY_COUNT AS SIGNED) AS SUBPARTITIONING_KEY_COUNT,  CAST(CASE I_T.IS_LOCAL WHEN 1 THEN 'LOCAL'      ELSE 'GLOBAL' END AS CHAR(6)) AS LOCALITY,  CAST(CASE WHEN I_T.IS_LOCAL = 0 THEN 'PREFIXED'           WHEN (I_T.IS_LOCAL = 1 AND LOCAL_PARTITIONED_PREFIX_INDEX.IS_PREFIXED = 1) THEN 'PREFIXED'           ELSE 'NON_PREFIXED' END AS CHAR(12)) AS ALIGNMENT,  CAST(TP.TABLESPACE_NAME AS CHAR(30)) AS DEF_TABLESPACE_NAME, CAST(0 AS SIGNED) AS DEF_PCT_FREE, CAST(0 AS SIGNED) AS DEF_INI_TRANS, CAST(0 AS SIGNED) AS DEF_MAX_TRANS, CAST(NULL AS CHAR(40)) AS DEF_INITIAL_EXTENT, CAST(NULL AS CHAR(40)) AS DEF_NEXT_EXTENT, CAST(NULL AS CHAR(40)) AS DEF_MIN_EXTENTS, CAST(NULL AS CHAR(40)) AS DEF_MAX_EXTENTS, CAST(NULL AS CHAR(40)) AS DEF_MAX_SIZE, CAST(NULL AS CHAR(40)) AS DEF_PCT_INCREASE, CAST(0 AS SIGNED) AS DEF_FREELISTS, CAST(0 AS SIGNED) AS DEF_FREELIST_GROUPS, CAST(NULL AS CHAR(7)) AS DEF_LOGGING, CAST(NULL AS CHAR(7)) AS DEF_BUFFER_POOL, CAST(NULL AS CHAR(7)) AS DEF_FLASH_CACHE, CAST(NULL AS CHAR(7)) AS DEF_CELL_FLASH_CACHE, CAST(NULL AS CHAR(1000)) AS DEF_PARAMETERS, CAST('NO' AS CHAR(1000)) AS "INTERVAL", CAST('NO' AS CHAR(3)) AS AUTOLIST, CAST(NULL AS CHAR(1000)) AS INTERVAL_SUBPARTITION, CAST(NULL AS CHAR(1000)) AS AUTOLIST_SUBPARTITION  FROM (SELECT D.TENANT_ID,         D.DATABASE_NAME AS OWNER,         I.TABLE_ID AS INDEX_ID,         CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME             ELSE SUBSTR(I.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(I.TABLE_NAME, 7)))             END AS CHAR(128)) AS INDEX_NAME,         I.PART_LEVEL,         I.PART_FUNC_TYPE,         I.PART_NUM,         I.SUB_PART_FUNC_TYPE,         T.TABLE_NAME AS TABLE_NAME,         T.SUB_PART_NUM,         T.SUB_PART_TEMPLATE_FLAGS,         T.TABLESPACE_ID,         (CASE I.INDEX_TYPE          WHEN 1 THEN 1          WHEN 2 THEN 1          WHEN 10 THEN 1          WHEN 15 THEN 1          WHEN 23 THEN 1          WHEN 24 THEN 1          ELSE 0 END) AS IS_LOCAL,         (CASE I.INDEX_TYPE          WHEN 1 THEN T.TABLE_ID          WHEN 2 THEN T.TABLE_ID          WHEN 10 THEN T.TABLE_ID          WHEN 15 THEN T.TABLE_ID          WHEN 23 THEN T.TABLE_ID          WHEN 24 THEN T.TABLE_ID          ELSE I.TABLE_ID END) AS JOIN_TABLE_ID  FROM OCEANBASE.__ALL_TABLE I  JOIN OCEANBASE.__ALL_TABLE T  ON I.TENANT_ID = T.TENANT_ID AND I.DATA_TABLE_ID = T.TABLE_ID  JOIN OCEANBASE.__ALL_DATABASE D  ON T.TENANT_ID = D.TENANT_ID AND T.DATABASE_ID = D.DATABASE_ID  WHERE I.TABLE_TYPE = 5 AND I.INDEX_TYPE NOT IN (13, 14, 16, 17, 19, 20, 22) AND I.PART_LEVEL != 0  AND I.TABLE_MODE >> 12 & 15 in (0,1) ) I_T  JOIN (SELECT    TENANT_ID,    TABLE_ID,    SUM(CASE WHEN (PARTITION_KEY_POSITION & 255) != 0 THEN 1 ELSE 0 END) AS PARTITIONING_KEY_COUNT,    SUM(CASE WHEN (PARTITION_KEY_POSITION & 65280)/256 != 0 THEN 1 ELSE 0 END) AS SUBPARTITIONING_KEY_COUNT    FROM OCEANBASE.__ALL_COLUMN    GROUP BY TENANT_ID, TABLE_ID) PKC ON I_T.TENANT_ID = PKC.TENANT_ID AND I_T.JOIN_TABLE_ID = PKC.TABLE_ID  LEFT JOIN (  SELECT I.TENANT_ID,         I.TABLE_ID AS INDEX_ID,         1 AS IS_PREFIXED  FROM OCEANBASE.__ALL_TABLE I  WHERE I.TABLE_TYPE = 5    AND I.INDEX_TYPE IN (1, 2, 10, 15, 23, 24)    AND I.PART_LEVEL != 0    AND I.TENANT_ID = 0  AND NOT EXISTS  (SELECT *   FROM    (SELECT *     FROM OCEANBASE.__ALL_COLUMN C     WHERE C.TABLE_ID = I.DATA_TABLE_ID       AND C.PARTITION_KEY_POSITION != 0       AND C.TENANT_ID = 0    ) PART_COLUMNS    LEFT JOIN    (SELECT *     FROM OCEANBASE.__ALL_COLUMN C     WHERE C.TABLE_ID = I.TABLE_ID     AND C.TENANT_ID = 0     AND C.INDEX_POSITION != 0    ) INDEX_COLUMNS    ON PART_COLUMNS.COLUMN_ID = INDEX_COLUMNS.COLUMN_ID    WHERE    ((PART_COLUMNS.PARTITION_KEY_POSITION & 255) != 0     AND     (INDEX_COLUMNS.INDEX_POSITION IS NULL      OR (PART_COLUMNS.PARTITION_KEY_POSITION & 255) != INDEX_COLUMNS.INDEX_POSITION)    )    OR    ((PART_COLUMNS.PARTITION_KEY_POSITION & 65280)/256 != 0     AND (INDEX_COLUMNS.INDEX_POSITION IS NULL)    )  ) ) LOCAL_PARTITIONED_PREFIX_INDEX ON I_T.TENANT_ID = LOCAL_PARTITIONED_PREFIX_INDEX.TENANT_ID AND I_T.INDEX_ID = LOCAL_PARTITIONED_PREFIX_INDEX.INDEX_ID  LEFT JOIN OCEANBASE.__ALL_TENANT_TABLESPACE TP ON I_T.TENANT_ID = TP.TENANT_ID AND I_T.TABLESPACE_ID = TP.TABLESPACE_ID  WHERE I_T.TENANT_ID = 0     )__"))) {
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

int ObInnerTableSchema::dba_ind_partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_IND_PARTITIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(D.DATABASE_NAME AS CHAR(128)) AS INDEX_OWNER,     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(I.TABLE_NAME, 7)))         END AS CHAR(128)) AS INDEX_NAME,     CAST(DT.TABLE_NAME AS CHAR(128)) AS TABLE_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN 'YES'          ELSE 'NO' END AS CHAR(3)) COMPOSITE,      CAST(PART.PART_NAME AS CHAR(128)) AS PARTITION_NAME,      CAST(CASE I.PART_LEVEL          WHEN 2 THEN PART.SUB_PART_NUM          ELSE 0 END AS SIGNED)  SUBPARTITION_COUNT,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS CHAR(262144)) HIGH_VALUE,      CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS SIGNED) HIGH_VALUE_LENGTH,      CAST(PART.PARTITION_POSITION AS SIGNED) PARTITION_POSITION,     CAST(NULL AS CHAR(8)) AS STATUS,     CAST(NULL AS CHAR(30)) AS TABLESPACE_NAME,     CAST(NULL AS SIGNED) AS PCT_FREE,     CAST(NULL AS SIGNED) AS INI_TRANS,     CAST(NULL AS SIGNED) AS MAX_TRANS,     CAST(NULL AS SIGNED) AS INITIAL_EXTENT,     CAST(NULL AS SIGNED) AS NEXT_EXTENT,     CAST(NULL AS SIGNED) AS MIN_EXTENT,     CAST(NULL AS SIGNED) AS MAX_EXTENT,     CAST(NULL AS SIGNED) AS MAX_SIZE,     CAST(NULL AS SIGNED) AS PCT_INCREASE,     CAST(NULL AS SIGNED) AS FREELISTS,     CAST(NULL AS SIGNED) AS FREELIST_GROUPS,     CAST(NULL AS CHAR(7)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS CHAR(13)) AS COMPRESSION,     CAST(NULL AS SIGNED) AS BLEVEL,     CAST(NULL AS SIGNED) AS LEAF_BLOCKS,     CAST(NULL AS SIGNED) AS DISTINCT_KEYS,     CAST(NULL AS SIGNED) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS SIGNED) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS SIGNED) AS CLUSTERING_FACTOR,     CAST(NULL AS SIGNED) AS NUM_ROWS,     CAST(NULL AS SIGNED) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS CHAR(7)) AS BUFFER_POOL,     CAST(NULL AS CHAR(7)) AS FLASH_CACHE,     CAST(NULL AS CHAR(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS CHAR(3)) AS USER_STATS,     CAST(NULL AS SIGNED) AS PCT_DIRECT_ACCESS,     CAST(NULL AS CHAR(3)) AS GLOBAL_STATS,     CAST(NULL AS CHAR(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS CHAR(1000)) AS PARAMETERS,     CAST('NO' AS CHAR(3)) AS "INTERVAL",     CAST(NULL AS CHAR(3)) AS SEGMENT_CREATED,     CAST(NULL AS CHAR(3)) AS ORPHANED_ENTRIES     FROM     OCEANBASE.__ALL_TABLE I     JOIN OCEANBASE.__ALL_TABLE DT     ON I.TENANT_ID = DT.TENANT_ID AND I.DATA_TABLE_ID = DT.TABLE_ID     JOIN OCEANBASE.__ALL_DATABASE D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5      JOIN (SELECT TENANT_ID,                  TABLE_ID,                  PART_NAME,                  SUB_PART_NUM,                  HIGH_BOUND_VAL,                  LIST_VAL,                  COMPRESS_FUNC_NAME,                  ROW_NUMBER() OVER (                    PARTITION BY TENANT_ID, TABLE_ID                    ORDER BY PART_IDX, PART_ID ASC                  ) PARTITION_POSITION           FROM OCEANBASE.__ALL_PART) PART     ON I.TENANT_ID = PART.TENANT_ID        AND I.TABLE_ID = PART.TABLE_ID      WHERE I.TENANT_ID = 0     AND I.TABLE_MODE >> 12 & 15 in (0,1) )__"))) {
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

int ObInnerTableSchema::dba_ind_subpartitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_IND_SUBPARTITIONS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CAST(D.DATABASE_NAME AS CHAR(128)) AS INDEX_OWNER,     CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN I.TABLE_NAME         ELSE SUBSTR(I.TABLE_NAME, 7 + POSITION('_' IN SUBSTR(I.TABLE_NAME, 7)))         END AS CHAR(128)) AS INDEX_NAME,     CAST(DT.TABLE_NAME AS CHAR(128)) AS TABLE_NAME,     CAST(PART.PART_NAME AS CHAR(128)) PARTITION_NAME,     CAST(PART.SUB_PART_NAME AS CHAR(128))  SUBPARTITION_NAME,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL          ELSE PART.LIST_VAL END AS CHAR(262144)) HIGH_VALUE,     CAST(CASE WHEN length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL)          ELSE length(PART.LIST_VAL) END AS SIGNED) HIGH_VALUE_LENGTH,     CAST(PART.PARTITION_POSITION AS SIGNED) PARTITION_POSITION,     CAST(PART.SUBPARTITION_POSITION AS SIGNED) SUBPARTITION_POSITION,     CAST(NULL AS CHAR(8)) AS STATUS,     CAST(NULL AS CHAR(30)) AS TABLESPACE_NAME,     CAST(NULL AS SIGNED) AS PCT_FREE,     CAST(NULL AS SIGNED) AS INI_TRANS,     CAST(NULL AS SIGNED) AS MAX_TRANS,     CAST(NULL AS SIGNED) AS INITIAL_EXTENT,     CAST(NULL AS SIGNED) AS NEXT_EXTENT,     CAST(NULL AS SIGNED) AS MIN_EXTENT,     CAST(NULL AS SIGNED) AS MAX_EXTENT,     CAST(NULL AS SIGNED) AS MAX_SIZE,     CAST(NULL AS SIGNED) AS PCT_INCREASE,     CAST(NULL AS SIGNED) AS FREELISTS,     CAST(NULL AS SIGNED) AS FREELIST_GROUPS,     CAST(NULL AS CHAR(3)) AS LOGGING,     CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS CHAR(13)) AS COMPRESSION,     CAST(NULL AS SIGNED) AS BLEVEL,     CAST(NULL AS SIGNED) AS LEAF_BLOCKS,     CAST(NULL AS SIGNED) AS DISTINCT_KEYS,     CAST(NULL AS SIGNED) AS AVG_LEAF_BLOCKS_PER_KEY,     CAST(NULL AS SIGNED) AS AVG_DATA_BLOCKS_PER_KEY,     CAST(NULL AS SIGNED) AS CLUSTERING_FACTOR,     CAST(NULL AS SIGNED) AS NUM_ROWS,     CAST(NULL AS SIGNED) AS SAMPLE_SIZE,     CAST(NULL AS DATE) AS LAST_ANALYZED,     CAST(NULL AS CHAR(7)) AS BUFFER_POOL,     CAST(NULL AS CHAR(7)) AS FLASH_CACHE,     CAST(NULL AS CHAR(7)) AS CELL_FLASH_CACHE,     CAST(NULL AS CHAR(3)) AS USER_STATS,     CAST(NULL AS CHAR(3)) AS GLOBAL_STATS,     CAST('NO' AS CHAR(3)) AS "INTERVAL",     CAST(NULL AS CHAR(3)) AS SEGMENT_CREATED,     CAST(NULL AS CHAR(6)) AS DOMIDX_OPSTATUS,     CAST(NULL AS CHAR(1000)) AS PARAMETERS     FROM OCEANBASE.__ALL_TABLE I     JOIN OCEANBASE.__ALL_TABLE DT     ON I.TENANT_ID = DT.TENANT_ID AND I.DATA_TABLE_ID = DT.TABLE_ID     JOIN OCEANBASE.__ALL_DATABASE D     ON I.TENANT_ID = D.TENANT_ID        AND I.DATABASE_ID = D.DATABASE_ID        AND I.TABLE_TYPE = 5     JOIN     (SELECT P_PART.TENANT_ID,             P_PART.TABLE_ID,             P_PART.PART_NAME,             P_PART.PARTITION_POSITION,             S_PART.SUB_PART_NAME,             S_PART.HIGH_BOUND_VAL,             S_PART.LIST_VAL,             S_PART.COMPRESS_FUNC_NAME,             S_PART.SUBPARTITION_POSITION      FROM (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              PART_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID                ORDER BY PART_IDX, PART_ID ASC              ) AS PARTITION_POSITION            FROM OCEANBASE.__ALL_PART) P_PART,           (SELECT              TENANT_ID,              TABLE_ID,              PART_ID,              SUB_PART_NAME,              HIGH_BOUND_VAL,              LIST_VAL,              COMPRESS_FUNC_NAME,              ROW_NUMBER() OVER (                PARTITION BY TENANT_ID, TABLE_ID, PART_ID                ORDER BY SUB_PART_IDX, SUB_PART_ID ASC              ) AS SUBPARTITION_POSITION            FROM OCEANBASE.__ALL_SUB_PART) S_PART      WHERE P_PART.PART_ID = S_PART.PART_ID AND            P_PART.TABLE_ID = S_PART.TABLE_ID            AND P_PART.TENANT_ID = S_PART.TENANT_ID) PART     ON I.TABLE_ID = PART.TABLE_ID AND I.TENANT_ID = PART.TENANT_ID     WHERE I.TENANT_ID = 0     AND I.TABLE_MODE >> 12 & 15 in (0,1) )__"))) {
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

int ObInnerTableSchema::gv_ob_servers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SERVERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SERVERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP,   SVR_PORT,   ZONE,   SQL_PORT,   CPU_CAPACITY,   CPU_CAPACITY_MAX,   CPU_ASSIGNED,   CPU_ASSIGNED_MAX,   MEM_CAPACITY,   MEM_ASSIGNED,   LOG_DISK_CAPACITY,   LOG_DISK_ASSIGNED,   LOG_DISK_IN_USE,   DATA_DISK_CAPACITY,   DATA_DISK_IN_USE,   DATA_DISK_HEALTH_STATUS,   MEMORY_LIMIT,   DATA_DISK_ALLOCATED,   (CASE       WHEN data_disk_abnormal_time > 0 THEN usec_to_time(data_disk_abnormal_time)       ELSE NULL    END) AS DATA_DISK_ABNORMAL_TIME,   (CASE       WHEN ssl_cert_expired_time > 0 THEN usec_to_time(ssl_cert_expired_time)       ELSE NULL    END) AS SSL_CERT_EXPIRED_TIME FROM oceanbase.__all_virtual_server )__"))) {
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

int ObInnerTableSchema::v_ob_servers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SERVERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SERVERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,   SVR_PORT,   ZONE,   SQL_PORT,   CPU_CAPACITY,   CPU_CAPACITY_MAX,   CPU_ASSIGNED,   CPU_ASSIGNED_MAX,   MEM_CAPACITY,   MEM_ASSIGNED,   LOG_DISK_CAPACITY,   LOG_DISK_ASSIGNED,   LOG_DISK_IN_USE,   DATA_DISK_CAPACITY,   DATA_DISK_IN_USE,   DATA_DISK_HEALTH_STATUS,   MEMORY_LIMIT,   DATA_DISK_ALLOCATED,   DATA_DISK_ABNORMAL_TIME,   SSL_CERT_EXPIRED_TIME     FROM oceanbase.GV$OB_SERVERS     WHERE SVR_IP = host_ip() AND SVR_PORT = rpc_port() )__"))) {
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

int ObInnerTableSchema::gv_ob_units_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_UNITS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_UNITS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,            SVR_PORT,            UNIT_ID,            TENANT_ID,            ZONE,            ZONE_TYPE,            REGION,            MAX_CPU,            MIN_CPU,            MEMORY_SIZE,            MAX_IOPS,            MIN_IOPS,            IOPS_WEIGHT,            LOG_DISK_SIZE,            LOG_DISK_IN_USE,            DATA_DISK_IN_USE,            STATUS,            usec_to_time(create_time) AS CREATE_TIME     FROM oceanbase.__all_virtual_unit )__"))) {
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

int ObInnerTableSchema::v_ob_units_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_UNITS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_UNITS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,            SVR_PORT,            UNIT_ID,            TENANT_ID,            ZONE,            ZONE_TYPE,            REGION,            MAX_CPU,            MIN_CPU,            MEMORY_SIZE,            MAX_IOPS,            MIN_IOPS,            IOPS_WEIGHT,            LOG_DISK_SIZE,            LOG_DISK_IN_USE,            DATA_DISK_IN_USE,            STATUS,            CREATE_TIME     FROM oceanbase.GV$OB_UNITS     WHERE SVR_IP = host_ip() AND SVR_PORT = rpc_port() )__"))) {
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

int ObInnerTableSchema::gv_ob_parameters_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_PARAMETERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_PARAMETERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP,   SVR_PORT,   ZONE,   SCOPE,   TENANT_ID,   NAME,   DATA_TYPE,   VALUE,   INFO,   SECTION,   EDIT_LEVEL,   DEFAULT_VALUE,   CAST (CASE ISDEFAULT         WHEN 1         THEN 'YES'         ELSE 'NO'         END AS CHAR(3)) AS ISDEFAULT FROM oceanbase.__all_virtual_tenant_parameter_stat )__"))) {
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

int ObInnerTableSchema::v_ob_parameters_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_PARAMETERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_PARAMETERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,     SVR_PORT,     ZONE,     SCOPE,     TENANT_ID,     NAME,     DATA_TYPE,     VALUE,     INFO,     SECTION,     EDIT_LEVEL,     DEFAULT_VALUE,     CAST (CASE ISDEFAULT           WHEN 1           THEN 'YES'           ELSE 'NO'           END AS CHAR(3)) AS ISDEFAULT     FROM oceanbase.GV$OB_PARAMETERS     WHERE SVR_IP = host_ip() AND SVR_PORT = rpc_port() )__"))) {
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

int ObInnerTableSchema::gv_ob_processlist_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_PROCESSLIST_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_PROCESSLIST_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP, SVR_PORT, SQL_PORT,   ID,   USER,   HOST,   DB,   TENANT,   COMMAND,   TIME,   TOTAL_TIME,   STATE,   INFO,   PROXY_SESSID,   MASTER_SESSID,   USER_CLIENT_IP,   USER_HOST,   RETRY_CNT,   RETRY_INFO,   SQL_ID,   TRANS_ID,   THREAD_ID,   SSL_CIPHER,   TRACE_ID,   TRANS_STATE,   ACTION,   MODULE,   CLIENT_INFO,   LEVEL,   SAMPLE_PERCENTAGE,   RECORD_POLICY,   LB_VID,   LB_VIP,   LB_VPORT,   IN_BYTES,   OUT_BYTES,   USER_CLIENT_PORT,   cast(total_cpu_time as SIGNED) as TOTAL_CPU_TIME,   PROXY_USER FROM oceanbase.__all_virtual_processlist )__"))) {
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

int ObInnerTableSchema::v_ob_processlist_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_PROCESSLIST_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_PROCESSLIST_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP, SVR_PORT, SQL_PORT,     ID,     USER,     HOST,     DB,     TENANT,     COMMAND,     TIME,     TOTAL_TIME,     STATE,     INFO,     PROXY_SESSID,     MASTER_SESSID,     USER_CLIENT_IP,     USER_HOST,     RETRY_CNT,     RETRY_INFO,     SQL_ID,     TRANS_ID,     THREAD_ID,     SSL_CIPHER,     TRACE_ID,     TRANS_STATE,     ACTION,     MODULE,     CLIENT_INFO,     LEVEL,     SAMPLE_PERCENTAGE,     RECORD_POLICY,     LB_VID,     LB_VIP,     LB_VPORT,     IN_BYTES,     OUT_BYTES,     USER_CLIENT_PORT,     cast(total_cpu_time as SIGNED) as TOTAL_CPU_TIME,     PROXY_USER     FROM oceanbase.GV$OB_PROCESSLIST     WHERE SVR_IP = host_ip() AND SVR_PORT = rpc_port() )__"))) {
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

int ObInnerTableSchema::gv_ob_kvcache_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_KVCACHE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_KVCACHE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP,   SVR_PORT,   TENANT_ID,   CACHE_NAME,   PRIORITY,   CACHE_SIZE,   HIT_RATIO,   TOTAL_PUT_CNT,   TOTAL_HIT_CNT,   TOTAL_MISS_CNT FROM oceanbase.__all_virtual_kvcache_info )__"))) {
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

int ObInnerTableSchema::v_ob_kvcache_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_KVCACHE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_KVCACHE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,     SVR_PORT,     TENANT_ID,     CACHE_NAME,     PRIORITY,     CACHE_SIZE,     HIT_RATIO,     TOTAL_PUT_CNT,     TOTAL_HIT_CNT,     TOTAL_MISS_CNT     FROM oceanbase.GV$OB_KVCACHE     WHERE SVR_IP = host_ip() AND SVR_PORT = rpc_port() )__"))) {
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

int ObInnerTableSchema::gv_ob_transaction_participants_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TRANSACTION_PARTICIPANTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TRANSACTION_PARTICIPANTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       tenant_id AS TENANT_ID,       svr_ip AS SVR_IP,       svr_port AS SVR_PORT,       session_id AS SESSION_ID,       scheduler_addr AS SCHEDULER_ADDR,       CASE         WHEN part_trans_action >= 3 AND trans_type = 0           THEN 'LOCAL'         WHEN part_trans_action >= 3 AND trans_type = 2           THEN 'DISTRIBUTED'         WHEN trans_type = 0 and state = 10           THEN 'UNDECIDED'         WHEN trans_type = 0           THEN 'LOCAL'         ELSE 'DISTRIBUTED'         END AS TX_TYPE,       trans_id AS TX_ID,       ls_id AS LS_ID,       participants AS PARTICIPANTS,       ctx_create_time AS CTX_CREATE_TIME,       expired_time AS TX_EXPIRED_TIME,       CASE         WHEN state = 0 THEN 'UNKNOWN'         WHEN state = 10 THEN 'ACTIVE'         WHEN state = 20 THEN 'REDO COMPLETE'         WHEN state = 30 THEN 'PREPARE'         WHEN state = 40 THEN 'PRECOMMIT'         WHEN state = 50 THEN 'COMMIT'         WHEN state = 60 THEN 'ABORT'         WHEN state = 70 THEN 'CLEAR'         ELSE 'UNDEFINED'         END AS STATE,       CAST (CASE         WHEN part_trans_action = 1 THEN 'NULL'         WHEN part_trans_action = 2 THEN 'START'         WHEN part_trans_action = 3 THEN 'COMMIT'         WHEN part_trans_action = 4 THEN 'ABORT'         WHEN part_trans_action = 5 THEN 'DIED'         WHEN part_trans_action = 6 THEN 'END'         ELSE 'UNKNOWN'         END AS CHAR(10)) AS ACTION,       pending_log_size AS PENDING_LOG_SIZE,       flushed_log_size AS FLUSHED_LOG_SIZE,       CASE         WHEN role = 0 THEN 'LEADER'         ELSE 'FOLLOWER'       END AS ROLE,       COORDINATOR AS COORD,       LAST_REQUEST_TIME,       FORMAT_ID AS FORMATID,       HEX(GTRID) AS GLOBALID,       HEX(BQUAL) AS BRANCHID     FROM oceanbase.__all_virtual_trans_stat     WHERE is_exiting = 0 )__"))) {
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

int ObInnerTableSchema::v_ob_transaction_participants_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TRANSACTION_PARTICIPANTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TRANSACTION_PARTICIPANTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT TENANT_ID,     SVR_IP,     SVR_PORT,     SESSION_ID,     SCHEDULER_ADDR,     TX_TYPE,     TX_ID,     LS_ID,     PARTICIPANTS,     CTX_CREATE_TIME,     TX_EXPIRED_TIME,     STATE,     ACTION,     PENDING_LOG_SIZE,     FLUSHED_LOG_SIZE,     ROLE,     COORD,     LAST_REQUEST_TIME,     FORMATID,     GLOBALID,     BRANCHID     FROM OCEANBASE.GV$OB_TRANSACTION_PARTICIPANTS     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_compaction_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_COMPACTION_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_COMPACTION_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       SVR_IP,       SVR_PORT,       TENANT_ID,       TYPE,       ZONE,       COMPACTION_SCN,       STATUS,       TOTAL_TABLET_COUNT,       UNFINISHED_TABLET_COUNT,       DATA_SIZE,       UNFINISHED_DATA_SIZE,       COMPRESSION_RATIO,       START_TIME,       ESTIMATED_FINISH_TIME,       COMMENTS     FROM oceanbase.__all_virtual_server_compaction_progress )__"))) {
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

int ObInnerTableSchema::v_ob_compaction_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_COMPACTION_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_COMPACTION_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,       SVR_PORT,       TENANT_ID,       TYPE,       ZONE,       COMPACTION_SCN,       STATUS,       TOTAL_TABLET_COUNT,       UNFINISHED_TABLET_COUNT,       DATA_SIZE,       UNFINISHED_DATA_SIZE,       COMPRESSION_RATIO,       START_TIME,       ESTIMATED_FINISH_TIME,       COMMENTS     FROM oceanbase.GV$OB_COMPACTION_PROGRESS     WHERE         SVR_IP=HOST_IP()     AND         SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_tablet_compaction_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TABLET_COMPACTION_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TABLET_COMPACTION_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       SVR_IP,       SVR_PORT,       TENANT_ID,       TYPE,       LS_ID,       TABLET_ID,       COMPACTION_SCN,       TASK_ID,       STATUS,       DATA_SIZE,       UNFINISHED_DATA_SIZE,       PROGRESSIVE_COMPACTION_ROUND,       CREATE_TIME,       START_TIME,       ESTIMATED_FINISH_TIME,       START_CG_ID,       END_CG_ID     FROM oceanbase.__all_virtual_tablet_compaction_progress )__"))) {
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

int ObInnerTableSchema::v_ob_tablet_compaction_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TABLET_COMPACTION_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TABLET_COMPACTION_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,       SVR_PORT,       TENANT_ID,       TYPE,       LS_ID,       TABLET_ID,       COMPACTION_SCN,       TASK_ID,       STATUS,       DATA_SIZE,       UNFINISHED_DATA_SIZE,       PROGRESSIVE_COMPACTION_ROUND,       CREATE_TIME,       START_TIME,       ESTIMATED_FINISH_TIME,       START_CG_ID,       END_CG_ID     FROM oceanbase.GV$OB_TABLET_COMPACTION_PROGRESS     WHERE         SVR_IP=HOST_IP()     AND         SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_tablet_compaction_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TABLET_COMPACTION_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TABLET_COMPACTION_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       SVR_IP,       SVR_PORT,       TENANT_ID,       LS_ID,       TABLET_ID,       TYPE,       COMPACTION_SCN,       START_TIME,       FINISH_TIME,       TASK_ID,       OCCUPY_SIZE,       MACRO_BLOCK_COUNT,       MULTIPLEXED_MACRO_BLOCK_COUNT,       NEW_MICRO_COUNT_IN_NEW_MACRO,       MULTIPLEXED_MICRO_COUNT_IN_NEW_MACRO,       TOTAL_ROW_COUNT,       INCREMENTAL_ROW_COUNT,       COMPRESSION_RATIO,       NEW_FLUSH_DATA_RATE,       PROGRESSIVE_COMPACTION_ROUND,       PROGRESSIVE_COMPACTION_NUM,       PARALLEL_DEGREE,       PARALLEL_INFO,       PARTICIPANT_TABLE,       MACRO_ID_LIST,       COMMENTS,       START_CG_ID,       END_CG_ID,       KEPT_SNAPSHOT,       MERGE_LEVEL     FROM oceanbase.__all_virtual_tablet_compaction_history )__"))) {
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

int ObInnerTableSchema::v_ob_tablet_compaction_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TABLET_COMPACTION_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TABLET_COMPACTION_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,       SVR_PORT,       TENANT_ID,       LS_ID,       TABLET_ID,       TYPE,       COMPACTION_SCN,       START_TIME,       FINISH_TIME,       TASK_ID,       OCCUPY_SIZE,       MACRO_BLOCK_COUNT,       MULTIPLEXED_MACRO_BLOCK_COUNT,       NEW_MICRO_COUNT_IN_NEW_MACRO,       MULTIPLEXED_MICRO_COUNT_IN_NEW_MACRO,       TOTAL_ROW_COUNT,       INCREMENTAL_ROW_COUNT,       COMPRESSION_RATIO,       NEW_FLUSH_DATA_RATE,       PROGRESSIVE_COMPACTION_ROUND,       PROGRESSIVE_COMPACTION_NUM,       PARALLEL_DEGREE,       PARALLEL_INFO,       PARTICIPANT_TABLE,       MACRO_ID_LIST,       COMMENTS,       START_CG_ID,       END_CG_ID,       KEPT_SNAPSHOT,       MERGE_LEVEL     FROM oceanbase.GV$OB_TABLET_COMPACTION_HISTORY     WHERE         SVR_IP=HOST_IP()     AND         SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_compaction_diagnose_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_COMPACTION_DIAGNOSE_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_COMPACTION_DIAGNOSE_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       SVR_IP,       SVR_PORT,       TENANT_ID,       TYPE,       LS_ID,       TABLET_ID,       STATUS,       CREATE_TIME,       DIAGNOSE_INFO     FROM oceanbase.__all_virtual_compaction_diagnose_info     WHERE       STATUS != "RS_UNCOMPACTED"     AND       STATUS != "NOT_SCHEDULE"     AND       STATUS != "SPECIAL" )__"))) {
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

int ObInnerTableSchema::v_ob_compaction_diagnose_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_COMPACTION_DIAGNOSE_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_COMPACTION_DIAGNOSE_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,       SVR_PORT,       TENANT_ID,       TYPE,       LS_ID,       TABLET_ID,       STATUS,       CREATE_TIME,       DIAGNOSE_INFO     FROM oceanbase.GV$OB_COMPACTION_DIAGNOSE_INFO     WHERE         SVR_IP=HOST_IP()     AND         SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_compaction_suggestions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_COMPACTION_SUGGESTIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_COMPACTION_SUGGESTIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       SVR_IP,       SVR_PORT,       TENANT_ID,       TYPE,       LS_ID,       TABLET_ID,       START_TIME,       FINISH_TIME,       SUGGESTION     FROM oceanbase.__all_virtual_compaction_suggestion )__"))) {
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

int ObInnerTableSchema::v_ob_compaction_suggestions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_COMPACTION_SUGGESTIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_COMPACTION_SUGGESTIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,       SVR_PORT,       TENANT_ID,       TYPE,       LS_ID,       TABLET_ID,       START_TIME,       FINISH_TIME,       SUGGESTION     FROM oceanbase.GV$OB_COMPACTION_SUGGESTIONS     WHERE         SVR_IP=HOST_IP()     AND         SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_dtl_interm_result_monitor_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_DTL_INTERM_RESULT_MONITOR_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_DTL_INTERM_RESULT_MONITOR_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           SVR_IP,           SVR_PORT,           TENANT_ID,           TRACE_ID,           OWNER,           START_TIME,           EXPIRE_TIME,           HOLD_MEMORY,           DUMP_SIZE,           DUMP_COST,           DUMP_TIME,           DUMP_FD,           DUMP_DIR_ID,           CHANNEL_ID,           QC_ID,           DFO_ID,           SQC_ID,           BATCH_ID,           MAX_HOLD_MEMORY         FROM oceanbase.__all_virtual_dtl_interm_result_monitor )__"))) {
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

int ObInnerTableSchema::v_ob_dtl_interm_result_monitor_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_DTL_INTERM_RESULT_MONITOR_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_DTL_INTERM_RESULT_MONITOR_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,           SVR_PORT,           TENANT_ID,           TRACE_ID,           OWNER,           START_TIME,           EXPIRE_TIME,           HOLD_MEMORY,           DUMP_SIZE,           DUMP_COST,           DUMP_TIME,           DUMP_FD,           DUMP_DIR_ID,           CHANNEL_ID,           QC_ID,           DFO_ID,           SQC_ID,           BATCH_ID,           MAX_HOLD_MEMORY FROM OCEANBASE.GV$OB_DTL_INTERM_RESULT_MONITOR     WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_io_calibration_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_IO_CALIBRATION_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_IO_CALIBRATION_STATUS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT         SVR_IP,         SVR_PORT,         STORAGE_NAME,         STATUS,         START_TIME,         FINISH_TIME     FROM oceanbase.__all_virtual_io_calibration_status   )__"))) {
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

int ObInnerTableSchema::v_ob_io_calibration_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_IO_CALIBRATION_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_IO_CALIBRATION_STATUS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,         SVR_PORT,         STORAGE_NAME,         STATUS,         START_TIME,         FINISH_TIME FROM oceanbase.GV$OB_IO_CALIBRATION_STATUS     WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()   )__"))) {
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

int ObInnerTableSchema::gv_ob_io_benchmark_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_IO_BENCHMARK_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_IO_BENCHMARK_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT         SVR_IP,         SVR_PORT,         STORAGE_NAME,         MODE,         SIZE,         IOPS,         MBPS,         LATENCY     FROM oceanbase.__all_virtual_io_benchmark   )__"))) {
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

int ObInnerTableSchema::v_ob_io_benchmark_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_IO_BENCHMARK_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_IO_BENCHMARK_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP,         SVR_PORT,         STORAGE_NAME,         MODE,         SIZE,         IOPS,         MBPS,         LATENCY FROM oceanbase.GV$OB_IO_BENCHMARK     WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()   )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_delete_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_DELETE_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_DELETE_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TENANT_ID,       JOB_ID,       INCARNATION,       INITIATOR_TENANT_ID,       INITIATOR_JOB_ID,       EXECUTOR_TENANT_ID,       TYPE,       USEC_TO_TIME(PARAMETER) AS PARAMETER,       JOB_LEVEL,       USEC_TO_TIME(START_TS) AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           USEC_TO_TIME(END_TS)         END AS END_TIMESTAMP,       STATUS,       TASK_COUNT,       SUCCESS_TASK_COUNT,       RESULT,       COMMENT     FROM oceanbase.__all_virtual_backup_delete_job )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_delete_job_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_DELETE_JOB_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_DELETE_JOB_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TENANT_ID,       JOB_ID,       INCARNATION,       INITIATOR_TENANT_ID,       INITIATOR_JOB_ID,       EXECUTOR_TENANT_ID,       TYPE,       USEC_TO_TIME(PARAMETER) AS PARAMETER,       JOB_LEVEL,       USEC_TO_TIME(START_TS) AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           USEC_TO_TIME(END_TS)         END AS END_TIMESTAMP,       STATUS,       TASK_COUNT,       SUCCESS_TASK_COUNT,       RESULT,       COMMENT     FROM oceanbase.__all_virtual_backup_delete_job_history )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_delete_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_DELETE_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_DELETE_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TENANT_ID,       TASK_ID,       INCARNATION,       JOB_ID,       TASK_TYPE,       ID,       ROUND_ID,       DEST_ID,       USEC_TO_TIME(START_TS) AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           USEC_TO_TIME(END_TS)         END AS END_TIMESTAMP,       STATUS,       TOTAL_LS_COUNT,       FINISH_LS_COUNT,       RESULT,       COMMENT,       PATH     FROM oceanbase.__all_virtual_backup_delete_task )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_delete_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_DELETE_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_DELETE_TASK_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TENANT_ID,       TASK_ID,       INCARNATION,       JOB_ID,       TASK_TYPE,       ID,       ROUND_ID,       DEST_ID,       USEC_TO_TIME(START_TS) AS START_TIMESTAMP,       CASE         WHEN END_TS = 0           THEN NULL         ELSE           USEC_TO_TIME(END_TS)         END AS END_TIMESTAMP,       STATUS,       TOTAL_LS_COUNT,       FINISH_LS_COUNT,       RESULT,       COMMENT,       PATH     FROM oceanbase.__all_virtual_backup_delete_task_history )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_delete_policy_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_DELETE_POLICY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_DELETE_POLICY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       TENANT_ID,       POLICY_NAME,       RECOVERY_WINDOW     FROM oceanbase.__all_virtual_backup_delete_policy )__"))) {
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
