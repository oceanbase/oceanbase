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

int ObInnerTableSchema::gv_ob_sstables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SSTABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SSTABLES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT  M.SVR_IP,  M.SVR_PORT,  (case M.TABLE_TYPE     when 0 then 'MEMTABLE' when 1 then 'TX_DATA_MEMTABLE' when 2 then 'TX_CTX_MEMTABLE'     when 3 then 'LOCK_MEMTABLE' when 4 then 'DIRECT_LOAD_MEMTABLE' when 10 then 'MAJOR' when 11 then 'MINOR'     when 12 then 'MINI' when 13 then 'META'     when 14 then 'DDL_DUMP' when 15 then 'REMOTE_LOGICAL_MINOR' when 16 then 'DDL_MEM'     when 17 then 'CO_MAJOR' when 18 then 'NORMAL_CG' when 19 then 'ROWKEY_CG' when 20 then 'COL_ORIENTED_META'     when 21 then 'DDL_MERGE_CO' when 22 then 'DDL_MERGE_CG' when 23 then 'DDL_MEM_CO'     when 24 then 'DDL_MEM_CG' when 25 then 'DDL_MEM_MINI_SSTABLE'     when 26 then 'MDS_MINI' when 27 then 'MDS_MINOR'     else 'INVALID'   end) as TABLE_TYPE,  M.TENANT_ID,  M.LS_ID,  M.TABLET_ID,  M.CG_IDX,  M.START_LOG_SCN,  M.END_LOG_SCN,  M.DATA_CHECKSUM,  M.SIZE,  M.REF,  M.UPPER_TRANS_VERSION,  M.IS_ACTIVE,  M.CONTAIN_UNCOMMITTED_ROW FROM  oceanbase.__all_virtual_table_mgr M )__"))) {
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

int ObInnerTableSchema::v_ob_sstables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SSTABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SSTABLES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT M.SVR_IP,  M.SVR_PORT,  M.TABLE_TYPE,  M.TENANT_ID,  M.LS_ID,  M.TABLET_ID,  M.CG_IDX,  M.START_LOG_SCN,  M.END_LOG_SCN,  M.DATA_CHECKSUM,  M.SIZE,  M.REF,  M.UPPER_TRANS_VERSION,  M.IS_ACTIVE,  M.CONTAIN_UNCOMMITTED_ROW FROM OCEANBASE.GV$OB_SSTABLES M WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::cdb_ob_restore_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_RESTORE_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_RESTORE_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     P.TENANT_ID AS TENANT_ID,     P.JOB_ID AS JOB_ID,     RESTORE_TENANT_NAME,     RESTORE_TENANT_ID,     BACKUP_TENANT_NAME,     BACKUP_TENANT_ID,     BACKUP_CLUSTER_NAME,     BACKUP_DEST,     RESTORE_OPTION,     RESTORE_TYPE,     RESTORE_SCN,     CASE       WHEN RESTORE_SCN IS NULL         THEN NULL       WHEN RESTORE_SCN=0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(RESTORE_SCN)       END AS RESTORE_SCN_DISPLAY,     CASE       WHEN STATUS = 'RESTORE_PRE'         THEN 'RESTORING'       WHEN STATUS = 'RESTORE_CREATE_INIT_LS'         THEN 'RESTORING'       WHEN STATUS = 'PHYSICAL_RESTORE_WAIT_RESTORE_TO_CONSISTENT_SCN'         THEN 'RESTORING'       WHEN STATUS = 'RESTORE_WAIT_LS'         THEN 'RESTORING'       WHEN STATUS = 'POST_CHECK'         THEN 'RESTORING'       ELSE STATUS       END AS STATUS,     CASE       WHEN START_TIMESTAMP IS NULL         THEN NULL       WHEN START_TIMESTAMP=''         THEN NULL       WHEN START_TIMESTAMP='0'         THEN NULL       ELSE         USEC_TO_TIME(START_TIMESTAMP)       END AS START_TIMESTAMP,     BACKUP_SET_LIST,     BACKUP_PIECE_LIST,     TOTAL_BYTES,     CASE       WHEN TOTAL_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN TOTAL_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN TOTAL_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(TOTAL_BYTES/1024/1024,2), 'MB')       END AS TOTAL_BYTES_DISPLAY,     FINISH_BYTES,     CASE       WHEN FINISH_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN FINISH_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN FINISH_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(FINISH_BYTES/1024/1024,2), 'MB')       END AS FINISH_BYTES_DISPLAY,     DESCRIPTION     FROM   (       SELECT       TENANT_ID,       JOB_ID,       MAX(CASE NAME WHEN 'tenant_name' THEN VALUE ELSE '' END) AS RESTORE_TENANT_NAME,       MAX(CASE NAME WHEN 'tenant_id' THEN VALUE ELSE '' END) AS RESTORE_TENANT_ID,       MAX(CASE NAME WHEN 'backup_tenant_name' THEN VALUE ELSE '' END) AS BACKUP_TENANT_NAME,       MAX(CASE NAME WHEN 'backup_tenant_id' THEN VALUE ELSE '' END) AS BACKUP_TENANT_ID,       MAX(CASE NAME WHEN 'backup_cluster_name' THEN VALUE ELSE '' END) AS BACKUP_CLUSTER_NAME,       MAX(CASE NAME WHEN 'target_tenant_role' THEN VALUE ELSE '' END) AS TENANT_ROLE,       MAX(CASE NAME WHEN 'backup_dest' THEN VALUE ELSE '' END) AS BACKUP_DEST,       MAX(CASE NAME WHEN 'restore_option' THEN VALUE ELSE '' END) AS RESTORE_OPTION,       MAX(CASE NAME WHEN 'status' THEN VALUE ELSE '' END) AS STATUS,       MAX(CASE NAME WHEN 'restore_scn' THEN VALUE ELSE '' END) AS RESTORE_SCN,       MAX(CASE NAME WHEN 'restore_start_ts' THEN VALUE ELSE '' END) AS START_TIMESTAMP,       MAX(CASE NAME WHEN 'backup_set_list' THEN VALUE ELSE '' END) AS BACKUP_SET_LIST,       MAX(CASE NAME WHEN 'backup_piece_list' THEN VALUE ELSE '' END) AS BACKUP_PIECE_LIST,       MAX(CASE NAME WHEN 'description' THEN VALUE ELSE '' END) AS DESCRIPTION,       MAX(CASE NAME WHEN 'restore_type' THEN VALUE ELSE '' END) AS RESTORE_TYPE       FROM OCEANBASE.__ALL_VIRTUAL_RESTORE_JOB GROUP BY TENANT_ID, JOB_ID   ) P LEFT JOIN   (       SELECT       TENANT_ID,       JOB_ID,       TOTAL_BYTES,       FINISH_BYTES       FROM OCEANBASE.__ALL_VIRTUAL_RESTORE_PROGRESS   ) J     ON P.TENANT_ID=J.TENANT_ID AND P.JOB_ID=J.JOB_ID )__"))) {
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

int ObInnerTableSchema::cdb_ob_restore_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_RESTORE_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_RESTORE_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     JOB_ID,     RESTORE_TENANT_NAME,     RESTORE_TENANT_ID,     BACKUP_TENANT_NAME,     BACKUP_TENANT_ID,     BACKUP_CLUSTER_NAME,     BACKUP_DEST,     RESTORE_SCN,     CASE       WHEN RESTORE_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(RESTORE_SCN)       END AS RESTORE_SCN_DISPLAY,     RESTORE_OPTION,     RESTORE_TYPE,     START_TIME AS START_TIMESTAMP,     FINISH_TIME AS FINISH_TIMESTAMP,     STATUS,     BACKUP_PIECE_LIST,     BACKUP_SET_LIST,     BACKUP_CLUSTER_VERSION,     LS_COUNT,     FINISH_LS_COUNT,     TABLET_COUNT,     FINISH_TABLET_COUNT,     TOTAL_BYTES,     CASE       WHEN TOTAL_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN TOTAL_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN TOTAL_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(TOTAL_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(TOTAL_BYTES/1024/1024,2), 'MB')       END AS TOTAL_BYTES_DISPLAY,     FINISH_BYTES,     CASE       WHEN FINISH_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN FINISH_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN FINISH_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(FINISH_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(FINISH_BYTES/1024/1024,2), 'MB')       END AS FINISH_BYTES_DISPLAY,     DESCRIPTION,     COMMENT     FROM OCEANBASE.__ALL_VIRTUAL_RESTORE_JOB_HISTORY )__"))) {
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

int ObInnerTableSchema::gv_ob_server_schema_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SERVER_SCHEMA_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SERVER_SCHEMA_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP,   SVR_PORT,   TENANT_ID,   REFRESHED_SCHEMA_VERSION,   RECEIVED_SCHEMA_VERSION,   SCHEMA_COUNT,   SCHEMA_SIZE,   MIN_SSTABLE_SCHEMA_VERSION FROM   oceanbase.__all_virtual_server_schema_info )__"))) {
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

int ObInnerTableSchema::v_ob_server_schema_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SERVER_SCHEMA_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SERVER_SCHEMA_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP,   SVR_PORT,   TENANT_ID,   REFRESHED_SCHEMA_VERSION,   RECEIVED_SCHEMA_VERSION,   SCHEMA_COUNT,   SCHEMA_SIZE,   MIN_SSTABLE_SCHEMA_VERSION FROM   oceanbase.GV$OB_SERVER_SCHEMA_INFO WHERE   SVR_IP=HOST_IP() AND   SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::v_sql_monitor_statname_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_SQL_MONITOR_STATNAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_SQL_MONITOR_STATNAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(NULL AS UNSIGNED) AS CON_ID,       ID,       GROUP_ID,       NAME,       DESCRIPTION,       TYPE,       0 FLAGS     FROM oceanbase.__all_virtual_sql_monitor_statname )__"))) {
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

int ObInnerTableSchema::gv_ob_merge_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_MERGE_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_MERGE_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT         SVR_IP,         SVR_PORT,         TENANT_ID,         LS_ID,         TABLET_ID,         TYPE AS ACTION,         COMPACTION_SCN,         START_TIME,         FINISH_TIME AS END_TIME,         MACRO_BLOCK_COUNT,         CASE MACRO_BLOCK_COUNT WHEN 0 THEN 0.00 ELSE ROUND(MULTIPLEXED_MACRO_BLOCK_COUNT/MACRO_BLOCK_COUNT*100, 2) END AS REUSE_PCT,         PARALLEL_DEGREE     FROM oceanbase.__ALL_VIRTUAL_TABLET_COMPACTION_HISTORY )__"))) {
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

int ObInnerTableSchema::v_ob_merge_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_MERGE_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_MERGE_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT         SVR_IP,         SVR_PORT,         TENANT_ID,         LS_ID,         TABLET_ID,         ACTION,         COMPACTION_SCN,         START_TIME,         END_TIME,         MACRO_BLOCK_COUNT,         REUSE_PCT,         PARALLEL_DEGREE     FROM OCEANBASE.GV$OB_MERGE_INFO     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::v_ob_encrypted_tables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_ENCRYPTED_TABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_ENCRYPTED_TABLES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       A.table_id AS TABLE_ID,       A.table_name AS TABLE_NAME,       B.tablespace_id AS TABLESPACE_ID,       B.encryption_name AS ENCRYPTIONALG,       CASE WHEN B.encryption_name != '' AND sum(encrypted_macro_block_count) = sum(macro_block_count) THEN 'YES'            ELSE 'NO' END AS ENCRYPTED,       hex(B.encrypt_key) AS ENCRYPTEDKEY,       B.master_key_id AS MASTERKEYID,       sum(encrypted_macro_block_count) AS BLOCKS_ENCRYPTED,       (sum(macro_block_count) - sum(encrypted_macro_block_count)) AS BLOCKS_DECRYPTED,       CASE WHEN (B.encryption_name != '' AND sum(encrypted_macro_block_count) < sum(macro_block_count)) THEN 'ENCRYPTING'            WHEN (B.encryption_name = '' AND sum(encrypted_macro_block_count) > 0) THEN 'DECRYPTING'            ELSE 'NORMAL' END AS STATUS,       A.tenant_id as CON_ID     FROM       (SELECT T.tenant_id, T.table_id, T.table_name, T.table_type, T.tablespace_id, T.tablet_id        FROM oceanbase.__all_table T where T.part_level = 0 and T.table_mode >> 12 & 15 in (0,1)        UNION ALL        SELECT T.tenant_id, T.table_id, T.table_name, T.table_type, T.tablespace_id, P.tablet_id        FROM oceanbase.__all_table T, oceanbase.__all_part P        WHERE T.part_level = 1 and T.tenant_id = P.tenant_id and T.table_id = P.table_id        UNION ALL        SELECT T.tenant_id, T.table_id, T.table_name, T.table_type, T.tablespace_id, SP.tablet_id        FROM oceanbase.__all_table T, oceanbase.__all_sub_part SP        WHERE T.part_level = 2 and T.tenant_id = SP.tenant_id and T.table_id = SP.table_id       ) A       JOIN oceanbase.__all_tenant_tablespace B       ON A.tenant_id = B.tenant_id AND A.tablespace_id = B.tablespace_id       JOIN oceanbase.__all_virtual_tablet_encrypt_info E       ON E.tenant_id = effective_tenant_id() and E.tablet_id = A.tablet_id     WHERE A.tenant_id = 0 AND A.table_type != 12 AND A.table_type != 13     GROUP BY A.tenant_id, A.table_id, A.table_name, B.tablespace_id, B.encryption_name, B.encrypt_key, B.master_key_id   )__"))) {
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

int ObInnerTableSchema::v_encrypted_tablespaces_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_ENCRYPTED_TABLESPACES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_ENCRYPTED_TABLESPACES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       tablespace_id AS TABLESPACE_ID,       encryptionalg AS ENCRYPTIONALG,       CASE WHEN encryptionalg != '' AND sum(blocks_decrypted) = 0 THEN 'YES'           ELSE 'NO' END AS ENCRYPTED,       encryptedkey AS ENCRYPTEDKEY,       masterkeyid AS MASTERKEYID,       sum(blocks_encrypted) AS BLOCKS_ENCRYPTED,       sum(blocks_decrypted) AS BLOCKS_DECRYPTED,       CASE WHEN (encryptionalg != '' AND sum(blocks_decrypted) > 0) THEN 'ENCRYPTING'            WHEN (encryptionalg = '' AND sum(blocks_encrypted) > 0) THEN 'DECRYPTING'         ELSE 'NORMAL' END AS STATUS,       con_id AS CON_ID     FROM       oceanbase.V$OB_ENCRYPTED_TABLES     GROUP BY con_id, tablespace_id, encryptionalg, encryptedkey, masterkeyid   )__"))) {
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

int ObInnerTableSchema::cdb_ob_archivelog_piece_files_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_ARCHIVELOG_PIECE_FILES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_ARCHIVELOG_PIECE_FILES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     DEST_ID,     ROUND_ID,     PIECE_ID,     INCARNATION,     DEST_NO,     STATUS,     START_SCN,     CASE       WHEN START_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(START_SCN)       END AS START_SCN_DISPLAY,     CHECKPOINT_SCN,     CASE       WHEN CHECKPOINT_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(CHECKPOINT_SCN)       END AS CHECKPOINT_SCN_DISPLAY,     MAX_SCN,     END_SCN,     CASE       WHEN END_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(END_SCN)       END AS END_SCN_DISPLAY,     COMPATIBLE,     UNIT_SIZE,     COMPRESSION,     INPUT_BYTES,     CASE       WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN INPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN INPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')       END AS INPUT_BYTES_DISPLAY,     OUTPUT_BYTES,     CASE       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')       WHEN OUTPUT_BYTES >= 1024*1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')       WHEN OUTPUT_BYTES >= 1024*1024*1024         THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')       ELSE         CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')       END AS OUTPUT_BYTES_DISPLAY,     CASE       WHEN INPUT_BYTES = 0         THEN 0       ELSE         ROUND(OUTPUT_BYTES / INPUT_BYTES, 2)       END AS COMPRESSION_RATIO,     FILE_STATUS,     PATH     FROM OCEANBASE.__ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES )__"))) {
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

int ObInnerTableSchema::cdb_ob_backup_set_files_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_BACKUP_SET_FILES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_BACKUP_SET_FILES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT     TENANT_ID,     BACKUP_SET_ID,     DEST_ID,     INCARNATION,     BACKUP_TYPE,     PREV_FULL_BACKUP_SET_ID,     PREV_INC_BACKUP_SET_ID,     USEC_TO_TIME(START_TS) AS START_TIMESTAMP,     CASE       WHEN END_TS = 0           THEN NULL       ELSE           USEC_TO_TIME(END_TS)       END AS END_TIMESTAMP,     STATUS,     FILE_STATUS,     CASE       WHEN END_TS = 0         THEN 0       ELSE         ROUND((END_TS - START_TS)/1000/1000,0)       END AS ELAPSED_SECONDES,     PLUS_ARCHIVELOG,     START_REPLAY_SCN,     CASE       WHEN START_REPLAY_SCN = 0         THEN NULL       ELSE         SCN_TO_TIMESTAMP(START_REPLAY_SCN)       END AS START_REPLAY_SCN_DISPLAY,     MIN_RESTORE_SCN,     CASE       WHEN MIN_RESTORE_SCN_DISPLAY != ''         THEN MIN_RESTORE_SCN_DISPLAY       WHEN MIN_RESTORE_SCN = 0          THEN NULL       ELSE         SCN_TO_TIMESTAMP(MIN_RESTORE_SCN)       END AS MIN_RESTORE_SCN_DISPLAY,     INPUT_BYTES,     OUTPUT_BYTES,     CASE       WHEN END_TS = 0         THEN 0       ELSE         OUTPUT_BYTES / ((END_TS - START_TS)/1000/1000)       END AS OUTPUT_RATE_BYTES,     EXTRA_BYTES AS EXTRA_META_BYTES,     TABLET_COUNT,     FINISH_TABLET_COUNT,     MACRO_BLOCK_COUNT,     FINISH_MACRO_BLOCK_COUNT,     FILE_COUNT,     META_TURN_ID,     DATA_TURN_ID,     RESULT,     COMMENT,     ENCRYPTION_MODE,     PASSWD,     TENANT_COMPATIBLE,     BACKUP_COMPATIBLE,     PATH,     CLUSTER_VERSION,     CONSISTENT_SCN,     MINOR_TURN_ID,     MAJOR_TURN_ID     FROM OCEANBASE.__ALL_VIRTUAL_BACKUP_SET_FILES )__"))) {
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

int ObInnerTableSchema::connection_control_failed_login_attempts_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   select     concat('''',u.user_name,'''@''',u.host,'''') as USERHOST,     s.failed_login_attempts as FAILED_ATTEMPTS   from oceanbase.__all_virtual_tenant_user_failed_login_stat s   join oceanbase.__all_virtual_user u   on s.tenant_id = u.tenant_id and s.user_id = u.user_id   where s.tenant_id = effective_tenant_id() )__"))) {
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

int ObInnerTableSchema::gv_ob_tenant_memory_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TENANT_MEMORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TENANT_MEMORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT      TENANT_ID,      svr_ip AS SVR_IP,      svr_port AS SVR_PORT,      hold AS HOLD,      `limit` - hold AS FREE FROM     oceanbase.__all_virtual_tenant_memory_info ORDER BY tenant_id, svr_ip, svr_port )__"))) {
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

int ObInnerTableSchema::v_ob_tenant_memory_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TENANT_MEMORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TENANT_MEMORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT TENANT_ID,        SVR_IP,        SVR_PORT,        HOLD,        FREE FROM     oceanbase.GV$OB_TENANT_MEMORY WHERE         SVR_IP=HOST_IP()     AND         SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_px_target_monitor_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_PX_TARGET_MONITOR_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_PX_TARGET_MONITOR_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           SVR_IP,           SVR_PORT,           TENANT_ID,           CASE is_leader WHEN 1 THEN 'Y'                          ELSE 'N' END AS IS_LEADER,           VERSION,           PEER_IP,           PEER_PORT,           PEER_TARGET,           PEER_TARGET_USED,           LOCAL_TARGET_USED,           LOCAL_PARALLEL_SESSION_COUNT         FROM oceanbase.__all_virtual_px_target_monitor )__"))) {
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

int ObInnerTableSchema::v_ob_px_target_monitor_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_PX_TARGET_MONITOR_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_PX_TARGET_MONITOR_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT                           SVR_IP,                           SVR_PORT,                           TENANT_ID,                           IS_LEADER,                           VERSION,                           PEER_IP,                           PEER_PORT,                           PEER_TARGET,                           PEER_TARGET_USED,                           LOCAL_TARGET_USED,                           LOCAL_PARALLEL_SESSION_COUNT         FROM oceanbase.GV$OB_PX_TARGET_MONITOR         WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT() )__"))) {
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
