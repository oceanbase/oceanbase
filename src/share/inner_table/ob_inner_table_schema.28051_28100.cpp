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

int ObInnerTableSchema::v_ob_sql_workarea_memory_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT MAX_WORKAREA_SIZE,       WORKAREA_HOLD_SIZE,       MAX_AUTO_WORKAREA_SIZE,       MEM_TARGET,       TOTAL_MEM_USED,       GLOBAL_MEM_BOUND,       DRIFT_SIZE,       WORKAREA_COUNT,       MANUAL_CALC_COUNT,       SVR_IP,       SVR_PORT   FROM SYS.GV$OB_SQL_WORKAREA_MEMORY_INFO   WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_plan_cache_reference_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT SVR_IP, SVR_PORT, TENANT_ID, PC_REF_PLAN_LOCAL, PC_REF_PLAN_REMOTE, PC_REF_PLAN_DIST, PC_REF_PLAN_ARR, PC_REF_PL, PC_REF_PL_STAT, PLAN_GEN, CLI_QUERY, OUTLINE_EXEC, PLAN_EXPLAIN, ASYN_BASELINE, LOAD_BASELINE, PS_EXEC, GV_SQL, PL_ANON, PL_ROUTINE, PACKAGE_VAR, PACKAGE_TYPE, PACKAGE_SPEC, PACKAGE_BODY, PACKAGE_RESV, GET_PKG, INDEX_BUILDER, PCV_SET, PCV_RD, PCV_WR, PCV_GET_PLAN_KEY, PCV_GET_PL_KEY, PCV_EXPIRE_BY_USED, PCV_EXPIRE_BY_MEM, LC_REF_CACHE_NODE, LC_NODE, LC_NODE_RD, LC_NODE_WR, LC_REF_CACHE_OBJ_STAT FROM SYS.ALL_VIRTUAL_PLAN_CACHE_STAT )__"))) {
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

int ObInnerTableSchema::v_ob_plan_cache_reference_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT SVR_IP, SVR_PORT, TENANT_ID, PC_REF_PLAN_LOCAL, PC_REF_PLAN_REMOTE, PC_REF_PLAN_DIST, PC_REF_PLAN_ARR, PC_REF_PL, PC_REF_PL_STAT, PLAN_GEN, CLI_QUERY, OUTLINE_EXEC, PLAN_EXPLAIN, ASYN_BASELINE, LOAD_BASELINE, PS_EXEC, GV_SQL, PL_ANON, PL_ROUTINE, PACKAGE_VAR, PACKAGE_TYPE, PACKAGE_SPEC, PACKAGE_BODY, PACKAGE_RESV, GET_PKG, INDEX_BUILDER, PCV_SET, PCV_RD, PCV_WR, PCV_GET_PLAN_KEY, PCV_GET_PL_KEY, PCV_EXPIRE_BY_USED, PCV_EXPIRE_BY_MEM, LC_REF_CACHE_NODE, LC_NODE, LC_NODE_RD, LC_NODE_WR, LC_REF_CACHE_OBJ_STAT FROM SYS.GV$OB_PLAN_CACHE_REFERENCE_INFO WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_sql_workarea_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_SQL_WORKAREA_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_SQL_WORKAREA_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CAST(NULL AS RAW(8)) AS ADDRESS,       CAST(NULL AS NUMBER) AS HASH_VALUE,       DB_ID,       SQL_ID,       CAST(PLAN_ID AS NUMBER) AS CHILD_NUMBER,       CAST(NULL AS RAW(8)) AS WORKAREA_ADDRESS,       CAST(OPERATION_TYPE AS VARCHAR2(160)) AS OPERATION_TYPE,       CAST(OPERATION_ID AS NUMBER) AS OPERATION_ID,       CAST(POLICY AS VARCHAR2(40)) AS POLICY,       CAST(ESTIMATED_OPTIMAL_SIZE AS NUMBER) AS ESTIMATED_OPTIMAL_SIZE,       CAST(ESTIMATED_ONEPASS_SIZE AS NUMBER) AS ESTIMATED_ONEPASS_SIZE,       CAST(LAST_MEMORY_USED AS NUMBER) AS LAST_MEMORY_USED,       CAST(LAST_EXECUTION AS VARCHAR2(40)) AS LAST_EXECUTION,       CAST(LAST_DEGREE AS NUMBER) AS LAST_DEGREE,       CAST(TOTAL_EXECUTIONS AS NUMBER) AS TOTAL_EXECUTIONS,       CAST(OPTIMAL_EXECUTIONS AS NUMBER) AS OPTIMAL_EXECUTIONS,       CAST(ONEPASS_EXECUTIONS AS NUMBER) AS ONEPASS_EXECUTIONS,       CAST(MULTIPASSES_EXECUTIONS AS NUMBER) AS MULTIPASSES_EXECUTIONS,       CAST(ACTIVE_TIME AS NUMBER) AS ACTIVE_TIME,       CAST(MAX_TEMPSEG_SIZE AS NUMBER) AS MAX_TEMPSEG_SIZE,       CAST(LAST_TEMPSEG_SIZE AS NUMBER) AS LAST_TEMPSEG_SIZE,       CAST(TENANT_ID AS NUMBER) AS CON_ID,       SVR_IP,       SVR_PORT   FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT )__"))) {
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

int ObInnerTableSchema::v_sql_workarea_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_SQL_WORKAREA_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_SQL_WORKAREA_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT ADDRESS, HASH_VALUE, DB_ID, SQL_ID, CHILD_NUMBER, WORKAREA_ADDRESS, OPERATION_TYPE, OPERATION_ID, POLICY, ESTIMATED_OPTIMAL_SIZE, ESTIMATED_ONEPASS_SIZE, LAST_MEMORY_USED, LAST_EXECUTION, LAST_DEGREE, TOTAL_EXECUTIONS, OPTIMAL_EXECUTIONS, ONEPASS_EXECUTIONS, MULTIPASSES_EXECUTIONS, ACTIVE_TIME, MAX_TEMPSEG_SIZE, LAST_TEMPSEG_SIZE, CON_ID, SVR_IP, SVR_PORT   FROM SYS.GV$SQL_WORKAREA   WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_sstables_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SSTABLES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SSTABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT  M.SVR_IP,  M.SVR_PORT,   (case M.TABLE_TYPE     when 0 then 'MEMTABLE' when 1 then 'TX_DATA_MEMTABLE' when 2 then 'TX_CTX_MEMTABLE'     when 3 then 'LOCK_MEMTABLE' when 4 then 'DIRECT_LOAD_MEMTABLE' when 10 then 'MAJOR' when 11 then 'MINOR'     when 12 then 'MINI' when 13 then 'META'     when 14 then 'DDL_DUMP' when 15 then 'REMOTE_LOGICAL_MINOR' when 16 then 'DDL_MEM'     when 17 then 'CO_MAJOR' when 18 then 'NORMAL_CG' when 19 then 'ROWKEY_CG' when 20 then 'COL_ORIENTED_META'     when 21 then 'DDL_MERGE_CO' when 22 then 'DDL_MERGE_CG' when 23 then 'DDL_MEM_CO'     when 24 then 'DDL_MEM_CG' when 25 then 'DDL_MEM_MINI_SSTABLE'     when 26 then 'MDS_MINI' when 27 then 'MDS_MINOR'     else 'INVALID'   end) as TABLE_TYPE,  M.LS_ID,  M.TABLET_ID,  M.START_LOG_SCN,  M.END_LOG_SCN,  M."SIZE",  M.REF,  M.UPPER_TRANS_VERSION,  M.IS_ACTIVE,  M.CONTAIN_UNCOMMITTED_ROW FROM  SYS.ALL_VIRTUAL_TABLE_MGR M )__"))) {
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

int ObInnerTableSchema::v_ob_sstables_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SSTABLES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SSTABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT M.SVR_IP,  M.SVR_PORT,  M.TABLE_TYPE,  M.LS_ID,  M.TABLET_ID,  M.START_LOG_SCN,  M.END_LOG_SCN,  M."SIZE",  M.REF,  M.UPPER_TRANS_VERSION,  M.IS_ACTIVE,  M.CONTAIN_UNCOMMITTED_ROW FROM SYS.GV$OB_SSTABLES M WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_server_schema_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SERVER_SCHEMA_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SERVER_SCHEMA_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP,   SVR_PORT,   TENANT_ID,   REFRESHED_SCHEMA_VERSION,   RECEIVED_SCHEMA_VERSION,   SCHEMA_COUNT,   SCHEMA_SIZE,   MIN_SSTABLE_SCHEMA_VERSION FROM   SYS.ALL_VIRTUAL_SERVER_SCHEMA_INFO )__"))) {
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

int ObInnerTableSchema::v_ob_server_schema_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SERVER_SCHEMA_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SERVER_SCHEMA_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT SVR_IP,   SVR_PORT,   TENANT_ID,   REFRESHED_SCHEMA_VERSION,   RECEIVED_SCHEMA_VERSION,   SCHEMA_COUNT,   SCHEMA_SIZE,   MIN_SSTABLE_SCHEMA_VERSION FROM   SYS.GV$OB_SERVER_SCHEMA_INFO WHERE   SVR_IP=HOST_IP() AND   SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_sql_plan_monitor_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_SQL_PLAN_MONITOR_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_SQL_PLAN_MONITOR_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           CAST(TENANT_ID AS NUMBER) CON_ID,           REQUEST_ID,           CAST(NULL AS NUMBER) KEY,           CAST(NULL AS VARCHAR2(19)) STATUS,           SVR_IP,           SVR_PORT,           TRACE_ID,           CAST(FIRST_REFRESH_TIME AS TIMESTAMP) FIRST_REFRESH_TIME,           CAST(LAST_REFRESH_TIME AS TIMESTAMP) LAST_REFRESH_TIME,           CAST(FIRST_CHANGE_TIME AS TIMESTAMP) FIRST_CHANGE_TIME,           CAST(LAST_CHANGE_TIME AS TIMESTAMP) LAST_CHANGE_TIME,           CAST(NULL AS NUMBER) REFRESH_COUNT,           CAST(NULL AS NUMBER) SID,           CAST(THREAD_ID AS VARCHAR2(10)) PROCESS_NAME,           CAST(NULL AS VARCHAR2(13)) SQL_ID,           CAST(NULL AS TIMESTAMP) SQL_EXEC_START,           CAST(NULL AS NUMBER) SQL_EXEC_ID,           CAST(NULL AS NUMBER) SQL_PLAN_HASH_VALUE,           CAST(NULL AS RAW(8)) SQL_CHILD_ADDRESS,           CAST(NULL AS NUMBER) PLAN_PARENT_ID,           CAST(PLAN_LINE_ID AS NUMBER) PLAN_LINE_ID,           CAST(PLAN_OPERATION AS VARCHAR2(30)) PLAN_OPERATION,           CAST(NULL AS VARCHAR2(30)) PLAN_OPTIONS,           CAST(NULL AS VARCHAR2(128)) PLAN_OBJECT_OWNER,           CAST(NULL AS VARCHAR2(128)) PLAN_OBJECT_NAME,           CAST(NULL AS VARCHAR2(80)) PLAN_OBJECT_TYPE,           CAST(PLAN_DEPTH AS NUMBER) PLAN_DEPTH,           CAST(NULL AS NUMBER) PLAN_POSITION,           CAST(NULL AS NUMBER) PLAN_COST,           CAST(NULL AS NUMBER) PLAN_CARDINALITY,           CAST(NULL AS NUMBER) PLAN_BYTES,           CAST(NULL AS NUMBER) PLAN_TIME,           CAST(NULL AS VARCHAR2(256)) PLAN_PARTITION_START,           CAST(NULL AS VARCHAR2(256)) PLAN_PARTITION_STOP,           CAST(NULL AS NUMBER) PLAN_CPU_COST,           CAST(NULL AS NUMBER) PLAN_IO_COST,           CAST(NULL AS NUMBER) PLAN_TEMP_SPACE,           CAST(STARTS AS NUMBER) STARTS,           CAST(OUTPUT_ROWS AS NUMBER) OUTPUT_ROWS,           CAST(NULL AS NUMBER) IO_INTERCONNECT_BYTES,           CAST(NULL AS NUMBER) PHYSICAL_READ_REQUESTS,           CAST(NULL AS NUMBER) PHYSICAL_READ_BYTES,           CAST(NULL AS NUMBER) PHYSICAL_WRITE_REQUESTS,           CAST(NULL AS NUMBER) PHYSICAL_WRITE_BYTES,           CAST(WORKAREA_MEM AS NUMBER) WORKAREA_MEM,           CAST(WORKAREA_MAX_MEM AS NUMBER) WORKAREA_MAX_MEM,           CAST(WORKAREA_TEMPSEG AS NUMBER) WORKAREA_TEMPSEG,           CAST(WORKAREA_MAX_TEMPSEG AS NUMBER) WORKAREA_MAX_TEMPSEG,           CAST(NULL AS NUMBER) OTHERSTAT_GROUP_ID,           CAST(OTHERSTAT_1_ID AS NUMBER) OTHERSTAT_1_ID,           CAST(NULL AS NUMBER) OTHERSTAT_1_TYPE,           CAST(OTHERSTAT_1_VALUE AS NUMBER) OTHERSTAT_1_VALUE,           CAST(OTHERSTAT_2_ID AS NUMBER) OTHERSTAT_2_ID,           CAST(NULL AS NUMBER) OTHERSTAT_2_TYPE,           CAST(OTHERSTAT_2_VALUE AS NUMBER) OTHERSTAT_2_VALUE,           CAST(OTHERSTAT_3_ID AS NUMBER) OTHERSTAT_3_ID,           CAST(NULL AS NUMBER) OTHERSTAT_3_TYPE,           CAST(OTHERSTAT_3_VALUE AS NUMBER) OTHERSTAT_3_VALUE,           CAST(OTHERSTAT_4_ID AS NUMBER) OTHERSTAT_4_ID,           CAST(NULL AS NUMBER) OTHERSTAT_4_TYPE,           CAST(OTHERSTAT_4_VALUE AS NUMBER) OTHERSTAT_4_VALUE,           CAST(OTHERSTAT_5_ID AS NUMBER) OTHERSTAT_5_ID,           CAST(NULL AS NUMBER) OTHERSTAT_5_TYPE,           CAST(OTHERSTAT_5_VALUE AS NUMBER) OTHERSTAT_5_VALUE,           CAST(OTHERSTAT_6_ID AS NUMBER) OTHERSTAT_6_ID,           CAST(NULL AS NUMBER) OTHERSTAT_6_TYPE,           CAST(OTHERSTAT_6_VALUE AS NUMBER) OTHERSTAT_6_VALUE,           CAST(OTHERSTAT_7_ID AS NUMBER) OTHERSTAT_7_ID,           CAST(NULL AS NUMBER) OTHERSTAT_7_TYPE,           CAST(OTHERSTAT_7_VALUE AS NUMBER) OTHERSTAT_7_VALUE,           CAST(OTHERSTAT_8_ID AS NUMBER) OTHERSTAT_8_ID,           CAST(NULL AS NUMBER) OTHERSTAT_8_TYPE,           CAST(OTHERSTAT_8_VALUE AS NUMBER) OTHERSTAT_8_VALUE,           CAST(OTHERSTAT_9_ID AS NUMBER) OTHERSTAT_9_ID,           CAST(NULL AS NUMBER) OTHERSTAT_9_TYPE,           CAST(OTHERSTAT_9_VALUE AS NUMBER) OTHERSTAT_9_VALUE,           CAST(OTHERSTAT_10_ID AS NUMBER) OTHERSTAT_10_ID,           CAST(NULL AS NUMBER) OTHERSTAT_10_TYPE,           CAST(OTHERSTAT_10_VALUE AS NUMBER) OTHERSTAT_10_VALUE,           CAST(NULL AS VARCHAR(1)) OTHER_XML,           CAST(NULL AS NUMBER) PLAN_OPERATION_INACTIVE,           OUTPUT_BATCHES,           SKIPPED_ROWS_COUNT,           DB_TIME,           USER_IO_WAIT_TIME,           NULL OTHER_WAIT_TIME         FROM SYS.ALL_VIRTUAL_SQL_PLAN_MONITOR )__"))) {
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

int ObInnerTableSchema::v_sql_plan_monitor_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_SQL_PLAN_MONITOR_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_SQL_PLAN_MONITOR_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT CON_ID, REQUEST_ID, KEY, STATUS, SVR_IP, SVR_PORT, TRACE_ID, FIRST_REFRESH_TIME, LAST_REFRESH_TIME, FIRST_CHANGE_TIME, LAST_CHANGE_TIME, REFRESH_COUNT, SID, PROCESS_NAME, SQL_ID, SQL_EXEC_START, SQL_EXEC_ID, SQL_PLAN_HASH_VALUE, SQL_CHILD_ADDRESS, PLAN_PARENT_ID, PLAN_LINE_ID, PLAN_OPERATION, PLAN_OPTIONS, PLAN_OBJECT_OWNER, PLAN_OBJECT_NAME, PLAN_OBJECT_TYPE, PLAN_DEPTH, PLAN_POSITION, PLAN_COST, PLAN_CARDINALITY, PLAN_BYTES, PLAN_TIME, PLAN_PARTITION_START, PLAN_PARTITION_STOP, PLAN_CPU_COST, PLAN_IO_COST, PLAN_TEMP_SPACE, STARTS, OUTPUT_ROWS, IO_INTERCONNECT_BYTES, PHYSICAL_READ_REQUESTS, PHYSICAL_READ_BYTES, PHYSICAL_WRITE_REQUESTS, PHYSICAL_WRITE_BYTES, WORKAREA_MEM, WORKAREA_MAX_MEM, WORKAREA_TEMPSEG, WORKAREA_MAX_TEMPSEG, OTHERSTAT_GROUP_ID, OTHERSTAT_1_ID, OTHERSTAT_1_TYPE, OTHERSTAT_1_VALUE, OTHERSTAT_2_ID, OTHERSTAT_2_TYPE, OTHERSTAT_2_VALUE, OTHERSTAT_3_ID, OTHERSTAT_3_TYPE, OTHERSTAT_3_VALUE, OTHERSTAT_4_ID, OTHERSTAT_4_TYPE, OTHERSTAT_4_VALUE, OTHERSTAT_5_ID, OTHERSTAT_5_TYPE, OTHERSTAT_5_VALUE, OTHERSTAT_6_ID, OTHERSTAT_6_TYPE, OTHERSTAT_6_VALUE, OTHERSTAT_7_ID, OTHERSTAT_7_TYPE, OTHERSTAT_7_VALUE, OTHERSTAT_8_ID, OTHERSTAT_8_TYPE, OTHERSTAT_8_VALUE, OTHERSTAT_9_ID, OTHERSTAT_9_TYPE, OTHERSTAT_9_VALUE, OTHERSTAT_10_ID, OTHERSTAT_10_TYPE, OTHERSTAT_10_VALUE, OTHER_XML, PLAN_OPERATION_INACTIVE, OUTPUT_BATCHES, SKIPPED_ROWS_COUNT, DB_TIME, USER_IO_WAIT_TIME, OTHER_WAIT_TIME FROM SYS.GV$SQL_PLAN_MONITOR  WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::v_sql_monitor_statname_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_SQL_MONITOR_STATNAME_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_SQL_MONITOR_STATNAME_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(NULL AS NUMBER) CON_ID,       CAST(ID AS NUMBER) ID,       CAST(GROUP_ID AS NUMBER) GROUP_ID,       CAST(NAME AS VARCHAR2(40)) NAME,       CAST(DESCRIPTION AS VARCHAR2(200)) DESCRIPTION,       CAST(TYPE AS NUMBER) TYPE,       CAST(0 AS NUMBER) FLAGS     FROM SYS.ALL_VIRTUAL_SQL_MONITOR_STATNAME )__"))) {
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

int ObInnerTableSchema::gv_open_cursor_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OPEN_CURSOR_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OPEN_CURSOR_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       SVR_IP,       SVR_PORT,       CAST(SADDR AS VARCHAR2(8)) SADDR,       CAST(SID AS NUMBER) SID,       CAST(USER_NAME AS VARCHAR2(30)) USER_NAME,       CAST(ADDRESS AS VARCHAR2(8)) ADDRESS,       CAST(HASH_VALUE AS NUMBER) HASH_VALUE,       CAST(SQL_ID AS VARCHAR2(32)) SQL_ID,       CAST(SQL_TEXT AS VARCHAR2(60)) SQL_TEXT,       CAST(LAST_SQL_ACTIVE_TIME as DATE) LAST_SQL_ACTIVE_TIME,       CAST(SQL_EXEC_ID AS NUMBER) SQL_EXEC_ID,       CAST(CURSOR_TYPE AS VARCHAR2(30)) CURSOR_TYPE,       CAST(CHILD_ADDRESS AS VARCHAR2(30)) CHILD_ADDRESS,       CAST(CON_ID AS NUMBER) CON_ID FROM SYS.ALL_VIRTUAL_OPEN_CURSOR   )__"))) {
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

int ObInnerTableSchema::v_open_cursor_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OPEN_CURSOR_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OPEN_CURSOR_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT SVR_IP, SVR_PORT, SADDR, SID, USER_NAME, ADDRESS, HASH_VALUE, SQL_ID, SQL_TEXT, LAST_SQL_ACTIVE_TIME, SQL_EXEC_ID, CURSOR_TYPE, CHILD_ADDRESS, CON_ID FROM SYS.GV$OPEN_CURSOR       WHERE svr_ip = host_ip() AND svr_port = rpc_port()   )__"))) {
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

int ObInnerTableSchema::v_timezone_names_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_TIMEZONE_NAMES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_TIMEZONE_NAMES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       NA.NAME AS TZNAME,       TR.ABBREVIATION AS TZABBREV,       NA.TENANT_ID AS CON_ID     FROM SYS.ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT NA         JOIN SYS.ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT TR         ON NA.TIME_ZONE_ID = TR.TIME_ZONE_ID         AND NA.TENANT_ID = EFFECTIVE_TENANT_ID()         AND TR.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::gv_global_transaction_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_GLOBAL_TRANSACTION_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_GLOBAL_TRANSACTION_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT A.FORMAT_ID AS FORMATID,       A.SCHEDULER_IP AS SVR_IP,       A.SCHEDULER_PORT AS SVR_PORT,       RAWTOHEX(A.GTRID) AS GLOBALID,       RAWTOHEX(A.BQUAL) AS BRANCHID,       B.BRANCHES AS BRANCHES,       B.BRANCHES AS REFCOUNT,       NVL(C.PREPARECOUNT, 0) AS PREPARECOUNT,       'ACTIVE' AS STATE,       0 AS FLAGS,       CASE WHEN bitand(flag,65536)=65536 THEN 'LOOSELY COUPLED' ELSE 'TIGHTLY COUPLED' END AS COUPLING,       SYS_CONTEXT('USERENV', 'CON_ID') AS CON_ID     FROM SYS.ALL_VIRTUAL_GLOBAL_TRANSACTION A     LEFT JOIN (SELECT GTRID, FORMAT_ID, COUNT(BQUAL) AS BRANCHES FROM SYS.ALL_VIRTUAL_GLOBAL_TRANSACTION GROUP BY GTRID, FORMAT_ID) B     ON A.GTRID = B.GTRID AND A.FORMAT_ID = B.FORMAT_ID     LEFT JOIN (SELECT GTRID, FORMAT_ID, COUNT(BQUAL) AS PREPARECOUNT FROM SYS.ALL_VIRTUAL_GLOBAL_TRANSACTION WHERE STATE = 3 GROUP BY GTRID, FORMAT_ID) C     ON B.GTRID = C.GTRID AND B.FORMAT_ID = C.FORMAT_ID     WHERE A.format_id != -2   )__"))) {
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

int ObInnerTableSchema::v_global_transaction_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_GLOBAL_TRANSACTION_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_GLOBAL_TRANSACTION_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       FORMATID,       GLOBALID,       BRANCHID,       BRANCHES,       REFCOUNT,       PREPARECOUNT,       STATE,       FLAGS,       COUPLING,       CON_ID     FROM SYS.GV$GLOBAL_TRANSACTION     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()   )__"))) {
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

int ObInnerTableSchema::v_rsrc_plan_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_RSRC_PLAN_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_RSRC_PLAN_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           CAST(NULL as NUMBER) AS ID,           B.plan NAME,           CAST('TRUE' AS VARCHAR2(5)) AS IS_TOP_PLAN,           CAST('ON' AS VARCHAR2(3)) AS CPU_MANAGED,           CAST(NULL AS VARCHAR2(3)) AS INSTANCE_CAGING,           CAST(NULL AS NUMBER) AS PARALLEL_SERVERS_ACTIVE,           CAST(NULL AS NUMBER) AS PARALLEL_SERVERS_TOTAL,           CAST(NULL AS VARCHAR2(32)) AS PARALLEL_EXECUTION_MANAGED         FROM SYS.tenant_virtual_global_variable A, SYS.DBA_RSRC_PLANS B         WHERE A.variable_name = 'resource_manager_plan' AND A.value = B.plan )__"))) {
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

int ObInnerTableSchema::v_ob_encrypted_tables_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_ENCRYPTED_TABLES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_ENCRYPTED_TABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       A.TABLE_ID AS TABLE_ID,       A.TABLE_NAME AS TABLE_NAME,       B.TABLESPACE_ID AS TABLESPACE_ID,       B.ENCRYPTION_NAME AS ENCRYPTIONALG,       CAST(CASE WHEN B.ENCRYPTION_NAME IS NOT NULL AND SUM(ENCRYPTED_MACRO_BLOCK_COUNT) = SUM(MACRO_BLOCK_COUNT) THEN 'YES'               ELSE 'NO' END AS VARCHAR2(3)) AS ENCRYPTED,       RAWTOHEX(B.ENCRYPT_KEY) AS ENCRYPTEDKEY,       B.MASTER_KEY_ID AS MASTERKEYID,       SUM(ENCRYPTED_MACRO_BLOCK_COUNT) AS BLOCKS_ENCRYPTED,       (SUM(MACRO_BLOCK_COUNT) - SUM(ENCRYPTED_MACRO_BLOCK_COUNT)) AS BLOCKS_DECRYPTED,       CAST(CASE WHEN (B.ENCRYPTION_NAME IS NOT NULL AND SUM(ENCRYPTED_MACRO_BLOCK_COUNT) < SUM(MACRO_BLOCK_COUNT)) THEN 'ENCRYPTING'                 WHEN (B.ENCRYPTION_NAME IS NULL AND SUM(ENCRYPTED_MACRO_BLOCK_COUNT) > 0) THEN 'DECRYPTING'               ELSE 'NORMAL' END AS VARCHAR2(10)) AS STATUS,       A.TENANT_ID AS CON_ID     FROM       (SELECT T.TENANT_ID, T.TABLE_ID, T.TABLE_NAME, T.TABLE_TYPE, T.TABLESPACE_ID, T.TABLET_ID        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T WHERE T.PART_LEVEL = 0 AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)        UNION ALL        SELECT T.TENANT_ID, T.TABLE_ID, T.TABLE_NAME, T.TABLE_TYPE, T.TABLESPACE_ID, P.TABLET_ID        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T,  SYS.ALL_VIRTUAL_PART_REAL_AGENT P        WHERE T.PART_LEVEL = 1 AND T.TENANT_ID = P.TENANT_ID AND T.TABLE_ID = P.TABLE_ID        UNION ALL        SELECT T.TENANT_ID, T.TABLE_ID, T.TABLE_NAME, T.TABLE_TYPE, T.TABLESPACE_ID, SP.TABLET_ID        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP        WHERE T.PART_LEVEL = 2 AND T.TENANT_ID = SP.TENANT_ID AND T.TABLE_ID = SP.TABLE_ID       ) A       JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT B       ON A.TENANT_ID = B.TENANT_ID AND A.TABLESPACE_ID = B.TABLESPACE_ID       JOIN SYS.ALL_VIRTUAL_TABLET_ENCRYPT_INFO E       ON A.TENANT_ID = E.TENANT_ID AND A.TABLET_ID = E.TABLET_ID     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID() AND A.TABLE_TYPE != 12 AND A.TABLE_TYPE != 13     GROUP BY A.TENANT_ID, A.TABLE_ID, A.TABLE_NAME, B.TABLESPACE_ID, B.ENCRYPTION_NAME, B.ENCRYPT_KEY, B.MASTER_KEY_ID   )__"))) {
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

int ObInnerTableSchema::v_encrypted_tablespaces_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_ENCRYPTED_TABLESPACES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_ENCRYPTED_TABLESPACES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       A.TABLESPACE_ID AS TABLESPACE_ID,       A.ENCRYPTION_NAME AS ENCRYPTIONALG,       CAST(CASE WHEN A.ENCRYPTION_NAME IS NOT NULL AND B.BLOCKS_DECRYPTED = 0 THEN 'YES'               ELSE 'NO' END AS VARCHAR2(3)) AS ENCRYPTED,       RAWTOHEX(A.ENCRYPT_KEY) AS ENCRYPTEDKEY,       A.MASTER_KEY_ID AS MASTERKEYID,       B.BLOCKS_ENCRYPTED AS BLOCKS_ENCRYPTED,       B.BLOCKS_DECRYPTED AS BLOCKS_DECRYPTED,       CAST(CASE WHEN (A.ENCRYPTION_NAME IS NOT NULL AND B.BLOCKS_DECRYPTED > 0) THEN 'ENCRYPTING'                 WHEN (A.ENCRYPTION_NAME IS NULL AND B.BLOCKS_ENCRYPTED > 0) THEN 'DECRYPTING'               ELSE 'NORMAL' END AS VARCHAR2(10)) AS STATUS,       A.TENANT_ID AS CON_ID     FROM SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT A, (SELECT         TABLESPACE_ID AS TABLESPACE_ID,         SUM(BLOCKS_ENCRYPTED) AS BLOCKS_ENCRYPTED,         SUM(BLOCKS_DECRYPTED) AS BLOCKS_DECRYPTED       FROM         SYS.V$OB_ENCRYPTED_TABLES       GROUP BY TABLESPACE_ID) B     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID() AND A.TABLESPACE_ID = B.TABLESPACE_ID   )__"))) {
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

int ObInnerTableSchema::all_tab_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TAB_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TAB_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(TC.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(TC.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as varchar(128)) as  LOW_VALUE,   cast(stat.max_value as varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM,   cast(NULL as VARCHAR2(7)) SCOPE FROM     (SELECT T.TENANT_ID,             T.DATABASE_ID,             T.TABLE_ID,             T.TABLE_NAME,             C.COLUMN_ID,             C.COLUMN_NAME,             C.IS_HIDDEN       FROM SYS.ALL_VIRTUAL_CORE_ALL_TABLE T,            SYS.ALL_VIRTUAL_CORE_COLUMN_TABLE C       WHERE C.tenant_id = T.tenant_id         AND C.table_id = T.table_id         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION ALL        SELECT T.TENANT_ID,               T.DATABASE_ID,               T.TABLE_ID,               T.TABLE_NAME,               C.COLUMN_ID,               C.COLUMN_NAME,               C.IS_HIDDEN       FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T,            SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C       WHERE table_type in (0,2,3,8,9,14)         AND bitand((TABLE_MODE / 4096), 15) IN (0,1)         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.tenant_id = T.tenant_id         AND C.table_id = T.table_id) TC   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = TC.tenant_id     AND db.database_id = TC.database_id     AND (TC.database_id = userenv('SCHEMAID')          OR user_can_access_obj(1, TC.table_id, TC.database_id) = 1)     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   LEFT JOIN     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON TC.table_id = stat.table_id     AND TC.column_id = stat.column_id     AND stat.object_type = 1 WHERE   TC.is_hidden = 0; )__"))) {
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

int ObInnerTableSchema::dba_tab_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(TC.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(TC.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as varchar(128)) as  LOW_VALUE,   cast(stat.max_value as varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM,   cast(NULL as VARCHAR2(7)) SCOPE FROM     (SELECT T.TENANT_ID,             T.DATABASE_ID,             T.TABLE_ID,             T.TABLE_NAME,             C.COLUMN_ID,             C.COLUMN_NAME,             C.IS_HIDDEN       FROM SYS.ALL_VIRTUAL_CORE_ALL_TABLE T,            SYS.ALL_VIRTUAL_CORE_COLUMN_TABLE C       WHERE C.tenant_id = T.tenant_id         AND C.table_id = T.table_id         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     UNION ALL        SELECT T.TENANT_ID,               T.DATABASE_ID,               T.TABLE_ID,               T.TABLE_NAME,               C.COLUMN_ID,               C.COLUMN_NAME,               C.IS_HIDDEN       FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T,            SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C       WHERE table_type in (0,2,3,8,9,14)         AND bitand((TABLE_MODE / 4096), 15) IN (0,1)         AND C.TENANT_ID = EFFECTIVE_TENANT_ID()         AND T.TENANT_ID = EFFECTIVE_TENANT_ID()         AND C.tenant_id = T.tenant_id         AND C.table_id = T.table_id) TC   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = TC.tenant_id     AND db.database_id = TC.database_id     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   LEFT JOIN     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON TC.table_id = stat.table_id     AND TC.column_id = stat.column_id     AND stat.object_type = 1 WHERE   TC.is_hidden = 0; )__"))) {
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

int ObInnerTableSchema::user_tab_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TAB_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TAB_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as varchar(128)) as  LOW_VALUE,   cast(stat.max_value as varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM,   cast(NULL as VARCHAR2(7)) SCOPE FROM     (SELECT TENANT_ID,             DATABASE_ID,             TABLE_ID,             TABLE_NAME       FROM SYS.ALL_VIRTUAL_CORE_ALL_TABLE     UNION ALL        SELECT TENANT_ID,               DATABASE_ID,               TABLE_ID,               TABLE_NAME       FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT       WHERE table_type in (0,2,3,8,9,14)       AND bitand((TABLE_MODE / 4096), 15) IN (0,1)) t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND t.database_id = userenv('SCHEMAID')     AND t.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c   ON c.tenant_id = t.tenant_id   AND c.table_id = t.table_id   LEFT JOIN     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND stat.object_type = 1 WHERE   c.is_hidden = 0; )__"))) {
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

int ObInnerTableSchema::all_part_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PART_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast (part.part_name as VARCHAR2(128)) as PARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,   cast(stat.max_value as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND (t.database_id = userenv('SCHEMAID')          OR user_can_access_obj(1, t.table_id, t.database_id) = 1)     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN    SYS.ALL_VIRTUAL_PART_REAL_AGENT part    on t.tenant_id = part.tenant_id    and t.table_id = part.table_id   left join     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND part.part_id = stat.partition_id     AND stat.object_type = 2 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::dba_part_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PART_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PART_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast (part.part_name as VARCHAR2(128)) as PARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,   cast(stat.max_value as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN    SYS.ALL_VIRTUAL_PART_REAL_AGENT part    on t.tenant_id = part.tenant_id    and t.table_id = part.table_id   left join     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND part.part_id = stat.partition_id     AND stat.object_type = 2 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::user_part_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_PART_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PART_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast (part.part_name as VARCHAR2(128)) as PARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,   cast(stat.max_value as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN    SYS.ALL_VIRTUAL_PART_REAL_AGENT part    on t.tenant_id = part.tenant_id    and t.table_id = part.table_id   left join     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND part.part_id = stat.partition_id     AND stat.object_type = 2 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND t.database_id = USERENV('SCHEMAID')   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::all_subpart_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SUBPART_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SUBPART_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast (subpart.sub_part_name as VARCHAR2(128)) as SUBPARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,   cast(stat.max_value as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND (t.database_id = userenv('SCHEMAID')          OR user_can_access_obj(1, t.table_id, t.database_id) = 1)     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT subpart    on t.tenant_id = subpart.tenant_id    and t.table_id = subpart.table_id   left join     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND stat.partition_id = subpart.sub_part_id     AND stat.object_type = 3 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::dba_subpart_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SUBPART_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SUBPART_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast (subpart.sub_part_name as VARCHAR2(128)) as SUBPARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,   cast(stat.max_value as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT subpart    on t.tenant_id = subpart.tenant_id    and t.table_id = subpart.table_id   left join     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND stat.partition_id = subpart.sub_part_id     AND stat.object_type = 3 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::user_subpart_col_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_SUBPART_COL_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SUBPART_COL_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast (subpart.sub_part_name as VARCHAR2(128)) as SUBPARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(stat.distinct_cnt as NUMBER) as  NUM_DISTINCT,   cast(stat.min_value as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,   cast(stat.max_value as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,   cast(stat.density as NUMBER) as  DENSITY,   cast(stat.null_cnt as NUMBER) as  NUM_NULLS,   cast(stat.bucket_cnt as NUMBER) as  NUM_BUCKETS,   cast(stat.sample_size as NUMBER) as  SAMPLE_SIZE,   cast(stat.last_analyzed as DATE) as  LAST_ANALYZED,   CAST(decode(stat.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,   CAST(decode(stat.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,   cast(NULL as VARCHAR2(80)) as  NOTES,   cast(stat.avg_len as NUMBER) as  AVG_COL_LEN,   cast((case when stat.histogram_type = 1 then 'FREQUENCY'         when stat.histogram_type = 3 then 'TOP-FREQUENCY'         when stat.histogram_type = 4 then 'HYBRID'         else NULL end) as VARCHAR2(15)) as HISTOGRAM FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT subpart    on t.tenant_id = subpart.tenant_id    and t.table_id = subpart.table_id   left join     SYS.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT stat     ON c.table_id = stat.table_id     AND c.column_id = stat.column_id     AND stat.partition_id = subpart.sub_part_id     AND stat.object_type = 3 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND t.database_id = USERENV('SCHEMAID')   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::all_tab_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TAB_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TAB_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.b_endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT,   cast(NULL as VARCHAR2(7)) as SCOPE FROM     (SELECT TENANT_ID,             DATABASE_ID,             TABLE_ID,             TABLE_NAME       FROM SYS.ALL_VIRTUAL_CORE_ALL_TABLE     UNION ALL        SELECT TENANT_ID,               DATABASE_ID,               TABLE_ID,               TABLE_NAME       FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT       WHERE table_type in (0,2,3,8,9,14)       AND bitand((TABLE_MODE / 4096), 15) IN (0,1)) t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND (t.database_id = userenv('SCHEMAID')          OR user_can_access_obj(1, t.table_id, t.database_id) = 1)     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND hist.object_type = 1 WHERE   c.is_hidden = 0 )__"))) {
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

int ObInnerTableSchema::dba_tab_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.b_endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT,   cast(NULL as VARCHAR2(7)) as SCOPE FROM     (SELECT TENANT_ID,             DATABASE_ID,             TABLE_ID,             TABLE_NAME       FROM SYS.ALL_VIRTUAL_CORE_ALL_TABLE     UNION ALL        SELECT TENANT_ID,               DATABASE_ID,               TABLE_ID,               TABLE_NAME       FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT       WHERE table_type in (0,2,3,8,9,14)       AND bitand((TABLE_MODE / 4096), 15) IN (0,1)) t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND hist.object_type = 1 WHERE   c.is_hidden = 0 )__"))) {
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

int ObInnerTableSchema::user_tab_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TAB_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TAB_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.b_endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT,   cast(NULL as VARCHAR2(7)) as SCOPE FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND hist.object_type = 1 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND t.database_id = USERENV('SCHEMAID')   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::all_part_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PART_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(part.part_name as VARCHAR2(128)) as PARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND (t.database_id = userenv('SCHEMAID')          OR user_can_access_obj(1, t.table_id, t.database_id) = 1)     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN    SYS.ALL_VIRTUAL_PART_REAL_AGENT part    on t.tenant_id = part.tenant_id    and t.table_id = part.table_id   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND part.part_id = hist.partition_id     AND hist.object_type = 2 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1)  )__"))) {
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

int ObInnerTableSchema::dba_part_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PART_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PART_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(part.part_name as VARCHAR2(128)) as PARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN    SYS.ALL_VIRTUAL_PART_REAL_AGENT part    on t.tenant_id = part.tenant_id    and t.table_id = part.table_id   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND part.part_id = hist.partition_id     AND hist.object_type = 2 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::user_part_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_PART_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PART_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(part.part_name as VARCHAR2(128)) as PARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN    SYS.ALL_VIRTUAL_PART_REAL_AGENT part    on t.tenant_id = part.tenant_id    and t.table_id = part.table_id   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND part.part_id = hist.partition_id     AND hist.object_type = 2 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND t.database_id = USERENV('SCHEMAID')   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::all_subpart_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SUBPART_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SUBPART_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(subpart.sub_part_name as VARCHAR2(128)) as SUBPARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND (t.database_id = userenv('SCHEMAID')          OR user_can_access_obj(1, t.table_id, t.database_id) = 1)     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT subpart    on t.tenant_id = subpart.tenant_id    and t.table_id = subpart.table_id   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND hist.partition_id = subpart.sub_part_id     AND hist.object_type = 3 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::dba_subpart_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SUBPART_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SUBPART_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(db.database_name as VARCHAR2(128)) as OWNER,   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(subpart.sub_part_name as VARCHAR2(128)) as SUBPARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db     ON db.tenant_id = t.tenant_id     AND db.database_id = t.database_id     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT subpart    on t.tenant_id = subpart.tenant_id    and t.table_id = subpart.table_id   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND hist.partition_id = subpart.sub_part_id     AND hist.object_type = 3 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::user_subpart_histograms_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_SUBPART_HISTOGRAMS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SUBPART_HISTOGRAMS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select   cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,   cast(subpart.sub_part_name as VARCHAR2(128)) as SUBPARTITION_NAME,   cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,   cast(hist.endpoint_num as NUMBER) as  ENDPOINT_NUMBER,   cast(NULL as NUMBER) as  ENDPOINT_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE,   cast(hist.endpoint_value as VARCHAR2(4000)) as ENDPOINT_ACTUAL_VALUE_RAW,   cast(hist.endpoint_repeat_cnt as NUMBER) as ENDPOINT_REPEAT_COUNT FROM     SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t   JOIN     SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT c     ON c.tenant_id = t.tenant_id     AND c.table_id = t.table_id     AND C.TENANT_ID = EFFECTIVE_TENANT_ID()   JOIN     SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT subpart    on t.tenant_id = subpart.tenant_id    and t.table_id = subpart.table_id   join     SYS.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT hist     ON c.table_id = hist.table_id     AND c.column_id = hist.column_id     AND hist.partition_id = subpart.sub_part_id     AND hist.object_type = 3 WHERE   c.is_hidden = 0   AND t.table_type in (0,2,3,8,9,14)   AND t.database_id = USERENV('SCHEMAID')   AND bitand((t.TABLE_MODE / 4096), 15) IN (0,1) )__"))) {
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

int ObInnerTableSchema::all_tab_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TAB_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TAB_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS OWNER,     CAST(V.TABLE_NAME       AS  VARCHAR2(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  VARCHAR2(128)) AS PARTITION_NAME,     CAST(V.PARTITION_POSITION AS    NUMBER) AS PARTITION_POSITION,     CAST(V.SUBPARTITION_NAME  AS    VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(V.SUBPARTITION_POSITION AS NUMBER) AS SUBPARTITION_POSITION,     CAST(V.OBJECT_TYPE AS   VARCHAR2(12)) AS OBJECT_TYPE,     CAST(STAT.ROW_CNT AS    NUMBER) AS NUM_ROWS,     CAST(NULL AS    NUMBER) AS BLOCKS,     CAST(NULL AS    NUMBER) AS EMPTY_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_SPACE,     CAST(NULL AS    NUMBER) AS CHAIN_CNT,     CAST(STAT.AVG_ROW_LEN AS    NUMBER) AS AVG_ROW_LEN,     CAST(NULL AS    NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,     CAST(NULL AS    NUMBER) AS NUM_FREELIST_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHED_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHE_HIT_RATIO,     CAST(NULL AS    NUMBER) AS IM_IMCU_COUNT,     CAST(NULL AS    NUMBER) AS IM_BLOCK_COUNT,     CAST(NULL AS    TIMESTAMP(9)) AS IM_STAT_UPDATE_TIME,     CAST(NULL AS    NUMBER) AS SCAN_RATE,     CAST(STAT.SPARE1 AS    NUMBER) AS SAMPLE_SIZE,     CAST(STAT.LAST_ANALYZED AS  DATE) AS LAST_ANALYZED,     CAST(decode(STAT.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,     CAST(decode(STAT.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,     CAST(decode(bitand(STAT.STATTYPE_LOCKED, 15), NULL, NULL, 0, NULL, 1, 'DATA', 2, 'CACHE', 'ALL') AS    VARCHAR2(5)) AS STATTYPE_LOCKED,     CAST(decode(STAT.STALE_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS STALE_STATS,     CAST(NULL AS    VARCHAR2(7)) AS SCOPE     FROM     (       (SELECT TENANT_ID,               DATABASE_ID,               TABLE_ID,               TABLE_ID AS PARTITION_ID,               TABLE_NAME,               NULL AS PARTITION_NAME,               NULL AS SUBPARTITION_NAME,               NULL AS PARTITION_POSITION,               NULL AS SUBPARTITION_POSITION,               'TABLE' AS OBJECT_TYPE           FROM              SYS.ALL_VIRTUAL_CORE_ALL_TABLE           WHERE TABLE_TYPE IN (0,2,3,8,9,14,15)         UNION ALL         SELECT TENANT_ID,                DATABASE_ID,                TABLE_ID,                CASE WHEN PART_LEVEL = 0 THEN TABLE_ID ELSE -1 END AS PARTITION_ID,                TABLE_NAME,                NULL AS PARTITION_NAME,                NULL AS SUBPARTITION_NAME,                NULL AS PARTITION_POSITION,                NULL AS SUBPARTITION_POSITION,                'TABLE' AS OBJECT_TYPE         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1))     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 P.PART_ID,                 T.TABLE_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T           JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)             AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                T.TABLE_NAME,                  P.PART_NAME,                  SP.SUB_PART_NAME,                  P.PART_IDX + 1,                  SP.SUB_PART_IDX + 1,                  'SUBPARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID             AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)         JOIN             SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)     ) V     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB         ON DB.TENANT_ID = V.TENANT_ID         AND DB.DATABASE_ID = V.DATABASE_ID         AND (V.DATABASE_ID = USERENV('SCHEMAID')              OR USER_CAN_ACCESS_OBJ(1, V.TABLE_ID, V.DATABASE_ID) = 1)         AND V.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN         SYS.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND V.PARTITION_ID = STAT.PARTITION_ID )__"))) {
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

int ObInnerTableSchema::dba_tab_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TAB_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TAB_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(DB.DATABASE_NAME AS     VARCHAR2(128)) AS OWNER,     CAST(V.TABLE_NAME       AS  VARCHAR2(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  VARCHAR2(128)) AS PARTITION_NAME,     CAST(V.PARTITION_POSITION AS    NUMBER) AS PARTITION_POSITION,     CAST(V.SUBPARTITION_NAME  AS    VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(V.SUBPARTITION_POSITION AS NUMBER) AS SUBPARTITION_POSITION,     CAST(V.OBJECT_TYPE AS   VARCHAR2(12)) AS OBJECT_TYPE,     CAST(STAT.ROW_CNT AS    NUMBER) AS NUM_ROWS,     CAST(NULL AS    NUMBER) AS BLOCKS,     CAST(NULL AS    NUMBER) AS EMPTY_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_SPACE,     CAST(NULL AS    NUMBER) AS CHAIN_CNT,     CAST(STAT.AVG_ROW_LEN AS    NUMBER) AS AVG_ROW_LEN,     CAST(NULL AS    NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,     CAST(NULL AS    NUMBER) AS NUM_FREELIST_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHED_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHE_HIT_RATIO,     CAST(NULL AS    NUMBER) AS IM_IMCU_COUNT,     CAST(NULL AS    NUMBER) AS IM_BLOCK_COUNT,     CAST(NULL AS    TIMESTAMP(9)) AS IM_STAT_UPDATE_TIME,     CAST(NULL AS    NUMBER) AS SCAN_RATE,     CAST(STAT.SPARE1 AS    NUMBER) AS SAMPLE_SIZE,     CAST(STAT.LAST_ANALYZED AS  DATE) AS LAST_ANALYZED,     CAST(decode(STAT.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,     CAST(decode(STAT.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,     CAST(decode(bitand(STAT.STATTYPE_LOCKED, 15), NULL, NULL, 0, NULL, 1, 'DATA', 2, 'CACHE', 'ALL') AS    VARCHAR2(5)) AS STATTYPE_LOCKED,     CAST(decode(STAT.STALE_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS STALE_STATS,     CAST(NULL AS    VARCHAR2(7)) AS SCOPE     FROM     (         (SELECT TENANT_ID,                 DATABASE_ID,                 TABLE_ID,                 TABLE_ID AS PARTITION_ID,                 TABLE_NAME,                 NULL AS PARTITION_NAME,                 NULL AS SUBPARTITION_NAME,                 NULL AS PARTITION_POSITION,                 NULL AS SUBPARTITION_POSITION,                'TABLE' AS OBJECT_TYPE           FROM              SYS.ALL_VIRTUAL_CORE_ALL_TABLE           WHERE TABLE_TYPE IN (0,2,3,8,9,14,15)         UNION ALL         SELECT TENANT_ID,                DATABASE_ID,                TABLE_ID,                CASE WHEN PART_LEVEL = 0 THEN TABLE_ID ELSE -1 END AS PARTITION_ID,                TABLE_NAME,                NULL AS PARTITION_NAME,                NULL AS SUBPARTITION_NAME,                NULL AS PARTITION_POSITION,                NULL AS SUBPARTITION_POSITION,                'TABLE' AS OBJECT_TYPE         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)         AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1))     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 P.PART_ID,                 T.TABLE_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T           JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)             AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                T.TABLE_NAME,                  P.PART_NAME,                  SP.SUB_PART_NAME,                  P.PART_IDX + 1,                  SP.SUB_PART_IDX + 1,                  'SUBPARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID             AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)         JOIN             SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)     ) V     JOIN         SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db         ON db.tenant_id = V.tenant_id         AND db.database_id = V.database_id         AND V.TENANT_ID = EFFECTIVE_TENANT_ID()         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()     LEFT JOIN         SYS.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND V.PARTITION_ID = STAT.PARTITION_ID )__"))) {
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

int ObInnerTableSchema::user_tab_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TAB_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TAB_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(V.TABLE_NAME       AS  VARCHAR2(128)) AS TABLE_NAME,     CAST(V.PARTITION_NAME   AS  VARCHAR2(128)) AS PARTITION_NAME,     CAST(V.PARTITION_POSITION AS    NUMBER) AS PARTITION_POSITION,     CAST(V.SUBPARTITION_NAME  AS    VARCHAR2(128)) AS SUBPARTITION_NAME,     CAST(V.SUBPARTITION_POSITION AS NUMBER) AS SUBPARTITION_POSITION,     CAST(V.OBJECT_TYPE AS   VARCHAR2(12)) AS OBJECT_TYPE,     CAST(STAT.ROW_CNT AS    NUMBER) AS NUM_ROWS,     CAST(NULL AS    NUMBER) AS BLOCKS,     CAST(NULL AS    NUMBER) AS EMPTY_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_SPACE,     CAST(NULL AS    NUMBER) AS CHAIN_CNT,     CAST(STAT.AVG_ROW_LEN AS    NUMBER) AS AVG_ROW_LEN,     CAST(NULL AS    NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,     CAST(NULL AS    NUMBER) AS NUM_FREELIST_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHED_BLOCKS,     CAST(NULL AS    NUMBER) AS AVG_CACHE_HIT_RATIO,     CAST(NULL AS    NUMBER) AS IM_IMCU_COUNT,     CAST(NULL AS    NUMBER) AS IM_BLOCK_COUNT,     CAST(NULL AS    TIMESTAMP(9)) AS IM_STAT_UPDATE_TIME,     CAST(NULL AS    NUMBER) AS SCAN_RATE,     CAST(STAT.SPARE1 AS    NUMBER) AS SAMPLE_SIZE,     CAST(STAT.LAST_ANALYZED AS  DATE) AS LAST_ANALYZED,     CAST(decode(STAT.GLOBAL_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS GLOBAL_STATS,     CAST(decode(STAT.USER_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS USER_STATS,     CAST(decode(bitand(STAT.STATTYPE_LOCKED, 15), NULL, NULL, 0, NULL, 1, 'DATA', 2, 'CACHE', 'ALL') AS    VARCHAR2(5)) AS STATTYPE_LOCKED,     CAST(decode(STAT.STALE_STATS, 0, 'NO', 1, 'YES', NULL) AS    VARCHAR2(3)) AS STALE_STATS,     CAST(NULL AS    VARCHAR2(7)) AS SCOPE     FROM     (         SELECT TENANT_ID,                 DATABASE_ID,                 TABLE_ID,                 CASE WHEN PART_LEVEL = 0 THEN TABLE_ID ELSE -1 END AS PARTITION_ID,                 TABLE_NAME,                 NULL AS PARTITION_NAME,                 NULL AS SUBPARTITION_NAME,                 NULL AS PARTITION_POSITION,                 NULL AS SUBPARTITION_POSITION,                 'TABLE' AS OBJECT_TYPE         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)             AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)             AND t.database_id = USERENV('SCHEMAID')     UNION ALL         SELECT T.TENANT_ID,                 T.DATABASE_ID,                 T.TABLE_ID,                 P.PART_ID,                 T.TABLE_NAME,                 P.PART_NAME,                 NULL,                 P.PART_IDX + 1,                 NULL,                 'PARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T           JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)             AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)             AND t.database_id = USERENV('SCHEMAID')     UNION ALL         SELECT T.TENANT_ID,                T.DATABASE_ID,                T.TABLE_ID,                SP.SUB_PART_ID AS PARTITION_ID,                T.TABLE_NAME,                  P.PART_NAME,                  SP.SUB_PART_NAME,                  P.PART_IDX + 1,                  SP.SUB_PART_IDX + 1,                  'SUBPARTITION'         FROM             SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T         JOIN             SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TENANT_ID = P.TENANT_ID             AND T.TABLE_ID = P.TABLE_ID             AND bitand((T.TABLE_MODE / 4096), 15) IN (0,1)         JOIN             SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SP             ON T.TENANT_ID = SP.TENANT_ID             AND T.TABLE_ID = SP.TABLE_ID             AND P.PART_ID = SP.PART_ID         WHERE T.TABLE_TYPE IN (0,2,3,8,9,14,15)             AND t.database_id = USERENV('SCHEMAID')     ) V     LEFT JOIN         SYS.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT STAT         ON V.TENANT_ID = STAT.TENANT_ID         AND V.TABLE_ID = STAT.TABLE_ID         AND V.PARTITION_ID = STAT.PARTITION_ID )__"))) {
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

int ObInnerTableSchema::dba_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     JOB,     lowner LOG_USER,     powner PRIV_USER,     cowner SCHEMA_USER,     CAST(LAST_DATE AS DATE) LAST_DATE,     substr(to_char(last_date, 'HH24:MI:SS'), 1, 8) LAST_SEC,     CAST(THIS_DATE AS DATE) THIS_DATE,     substr(to_char(this_date, 'HH24:MI:SS'), 1, 8) THIS_SEC,     CAST(NEXT_DATE AS DATE) NEXT_DATE,     substr(to_char(next_date, 'HH24:MI:SS'), 1, 8) NEXT_SEC,     (total/1000/1000) + (extract(day from (sysdate - nvl(this_date, sysdate))) * 86400) TOTAL_TIME,     decode(mod(FLAG, 2), 1, 'Y', 0, 'N', '?') BROKEN,     INTERVAL# interval,     FAILURES,     WHAT,     nlsenv NLS_ENV,     CAST(NULL AS RAW(32)) MISC_ENV,     field1 INSTANCE   FROM SYS.ALL_VIRTUAL_JOB_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::user_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     j."JOB",     j."LOG_USER",     j."PRIV_USER",     j."SCHEMA_USER",     j."LAST_DATE",     j."LAST_SEC",     j."THIS_DATE",     j."THIS_SEC",     j."NEXT_DATE",     j."NEXT_SEC",     j."TOTAL_TIME",     j."BROKEN",     j."INTERVAL",     j."FAILURES",     j."WHAT",     j."NLS_ENV",     j."MISC_ENV",     j."INSTANCE"   FROM     SYS.DBA_JOBS j   WHERE j.priv_user = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::dba_jobs_running_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_JOBS_RUNNING_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_JOBS_RUNNING_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(NULL AS NUMBER) SID,     CAST(j.JOB AS NUMBER) JOB,     CAST(j.FAILURES AS NUMBER) FAILURES,     CAST(LAST_DATE AS DATE) LAST_DATE,     CAST(substr(to_char(last_date,'HH24:MI:SS'),1,8) AS VARCHAR2(32)) LAST_SEC,     CAST(THIS_DATE AS DATE) THIS_DATE,     CAST(substr(to_char(this_date,'HH24:MI:SS'),1,8) AS VARCHAR2(32)) THIS_SEC,     CAST(j.field1 AS VARCHAR2(128)) INSTANCE   from     SYS.ALL_VIRTUAL_JOB_REAL_AGENT j   WHERE THIS_DATE IS NOT NULL )__"))) {
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

int ObInnerTableSchema::all_directories_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DIRECTORIES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DIRECTORIES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST('SYS' AS VARCHAR2(128)) AS OWNER,       t.DIRECTORY_NAME AS DIRECTORY_NAME,       t.DIRECTORY_PATH AS DIRECTORY_PATH,       t.TENANT_ID AS ORIGIN_CON_ID     FROM       SYS.ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT t       WHERE t.TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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
