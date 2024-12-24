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

int ObInnerTableSchema::gv_ob_cgroup_config_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_CGROUP_CONFIG_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_CGROUP_CONFIG_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT svr_ip AS SVR_IP,        svr_port AS SVR_PORT,        cfs_quota_us AS CFS_QUOTA_US,        cfs_period_us AS CFS_PERIOD_US,        shares AS SHARES,        cgroup_path AS CGROUP_PATH FROM SYS.ALL_VIRTUAL_CGROUP_CONFIG )__"))) {
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

int ObInnerTableSchema::v_ob_cgroup_config_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_CGROUP_CONFIG_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_CGROUP_CONFIG_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP,   SVR_PORT,   CFS_QUOTA_US,   CFS_PERIOD_US,   SHARES,   CGROUP_PATH FROM SYS.GV$OB_CGROUP_CONFIG WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_sqlstat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SQLSTAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SQLSTAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       CAST(SVR_IP AS VARCHAR2(46)) AS SVR_IP,       CAST(SVR_PORT AS NUMBER) AS SVR_PORT,       CAST(TENANT_ID AS NUMBER) AS TENANT_ID,       CAST(SQL_ID AS VARCHAR(32)) AS SQL_ID,       CAST(PLAN_ID AS NUMBER) AS PLAN_ID,       CAST(PLAN_HASH AS NUMBER) AS PLAN_HASH,       CAST(PLAN_TYPE AS NUMBER) AS PLAN_TYPE,       TO_CLOB(QUERY_SQL) AS QUERY_SQL,       CAST(MODULE AS VARCHAR(64)) AS MODULE,       CAST(ACTION AS VARCHAR(64)) AS ACTION,       CAST(PARSING_DB_ID AS NUMBER) AS PARSING_DB_ID,       CAST(PARSING_DB_NAME AS VARCHAR(128)) AS PARSING_DB_NAME,       CAST(PARSING_USER_ID AS NUMBER) AS PARSING_USER_ID,       CAST(EXECUTIONS_TOTAL AS NUMBER) AS EXECUTIONS_TOTAL,       CAST(EXECUTIONS_DELTA AS NUMBER) AS EXECUTIONS_DELTA,       CAST(DISK_READS_TOTAL AS NUMBER) AS DISK_READS_TOTAL,       CAST(DISK_READS_DELTA AS NUMBER) AS DISK_READS_DELTA,       CAST(BUFFER_GETS_TOTAL AS NUMBER) AS BUFFER_GETS_TOTAL,       CAST(BUFFER_GETS_DELTA AS NUMBER) AS BUFFER_GETS_DELTA,       CAST(ELAPSED_TIME_TOTAL AS NUMBER) AS ELAPSED_TIME_TOTAL,       CAST(ELAPSED_TIME_DELTA AS NUMBER) AS ELAPSED_TIME_DELTA,       CAST(CPU_TIME_TOTAL AS NUMBER) AS CPU_TIME_TOTAL,       CAST(CPU_TIME_DELTA AS NUMBER) AS CPU_TIME_DELTA,       CAST(CCWAIT_TOTAL AS NUMBER) AS CCWAIT_TOTAL,       CAST(CCWAIT_DELTA AS NUMBER) AS CCWAIT_DELTA,       CAST(USERIO_WAIT_TOTAL AS NUMBER) AS USERIO_WAIT_TOTAL,       CAST(USERIO_WAIT_DELTA AS NUMBER) AS USERIO_WAIT_DELTA,       CAST(APWAIT_TOTAL AS NUMBER) AS APWAIT_TOTAL,       CAST(APWAIT_DELTA AS NUMBER) AS APWAIT_DELTA,       CAST(PHYSICAL_READ_REQUESTS_TOTAL AS NUMBER) AS PHYSICAL_READ_REQUESTS_TOTAL,       CAST(PHYSICAL_READ_REQUESTS_DELTA AS NUMBER) AS PHYSICAL_READ_REQUESTS_DELTA,       CAST(PHYSICAL_READ_BYTES_TOTAL AS NUMBER) AS PHYSICAL_READ_BYTES_TOTAL,       CAST(PHYSICAL_READ_BYTES_DELTA AS NUMBER) AS PHYSICAL_READ_BYTES_DELTA,       CAST(WRITE_THROTTLE_TOTAL AS NUMBER) AS WRITE_THROTTLE_TOTAL,       CAST(WRITE_THROTTLE_DELTA AS NUMBER) AS WRITE_THROTTLE_DELTA,       CAST(ROWS_PROCESSED_TOTAL AS NUMBER) AS ROWS_PROCESSED_TOTAL,       CAST(ROWS_PROCESSED_DELTA AS NUMBER) AS ROWS_PROCESSED_DELTA,       CAST(MEMSTORE_READ_ROWS_TOTAL AS NUMBER) AS MEMSTORE_READ_ROWS_TOTAL,       CAST(MEMSTORE_READ_ROWS_DELTA AS NUMBER) AS MEMSTORE_READ_ROWS_DELTA,       CAST(MINOR_SSSTORE_READ_ROWS_TOTAL AS NUMBER) AS MINOR_SSSTORE_READ_ROWS_TOTAL,       CAST(MINOR_SSSTORE_READ_ROWS_DELTA AS NUMBER) AS MINOR_SSSTORE_READ_ROWS_DELTA,       CAST(MAJOR_SSSTORE_READ_ROWS_TOTAL AS NUMBER) AS MAJOR_SSSTORE_READ_ROWS_TOTAL,       CAST(MAJOR_SSSTORE_READ_ROWS_DELTA AS NUMBER) AS MAJOR_SSSTORE_READ_ROWS_DELTA,       CAST(RPC_TOTAL AS NUMBER) AS RPC_TOTAL,       CAST(RPC_DELTA AS NUMBER) AS RPC_DELTA,       CAST(FETCHES_TOTAL AS NUMBER) AS FETCHES_TOTAL,       CAST(FETCHES_DELTA AS NUMBER) AS FETCHES_DELTA,       CAST(RETRY_TOTAL AS NUMBER) AS RETRY_TOTAL,       CAST(RETRY_DELTA AS NUMBER) AS RETRY_DELTA,       CAST(PARTITION_TOTAL AS NUMBER) AS PARTITION_TOTAL,       CAST(PARTITION_DELTA AS NUMBER) AS PARTITION_DELTA,       CAST(NESTED_SQL_TOTAL AS NUMBER) AS NESTED_SQL_TOTAL,       CAST(NESTED_SQL_DELTA AS NUMBER) AS NESTED_SQL_DELTA,       CAST(SOURCE_IP AS CHAR(46)) AS SOURCE_IP,       CAST(SOURCE_PORT AS NUMBER) AS SOURCE_PORT,       CAST(ROUTE_MISS_TOTAL AS NUMBER) AS ROUTE_MISS_TOTAL,       CAST(ROUTE_MISS_DELTA AS NUMBER) AS ROUTE_MISS_DELTA,       CAST(FIRST_LOAD_TIME AS TIMESTAMP(6)) AS FIRST_LOAD_TIME,       CAST(PLAN_CACHE_HIT_TOTAL AS NUMBER) AS PLAN_CACHE_HIT_TOTAL,       CAST(PLAN_CACHE_HIT_DELTA AS NUMBER) AS PLAN_CACHE_HIT_DELTA     FROM SYS.ALL_VIRTUAL_SQLSTAT )__"))) {
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

int ObInnerTableSchema::v_ob_sqlstat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SQLSTAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SQLSTAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT SVR_IP, SVR_PORT, TENANT_ID, SQL_ID, PLAN_ID, PLAN_HASH, PLAN_TYPE, QUERY_SQL, MODULE, ACTION, PARSING_DB_ID, PARSING_DB_NAME, PARSING_USER_ID, EXECUTIONS_TOTAL, EXECUTIONS_DELTA, DISK_READS_TOTAL, DISK_READS_DELTA, BUFFER_GETS_TOTAL, BUFFER_GETS_DELTA, ELAPSED_TIME_TOTAL, ELAPSED_TIME_DELTA, CPU_TIME_TOTAL, CPU_TIME_DELTA, CCWAIT_TOTAL, CCWAIT_DELTA, USERIO_WAIT_TOTAL, USERIO_WAIT_DELTA, APWAIT_TOTAL, APWAIT_DELTA, PHYSICAL_READ_REQUESTS_TOTAL, PHYSICAL_READ_REQUESTS_DELTA, PHYSICAL_READ_BYTES_TOTAL, PHYSICAL_READ_BYTES_DELTA, WRITE_THROTTLE_TOTAL, WRITE_THROTTLE_DELTA, ROWS_PROCESSED_TOTAL, ROWS_PROCESSED_DELTA, MEMSTORE_READ_ROWS_TOTAL, MEMSTORE_READ_ROWS_DELTA, MINOR_SSSTORE_READ_ROWS_TOTAL, MINOR_SSSTORE_READ_ROWS_DELTA, MAJOR_SSSTORE_READ_ROWS_TOTAL, MAJOR_SSSTORE_READ_ROWS_DELTA, RPC_TOTAL, RPC_DELTA, FETCHES_TOTAL, FETCHES_DELTA, RETRY_TOTAL, RETRY_DELTA, PARTITION_TOTAL, PARTITION_DELTA, NESTED_SQL_TOTAL, NESTED_SQL_DELTA, SOURCE_IP, SOURCE_PORT, ROUTE_MISS_TOTAL, ROUTE_MISS_DELTA, FIRST_LOAD_TIME, PLAN_CACHE_HIT_TOTAL, PLAN_CACHE_HIT_DELTA FROM SYS.GV$OB_SQLSTAT WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_sess_time_model_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SESS_TIME_MODEL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SESS_TIME_MODEL_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SID,     CAST(GV$SESSTAT.CON_ID AS NUMBER) AS TENANT_ID,     SVR_IP,     SVR_PORT,     STAT_ID,     CAST(NAME AS VARCHAR2(64)) AS STAT_NAME,     VALUE   FROM     SYS.GV$SESSTAT   left join     SYS.v$statname   on SYS.GV$SESSTAT.statistic#=SYS.v$statname.statistic#   WHERE     STAT_ID in (200001, 200002, 200010, 200011, 200005, 200006);   )__"))) {
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

int ObInnerTableSchema::v_ob_sess_time_model_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SESS_TIME_MODEL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SESS_TIME_MODEL_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SID,     TENANT_ID,     SVR_IP,     SVR_PORT,     STAT_ID,     STAT_NAME,     VALUE   FROM     SYS.GV$OB_SESS_TIME_MODEL   WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT();   )__"))) {
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

int ObInnerTableSchema::gv_ob_sys_time_model_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SYS_TIME_MODEL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SYS_TIME_MODEL_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     CON_ID AS TENANT_ID,     SVR_IP,     SVR_PORT,     STAT_ID,     CAST(NAME AS VARCHAR2(64)) AS STAT_NAME,     VALUE   FROM     SYS.GV$SYSSTAT   WHERE     STAT_ID in (200001, 200002, 200010, 200011, 200005, 200006);   )__"))) {
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

int ObInnerTableSchema::v_ob_sys_time_model_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SYS_TIME_MODEL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SYS_TIME_MODEL_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     TENANT_ID,     SVR_IP,     SVR_PORT,     STAT_ID,     STAT_NAME,     VALUE   FROM     SYS.GV$OB_SYS_TIME_MODEL   WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT();   )__"))) {
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

int ObInnerTableSchema::v_statname_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_STATNAME_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_STATNAME_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   select CAST(TENANT_ID AS NUMBER) AS CON_ID,          CAST(STAT_ID AS NUMBER) as STAT_ID,          CAST("STATISTIC#" AS NUMBER) as "STATISTIC#",          CAST(NAME AS VARCHAR2(64)) AS NAME,          CAST(DISPLAY_NAME AS VARCHAR2(64)) AS DISPLAY_NAME,          CAST(CLASS AS NUMBER) AS CLASS   from SYS.TENANT_VIRTUAL_STATNAME   )__"))) {
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

int ObInnerTableSchema::dba_ob_aux_statistics_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_AUX_STATISTICS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_AUX_STATISTICS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   	SELECT       LAST_ANALYZED,       CPU_SPEED AS "CPU_SPEED(MHZ)",       DISK_SEQ_READ_SPEED AS "DISK_SEQ_READ_SPEED(MB/S)",       DISK_RND_READ_SPEED AS "DISK_RND_READ_SPEED(MB/S)",       NETWORK_SPEED AS "NETWORK_SPEED(MB/S)"     FROM SYS.ALL_VIRTUAL_AUX_STAT_REAL_AGENT     WHERE TENANT_ID = EFFECTIVE_TENANT_ID(); )__"))) {
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

int ObInnerTableSchema::dba_ob_sys_variables_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SYS_VARIABLES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SYS_VARIABLES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     A.GMT_CREATE AS CREATE_TIME,     A.GMT_MODIFIED AS MODIFY_TIME,     A.NAME as NAME,     A.VALUE as VALUE,     A.MIN_VAL as MIN_VALUE,     A.MAX_VAL as MAX_VALUE,     CASE BITAND(A.FLAGS,3)         WHEN 1 THEN 'GLOBAL_ONLY'         WHEN 2 THEN 'SESSION_ONLY'         WHEN 3 THEN 'GLOBAL | SESSION'         ELSE NULL     END as SCOPE,     A.INFO as INFO,     B.DEFAULT_VALUE as DEFAULT_VALUE,     CAST (CASE WHEN A.VALUE = B.DEFAULT_VALUE           THEN 'YES'           ELSE 'NO'           END AS VARCHAR2(3)) AS ISDEFAULT   FROM SYS.ALL_VIRTUAL_SYS_VARIABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE B   WHERE A.NAME = B.VARIABLE_NAME;   )__"))) {
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

int ObInnerTableSchema::gv_ob_active_session_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_ACTIVE_SESSION_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_ACTIVE_SESSION_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       CAST(SVR_IP AS VARCHAR2(46)) AS SVR_IP,       CAST(SVR_PORT AS NUMBER) AS SVR_PORT,       CAST(SAMPLE_ID AS NUMBER) AS SAMPLE_ID,       SAMPLE_TIME AS SAMPLE_TIME,       CAST(TENANT_ID AS NUMBER) AS CON_ID,       CAST(USER_ID AS NUMBER) AS USER_ID,       CAST(SESSION_ID AS NUMBER) AS SESSION_ID,       CAST(DECODE(SESSION_TYPE, 0, 'FOREGROUND', 'BACKGROUND') AS VARCHAR2(10)) AS SESSION_TYPE,       CAST(DECODE(EVENT_NO, 0, 'ON CPU', 'WAITING') AS VARCHAR2(7)) AS SESSION_STATE,       CAST(SQL_ID AS VARCHAR(32)) AS SQL_ID,       CAST(PLAN_ID AS NUMBER) AS PLAN_ID,       CAST(TRACE_ID AS VARCHAR(64)) AS TRACE_ID,       CAST(NAME AS VARCHAR2(64)) AS EVENT,       CAST(EVENT_NO AS NUMBER) AS EVENT_NO,       CAST(SYS.ALL_VIRTUAL_ASH.EVENT_ID AS NUMBER) AS EVENT_ID,       CAST(PARAMETER1 AS VARCHAR2(64)) AS P1TEXT,       CAST(P1 AS NUMBER) AS P1,       CAST(PARAMETER2 AS VARCHAR2(64)) AS P2TEXT,       CAST(P2 AS NUMBER) AS P2,       CAST(PARAMETER3 AS VARCHAR2(64)) AS P3TEXT,       CAST(P3 AS NUMBER) AS P3,       CAST(WAIT_CLASS AS VARCHAR2(64)) AS WAIT_CLASS,       CAST(WAIT_CLASS_ID AS NUMBER) AS WAIT_CLASS_ID,       CAST(TIME_WAITED AS NUMBER) AS TIME_WAITED,       CAST(SQL_PLAN_LINE_ID AS NUMBER) SQL_PLAN_LINE_ID,       CAST(GROUP_ID AS NUMBER) GROUP_ID,       CAST(PLAN_HASH AS NUMBER) PLAN_HASH,       CAST(THREAD_ID AS NUMBER) THREAD_ID,       CAST(STMT_TYPE AS NUMBER) STMT_TYPE,       CAST(TIME_MODEL AS NUMBER) TIME_MODEL,       CAST(DECODE(IN_PARSE, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_PARSE,       CAST(DECODE(IN_PL_PARSE, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_PL_PARSE,       CAST(DECODE(IN_PLAN_CACHE, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_PLAN_CACHE,       CAST(DECODE(IN_SQL_OPTIMIZE, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_SQL_OPTIMIZE,       CAST(DECODE(IN_SQL_EXECUTION, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_SQL_EXECUTION,       CAST(DECODE(IN_PX_EXECUTION, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_PX_EXECUTION,       CAST(DECODE(IN_SEQUENCE_LOAD, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_SEQUENCE_LOAD,       CAST(DECODE(IN_COMMITTING, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_COMMITTING,       CAST(DECODE(IN_STORAGE_READ, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_STORAGE_READ,       CAST(DECODE(IN_STORAGE_WRITE, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_STORAGE_WRITE,       CAST(DECODE(IN_REMOTE_DAS_EXECUTION, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_REMOTE_DAS_EXECUTION,       CAST(DECODE(IN_FILTER_ROWS, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_FILTER_ROWS,       CAST(CASE WHEN BITAND(TIME_MODEL , 16384) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_RPC_ENCODE,       CAST(CASE WHEN BITAND(TIME_MODEL , 32768) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_RPC_DECODE,       CAST(CASE WHEN BITAND(TIME_MODEL , 65536) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_CONNECTION_MGR,       CAST(PROGRAM AS VARCHAR2(64)) AS PROGRAM,       CAST(MODULE AS VARCHAR2(64)) AS MODULE,       CAST(ACTION AS VARCHAR2(64)) AS ACTION,       CAST(CLIENT_ID AS VARCHAR2(64)) AS CLIENT_ID,       CAST(BACKTRACE AS VARCHAR2(512)) AS BACKTRACE,       CAST(TM_DELTA_TIME AS NUMBER) AS TM_DELTA_TIME,       CAST(TM_DELTA_CPU_TIME AS NUMBER) AS TM_DELTA_CPU_TIME,       CAST(TM_DELTA_DB_TIME AS NUMBER) AS TM_DELTA_DB_TIME,       CAST(TOP_LEVEL_SQL_ID AS CHAR(32)) AS TOP_LEVEL_SQL_ID,       CAST(DECODE(IN_PLSQL_COMPILATION, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_PLSQL_COMPILATION,       CAST(DECODE(IN_PLSQL_EXECUTION, 1, 'Y', 'N') AS VARCHAR2(1)) AS IN_PLSQL_EXECUTION,       CAST(PLSQL_ENTRY_OBJECT_ID AS NUMBER) AS PLSQL_ENTRY_OBJECT_ID,       CAST(PLSQL_ENTRY_SUBPROGRAM_ID AS NUMBER) AS PLSQL_ENTRY_SUBPROGRAM_ID,       CAST(PLSQL_ENTRY_SUBPROGRAM_NAME AS VARCHAR2(32)) AS PLSQL_ENTRY_SUBPROGRAM_NAME,       CAST(PLSQL_OBJECT_ID AS NUMBER) AS PLSQL_OBJECT_ID,       CAST(PLSQL_SUBPROGRAM_ID AS NUMBER) AS PLSQL_SUBPROGRAM_ID,       CAST(PLSQL_SUBPROGRAM_NAME AS VARCHAR2(32)) AS PLSQL_SUBPROGRAM_NAME,       CAST(TX_ID AS NUMBER) AS TX_ID,       CAST(BLOCKING_SESSION_ID AS NUMBER) AS BLOCKING_SESSION_ID,       CAST(TABLET_ID AS NUMBER) AS TABLET_ID,       CAST(PROXY_SID AS NUMBER) AS PROXY_SID     FROM SYS.ALL_VIRTUAL_ASH LEFT JOIN SYS.V$EVENT_NAME on EVENT_NO = "EVENT#" )__"))) {
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

int ObInnerTableSchema::v_ob_active_session_history_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_ACTIVE_SESSION_HISTORY_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_ACTIVE_SESSION_HISTORY_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT SVR_IP, SVR_PORT, SAMPLE_ID, SAMPLE_TIME, CON_ID, USER_ID, SESSION_ID, SESSION_TYPE, SESSION_STATE, SQL_ID, PLAN_ID, TRACE_ID, EVENT, EVENT_NO, EVENT_ID, P1TEXT, P1, P2TEXT, P2, P3TEXT, P3, WAIT_CLASS, WAIT_CLASS_ID, TIME_WAITED, SQL_PLAN_LINE_ID, GROUP_ID, PLAN_HASH, THREAD_ID, STMT_TYPE, TIME_MODEL, IN_PARSE, IN_PL_PARSE, IN_PLAN_CACHE, IN_SQL_OPTIMIZE, IN_SQL_EXECUTION, IN_PX_EXECUTION, IN_SEQUENCE_LOAD, IN_COMMITTING, IN_STORAGE_READ, IN_STORAGE_WRITE, IN_REMOTE_DAS_EXECUTION, IN_FILTER_ROWS, IN_RPC_ENCODE, IN_RPC_DECODE, IN_CONNECTION_MGR, PROGRAM, MODULE, ACTION, CLIENT_ID, BACKTRACE, TM_DELTA_TIME, TM_DELTA_CPU_TIME, TM_DELTA_DB_TIME, TOP_LEVEL_SQL_ID, IN_PLSQL_COMPILATION, IN_PLSQL_EXECUTION, PLSQL_ENTRY_OBJECT_ID, PLSQL_ENTRY_SUBPROGRAM_ID, PLSQL_ENTRY_SUBPROGRAM_NAME, PLSQL_OBJECT_ID, PLSQL_SUBPROGRAM_ID, PLSQL_SUBPROGRAM_NAME, TX_ID, BLOCKING_SESSION_ID, TABLET_ID, PROXY_SID FROM SYS.GV$OB_ACTIVE_SESSION_HISTORY WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::dba_index_usage_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_INDEX_USAGE_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_INDEX_USAGE_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(IUT.OBJECT_ID AS NUMBER) AS OBJECT_ID,       CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,       CAST(DB.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,       CAST(IUT.TOTAL_ACCESS_COUNT AS NUMBER) AS TOTAL_ACCESS_COUNT,       CAST(IUT.TOTAL_EXEC_COUNT AS NUMBER) AS TOTAL_EXEC_COUNT,       CAST(IUT.TOTAL_ROWS_RETURNED AS NUMBER) AS TOTAL_ROWS_RETURNED,       CAST(IUT.BUCKET_0_ACCESS_COUNT AS NUMBER) AS BUCKET_0_ACCESS_COUNT,       CAST(IUT.BUCKET_1_ACCESS_COUNT AS NUMBER) AS BUCKET_1_ACCESS_COUNT,       CAST(IUT.BUCKET_2_10_ACCESS_COUNT AS NUMBER) AS BUCKET_2_10_ACCESS_COUNT,       CAST(IUT.BUCKET_2_10_ROWS_RETURNED AS NUMBER) AS BUCKET_2_10_ROWS_RETURNED,       CAST(IUT.BUCKET_11_100_ACCESS_COUNT AS NUMBER) AS BUCKET_11_100_ACCESS_COUNT,       CAST(IUT.BUCKET_11_100_ROWS_RETURNED AS NUMBER) AS BUCKET_11_100_ROWS_RETURNED,       CAST(IUT.BUCKET_101_1000_ACCESS_COUNT AS NUMBER) AS BUCKET_101_1000_ACCESS_COUNT,       CAST(IUT.BUCKET_101_1000_ROWS_RETURNED AS NUMBER) AS BUCKET_101_1000_ROWS_RETURNED,       CAST(IUT.BUCKET_1000_PLUS_ACCESS_COUNT AS NUMBER) AS BUCKET_1000_PLUS_ACCESS_COUNT,       CAST(IUT.BUCKET_1000_PLUS_ROWS_RETURNED AS NUMBER) AS BUCKET_1000_PLUS_ROWS_RETURNED,       CAST(IUT.LAST_USED AS VARCHAR2(128)) AS LAST_USED     FROM       SYS.ALL_VIRTUAL_INDEX_USAGE_INFO_REAL_AGENT IUT       JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON IUT.OBJECT_ID = T.TABLE_ID       JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB ON T.DATABASE_ID = DB.DATABASE_ID     WHERE T.TABLE_ID = IUT.OBJECT_ID )__"))) {
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

int ObInnerTableSchema::gv_ob_ls_snapshots_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_LS_SNAPSHOTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_LS_SNAPSHOTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       tenant_id AS TENANT_ID,       snapshot_id AS SNAPSHOT_ID,       ls_id AS LS_ID,       svr_ip AS SVR_IP,       svr_port AS SVR_PORT,       (CASE         WHEN meta_existed = 1 THEN 'YES'         ELSE 'NO'         END) AS META_EXISTED,       (CASE         WHEN build_status = 0 THEN 'BUILDING'         WHEN build_status = 1 THEN 'FAILED'         WHEN build_status = 2 THEN 'SUCCESSFUL'         ELSE 'UNKNOWN'         END) AS BUILD_STATUS,       rebuild_seq_start AS REBUILD_SEQ_START,       rebuild_seq_end AS REBUILD_SEQ_END,       end_interval_scn AS END_INTERVAL_SCN,       ls_meta_package AS LS_META_PACKAGE,       (CASE         WHEN tsnap_is_running = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_IS_RUNNING,       (CASE         WHEN tsnap_has_unfinished_create_dag = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_HAS_UNFINISHED_CREATE_DAG,       (CASE         WHEN tsnap_has_unfinished_gc_dag = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_HAS_UNFINISHED_GC_DAG,       tsnap_clone_ref AS TSNAP_CLONE_REF,       (CASE         WHEN tsnap_meta_existed = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_META_EXISTED     FROM SYS.ALL_VIRTUAL_LS_SNAPSHOT )__"))) {
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

int ObInnerTableSchema::v_ob_ls_snapshots_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_LS_SNAPSHOTS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_LS_SNAPSHOTS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT TENANT_ID,       SNAPSHOT_ID,       LS_ID,       SVR_IP,       SVR_PORT,       META_EXISTED,       BUILD_STATUS,       REBUILD_SEQ_START,       REBUILD_SEQ_END,       END_INTERVAL_SCN,       LS_META_PACKAGE,       TSNAP_IS_RUNNING,       TSNAP_HAS_UNFINISHED_CREATE_DAG,       TSNAP_HAS_UNFINISHED_GC_DAG,       TSNAP_CLONE_REF,       TSNAP_META_EXISTED     FROM SYS.GV$OB_LS_SNAPSHOTS     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_shared_storage_quota_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SHARED_STORAGE_QUOTA_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SHARED_STORAGE_QUOTA_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( ( SELECT   A.SVR_IP AS SVR_IP,   A.SVR_PORT AS SVR_PORT,   NVL(B.ENDPOINT, '') AS ENDPOINT,   NVL(B.PATH, 'local://') AS PATH,   A.CLASS_ID AS CLASS_ID,   A.TYPE AS TYPE,   A.REQUIREMENT AS REQUIREMENT,   A.ASSIGN AS ASSIGN FROM   SYS.ALL_VIRTUAL_SHARED_STORAGE_QUOTA A JOIN   (SELECT dest_id, path, endpoint FROM SYS.ALL_VIRTUAL_BACKUP_STORAGE_INFO GROUP BY dest_id, path, endpoint) B ON   A.STORAGE_ID = B.DEST_ID WHERE   A.MODULE = 'BACKUP/ARCHIVE/RESTORE' ) UNION ( SELECT   C.SVR_IP AS SVR_IP,   C.SVR_PORT AS SVR_PORT,   NVL(D.ENDPOINT, '') AS ENDPOINT,   NVL(D.PATH, 'local://') AS PATH,   C.CLASS_ID AS CLASS_ID,   C.TYPE AS TYPE,   C.REQUIREMENT AS REQUIREMENT,   C.ASSIGN AS ASSIGN FROM   SYS.ALL_VIRTUAL_SHARED_STORAGE_QUOTA C JOIN   (SELECT storage_id, path, endpoint FROM SYS.ALL_VIRTUAL_ZONE_STORAGE_SYS_AGENT GROUP BY storage_id, path, endpoint) D ON   C.STORAGE_ID = D.STORAGE_ID WHERE   C.MODULE = 'CLOG/DATA' ) )__"))) {
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

int ObInnerTableSchema::v_ob_shared_storage_quota_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SHARED_STORAGE_QUOTA_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SHARED_STORAGE_QUOTA_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   SVR_IP,   SVR_PORT,   ENDPOINT,   PATH,   CLASS_ID,   TYPE,   REQUIREMENT,   ASSIGN FROM SYS.GV$OB_SHARED_STORAGE_QUOTA WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_session_ps_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SESSION_PS_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SESSION_PS_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT    SVR_IP,    SVR_PORT,    TENANT_ID,    PROXY_SESSION_ID,    SESSION_ID,    PS_CLIENT_STMT_ID,    PS_INNER_STMT_ID,    STMT_TYPE,   PARAM_COUNT,    PARAM_TYPES,    REF_COUNT,    CHECKSUM  FROM    SYS.ALL_VIRTUAL_SESSION_PS_INFO; )__"))) {
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

int ObInnerTableSchema::v_ob_session_ps_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SESSION_PS_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SESSION_PS_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT      SVR_IP,      SVR_PORT,      TENANT_ID,      PROXY_SESSION_ID,      SESSION_ID,      PS_CLIENT_STMT_ID,      PS_INNER_STMT_ID,      STMT_TYPE,     PARAM_COUNT,      PARAM_TYPES,      REF_COUNT,      CHECKSUM    FROM SYS.GV$OB_SESSION_PS_INFO   WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_tracepoint_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TRACEPOINT_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TRACEPOINT_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           SVR_IP,           SVR_PORT,           TP_NO,           TP_NAME,           TP_DESCRIBE,           TP_FREQUENCY,           TP_ERROR_CODE,           TP_OCCUR,           TP_MATCH         FROM SYS.ALL_VIRTUAL_TRACEPOINT_INFO )__"))) {
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

int ObInnerTableSchema::v_ob_tracepoint_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TRACEPOINT_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TRACEPOINT_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           SVR_IP,           SVR_PORT,           TP_NO,           TP_NAME,           TP_DESCRIBE,           TP_FREQUENCY,           TP_ERROR_CODE,           TP_OCCUR,           TP_MATCH     FROM SYS.GV$OB_TRACEPOINT_INFO     WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::dba_ob_rsrc_directives_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RSRC_DIRECTIVES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RSRC_DIRECTIVES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       PLAN,       GROUP_OR_SUBPLAN,       COMMENTS,       MGMT_P1,       UTILIZATION_LIMIT,       MIN_IOPS,       MAX_IOPS,       WEIGHT_IOPS,       MAX_NET_BANDWIDTH,       NET_BANDWIDTH_WEIGHT     FROM       SYS.ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT )__"))) {
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

int ObInnerTableSchema::gv_ob_tenant_resource_limit_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TENANT_RESOURCE_LIMIT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TENANT_RESOURCE_LIMIT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT     SVR_IP,     SVR_PORT,     TENANT_ID,     ZONE,     RESOURCE_NAME,     CURRENT_UTILIZATION,     MAX_UTILIZATION,     RESERVED_VALUE,     LIMIT_VALUE,     EFFECTIVE_LIMIT_TYPE FROM     SYS.ALL_VIRTUAL_TENANT_RESOURCE_LIMIT )__"))) {
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

int ObInnerTableSchema::v_ob_tenant_resource_limit_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TENANT_RESOURCE_LIMIT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TENANT_RESOURCE_LIMIT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT     SVR_IP, SVR_PORT, TENANT_ID, ZONE, RESOURCE_NAME, CURRENT_UTILIZATION, MAX_UTILIZATION,     RESERVED_VALUE, LIMIT_VALUE, EFFECTIVE_LIMIT_TYPE  FROM     SYS.GV$OB_TENANT_RESOURCE_LIMIT WHERE     SVR_IP=HOST_IP() AND     SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_tenant_resource_limit_detail_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT     SVR_IP,     SVR_PORT,     TENANT_ID,     RESOURCE_NAME,     LIMIT_TYPE,     LIMIT_VALUE FROM    SYS.ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL )__"))) {
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

int ObInnerTableSchema::v_ob_tenant_resource_limit_detail_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT     SVR_IP, SVR_PORT, TENANT_ID, RESOURCE_NAME, LIMIT_TYPE, LIMIT_VALUE  FROM     SYS.GV$OB_TENANT_RESOURCE_LIMIT_DETAIL WHERE     SVR_IP=HOST_IP() AND     SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::v_ob_group_io_stat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_GROUP_IO_STAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_GROUP_IO_STAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       A.TENANT_ID AS TENANT_ID,       A.SVR_IP AS SVR_IP,       A.SVR_PORT AS SVR_PORT,       A.GROUP_ID AS GROUP_ID,       A.GROUP_NAME AS GROUP_NAME,       A."MODE" AS "MODE",       A.MIN_IOPS AS MIN_IOPS,       A.MAX_IOPS AS MAX_IOPS,       A.REAL_IOPS AS REAL_IOPS,       A.MAX_NET_BANDWIDTH AS MAX_NET_BANDWIDTH,       A.MAX_NET_BANDWIDTH_DISPLAY AS MAX_NET_BANDWIDTH_DISPLAY,       A.REAL_NET_BANDWIDTH AS REAL_NET_BANDWIDTH,       A.REAL_NET_BANDWIDTH_DISPLAY AS REAL_NET_BANDWIDTH_DISPLAY     FROM       SYS.GV$OB_GROUP_IO_STAT A     WHERE       SVR_IP=HOST_IP()       AND       SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_group_io_stat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_GROUP_IO_STAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_GROUP_IO_STAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     A.TENANT_ID AS TENANT_ID,     A.SVR_IP AS SVR_IP,     A.SVR_PORT AS SVR_PORT,     A.GROUP_ID AS GROUP_ID,     A.GROUP_NAME AS GROUP_NAME,     A."MODE" AS "MODE",     A.MIN_IOPS AS MIN_IOPS,     A.MAX_IOPS AS MAX_IOPS,     A.REAL_IOPS AS REAL_IOPS,     A.MAX_NET_BANDWIDTH AS MAX_NET_BANDWIDTH,     A.MAX_NET_BANDWIDTH_DISPLAY AS MAX_NET_BANDWIDTH_DISPLAY,     A.REAL_NET_BANDWIDTH AS REAL_NET_BANDWIDTH,     A.REAL_NET_BANDWIDTH_DISPLAY AS REAL_NET_BANDWIDTH_DISPLAY   FROM     SYS.ALL_VIRTUAL_GROUP_IO_STAT A )__"))) {
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

int ObInnerTableSchema::gv_ob_nic_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_NIC_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_NIC_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SVR_IP,     SVR_PORT,     DEVNAME,     SPEED_MBPS   FROM SYS.ALL_VIRTUAL_NIC_INFO   )__"))) {
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

int ObInnerTableSchema::v_ob_nic_info_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_NIC_INFO_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_NIC_INFO_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SVR_IP,     SVR_PORT,     DEVNAME,     SPEED_MBPS   FROM SYS.GV$OB_NIC_INFO   WHERE SVR_IP = host_ip() AND SVR_PORT = rpc_port()   )__"))) {
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

int ObInnerTableSchema::gv_ob_query_response_time_histogram_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_QUERY_RESPONSE_TIME_HISTOGRAM_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_QUERY_RESPONSE_TIME_HISTOGRAM_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(      SELECT       SVR_IP AS SVR_IP,       SVR_PORT AS SVR_PORT,       TENANT_ID AS TENANT_ID,       SQL_TYPE AS SQL_TYPE,       RESPONSE_TIME / 1000000 AS RESPONSE_TIME,       COUNT AS COUNT,       TOTAL / 1000000 AS TOTAL     FROM SYS.ALL_VIRTUAL_QUERY_RESPONSE_TIME )__"))) {
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

int ObInnerTableSchema::v_ob_query_response_time_histogram_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_QUERY_RESPONSE_TIME_HISTOGRAM_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_QUERY_RESPONSE_TIME_HISTOGRAM_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT        SVR_IP,       SVR_PORT,       TENANT_ID,       SQL_TYPE,       RESPONSE_TIME,       COUNT,       TOTAL      FROM SYS.GV$OB_QUERY_RESPONSE_TIME_HISTOGRAM WHERE SVR_IP =HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::dba_ob_spatial_columns_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SPATIAL_COLUMNS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SPATIAL_COLUMNS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   select G.TABLE_SCHEMA AS TABLE_SCHEMA,        G.TABLE_NAME AS TABLE_NAME,        G.COLUMN_NAME AS COLUMN_NAME,        G.SRS_NAME AS SRS_NAME,        G.SRS_ID AS SRS_ID,        G.GEOMETRY_TYPE_NAME AS GEOMETRY_TYPE_NAME,        CAST('SDO_DIM_ARRAY(SDO_DIM_ELEMENT(NULL, -180, 180, 0.5), SDO_DIM_ELEMENT(NULL, -90, 90, 0.5))'          AS VARCHAR2(512)) AS DIMINFO,        D.INDEX_NAME AS INDEX_NAME from     (select CAST(db.database_name AS VARCHAR2(128)) as TABLE_SCHEMA,          CAST(tbl.table_name AS VARCHAR2(256))  as TABLE_NAME,          CAST(col.column_name AS VARCHAR2(128)) as COLUMN_NAME,          CAST(srs.srs_name AS VARCHAR2(128)) as SRS_NAME,          CAST(               CASE                  WHEN TRUNC(col.new_srs_id / POWER(2, 32)) = 4294967295 THEN NULL                 ELSE TRUNC(col.new_srs_id / POWER(2, 32))               END           AS NUMBER(10)) AS SRS_ID,         CAST(             CASE                WHEN (BITAND(col.srs_id, 31) IN (0, 1, 2, 3, 4, 5, 6, 7)) THEN 'SDO_GEOMETRY'               ELSE 'invalid'             END           AS VARCHAR2(128)) AS GEOMETRY_TYPE_NAME,          db.tenant_id as TENANT_ID,          tbl.table_id AS TABLE_ID,          tbl.database_id AS DATABASE_ID     from       (select col1.*,       CASE           WHEN col1.srs_id < 0 THEN             col1.srs_id + POWER(2, 64)          ELSE             col1.srs_id        END AS new_srs_id from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT col1) col left join SYS.ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_REAL_AGENT srs on TRUNC(col.new_srs_id / POWER(2, 32)) = srs.srs_id       join SYS.ALL_VIRTUAL_TABLE_REAL_AGENT tbl on (tbl.table_id = col.table_id and tbl.tenant_id = col.tenant_id)       join SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db on (db.database_id = tbl.database_id and db.tenant_id = tbl.tenant_id)       and db.database_name != '__recyclebin'     where col.data_type = 48         AND BITAND(TRUNC(tbl.table_mode / POWER(2, 12)), 15) IN (0, 1)) G left join      (select T.TABLE_NAME AS TABLE_NAME,            T.INDEX_NAME AS INDEX_NAME,            C.COLUMN_NAME AS COLUMN_NAME,            C.COLUMN_ID AS COLUMN_ID,             T.DATABASE_ID AS DATABASE_ID,            T.TENANT_ID AS TENANT_ID,             C.TABLE_ID AS TABLE_ID     from      (select A.TABLE_NAME AS TABLE_NAME,             A.INDEX_NAME AS INDEX_NAME,             A.COLUMN_NAME  AS COLUMN_NAME,             B.COLUMN_ID AS COLUMN_ID,              B.TENANT_ID as TENANT_ID,             B.TABLE_ID as TABLE_ID,             B.DATABASE_ID AS DATABASE_ID,             B.SRS_ID AS SRS_ID     from (select TABLE_NAME, INDEX_NAME, COLUMN_NAME from ALL_IND_COLUMNS idx_col) A      join (select cl.COLUMN_ID as COLUMN_ID, cl.COLUMN_NAME as COLUMN_NAME, cl.TENANT_ID as TENANT_ID, cl.TABLE_ID as TABLE_ID,            tb.TABLE_NAME as TABLE_NAME, cl.IS_HIDDEN as IS_HIDDEN,            cl.SRS_ID as SRS_ID, tb.DATABASE_ID as DATABASE_ID from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT cl            join SYS.ALL_VIRTUAL_TABLE_REAL_AGENT tb on (tb.table_id = cl.table_id and tb.tenant_id = cl.tenant_id)) B on            (B.COLUMN_NAME = A.COLUMN_NAME AND B.TABLE_NAME = A.TABLE_NAME)           where b.IS_HIDDEN = 1 and SUBSTR(A.COLUMN_NAME, 1, 5) = '__mbr') T            join SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C on (T.SRS_ID = C.COLUMN_ID and T.TENANT_ID = C.TENANT_ID)           where C.DATA_TYPE = 48) D            ON D.TABLE_ID = G.TABLE_ID and D.TENANT_ID = G.TENANT_ID and D.DATABASE_ID = G.DATABASE_ID and D.COLUMN_NAME = G.COLUMN_NAME           ORDER BY 1, 2, 3 )__"))) {
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

int ObInnerTableSchema::dba_ob_table_space_usage_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLE_SPACE_USAGE_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLE_SPACE_USAGE_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select        subquery.TABLE_ID AS TABLE_ID,       subquery.DATABASE_NAME AS DATABASE_NAME,       atrg_name.TABLE_NAME AS TABLE_NAME,       subquery.OCCUPY_SIZE AS OCCUPY_SIZE,       subquery.REQUIRED_SIZE AS REQUIRED_SIZE     from      (        SELECT          CASE            WHEN (atrg.table_type IN (12, 13)) THEN atrg.data_table_id           ELSE atrg.table_id         END AS TABLE_ID,         ad.database_name AS DATABASE_NAME,         SUM(avtps.occupy_size) AS OCCUPY_SIZE,         SUM(avtps.required_size) AS REQUIRED_SIZE       FROM SYS.ALL_VIRTUAL_TABLET_POINTER_STATUS avtps        JOIN SYS.DBA_OB_TABLE_LOCATIONS attl          ON attl.tablet_id = avtps.tablet_id       JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT atrg         ON atrg.table_id = attl.table_id         AND atrg.table_id > 500000       JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT ad         ON ad.database_id = atrg.database_id       JOIN SYS.DBA_OB_LS_LOCATIONS avlmt         ON avtps.ls_id = avlmt.ls_id         AND avtps.svr_ip = avlmt.svr_ip         AND avtps.svr_port = avlmt.svr_port         AND avlmt.role = 'LEADER'       GROUP BY          CASE            WHEN (atrg.table_type IN (12, 13)) THEN atrg.data_table_id           ELSE atrg.table_id         END,         ad.database_name     ) subquery     JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT atrg_name       ON   subquery.TABLE_ID = atrg_name.table_id     ORDER BY TABLE_ID )__"))) {
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

int ObInnerTableSchema::gv_ob_log_transport_dest_stat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_LOG_TRANSPORT_DEST_STAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_LOG_TRANSPORT_DEST_STAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT           TENANT_ID,           SVR_IP,           SVR_PORT,           LS_ID,           CLIENT_IP,           CLIENT_PID,           CLIENT_TENANT_ID,           CASE CLIENT_TYPE             WHEN 1 THEN 'CDC'             WHEN 2 THEN 'STANDBY'             ELSE 'UNKNOWN'           END AS CLIENT_TYPE,           START_SERVE_TIME,           LAST_SERVE_TIME,           CASE LAST_READ_SOURCE             WHEN 1 THEN 'ONLINE'             WHEN 2 THEN 'ARCHIVE'             ELSE 'UNKNOWN'           END AS LAST_READ_SOURCE,           CASE LAST_REQUEST_TYPE             WHEN 0 THEN 'SEQUENTIAL_READ_SERIAL'             WHEN 1 THEN 'SEQUENTIAL_READ_PARALLEL'             WHEN 2 THEN 'SCATTERED_READ'             ELSE 'UNKNOWN'           END AS LAST_REQUEST_TYPE,           LAST_REQUEST_LOG_LSN,           LAST_REQUEST_LOG_SCN,           LAST_FAILED_REQUEST,           AVG_REQUEST_PROCESS_TIME,           AVG_REQUEST_QUEUE_TIME,           AVG_REQUEST_READ_LOG_TIME,           AVG_REQUEST_READ_LOG_SIZE,           CASE             WHEN AVG_LOG_TRANSPORT_BANDWIDTH >= 1024 * 1024 * 1024 THEN               CONCAT(ROUND(AVG_LOG_TRANSPORT_BANDWIDTH/1024/1024/1024, 2), 'GB/S')             WHEN AVG_LOG_TRANSPORT_BANDWIDTH >= 1024 * 1024  THEN               CONCAT(ROUND(AVG_LOG_TRANSPORT_BANDWIDTH/1024/1024, 2), 'MB/S')             WHEN AVG_LOG_TRANSPORT_BANDWIDTH >= 1024 THEN               CONCAT(ROUND(AVG_LOG_TRANSPORT_BANDWIDTH/1024, 2), 'KB/S')             ELSE               CONCAT(AVG_LOG_TRANSPORT_BANDWIDTH, 'B/s')           END AS AVG_LOG_TRANSPORT_BANDWIDTH     FROM SYS.ALL_VIRTUAL_LOG_TRANSPORT_DEST_STAT )__"))) {
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

int ObInnerTableSchema::v_ob_log_transport_dest_stat_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_LOG_TRANSPORT_DEST_STAT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_LOG_TRANSPORT_DEST_STAT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT           TENANT_ID,           SVR_IP,           SVR_PORT,           LS_ID,           CLIENT_IP,           CLIENT_PID,           CLIENT_TENANT_ID,           CLIENT_TYPE,           START_SERVE_TIME,           LAST_SERVE_TIME,           LAST_READ_SOURCE,           LAST_REQUEST_TYPE,           LAST_REQUEST_LOG_LSN,           LAST_REQUEST_LOG_SCN,           LAST_FAILED_REQUEST,           AVG_REQUEST_PROCESS_TIME,           AVG_REQUEST_QUEUE_TIME,           AVG_REQUEST_READ_LOG_TIME,           AVG_REQUEST_READ_LOG_SIZE,           AVG_LOG_TRANSPORT_BANDWIDTH     FROM SYS.GV$OB_LOG_TRANSPORT_DEST_STAT     WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::gv_ob_ss_local_cache_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SS_LOCAL_CACHE_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SS_LOCAL_CACHE_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SVR_IP,     SVR_PORT,     TENANT_ID, 		     CACHE_NAME,     PRIORITY,     HIT_RATIO,     TOTAL_HIT_CNT,     TOTAL_MISS_CNT,     HOLD_SIZE,     ALLOC_DISK_SIZE,     USED_DISK_SIZE,     USED_MEM_SIZE   FROM SYS.ALL_VIRTUAL_SS_LOCAL_CACHE_INFO   )__"))) {
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

int ObInnerTableSchema::v_ob_ss_local_cache_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SS_LOCAL_CACHE_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SS_LOCAL_CACHE_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     SVR_IP,     SVR_PORT,     TENANT_ID, 		     CACHE_NAME,     PRIORITY,     HIT_RATIO,     TOTAL_HIT_CNT,     TOTAL_MISS_CNT,     HOLD_SIZE,     ALLOC_DISK_SIZE,     USED_DISK_SIZE,     USED_MEM_SIZE   FROM SYS.GV$OB_SS_LOCAL_CACHE   WHERE SVR_IP = host_ip() AND SVR_PORT = rpc_port()   )__"))) {
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

int ObInnerTableSchema::all_plsql_types_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PLSQL_TYPES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PLSQL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       d.database_name as owner,       cast(t.type_name as varchar2(136))as type_name,       p.package_name as package_name,       cast(utl_raw.cast_from_number(t.type_id) as raw(16)) as type_oid,       cast(decode(t.typecode, 1, 'COLLECTION',                         6, 'PL/SQL RECORD',                         7, 'COLLECTION',                         8, 'SUBTYPE',                         'UNKNOWN TYPECODE:' || T.typecode) as varchar2(58)) as typecode,       t.attributes as attributes,       cast(decode(bitand(t.properties, 4611686018427387904), 4611686018427387904, 'YES', 0, 'NO') as varchar2(3)) as CONTAINS_PLSQL     from        sys.all_virtual_pkg_type_real_agent T       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT D         on d.database_id = t.database_id         and (T.DATABASE_ID = USERENV('SCHEMAID')             OR USER_CAN_ACCESS_OBJ(3, T.PACKAGE_ID, T.DATABASE_ID) = 1)         and t.typecode in (1, 6, 7, 8)       join sys.all_virtual_package_real_agent P         on t.package_id = p.package_id     UNION all     select       'SYS' as owner,       cast(ts.type_name as varchar2(136)) as type_name,       ps.package_name as package_name,       cast(utl_raw.cast_from_number(ts.type_id) as raw(16)) as type_oid,       cast(decode(ts.typecode, 1, 'COLLECTION',                           6, 'PL/SQL RECORD',                           7, 'COLLECTION',                           8, 'SUBTYPE',                         'UNKNOWN TYPECODE:' || Ts.typecode) as varchar2(58)) as typecode,       ts.attributes as attributes,       cast(decode(bitand(ts.properties, 4611686018427387904), 4611686018427387904, 'YES', 0, 'NO') as varchar2(3)) as CONTAINS_PLSQL     from sys.all_virtual_pkg_type_sys_agent ts       join sys.all_virtual_package_sys_agent ps       on ts.typecode in (1, 6, 7)         and ts.package_id = ps.package_id )__"))) {
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

int ObInnerTableSchema::dba_plsql_types_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PLSQL_TYPES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PLSQL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       d.database_name as owner,       cast(t.type_name as varchar2(136))as type_name,       p.package_name as package_name,       cast(null as raw(16)) as type_oid,       cast(decode(t.typecode, 1, 'COLLECTION',                         6, 'PL/SQL RECORD',                         7, 'COLLECTION',                         8, 'SUBTYPE',                         'UNKNOWN TYPECODE:' || T.typecode) as varchar2(58)) as typecode,       t.attributes as attributes,       cast(decode(bitand(t.properties, 4611686018427387904), 4611686018427387904, 'YES', 0, 'NO') as varchar2(3)) as CONTAINS_PLSQL     from        sys.all_virtual_pkg_type_real_agent T       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT D         on d.database_id = t.database_id         and t.typecode in (1, 6, 7, 8)       join sys.all_virtual_package_real_agent P         on t.package_id = p.package_id     UNION all     select       'SYS' as owner,       cast(ts.type_name as varchar2(136)) as type_name,       ps.package_name as package_name,       cast(NULL as raw(16)) as type_oid,       cast(decode(ts.typecode, 1, 'COLLECTION',                           6, 'PL/SQL RECORD',                           7, 'COLLECTION',                           8, 'SUBTYPE',                         'UNKNOWN TYPECODE:' || Ts.typecode) as varchar2(58)) as typecode,       ts.attributes as attributes,       cast(decode(bitand(ts.properties, 4611686018427387904), 4611686018427387904, 'YES', 0, 'NO') as varchar2(3)) as CONTAINS_PLSQL     from sys.all_virtual_pkg_type_sys_agent ts       join sys.all_virtual_package_sys_agent ps       on ts.typecode in (1, 6, 7)         and ts.package_id = ps.package_id )__"))) {
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

int ObInnerTableSchema::user_plsql_types_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_PLSQL_TYPES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PLSQL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       cast(t.type_name as varchar2(136))as type_name,       p.package_name as package_name,       cast(null as raw(16)) as type_oid,       cast(decode(t.typecode, 1, 'COLLECTION',                         6, 'PL/SQL RECORD',                         7, 'COLLECTION',                         8, 'SUBTYPE',                         'UNKNOWN TYPECODE:' || T.typecode) as varchar2(58)) as typecode,       t.attributes as attributes,       cast(decode(bitand(t.properties, 4611686018427387904), 4611686018427387904, 'YES', 0, 'NO') as varchar2(3)) as CONTAINS_PLSQL     from        sys.all_virtual_pkg_type_real_agent T       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT D         on d.database_id = t.database_id         and t.typecode in (1, 6, 7, 8)       join sys.all_virtual_package_real_agent P         on t.package_id = p.package_id       where t.database_id = USERENV('SCHEMAID') )__"))) {
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

int ObInnerTableSchema::all_plsql_coll_types_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PLSQL_COLL_TYPES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PLSQL_COLL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       d.database_name as owner,       cast(pt.type_name as varchar2(128)) as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast(null as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(             CASE BITAND(C.PROPERTIES, 15)             WHEN 3               THEN DECODE (C.ELEM_TYPE_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),                 46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.number_precision, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id         and bitand(c.properties, 15) = 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)     UNION ALL     select       d.database_name as owner,       cast(pt.type_name as varchar2(128)) as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast(d1.database_name as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(t.TYPE_NAME AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.number_precision, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id            and bitand(c.properties, 15) != 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_type_real_agent t         on t.type_id = c.elem_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on t.database_id = d1.database_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       d1.database_name as ELEM_TYPE_OWNER,       cast(pt1.type_name as varchar2(136)) as ELEM_TYPE_NAME,       p1.package_name as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')             or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent p1         on c.elem_package_id = p1.package_id       join sys.all_virtual_pkg_type_real_agent pt1         on c.elem_package_id = pt1.package_id         and pt1.type_id = c.elem_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on p1.database_id = d1.database_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       'SYS' as ELEM_TYPE_OWNER,       cast(t.TYPE_NAME AS VARCHAR2(136)) as elem_type_name,       NULL as elem_type_package,       NULL as length,       NULL as PRECISION,       NULL as scale,       NULL as CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_type_sys_agent T         on t.type_id = c.elem_type_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       'SYS' as ELEM_TYPE_OWNER,       cast(pts.type_name as varchar2(136)) as ELEM_TYPE_NAME,       ps.package_name as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id       join sys.all_virtual_package_sys_agent ps         on c.elem_package_id = ps.package_id       join sys.all_virtual_pkg_type_sys_agent pts         on c.elem_package_id = pts.package_id         and pts.type_id = c.elem_type_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       d.database_name as ELEM_TYPE_OWNER,       cast(case bitand(c.properties, 15)           when 9 then tbl.table_name || '%ROWTYPE'           else 'NOT SUPPORT' end as varchar2(136)) as ELEM_TYPE_NAME,       NULL as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id         and (bitand(c.properties, 15) = 9             or bitand(c.properties, 15) = 10)       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')             or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.ALL_VIRTUAL_TABLE_REAL_AGENT tbl         on c.elem_type_id = tbl.table_id     UNION ALL     select       'SYS' as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast(NULL as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(             CASE BITAND(C.PROPERTIES, 15)             WHEN 3               THEN DECODE (C.ELEM_TYPE_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),                 46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.number_precision, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_sys_agent pt       join sys.all_virtual_package_sys_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_sys_agent c         on pt.type_id = c.coll_type_id            and bitand(c.properties, 15) = 3     UNION ALL     select       'SYS' as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast('SYS' as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(t.TYPE_NAME AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.number_precision, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_sys_agent pt       join sys.all_virtual_package_sys_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_sys_agent c         on pt.type_id = c.coll_type_id            and bitand(c.properties, 15) != 3       join sys.all_virtual_type_sys_agent t         on t.type_id = c.elem_type_id )__"))) {
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

int ObInnerTableSchema::dba_plsql_coll_types_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_PLSQL_COLL_TYPES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_PLSQL_COLL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       d.database_name as owner,       cast(pt.type_name as varchar2(128)) as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast(null as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(             CASE BITAND(C.PROPERTIES, 15)             WHEN 3               THEN DECODE (C.ELEM_TYPE_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),                 46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.length, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id         and bitand(c.properties, 15) = 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id     UNION ALL     select       d.database_name as owner,       cast(pt.type_name as varchar2(128)) as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast(d1.database_name as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(t.TYPE_NAME AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.length, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id            and bitand(c.properties, 15) != 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id       join sys.all_virtual_type_real_agent t         on t.type_id = c.elem_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on t.database_id = d1.database_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       d1.database_name as ELEM_TYPE_OWNER,       cast(pt1.type_name as varchar2(136)) as ELEM_TYPE_NAME,       p1.package_name as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id       join sys.all_virtual_package_real_agent p1         on c.elem_package_id = p1.package_id       join sys.all_virtual_pkg_type_real_agent pt1         on c.elem_package_id = pt1.package_id         and pt1.type_id = c.elem_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on p1.database_id = d1.database_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       'SYS' as ELEM_TYPE_OWNER,       cast(t.TYPE_NAME AS VARCHAR2(136)) as elem_type_name,       NULL as elem_type_package,       NULL as length,       NULL as PRECISION,       NULL as scale,       NULL as CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id       join sys.all_virtual_type_sys_agent T         on t.type_id = c.elem_type_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       'SYS' as ELEM_TYPE_OWNER,       cast(pts.type_name as varchar2(136)) as ELEM_TYPE_NAME,       ps.package_name as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id       join sys.all_virtual_package_sys_agent ps         on c.elem_package_id = ps.package_id       join sys.all_virtual_pkg_type_sys_agent pts         on c.elem_package_id = pts.package_id         and pts.type_id = c.elem_type_id     UNION ALL     select       d.database_name as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       d.database_name as ELEM_TYPE_OWNER,       cast(case bitand(c.properties, 15)           when 9 then tbl.table_name || '%ROWTYPE'           else 'NOT SUPPORT' end as varchar2(136)) as ELEM_TYPE_NAME,       NULL as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id         and (bitand(c.properties, 15) = 9             or bitand(c.properties, 15) = 10)       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')             or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.ALL_VIRTUAL_TABLE_REAL_AGENT tbl         on c.elem_type_id = tbl.table_id     UNION ALL     select       'SYS' as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast(NULL as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(             CASE BITAND(C.PROPERTIES, 15)             WHEN 3               THEN DECODE (C.ELEM_TYPE_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),                 46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.length, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_sys_agent pt       join sys.all_virtual_package_sys_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_sys_agent c         on pt.type_id = c.coll_type_id            and bitand(c.properties, 15) = 3     UNION ALL     select       'SYS' as owner,       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast('SYS' as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(t.TYPE_NAME AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.length, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_sys_agent pt       join sys.all_virtual_package_sys_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_sys_agent c         on pt.type_id = c.coll_type_id            and bitand(c.properties, 15) != 3       join sys.all_virtual_type_sys_agent t         on t.type_id = c.elem_type_id )__"))) {
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

int ObInnerTableSchema::user_plsql_coll_types_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_PLSQL_COLL_TYPES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PLSQL_COLL_TYPES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     select       cast(pt.type_name as varchar2(128)) as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast(null as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(             CASE BITAND(C.PROPERTIES, 15)             WHEN 3               THEN DECODE (C.ELEM_TYPE_ID,                 0,  'NULL',                 1,  'NUMBER',                 2,  'NUMBER',                 3,  'NUMBER',                 4,  'NUMBER',                 5,  'NUMBER',                 6,  'NUMBER',                 7,  'NUMBER',                 8,  'NUMBER',                 9,  'NUMBER',                 10, 'NUMBER',                 11, 'BINARY_FLOAT',                 12, 'BINARY_DOUBLE',                 13, 'NUMBER',                 14, 'NUMBER',                 15, 'NUMBER',                 16, 'NUMBER',                 17, 'DATE',                 18, 'TIMESTAMP',                 19, 'DATE',                 20, 'TIME',                 21, 'YEAR',                 22, 'VARCHAR2',                 23, 'CHAR',                 24, 'HEX_STRING',                 25, 'EXT',                 26, 'UNKNOWN',                 27, 'TINYTEXT',                 28, 'TEXT',                 29, 'MEDIUMTEXT',                 30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 31, 'BIT',                 32, 'ENUM',                 33, 'SET',                 34, 'ENUM_INNER',                 35, 'SET_INNER',                 36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),                 37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),                 38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),                 39, 'RAW',                 40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),                 41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),                 42, 'FLOAT',                 43, 'NVARCHAR2',                 44, 'NCHAR',                 45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),                 46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),                 47, 'JSON',                 'NOT_SUPPORT')             ELSE 'NOT_SUPPORT' END AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.length, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id         and bitand(c.properties, 15) = 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')     UNION ALL     select       cast(pt.type_name as varchar2(128)) as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       cast(d1.database_name as varchar2(128)) as ELEM_TYPE_OWNER,       CAST(t.TYPE_NAME AS VARCHAR2(136)) AS ELEM_TYPE_NAME,       NULL as elem_type_package,       c.length as length,       c.number_precision as PRECISION,       c.scale as scale,       CAST('CHAR_CS' AS VARCHAR2(44)) AS CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       CAST(DECODE(c.length, 1, 'C', 'B') AS VARCHAR2(1)) AS CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                   NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id            and bitand(c.properties, 15) != 3       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')       join sys.all_virtual_type_real_agent t         on t.type_id = c.elem_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on t.database_id = d1.database_id     UNION ALL     select       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       d1.database_name as ELEM_TYPE_OWNER,       cast(pt1.type_name as varchar2(136)) as ELEM_TYPE_NAME,       p1.package_name as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')       join sys.all_virtual_package_real_agent p1         on c.elem_package_id = p1.package_id       join sys.all_virtual_pkg_type_real_agent pt1         on c.elem_package_id = pt1.package_id         and pt1.type_id = c.elem_type_id       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d1         on p1.database_id = d1.database_id     UNION ALL     select       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       'SYS' as ELEM_TYPE_OWNER,       cast(t.TYPE_NAME AS VARCHAR2(136)) as elem_type_name,       NULL as elem_type_package,       NULL as length,       NULL as PRECISION,       NULL as scale,       NULL as CHARACTER_SET_NAME,       CAST(NULL AS VARCHAR2(7)) AS ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and (pt.database_id = USERENV('SCHEMAID')               or USER_CAN_ACCESS_OBJ(3, PT.package_id, PT.DATABASE_ID) = 1)       join sys.all_virtual_package_real_agent p         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id         and pt.database_id = USERENV('SCHEMAID')       join sys.all_virtual_type_sys_agent T         on t.type_id = c.elem_type_id     UNION ALL     select       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       'SYS' as ELEM_TYPE_OWNER,       cast(pts.type_name as varchar2(136)) as ELEM_TYPE_NAME,       ps.package_name as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id       join sys.all_virtual_package_sys_agent ps         on c.elem_package_id = ps.package_id       join sys.all_virtual_pkg_type_sys_agent pts         on c.elem_package_id = pts.package_id         and pts.type_id = c.elem_type_id     UNION ALL     select       pt.type_name as type_name,       p.package_name as package_name,       cast(decode(pt.typecode, 1, decode(c.upper_bound, -1, 'TABLE', 'VARYING ARRAY'),                               7, 'PL/SQL INDEX TABLE',                               'UNKNOWN')           AS varchar2(18)) as coll_type,       cast(decode(c.upper_bound, -1, NULL, c.upper_bound) as int) AS UPPER_BOUND,       d.database_name as ELEM_TYPE_OWNER,       cast(case bitand(c.properties, 15)           when 9 then tbl.table_name || '%ROWTYPE'           else 'NOT SUPPORT' end as varchar2(136)) as ELEM_TYPE_NAME,       NULL as elem_type_package,       null as length,       null as PRECISION,       null as scale,       null as CHARACTER_SET_NAME,       null as ELEM_STORAGE,       cast('YES' as varchar2(3)) as NULLS_STORED,       'B' as CHAR_USED,       CAST(DECODE(pt.TYPECODE, 7, DECODE(C.UPPER_BOUND, -1, 'BINARY_INTEGER', 'VARCHAR2'),                                 NULL) AS VARCHAR2(14)) AS INDEX_BY,       CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD     from sys.all_virtual_pkg_type_real_agent pt       join sys.all_virtual_package_real_agent P         on pt.package_id = p.package_id       join sys.all_virtual_pkg_coll_type_real_agent C         on pt.type_id = c.coll_type_id         and (bitand(c.properties, 15) = 9             or bitand(c.properties, 15) = 10)       join sys.ALL_VIRTUAL_DATABASE_REAL_AGENT d         on pt.database_id = d.database_id         and pt.database_id = USERENV('SCHEMAID')       join sys.ALL_VIRTUAL_TABLE_REAL_AGENT tbl         on c.elem_type_id = tbl.table_id )__"))) {
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
