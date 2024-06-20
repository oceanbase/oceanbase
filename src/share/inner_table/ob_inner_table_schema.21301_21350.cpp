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

int ObInnerTableSchema::dba_ob_kv_ttl_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_KV_TTL_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_KV_TTL_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       b.table_name as TABLE_NAME,       a.table_id as TABLE_ID,       a.tablet_id as TABLET_ID,       a.task_id as TASK_ID,       usec_to_time(a.task_start_time) as START_TIME,       usec_to_time(a.task_update_time) as END_TIME,       case a.trigger_type         when 0 then "PERIODIC"          when 1 then "USER"         else "INVALID" END AS TRIGGER_TYPE,       case a.status         when 0 then "PREPARED"          when 1 then "RUNNING"          when 2 then "PENDING"          when 3 then "CANCELED"          when 4 then "FINISHED"          when 5 then "MOVED"          when 15 then "RS_TRIGGERING"         when 16 then "RS_SUSPENDING"         when 17 then "RS_CANCELING"         when 18 then "RS_MOVING"         when 47 then "RS_TRIGGERD"         when 48 then "RS_SUSPENDED"         when 49 then "RS_CANCELED"         when 50 then "RS_MOVED"         else "INVALID" END AS STATUS,       a.ttl_del_cnt as TTL_DEL_CNT,       a.max_version_del_cnt as MAX_VERSION_DEL_CNT,       a.scan_cnt as SCAN_CNT,       a.ret_code as RET_CODE       FROM oceanbase.__all_virtual_kv_ttl_task a left outer JOIN oceanbase.__all_table b on           a.table_id = b.table_id and a.tenant_id = effective_tenant_id()           and b.table_mode >> 12 & 15 in (0,1) )__"))) {
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

int ObInnerTableSchema::dba_ob_kv_ttl_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_KV_TTL_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_KV_TTL_TASK_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       b.table_name as TABLE_NAME,       a.table_id as TABLE_ID,       a.tablet_id as TABLET_ID,       a.task_id as TASK_ID,       usec_to_time(a.task_start_time) as START_TIME,       usec_to_time(a.task_update_time) as END_TIME,       case a.trigger_type         when 0 then "PERIODIC"          when 1 then "USER"         else "INVALID" END AS TRIGGER_TYPE,       case a.status         when 0 then "PREPARED"          when 1 then "RUNNING"          when 2 then "PENDING"          when 3 then "CANCELED"          when 4 then "FINISHED"          when 5 then "MOVED"          else "INVALID" END AS STATUS,       a.ttl_del_cnt as TTL_DEL_CNT,       a.max_version_del_cnt as MAX_VERSION_DEL_CNT,       a.scan_cnt as SCAN_CNT,       a.ret_code as RET_CODE       FROM oceanbase.__all_virtual_kv_ttl_task_history a left outer JOIN oceanbase.__all_table b on           a.table_id = b.table_id and a.tenant_id = effective_tenant_id()           and b.table_mode >> 12 & 15 in (0,1) )__"))) {
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

int ObInnerTableSchema::gv_ob_log_stat_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_LOG_STAT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_LOG_STAT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     TENANT_ID,     LS_ID,     SVR_IP,     SVR_PORT,     ROLE,     PROPOSAL_ID,     CONFIG_VERSION,     ACCESS_MODE,     PAXOS_MEMBER_LIST,     PAXOS_REPLICA_NUM,     CASE in_sync       WHEN 1 THEN 'YES'       ELSE 'NO' END     AS IN_SYNC,     BASE_LSN,     BEGIN_LSN,     BEGIN_SCN,     END_LSN,     END_SCN,     MAX_LSN,     MAX_SCN,     ARBITRATION_MEMBER,     DEGRADED_LIST,     LEARNER_LIST   FROM oceanbase.__all_virtual_log_stat )__"))) {
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

int ObInnerTableSchema::v_ob_log_stat_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_LOG_STAT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_LOG_STAT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,     LS_ID,     SVR_IP,     SVR_PORT,     ROLE,     PROPOSAL_ID,     CONFIG_VERSION,     ACCESS_MODE,     PAXOS_MEMBER_LIST,     PAXOS_REPLICA_NUM,     IN_SYNC,     BASE_LSN,     BEGIN_LSN,     BEGIN_SCN,     END_LSN,     END_SCN,     MAX_LSN,     MAX_SCN,     ARBITRATION_MEMBER,     DEGRADED_LIST,     LEARNER_LIST   FROM oceanbase.GV$OB_LOG_STAT   WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::st_geometry_columns_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_ST_GEOMETRY_COLUMNS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ST_GEOMETRY_COLUMNS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   select CAST(db.database_name AS CHAR(128)) collate utf8mb4_name_case as TABLE_SCHEMA,          CAST(tbl.table_name AS CHAR(256)) collate utf8mb4_name_case as TABLE_NAME,          CAST(col.column_name AS CHAR(128)) as COLUMN_NAME,          CAST(srs.srs_name AS CHAR(128)) as SRS_NAME,          CAST(if ((col.srs_id >> 32) = 4294967295, NULL, col.srs_id >> 32) AS UNSIGNED) as SRS_ID,          CAST(case (col.srs_id & 31)                 when 0 then 'geometry'                 when 1 then 'point'                 when 2 then 'linestring'                 when 3 then 'polygon'                 when 4 then 'multipoint'                 when 5 then 'multilinestring'                 when 6 then 'multipolygon'                 when 7 then 'geomcollection'                 else 'invalid'           end AS CHAR(128))as GEOMETRY_TYPE_NAME   from       oceanbase.__all_column col left join oceanbase.__all_spatial_reference_systems srs on (col.srs_id >> 32) = srs.srs_id       join oceanbase.__all_table tbl on (tbl.table_id = col.table_id and tbl.tenant_id = col.tenant_id)       join oceanbase.__all_database db on (db.database_id = tbl.database_id and db.tenant_id = tbl.tenant_id)       and db.database_name != '__recyclebin'   where col.data_type  = 48   and tbl.table_mode >> 12 & 15 in (0,1); )__"))) {
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

int ObInnerTableSchema::st_spatial_reference_systems_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_ST_SPATIAL_REFERENCE_SYSTEMS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ST_SPATIAL_REFERENCE_SYSTEMS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(
  select CAST(srs_name AS CHAR(128)) as SRS_NAME,
         CAST(srs_id AS UNSIGNED) as SRS_ID,
         CAST(organization AS CHAR(256)) as ORGANIZATION,
         CAST(organization_coordsys_id AS UNSIGNED) as ORGANIZATION_COORDSYS_ID,
         CAST(definition AS CHAR(4096)) as DEFINITION,
         CAST(description AS CHAR(2048)) as DESCRIPTION
  from oceanbase.__all_spatial_reference_systems; )__"))) {
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

int ObInnerTableSchema::query_response_time_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_QUERY_RESPONSE_TIME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_QUERY_RESPONSE_TIME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select response_time as RESPONSE_TIME,                    count as COUNT,                    total as TOTAL                    from oceanbase.__all_virtual_query_response_time                    where tenant_id = effective_tenant_id() )__"))) {
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

int ObInnerTableSchema::cdb_ob_kv_ttl_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_KV_TTL_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_KV_TTL_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       a.tenant_id as TENANT_ID,       b.table_name as TABLE_NAME,       a.table_id as TABLE_ID,       a.tablet_id as TABLET_ID,       a.task_id as TASK_ID,       usec_to_time(a.task_start_time) as START_TIME,       usec_to_time(a.task_update_time) as END_TIME,       case a.trigger_type         when 0 then "PERIODIC"          when 1 then "USER"         else "INVALID" END AS TRIGGER_TYPE,       case a.status         when 0 then "PREPARED"          when 1 then "RUNNING"          when 2 then "PENDING"          when 3 then "CANCELED"          when 4 then "FINISHED"          when 5 then "MOVED"          when 15 then "RS_TRIGGERING"         when 16 then "RS_SUSPENDING"         when 17 then "RS_CANCELING"         when 18 then "RS_MOVING"         when 47 then "RS_TRIGGERD"         when 48 then "RS_SUSPENDED"         when 49 then "RS_CANCELED"         when 50 then "RS_MOVED"         else "INVALID" END AS STATUS,       a.ttl_del_cnt as TTL_DEL_CNT,       a.max_version_del_cnt as MAX_VERSION_DEL_CNT,       a.scan_cnt as SCAN_CNT,       a.ret_code as RET_CODE       FROM oceanbase.__all_virtual_kv_ttl_task a left outer JOIN oceanbase.__all_virtual_table b on           a.table_id = b.table_id and a.tenant_id = b.tenant_id           and b.table_mode >> 12 & 15 in (0,1) )__"))) {
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

int ObInnerTableSchema::cdb_ob_kv_ttl_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_KV_TTL_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_KV_TTL_TASK_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       a.tenant_id as TENANT_ID,       b.table_name as TABLE_NAME,       a.table_id as TABLE_ID,       a.tablet_id as TABLET_ID,       a.task_id as TASK_ID,       usec_to_time(a.task_start_time) as START_TIME,       usec_to_time(a.task_update_time) as END_TIME,       case a.trigger_type         when 0 then "PERIODIC"          when 1 then "USER"         else "INVALID" END AS TRIGGER_TYPE,       case a.status         when 0 then "PREPARED"          when 1 then "RUNNING"          when 2 then "PENDING"          when 3 then "CANCELED"          when 4 then "FINISHED"          when 5 then "MOVED"          else "INVALID" END AS STATUS,       a.ttl_del_cnt as TTL_DEL_CNT,       a.max_version_del_cnt as MAX_VERSION_DEL_CNT,       a.scan_cnt as SCAN_CNT,       a.ret_code as RET_CODE       FROM oceanbase.__all_virtual_kv_ttl_task_history a left outer JOIN oceanbase.__all_virtual_table b on           a.table_id = b.table_id and a.tenant_id = b.tenant_id           and b.table_mode >> 12 & 15 in (0,1) )__"))) {
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

int ObInnerTableSchema::dba_rsrc_plans_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RSRC_PLANS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(NULL AS NUMBER) AS PLAN_ID,       PLAN,       CAST(NULL AS NUMBER) AS NUM_PLAN_DIRECTIVES,       CAST(NULL AS CHAR(128)) AS CPU_METHOD,       CAST(NULL AS CHAR(128)) AS MGMT_METHOD,       CAST(NULL AS CHAR(128)) AS ACTIVE_SESS_POOL_MTH,       CAST(NULL AS CHAR(128)) AS PARALLEL_DEGREE_LIMIT_MTH,       CAST(NULL AS CHAR(128)) AS QUEUING_MTH,       CAST(NULL AS CHAR(3)) AS SUB_PLAN,       COMMENTS,       CAST(NULL AS CHAR(128)) AS STATUS,       CAST(NULL AS CHAR(3)) AS MANDATORY     FROM        oceanbase.__all_res_mgr_plan )__"))) {
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

int ObInnerTableSchema::dba_rsrc_plan_directives_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RSRC_PLAN_DIRECTIVES_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       PLAN,       GROUP_OR_SUBPLAN,       CAST(NULL AS CHAR(14)) AS TYPE,       CAST(NULL AS NUMBER) AS CPU_P1,       CAST(NULL AS NUMBER) AS CPU_P2,       CAST(NULL AS NUMBER) AS CPU_P3,       CAST(NULL AS NUMBER) AS CPU_P4,       CAST(NULL AS NUMBER) AS CPU_P5,       CAST(NULL AS NUMBER) AS CPU_P6,       CAST(NULL AS NUMBER) AS CPU_P7,       CAST(NULL AS NUMBER) AS CPU_P8,       MGMT_P1,       CAST(NULL AS NUMBER) AS MGMT_P2,       CAST(NULL AS NUMBER) AS MGMT_P3,       CAST(NULL AS NUMBER) AS MGMT_P4,       CAST(NULL AS NUMBER) AS MGMT_P5,       CAST(NULL AS NUMBER) AS MGMT_P6,       CAST(NULL AS NUMBER) AS MGMT_P7,       CAST(NULL AS NUMBER) AS MGMT_P8,       CAST(NULL AS NUMBER) AS ACTIVE_SESS_POOL_P1,       CAST(NULL AS NUMBER) AS QUEUEING_P1,       CAST(NULL AS NUMBER) AS PARALLEL_TARGET_PERCENTAGE,       CAST(NULL AS NUMBER) AS PARALLEL_DEGREE_LIMIT_P1,       CAST(NULL AS CHAR(128)) AS SWITCH_GROUP,       CAST(NULL AS CHAR(5)) AS SWITCH_FOR_CALL,       CAST(NULL AS NUMBER) AS SWITCH_TIME,       CAST(NULL AS NUMBER) AS SWITCH_IO_MEGABYTES,       CAST(NULL AS NUMBER) AS SWITCH_IO_REQS,       CAST(NULL AS CHAR(5)) AS SWITCH_ESTIMATE,       CAST(NULL AS NUMBER) AS MAX_EST_EXEC_TIME,       CAST(NULL AS NUMBER) AS UNDO_POOL,       CAST(NULL AS NUMBER) AS MAX_IDLE_TIME,       CAST(NULL AS NUMBER) AS MAX_IDLE_BLOCKER_TIME,       CAST(NULL AS NUMBER) AS MAX_UTILIZATION_LIMIT,       CAST(NULL AS NUMBER) AS PARALLEL_QUEUE_TIMEOUT,       CAST(NULL AS NUMBER) AS SWITCH_TIME_IN_CALL,       CAST(NULL AS NUMBER) AS SWITCH_IO_LOGICAL,       CAST(NULL AS NUMBER) AS SWITCH_ELAPSED_TIME,       CAST(NULL AS NUMBER) AS PARALLEL_SERVER_LIMIT,       UTILIZATION_LIMIT,       CAST(NULL AS CHAR(12)) AS PARALLEL_STMT_CRITICAL,       CAST(NULL AS NUMBER) AS SESSION_PGA_LIMIT,       CAST(NULL AS CHAR(6)) AS PQ_TIMEOUT_ACTION,       COMMENTS,       CAST(NULL AS CHAR(128)) AS STATUS,       CAST('YES' AS CHAR(3)) AS MANDATORY     FROM        oceanbase.__all_res_mgr_directive )__"))) {
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

int ObInnerTableSchema::dba_rsrc_group_mappings_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RSRC_GROUP_MAPPINGS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       ATTRIBUTE,       VALUE,       CONSUMER_GROUP,       CAST(NULL AS CHAR(128)) AS STATUS     FROM        oceanbase.__all_res_mgr_mapping_rule )__"))) {
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

int ObInnerTableSchema::dba_rsrc_consumer_groups_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_RSRC_CONSUMER_GROUPS_TID);
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CONSUMER_GROUP_ID,       CONSUMER_GROUP,       CAST(NULL AS CHAR(128)) AS CPU_METHOD,       CAST(NULL AS CHAR(128)) AS MGMT_METHOD,       CAST(NULL AS CHAR(3)) AS INTERNAL_USE,       COMMENTS,       CAST(NULL AS CHAR(128)) AS CATEGORY,       CAST(NULL AS CHAR(128)) AS STATUS,       CAST(NULL AS CHAR(3)) AS MANDATORY     FROM        oceanbase.__all_res_mgr_consumer_group )__"))) {
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

int ObInnerTableSchema::v_rsrc_plan_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_RSRC_PLAN_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_RSRC_PLAN_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           CAST(NULL as NUMBER) AS ID,           B.plan NAME,           CAST('TRUE' AS CHAR(5)) AS IS_TOP_PLAN,           CAST('ON' AS CHAR(3)) AS CPU_MANAGED,           CAST(NULL AS CHAR(3)) AS INSTANCE_CAGING,           CAST(NULL AS NUMBER) AS PARALLEL_SERVERS_ACTIVE,           CAST(NULL AS NUMBER) AS PARALLEL_SERVERS_TOTAL,           CAST(NULL AS CHAR(32)) AS PARALLEL_EXECUTION_MANAGED         FROM oceanbase.__tenant_virtual_global_variable A, oceanbase.dba_rsrc_plans B         WHERE A.variable_name = 'resource_manager_plan' AND A.value = B.plan )__"))) {
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

int ObInnerTableSchema::cdb_ob_column_checksum_error_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_COLUMN_CHECKSUM_ERROR_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_COLUMN_CHECKSUM_ERROR_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          FROZEN_SCN,          (CASE index_type                WHEN 0 THEN 'LOCAL_INDEX'                WHEN 1 THEN 'GLOBAL_INDEX'                ELSE 'UNKNOWN' END) AS INDEX_TYPE,           DATA_TABLE_ID,           INDEX_TABLE_ID,           DATA_TABLET_ID,           INDEX_TABLET_ID,           COLUMN_ID,           DATA_COLUMN_CKM AS DATA_COLUMN_CHECKSUM,           INDEX_COLUMN_CKM AS INDEX_COLUMN_CHECKSUM   FROM OCEANBASE.__ALL_VIRTUAL_COLUMN_CHECKSUM_ERROR_INFO   )__"))) {
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

int ObInnerTableSchema::cdb_ob_tablet_checksum_error_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TABLET_CHECKSUM_ERROR_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TABLET_CHECKSUM_ERROR_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          TABLET_ID   FROM     (       SELECT TENANT_ID,              TABLET_ID,              ROW_COUNT,              DATA_CHECKSUM,              B_COLUMN_CHECKSUMS,              COMPACTION_SCN       FROM OCEANBASE.__ALL_VIRTUAL_TABLET_REPLICA_CHECKSUM     ) J   GROUP BY J.TENANT_ID, J.TABLET_ID, J.COMPACTION_SCN   HAVING MIN(J.DATA_CHECKSUM) != MAX(J.DATA_CHECKSUM)          OR MIN(J.ROW_COUNT) != MAX(J.ROW_COUNT)          OR MIN(J.B_COLUMN_CHECKSUMS) != MAX(J.B_COLUMN_CHECKSUMS)   )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT A.LS_ID,            A.STATUS,            C.ZONE_PRIORITY AS PRIMARY_ZONE,            A.UNIT_GROUP_ID,            A.LS_GROUP_ID,             /* SYS LS's CREATE_SCN always is NULL, it means nothing */            (CASE A.LS_ID                 WHEN 1 THEN NULL                 ELSE B.CREATE_SCN            END) AS CREATE_SCN,             /* show NULL if not dropped */            (CASE B.DROP_SCN                 WHEN 1 THEN NULL                 ELSE B.DROP_SCN            END) AS DROP_SCN,             /* SYS tenant and Meta tenant always show NULL */            (CASE                 WHEN A.TENANT_ID = 1 THEN NULL                 WHEN (A.TENANT_ID & 0x1) = 1 THEN NULL                 ELSE B.SYNC_SCN             END) AS SYNC_SCN,             /* SYS tenant and Meta tenant always show NULL */            (CASE                 WHEN A.TENANT_ID = 1 THEN NULL                 WHEN (A.TENANT_ID & 0x1) = 1 THEN NULL                 ELSE B.READABLE_SCN             END) AS READABLE_SCN,             FLAG     FROM OCEANBASE.__ALL_VIRTUAL_LS_STATUS AS A          JOIN OCEANBASE.__ALL_VIRTUAL_LS_RECOVERY_STAT AS B          JOIN OCEANBASE.__ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO AS C               ON A.TENANT_ID = B.TENANT_ID AND A.LS_ID = B.LS_ID               AND A.TENANT_ID = C.TENANT_ID AND A.LS_ID = C.LS_ID     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::cdb_ob_ls_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_LS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_LS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT A.TENANT_ID,            A.LS_ID,            A.STATUS,            C.ZONE_PRIORITY AS PRIMARY_ZONE,            A.UNIT_GROUP_ID,            A.LS_GROUP_ID,             /* SYS LS's CREATE_SCN always is NULL, it means nothing */            (CASE A.LS_ID                 WHEN 1 THEN NULL                 ELSE B.CREATE_SCN            END) AS CREATE_SCN,             /* show NULL if not dropped */            (CASE B.DROP_SCN                 WHEN 1 THEN NULL                 ELSE B.DROP_SCN            END) AS DROP_SCN,             /* SYS tenant and Meta tenant always show NULL */            (CASE                 WHEN A.TENANT_ID = 1 THEN NULL                 WHEN (A.TENANT_ID & 0x1) = 1 THEN NULL                 ELSE B.SYNC_SCN             END) AS SYNC_SCN,             /* SYS tenant and Meta tenant always show NULL */            (CASE                 WHEN A.TENANT_ID = 1 THEN NULL                 WHEN (A.TENANT_ID & 0x1) = 1 THEN NULL                 ELSE B.READABLE_SCN             END) AS READABLE_SCN,             FLAG     FROM OCEANBASE.__ALL_VIRTUAL_LS_STATUS AS A          JOIN OCEANBASE.__ALL_VIRTUAL_LS_RECOVERY_STAT AS B          JOIN OCEANBASE.__ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO AS C               ON A.TENANT_ID = B.TENANT_ID AND A.LS_ID = B.LS_ID               AND A.TENANT_ID = C.TENANT_ID AND A.LS_ID = C.LS_ID   )__"))) {
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

int ObInnerTableSchema::dba_ob_table_locations_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLE_LOCATIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLE_LOCATIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT     D.DATABASE_NAME,     A.TABLE_NAME,     A.TABLE_ID,      CASE WHEN A.TABLE_TYPE IN (0) THEN 'SYSTEM TABLE'          WHEN A.TABLE_TYPE IN (3,6,8,9) THEN 'USER TABLE'          WHEN A.TABLE_TYPE IN (5) THEN 'INDEX'          WHEN A.TABLE_TYPE IN (12,13) THEN 'LOB AUX TABLE'          WHEN A.TABLE_TYPE IN (15) THEN 'MATERIALIZED VIEW LOG'          ELSE NULL     END AS TABLE_TYPE,      A.PARTITION_NAME,     A.SUBPARTITION_NAME,      /* INDEX_NAME is valid when table is index */     CASE WHEN A.TABLE_TYPE != 5 THEN NULL          WHEN D.DATABASE_NAME != '__recyclebin'               THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))          ELSE TABLE_NAME     END AS INDEX_NAME,      CASE WHEN DATA_TABLE_ID = 0 THEN NULL          ELSE DATA_TABLE_ID     END AS DATA_TABLE_ID,      A.TABLET_ID,     C.LS_ID,     C.ZONE,     C.SVR_IP AS SVR_IP,     C.SVR_PORT AS SVR_PORT,     C.ROLE,     C.REPLICA_TYPE,     CASE WHEN A.DUPLICATE_SCOPE = 1 THEN 'CLUSTER'          ELSE 'NONE'     END AS DUPLICATE_SCOPE,     A.OBJECT_ID,     TG.TABLEGROUP_NAME,     TG.TABLEGROUP_ID,     TG.SHARDING FROM (       SELECT DATABASE_ID,              TABLE_NAME,              TABLE_ID,              'NULL' AS PARTITION_NAME,              'NULL' AS SUBPARTITION_NAME,              TABLE_ID AS OBJECT_ID,              TABLET_ID AS TABLET_ID,              TABLE_TYPE,              DATA_TABLE_ID,              DUPLICATE_SCOPE,              TABLEGROUP_ID       FROM OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE       WHERE TABLET_ID != 0 AND TENANT_ID = EFFECTIVE_TENANT_ID()        UNION ALL        SELECT       DATABASE_ID,       TABLE_NAME,       TABLE_ID,       'NULL' AS PARTITION_NAME,       'NULL' AS SUBPARTITION_NAME,       TABLE_ID AS OBJECT_ID,       TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID,       DUPLICATE_SCOPE,       TABLEGROUP_ID       FROM OCEANBASE.__ALL_TABLE       WHERE TABLET_ID != 0 AND PART_LEVEL = 0 AND TENANT_ID = 0        UNION ALL        SELECT       T.DATABASE_ID AS DATABASE_ID,       T.TABLE_NAME AS TABLE_NAME,       T.TABLE_ID AS TABLE_ID,       P.PART_NAME AS PARTITION_NAME,       'NULL' AS SUBPARTITION_NAME,       P.PART_ID AS OBJECT_ID,       P.TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID,       DUPLICATE_SCOPE,       TABLEGROUP_ID       FROM OCEANBASE.__ALL_TABLE T JOIN OCEANBASE.__ALL_PART P            ON T.TABLE_ID = P.TABLE_ID AND T.TENANT_ID = P.TENANT_ID       WHERE T.PART_LEVEL = 1 AND T.TENANT_ID = 0        UNION ALL        SELECT       T.DATABASE_ID AS DATABASE_ID,       T.TABLE_NAME AS TABLE_NAME,       T.TABLE_ID AS TABLE_ID,       P.PART_NAME AS PARTITION_NAME,       Q.SUB_PART_NAME AS SUBPARTITION_NAME,       Q.SUB_PART_ID AS OBJECT_ID,       Q.TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID,       DUPLICATE_SCOPE,       TABLEGROUP_ID       FROM OCEANBASE.__ALL_TABLE T, OCEANBASE.__ALL_PART P,OCEANBASE.__ALL_SUB_PART Q       WHERE T.TABLE_ID =P.TABLE_ID AND P.TABLE_ID=Q.TABLE_ID AND P.PART_ID = Q.PART_ID       AND T.TENANT_ID = P.TENANT_ID AND P.TENANT_ID = Q.TENANT_ID AND T.PART_LEVEL = 2       AND T.TENANT_ID = 0     ) A     JOIN OCEANBASE.DBA_OB_TABLET_TO_LS B ON A.TABLET_ID = B.TABLET_ID     JOIN OCEANBASE.DBA_OB_LS_LOCATIONS C ON B.LS_ID = C.LS_ID     JOIN OCEANBASE.__ALL_DATABASE D ON A.DATABASE_ID = D.DATABASE_ID     LEFT JOIN OCEANBASE.__ALL_TABLEGROUP TG ON A.TABLEGROUP_ID = TG.TABLEGROUP_ID     WHERE D.TENANT_ID = 0     ORDER BY A.TABLE_ID, A.TABLET_ID, C.ZONE, SVR_IP, SVR_PORT   )__"))) {
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

int ObInnerTableSchema::cdb_ob_table_locations_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_TABLE_LOCATIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_TABLE_LOCATIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT     A.TENANT_ID,     D.DATABASE_NAME,     A.TABLE_NAME,     A.TABLE_ID,      CASE WHEN A.TABLE_TYPE IN (0) THEN 'SYSTEM TABLE'          WHEN A.TABLE_TYPE IN (3,6,8,9) THEN 'USER TABLE'          WHEN A.TABLE_TYPE IN (5) THEN 'INDEX'          WHEN A.TABLE_TYPE IN (12,13) THEN 'LOB AUX TABLE'          WHEN A.TABLE_TYPE IN (15) THEN 'MATERIALIZED VIEW LOG'          ELSE NULL     END AS TABLE_TYPE,      A.PARTITION_NAME,     A.SUBPARTITION_NAME,      /* INDEX_NAME is valid when table is index */     CASE WHEN A.TABLE_TYPE != 5 THEN NULL          WHEN D.DATABASE_NAME != '__recyclebin'               THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))          ELSE TABLE_NAME     END AS INDEX_NAME,      CASE WHEN DATA_TABLE_ID = 0 THEN NULL          ELSE DATA_TABLE_ID     END AS DATA_TABLE_ID,      A.TABLET_ID,     C.LS_ID,     C.ZONE,     C.SVR_IP AS SVR_IP,     C.SVR_PORT AS SVR_PORT,     C.ROLE,     C.REPLICA_TYPE,     CASE WHEN A.DUPLICATE_SCOPE = 1 THEN 'CLUSTER'          ELSE 'NONE'     END AS DUPLICATE_SCOPE,     A.OBJECT_ID,     TG.TABLEGROUP_NAME,     TG.TABLEGROUP_ID,     TG.SHARDING FROM (       SELECT TENANT_ID,              DATABASE_ID,              TABLE_NAME,              TABLE_ID,              'NULL' AS PARTITION_NAME,              'NULL' AS SUBPARTITION_NAME,              TABLE_ID AS OBJECT_ID,              TABLET_ID AS TABLET_ID,              TABLE_TYPE,              DATA_TABLE_ID,              DUPLICATE_SCOPE,              TABLEGROUP_ID       FROM OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE       WHERE TABLET_ID != 0        UNION ALL        SELECT       TENANT_ID,       DATABASE_ID,       TABLE_NAME,       TABLE_ID,       'NULL' AS PARTITION_NAME,       'NULL' AS SUBPARTITION_NAME,       TABLE_ID AS OBJECT_ID,       TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID,       DUPLICATE_SCOPE,       TABLEGROUP_ID       FROM OCEANBASE.__ALL_VIRTUAL_TABLE       WHERE TABLET_ID != 0 AND PART_LEVEL = 0        UNION ALL        SELECT       P.TENANT_ID AS TENANT_ID,       T.DATABASE_ID AS DATABASE_ID,       T.TABLE_NAME AS TABLE_NAME,       T.TABLE_ID AS TABLE_ID,       P.PART_NAME AS PARTITION_NAME,       'NULL' AS SUBPARTITION_NAME,       P.PART_ID AS OBJECT_ID,       P.TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID,       DUPLICATE_SCOPE,       TABLEGROUP_ID       FROM OCEANBASE.__ALL_VIRTUAL_TABLE T JOIN OCEANBASE.__ALL_VIRTUAL_PART P ON T.TABLE_ID = P.TABLE_ID       WHERE T.TENANT_ID = P.TENANT_ID AND T.PART_LEVEL = 1        UNION ALL        SELECT       T.TENANT_ID AS TENANT_ID,       T.DATABASE_ID AS DATABASE_ID,       T.TABLE_NAME AS TABLE_NAME,       T.TABLE_ID AS TABLE_ID,       P.PART_NAME AS PARTITION_NAME,       Q.SUB_PART_NAME AS SUBPARTITION_NAME,       Q.SUB_PART_ID AS OBJECT_ID,       Q.TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID,       DUPLICATE_SCOPE,       TABLEGROUP_ID       FROM OCEANBASE.__ALL_VIRTUAL_TABLE T, OCEANBASE.__ALL_VIRTUAL_PART P,OCEANBASE.__ALL_VIRTUAL_SUB_PART Q       WHERE T.TABLE_ID =P.TABLE_ID AND P.TABLE_ID=Q.TABLE_ID AND P.PART_ID =Q.PART_ID       AND T.TENANT_ID = P.TENANT_ID AND P.TENANT_ID = Q.TENANT_ID AND T.PART_LEVEL = 2     ) A     JOIN OCEANBASE.CDB_OB_TABLET_TO_LS B ON A.TABLET_ID = B.TABLET_ID AND A.TENANT_ID = B.TENANT_ID     JOIN OCEANBASE.CDB_OB_LS_LOCATIONS C ON B.LS_ID = C.LS_ID AND A.TENANT_ID = C.TENANT_ID     JOIN OCEANBASE.__ALL_VIRTUAL_DATABASE D ON A.TENANT_ID = D.TENANT_ID AND A.DATABASE_ID = D.DATABASE_ID     LEFT JOIN OCEANBASE.__ALL_VIRTUAL_TABLEGROUP TG ON A.TABLEGROUP_ID = TG.TABLEGROUP_ID AND A.TENANT_ID = TG.TENANT_ID ORDER BY A.TENANT_ID, A.TABLE_ID, A.TABLET_ID, C.ZONE, SVR_IP, SVR_PORT   )__"))) {
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

int ObInnerTableSchema::dba_ob_server_event_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SERVER_EVENT_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SERVER_EVENT_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   gmt_create AS `TIMESTAMP`,   SVR_IP,   SVR_PORT,   MODULE,   EVENT,   NAME1, VALUE1,   NAME2, VALUE2,   NAME3, VALUE3,   NAME4, VALUE4,   NAME5, VALUE5,   NAME6, VALUE6,   EXTRA_INFO FROM oceanbase.__all_server_event_history   )__"))) {
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

int ObInnerTableSchema::cdb_ob_freeze_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_FREEZE_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_FREEZE_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          FROZEN_SCN,          CLUSTER_VERSION,          SCHEMA_VERSION,          GMT_CREATE,          GMT_MODIFIED   FROM OCEANBASE.__ALL_VIRTUAL_FREEZE_INFO   )__"))) {
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

int ObInnerTableSchema::dba_ob_freeze_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_FREEZE_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_FREEZE_INFO_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT FROZEN_SCN,          CLUSTER_VERSION,          SCHEMA_VERSION,          GMT_CREATE,          GMT_MODIFIED   FROM OCEANBASE.__ALL_FREEZE_INFO   )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_replica_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_REPLICA_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_REPLICA_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT LS_ID,          TASK_TYPE,          TASK_ID,          TASK_STATUS,          CAST(CASE PRIORITY               WHEN 0 THEN 'HIGH'               WHEN 1 THEN 'LOW'               ELSE NULL END AS CHAR(5)) AS PRIORITY,          TARGET_REPLICA_SVR_IP,          TARGET_REPLICA_SVR_PORT,          TARGET_PAXOS_REPLICA_NUMBER,          TARGET_REPLICA_TYPE,          (CASE SOURCE_REPLICA_SVR_IP               WHEN "" THEN NULL               ELSE SOURCE_REPLICA_SVR_IP END) AS SOURCE_REPLICA_SVR_IP,          SOURCE_REPLICA_SVR_PORT,          SOURCE_PAXOS_REPLICA_NUMBER,          (CASE SOURCE_REPLICA_TYPE               WHEN "" THEN NULL               ELSE SOURCE_REPLICA_TYPE END) AS SOURCE_REPLICA_TYPE,          TASK_EXEC_SVR_IP,          TASK_EXEC_SVR_PORT,          CAST(GMT_CREATE AS DATETIME) AS CREATE_TIME,          CAST(SCHEDULE_TIME AS DATETIME) AS START_TIME,          CAST(GMT_MODIFIED AS DATETIME) AS MODIFY_TIME,          COMMENT   FROM OCEANBASE.__ALL_VIRTUAL_LS_REPLICA_TASK   WHERE     TENANT_ID = EFFECTIVE_TENANT_ID()   )   )__"))) {
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

int ObInnerTableSchema::cdb_ob_ls_replica_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_LS_REPLICA_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_LS_REPLICA_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT TENANT_ID,          LS_ID,          TASK_TYPE,          TASK_ID,          TASK_STATUS,          CAST(CASE PRIORITY               WHEN 0 THEN 'HIGH'               WHEN 1 THEN 'LOW'               ELSE NULL END AS CHAR(5)) AS PRIORITY,          TARGET_REPLICA_SVR_IP,          TARGET_REPLICA_SVR_PORT,          TARGET_PAXOS_REPLICA_NUMBER,          TARGET_REPLICA_TYPE,          (CASE SOURCE_REPLICA_SVR_IP               WHEN "" THEN NULL               ELSE SOURCE_REPLICA_SVR_IP END) AS SOURCE_REPLICA_SVR_IP,          SOURCE_REPLICA_SVR_PORT,          SOURCE_PAXOS_REPLICA_NUMBER,          (CASE SOURCE_REPLICA_TYPE               WHEN "" THEN NULL               ELSE SOURCE_REPLICA_TYPE END) AS SOURCE_REPLICA_TYPE,          TASK_EXEC_SVR_IP,          TASK_EXEC_SVR_PORT,          CAST(GMT_CREATE AS DATETIME) AS CREATE_TIME,          CAST(SCHEDULE_TIME AS DATETIME) AS START_TIME,          CAST(GMT_MODIFIED AS DATETIME) AS MODIFY_TIME,          COMMENT   FROM OCEANBASE.__ALL_VIRTUAL_LS_REPLICA_TASK   )   )__"))) {
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

int ObInnerTableSchema::v_ob_ls_replica_task_plan_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_LS_REPLICA_TASK_PLAN_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_LS_REPLICA_TASK_PLAN_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   (   SELECT TENANT_ID,          LS_ID,          TASK_TYPE,          CAST(CASE PRIORITY               WHEN 0 THEN 'HIGH'               WHEN 1 THEN 'LOW'               ELSE NULL END AS CHAR(5)) AS PRIORITY,          TARGET_REPLICA_SVR_IP,          TARGET_REPLICA_SVR_PORT,          TARGET_PAXOS_REPLICA_NUMBER,          TARGET_REPLICA_TYPE,          (CASE SOURCE_REPLICA_SVR_IP               WHEN "" THEN NULL               ELSE SOURCE_REPLICA_SVR_IP END) AS SOURCE_REPLICA_SVR_IP,          SOURCE_REPLICA_SVR_PORT,          SOURCE_PAXOS_REPLICA_NUMBER,          (CASE SOURCE_REPLICA_TYPE               WHEN "" THEN NULL               ELSE SOURCE_REPLICA_TYPE END) AS SOURCE_REPLICA_TYPE,          TASK_EXEC_SVR_IP,          TASK_EXEC_SVR_PORT,          COMMENT   FROM OCEANBASE.__ALL_VIRTUAL_LS_REPLICA_TASK_PLAN   )   )__"))) {
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

int ObInnerTableSchema::dba_ob_auto_increment_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_AUTO_INCREMENT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_AUTO_INCREMENT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(GMT_CREATE AS DATETIME(6)) AS CREATE_TIME,          CAST(GMT_MODIFIED AS DATETIME(6)) AS MODIFY_TIME,          CAST(SEQUENCE_KEY AS SIGNED) AS AUTO_INCREMENT_KEY,          CAST(COLUMN_ID AS SIGNED) AS COLUMN_ID,          CAST(SEQUENCE_VALUE AS UNSIGNED) AS AUTO_INCREMENT_VALUE,          CAST(SYNC_VALUE AS UNSIGNED) AS SYNC_VALUE   FROM OCEANBASE.__ALL_AUTO_INCREMENT   )__"))) {
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

int ObInnerTableSchema::cdb_ob_auto_increment_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_AUTO_INCREMENT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_AUTO_INCREMENT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(GMT_CREATE AS DATETIME(6)) AS CREATE_TIME,          CAST(GMT_MODIFIED AS DATETIME(6)) AS MODIFY_TIME,          CAST(TENANT_ID AS SIGNED) AS TENANT_ID,          CAST(SEQUENCE_KEY AS SIGNED) AS AUTO_INCREMENT_KEY,          CAST(COLUMN_ID AS SIGNED) AS COLUMN_ID,          CAST(SEQUENCE_VALUE AS UNSIGNED) AS AUTO_INCREMENT_VALUE,          CAST(SYNC_VALUE AS UNSIGNED) AS SYNC_VALUE   FROM OCEANBASE.__ALL_VIRTUAL_AUTO_INCREMENT   )__"))) {
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

int ObInnerTableSchema::dba_sequences_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SEQUENCES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SEQUENCES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(C.DATABASE_NAME AS CHAR(128)) AS SEQUENCE_OWNER,       CAST(A.SEQUENCE_NAME AS CHAR(128)) AS SEQUENCE_NAME,       CAST(A.MIN_VALUE AS NUMBER(28, 0)) AS MIN_VALUE,       CAST(A.MAX_VALUE AS NUMBER(28, 0)) AS MAX_VALUE,       CAST(A.INCREMENT_BY AS NUMBER(28, 0)) AS INCREMENT_BY,       CAST(CASE A.CYCLE_FLAG WHEN 1 THEN 'Y'                              WHEN 0 THEN 'N'                              ELSE NULL END AS CHAR(1)) AS CYCLE_FLAG,       CAST(CASE A.ORDER_FLAG WHEN 1 THEN 'Y'                              WHEN 0 THEN 'N'                              ELSE NULL END AS CHAR(1)) AS ORDER_FLAG,       CAST(A.CACHE_SIZE AS NUMBER(28, 0)) AS CACHE_SIZE,       CAST(COALESCE(B.NEXT_VALUE,A.START_WITH) AS NUMBER(38,0)) AS LAST_NUMBER     FROM       OCEANBASE.__ALL_SEQUENCE_OBJECT A     INNER JOIN       OCEANBASE.__ALL_DATABASE C     ON       A.TENANT_ID = C.TENANT_ID AND A.DATABASE_ID = C.DATABASE_ID     LEFT JOIN       OCEANBASE.__ALL_SEQUENCE_VALUE B     ON       A.SEQUENCE_ID = B.SEQUENCE_ID )__"))) {
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

int ObInnerTableSchema::dba_scheduler_windows_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SCHEDULER_WINDOWS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SCHEDULER_WINDOWS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(T.POWNER AS CHAR(128)) AS OWNER,     CAST(T.JOB_NAME AS CHAR(128)) AS WINDOW_NAME,     CAST(NULL AS CHAR(128)) AS RESOURCE_PLAN,     CAST(NULL AS CHAR(4000)) AS SCHEDULE_OWNER,     CAST(NULL AS CHAR(4000)) AS SCHEDULE_NAME,     CAST(NULL AS CHAR(8)) AS SCHEDULE_TYPE,     CAST(T.START_DATE AS DATETIME(6)) AS START_DATE,     CAST(T.REPEAT_INTERVAL AS CHAR(4000)) AS REPEAT_INTERVAL,     CAST(T.END_DATE AS DATETIME(6)) AS END_DATE,     CAST(T.MAX_RUN_DURATION AS SIGNED) AS DURATION,     CAST(NULL AS CHAR(4)) AS WINDOW_PRIORITY,     CAST(T.NEXT_DATE AS DATETIME(6)) AS NEXT_RUN_DATE,     CAST(T.LAST_DATE AS DATETIME(6)) AS LAST_START_DATE,     CAST(T.ENABLED AS CHAR(5)) AS ENABLED,     CAST(NULL AS CHAR(5)) AS ACTIVE,     CAST(NULL AS DATETIME(6)) AS MANUAL_OPEN_TIME,     CAST(NULL AS SIGNED) AS MANUAL_DURATION,     CAST(T.COMMENTS AS CHAR(4000)) AS COMMENTS   FROM oceanbase.__all_tenant_scheduler_job T WHERE T.JOB > 0 and T.JOB_NAME in ('MONDAY_WINDOW',     'TUESDAY_WINDOW', 'WEDNESDAY_WINDOW', 'THURSDAY_WINDOW', 'FRIDAY_WINDOW', 'SATURDAY_WINDOW', 'SUNDAY_WINDOW')   )__"))) {
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

int ObInnerTableSchema::dba_ob_users_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_USERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_USERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT USER_NAME,           HOST,           PASSWD,           INFO,           (CASE WHEN PRIV_ALTER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER,           (CASE WHEN PRIV_CREATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE,           (CASE WHEN PRIV_DELETE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DELETE,           (CASE WHEN PRIV_DROP = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DROP,           (CASE WHEN PRIV_GRANT_OPTION = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_GRANT_OPTION,           (CASE WHEN PRIV_INSERT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INSERT,           (CASE WHEN PRIV_UPDATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_UPDATE,           (CASE WHEN PRIV_SELECT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SELECT,           (CASE WHEN PRIV_INDEX = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INDEX,           (CASE WHEN PRIV_CREATE_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_VIEW,           (CASE WHEN PRIV_SHOW_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SHOW_VIEW,           (CASE WHEN PRIV_SHOW_DB = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SHOW_DB,           (CASE WHEN PRIV_CREATE_USER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_USER,           (CASE WHEN PRIV_SUPER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SUPER,           (CASE WHEN IS_LOCKED = 0 THEN 'NO' ELSE 'YES' END) AS IS_LOCKED,           (CASE WHEN PRIV_PROCESS = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_PROCESS,           (CASE WHEN PRIV_CREATE_SYNONYM = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_SYNONYM,           SSL_TYPE,           SSL_CIPHER,           X509_ISSUER,           X509_SUBJECT,           (CASE WHEN TYPE = 0 THEN 'USER' ELSE 'ROLE' END) AS TYPE,           PROFILE_ID,           PASSWORD_LAST_CHANGED,           (CASE WHEN PRIV_FILE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_FILE,           (CASE WHEN PRIV_ALTER_TENANT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER_TENANT,           (CASE WHEN PRIV_ALTER_SYSTEM = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER_SYSTEM,           (CASE WHEN PRIV_CREATE_RESOURCE_POOL = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_RESOURCE_POOL,           (CASE WHEN PRIV_CREATE_RESOURCE_UNIT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_RESOURCE_UNIT,           MAX_CONNECTIONS,           MAX_USER_CONNECTIONS,           (CASE WHEN PRIV_REPL_SLAVE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_REPL_SLAVE,           (CASE WHEN PRIV_REPL_CLIENT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_REPL_CLIENT,           (CASE WHEN PRIV_DROP_DATABASE_LINK = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DROP_DATABASE_LINK,           (CASE WHEN PRIV_CREATE_DATABASE_LINK = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_DATABASE_LINK,           (CASE WHEN (PRIV_OTHERS & (1 << 0)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_EXECUTE,           (CASE WHEN (PRIV_OTHERS & (1 << 1)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_ALTER_ROUTINE,           (CASE WHEN (PRIV_OTHERS & (1 << 2)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_CREATE_ROUTINE,           (CASE WHEN (PRIV_OTHERS & (1 << 3)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_CREATE_TABLESPACE,           (CASE WHEN (PRIV_OTHERS & (1 << 4)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_SHUTDOWN,           (CASE WHEN (PRIV_OTHERS & (1 << 5)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_RELOAD   FROM OCEANBASE.__all_user;   )__"))) {
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

int ObInnerTableSchema::cdb_ob_users_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_USERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_USERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,           USER_NAME,           HOST,           PASSWD,           INFO,           (CASE WHEN PRIV_ALTER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER,           (CASE WHEN PRIV_CREATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE,           (CASE WHEN PRIV_DELETE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DELETE,           (CASE WHEN PRIV_DROP = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DROP,           (CASE WHEN PRIV_GRANT_OPTION = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_GRANT_OPTION,           (CASE WHEN PRIV_INSERT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INSERT,           (CASE WHEN PRIV_UPDATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_UPDATE,           (CASE WHEN PRIV_SELECT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SELECT,           (CASE WHEN PRIV_INDEX = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INDEX,           (CASE WHEN PRIV_CREATE_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_VIEW,           (CASE WHEN PRIV_SHOW_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SHOW_VIEW,           (CASE WHEN PRIV_SHOW_DB = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SHOW_DB,           (CASE WHEN PRIV_CREATE_USER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_USER,           (CASE WHEN PRIV_SUPER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SUPER,           (CASE WHEN IS_LOCKED = 0 THEN 'NO' ELSE 'YES' END) AS IS_LOCKED,           (CASE WHEN PRIV_PROCESS = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_PROCESS,           (CASE WHEN PRIV_CREATE_SYNONYM = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_SYNONYM,           SSL_TYPE,           SSL_CIPHER,           X509_ISSUER,           X509_SUBJECT,           (CASE WHEN TYPE = 0 THEN 'USER' ELSE 'ROLE' END) AS TYPE,           PROFILE_ID,           PASSWORD_LAST_CHANGED,           (CASE WHEN PRIV_FILE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_FILE,           (CASE WHEN PRIV_ALTER_TENANT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER_TENANT,           (CASE WHEN PRIV_ALTER_SYSTEM = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER_SYSTEM,           (CASE WHEN PRIV_CREATE_RESOURCE_POOL = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_RESOURCE_POOL,           (CASE WHEN PRIV_CREATE_RESOURCE_UNIT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_RESOURCE_UNIT,           MAX_CONNECTIONS,           MAX_USER_CONNECTIONS,           (CASE WHEN PRIV_REPL_SLAVE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_REPL_SLAVE,           (CASE WHEN PRIV_REPL_CLIENT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_REPL_CLIENT,           (CASE WHEN PRIV_DROP_DATABASE_LINK = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DROP_DATABASE_LINK,           (CASE WHEN PRIV_CREATE_DATABASE_LINK = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_DATABASE_LINK,           (CASE WHEN (PRIV_OTHERS & (1 << 0)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_EXECUTE,           (CASE WHEN (PRIV_OTHERS & (1 << 1)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_ALTER_ROUTINE,           (CASE WHEN (PRIV_OTHERS & (1 << 2)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_CREATE_ROUTINE,           (CASE WHEN (PRIV_OTHERS & (1 << 3)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_CREATE_TABLESPACE,           (CASE WHEN (PRIV_OTHERS & (1 << 4)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_SHUTDOWN,           (CASE WHEN (PRIV_OTHERS & (1 << 5)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_RELOAD   FROM OCEANBASE.__all_virtual_user;   )__"))) {
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

int ObInnerTableSchema::dba_ob_database_privilege_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_DATABASE_PRIVILEGE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_DATABASE_PRIVILEGE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   WITH DB_PRIV AS (     select A.tenant_id TENANT_ID,            A.user_id USER_ID,            A.database_name DATABASE_NAME,            A.priv_alter PRIV_ALTER,            A.priv_create PRIV_CREATE,            A.priv_delete PRIV_DELETE,            A.priv_drop PRIV_DROP,            A.priv_grant_option PRIV_GRANT_OPTION,            A.priv_insert PRIV_INSERT,            A.priv_update PRIV_UPDATE,            A.priv_select PRIV_SELECT,            A.priv_index PRIV_INDEX,            A.priv_create_view PRIV_CREATE_VIEW,            A.priv_show_view PRIV_SHOW_VIEW,            A.GMT_CREATE GMT_CREATE,            A.GMT_MODIFIED GMT_MODIFIED,            A.priv_others PRIV_OTHERS     from oceanbase.__all_database_privilege_history A,          (select tenant_id, user_id, database_name, max(schema_version) schema_version from oceanbase.__all_database_privilege_history group by tenant_id, user_id, database_name, database_name collate utf8mb4_bin) B     where A.tenant_id = B.tenant_id and A.user_id = B.user_id and A.database_name collate utf8mb4_bin = B.database_name collate utf8mb4_bin and A.schema_version = B.schema_version and A.is_deleted = 0   )   SELECT A.USER_ID USER_ID,           B.USER_NAME USERNAME,           A.DATABASE_NAME DATABASE_NAME,           A.GMT_CREATE GMT_CREATE,           A.GMT_MODIFIED GMT_MODIFIED,           (CASE WHEN A.PRIV_ALTER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER,           (CASE WHEN A.PRIV_CREATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE,           (CASE WHEN A.PRIV_DELETE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DELETE,           (CASE WHEN A.PRIV_DROP = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DROP,           (CASE WHEN A.PRIV_GRANT_OPTION = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_GRANT_OPTION,           (CASE WHEN A.PRIV_INSERT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INSERT,           (CASE WHEN A.PRIV_UPDATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_UPDATE,           (CASE WHEN A.PRIV_SELECT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SELECT,           (CASE WHEN A.PRIV_INDEX = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INDEX,           (CASE WHEN A.PRIV_CREATE_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_VIEW,           (CASE WHEN A.PRIV_SHOW_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SHOW_VIEW,           (CASE WHEN (A.PRIV_OTHERS & (1 << 0)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_EXECUTE,           (CASE WHEN (A.PRIV_OTHERS & (1 << 1)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_ALTER_ROUTINE,           (CASE WHEN (A.PRIV_OTHERS & (1 << 2)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_CREATE_ROUTINE   FROM DB_PRIV A INNER JOIN OCEANBASE.__all_user B         ON A.TENANT_ID = B.TENANT_ID AND A.USER_ID = B.USER_ID;   )__"))) {
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

int ObInnerTableSchema::cdb_ob_database_privilege_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_DATABASE_PRIVILEGE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_DATABASE_PRIVILEGE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   WITH DB_PRIV AS (     select A.tenant_id TENANT_ID,            A.user_id USER_ID,            A.database_name DATABASE_NAME,            A.priv_alter PRIV_ALTER,            A.priv_create PRIV_CREATE,            A.priv_delete PRIV_DELETE,            A.priv_drop PRIV_DROP,            A.priv_grant_option PRIV_GRANT_OPTION,            A.priv_insert PRIV_INSERT,            A.priv_update PRIV_UPDATE,            A.priv_select PRIV_SELECT,            A.priv_index PRIV_INDEX,            A.priv_create_view PRIV_CREATE_VIEW,            A.priv_show_view PRIV_SHOW_VIEW,            A.GMT_CREATE GMT_CREATE,            A.GMT_MODIFIED GMT_MODIFIED,            A.PRIV_OTHERS PRIV_OTHERS     from oceanbase.__all_virtual_database_privilege_history A,          (select tenant_id, user_id, database_name, max(schema_version) schema_version from oceanbase.__all_virtual_database_privilege_history group by tenant_id, user_id, database_name, database_name collate utf8mb4_bin) B     where A.tenant_id = B.tenant_id and A.user_id = B.user_id and A.database_name collate utf8mb4_bin = B.database_name collate utf8mb4_bin and A.schema_version = B.schema_version and A.is_deleted = 0   )   SELECT A.TENANT_ID,           A.USER_ID USER_ID,           B.USER_NAME USERNAME,           A.DATABASE_NAME DATABASE_NAME,           A.GMT_CREATE GMT_CREATE,           A.GMT_MODIFIED GMT_MODIFIED,           (CASE WHEN A.PRIV_ALTER = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_ALTER,           (CASE WHEN A.PRIV_CREATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE,           (CASE WHEN A.PRIV_DELETE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DELETE,           (CASE WHEN A.PRIV_DROP = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_DROP,           (CASE WHEN A.PRIV_GRANT_OPTION = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_GRANT_OPTION,           (CASE WHEN A.PRIV_INSERT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INSERT,           (CASE WHEN A.PRIV_UPDATE = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_UPDATE,           (CASE WHEN A.PRIV_SELECT = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SELECT,           (CASE WHEN A.PRIV_INDEX = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_INDEX,           (CASE WHEN A.PRIV_CREATE_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_CREATE_VIEW,           (CASE WHEN A.PRIV_SHOW_VIEW = 0 THEN 'NO' ELSE 'YES' END) AS PRIV_SHOW_VIEW,           (CASE WHEN (A.PRIV_OTHERS & (1 << 0)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_EXECUTE,           (CASE WHEN (A.PRIV_OTHERS & (1 << 1)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_ALTER_ROUTINE,           (CASE WHEN (A.PRIV_OTHERS & (1 << 2)) != 0 THEN 'YES' ELSE 'NO' END) AS PRIV_CREATE_ROUTINE   FROM DB_PRIV A INNER JOIN OCEANBASE.__all_virtual_user B         ON A.USER_ID = B.USER_ID AND A.TENANT_ID = B.TENANT_ID;   )__"))) {
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

int ObInnerTableSchema::dba_ob_user_defined_rules_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_USER_DEFINED_RULES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_USER_DEFINED_RULES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       CAST(T.DB_NAME AS CHAR(128)) AS DB_NAME,       CAST(T.RULE_NAME AS CHAR(256)) AS RULE_NAME,       CAST(T.RULE_ID AS SIGNED) AS RULE_ID,       PATTERN,       REPLACEMENT,       NORMALIZED_PATTERN,       CAST(CASE STATUS WHEN 1 THEN 'ENABLE'                       WHEN 2 THEN 'DISABLE'                       ELSE NULL END AS CHAR(10)) AS STATUS,       CAST(T.VERSION AS SIGNED) AS VERSION,       CAST(T.PATTERN_DIGEST AS UNSIGNED) AS PATTERN_DIGEST     FROM       oceanbase.__all_tenant_rewrite_rules T     WHERE T.STATUS != 3 )__"))) {
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

int ObInnerTableSchema::gv_ob_sql_plan_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_SQL_PLAN_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_SQL_PLAN_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT                         SVR_IP,                         SVR_PORT,                         PLAN_ID,                         SQL_ID,                         DB_ID,                         PLAN_HASH,                         GMT_CREATE,                         OPERATOR,                         OBJECT_NODE,                         OBJECT_ID,                         OBJECT_OWNER,                         OBJECT_NAME,                         OBJECT_ALIAS,                         OBJECT_TYPE,                         OPTIMIZER,                         ID,                         PARENT_ID,                         DEPTH,                         POSITION,                         COST,                         REAL_COST,                         CARDINALITY,                         REAL_CARDINALITY,                         IO_COST,                         CPU_COST,                         BYTES,                         ROWSET,                         OTHER_TAG,                         PARTITION_START,                         OTHER,                         ACCESS_PREDICATES,                         FILTER_PREDICATES,                         STARTUP_PREDICATES,                         PROJECTION,                         SPECIAL_PREDICATES,                         QBLOCK_NAME,                         REMARKS,                         OTHER_XML                     FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN                     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::v_ob_sql_plan_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_SQL_PLAN_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_SQL_PLAN_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT                       SQL_ID,                       DB_ID,                       PLAN_HASH,                       PLAN_ID,                       GMT_CREATE,                       OPERATOR,                       OBJECT_NODE,                       OBJECT_ID,                       OBJECT_OWNER,                       OBJECT_NAME,                       OBJECT_ALIAS,                       OBJECT_TYPE,                       OPTIMIZER,                       ID,                       PARENT_ID,                       DEPTH,                       POSITION,                       COST,                       REAL_COST,                       CARDINALITY,                       REAL_CARDINALITY,                       IO_COST,                       CPU_COST,                       BYTES,                       ROWSET,                       OTHER_TAG,                       PARTITION_START,                       OTHER,                       ACCESS_PREDICATES,                       FILTER_PREDICATES,                       STARTUP_PREDICATES,                       PROJECTION,                       SPECIAL_PREDICATES,                       QBLOCK_NAME,                       REMARKS,                       OTHER_XML     FROM OCEANBASE.GV$OB_SQL_PLAN     WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::dba_ob_cluster_event_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_CLUSTER_EVENT_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_CLUSTER_EVENT_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   gmt_create AS `TIMESTAMP`,   CAST(MODULE AS CHAR(256)) MODULE,   CAST(EVENT AS CHAR(256)) EVENT,   CAST(NAME1 AS CHAR(256)) NAME1,   CAST(VALUE1 AS CHAR(4096)) VALUE1,   CAST(NAME2 AS CHAR(256)) NAME2,   CAST(VALUE2 AS CHAR(4096)) VALUE2,   CAST(NAME3 AS CHAR(256)) NAME3,   CAST(VALUE3 AS CHAR(4096)) VALUE3,   CAST(NAME4 AS CHAR(256)) NAME4,   CAST(VALUE4 AS CHAR(4096)) VALUE4,   CAST(NAME5 AS CHAR(256)) NAME5,   CAST(VALUE5 AS CHAR(4096)) VALUE5,   CAST(NAME6 AS CHAR(256)) NAME6,   CAST(VALUE6 AS CHAR(4096)) VALUE6,   CAST(EXTRA_INFO AS CHAR(4096)) EXTRA_INFO FROM oceanbase.__all_cluster_event_history )__"))) {
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

int ObInnerTableSchema::parameters_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_PARAMETERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_PARAMETERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(select CAST('def' AS CHAR(512)) AS SPECIFIC_CATALOG,                         CAST(d.database_name AS CHAR(128)) collate utf8mb4_name_case AS SPECIFIC_SCHEMA,                         CAST(r.routine_name AS CHAR(64)) AS SPECIFIC_NAME,                         CAST(rp.param_position AS signed) AS ORDINAL_POSITION,                         CAST(CASE rp.param_position WHEN 0 THEN NULL                           ELSE CASE rp.flag & 0x03                           WHEN 1 THEN 'IN'                           WHEN 2 THEN 'OUT'                           WHEN 3 THEN 'INOUT'                           ELSE NULL                           END                         END AS CHAR(5)) AS PARAMETER_MODE,                         CAST(rp.param_name AS CHAR(64)) AS PARAMETER_NAME,                         CAST(lower(case v.data_type_str                                    when 'TINYINT UNSIGNED' then 'TINYINT'                                    when 'SMALLINT UNSIGNED' then 'SMALLINT'                                    when 'MEDIUMINT UNSIGNED' then 'MEDIUMINT'                                    when 'INT UNSIGNED' then 'INT'                                    when 'BIGINT UNSIGNED' then 'BIGINT'                                    when 'FLOAT UNSIGNED' then 'FLOAT'                                    when 'DOUBLE UNSIGNED' then 'DOUBLE'                                    when 'DECIMAL UNSIGNED' then 'DECIMAL'                                    else v.data_type_str end) AS CHAR(64)) AS DATA_TYPE,                         CASE WHEN rp.param_type IN (22, 23, 27, 28, 29, 30) THEN CAST(rp.param_length AS SIGNED)                           ELSE CAST(NULL AS SIGNED)                         END AS CHARACTER_MAXIMUM_LENGTH,                         CASE WHEN rp.param_type IN (22, 23, 27, 28, 29, 30, 43, 44, 46)                           THEN CAST(                             rp.param_length * CASE rp.param_coll_type                             WHEN 63 THEN 1                             WHEN 249 THEN 4                             WHEN 248 THEN 4                             WHEN 87 THEN 2                             WHEN 28 THEN 2                             WHEN 55 THEN 4                             WHEN 54 THEN 4                             WHEN 101 THEN 2                             WHEN 46 THEN 4                             WHEN 45 THEN 4                             WHEN 224 THEN 4                             ELSE 1                             END                               AS SIGNED                           )                           ELSE CAST(NULL AS SIGNED)                         END AS CHARACTER_OCTET_LENGTH,                         CASE WHEN rp.param_type IN (1, 2, 3, 4, 5, 15, 16, 50)                           THEN CAST(rp.param_precision AS UNSIGNED)                           ELSE CAST(NULL AS UNSIGNED)                         END AS NUMERIC_PRECISION,                         CASE WHEN rp.param_type IN (15, 16, 50) THEN CAST(rp.param_scale AS SIGNED)                           WHEN rp.param_type IN (1, 2, 3, 4, 5, 11, 12, 13, 14) THEN CAST(0 AS SIGNED)                           ELSE CAST(NULL AS SIGNED)                         END AS NUMERIC_SCALE,                         CASE WHEN rp.param_type IN (17, 18, 20) THEN CAST(rp.param_scale AS UNSIGNED)                           ELSE CAST(NULL AS UNSIGNED)                         END AS DATETIME_PRECISION,                         CAST(CASE rp.param_charset                           WHEN 1 THEN 'binary'                           WHEN 2 THEN 'utf8mb4'                           WHEN 3 THEN 'gbk'                           WHEN 4 THEN 'utf16'                           WHEN 5 THEN 'gb18030'                           WHEN 6 THEN 'latin1'                           WHEN 7 THEN 'gb18030_2022'                           ELSE NULL                         END AS CHAR(64)) AS CHARACTER_SET_NAME,                         CAST(CASE rp.param_coll_type                           WHEN 45 THEN 'utf8mb4_general_ci'                           WHEN 46 THEN 'utf8mb4_bin'                           WHEN 63 THEN 'binary'                           ELSE NULL                         END AS CHAR(64)) AS COLLATION_NAME,                         CAST(CASE WHEN rp.param_type IN (1, 2, 3, 4, 5)                           THEN CONCAT(lower(v.data_type_str),'(',rp.param_precision,')')                           WHEN rp.param_type IN (15,16,50)                           THEN CONCAT(lower(v.data_type_str),'(',rp.param_precision, ',', rp.param_scale,')')                           WHEN rp.param_type IN (18, 20)                           THEN CONCAT(lower(v.data_type_str),'(', rp.param_scale, ')')                           WHEN rp.param_type IN (22, 23)                           THEN CONCAT(lower(v.data_type_str),'(', rp.param_length, ')')                           ELSE lower(v.data_type_str) END AS char(4194304)) AS DTD_IDENTIFIER,                         CAST(CASE WHEN r.routine_type = 1 THEN 'PROCEDURE'                           WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'                           ELSE NULL                         END AS CHAR(9)) AS ROUTINE_TYPE                       from                         oceanbase.__all_routine_param as rp                         join oceanbase.__all_routine as r on rp.subprogram_id = r.subprogram_id                         and rp.tenant_id = r.tenant_id                         and rp.routine_id = r.routine_id                         join oceanbase.__all_database as d on r.database_id = d.database_id                         left join oceanbase.__all_virtual_data_type v on rp.param_type = v.data_type                       WHERE                         rp.tenant_id = 0                         and in_recyclebin = 0                         and database_name != '__recyclebin'                       order by SPECIFIC_SCHEMA,                         SPECIFIC_NAME,                         ORDINAL_POSITION                       )__"))) {
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

int ObInnerTableSchema::table_privileges_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_TABLE_PRIVILEGES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TABLE_PRIVILEGES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   WITH DB_PRIV AS (     select A.tenant_id TENANT_ID,            A.user_id USER_ID,            A.database_name DATABASE_NAME,            A.priv_alter PRIV_ALTER,            A.priv_create PRIV_CREATE,            A.priv_delete PRIV_DELETE,            A.priv_drop PRIV_DROP,            A.priv_grant_option PRIV_GRANT_OPTION,            A.priv_insert PRIV_INSERT,            A.priv_update PRIV_UPDATE,            A.priv_select PRIV_SELECT,            A.priv_index PRIV_INDEX,            A.priv_create_view PRIV_CREATE_VIEW,            A.priv_show_view PRIV_SHOW_VIEW,            A.GMT_CREATE GMT_CREATE,            A.GMT_MODIFIED GMT_MODIFIED,            A.PRIV_OTHERS PRIV_OTHERS     from oceanbase.__all_database_privilege_history A,          (select tenant_id, user_id, database_name, max(schema_version) schema_version from oceanbase.__all_database_privilege_history group by tenant_id, user_id, database_name, database_name collate utf8mb4_bin) B     where A.tenant_id = B.tenant_id and A.user_id = B.user_id and A.database_name collate utf8mb4_bin = B.database_name collate utf8mb4_bin and A.schema_version = B.schema_version and A.is_deleted = 0   ),   TABLE_PRIV AS (     select A.tenant_id TENANT_ID,            A.user_id USER_ID,            A.database_name DATABASE_NAME,            A.table_name TABLE_NAME,            A.priv_alter PRIV_ALTER,            A.priv_create PRIV_CREATE,            A.priv_delete PRIV_DELETE,            A.priv_drop PRIV_DROP,            A.priv_grant_option PRIV_GRANT_OPTION,            A.priv_insert PRIV_INSERT,            A.priv_update PRIV_UPDATE,            A.priv_select PRIV_SELECT,            A.priv_index PRIV_INDEX,            A.priv_create_view PRIV_CREATE_VIEW,            A.priv_show_view PRIV_SHOW_VIEW,            A.PRIV_OTHERS PRIV_OTHERS     from oceanbase.__all_table_privilege_history A,          (select tenant_id, user_id, database_name, table_name, max(schema_version) schema_version from oceanbase.__all_table_privilege_history group by tenant_id, user_id, database_name, database_name collate utf8mb4_bin, table_name, table_name collate utf8mb4_bin) B     where A.tenant_id = B.tenant_id and A.user_id = B.user_id and A.database_name collate utf8mb4_bin = B.database_name collate utf8mb4_bin and A.schema_version = B.schema_version and A.table_name collate utf8mb4_bin = B.table_name collate utf8mb4_bin and A.is_deleted = 0   )   SELECT          CAST(CONCAT('''', V.USER_NAME, '''', '@', '''', V.HOST, '''') AS CHAR(81)) AS GRANTEE ,          CAST('def' AS CHAR(512)) AS TABLE_CATALOG ,          CAST(V.DATABASE_NAME AS CHAR(128)) collate utf8mb4_name_case AS TABLE_SCHEMA ,          CAST(V.TABLE_NAME AS CHAR(64)) collate utf8mb4_name_case AS TABLE_NAME,          CAST(V.PRIVILEGE_TYPE AS CHAR(64)) AS PRIVILEGE_TYPE ,          CAST(V.IS_GRANTABLE AS CHAR(3)) AS IS_GRANTABLE   FROM     (SELECT TP.DATABASE_NAME AS DATABASE_NAME,             TP.TABLE_NAME AS TABLE_NAME,             U.USER_NAME AS USER_NAME,             U.HOST AS HOST,             CASE                 WHEN V1.C1 = 1                      AND TP.PRIV_ALTER = 1 THEN 'ALTER'                 WHEN V1.C1 = 2                      AND TP.PRIV_CREATE = 1 THEN 'CREATE'                 WHEN V1.C1 = 4                      AND TP.PRIV_DELETE = 1 THEN 'DELETE'                 WHEN V1.C1 = 5                      AND TP.PRIV_DROP = 1 THEN 'DROP'                 WHEN V1.C1 = 7                      AND TP.PRIV_INSERT = 1 THEN 'INSERT'                 WHEN V1.C1 = 8                      AND TP.PRIV_UPDATE = 1 THEN 'UPDATE'                 WHEN V1.C1 = 9                      AND TP.PRIV_SELECT = 1 THEN 'SELECT'                 WHEN V1.C1 = 10                      AND TP.PRIV_INDEX = 1 THEN 'INDEX'                 WHEN V1.C1 = 11                      AND TP.PRIV_CREATE_VIEW = 1 THEN 'CREATE VIEW'                 WHEN V1.C1 = 12                      AND TP.PRIV_SHOW_VIEW = 1 THEN 'SHOW VIEW'                 ELSE NULL             END PRIVILEGE_TYPE ,             CASE                 WHEN TP.PRIV_GRANT_OPTION = 1 THEN 'YES'                 WHEN TP.PRIV_GRANT_OPTION = 0 THEN 'NO'             END IS_GRANTABLE      FROM TABLE_PRIV TP,                       oceanbase.__all_user U,        (SELECT 1 AS C1         UNION ALL SELECT 2 AS C1         UNION ALL SELECT 4 AS C1         UNION ALL SELECT 5 AS C1         UNION ALL SELECT 7 AS C1         UNION ALL SELECT 8 AS C1         UNION ALL SELECT 9 AS C1         UNION ALL SELECT 10 AS C1         UNION ALL SELECT 11 AS C1         UNION ALL SELECT 12 AS C1) V1,        (SELECT USER_ID         FROM oceanbase.__all_user         WHERE TENANT_ID = 0           AND CONCAT(USER_NAME, '@', HOST) = CURRENT_USER()) CURR      LEFT JOIN        (SELECT USER_ID         FROM DB_PRIV         WHERE TENANT_ID = 0           AND DATABASE_NAME = 'mysql'           AND PRIV_SELECT = 1) DB ON CURR.USER_ID = DB.USER_ID      WHERE TP.TENANT_ID = 0        AND TP.TENANT_ID = U.TENANT_ID        AND TP.USER_ID = U.USER_ID        AND (DB.USER_ID IS NOT NULL             OR 512 & CURRENT_USER_PRIV() = 512             OR TP.USER_ID = CURR.USER_ID)) V   WHERE V.PRIVILEGE_TYPE IS NOT NULL   )__"))) {
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

int ObInnerTableSchema::user_privileges_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_USER_PRIVILEGES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PRIVILEGES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(CONCAT('''', V.USER_NAME, '''', '@', '''', V.HOST, '''') AS CHAR(81)) AS GRANTEE ,          CAST('def' AS CHAR(512)) AS TABLE_CATALOG ,          CAST(V.PRIVILEGE_TYPE AS CHAR(64)) AS PRIVILEGE_TYPE ,          CAST(V.IS_GRANTABLE AS CHAR(3)) AS IS_GRANTABLE   FROM     (SELECT U.USER_NAME AS USER_NAME,             U.HOST AS HOST,             CASE                 WHEN V1.C1 = 1                      AND U.PRIV_ALTER = 1 THEN 'ALTER'                 WHEN V1.C1 = 2                      AND U.PRIV_CREATE = 1 THEN 'CREATE'                 WHEN V1.C1 = 3                      AND U.PRIV_CREATE_USER = 1 THEN 'CREATE USER'                 WHEN V1.C1 = 4                      AND U.PRIV_DELETE = 1 THEN 'DELETE'                 WHEN V1.C1 = 5                      AND U.PRIV_DROP = 1 THEN 'DROP'                 WHEN V1.C1 = 7                      AND U.PRIV_INSERT = 1 THEN 'INSERT'                 WHEN V1.C1 = 8                      AND U.PRIV_UPDATE = 1 THEN 'UPDATE'                 WHEN V1.C1 = 9                      AND U.PRIV_SELECT = 1 THEN 'SELECT'                 WHEN V1.C1 = 10                      AND U.PRIV_INDEX = 1 THEN 'INDEX'                 WHEN V1.C1 = 11                      AND U.PRIV_CREATE_VIEW = 1 THEN 'CREATE VIEW'                 WHEN V1.C1 = 12                      AND U.PRIV_SHOW_VIEW = 1 THEN 'SHOW VIEW'                 WHEN V1.C1 = 13                      AND U.PRIV_SHOW_DB = 1 THEN 'SHOW DATABASES'                 WHEN V1.C1 = 14                      AND U.PRIV_SUPER = 1 THEN 'SUPER'                 WHEN V1.C1 = 15                      AND U.PRIV_PROCESS = 1 THEN 'PROCESS'                 WHEN V1.C1 = 17                      AND U.PRIV_CREATE_SYNONYM = 1 THEN 'CREATE SYNONYM'                 WHEN V1.C1 = 23                      AND (U.PRIV_OTHERS & (1 << 0)) != 0 THEN 'EXECUTE'                 WHEN V1.C1 = 27                      AND U.PRIV_FILE = 1 THEN 'FILE'                 WHEN V1.C1 = 28                      AND U.PRIV_ALTER_TENANT = 1 THEN 'ALTER TENANT'                 WHEN V1.C1 = 29                      AND U.PRIV_ALTER_SYSTEM = 1 THEN 'ALTER SYSTEM'                 WHEN V1.C1 = 30                      AND U.PRIV_CREATE_RESOURCE_POOL = 1 THEN 'CREATE RESOURCE POOL'                 WHEN V1.C1 = 31                      AND U.PRIV_CREATE_RESOURCE_UNIT = 1 THEN 'CREATE RESOURCE UNIT'                 WHEN V1.C1 = 33                      AND U.PRIV_REPL_SLAVE = 1 THEN 'REPLICATION SLAVE'                 WHEN V1.C1 = 34                      AND U.PRIV_REPL_CLIENT = 1 THEN 'REPLICATION CLIENT'                 WHEN V1.C1 = 35                      AND U.PRIV_DROP_DATABASE_LINK = 1 THEN 'DROP DATABASE LINK'                 WHEN V1.C1 = 36                      AND U.PRIV_CREATE_DATABASE_LINK = 1 THEN 'CREATE DATABASE LINK'                 WHEN V1.C1 = 37                      AND (U.PRIV_OTHERS & (1 << 1)) != 0 THEN 'ALTER ROUTINE'                 WHEN V1.C1 = 38                      AND (U.PRIV_OTHERS & (1 << 2)) != 0 THEN 'CREATE ROUTINE'                 WHEN V1.C1 = 39                      AND (U.PRIV_OTHERS & (1 << 3)) != 0 THEN 'CREATE TABLESPACE'                 WHEN V1.C1 = 40                      AND (U.PRIV_OTHERS & (1 << 4)) != 0 THEN 'SHUTDOWN'                 WHEN V1.C1 = 41                      AND (U.PRIV_OTHERS & (1 << 5)) != 0 THEN 'RELOAD'                 WHEN V1.C1 = 0                      AND U.PRIV_ALTER = 0                      AND U.PRIV_CREATE = 0                      AND U.PRIV_CREATE_USER = 0                      AND U.PRIV_DELETE = 0                      AND U.PRIV_DROP = 0                      AND U.PRIV_INSERT = 0                      AND U.PRIV_UPDATE = 0                      AND U.PRIV_SELECT = 0                      AND U.PRIV_INDEX = 0                      AND U.PRIV_CREATE_VIEW = 0                      AND U.PRIV_SHOW_VIEW = 0                      AND U.PRIV_SHOW_DB = 0                      AND U.PRIV_SUPER = 0                      AND U.PRIV_PROCESS = 0                      AND U.PRIV_CREATE_SYNONYM = 0                      AND U.PRIV_FILE = 0                      AND U.PRIV_ALTER_TENANT = 0                      AND U.PRIV_ALTER_SYSTEM = 0                      AND U.PRIV_CREATE_RESOURCE_POOL = 0                      AND U.PRIV_CREATE_RESOURCE_UNIT = 0                      AND U.PRIV_REPL_SLAVE = 0                      AND U.PRIV_REPL_CLIENT = 0                       AND U.PRIV_DROP_DATABASE_LINK = 0                       AND U.PRIV_CREATE_DATABASE_LINK = 0                       AND U.PRIV_OTHERS = 0 THEN 'USAGE'             END PRIVILEGE_TYPE ,             CASE                 WHEN U.PRIV_GRANT_OPTION = 0 THEN 'NO'                 WHEN U.PRIV_ALTER = 0                      AND U.PRIV_CREATE = 0                      AND U.PRIV_CREATE_USER = 0                      AND U.PRIV_DELETE = 0                      AND U.PRIV_DROP = 0                      AND U.PRIV_INSERT = 0                      AND U.PRIV_UPDATE = 0                      AND U.PRIV_SELECT = 0                      AND U.PRIV_INDEX = 0                      AND U.PRIV_CREATE_VIEW = 0                      AND U.PRIV_SHOW_VIEW = 0                      AND U.PRIV_SHOW_DB = 0                      AND U.PRIV_SUPER = 0                      AND U.PRIV_PROCESS = 0                      AND U.PRIV_CREATE_SYNONYM = 0                      AND U.PRIV_FILE = 0                      AND U.PRIV_ALTER_TENANT = 0                      AND U.PRIV_ALTER_SYSTEM = 0                      AND U.PRIV_CREATE_RESOURCE_POOL = 0                      AND U.PRIV_CREATE_RESOURCE_UNIT = 0                      AND U.PRIV_REPL_SLAVE = 0                      AND U.PRIV_REPL_CLIENT = 0                       AND U.PRIV_DROP_DATABASE_LINK = 0                       AND U.PRIV_CREATE_DATABASE_LINK = 0                      AND U.PRIV_OTHERS = 0 THEN 'NO'                 WHEN U.PRIV_GRANT_OPTION = 1 THEN 'YES'             END IS_GRANTABLE      FROM oceanbase.__all_user U,        (SELECT 0 AS C1         UNION ALL SELECT 1 AS C1         UNION ALL SELECT 2 AS C1         UNION ALL SELECT 3 AS C1         UNION ALL SELECT 4 AS C1         UNION ALL SELECT 5 AS C1         UNION ALL SELECT 7 AS C1         UNION ALL SELECT 8 AS C1         UNION ALL SELECT 9 AS C1         UNION ALL SELECT 10 AS C1         UNION ALL SELECT 11 AS C1         UNION ALL SELECT 12 AS C1         UNION ALL SELECT 13 AS C1         UNION ALL SELECT 14 AS C1         UNION ALL SELECT 15 AS C1         UNION ALL SELECT 17 AS C1         UNION ALL SELECT 23 AS C1         UNION ALL SELECT 27 AS C1         UNION ALL SELECT 28 AS C1         UNION ALL SELECT 29 AS C1         UNION ALL SELECT 30 AS C1         UNION ALL SELECT 31 AS C1         UNION ALL SELECT 33 AS C1         UNION ALL SELECT 34 AS C1         UNION ALL SELECT 35 AS C1         UNION ALL SELECT 36 AS C1         UNION ALL SELECT 37 AS C1         UNION ALL SELECT 38 AS C1         UNION ALL SELECT 39 AS C1         UNION ALL SELECT 40 AS C1         UNION ALL SELECT 41 AS C1) V1,        (SELECT USER_ID         FROM oceanbase.__all_user         WHERE TENANT_ID = 0           AND CONCAT(USER_NAME, '@', HOST) = CURRENT_USER()) CURR      LEFT JOIN        (SELECT USER_ID         FROM oceanbase.__all_database_privilege         WHERE TENANT_ID = 0           AND DATABASE_NAME = 'mysql'           AND PRIV_SELECT = 1) DB ON CURR.USER_ID = DB.USER_ID      WHERE U.TENANT_ID = 0        AND (DB.USER_ID IS NOT NULL             OR 512 & CURRENT_USER_PRIV() = 512             OR U.USER_ID = CURR.USER_ID)) V   WHERE V.PRIVILEGE_TYPE IS NOT NULL   )__"))) {
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

int ObInnerTableSchema::schema_privileges_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_SCHEMA_PRIVILEGES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_SCHEMA_PRIVILEGES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   WITH DB_PRIV AS (     select A.tenant_id TENANT_ID,            A.user_id USER_ID,            A.database_name DATABASE_NAME,            A.priv_alter PRIV_ALTER,            A.priv_create PRIV_CREATE,            A.priv_delete PRIV_DELETE,            A.priv_drop PRIV_DROP,            A.priv_grant_option PRIV_GRANT_OPTION,            A.priv_insert PRIV_INSERT,            A.priv_update PRIV_UPDATE,            A.priv_select PRIV_SELECT,            A.priv_index PRIV_INDEX,            A.priv_create_view PRIV_CREATE_VIEW,            A.priv_show_view PRIV_SHOW_VIEW,            A.priv_others PRIV_OTHERS     from oceanbase.__all_database_privilege_history A,          (select tenant_id, user_id, database_name, max(schema_version) schema_version from oceanbase.__all_database_privilege_history group by tenant_id, user_id, database_name, database_name collate utf8mb4_bin) B     where A.tenant_id = B.tenant_id and A.user_id = B.user_id and A.database_name collate utf8mb4_bin = B.database_name collate utf8mb4_bin and A.schema_version = B.schema_version and A.is_deleted = 0   )   SELECT CAST(CONCAT('''', V.USER_NAME, '''', '@', '''', V.HOST, '''') AS CHAR(81)) AS GRANTEE ,          CAST('def' AS CHAR(512)) AS TABLE_CATALOG ,          CAST(V.DATABASE_NAME AS CHAR(128)) collate utf8mb4_name_case AS TABLE_SCHEMA ,          CAST(V.PRIVILEGE_TYPE AS CHAR(64)) AS PRIVILEGE_TYPE ,          CAST(V.IS_GRANTABLE AS CHAR(3)) AS IS_GRANTABLE   FROM     (SELECT DP.DATABASE_NAME DATABASE_NAME,             U.USER_NAME AS USER_NAME,             U.HOST AS HOST,             CASE                 WHEN V1.C1 = 1                      AND DP.PRIV_ALTER = 1 THEN 'ALTER'                 WHEN V1.C1 = 2                      AND DP.PRIV_CREATE = 1 THEN 'CREATE'                 WHEN V1.C1 = 4                      AND DP.PRIV_DELETE = 1 THEN 'DELETE'                 WHEN V1.C1 = 5                      AND DP.PRIV_DROP = 1 THEN 'DROP'                 WHEN V1.C1 = 7                      AND DP.PRIV_INSERT = 1 THEN 'INSERT'                 WHEN V1.C1 = 8                      AND DP.PRIV_UPDATE = 1 THEN 'UPDATE'                 WHEN V1.C1 = 9                      AND DP.PRIV_SELECT = 1 THEN 'SELECT'                 WHEN V1.C1 = 10                      AND DP.PRIV_INDEX = 1 THEN 'INDEX'                 WHEN V1.C1 = 11                      AND DP.PRIV_CREATE_VIEW = 1 THEN 'CREATE VIEW'                 WHEN V1.C1 = 12                      AND DP.PRIV_SHOW_VIEW = 1 THEN 'SHOW VIEW'                 WHEN V1.C1 = 13                      AND (U.PRIV_OTHERS & (1 << 0)) != 0 THEN 'EXECUTE'                 WHEN V1.C1 = 14                       AND (U.PRIV_OTHERS & (1 << 1)) != 0 THEN 'ALTER ROUTINE'                 WHEN V1.C1 = 15                       AND (U.PRIV_OTHERS & (1 << 2)) != 0 THEN 'CREATE ROUTINE'                 ELSE NULL             END PRIVILEGE_TYPE ,             CASE                 WHEN DP.PRIV_GRANT_OPTION = 1 THEN 'YES'                 WHEN DP.PRIV_GRANT_OPTION = 0 THEN 'NO'             END IS_GRANTABLE      FROM DB_PRIV DP,                       oceanbase.__all_user U,        (SELECT 1 AS C1         UNION ALL SELECT 2 AS C1         UNION ALL SELECT 4 AS C1         UNION ALL SELECT 5 AS C1         UNION ALL SELECT 7 AS C1         UNION ALL SELECT 8 AS C1         UNION ALL SELECT 9 AS C1         UNION ALL SELECT 10 AS C1         UNION ALL SELECT 11 AS C1         UNION ALL SELECT 12 AS C1         UNION ALL SELECT 13 AS C1         UNION ALL SELECT 14 AS C1         UNION ALL SELECT 15 AS C1) V1,        (SELECT USER_ID         FROM oceanbase.__all_user         WHERE TENANT_ID= 0           AND CONCAT(USER_NAME, '@', HOST) = CURRENT_USER()) CURR      LEFT JOIN        (SELECT USER_ID         FROM DB_PRIV         WHERE TENANT_ID = 0           AND DATABASE_NAME = 'mysql'           AND PRIV_SELECT = 1) DB ON CURR.USER_ID = DB.USER_ID      WHERE DP.TENANT_ID = 0        AND DP.TENANT_ID = U.TENANT_ID        AND DP.USER_ID = U.USER_ID        AND DP.DATABASE_NAME != '__recyclebin'        AND DP.DATABASE_NAME != '__public'        AND DP.DATABASE_NAME != 'SYS'        AND DP.DATABASE_NAME != 'LBACSYS'        AND DP.DATABASE_NAME != 'ORAAUDITOR'        AND (DB.USER_ID IS NOT NULL             OR 512 & CURRENT_USER_PRIV() = 512             OR DP.USER_ID = CURR.USER_ID)) V   WHERE V.PRIVILEGE_TYPE IS NOT NULL   )__"))) {
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
