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

int ObInnerTableSchema::dba_ob_trusted_root_certificate_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TRUSTED_ROOT_CERTIFICATE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TRUSTED_ROOT_CERTIFICATE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT    common_name AS COMMON_NAME,   description AS DESCRIPTION,   EXTRACT_CERT_EXPIRED_TIME(content) AS CERT_EXPIRED_TIME   FROM     oceanbase.__all_trusted_root_certificate   )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_clone_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_CLONE_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_CLONE_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT job_id AS CLONE_JOB_ID,        trace_id AS TRACE_ID,        source_tenant_id AS SOURCE_TENANT_ID,        source_tenant_name AS SOURCE_TENANT_NAME,        clone_tenant_id AS CLONE_TENANT_ID,        clone_tenant_name AS CLONE_TENANT_NAME,        tenant_snapshot_id AS TENANT_SNAPSHOT_ID,        tenant_snapshot_name AS TENANT_SNAPSHOT_NAME,        resource_pool_id AS RESOURCE_POOL_ID,        resource_pool_name AS RESOURCE_POOL_NAME,        unit_config_name AS UNIT_CONFIG_NAME,        restore_scn AS RESTORE_SCN,        status AS STATUS,        job_type AS CLONE_JOB_TYPE,        clone_start_time AS CLONE_START_TIME,        clone_finished_time AS CLONE_FINISHED_TIME,        ret_code AS RET_CODE,        error_msg AS ERROR_MESSAGE FROM oceanbase.__all_clone_job ORDER BY CLONE_START_TIME )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::cdb_index_usage_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_INDEX_USAGE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_INDEX_USAGE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       IUT.TENANT_ID AS CON_ID,       CAST(IUT.OBJECT_ID AS SIGNED) AS OBJECT_ID,       CAST(T.TABLE_NAME AS CHAR(128)) AS NAME,       CAST(DB.DATABASE_NAME AS CHAR(128)) AS OWNER,       CAST(IUT.TOTAL_ACCESS_COUNT AS SIGNED) AS TOTAL_ACCESS_COUNT,       CAST(IUT.TOTAL_EXEC_COUNT AS SIGNED) AS TOTAL_EXEC_COUNT,       CAST(IUT.TOTAL_ROWS_RETURNED AS SIGNED) AS TOTAL_ROWS_RETURNED,       CAST(IUT.BUCKET_0_ACCESS_COUNT AS SIGNED) AS BUCKET_0_ACCESS_COUNT,       CAST(IUT.BUCKET_1_ACCESS_COUNT AS SIGNED) AS BUCKET_1_ACCESS_COUNT,       CAST(IUT.BUCKET_2_10_ACCESS_COUNT AS SIGNED) AS BUCKET_2_10_ACCESS_COUNT,       CAST(IUT.BUCKET_2_10_ROWS_RETURNED AS SIGNED) AS BUCKET_2_10_ROWS_RETURNED,       CAST(IUT.BUCKET_11_100_ACCESS_COUNT AS SIGNED) AS BUCKET_11_100_ACCESS_COUNT,       CAST(IUT.BUCKET_11_100_ROWS_RETURNED AS SIGNED) AS BUCKET_11_100_ROWS_RETURNED,       CAST(IUT.BUCKET_101_1000_ACCESS_COUNT AS SIGNED) AS BUCKET_101_1000_ACCESS_COUNT,       CAST(IUT.BUCKET_101_1000_ROWS_RETURNED AS SIGNED) AS BUCKET_101_1000_ROWS_RETURNED,       CAST(IUT.BUCKET_1000_PLUS_ACCESS_COUNT AS SIGNED) AS BUCKET_1000_PLUS_ACCESS_COUNT,       CAST(IUT.BUCKET_1000_PLUS_ROWS_RETURNED AS SIGNED) AS BUCKET_1000_PLUS_ROWS_RETURNED,       CAST(IUT.LAST_USED AS CHAR(128)) AS LAST_USED     FROM       oceanbase.__all_virtual_index_usage_info IUT       JOIN oceanbase.__all_virtual_table T       ON IUT.TENANT_ID = T.TENANT_ID AND IUT.OBJECT_ID = T.TABLE_ID       JOIN oceanbase.__all_virtual_database DB       ON IUT.TENANT_ID = DB.TENANT_ID AND t.DATABASE_ID = DB.DATABASE_ID     WHERE T.TABLE_ID = IUT.OBJECT_ID )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::gv_ob_ls_snapshots_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_LS_SNAPSHOTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_LS_SNAPSHOTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT       tenant_id AS TENANT_ID,       snapshot_id AS SNAPSHOT_ID,       ls_id AS LS_ID,       svr_ip AS SVR_IP,       svr_port AS SVR_PORT,       (CASE         WHEN meta_existed = 1 THEN 'YES'         ELSE 'NO'         END) AS META_EXISTED,       (CASE         WHEN build_status = 0 THEN 'BUILDING'         WHEN build_status = 1 THEN 'FAILED'         WHEN build_status = 2 THEN 'SUCCESSFUL'         ELSE 'UNKNOWN'         END) AS BUILD_STATUS,       rebuild_seq_start AS REBUILD_SEQ_START,       rebuild_seq_end AS REBUILD_SEQ_END,       end_interval_scn AS END_INTERVAL_SCN,       ls_meta_package AS LS_META_PACKAGE,       (CASE         WHEN tsnap_is_running = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_IS_RUNNING,       (CASE         WHEN tsnap_has_unfinished_create_dag = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_HAS_UNFINISHED_CREATE_DAG,       (CASE         WHEN tsnap_has_unfinished_gc_dag = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_HAS_UNFINISHED_GC_DAG,       tsnap_clone_ref AS TSNAP_CLONE_REF,       (CASE         WHEN tsnap_meta_existed = 1 THEN 'YES'         ELSE 'NO'         END) AS TSNAP_META_EXISTED     FROM oceanbase.__all_virtual_ls_snapshot )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::v_ob_ls_snapshots_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_LS_SNAPSHOTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_LS_SNAPSHOTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT *     FROM oceanbase.GV$OB_LS_SNAPSHOTS     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::dba_ob_clone_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_CLONE_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_CLONE_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT job_id AS CLONE_JOB_ID,        trace_id AS TRACE_ID,        source_tenant_id AS SOURCE_TENANT_ID,        source_tenant_name AS SOURCE_TENANT_NAME,        clone_tenant_id AS CLONE_TENANT_ID,        clone_tenant_name AS CLONE_TENANT_NAME,        tenant_snapshot_id AS TENANT_SNAPSHOT_ID,        tenant_snapshot_name AS TENANT_SNAPSHOT_NAME,        resource_pool_id AS RESOURCE_POOL_ID,        resource_pool_name AS RESOURCE_POOL_NAME,        unit_config_name AS UNIT_CONFIG_NAME,        restore_scn AS RESTORE_SCN,        status AS STATUS,        job_type AS CLONE_JOB_TYPE,        clone_start_time AS CLONE_START_TIME,        clone_finished_time AS CLONE_FINISHED_TIME,        ret_code AS RET_CODE,        error_msg AS ERROR_MESSAGE FROM oceanbase.__all_clone_job_history ORDER BY CLONE_START_TIME )__"))) {
      LOG_ERROR("fail to set view_definition", K(ret));
    }
  }
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
