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

int ObInnerTableSchema::dba_scheduler_running_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_SCHEDULER_RUNNING_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_SCHEDULER_RUNNING_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     CAST(T.POWNER AS CHAR(128)) AS OWNER,     CAST(T.JOB_NAME AS CHAR(128)) AS JOB_NAME,     CAST(NULL AS CHAR(128)) AS JOB_SUBNAME,     CAST(T.JOB_STYLE AS CHAR(17)) AS JOB_STYLE,     CAST(NULL AS CHAR(5)) AS DETACHED,     CAST(T.THIS_EXEC_SESS_ID AS NUMBER) AS SESSION_ID,     CAST(NULL AS NUMBER) AS SLAVE_PROCESS_ID,     CAST(NULL AS NUMBER) AS SLAVE_OS_PROCESS_ID,     CAST(NULL AS NUMBER) AS RUNNING_INSTANCE,     CAST(NULL AS CHAR(128)) AS RESOURCE_CONSUMER_GROUP,     CAST(TIMESTAMPDIFF(MICROSECOND, T.THIS_DATE, NOW(6)) / 1000000 AS NUMBER) AS ELAPSED_TIME,     CAST(NULL AS NUMBER) AS CPU_USED,     CAST(NULL AS CHAR(261)) AS DESTINATION_OWNER,     CAST(T.DESTINATION_NAME AS CHAR(261)) AS DESTINATION,     CAST(NULL AS CHAR(128)) AS CREDENTIAL_OWNER,     CAST(T.CREDENTIAL_NAME AS CHAR(128)) AS CREDENTIAL_NAME,     CAST(T.THIS_DATE AS DATETIME(6)) AS THIS_DATE,     CAST(T.THIS_EXEC_DATE AS DATETIME(6)) AS THIS_EXEC_DATE,     CAST(T.THIS_EXEC_ADDR AS CHAR(128)) AS THIS_EXEC_ADDR,     CAST(T.THIS_EXEC_TRACE_ID AS CHAR(128)) AS THIS_EXEC_TRACE_ID     FROM oceanbase.__all_tenant_scheduler_job T WHERE T.JOB_NAME != '__dummy_guard' AND T.JOB > 0 AND T.THIS_DATE IS NOT NULL )__"))) {
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

int ObInnerTableSchema::cdb_scheduler_running_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_SCHEDULER_RUNNING_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_SCHEDULER_RUNNING_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     TENANT_ID,     CAST(T.POWNER AS CHAR(128)) AS OWNER,     CAST(T.JOB_NAME AS CHAR(128)) AS JOB_NAME,     CAST(NULL AS CHAR(128)) AS JOB_SUBNAME,     CAST(T.JOB_STYLE AS CHAR(17)) AS JOB_STYLE,     CAST(NULL AS CHAR(5)) AS DETACHED,     CAST(T.THIS_EXEC_SESS_ID AS NUMBER) AS SESSION_ID,     CAST(NULL AS NUMBER) AS SLAVE_PROCESS_ID,     CAST(NULL AS NUMBER) AS SLAVE_OS_PROCESS_ID,     CAST(NULL AS NUMBER) AS RUNNING_INSTANCE,     CAST(NULL AS CHAR(128)) AS RESOURCE_CONSUMER_GROUP,     CAST(TIMESTAMPDIFF(MICROSECOND, T.THIS_DATE, NOW(6)) / 1000000 AS NUMBER) AS ELAPSED_TIME,     CAST(NULL AS NUMBER) AS CPU_USED,     CAST(NULL AS CHAR(261)) AS DESTINATION_OWNER,     CAST(T.DESTINATION_NAME AS CHAR(261)) AS DESTINATION,     CAST(NULL AS CHAR(128)) AS CREDENTIAL_OWNER,     CAST(T.CREDENTIAL_NAME AS CHAR(128)) AS CREDENTIAL_NAME,     CAST(T.THIS_DATE AS DATETIME(6)) AS THIS_DATE,     CAST(T.THIS_EXEC_DATE AS DATETIME(6)) AS THIS_EXEC_DATE,     CAST(T.THIS_EXEC_ADDR AS CHAR(128)) AS THIS_EXEC_ADDR,     CAST(T.THIS_EXEC_TRACE_ID AS CHAR(128)) AS THIS_EXEC_TRACE_ID     FROM oceanbase.__all_virtual_tenant_scheduler_job T WHERE T.JOB_NAME != '__dummy_guard' AND T.JOB > 0 AND T.THIS_DATE IS NOT NULL )__"))) {
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

int ObInnerTableSchema::cdb_scheduler_jobs_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_SCHEDULER_JOBS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_SCHEDULER_JOBS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     TENANT_ID,     CAST(T.POWNER AS CHAR(128)) AS OWNER,     CAST(T.JOB_NAME AS CHAR(128)) AS JOB_NAME,     CAST(NULL AS CHAR(128)) AS JOB_SUBNAME,     CAST(T.JOB_STYLE AS CHAR(17)) AS JOB_STYLE,     CAST(NULL AS CHAR(128)) AS JOB_CREATOR,     CAST(NULL AS CHAR(65)) AS CLIENT_ID,     CAST(NULL AS CHAR(33)) AS GLOBAL_UID,     CAST(T.POWNER AS CHAR(4000)) AS PROGRAM_OWNER,     CAST(T.PROGRAM_NAME AS CHAR(4000)) AS PROGRAM_NAME,     CAST(T.JOB_TYPE AS CHAR(16)) AS JOB_TYPE,     CAST(T.JOB_ACTION AS CHAR(4000)) AS JOB_ACTION,     CAST(T.NUMBER_OF_ARGUMENT AS SIGNED) AS NUMBER_OF_ARGUMENTS,     CAST(NULL AS CHAR(4000)) AS SCHEDULE_OWNER,     CAST(NULL AS CHAR(4000)) AS SCHEDULE_NAME,     CAST(NULL AS CHAR(12)) AS SCHEDULE_TYPE,     CAST(T.START_DATE AS DATETIME(6)) AS START_DATE,     CAST(T.REPEAT_INTERVAL AS CHAR(4000)) AS REPEAT_INTERVAL,     CAST(NULL AS CHAR(128)) AS EVENT_QUEUE_OWNER,     CAST(NULL AS CHAR(128)) AS EVENT_QUEUE_NAME,     CAST(NULL AS CHAR(523)) AS EVENT_QUEUE_AGENT,     CAST(NULL AS CHAR(4000)) AS EVENT_CONDITION,     CAST(NULL AS CHAR(261)) AS EVENT_RULE,     CAST(NULL AS CHAR(261)) AS FILE_WATCHER_OWNER,     CAST(NULL AS CHAR(261)) AS FILE_WATCHER_NAME,     CAST(T.END_DATE AS DATETIME(6)) AS END_DATE,     CAST(T.JOB_CLASS AS CHAR(128)) AS JOB_CLASS,     CAST(T.ENABLED AS CHAR(5)) AS ENABLED,     CAST(T.AUTO_DROP AS CHAR(5)) AS AUTO_DROP,     CAST(NULL AS CHAR(5)) AS RESTART_ON_RECOVERY,     CAST(NULL AS CHAR(5)) AS RESTART_ON_FAILURE,     CAST(T.STATE AS CHAR(15)) AS STATE,     CAST(NULL AS SIGNED) AS JOB_PRIORITY,     CAST(T.RUN_COUNT AS SIGNED) AS RUN_COUNT,     CAST(NULL AS SIGNED) AS MAX_RUNS,     CAST(T.FAILURES AS SIGNED) AS FAILURE_COUNT,     CAST(NULL AS SIGNED) AS MAX_FAILURES,     CAST(T.RETRY_COUNT AS SIGNED) AS RETRY_COUNT,     CAST(T.LAST_DATE AS DATETIME(6)) AS LAST_START_DATE,     CAST(T.LAST_RUN_DURATION AS SIGNED) AS LAST_RUN_DURATION,     CAST(T.NEXT_DATE AS DATETIME(6)) AS NEXT_RUN_DATE,     CAST(NULL AS SIGNED) AS SCHEDULE_LIMIT,     CAST(T.MAX_RUN_DURATION AS SIGNED) AS MAX_RUN_DURATION,     CAST(NULL AS CHAR(11)) AS LOGGING_LEVEL,     CAST(NULL AS CHAR(5)) AS STORE_OUTPUT,     CAST(NULL AS CHAR(5)) AS STOP_ON_WINDOW_CLOSE,     CAST(NULL AS CHAR(5)) AS INSTANCE_STICKINESS,     CAST(NULL AS CHAR(4000)) AS RAISE_EVENTS,     CAST(NULL AS CHAR(5)) AS SYSTEM,     CAST(NULL AS SIGNED) AS JOB_WEIGHT,     CAST(T.NLSENV AS CHAR(4000)) AS NLS_ENV,     CAST(NULL AS CHAR(128)) AS SOURCE,     CAST(NULL AS SIGNED) AS NUMBER_OF_DESTINATIONS,     CAST(NULL AS CHAR(261)) AS DESTINATION_OWNER,     CAST(NULL AS CHAR(261)) AS DESTINATION,     CAST(NULL AS CHAR(128)) AS CREDENTIAL_OWNER,     CAST(NULL AS CHAR(128)) AS CREDENTIAL_NAME,     CAST(T.FIELD1 AS CHAR(128)) AS INSTANCE_ID,     CAST(NULL AS CHAR(5)) AS DEFERRED_DROP,     CAST(NULL AS CHAR(5)) AS ALLOW_RUNS_IN_RESTRICTED_MODE,     CAST(T.COMMENTS AS CHAR(4000)) AS COMMENTS,     CAST(T.FLAG AS SIGNED) AS FLAGS,     CAST(NULL AS CHAR(5)) AS RESTARTABLE,     CAST(NULL AS CHAR(128)) AS CONNECT_CREDENTIAL_OWNER,     CAST(NULL AS CHAR(128)) AS CONNECT_CREDENTIAL_NAME   FROM oceanbase.__all_virtual_tenant_scheduler_job T WHERE T.JOB_NAME != '__dummy_guard' and T.JOB > 0 )__"))) {
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
