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

#include "share/schema/ob_schema_macro_define.h"
#include "share/schema/ob_schema_service_sql_impl.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
namespace share
{

int ObInnerTableSchema::proxy_users_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_PROXY_USERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_PROXY_USERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   select cast(U1.USER_NAME as VARCHAR2(128)) as PROXY,          cast(U2.USER_NAME as VARCHAR2(128)) as CLIENT,          cast(DECODE(P.CREDENTIAL_TYPE, 0, 'NO', 5, 'YES') as VARCHAR2(3)) as AUTHENTICATION,          cast(DECODE(case when V.CNT = 0 and P.FLAGS = 4 then 2                           when V.CNT = 0 and P.FlAGS = 8 then 1                           else P.FLAGS end,                        0, NULL,                        1, 'PROXY MAY ACTIVATE ALL CLIENT ROLES',                        2, 'NO CLIENT ROLES MAY BE ACTIVATED',                        4, 'PROXY MAY ACTIVATE ROLE',                        5, 'PROXY MAY ACTIVATE ALL CLIENT ROLES',                        8, 'PROXY MAY NOT ACTIVATE ROLE') as VARCHAR2(35)) as FLAGS from SYS.ALL_VIRTUAL_USER_REAL_AGENT U1, SYS.ALL_VIRTUAL_USER_REAL_AGENT U2, SYS.ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT P,     (SELECT COUNT(B.ROLE_ID) CNT, A.TENANT_ID TENANT_ID, A.PROXY_USER_ID PROXY_USER_ID, A.CLIENT_USER_ID CLIENT_USER_ID      FROM SYS.ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT A LEFT JOIN           SYS.ALL_VIRTUAL_USER_PROXY_ROLE_INFO_REAL_AGENT B         ON A.TENANT_ID = B.TENANT_ID         AND A.CLIENT_USER_ID = B.CLIENT_USER_ID         AND A.PROXY_USER_ID = B.PROXY_USER_ID       GROUP BY A.TENANT_ID, A.PROXY_USER_ID, A.CLIENT_USER_ID     ) V where U1.TENANT_ID = U2.TENANT_ID   and U2.TENANT_ID = P.TENANT_ID   and U1.TENANT_ID = EFFECTIVE_TENANT_ID()   and U1.USER_ID = P.PROXY_USER_ID   and U2.USER_ID = P.CLIENT_USER_ID   and V.TENANT_ID = U1.TENANT_ID   and V.PROXY_USER_ID = P.PROXY_USER_ID   and V.CLIENT_USER_ID = P.CLIENT_USER_ID )__"))) {
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

int ObInnerTableSchema::dba_ob_services_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_SERVICES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_SERVICES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     gmt_create AS "CREATE_TIME",     gmt_modified AS "MODIFIED_TIME",     SERVICE_NAME_ID,     SERVICE_NAME,     SERVICE_STATUS   FROM SYS.ALL_VIRTUAL_SERVICE   WHERE TENANT_ID=EFFECTIVE_TENANT_ID();   )__"))) {
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

int ObInnerTableSchema::dba_ob_object_balance_weight_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_OBJECT_BALANCE_WEIGHT_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_OBJECT_BALANCE_WEIGHT_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT A.TABLE_ID,          CASE A.PARTITION_ID WHEN -1 THEN NULL ELSE A.PARTITION_ID END AS PARTITION_ID,          CASE A.SUBPARTITION_ID WHEN -1 THEN NULL ELSE A.SUBPARTITION_ID END AS SUBPARTITION_ID,          A.WEIGHT,          C.DATABASE_NAME,          B.TABLE_NAME,          B.PARTITION_NAME,          B.SUBPARTITION_NAME,          D.TABLEGROUP_NAME,          B.DATABASE_ID,          D.TABLEGROUP_ID,          B.OBJECT_ID   FROM SYS.ALL_VIRTUAL_OBJECT_BALANCE_WEIGHT_REAL_AGENT A   JOIN (         SELECT         TENANT_ID,         DATABASE_ID,         TABLE_NAME,         TABLE_ID,         -1 AS PART_ID,         -1 AS SUBPART_ID,         'NULL' AS PARTITION_NAME,         'NULL' AS SUBPARTITION_NAME,         TABLE_ID AS OBJECT_ID,         TABLEGROUP_ID         FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID() AND TABLE_ID > 500000          UNION ALL          SELECT         T.TENANT_ID AS TENANT_ID,         T.DATABASE_ID AS DATABASE_ID,         T.TABLE_NAME AS TABLE_NAME,         T.TABLE_ID AS TABLE_ID,         P.PART_ID AS PART_ID,         -1 AS SUBPART_ID,         P.PART_NAME AS PARTITION_NAME,         'NULL' AS SUBPARTITION_NAME,         P.PART_ID AS OBJECT_ID,         T.TABLEGROUP_ID AS TABLEGROUP_ID         FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P             ON T.TABLE_ID = P.TABLE_ID AND T.TENANT_ID = P.TENANT_ID         WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID() AND T.TABLE_ID > 500000          UNION ALL          SELECT         T.TENANT_ID AS TENANT_ID,         T.DATABASE_ID AS DATABASE_ID,         T.TABLE_NAME AS TABLE_NAME,         T.TABLE_ID AS TABLE_ID,         Q.PART_ID AS PART_ID,         Q.SUB_PART_ID AS SUBPART_ID,         P.PART_NAME AS PARTITION_NAME,         Q.SUB_PART_NAME AS SUBPARTITION_NAME,         Q.SUB_PART_ID AS OBJECT_ID,         T.TABLEGROUP_ID AS TABLEGROUP_ID         FROM SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT Q             JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON P.PART_ID =Q.PART_ID AND Q.TENANT_ID = P.TENANT_ID             JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON T.TABLE_ID =P.TABLE_ID AND T.TENANT_ID = Q.TENANT_ID         WHERE T.TABLE_ID = P.TABLE_ID AND P.TABLE_ID = Q.TABLE_ID AND P.PART_ID = Q.PART_ID         AND T.TENANT_ID = P.TENANT_ID AND P.TENANT_ID = Q.TENANT_ID         AND T.TENANT_ID = EFFECTIVE_TENANT_ID() AND T.TABLE_ID > 500000       ) B       ON A.TABLE_ID = B.TABLE_ID AND A.PARTITION_ID = B.PART_ID          AND A.SUBPARTITION_ID = B.SUBPART_ID   JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C       ON B.DATABASE_ID = C.DATABASE_ID AND B.TENANT_ID = C.TENANT_ID   LEFT JOIN SYS.ALL_VIRTUAL_TABLEGROUP_REAL_AGENT D       ON B.TABLEGROUP_ID = D.TABLEGROUP_ID AND B.TENANT_ID = D.TENANT_ID   ORDER BY A.TABLE_ID, A.PARTITION_ID, A.SUBPARTITION_ID   )__"))) {
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

int ObInnerTableSchema::user_scheduler_jobs_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_SCHEDULER_JOBS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_SCHEDULER_JOBS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     OWNER,     JOB_NAME,     JOB_SUBNAME,     JOB_STYLE,     JOB_CREATOR,     CLIENT_ID,     GLOBAL_UID,     PROGRAM_OWNER,     PROGRAM_NAME,     JOB_TYPE,     JOB_ACTION,     NUMBER_OF_ARGUMENTS,     SCHEDULE_OWNER,     SCHEDULE_NAME,     SCHEDULE_TYPE,     START_DATE,     REPEAT_INTERVAL,     EVENT_QUEUE_OWNER,     EVENT_QUEUE_NAME,     EVENT_QUEUE_AGENT,     EVENT_CONDITION,     EVENT_RULE,     FILE_WATCHER_OWNER,     FILE_WATCHER_NAME,     END_DATE,     JOB_CLASS,     ENABLED,     AUTO_DROP,     RESTART_ON_RECOVERY,     RESTART_ON_FAILURE,     STATE,     JOB_PRIORITY,     RUN_COUNT,     MAX_RUNS,     FAILURE_COUNT,     MAX_FAILURES,     RETRY_COUNT,     LAST_START_DATE,     LAST_RUN_DURATION,     NEXT_RUN_DATE,     SCHEDULE_LIMIT,     MAX_RUN_DURATION,     LOGGING_LEVEL,     STORE_OUTPUT,     STOP_ON_WINDOW_CLOSE,     INSTANCE_STICKINESS,     RAISE_EVENTS,     SYSTEM,     JOB_WEIGHT,     NLS_ENV,     SOURCE,     NUMBER_OF_DESTINATIONS,     DESTINATION_OWNER,     DESTINATION,     CREDENTIAL_OWNER,     CREDENTIAL_NAME,     INSTANCE_ID,     DEFERRED_DROP,     ALLOW_RUNS_IN_RESTRICTED_MODE,     COMMENTS,     FLAGS,     RESTARTABLE,     CONNECT_CREDENTIAL_OWNER,     CONNECT_CREDENTIAL_NAME     FROM DBA_SCHEDULER_JOBS WHERE OWNER = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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

int ObInnerTableSchema::dba_ob_tenant_flashback_log_scn_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TENANT_FLASHBACK_LOG_SCN_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TENANT_FLASHBACK_LOG_SCN_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT     TENANT_ID,     gmt_create AS TIMESTAMP,     SWITCHOVER_EPOCH,     OP_TYPE,     FLASHBACK_LOG_SCN   FROM SYS.ALL_VIRTUAL_TENANT_FLASHBACK_LOG_SCN   WHERE TENANT_ID=EFFECTIVE_TENANT_ID();   )__"))) {
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
