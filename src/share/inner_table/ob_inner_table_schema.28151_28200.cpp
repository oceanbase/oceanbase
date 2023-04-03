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

int ObInnerTableSchema::v_ob_global_transaction_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_GLOBAL_TRANSACTION_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_GLOBAL_TRANSACTION_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT *     FROM SYS.GV$OB_GLOBAL_TRANSACTION     WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()   )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT CAST(A.LS_ID AS NUMBER) AS LS_ID,            A.STATUS,            C.ZONE_PRIORITY AS PRIMARY_ZONE,            CAST(A.UNIT_GROUP_ID AS NUMBER) AS UNIT_GROUP_ID,            CAST(A.LS_GROUP_ID AS NUMBER) AS LS_GROUP_ID,             /* SYS LS's CREATE_SCN always is NULL, it means nothing */            (CASE A.LS_ID                 WHEN 1 THEN NULL                 ELSE CAST(B.CREATE_SCN AS NUMBER)            END) AS CREATE_SCN,             /* show NULL if not dropped */            (CASE B.DROP_SCN                 WHEN 1 THEN NULL                 ELSE CAST(B.DROP_SCN AS NUMBER)            END) AS DROP_SCN,             /* SYS tenant and Meta tenant always show NULL */            (CASE                 WHEN A.TENANT_ID = 1 THEN NULL                 WHEN BITAND(A.TENANT_ID, 1) = 1 THEN NULL                 ELSE CAST(B.SYNC_SCN AS NUMBER)             END) AS SYNC_SCN,             /* SYS tenant and Meta tenant always show NULL */            (CASE                 WHEN A.TENANT_ID = 1 THEN NULL                 WHEN BITAND(A.TENANT_ID, 1) = 1 THEN NULL                 ELSE CAST(B.READABLE_SCN AS NUMBER)             END) AS READABLE_SCN     FROM SYS.ALL_VIRTUAL_LS_STATUS A          JOIN SYS.ALL_VIRTUAL_LS_RECOVERY_STAT B               ON A.TENANT_ID = B.TENANT_ID AND A.LS_ID = B.LS_ID          JOIN SYS.ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO C               ON A.TENANT_ID = C.TENANT_ID AND A.LS_ID = C.LS_ID     WHERE A.TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::dba_ob_table_locations_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLE_LOCATIONS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLE_LOCATIONS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT     D.DATABASE_NAME,     A.TABLE_NAME,     A.TABLE_ID,      CASE WHEN A.TABLE_TYPE IN (0) THEN 'SYSTEM TABLE'          WHEN A.TABLE_TYPE IN (3,6,8,9) THEN 'USER TABLE'          WHEN A.TABLE_TYPE IN (5) THEN 'INDEX'          WHEN A.TABLE_TYPE IN (12,13) THEN 'LOB AUX TABLE'          ELSE NULL     END AS TABLE_TYPE,      A.PARTITION_NAME,     A.SUBPARTITION_NAME,      /* INDEX_NAME is valid when table is index */     CASE WHEN A.TABLE_TYPE != 5 THEN NULL          WHEN D.DATABASE_NAME != '__recyclebin'               THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))          ELSE TABLE_NAME     END AS INDEX_NAME,      CASE WHEN DATA_TABLE_ID = 0 THEN NULL          ELSE DATA_TABLE_ID     END AS DATA_TABLE_ID,      A.TABLET_ID,     C.LS_ID,     C.ZONE,     C.SVR_IP AS SVR_IP,     C.SVR_PORT AS SVR_PORT,     C.ROLE,     C.REPLICA_TYPE FROM (     SELECT TENANT_ID,              DATABASE_ID,              TABLE_NAME,              TABLE_ID,              'NULL' AS PARTITION_NAME,              'NULL' AS SUBPARTITION_NAME,              TABLET_ID AS TABLET_ID,              TABLE_TYPE,              DATA_TABLE_ID       FROM SYS.ALL_VIRTUAL_CORE_ALL_TABLE       WHERE TABLET_ID != 0 AND TENANT_ID = EFFECTIVE_TENANT_ID()        UNION ALL        SELECT       T.TENANT_ID AS TENANT_ID,       T.DATABASE_ID AS DATABASE_ID,       T.TABLE_NAME AS TABLE_NAME,       T.TABLE_ID AS TABLE_ID,       'NULL' AS PARTITION_NAME,       'NULL' AS SUBPARTITION_NAME,       TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID       FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T       WHERE T.TABLET_ID != 0 AND T.PART_LEVEL = 0 AND T.TENANT_ID = EFFECTIVE_TENANT_ID()        UNION ALL        SELECT       T.TENANT_ID AS TENANT_ID,       T.DATABASE_ID AS DATABASE_ID,       T.TABLE_NAME AS TABLE_NAME,       T.TABLE_ID AS TABLE_ID,       P.PART_NAME AS PARTITION_NAME,       'NULL' AS SUBPARTITION_NAME,       P.TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID       FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P            ON T.TABLE_ID = P.TABLE_ID AND T.TENANT_ID = P.TENANT_ID       WHERE T.PART_LEVEL = 1 AND T.TENANT_ID = EFFECTIVE_TENANT_ID()        UNION ALL        SELECT       T.TENANT_ID AS TENANT_ID,       T.DATABASE_ID AS DATABASE_ID,       T.TABLE_NAME AS TABLE_NAME,       T.TABLE_ID AS TABLE_ID,       P.PART_NAME AS PARTITION_NAME,       Q.SUB_PART_NAME AS SUBPARTITION_NAME,       Q.TABLET_ID AS TABLET_ID,       TABLE_TYPE,       DATA_TABLE_ID       FROM SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT Q            JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON P.PART_ID =Q.PART_ID AND Q.TENANT_ID = P.TENANT_ID            JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON T.TABLE_ID =P.TABLE_ID AND T.TENANT_ID = Q.TENANT_ID       WHERE T.PART_LEVEL = 2 AND T.TENANT_ID = EFFECTIVE_TENANT_ID()     ) A     JOIN SYS.DBA_OB_TABLET_TO_LS B ON A.TABLET_ID = B.TABLET_ID     JOIN SYS.DBA_OB_LS_LOCATIONS C ON B.LS_ID = C.LS_ID     JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON A.DATABASE_ID = D.DATABASE_ID AND A.TENANT_ID = D.TENANT_ID     ORDER BY A.TABLE_ID, A.TABLET_ID, C.ZONE, SVR_IP, SVR_PORT   )__"))) {
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

int ObInnerTableSchema::all_trigger_ordering_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TRIGGER_ORDERING_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TRIGGER_ORDERING_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(DB1.DATABASE_NAME AS VARCHAR2(128)) AS TRIGGER_OWNER,          CAST(TRG.TRIGGER_NAME AS VARCHAR2(128)) AS TRIGGER_NAME,          CAST(TRG.REF_TRG_DB_NAME AS VARCHAR2(128)) AS REFERENCED_TRIGGER_OWNER,          CAST(TRG.REF_TRG_NAME AS VARCHAR2(128)) AS REFERENCED_TRIGGER_NAME,          CAST(DECODE(TRG.ORDER_TYPE,                      1, 'FOLLOWS',                      2, 'PRECEDES') AS VARCHAR2(8)) AS ORDERING_TYPE   FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT TRG        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB1        ON TRG.DATABASE_ID = DB1.DATABASE_ID            AND TRG.TENANT_ID = EFFECTIVE_TENANT_ID()            AND DB1.TENANT_ID = EFFECTIVE_TENANT_ID()            AND (TRG.DATABASE_ID = USERENV('SCHEMAID')                OR USER_CAN_ACCESS_OBJ(1, abs(nvl(TRG.BASE_OBJECT_ID,0)), TRG.DATABASE_ID) = 1)   WHERE TRG.ORDER_TYPE > 0 AND TRG.ACTION_ORDER > 1 AND TRG.DATABASE_ID != 201004 )__"))) {
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

int ObInnerTableSchema::dba_trigger_ordering_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_TRIGGER_ORDERING_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_TRIGGER_ORDERING_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(DB1.DATABASE_NAME AS VARCHAR2(128)) AS TRIGGER_OWNER,          CAST(TRG.TRIGGER_NAME AS VARCHAR2(128)) AS TRIGGER_NAME,          CAST(TRG.REF_TRG_DB_NAME AS VARCHAR2(128)) AS REFERENCED_TRIGGER_OWNER,          CAST(TRG.REF_TRG_NAME AS VARCHAR2(128)) AS REFERENCED_TRIGGER_NAME,          CAST(DECODE(TRG.ORDER_TYPE,                      1, 'FOLLOWS',                      2, 'PRECEDES') AS VARCHAR2(8)) AS ORDERING_TYPE   FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT TRG        INNER JOIN        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB1        ON TRG.DATABASE_ID = DB1.DATABASE_ID            AND TRG.TENANT_ID = EFFECTIVE_TENANT_ID()            AND DB1.TENANT_ID = EFFECTIVE_TENANT_ID()   WHERE TRG.ORDER_TYPE > 0 AND TRG.ACTION_ORDER > 1 AND TRG.DATABASE_ID != 201004 )__"))) {
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

int ObInnerTableSchema::user_trigger_ordering_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_TRIGGER_ORDERING_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TRIGGER_ORDERING_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(TRG.TRIGGER_NAME AS VARCHAR2(128)) AS TRIGGER_NAME,          CAST(TRG.REF_TRG_DB_NAME AS VARCHAR2(128)) AS REFERENCED_TRIGGER_OWNER,          CAST(TRG.REF_TRG_NAME AS VARCHAR2(128)) AS REFERENCED_TRIGGER_NAME,          CAST(DECODE(TRG.ORDER_TYPE,                      1, 'FOLLOWS',                      2, 'PRECEDES') AS VARCHAR2(8)) AS ORDERING_TYPE   FROM (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT         WHERE TENANT_ID = EFFECTIVE_TENANT_ID() AND ORDER_TYPE > 0 AND ACTION_ORDER > 1)TRG   WHERE TRG.DATABASE_ID = USERENV('SCHEMAID') )__"))) {
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

int ObInnerTableSchema::gv_ob_transaction_schedulers_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TRANSACTION_SCHEDULERS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TRANSACTION_SCHEDULERS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     tenant_id AS TENANT_ID,     svr_ip AS SVR_IP,     svr_port AS SVR_PORT,     session_id AS SESSION_ID,     trans_id AS TX_ID,     CASE       WHEN state = 0 THEN 'INVALID'       WHEN state = 1 THEN 'IDLE'       WHEN state = 2 THEN 'EXPLICIT_ACTIVE'       WHEN state = 3 THEN 'IMPLICIT_ACTIVE'       WHEN state = 4 THEN 'ROLLBACK_SAVEPOINT'       WHEN state = 5 THEN 'IN_TERMINATE'       WHEN state = 6 THEN 'ABORTED'       WHEN state = 7 THEN 'ROLLED_BACK'       WHEN state = 8 THEN 'COMMIT_TIMEOUT'       WHEN state = 9 THEN 'COMMIT_UNKNOWN'       WHEN state = 10 THEN 'COMMITTED'       WHEN state = 11 THEN 'SUB_PREPARING'       WHEN state = 12 THEN 'SUB_PREPARED'       WHEN state = 13 THEN 'SUB_COMMITTING'       WHEN state = 14 THEN 'SUB_COMMITTED'       WHEN state = 15 THEN 'SUB_ROLLBACKING'       WHEN state = 16 THEN 'SUB_ROLLBACKED'       ELSE 'UNKNOWN'       END AS STATE,     cluster_id AS CLUSTER_ID,     XA_trans_id AS XA_TX_ID,     coordinator AS COORDINATOR,     participants AS PARTICIPANTS,     CASE       WHEN isolation_level = -1 THEN 'INVALID'       WHEN isolation_level = 0 THEN 'READ UNCOMMITTED'       WHEN isolation_level = 1 THEN 'READ COMMITTED'       WHEN isolation_level = 2 THEN 'REPEATABLE READ'       WHEN isolation_level = 3 THEN 'SERIALIZABLE'       ELSE 'UNKNOWN'       END AS ISOLATION_LEVEL,     snapshot_version AS SNAPSHOT_VERSION,     CASE       WHEN access_mode = -1 THEN 'INVALID'       WHEN access_mode = 0 THEN 'READ_WRITE'       WHEN access_mode = 1 THEN 'READ_ONLY'       ELSE 'UNKNOWN'       END AS ACCESS_MODE,     tx_op_sn AS TX_OP_SN,     flag AS FLAG,     active_time AS ACTIVE_TIME,     expire_time AS EXPIRE_TIME,     timeout_us AS TIMEOUT_US,     savepoints AS SAVEPOINTS,     savepoints_total_cnt AS SAVEPOINTS_TOTAL_CNT,     CASE       WHEN can_early_lock_release = 0 THEN 'FALSE'       WHEN can_early_lock_release = 1 THEN 'TRUE'       ELSE 'UNKNOWN'       END AS CAN_EARLY_LOCK_RELEASE     FROM SYS.ALL_VIRTUAL_TRANS_SCHEDULER )__"))) {
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

int ObInnerTableSchema::v_ob_transaction_schedulers_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TRANSACTION_SCHEDULERS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TRANSACTION_SCHEDULERS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT *     FROM SYS.GV$OB_TRANSACTION_SCHEDULERS     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::dba_ob_user_defined_rules_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_USER_DEFINED_RULES_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_USER_DEFINED_RULES_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT       CAST(T.DB_NAME AS VARCHAR2(128)) AS DB_NAME,       CAST(T.RULE_NAME AS VARCHAR2(256)) AS RULE_NAME,       CAST(T.RULE_ID AS NUMBER) AS RULE_ID,       PATTERN,       REPLACEMENT,       NORMALIZED_PATTERN,       CAST(CASE WHEN STATUS = 1 THEN 'ENABLE'                 WHEN STATUS = 2 THEN 'DISABLE'                 ELSE NULL END AS VARCHAR2(8)) AS STATUS,       CAST(T.VERSION AS NUMBER) AS VERSION,       CAST(T.PATTERN_DIGEST AS NUMBER) AS PATTERN_DIGEST     FROM       SYS.ALL_VIRTUAL_TENANT_REWRITE_RULES_REAL_AGENT T     WHERE T.STATUS != 3   )__"))) {
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

int ObInnerTableSchema::v_ob_archive_dest_status_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_ARCHIVE_DEST_STATUS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_ARCHIVE_DEST_STATUS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          DEST_ID,          PATH,          STATUS,          CHECKPOINT_SCN,          SYNCHRONIZED,          "COMMENT"   FROM SYS.ALL_VIRTUAL_ARCHIVE_DEST_STATUS   )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_log_archive_progress_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT DEST_ID,          LS_ID,          ROUND_ID,          PIECE_ID,          INCARNATION,          START_SCN,          MIN_LSN,          MAX_LSN,          CHECKPOINT_SCN,          STATUS,          FILE_ID,          FILE_OFFSET,          INPUT_BYTES,          OUTPUT_BYTES   FROM SYS.ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS   WHERE TENANT_ID=EFFECTIVE_TENANT_ID();   )__"))) {
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

int ObInnerTableSchema::all_table_idx_data_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLE_IDX_DATA_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_IDX_DATA_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLE_IDX_DATA_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_table_id", //column_name
      column_id + 21, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLE_TID);

  table_schema.set_max_used_column_id(column_id + 21);
  return ret;
}

int ObInnerTableSchema::all_table_idx_db_tb_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLE_IDX_DB_TB_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_IDX_DB_TB_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLE_IDX_DB_TB_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj table_name_default;
    table_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("table_name", //column_name
      column_id + 3, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      table_name_default,
      table_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLE_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_table_idx_tb_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLE_IDX_TB_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_IDX_TB_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLE_IDX_TB_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj table_name_default;
    table_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("table_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      table_name_default,
      table_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLE_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_column_idx_tb_column_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj column_name_default;
    column_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("column_name", //column_name
      column_id + 4, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      column_name_default,
      column_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_COLUMN_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_column_idx_column_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_COLUMN_IDX_COLUMN_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLUMN_IDX_COLUMN_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_COLUMN_IDX_COLUMN_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj column_name_default;
    column_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("column_name", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      column_name_default,
      column_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_COLUMN_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_ddl_operation_idx_ddl_type_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("operation_type", //column_name
      column_id + 9, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DDL_OPERATION_TID);

  table_schema.set_max_used_column_id(column_id + 9);
  return ret;
}

int ObInnerTableSchema::all_table_history_idx_data_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_table_id", //column_name
      column_id + 23, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLE_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 23);
  return ret;
}

int ObInnerTableSchema::all_log_archive_piece_files_idx_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dest_id", //column_name
      column_id + 2, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj file_status_default;
    file_status_default.set_varchar(ObString::make_string("INVALID"));
    ADD_COLUMN_SCHEMA_T("file_status", //column_name
      column_id + 18, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_DEFAULT_STATUS_LENTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      file_status_default,
      file_status_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("round_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("piece_id", //column_name
      column_id + 4, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID);

  table_schema.set_max_used_column_id(column_id + 18);
  return ret;
}

int ObInnerTableSchema::all_backup_set_files_idx_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dest_id", //column_name
      column_id + 3, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("file_status", //column_name
      column_id + 11, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_DEFAULT_STATUS_LENTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_set_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_BACKUP_SET_FILES_TID);

  table_schema.set_max_used_column_id(column_id + 11);
  return ret;
}

int ObInnerTableSchema::all_ddl_task_status_idx_task_key_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_UNIQUE_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("target_object_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_id", //column_name
      column_id + 3, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 6, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_WITH_COLUMN_FLAGS("shadow_pk_0", //column_name
      column_id + 32768, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false,//is_autoincrement
      true,//is_hidden
      false);//is_storing_column
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_id", //column_name
      column_id + 1, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_UNIQUE_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DDL_TASK_STATUS_TID);

  table_schema.set_max_used_column_id(column_id + 32768);
  return ret;
}

int ObInnerTableSchema::all_user_idx_ur_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_USER_IDX_UR_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_USER_IDX_UR_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_USER_IDX_UR_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_USER_NAME_LENGTH_STORE, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_USER_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_database_idx_db_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DATABASE_IDX_DB_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DATABASE_IDX_DB_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DATABASE_IDX_DB_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj database_name_default;
    database_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("database_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      database_name_default,
      database_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DATABASE_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tablegroup_idx_tg_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLEGROUP_IDX_TG_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLEGROUP_IDX_TG_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLEGROUP_IDX_TG_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tablegroup_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLEGROUP_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tablegroup_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLEGROUP_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_history_idx_tenant_deleted_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_HISTORY_IDX_TENANT_DELETED_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_HISTORY_IDX_TENANT_DELETED_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_HISTORY_IDX_TENANT_DELETED_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_rootservice_event_history_idx_rs_module_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_MODULE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_MODULE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_MODULE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("module", //column_name
      column_id + 2, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_ROOTSERVICE_EVENT_DESC_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      0, //column_length
      -1, //column_precision
      6, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default);
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 2);
  return ret;
}

int ObInnerTableSchema::all_rootservice_event_history_idx_rs_event_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_EVENT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_EVENT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_EVENT_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("event", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_ROOTSERVICE_EVENT_DESC_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      0, //column_length
      -1, //column_precision
      6, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default);
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_recyclebin_idx_recyclebin_db_type_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 4, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type", //column_name
      column_id + 3, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_name", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_OBJECT_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RECYCLEBIN_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_part_idx_part_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PART_IDX_PART_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_IDX_PART_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_PART_IDX_PART_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj part_name_default;
    part_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("part_name", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      part_name_default,
      part_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_PART_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_sub_part_idx_sub_part_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj sub_part_name_default;
    sub_part_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("sub_part_name", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      sub_part_name_default,
      sub_part_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_id", //column_name
      column_id + 4, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_SUB_PART_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_def_sub_part_idx_def_sub_part_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj sub_part_name_default;
    sub_part_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("sub_part_name", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      sub_part_name_default,
      sub_part_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DEF_SUB_PART_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_server_event_history_idx_server_module_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_MODULE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_MODULE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_MODULE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("module", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_ROOTSERVICE_EVENT_DESC_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      0, //column_length
      -1, //column_precision
      6, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default);
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_SERVER_EVENT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_server_event_history_idx_server_event_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_EVENT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_EVENT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_EVENT_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("event", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_ROOTSERVICE_EVENT_DESC_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      0, //column_length
      -1, //column_precision
      6, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default);
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_SERVER_EVENT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_rootservice_job_idx_rs_job_type_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ROOTSERVICE_JOB_IDX_RS_JOB_TYPE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROOTSERVICE_JOB_IDX_RS_JOB_TYPE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_ROOTSERVICE_JOB_IDX_RS_JOB_TYPE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_type", //column_name
      column_id + 2, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_ROOTSERVICE_JOB_TID);

  table_schema.set_max_used_column_id(column_id + 2);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_idx_fk_child_tid_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("child_table_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_FOREIGN_KEY_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_idx_fk_parent_tid_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parent_table_id", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_FOREIGN_KEY_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_idx_fk_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj foreign_key_name_default;
    foreign_key_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("foreign_key_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONSTRAINT_NAME_LENGTH_ORACLE, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      foreign_key_name_default,
      foreign_key_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_FOREIGN_KEY_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_history_idx_fk_his_child_tid_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("child_table_id", //column_name
      column_id + 6, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 3, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_FOREIGN_KEY_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 6);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_history_idx_fk_his_parent_tid_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parent_table_id", //column_name
      column_id + 7, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 3, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_FOREIGN_KEY_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 7);
  return ret;
}

int ObInnerTableSchema::all_synonym_idx_db_synonym_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj synonym_name_default;
    synonym_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("synonym_name", //column_name
      column_id + 5, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SYNONYM_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      synonym_name_default,
      synonym_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("synonym_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_SYNONYM_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_synonym_idx_synonym_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj synonym_name_default;
    synonym_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("synonym_name", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SYNONYM_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      synonym_name_default,
      synonym_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("synonym_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_SYNONYM_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_ddl_checksum_idx_ddl_checksum_task_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(6);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ddl_task_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("execution_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id", //column_name
      column_id + 5, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_id", //column_name
      column_id + 6, //column_id
      6, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DDL_CHECKSUM_TID);

  table_schema.set_max_used_column_id(column_id + 6);
  return ret;
}

int ObInnerTableSchema::all_routine_idx_db_routine_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_name", //column_name
      column_id + 5, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_ROUTINE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_ROUTINE_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_routine_idx_routine_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_name", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_ROUTINE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_ROUTINE_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_routine_idx_routine_pkg_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_ROUTINE_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_routine_param_idx_routine_param_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj param_name_default;
    param_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("param_name", //column_name
      column_id + 7, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false, //is_autoincrement
      param_name_default,
      param_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_ROUTINE_PARAM_TID);

  table_schema.set_max_used_column_id(column_id + 7);
  return ret;
}

int ObInnerTableSchema::all_package_idx_db_pkg_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj package_name_default;
    package_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("package_name", //column_name
      column_id + 4, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PACKAGE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      package_name_default,
      package_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_PACKAGE_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_package_idx_pkg_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PACKAGE_IDX_PKG_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PACKAGE_IDX_PKG_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_PACKAGE_IDX_PKG_NAME_TID);

  if (OB_SUCC(ret)) {
    ObObj package_name_default;
    package_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("package_name", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PACKAGE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      package_name_default,
      package_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_PACKAGE_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_acquired_snapshot_idx_snapshot_tablet_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tablet_id", //column_name
      column_id + 6, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      0, //column_length
      -1, //column_precision
      6, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default);
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_ACQUIRED_SNAPSHOT_TID);

  table_schema.set_max_used_column_id(column_id + 6);
  return ret;
}

int ObInnerTableSchema::all_constraint_idx_cst_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_CONSTRAINT_IDX_CST_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_CONSTRAINT_IDX_CST_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_CONSTRAINT_IDX_CST_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("constraint_name", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONSTRAINT_NAME_LENGTH_ORACLE, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("constraint_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_CONSTRAINT_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_type_idx_db_type_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TYPE_IDX_DB_TYPE_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TYPE_IDX_DB_TYPE_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TYPE_IDX_DB_TYPE_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type_name", //column_name
      column_id + 18, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TYPE_TID);

  table_schema.set_max_used_column_id(column_id + 18);
  return ret;
}

int ObInnerTableSchema::all_type_idx_type_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TYPE_IDX_TYPE_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TYPE_IDX_TYPE_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TYPE_IDX_TYPE_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type_name", //column_name
      column_id + 18, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TYPE_TID);

  table_schema.set_max_used_column_id(column_id + 18);
  return ret;
}

int ObInnerTableSchema::all_type_attr_idx_type_attr_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("name", //column_name
      column_id + 6, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("attribute", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TYPE_ATTR_TID);

  table_schema.set_max_used_column_id(column_id + 6);
  return ret;
}

int ObInnerTableSchema::all_coll_type_idx_coll_name_type_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("coll_name", //column_name
      column_id + 16, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("coll_type", //column_name
      column_id + 13, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("coll_type_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_COLL_TYPE_TID);

  table_schema.set_max_used_column_id(column_id + 16);
  return ret;
}

int ObInnerTableSchema::all_dblink_idx_owner_dblink_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("owner_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dblink_name", //column_name
      column_id + 3, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DBLINK_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dblink_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DBLINK_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_dblink_idx_dblink_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DBLINK_IDX_DBLINK_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DBLINK_IDX_DBLINK_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DBLINK_IDX_DBLINK_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dblink_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DBLINK_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dblink_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DBLINK_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_role_grantee_map_idx_grantee_role_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("role_id", //column_name
      column_id + 3, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grantee_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_role_grantee_map_history_idx_grantee_his_role_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("role_id", //column_name
      column_id + 3, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 4, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grantee_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_tenant_keystore_idx_keystore_master_key_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("master_key_id", //column_name
      column_id + 6, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("keystore_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_KEYSTORE_TID);

  table_schema.set_max_used_column_id(column_id + 6);
  return ret;
}

int ObInnerTableSchema::all_tenant_keystore_history_idx_keystore_his_master_key_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("master_key_id", //column_name
      column_id + 8, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("keystore_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_KEYSTORE_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 8);
  return ret;
}

int ObInnerTableSchema::all_tenant_ols_policy_idx_ols_policy_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("policy_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_policy_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OLS_POLICY_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_ols_policy_idx_ols_policy_col_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_name", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_policy_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OLS_POLICY_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_tenant_ols_component_idx_ols_com_policy_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_policy_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comp_type", //column_name
      column_id + 4, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_component_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OLS_COMPONENT_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_policy_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_policy_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_label_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OLS_LABEL_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_tag_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_tag", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_label_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OLS_LABEL_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_label_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OLS_LABEL_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_tenant_ols_user_level_idx_ols_level_uid_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_user_level_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OLS_USER_LEVEL_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_ols_user_level_idx_ols_level_policy_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_policy_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("label_se_user_level_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OLS_USER_LEVEL_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_tenant_profile_idx_profile_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("profile_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_ORACLE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("profile_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_PROFILE_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_security_audit_idx_audit_type_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("audit_type", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("audit_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_SECURITY_AUDIT_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_trigger_idx_trigger_base_obj_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("base_object_id", //column_name
      column_id + 11, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_TRIGGER_TID);

  table_schema.set_max_used_column_id(column_id + 11);
  return ret;
}

int ObInnerTableSchema::all_tenant_trigger_idx_db_trigger_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_name", //column_name
      column_id + 3, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TRIGGER_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_TRIGGER_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_tenant_trigger_idx_trigger_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TRIGGER_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_TRIGGER_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_tenant_trigger_history_idx_trigger_his_base_obj_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("base_object_id", //column_name
      column_id + 12, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 3, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_TRIGGER_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 12);
  return ret;
}

int ObInnerTableSchema::all_tenant_objauth_idx_objauth_grantor_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grantor_id", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("obj_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("objtype", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("col_id", //column_name
      column_id + 4, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grantee_id", //column_name
      column_id + 6, //column_id
      6, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("priv_id", //column_name
      column_id + 7, //column_id
      7, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OBJAUTH_TID);

  table_schema.set_max_used_column_id(column_id + 7);
  return ret;
}

int ObInnerTableSchema::all_tenant_objauth_idx_objauth_grantee_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grantee_id", //column_name
      column_id + 6, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("obj_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("objtype", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("col_id", //column_name
      column_id + 4, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grantor_id", //column_name
      column_id + 5, //column_id
      6, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("priv_id", //column_name
      column_id + 7, //column_id
      7, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OBJAUTH_TID);

  table_schema.set_max_used_column_id(column_id + 7);
  return ret;
}

int ObInnerTableSchema::all_tenant_object_type_idx_obj_type_db_obj_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 13, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_name", //column_name
      column_id + 17, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_type_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type", //column_name
      column_id + 3, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OBJECT_TYPE_TID);

  table_schema.set_max_used_column_id(column_id + 17);
  return ret;
}

int ObInnerTableSchema::all_tenant_object_type_idx_obj_type_obj_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_name", //column_name
      column_id + 17, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_type_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_OBJECT_TYPE_TID);

  table_schema.set_max_used_column_id(column_id + 17);
  return ret;
}

int ObInnerTableSchema::all_tenant_global_transaction_idx_xa_trans_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_id", //column_name
      column_id + 5, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("gtrid", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("bqual", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj format_id_default;
    format_id_default.set_int(1);
    ADD_COLUMN_SCHEMA_T("format_id", //column_name
      column_id + 4, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      format_id_default,
      format_id_default); //default_value
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_GLOBAL_TRANSACTION_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_tenant_dependency_idx_dependency_ref_obj_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_obj_id", //column_name
      column_id + 8, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_obj_type", //column_name
      column_id + 7, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dep_obj_type", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dep_obj_id", //column_name
      column_id + 3, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dep_order", //column_name
      column_id + 4, //column_id
      6, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_DEPENDENCY_TID);

  table_schema.set_max_used_column_id(column_id + 8);
  return ret;
}

int ObInnerTableSchema::all_ddl_error_message_idx_ddl_error_object_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("target_object_id", //column_name
      column_id + 3, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 5, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      column_id + 6, //column_id
      6, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port", //column_name
      column_id + 7, //column_id
      7, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DDL_ERROR_MESSAGE_TID);

  table_schema.set_max_used_column_id(column_id + 7);
  return ret;
}

int ObInnerTableSchema::all_table_stat_history_idx_table_stat_his_savtime_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("savtime", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(ObPreciseDateTime), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false); //is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLE_STAT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_column_stat_history_idx_column_stat_his_savtime_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("savtime", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(ObPreciseDateTime), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false); //is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id", //column_name
      column_id + 4, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_COLUMN_STAT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_histogram_stat_history_idx_histogram_stat_his_savtime_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(6);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("savtime", //column_name
      column_id + 6, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(ObPreciseDateTime), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false); //is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id", //column_name
      column_id + 4, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("endpoint_num", //column_name
      column_id + 5, //column_id
      6, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_HISTOGRAM_STAT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 6);
  return ret;
}

int ObInnerTableSchema::all_tablet_to_ls_idx_tablet_to_ls_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ls_id", //column_name
      column_id + 2, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tablet_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLET_TO_LS_TID);

  table_schema.set_max_used_column_id(column_id + 2);
  return ret;
}

int ObInnerTableSchema::all_tablet_to_ls_idx_tablet_to_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tablet_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLET_TO_LS_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_pending_transaction_idx_pending_tx_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("gtrid", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("bqual", //column_name
      column_id + 4, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_BINARY, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj format_id_default;
    format_id_default.set_int(1);
    ADD_COLUMN_SCHEMA_T("format_id", //column_name
      column_id + 5, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      format_id_default,
      format_id_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_id", //column_name
      column_id + 2, //column_id
      5, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_PENDING_TRANSACTION_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_context_idx_ctx_namespace_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID);

  if (OB_SUCC(ret)) {
    ObObj namespace_default;
    namespace_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("namespace", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONTEXT_STRING_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      namespace_default,
      namespace_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("context_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_CONTEXT_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_plan_baseline_item_idx_spm_item_sql_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SQL_ID_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("plan_hash_value", //column_name
      column_id + 4, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_PLAN_BASELINE_ITEM_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_plan_baseline_item_idx_spm_item_value_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("plan_hash_value", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_id", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SQL_ID_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_PLAN_BASELINE_ITEM_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_tenant_directory_idx_directory_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("directory_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("directory_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TENANT_DIRECTORY_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_job_idx_job_powner_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_JOB_IDX_JOB_POWNER_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_JOB_IDX_JOB_POWNER_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_JOB_IDX_JOB_POWNER_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("powner", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_JOB_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_sequence_object_idx_seq_obj_db_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence_name", //column_name
      column_id + 5, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SEQUENCE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence_id", //column_name
      column_id + 2, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_SEQUENCE_OBJECT_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_sequence_object_idx_seq_obj_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence_name", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SEQUENCE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_SEQUENCE_OBJECT_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_recyclebin_idx_recyclebin_ori_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("original_name", //column_name
      column_id + 7, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_ORIGINAL_NANE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_name", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_OBJECT_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RECYCLEBIN_TID);

  table_schema.set_max_used_column_id(column_id + 7);
  return ret;
}

int ObInnerTableSchema::all_table_privilege_idx_tb_priv_db_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_name", //column_name
      column_id + 4, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CORE_TALBE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLE_PRIVILEGE_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_table_privilege_idx_tb_priv_tb_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_name", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CORE_TALBE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_name", //column_name
      column_id + 3, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_TABLE_PRIVILEGE_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_database_privilege_idx_db_priv_db_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_name", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_DATABASE_PRIVILEGE_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_rls_policy_idx_rls_policy_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rls_policy_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RLS_POLICY_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_rls_policy_idx_rls_policy_group_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rls_group_id", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rls_policy_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RLS_POLICY_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}

int ObInnerTableSchema::all_rls_policy_history_idx_rls_policy_his_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 6, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rls_policy_id", //column_name
      column_id + 2, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 3, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RLS_POLICY_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 6);
  return ret;
}

int ObInnerTableSchema::all_rls_group_idx_rls_group_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 4, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rls_group_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RLS_GROUP_TID);

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_rls_group_history_idx_rls_group_his_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 6, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rls_group_id", //column_name
      column_id + 2, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 3, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RLS_GROUP_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 6);
  return ret;
}

int ObInnerTableSchema::all_rls_context_idx_rls_context_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 3, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rls_context_id", //column_name
      column_id + 2, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RLS_CONTEXT_TID);

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_rls_context_history_idx_rls_context_his_table_id_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_create
  }

  if (OB_SUCC(ret)) {
    ++column_id; // for gmt_modified
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TID);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id + 5, //column_id
      1, //rowkey_id
      1, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rls_context_id", //column_name
      column_id + 2, //column_id
      2, //rowkey_id
      2, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id + 3, //column_id
      3, //rowkey_id
      3, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      column_id + 1, //column_id
      4, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false,//is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(OB_ALL_RLS_CONTEXT_HISTORY_TID);

  table_schema.set_max_used_column_id(column_id + 5);
  return ret;
}


} // end namespace share
} // end namespace oceanbase
