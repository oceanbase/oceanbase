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

int ObInnerTableSchema::user_mvref_stmt_stats_ora_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_ORA_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_USER_MVREF_STMT_STATS_ORA_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_MVREF_STMT_STATS_ORA_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       CAST(A.DATABASE_NAME AS VARCHAR2(128)) AS MV_OWNER,       CAST(B.TABLE_NAME AS VARCHAR2(128)) AS MV_NAME,       CAST(C.REFRESH_ID AS NUMBER) AS REFRESH_ID,       CAST(C.STEP AS NUMBER) AS STEP,       CAST(C.SQLID AS VARCHAR2(14)) AS SQLID /* TODO: VARCHAR2(14) */,       C.STMT AS STMT /* TODO: CLOB */,       CAST(C.EXECUTION_TIME AS NUMBER) AS EXECUTION_TIME,       C.EXECUTION_PLAN AS EXECUTION_PLAN /* TODO: XMLTYPE STORAGE BINARY */     FROM       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,       SYS.ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_REAL_AGENT C     WHERE A.DATABASE_ID = B.DATABASE_ID       AND B.TABLE_ID = C.MVIEW_ID       AND A.TENANT_ID = EFFECTIVE_TENANT_ID()       AND B.TENANT_ID = EFFECTIVE_TENANT_ID()       AND C.TENANT_ID = EFFECTIVE_TENANT_ID()       AND A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') )__"))) {
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
    if (OB_FAIL(table_schema.set_view_definition(R"__(   select cast(U1.USER_NAME as VARCHAR2(128)) as PROXY,          cast(U2.USER_NAME as VARCHAR2(128)) as CLIENT,          cast(DECODE(P.CREDENTIAL_TYPE, 0, 'NO', 5, 'YES') as VARCHAR2(3)) as AUTHENTICATION,          cast(DECODE(case when V.CNT = 0 and P.FLAGS = 4 then 2                            when V.CNT = 0 and P.FlAGS = 8 then 1                            else P.FLAGS end,                         0, NULL,                        1, 'PROXY MAY ACTIVATE ALL CLIENT ROLES',                        2, 'NO CLIENT ROLES MAY BE ACTIVATED',                        4, 'PROXY MAY ACTIVATE ROLE',                        5, 'PROXY MAY ACTIVATE ALL CLIENT ROLES',                        8, 'PROXY MAY NOT ACTIVATE ROLE') as VARCHAR2(35)) as FLAGS from SYS.ALL_VIRTUAL_USER_REAL_AGENT U1, SYS.ALL_VIRTUAL_USER_REAL_AGENT U2, SYS.ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT P,     (SELECT COUNT(B.ROLE_ID) CNT, A.TENANT_ID TENANT_ID, A.PROXY_USER_ID PROXY_USER_ID, A.CLIENT_USER_ID CLIENT_USER_ID      FROM SYS.ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT A LEFT JOIN            SYS.ALL_VIRTUAL_USER_PROXY_ROLE_INFO_REAL_AGENT B         ON A.TENANT_ID = B.TENANT_ID          AND A.CLIENT_USER_ID = B.CLIENT_USER_ID         AND A.PROXY_USER_ID = B.PROXY_USER_ID       GROUP BY A.TENANT_ID, A.PROXY_USER_ID, A.CLIENT_USER_ID     ) V where U1.TENANT_ID = U2.TENANT_ID   and U2.TENANT_ID = P.TENANT_ID   and U1.TENANT_ID = EFFECTIVE_TENANT_ID()   and U1.USER_ID = P.PROXY_USER_ID   and U2.USER_ID = P.CLIENT_USER_ID   and V.TENANT_ID = U1.TENANT_ID   and V.PROXY_USER_ID = P.PROXY_USER_ID   and V.CLIENT_USER_ID = P.CLIENT_USER_ID )__"))) {
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
