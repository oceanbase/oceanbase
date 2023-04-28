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

int ObInnerTableSchema::check_constraints_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_CHECK_CONSTRAINTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CHECK_CONSTRAINTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT CAST('def' AS CHAR(64)) AS CONSTRAINT_CATALOG,            CAST(d.database_name AS CHAR(128)) AS CONSTRAINT_SCHEMA,            CAST(c.constraint_name AS CHAR(64)) AS CONSTRAINT_NAME,            CAST(c.check_expr AS CHAR(2048)) AS CHECK_CLAUSE     FROM oceanbase.__all_database d     JOIN oceanbase.__all_table t ON d.database_id = t.database_id     JOIN oceanbase.__all_constraint c ON t.table_id = c.table_id     WHERE d.database_id > 500000 and d.in_recyclebin = 0       AND t.table_type = 3       AND c.constraint_type = 3   )__"))) {
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

int ObInnerTableSchema::referential_constraints_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_REFERENTIAL_CONSTRAINTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_REFERENTIAL_CONSTRAINTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(      select     CAST('def' AS CHAR(64)) AS CONSTRAINT_CATALOG,     CAST(cd.database_name AS CHAR(128)) AS CONSTRAINT_SCHEMA,     CAST(f.foreign_key_name AS CHAR(128)) AS CONSTRAINT_NAME,     CAST('def' AS CHAR(64)) AS UNIQUE_CONSTRAINT_CATALOG,     CAST(pd.database_name AS CHAR(128)) AS UNIQUE_CONSTRAINT_SCHEMA,     CAST(CASE WHEN f.ref_cst_type = 1 THEN 'PRIMARY'          ELSE NULL END AS CHAR(128)) AS UNIQUE_CONSTRAINT_NAME,     CAST('NONE' AS CHAR(64)) AS MATCH_OPTION,     CAST(CASE WHEN f.update_action = 1 THEN 'RESTRICT'               WHEN f.update_action = 2 THEN 'CASCADE'               WHEN f.update_action = 3 THEN 'SET_NULL'               WHEN f.update_action = 4 THEN 'NO_ACTION'               WHEN f.update_action = 5 THEN 'SET_DEFAULT'          ELSE NULL END AS CHAR(64)) AS UPDATE_RULE,     CAST(CASE WHEN f.delete_action = 1 THEN 'RESTRICT'               WHEN f.delete_action = 2 THEN 'CASCADE'               WHEN f.delete_action = 3 THEN 'SET_NULL'               WHEN f.delete_action = 4 THEN 'NO_ACTION'               WHEN f.delete_action = 5 THEN 'SET_DEFAULT'          ELSE NULL END AS CHAR(64)) AS DELETE_RULE,     CAST(ct.table_name AS CHAR(256)) AS TABLE_NAME,     CAST(pt.table_name AS CHAR(256)) AS REFERENCED_TABLE_NAME     FROM oceanbase.__all_foreign_key f     JOIN oceanbase.__all_table ct on f.child_table_id = ct.table_id and f.is_parent_table_mock = 0 and f.ref_cst_type != 2     JOIN oceanbase.__all_database cd on ct.database_id = cd.database_id     JOIN oceanbase.__all_table pt on f.parent_table_id = pt.table_id     JOIN oceanbase.__all_database pd on pt.database_id = pd.database_id     WHERE cd.database_id > 500000 and cd.in_recyclebin = 0       AND ct.table_type = 3      union all      select     CAST('def' AS CHAR(64)) AS CONSTRAINT_CATALOG,     CAST(cd.database_name AS CHAR(128)) AS CONSTRAINT_SCHEMA,     CAST(f.foreign_key_name AS CHAR(128)) AS CONSTRAINT_NAME,     CAST('def' AS CHAR(64)) AS UNIQUE_CONSTRAINT_CATALOG,     CAST(pd.database_name AS CHAR(128)) AS UNIQUE_CONSTRAINT_SCHEMA,     CAST(SUBSTR(it.table_name, 7 + INSTR(SUBSTR(it.table_name, 7), '_')) AS CHAR(128)) AS UNIQUE_CONSTRAINT_NAME,     CAST('NONE' AS CHAR(64)) AS MATCH_OPTION,     CAST(CASE WHEN f.update_action = 1 THEN 'RESTRICT'               WHEN f.update_action = 2 THEN 'CASCADE'               WHEN f.update_action = 3 THEN 'SET_NULL'               WHEN f.update_action = 4 THEN 'NO_ACTION'               WHEN f.update_action = 5 THEN 'SET_DEFAULT'          ELSE NULL END AS CHAR(64)) AS UPDATE_RULE,     CAST(CASE WHEN f.delete_action = 1 THEN 'RESTRICT'               WHEN f.delete_action = 2 THEN 'CASCADE'               WHEN f.delete_action = 3 THEN 'SET_NULL'               WHEN f.delete_action = 4 THEN 'NO_ACTION'               WHEN f.delete_action = 5 THEN 'SET_DEFAULT'          ELSE NULL END AS CHAR(64)) AS DELETE_RULE,     CAST(ct.table_name AS CHAR(256)) AS TABLE_NAME,     CAST(pt.table_name AS CHAR(256)) AS REFERENCED_TABLE_NAME     FROM oceanbase.__all_foreign_key f     JOIN oceanbase.__all_table ct on f.child_table_id = ct.table_id and f.is_parent_table_mock = 0 and f.ref_cst_type = 2     JOIN oceanbase.__all_database cd on ct.database_id = cd.database_id     JOIN oceanbase.__all_table pt on f.parent_table_id = pt.table_id     JOIN oceanbase.__all_database pd on pt.database_id = pd.database_id     JOIN oceanbase.__all_table it on f.ref_cst_id = it.table_id     WHERE cd.database_id > 500000 and cd.in_recyclebin = 0       AND ct.table_type = 3      union all      select     CAST('def' AS CHAR(64)) AS CONSTRAINT_CATALOG,     CAST(cd.database_name AS CHAR(128)) AS CONSTRAINT_SCHEMA,     CAST(f.foreign_key_name AS CHAR(128)) AS CONSTRAINT_NAME,     CAST('def' AS CHAR(64)) AS UNIQUE_CONSTRAINT_CATALOG,     CAST(pd.database_name AS CHAR(128)) AS UNIQUE_CONSTRAINT_SCHEMA,     CAST(NULL AS CHAR(128)) AS UNIQUE_CONSTRAINT_NAME,     CAST('NONE' AS CHAR(64)) AS MATCH_OPTION,     CAST(CASE WHEN f.update_action = 1 THEN 'RESTRICT'               WHEN f.update_action = 2 THEN 'CASCADE'               WHEN f.update_action = 3 THEN 'SET_NULL'               WHEN f.update_action = 4 THEN 'NO_ACTION'               WHEN f.update_action = 5 THEN 'SET_DEFAULT'          ELSE NULL END AS CHAR(64)) AS UPDATE_RULE,     CAST(CASE WHEN f.delete_action = 1 THEN 'RESTRICT'               WHEN f.delete_action = 2 THEN 'CASCADE'               WHEN f.delete_action = 3 THEN 'SET_NULL'               WHEN f.delete_action = 4 THEN 'NO_ACTION'               WHEN f.delete_action = 5 THEN 'SET_DEFAULT'          ELSE NULL END AS CHAR(64)) AS DELETE_RULE,     CAST(ct.table_name AS CHAR(256)) AS TABLE_NAME,     CAST(pt.mock_fk_parent_table_name AS CHAR(256)) AS REFERENCED_TABLE_NAME     FROM oceanbase.__all_foreign_key f     JOIN oceanbase.__all_table ct on f.child_table_id = ct.table_id and f.is_parent_table_mock = 1     JOIN oceanbase.__all_database cd on ct.database_id = cd.database_id     JOIN oceanbase.__all_mock_fk_parent_table pt on f.parent_table_id = pt.mock_fk_parent_table_id     JOIN oceanbase.__all_database pd on pt.database_id = pd.database_id     WHERE cd.database_id > 500000 and cd.in_recyclebin = 0       AND ct.table_type = 3   )__"))) {
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

int ObInnerTableSchema::table_constraints_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_TABLE_CONSTRAINTS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TABLE_CONSTRAINTS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(      SELECT            CAST('def' AS CHAR(64)) AS CONSTRAINT_CATALOG,            CAST(d.database_name AS CHAR(128)) AS CONSTRAINT_SCHEMA,            CAST('PRIMARY' AS CHAR(256)) AS CONSTRAINT_NAME,            CAST(d.database_name AS CHAR(128)) AS TABLE_SCHEMA,            CAST(t.table_name AS CHAR(256)) AS TABLE_NAME,            CAST('PRIMARY KEY' AS CHAR(11)) AS CONSTRAINT_TYPE,            CAST('YES' AS CHAR(3)) AS ENFORCED     FROM oceanbase.__all_database d     JOIN oceanbase.__all_table t ON d.database_id = t.database_id     WHERE (d.database_id = 201003 OR d.database_id > 500000) AND d.in_recyclebin = 0       AND t.table_type = 3       AND t.table_mode >> 16 & 1 = 0      union all      SELECT            CAST('def' AS CHAR(64)) AS CONSTRAINT_CATALOG,            CAST(d.database_name AS CHAR(128)) AS CONSTRAINT_SCHEMA,            CAST(SUBSTR(it.table_name, 7 + INSTR(SUBSTR(it.table_name, 7), '_')) AS CHAR(256)) AS CONSTRAINT_NAME,            CAST(d.database_name AS CHAR(128)) AS TABLE_SCHEMA,            CAST(ut.table_name AS CHAR(256)) AS TABLE_NAME,            CAST('UNIQUE' AS CHAR(11)) AS CONSTRAINT_TYPE,            CAST('YES' AS CHAR(3)) AS ENFORCED     FROM oceanbase.__all_database d     JOIN oceanbase.__all_table it ON d.database_id = it.database_id     JOIN oceanbase.__all_table ut ON it.data_table_id = ut.table_id     WHERE d.database_id > 500000 AND d.in_recyclebin = 0       AND it.table_type = 5       AND it.index_type IN (2, 4, 8)      union all      SELECT            CAST('def' AS CHAR(64)) AS CONSTRAINT_CATALOG,            CAST(d.database_name AS CHAR(128)) AS CONSTRAINT_SCHEMA,            CAST(c.constraint_name AS CHAR(256)) AS CONSTRAINT_NAME,            CAST(d.database_name AS CHAR(128)) AS TABLE_SCHEMA,            CAST(t.table_name AS CHAR(256)) AS TABLE_NAME,            CAST('CHECK' AS CHAR(11)) AS CONSTRAINT_TYPE,            CAST(CASE WHEN c.enable_flag = 1 THEN 'YES'                 ELSE 'NO' END AS CHAR(3)) AS ENFORCED     FROM oceanbase.__all_database d     JOIN oceanbase.__all_table t ON d.database_id = t.database_id     JOIN oceanbase.__all_constraint c ON t.table_id = c.table_id     WHERE d.database_id > 500000 AND d.in_recyclebin = 0       AND t.table_type = 3       AND c.constraint_type = 3      union all      SELECT            CAST('def' AS CHAR(64)) AS CONSTRAINT_CATALOG,            CAST(f.constraint_schema AS CHAR(128)) AS CONSTRAINT_SCHEMA,            CAST(f.constraint_name AS CHAR(256)) AS CONSTRAINT_NAME,            CAST(f.constraint_schema AS CHAR(128)) AS TABLE_SCHEMA,            CAST(f.table_name AS CHAR(256)) AS TABLE_NAME,            CAST('FOREIGN KEY' AS CHAR(11)) AS CONSTRAINT_TYPE,            CAST('YES' AS CHAR(3)) AS ENFORCED     FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS f    )__"))) {
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

int ObInnerTableSchema::gv_ob_transaction_schedulers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_TRANSACTION_SCHEDULERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_TRANSACTION_SCHEDULERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT     tenant_id AS TENANT_ID,     svr_ip AS SVR_IP,     svr_port AS SVR_PORT,     session_id AS SESSION_ID,     trans_id AS TX_ID,     CASE       WHEN state = 0 THEN 'INVALID'       WHEN state = 1 THEN 'IDLE'       WHEN state = 2 THEN 'EXPLICIT_ACTIVE'       WHEN state = 3 THEN 'IMPLICIT_ACTIVE'       WHEN state = 4 THEN 'ROLLBACK_SAVEPOINT'       WHEN state = 5 THEN 'IN_TERMINATE'       WHEN state = 6 THEN 'ABORTED'       WHEN state = 7 THEN 'ROLLED_BACK'       WHEN state = 8 THEN 'COMMIT_TIMEOUT'       WHEN state = 9 THEN 'COMMIT_UNKNOWN'       WHEN state = 10 THEN 'COMMITTED'       WHEN state = 11 THEN 'SUB_PREPARING'       WHEN state = 12 THEN 'SUB_PREPARED'       WHEN state = 13 THEN 'SUB_COMMITTING'       WHEN state = 14 THEN 'SUB_COMMITTED'       WHEN state = 15 THEN 'SUB_ROLLBACKING'       WHEN state = 16 THEN 'SUB_ROLLBACKED'       ELSE 'UNKNOWN'       END AS STATE,     cluster_id AS CLUSTER_ID,     coordinator AS COORDINATOR,     participants AS PARTICIPANTS,     CASE       WHEN isolation_level = -1 THEN 'INVALID'       WHEN isolation_level = 0 THEN 'READ UNCOMMITTED'       WHEN isolation_level = 1 THEN 'READ COMMITTED'       WHEN isolation_level = 2 THEN 'REPEATABLE READ'       WHEN isolation_level = 3 THEN 'SERIALIZABLE'       ELSE 'UNKNOWN'       END AS ISOLATION_LEVEL,     snapshot_version AS SNAPSHOT_VERSION,     CASE       WHEN access_mode = -1 THEN 'INVALID'       WHEN access_mode = 0 THEN 'READ_WRITE'       WHEN access_mode = 1 THEN 'READ_ONLY'       ELSE 'UNKNOWN'       END AS ACCESS_MODE,     tx_op_sn AS TX_OP_SN,     active_time AS ACTIVE_TIME,     expire_time AS EXPIRE_TIME,     CASE       WHEN can_early_lock_release = 0 THEN 'FALSE'       WHEN can_early_lock_release = 1 THEN 'TRUE'       ELSE 'UNKNOWN'       END AS CAN_EARLY_LOCK_RELEASE,     format_id AS FORMATID,     HEX(gtrid) AS GLOBALID,     HEX(bqual) AS BRANCHID     FROM oceanbase.__all_virtual_trans_scheduler )__"))) {
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

int ObInnerTableSchema::v_ob_transaction_schedulers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_TRANSACTION_SCHEDULERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_TRANSACTION_SCHEDULERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT *     FROM OCEANBASE.GV$OB_TRANSACTION_SCHEDULERS     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT() )__"))) {
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

int ObInnerTableSchema::triggers_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_TRIGGERS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TRIGGERS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT CAST('def' AS CHAR(512)) AS TRIGGER_CATALOG,       CAST(db.database_name AS CHAR(64)) AS TRIGGER_SCHEMA,       CAST(trg.trigger_name AS CHAR(64)) AS TRIGGER_NAME,       CAST((case when trg.trigger_events=1 then 'INSERT'                 when trg.trigger_events=2 then 'UPDATE'                 when trg.trigger_events=4 then 'DELETE' end)             AS CHAR(6)) AS EVENT_MANIPULATION,       CAST('def' AS CHAR(512)) AS EVENT_OBJECT_CATALOG,       CAST(db.database_name AS CHAR(64)) AS EVENT_OBJECT_SCHEMA,       CAST(t.table_name AS CHAR(64)) AS EVENT_OBJECT_TABLE,       CAST(trg.action_order AS SIGNED) AS ACTION_ORDER,       CAST(NULL AS CHAR(4194304)) AS ACTION_CONDITION,       CAST(trg.trigger_body AS CHAR(4194304)) AS ACTION_STATEMENT,       CAST('ROW' AS CHAR(9)) AS ACTION_ORIENTATION,       CAST((case when trg.TIMING_POINTS=4 then 'BEFORE'                 when trg.TIMING_POINTS=8 then 'AFTER' end)             AS CHAR(6)) AS ACTION_TIMING,       CAST(NULL AS CHAR(64)) AS ACTION_REFERENCE_OLD_TABLE,       CAST(NULL AS CHAR(64)) AS ACTION_REFERENCE_NEW_TABLE,       CAST('OLD' AS CHAR(3)) AS ACTION_REFERENCE_OLD_ROW,       CAST('NEW' AS CHAR(3)) AS ACTION_REFERENCE_NEW_ROW,       CAST(trg.gmt_create AS DATETIME(2)) AS CREATED,       CAST(sql_mode_convert(trg.sql_mode) AS CHAR(8192)) AS SQL_MODE,       CAST(trg.trigger_priv_user AS CHAR(93)) AS DEFINER,       CAST((select charset from oceanbase.__tenant_virtual_collation           where id = substring_index(substring_index(trg.package_exec_env, ',', 2), ',', -1)) AS CHAR(32)             ) AS CHARACTER_SET_CLIENT,       CAST((select collation from oceanbase.__tenant_virtual_collation             where collation_type = substring_index(substring_index(trg.package_exec_env, ',', 3), ',', -1)) AS CHAR(32)             ) AS COLLATION_CONNECTION,       CAST((select collation from oceanbase.__tenant_virtual_collation             where collation_type = substring_index(substring_index(trg.package_exec_env, ',', 4), ',', -1)) AS CHAR(32)             ) AS DATABASE_COLLATION       FROM oceanbase.__all_tenant_trigger trg           JOIN oceanbase.__all_database db on trg.database_id = db.database_id           JOIN oceanbase.__all_table t on trg.base_object_id = t.table_id       WHERE db.database_name != '__recyclebin' and db.in_recyclebin = 0 )__"))) {
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

int ObInnerTableSchema::partitions_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_INFORMATION_SCHEMA_ID);
  table_schema.set_table_id(OB_PARTITIONS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_PARTITIONS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT   CAST('def' as CHAR(4096)) AS TABLE_CATALOG,   DB.DATABASE_NAME AS TABLE_SCHEMA,   T.TABLE_NAME AS TABLE_NAME,   P.PART_NAME AS PARTITION_NAME,   SP.SUB_PART_NAME AS SUBPARTITION_NAME,   CAST(PART_POSITION AS UNSIGNED) AS PARTITION_ORDINAL_POSITION,   CAST(SUB_PART_POSITION AS UNSIGNED) AS SUBPARTITION_ORDINAL_POSITION,   CAST(CASE WHEN T.PART_LEVEL = 0             THEN NULL             ELSE (CASE T.PART_FUNC_TYPE                     WHEN 0 THEN 'HASH'                     WHEN 1 THEN 'KEY'                     WHEN 2 THEN 'KEY'                     WHEN 3 THEN 'RANGE'                     WHEN 4 THEN 'RANGE COLUMNS'                     WHEN 5 THEN 'LIST'                     WHEN 6 THEN 'LIST COLUMNS'                     WHEN 7 THEN 'RANGE'                   END)        END AS CHAR(13)) PARTITION_METHOD,   CAST(CASE WHEN (T.PART_LEVEL = 0 OR T.PART_LEVEL = 1)             THEN NULL             ELSE (CASE T.SUB_PART_FUNC_TYPE                     WHEN 0 THEN 'HASH'                     WHEN 1 THEN 'KEY'                     WHEN 2 THEN 'KEY'                     WHEN 3 THEN 'RANGE'                     WHEN 4 THEN 'RANGE COLUMNS'                     WHEN 5 THEN 'LIST'                     WHEN 6 THEN 'LIST COLUMNS'                     WHEN 7 THEN 'RANGE'                   END)        END AS CHAR(13)) SUBPARTITION_METHOD,   CAST(CASE WHEN (T.PART_LEVEL = 0)             THEN NULL             ELSE T.PART_FUNC_EXPR        END AS CHAR(2048)) PARTITION_EXPRESSION,   CAST(CASE WHEN (T.PART_LEVEL = 0 OR T.PART_LEVEL = 1)             THEN NULL             ELSE T.SUB_PART_FUNC_EXPR        END AS CHAR(2048)) SUBPARTITION_EXPRESSION,   CAST(CASE WHEN (T.PART_LEVEL = 0)             THEN NULL             ELSE (CASE WHEN LENGTH(P.HIGH_BOUND_VAL) > 0                        THEN P.HIGH_BOUND_VAL                        ELSE P.LIST_VAL                   END)        END AS CHAR(4096)) AS PARTITION_DESCRIPTION,   CAST(CASE WHEN (T.PART_LEVEL = 0 OR T.PART_LEVEL = 1)             THEN NULL             ELSE (CASE WHEN LENGTH(SP.HIGH_BOUND_VAL) > 0                        THEN SP.HIGH_BOUND_VAL                        ELSE SP.LIST_VAL                   END)        END AS CHAR(4096)) AS SUBPARTITION_DESCRIPTION,   CAST(TS.ROW_CNT AS UNSIGNED) AS TABLE_ROWS,   CAST(TS.AVG_ROW_LEN AS UNSIGNED) AS AVG_ROW_LENGTH,   CAST(NULL AS UNSIGNED) AS DATA_LENGTH,   CAST(NULL AS UNSIGNED) AS MAX_DATA_LENGTH,   CAST(NULL AS UNSIGNED) AS INDEX_LENGTH,   CAST(NULL AS UNSIGNED) AS DATA_FREE,   CASE T.PART_LEVEL     WHEN 0 THEN T.GMT_CREATE     WHEN 1 THEN P.GMT_CREATE     WHEN 2 THEN SP.GMT_CREATE   END AS CREATE_TIME,   CAST(NULL AS DATETIME) AS UPDATE_TIME,   CAST(NULL AS DATETIME) AS CHECK_TIME,   CAST(NULL AS SIGNED) AS CHECKSUM,   CAST(CASE T.PART_LEVEL          WHEN 0 THEN NULL          WHEN 1 THEN P.COMMENT          WHEN 2 THEN SP.COMMENT        END AS CHAR(1024)) AS PARTITION_COMMENT,   CAST('default' AS CHAR(256)) NODEGROUP,   CAST(TP.TABLESPACE_NAME AS CHAR(268)) AS TABLESPACE_NAME FROM   OCEANBASE.__ALL_TABLE T   JOIN OCEANBASE.__ALL_DATABASE DB ON T.DATABASE_ID = DB.DATABASE_ID AND T.TENANT_ID = DB.TENANT_ID   LEFT JOIN (       SELECT         TENANT_ID,         TABLE_ID,         PART_ID,         PART_NAME,         HIGH_BOUND_VAL,         LIST_VAL,         TABLESPACE_ID,         GMT_CREATE,         COMMENT,         ROW_NUMBER() OVER(PARTITION BY TENANT_ID,TABLE_ID ORDER BY PART_IDX) AS PART_POSITION       FROM OCEANBASE.__ALL_PART   ) P ON T.TABLE_ID = P.TABLE_ID AND T.TENANT_ID = P.TENANT_ID   LEFT JOIN (     SELECT         TENANT_ID,         TABLE_ID,         PART_ID,         SUB_PART_ID,         SUB_PART_NAME,         HIGH_BOUND_VAL,         LIST_VAL,         TABLESPACE_ID,         GMT_CREATE,         COMMENT,         ROW_NUMBER() OVER(PARTITION BY TENANT_ID,TABLE_ID,PART_ID ORDER BY SUB_PART_IDX) AS SUB_PART_POSITION     FROM OCEANBASE.__ALL_SUB_PART   ) SP ON T.TABLE_ID = SP.TABLE_ID AND P.PART_ID = SP.PART_ID AND T.TENANT_ID = SP.TENANT_ID    LEFT JOIN OCEANBASE.__ALL_TENANT_TABLESPACE TP ON TP.TABLESPACE_ID = IFNULL(SP.TABLESPACE_ID, P.TABLESPACE_ID) AND TP.TENANT_ID = T.TENANT_ID   LEFT JOIN OCEANBASE.__ALL_TABLE_STAT TS ON T.TENANT_ID = TS.TENANT_ID AND TS.TABLE_ID = T.TABLE_ID AND TS.PARTITION_ID = CASE T.PART_LEVEL WHEN 0 THEN T.TABLE_ID WHEN 1 THEN P.PART_ID WHEN 2 THEN SP.SUB_PART_ID END WHERE T.TABLE_TYPE IN (3,6,8,9)   )__"))) {
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

int ObInnerTableSchema::dba_ob_arbitration_service_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_ARBITRATION_SERVICE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_ARBITRATION_SERVICE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(GMT_CREATE AS DATETIME) AS CREATE_TIME,          CAST(GMT_MODIFIED AS DATETIME) AS MODIFY_TIME,          ARBITRATION_SERVICE_KEY,          ARBITRATION_SERVICE,          PREVIOUS_ARBITRATION_SERVICE,          TYPE   FROM OCEANBASE.__all_arbitration_service;   )__"))) {
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

int ObInnerTableSchema::cdb_ob_ls_arb_replica_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_LS_ARB_REPLICA_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_LS_ARB_REPLICA_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(GMT_CREATE AS DATETIME) AS CREATE_TIME,          CAST(GMT_MODIFIED AS DATETIME) AS MODIFY_TIME,          TENANT_ID,          LS_ID,          TASK_ID,          TRACE_ID,          TASK_TYPE,          ARBITRATION_SERVICE,          ARBITRATION_SERVICE_TYPE,          COMMENT   FROM OCEANBASE.__ALL_VIRTUAL_LS_ARB_REPLICA_TASK   )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_arb_replica_tasks_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_ARB_REPLICA_TASKS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_ARB_REPLICA_TASKS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT CAST(GMT_CREATE AS DATETIME) AS CREATE_TIME,          CAST(GMT_MODIFIED AS DATETIME) AS MODIFY_TIME,          TENANT_ID,          LS_ID,          TASK_ID,          TRACE_ID,          TASK_TYPE,          ARBITRATION_SERVICE,          ARBITRATION_SERVICE_TYPE,          COMMENT   FROM OCEANBASE.__ALL_VIRTUAL_LS_ARB_REPLICA_TASK   WHERE     TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::cdb_ob_ls_arb_replica_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_LS_ARB_REPLICA_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_LS_ARB_REPLICA_TASK_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          LS_ID,          TASK_ID,          EXECUTE_RESULT,          CAST(CREATE_TIME AS DATETIME) AS CREATE_TIME,          CAST(FINISH_TIME AS DATETIME) AS FINISH_TIME,          TRACE_ID,          TASK_TYPE,          ARBITRATION_SERVICE,          ARBITRATION_SERVICE_TYPE,          COMMENT   FROM OCEANBASE.__ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY   )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_arb_replica_task_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          LS_ID,          TASK_ID,          EXECUTE_RESULT,          CAST(CREATE_TIME AS DATETIME) AS CREATE_TIME,          CAST(FINISH_TIME AS DATETIME) AS FINISH_TIME,          TRACE_ID,          TASK_TYPE,          ARBITRATION_SERVICE,          ARBITRATION_SERVICE_TYPE,          COMMENT   FROM OCEANBASE.__ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY   WHERE     TENANT_ID = EFFECTIVE_TENANT_ID()   )__"))) {
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

int ObInnerTableSchema::v_ob_archive_dest_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_ARCHIVE_DEST_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_ARCHIVE_DEST_STATUS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          DEST_ID,          PATH,          STATUS,          CHECKPOINT_SCN,          SYNCHRONIZED,          COMMENT   FROM OCEANBASE.__all_virtual_archive_dest_status;   )__"))) {
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

int ObInnerTableSchema::dba_ob_ls_log_archive_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT DEST_ID,          LS_ID,          ROUND_ID,          PIECE_ID,          INCARNATION,          START_SCN,          MIN_LSN,          MAX_LSN,          CHECKPOINT_SCN,          STATUS,          FILE_ID,          FILE_OFFSET,          INPUT_BYTES,          OUTPUT_BYTES   FROM OCEANBASE.__ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS   WHERE TENANT_ID=EFFECTIVE_TENANT_ID();   )__"))) {
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

int ObInnerTableSchema::cdb_ob_ls_log_archive_progress_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_CDB_OB_LS_LOG_ARCHIVE_PROGRESS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_CDB_OB_LS_LOG_ARCHIVE_PROGRESS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(   SELECT TENANT_ID,          DEST_ID,          LS_ID,          ROUND_ID,          PIECE_ID,          INCARNATION,          START_SCN,          MIN_LSN,          MAX_LSN,          CHECKPOINT_SCN,          STATUS,          FILE_ID,          FILE_OFFSET,          INPUT_BYTES,          OUTPUT_BYTES   FROM OCEANBASE.__ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS;   )__"))) {
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

int ObInnerTableSchema::dba_ob_rsrc_io_directives_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_RSRC_IO_DIRECTIVES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_RSRC_IO_DIRECTIVES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT       PLAN,       GROUP_OR_SUBPLAN,       COMMENTS,       MIN_IOPS,       MAX_IOPS,       WEIGHT_IOPS     FROM        oceanbase.__all_res_mgr_directive )__"))) {
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

int ObInnerTableSchema::gv_ob_opt_stat_gather_monitor_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_OPT_STAT_GATHER_MONITOR_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_OPT_STAT_GATHER_MONITOR_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(SELECT           CAST(TENANT_ID AS SIGNED) AS TENANT_ID,           CAST(SVR_IP AS CHAR(46)) AS SVR_IP,           CAST(SVR_PORT AS SIGNED) AS SVR_PORT,           CAST(SESSION_ID AS SIGNED) AS SESSION_ID,           CAST(TRACE_ID AS CHAR(64)) AS TRACE_ID,           CAST(TASK_ID AS CHAR(36)) AS TASK_ID,           CAST((CASE WHEN TYPE = 0 THEN 'MANUAL GATHER' ELSE                 (CASE WHEN TYPE = 1 THEN 'AUTO GATHER' ELSE 'UNDEFINED GATHER' END) END) AS CHAR(16)) AS TYPE,           CAST(TASK_START_TIME AS DATETIME(6)) AS TASK_START_TIME,           CAST(TASK_DURATION_TIME AS SIGNED) AS TASK_DURATION_TIME,           CAST(TASK_TABLE_COUNT AS SIGNED) AS TASK_TABLE_COUNT,           CAST(COMPLETED_TABLE_COUNT AS SIGNED) AS COMPLETED_TABLE_COUNT,           CAST(RUNNING_TABLE_OWNER AS CHAR(128)) AS RUNNING_TABLE_OWNER,           CAST(RUNNING_TABLE_NAME AS CHAR(256)) AS RUNNING_TABLE_NAME,           CAST(RUNNING_TABLE_DURATION_TIME AS SIGNED) AS RUNNING_TABLE_DURATION_TIME           FROM oceanbase.__all_virtual_opt_stat_gather_monitor )__"))) {
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

int ObInnerTableSchema::v_ob_opt_stat_gather_monitor_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_OPT_STAT_GATHER_MONITOR_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_OPT_STAT_GATHER_MONITOR_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT * FROM oceanbase.GV$OB_OPT_STAT_GATHER_MONITOR WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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

int ObInnerTableSchema::dba_ob_task_opt_stat_gather_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(     SELECT         CAST(TENANT_ID           AS     SIGNED) AS TENANT_ID,         CAST(TASK_ID             AS     CHAR(36)) AS TASK_ID,         CAST((CASE  WHEN type = 0 THEN 'MANUAL GATHER'                ELSE ( CASE  WHEN type = 1 THEN 'AUTO GATHER'                          ELSE ( CASE  WHEN type IS NULL THEN NULL                                   ELSE 'UNDEFINED GATHER' END )END ) END ) AS CHAR(16)) AS TYPE,         CAST((CASE WHEN RET_CODE = 0 THEN 'SUCCESS' ELSE 'FAILED' END) AS CHAR(8)) AS STATUS,         CAST(TABLE_COUNT         AS     SIGNED) AS TABLE_COUNT,         CAST(FAILED_COUNT        AS     SIGNED) AS FAILED_COUNT,         CAST(START_TIME          AS     DATETIME(6)) AS START_TIME,         CAST(END_TIME            AS     DATETIME(6)) AS END_TIME     FROM         oceanbase.__all_virtual_task_opt_stat_gather_history     WHERE TENANT_ID = EFFECTIVE_TENANT_ID() )__"))) {
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

int ObInnerTableSchema::dba_ob_table_opt_stat_gather_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__(         SELECT         CAST(DB.DATABASE_NAME         AS     CHAR(128)) AS OWNER,         CAST(V.TABLE_NAME             AS     CHAR(256)) AS TABLE_NAME,         CAST(STAT.TASK_ID             AS     CHAR(36)) AS TASK_ID,         CAST((CASE WHEN RET_CODE = 0 THEN 'SUCCESS' ELSE 'FAILED' END) AS CHAR(8)) AS STATUS,         CAST(STAT.START_TIME          AS     DATETIME(6)) AS START_TIME,         CAST(STAT.END_TIME            AS     DATETIME(6)) AS END_TIME,         CAST(STAT.MEMORY_USED         AS     SIGNED) AS MEMORY_USED,         CAST(STAT.STAT_REFRESH_FAILED_LIST      AS     CHAR(4096)) AS STAT_REFRESH_FAILED_LIST,         CAST(STAT.PROPERTIES       AS     CHAR(4096)) AS PROPERTIES         FROM         (           (SELECT CAST(0 AS SIGNED) AS TENANT_ID,                   DATABASE_ID,                   TABLE_ID,                   TABLE_NAME               FROM                 OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE               WHERE TENANT_ID = EFFECTIVE_TENANT_ID()             UNION ALL             SELECT TENANT_ID,                   DATABASE_ID,                   TABLE_ID,                   TABLE_NAME             FROM                 oceanbase.__all_table T             WHERE T.TABLE_TYPE IN (0,2,3,6))         ) V         JOIN             oceanbase.__all_database DB             ON DB.TENANT_ID = V.TENANT_ID             AND DB.DATABASE_ID = V.DATABASE_ID             AND V.TENANT_ID = 0         LEFT JOIN             oceanbase.__all_virtual_table_opt_stat_gather_history STAT             ON STAT.TENANT_ID = EFFECTIVE_TENANT_ID()             AND V.TABLE_ID = STAT.TABLE_ID )__"))) {
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

int ObInnerTableSchema::gv_ob_thread_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_GV_OB_THREAD_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GV_OB_THREAD_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT svr_ip AS SVR_IP,        svr_port AS SVR_PORT,        tenant_id AS TENANT_ID,        tid AS TID,        tname AS TNAME,        status AS STATUS,        latch_wait AS LATCH_WAIT,        latch_hold AS LATCH_HOLD,        trace_id AS TRACE_ID FROM oceanbase.__all_virtual_thread )__"))) {
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

int ObInnerTableSchema::v_ob_thread_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_V_OB_THREAD_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_VIEW);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_V_OB_THREAD_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_view_definition(R"__( SELECT   * FROM oceanbase.GV$OB_THREAD WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT() )__"))) {
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
