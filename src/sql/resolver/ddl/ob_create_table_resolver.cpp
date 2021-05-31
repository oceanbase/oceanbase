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

#define USING_LOG_PREFIX SQL_RESV
#include "lib/compress/ob_compressor_pool.h"
#include "sql/resolver/ddl/ob_create_table_resolver.h"
#include <algorithm>
#include "lib/number/ob_number_v2.h"
#include "lib/string/ob_sql_string.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/ob_store_format.h"
#include "share/schema/ob_table_schema.h"
#include "share/config/ob_server_config.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_part_func_checker.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/ob_sql_utils.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/ob_select_stmt_printer.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace omt;
namespace sql {
ObCreateTableResolver::ObCreateTableResolver(ObResolverParams& params)
    : ObDDLResolver(params),
      cur_column_id_(OB_APP_MIN_COLUMN_ID - 1),
      primary_keys_(),
      column_name_set_(),
      if_not_exist_(false),
      is_oracle_temp_table_(false),
      index_arg_(),
      current_index_name_set_()
{}

ObCreateTableResolver::~ObCreateTableResolver()
{}

uint64_t ObCreateTableResolver::gen_column_id()
{
  return ++cur_column_id_;
}

int64_t ObCreateTableResolver::get_primary_key_size() const
{
  return primary_keys_.count();
}

int ObCreateTableResolver::add_primary_key_part(
    const ObString& column_name, ObArray<ObColumnResolveStat>& stats, int64_t& pk_data_length)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  if (OB_ISNULL(create_table_stmt)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "stmt_ is null.", K(ret));
  } else {
    ObColumnSchemaV2* col = NULL;
    ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
    col = table_schema.get_column_schema(column_name);
    if (OB_ISNULL(col)) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
      SQL_RESV_LOG(WARN, "column '%s' does not exists", K(ret), K(to_cstring(column_name)));
    } else if (ob_is_text_tc(col->get_data_type())) {
      ret = OB_ERR_WRONG_KEY_COLUMN;
      LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
    } else if (ObTimestampTZType == col->get_data_type()) {
      ret = OB_ERR_WRONG_KEY_COLUMN;
      LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
    } else if (col->is_generated_column()) {
      ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
      LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN, "Defining a generated column as primary key");
    } else { /*do nothing*/
    }
    if (OB_SUCC(ret)) {
      int64_t index = -1;
      bool is_found = false;
      for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < stats.count(); ++i) {
        if (stats.at(i).column_id_ == col->get_column_id()) {
          index = i;
          is_found = true;
        }
      }
      if (-1 == index) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "fail to find column stat", K(ret), K(column_name));
      } else if (!is_oracle_mode()) {
        if (stats.at(index).is_set_null_ ||
            (stats.at(index).is_set_default_value_ && col->get_cur_default_value().is_null())) {
          ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
        }
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (col->get_rowkey_position() > 0) {
        ret = OB_ERR_COLUMN_DUPLICATE;
        LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
      } else if (OB_USER_MAX_ROWKEY_COLUMN_NUMBER == primary_keys_.count()) {
        ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
        LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
      } else if (col->is_string_type()) {
        int64_t length = 0;
        if (OB_FAIL(col->get_byte_length(length))) {
          SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
        } else if ((pk_data_length += length) > OB_MAX_USER_ROW_KEY_LENGTH) {
          ret = OB_ERR_TOO_LONG_KEY_LENGTH;
          LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
        } else if (length <= 0) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
        } else {
          // do nothing
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(primary_keys_.push_back(col->get_column_id()))) {
        SQL_RESV_LOG(WARN, "push primary key to array failed", K(ret));
      } else {
        col->set_rowkey_position(primary_keys_.count());
        col->set_nullable(false);
        ret = table_schema.set_rowkey_info(*col);
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::add_hidden_primary_key()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "stmt is NULL", K(stmt_), K(ret));
  } else if (0 == get_primary_key_size()) {
    ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
    ObColumnSchemaV2 hidden_pk;
    for (int i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(HIDDEN_PK_COLUMN_IDS); ++i) {
      hidden_pk.reset();
      hidden_pk.set_column_id(HIDDEN_PK_COLUMN_IDS[i]);  // reserved for hidden primary key
      hidden_pk.set_data_type(ObUInt64Type);
      hidden_pk.set_nullable(false);
      hidden_pk.set_autoincrement(OB_HIDDEN_PK_INCREMENT_COLUMN_ID == HIDDEN_PK_COLUMN_IDS[i]);
      hidden_pk.set_is_hidden(true);
      hidden_pk.set_charset_type(CHARSET_BINARY);
      hidden_pk.set_collation_type(CS_TYPE_BINARY);
      if (HIDDEN_PK_COLUMN_IDS[i] == OB_HIDDEN_PK_CLUSTER_COLUMN_ID) {
        int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
        ObObj default_value;
        default_value.set_int(cluster_id);
        if (OB_FAIL(hidden_pk.set_orig_default_value(default_value))) {
          SQL_RESV_LOG(WARN, "set_orig_default_value failed", K(ret));
        } else if (OB_FAIL(hidden_pk.set_cur_default_value(default_value))) {
          SQL_RESV_LOG(WARN, "set_cur_default_value failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(hidden_pk.set_column_name(HIDDEN_PK_COLUMN_NAMES[i]))) {
          SQL_RESV_LOG(WARN, "failed to set column name", K(ret), "column_id", HIDDEN_PK_COLUMN_IDS[i], K(i));
        } else if (OB_FAIL(primary_keys_.push_back(HIDDEN_PK_COLUMN_IDS[i]))) {
          SQL_RESV_LOG(WARN, "failed to push_back column_id", K(ret), "column_id", HIDDEN_PK_COLUMN_IDS[i], K(i));
        } else {
          hidden_pk.set_rowkey_position(primary_keys_.count());
          if (OB_FAIL(table_schema.add_column(hidden_pk))) {
            SQL_RESV_LOG(WARN, "add column to table_schema failed", K(ret), K(hidden_pk));
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::set_partitioning_key(
    ObTableSchema& table_schema, const ObPartitionKeyInfo& partition_key_info)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_INVALID_ID;
  ObColumnSchemaV2* column_schema = NULL;
  for (int i = 0; OB_SUCC(ret) && i < partition_key_info.get_size(); ++i) {
    if (OB_FAIL(partition_key_info.get_column_id(i, column_id))) {
      SQL_RESV_LOG(WARN, "failed to get column id", K(stmt_), K(ret), K(i));
    } else {
      column_schema = table_schema.get_column_schema(column_id);
      if (OB_USER_MAX_ROWKEY_COLUMN_NUMBER == primary_keys_.count()) {
        ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
        LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
      } else if (ob_is_text_tc(column_schema->get_data_type())) {
        ret = OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
        LOG_USER_ERROR(OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
            column_schema->get_column_name_str().length(),
            column_schema->get_column_name_str().ptr());
        SQL_RESV_LOG(
            WARN, "BLOB, TEXT column can't be partitioning key of no primary key table", K(column_schema), K(ret));
      } else if (!column_schema->is_rowkey_column()) {  // not a rowkey yet
        if (OB_FAIL(primary_keys_.push_back(column_schema->get_column_id()))) {
          SQL_RESV_LOG(WARN, "push primary key to array failed", K(ret));
        } else {
          column_schema->set_rowkey_position(primary_keys_.count());
          column_schema->add_column_flag(HEAP_ALTER_ROWKEY_FLAG);
          ret = table_schema.set_rowkey_info(*column_schema);
          SQL_RESV_LOG(DEBUG,
              "success to push primary key to array",
              K(ret),
              KPC(column_schema),
              K(primary_keys_),
              K(column_schema->is_heap_alter_rowkey_column()));
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::set_partitioning_key_and_seq()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "stmt is NULL", K(stmt_), K(ret));
  } else if (0 == get_primary_key_size()) {
    ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
    if (OB_FAIL(set_partitioning_key(table_schema, table_schema.get_partition_key_info()))) {
      SQL_RESV_LOG(WARN, "failed to set partition key", K(stmt_), K(ret));
    } else if (OB_FAIL(set_partitioning_key(table_schema, table_schema.get_subpartition_key_info()))) {
      SQL_RESV_LOG(WARN, "failed to set subpartition key", K(stmt_), K(ret));
    } else {  // add sequence col
      ObColumnSchemaV2 hidden_pk;
      hidden_pk.reset();
      hidden_pk.set_column_id(OB_HIDDEN_PK_INCREMENT_COLUMN_ID);  // reserved for hidden primary key
      hidden_pk.set_data_type(ObUInt64Type);
      hidden_pk.set_nullable(false);
      hidden_pk.set_autoincrement(true);
      hidden_pk.set_is_hidden(true);
      hidden_pk.set_charset_type(CHARSET_BINARY);
      hidden_pk.set_collation_type(CS_TYPE_BINARY);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(hidden_pk.set_column_name(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME))) {
          SQL_RESV_LOG(WARN, "failed to set column name", K(ret));
        } else if (OB_FAIL(primary_keys_.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID))) {
          SQL_RESV_LOG(WARN, "failed to push_back column_id", K(ret));
        } else {
          hidden_pk.set_rowkey_position(primary_keys_.count());
          if (OB_FAIL(table_schema.add_column(hidden_pk))) {
            SQL_RESV_LOG(WARN, "add column to table_schema failed", K(ret), K(hidden_pk));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      SQL_RESV_LOG(DEBUG, "success to push partition key and sequence to array", K(table_schema), K(primary_keys_));
    }
  }
  return ret;
}

int ObCreateTableResolver::set_temp_table_info(ObTableSchema& table_schema, ParseNode* commit_option_node)
{
  int ret = OB_SUCCESS;
  session_info_->set_has_temp_table_flag();
  if (OB_FAIL(set_table_name(table_name_))) {
    LOG_WARN("failed to set table name", K(ret), K(table_name_));
  } else if (session_info_->is_obproxy_mode() && 0 == session_info_->get_sess_create_time()) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "can't create temporary table via obproxy, upgrade obproxy first", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "obproxy version is too old, create temporary table");
  } else {
    if (is_oracle_mode()) {
      if (OB_ISNULL(commit_option_node) || T_TRANSACTION == commit_option_node->type_) {
        table_schema.set_table_type(TMP_TABLE_ORA_TRX);
      } else {
        table_schema.set_table_type(TMP_TABLE_ORA_SESS);
      }
    } else {
      table_schema.set_table_type(TMP_TABLE);
      table_schema.set_session_id(session_info_->get_sessid_for_table());  // for cleaning job of temp table
    }
    table_schema.set_sess_active_time(ObTimeUtility::current_time());
  }
  LOG_DEBUG("resolve create temp table", K(session_info_->is_obproxy_mode()), K(*session_info_), K(table_schema));
  return ret;
}

int ObCreateTableResolver::add_new_column_for_oracle_temp_table(
    ObTableSchema& table_schema, ObArray<ObColumnResolveStat>& stats)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 column;
  ObColumnResolveStat stat;
  if (is_oracle_temp_table_) {
    ObObjMeta meta_int;
    meta_int.set_int();
    meta_int.set_collation_level(CS_LEVEL_NONE);
    meta_int.set_collation_type(CS_TYPE_BINARY);
    stat.reset();
    column.set_column_name(OB_HIDDEN_SESSION_ID_COLUMN_NAME);
    column.set_meta_type(meta_int);
    column.set_column_id(OB_HIDDEN_SESSION_ID_COLUMN_ID);
    column.set_is_hidden(true);
    stat.column_id_ = column.get_column_id();
    if (OB_FAIL(table_schema.add_column(column))) {
      SQL_RESV_LOG(WARN, "fail to add column", K(ret));
    } else if (OB_FAIL(stats.push_back(stat))) {
      SQL_RESV_LOG(WARN, "fail to push back stat", K(ret));
    } else {
      stat.reset();
      column.set_column_name(OB_HIDDEN_SESS_CREATE_TIME_COLUMN_NAME);
      column.set_meta_type(meta_int);
      column.set_column_id(OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID);
      column.set_is_hidden(true);
      stat.column_id_ = column.get_column_id();
      if (OB_FAIL(table_schema.add_column(column))) {
        SQL_RESV_LOG(WARN, "fail to add column", K(ret));
      } else if (OB_FAIL(stats.push_back(stat))) {
        SQL_RESV_LOG(WARN, "fail to push back stat", K(ret));
      } else {
        LOG_DEBUG("add __session_id & __sess_create_time succeed", K(table_schema.get_table_name_str()));
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::add_new_indexkey_for_oracle_temp_table(const int32_t org_key_len)
{
  int ret = OB_SUCCESS;
  if (is_oracle_temp_table_) {
    if (org_key_len + 1 > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
      ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
      LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    } else {
      ObColumnSortItem sort_item;
      sort_item.column_name_.assign_ptr(
          OB_HIDDEN_SESSION_ID_COLUMN_NAME, static_cast<int32_t>(strlen(OB_HIDDEN_SESSION_ID_COLUMN_NAME)));
      sort_item.prefix_len_ = 0;
      sort_item.order_type_ = common::ObOrderType::ASC;
      if (OB_FAIL(add_sort_column(sort_item))) {
        SQL_RESV_LOG(WARN, "add sort column failed", K(ret), K(sort_item));
      } else {
        LOG_DEBUG("add __session_id as first index key succeed", K(sort_item));
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::add_pk_key_for_oracle_temp_table(
    ObArray<ObColumnResolveStat>& stats, int64_t& pk_data_length)
{
  int ret = OB_SUCCESS;
  if (is_oracle_temp_table_) {
    ObString key_name(OB_HIDDEN_SESSION_ID_COLUMN_NAME);
    if (OB_FAIL(add_primary_key_part(key_name, stats, pk_data_length))) {
      SQL_RESV_LOG(WARN, "add primary key part failed", K(ret), K(key_name));
    }
  }
  return ret;
}

int ObCreateTableResolver::set_partition_info_for_oracle_temp_table(share::schema::ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_temp_table_) {
    ObString partition_expr;
    common::ObSEArray<ObString, 2> partition_keys;
    char expr_str_buf[64] = {'\0'};
    int64_t pos1 = 0;
    const int64_t buf_len = 64;
    const int64_t partition_num = 16;
    share::schema::ObPartitionOption* partition_option = NULL;
    share::schema::ObPartitionFuncType partition_func_type = share::schema::PARTITION_FUNC_TYPE_HASH;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3000) {
      partition_func_type = share::schema::PARTITION_FUNC_TYPE_HASH_V2;
    }
    if (OB_FAIL(databuff_printf(expr_str_buf, buf_len, pos1, "%s", OB_HIDDEN_SESSION_ID_COLUMN_NAME))) {
      LOG_WARN("fail to print __session_id", K(ret));
    } else {
      partition_expr.assign_ptr(expr_str_buf, static_cast<int32_t>(strlen(expr_str_buf)));
      common::ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, partition_expr);

      partition_option = &(table_schema.get_part_option());
      table_schema.set_part_level(share::schema::PARTITION_LEVEL_ONE);
      if (OB_FAIL(partition_keys.push_back(ObString(OB_HIDDEN_SESSION_ID_COLUMN_NAME)))) {
        SQL_RESV_LOG(WARN, "add partition key __session_id failed", K(ret));
      } else if (OB_FAIL(set_partition_keys(table_schema, partition_keys, false))) {
        SQL_RESV_LOG(WARN, "failed to set partition keys", K(ret));
      } else if (OB_FAIL(partition_option->set_part_expr(partition_expr))) {
        SQL_RESV_LOG(WARN, "set partition express string failed", K(ret));
      } else {
        partition_option->set_part_func_type(partition_func_type);
        partition_option->set_part_num(partition_num);
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  bool is_temporary_table = false;
  const bool is_mysql_mode = !is_oracle_mode();
  ParseNode* create_table_node = const_cast<ParseNode*>(&parse_tree);
  CHECK_COMPATIBILITY_MODE(session_info_);
  if (OB_ISNULL(create_table_node) || T_CREATE_TABLE != create_table_node->type_ ||
      (CREATE_TABLE_NUM_CHILD != create_table_node->num_child_ &&
          CREATE_TABLE_AS_SEL_NUM_CHILD != create_table_node->num_child_) ||
      OB_ISNULL(create_table_node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
  } else {
    ObCreateTableStmt* create_table_stmt = NULL;
    ObString table_name;
    ObString database_name;
    uint64_t database_id = OB_INVALID_ID;
    ObSEArray<ObString, 8> pk_columns_name;
    bool is_create_as_sel = (CREATE_TABLE_AS_SEL_NUM_CHILD == create_table_node->num_child_);
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    if (OB_ISNULL(create_table_stmt = create_stmt<ObCreateTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to create select stmt", K(ret));
    } else {
      create_table_stmt->set_allocator(*allocator_);
      stmt_ = create_table_stmt;
    }
    // resolve temporary option
    if (OB_SUCC(ret)) {
      if (NULL != create_table_node->children_[0]) {
        if (T_TEMPORARY != create_table_node->children_[0]->type_) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(create_table_node->children_[0]->type_));
        } else if (create_table_node->children_[5] != NULL) {
          ret = OB_ERR_TEMPORARY_TABLE_WITH_PARTITION;

        } else if (is_create_as_sel && is_mysql_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "View/Table's column refers to a temporary table");
        } else {
          is_temporary_table = true;
          is_oracle_temp_table_ = (is_mysql_mode == false);
        }
      }
    }
    // resolve if_not_exists
    if (OB_SUCC(ret)) {
      if (NULL != create_table_node->children_[1]) {
        if (T_IF_NOT_EXISTS != create_table_node->children_[1]->type_) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(create_table_node->children_[1]->type_));
        } else {
          if_not_exist_ = true;
        }
      }
    }
    // resolve table_name
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(create_table_node->children_[2])) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
      } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
        ret = OB_NOT_INIT;
        SQL_RESV_LOG(WARN, "session_info is null.", K(ret));
      } else if (OB_FAIL(resolve_table_relation_node(create_table_node->children_[2], table_name, database_name))) {
        SQL_RESV_LOG(WARN, "failed to resolve table relation node!", K(ret));
      } else if ((ObString(OB_RECYCLEBIN_SCHEMA_NAME) == database_name &&
                     ObSQLSessionInfo::USER_SESSION == session_info_->get_session_type()) ||
                 ObSchemaUtils::is_public_database(database_name, lib::is_oracle_mode())) {
        ret = OB_OP_NOT_ALLOW;
        SQL_RESV_LOG(WARN, "create table in recyclebin database is not permitted", K(ret));
      } else if (OB_FAIL(set_table_name(table_name))) {
        SQL_RESV_LOG(WARN, "set table name failed", K(ret));
      } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, database_name, database_id))) {
        if (OB_ERR_BAD_DATABASE == ret) {
          LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
        }
        SQL_RESV_LOG(WARN, "get database id failed", K(ret));
      } else if (OB_FAIL(set_database_name(database_name))) {
        SQL_RESV_LOG(WARN, "set database name failes", K(ret));
      } else if (OB_FAIL(ob_write_string(*allocator_, database_name, create_table_stmt->get_non_const_db_name()))) {
        SQL_RESV_LOG(WARN, "Failed to deep copy database name to stmt", K(ret));
      } else if (ObCharset::case_insensitive_equal(
                     ObString(strlen(OB_SYS_DATABASE_NAME), OB_SYS_DATABASE_NAME), database_name)) {
        uint64_t tenant_id = session_info_->get_effective_tenant_id();
        uint64_t database_id = OB_INVALID_ID;
        if (OB_ISNULL(schema_checker_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "schema_checker_ is null.", K(ret));
        } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, database_name, database_id))) {
          SQL_RESV_LOG(WARN, "fail to get database_id.", K(ret), K(database_name), K(tenant_id));
        } else {
          create_table_stmt->set_database_id(database_id);
        }
      } else {
        // root service need database_name instead of database_id for create operation,
        // but the temp schema_checker need database_id, so set any value here,
        // and reset to OB_INVALID_ID when resolve done.
        create_table_stmt->set_database_id(generate_table_id());
      }
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OZ(schema_checker_->check_ora_ddl_priv(tenant_id,
               session_info_->get_priv_user_id(),
               database_name,
               stmt::T_CREATE_TABLE,
               session_info_->get_enable_role_array()),
            session_info_->get_user_id(),
            database_name,
            session_info_->get_enable_role_array());
      }
      // the length of string column depends on the charset of column,
      // the default charset of column (if not specified) is charset of table,
      // so we need resolve the charset of table first.
      if (OB_SUCC(ret)) {
        if (OB_FAIL(resolve_table_charset_info(create_table_node->children_[4]))) {
          SQL_RESV_LOG(WARN, "fail to resolve charset and collation of table", K(ret));
        } else if (is_create_as_sel) {
          if (OB_NOT_NULL(create_table_node->children_[3]) &&
              T_TABLE_ELEMENT_LIST != create_table_node->children_[3]->type_) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(create_table_node->children_[2]->type_));
          } else { /* do nothing */
          }
        } else if (OB_ISNULL(create_table_node->children_[3])) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
        } else if (T_TABLE_ELEMENT_LIST != create_table_node->children_[3]->type_) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(create_table_node->children_[3]->type_));
        } else {
          // do nothing
        }

        // consider index can be defined before column, so column should be
        // resolved firstly;avoid to rescan table_element_list_node, use a
        // array named index_node_position_list to record the position of indexes
        ParseNode* table_element_list_node = create_table_node->children_[3];
        ObArray<int> index_node_position_list;
        ObArray<int> foreign_key_node_position_list;
        if (OB_SUCC(ret)) {
          if (false == is_create_as_sel) {
            if (OB_FAIL(resolve_table_elements(
                    table_element_list_node, index_node_position_list, foreign_key_node_position_list, RESOLVE_ALL))) {
              SQL_RESV_LOG(WARN, "resolve table elements failed", K(ret));
            }
          } else {
            if (OB_FAIL(resolve_table_elements(table_element_list_node,
                    index_node_position_list,
                    foreign_key_node_position_list,
                    RESOLVE_COL_ONLY))) {
              SQL_RESV_LOG(WARN, "resolve table elements col failed", K(ret));
            } else if (OB_FAIL(resolve_table_elements_from_select(parse_tree))) {
              SQL_RESV_LOG(WARN, "resolve table elements from select failed", K(ret));
            } else if (OB_FAIL(resolve_table_elements(table_element_list_node,
                           index_node_position_list,
                           foreign_key_node_position_list,
                           RESOLVE_NON_COL))) {
              SQL_RESV_LOG(WARN, "resolve table elements non-col failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && lib::is_oracle_mode()) {
            if (OB_FAIL(generate_primary_key_name_array(
                    create_table_stmt->get_create_table_arg().schema_, pk_columns_name))) {
              SQL_RESV_LOG(WARN, "generate primary_key_name_array failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2270) {
            // add hidden primary key
            if (OB_SUCC(ret) && 0 == get_primary_key_size()) {
              if (OB_FAIL(add_hidden_primary_key())) {
                SQL_RESV_LOG(WARN, "failed to add hidden primary key", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            reset();
            bool is_sync_ddl_user = false;
            if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
              LOG_WARN("Failed to check sync_ddl_user", K(ret));
            } else if (0 == get_primary_key_size() && !is_sync_ddl_user) {  // not agent mode
              table_mode_.pk_mode_ = TPKM_NEW_NO_PK;
            }
            ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
            if (!table_schema.is_sys_table()) {
              pctfree_ = 0;  // set default pctfree value for non-sys table
            }
            if (OB_FAIL(resolve_table_options(create_table_node->children_[4], false))) {
              SQL_RESV_LOG(WARN, "resolve table options failed", K(ret));
            } else if (OB_FAIL(set_table_option_to_schema(table_schema))) {
              SQL_RESV_LOG(WARN, "set table option to schema failed", K(ret));
            } else {
              table_schema.set_collation_type(collation_type_);
              table_schema.set_charset_type(charset_type_);
              // if (OB_FAIL(table_schema.fill_column_collation_info())) {
              //  SQL_RESV_LOG(WARN, "fail to fill column collation info", K(ret), K(table_name_));
              //} else {
              //  //do nothing
              //}
            }
          }
        }

        if (OB_SUCC(ret) && 0 == get_primary_key_size() && TPKM_NEW_NO_PK != table_mode_.pk_mode_) {  // old no-pk table
          // old no-pk table, need add hidden primary key before add partition key
          if (OB_FAIL(add_hidden_primary_key())) {
            SQL_RESV_LOG(WARN, "failed to add hidden primary key", K(ret));
          }
        }

        // !!Attention!! resolve_partition_option should always call after resolve_table_options
        if (OB_SUCC(ret)) {
          ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
          if (OB_FAIL(resolve_partition_option(create_table_node->children_[5], table_schema))) {
            SQL_RESV_LOG(WARN, "resolve partition option failed", K(ret));
          } else if (OB_FAIL(check_max_used_part_id_valid(table_schema, max_used_part_id_))) {
            LOG_WARN("max used part id is invalid", K(ret), K(max_used_part_id_), K(table_schema));
          } else {
            create_table_stmt->set_max_used_part_id(max_used_part_id_);
          }
        }

        // add hidden primary key
        if (OB_SUCC(ret) && 0 == get_primary_key_size() && TPKM_NEW_NO_PK == table_mode_.pk_mode_) {  // new no-pk table
          if (OB_FAIL(set_partitioning_key_and_seq())) {
            SQL_RESV_LOG(WARN, "failed to add hidden primary key", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
          create_table_stmt->set_if_not_exists(if_not_exist_);
          if (false == is_temporary_table && OB_NOT_NULL(create_table_node->children_[6])) {
            ret = OB_ERR_PARSER_SYNTAX;
            SQL_RESV_LOG(WARN, "on commit option can only be used for temp table", K(ret));
          } else if (is_temporary_table &&
                     OB_FAIL(set_temp_table_info(table_schema, create_table_node->children_[6]))) {
            SQL_RESV_LOG(WARN, "set temp table info failed", K(ret));
          } else if (OB_FAIL(table_schema.set_table_name(table_name_))) {
            SQL_RESV_LOG(WARN, "set table name failed", K(ret));
          } else {
            create_table_stmt->set_database_id(OB_INVALID_ID);
          }
          // save current host for temp table or create table as select for,
          // only the backend job of current host can drop it.
          if (OB_SUCC(ret) && (is_temporary_table || is_create_as_sel)) {
            char create_host_str[OB_MAX_HOST_NAME_LENGTH];
            MYADDR.ip_port_to_string(create_host_str, OB_MAX_HOST_NAME_LENGTH);
            table_schema.set_create_host(create_host_str);
            if (is_temporary_table) {
              table_schema.set_sess_active_time(ObTimeUtility::current_time());
            }
          }
        }
        // temp table does not support foreign key, so resolve foreign key after temp table.
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_index(table_element_list_node, index_node_position_list))) {
            SQL_RESV_LOG(WARN, "resolve index failed", K(ret));
          } else if (OB_FAIL(resolve_foreign_key(table_element_list_node, foreign_key_node_position_list))) {
            SQL_RESV_LOG(WARN, "resolve foreign key failed", K(ret));
          } else {
          }  // do nothing
        }
      }
    }
    // checking uk-pk and uk-uk duplicate in oracle mode
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      const ObSArray<obrpc::ObCreateIndexArg>& index_arg_list = create_table_stmt->get_index_arg_list();
      ObSEArray<int64_t, 8> uk_idx;
      if (OB_FAIL(generate_uk_idx_array(index_arg_list, uk_idx))) {
        SQL_RESV_LOG(WARN, "generate generate_uk_idx_array failed", K(ret));
      } else if (is_pk_uk_duplicate(pk_columns_name, index_arg_list, uk_idx)) {
        ret = OB_ERR_UK_PK_DUPLICATE;
        SQL_RESV_LOG(WARN, "uk and pk is duplicate", K(ret));
      } else if (is_uk_uk_duplicate(uk_idx, index_arg_list)) {
        ret = OB_ERR_UK_PK_DUPLICATE;
        SQL_RESV_LOG(WARN, "uk and pk is duplicate", K(ret));
      }
    }
    // check tablespace.
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
    }
    if (OB_SUCC(ret)) {
      bool strict_mode = true;
      if (OB_FAIL(session_info_->is_create_table_strict_mode(strict_mode))) {
        SQL_RESV_LOG(WARN, "failed to get variable ob_create_table_strict_mode");
      } else {
        obrpc::ObCreateTableMode create_mode =
            strict_mode ? obrpc::OB_CREATE_TABLE_MODE_STRICT : obrpc::OB_CREATE_TABLE_MODE_LOOSE;
        create_table_stmt->set_create_mode(create_mode);
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::generate_primary_key_name_array(
    const ObTableSchema& table_schema, ObIArray<ObString>& pk_columns_name)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* column = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && i < primary_keys_.count(); ++i) {
    if (NULL == (column = table_schema.get_column_schema(primary_keys_.at(i)))) {
      int ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "col is null", K(ret));
    } else if (OB_FAIL(pk_columns_name.push_back(column->get_column_name_str()))) {
      SQL_RESV_LOG(WARN, "failed to push back to pk_columns_name array", K(ret));
    }
  }

  return ret;
}

int ObCreateTableResolver::generate_uk_idx_array(
    const ObIArray<obrpc::ObCreateIndexArg>& index_arg_list, ObIArray<int64_t>& uk_idx_in_index_arg_list)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list.count(); ++i) {
    if (INDEX_TYPE_UNIQUE_LOCAL == index_arg_list.at(i).index_type_ ||
        INDEX_TYPE_UNIQUE_GLOBAL == index_arg_list.at(i).index_type_ ||
        INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_arg_list.at(i).index_type_) {
      if (OB_FAIL(uk_idx_in_index_arg_list.push_back(i))) {
        SQL_RESV_LOG(WARN, "failed to push back to uk_idx_in_index_arg_list", K(ret));
      }
    }
  }

  return ret;
}

bool ObCreateTableResolver::is_pk_uk_duplicate(const ObIArray<ObString>& pk_columns_name,
    const ObIArray<obrpc::ObCreateIndexArg>& index_arg_list, const ObIArray<int64_t>& uk_idx)
{
  bool is_tmp_match = true;
  bool is_match = false;

  if (pk_columns_name.empty() || uk_idx.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; !is_match && i < uk_idx.count(); ++i) {
      if (pk_columns_name.count() == index_arg_list.at(i).index_columns_.count()) {
        is_tmp_match = true;
        for (int64_t j = 0; is_tmp_match && j < pk_columns_name.count(); ++j) {
          if (0 != pk_columns_name.at(j).case_compare(index_arg_list.at(i).index_columns_.at(j).column_name_)) {
            is_tmp_match = false;
          }
        }
        if (is_tmp_match) {
          is_match = true;
        }
      }
    }
  }

  return is_match;
}

bool ObCreateTableResolver::is_uk_uk_duplicate(
    const ObIArray<int64_t>& uk_idx, const ObIArray<obrpc::ObCreateIndexArg>& index_arg_list)
{
  bool is_tmp_match = true;
  bool is_match = false;

  if (uk_idx.count() < 2) {
    // do nothing
  } else {
    for (int64_t i = 0; !is_match && i < uk_idx.count(); ++i) {
      for (int64_t j = i + 1; !is_match && j < uk_idx.count(); ++j) {
        if (index_arg_list.at(i).index_columns_.count() == index_arg_list.at(j).index_columns_.count()) {
          is_tmp_match = true;
          for (int64_t k = 0; is_tmp_match && k < index_arg_list.at(i).index_columns_.count(); ++k) {
            if (0 != index_arg_list.at(i).index_columns_.at(k).column_name_.case_compare(
                         index_arg_list.at(j).index_columns_.at(k).column_name_)) {
              is_tmp_match = false;
            }
          }
          if (is_tmp_match) {
            is_match = true;
          }
        }
      }
    }
  }

  return is_match;
}

int ObCreateTableResolver::resolve_partition_option(ParseNode* node, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(allocator_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "failed to build partition key info!", K(ret), KP(session_info_));
  } else {
    if (NULL != node) {
      ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
      if (T_PARTITION_OPTION == node->type_) {
        if (node->num_child_ < 1 || node->num_child_ > 2) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "node number is invalid.", K(ret), K(node->num_child_));
        } else if (NULL == node->children_[0]) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "partition node is null.", K(ret));
        } else {
          ParseNode* partition_node = node->children_[0];         // horizontal partition
          ParseNode* column_partition_node = node->children_[1];  // vertical partition
          if (OB_FAIL(resolve_partition_node(create_table_stmt, partition_node, table_schema))) {
            LOG_WARN("failed to resolve partition option", K(ret));
          } else {
            if (NULL != column_partition_node) {
              ret = OB_NOT_SUPPORTED;
              SQL_RESV_LOG(WARN, "partition type not supported", K(ret), K(node->type_));
            }
          }
        }
      } else if (T_VERTICAL_COLUMNS_PARTITION == node->type_) {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Vertical partition not supported", K(ret), K(node->type_));
      } else if (T_AUTO_PARTITION == node->type_) {
        if (OB_FAIL(resolve_auto_partition(node))) {
          SQL_RESV_LOG(WARN, "failed to resolve auto partition", KR(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "node type is invalid.", K(ret), K(node->type_));
      }
    } else if (is_oracle_temp_table_ && OB_FAIL(set_partition_info_for_oracle_temp_table(table_schema))) {
      SQL_RESV_LOG(WARN, "set __sess_id as partition key failed", K(ret));
    }
    if (OB_SUCC(ret) && (OB_NOT_NULL(node) || is_oracle_temp_table_)) {
      if (OB_FAIL(check_generated_partition_column(table_schema))) {
        LOG_WARN("Failed to check generate partiton column", K(ret));
      } else if (OB_FAIL(table_schema.check_primary_key_cover_partition_column())) {
        SQL_RESV_LOG(WARN, "fail to check primary key cover partition column", K(ret));
      } else if (OB_FAIL(table_schema.check_auto_partition_valid())) {
        LOG_WARN("failed to check auto partition valid", KR(ret));
      } else {
      }  // do nothing
    }
  }
  return ret;
}

int ObCreateTableResolver::check_generated_partition_column(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionKeyInfo& part_key_info = table_schema.get_partition_key_info();
  const ObPartitionKeyColumn* part_column = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  ObRawExpr* gen_col_expr = NULL;
  ObString expr_def;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < part_key_info.get_size(); ++idx) {
    if (OB_ISNULL(part_column = part_key_info.get_column(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Part column is NULL", K(ret), K(idx));
    } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(part_column->column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get column schema", K(ret), K(part_column->column_id_));
    } else if (column_schema->is_generated_column()) {
      if (OB_FAIL(column_schema->get_cur_default_value().get_string(expr_def))) {
        LOG_WARN("get string from current default value failed", K(ret), K(column_schema->get_cur_default_value()));
      } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(expr_def,
                     *params_.expr_factory_,
                     *params_.session_info_,
                     table_schema,
                     gen_col_expr,
                     schema_checker_))) {
        LOG_WARN("build generated column expr failed", K(ret));
      } else {
        // check Expr Function for generated column whether allowed.
        ObRawExprPartFuncChecker part_func_checker(true);
        if (OB_FAIL(gen_col_expr->preorder_accept(part_func_checker))) {
          LOG_WARN("check partition function failed", K(ret));
        }
      }
    } else {
    }  // do nothing
  }
  return ret;
}

int ObCreateTableResolver::check_column_name_duplicate(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node is null.", K(ret));
  } else if (OB_ISNULL(stmt_) || T_TABLE_ELEMENT_LIST != node->type_ || OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node->type_), K(node->num_child_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ParseNode* element = node->children_[i];
      if (OB_ISNULL(element)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("element node is null", K(ret));
      } else if (OB_LIKELY(T_COLUMN_DEFINITION == element->type_)) {
        if (OB_USER_ROW_MAX_COLUMNS_COUNT == column_name_set_.count()) {
          ret = OB_ERR_TOO_MANY_COLUMNS;

        } else if (element->num_child_ < COLUMN_DEFINITION_NUM_CHILD || OB_ISNULL(element->children_) ||
                   OB_ISNULL(element->children_[COLUMN_REF_NODE]) ||
                   T_COLUMN_REF != element->children_[COLUMN_REF_NODE]->type_ ||
                   COLUMN_DEF_NUM_CHILD != element->children_[COLUMN_REF_NODE]->num_child_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid parse node", K(ret));
        } else {
          ParseNode* name_node = element->children_[COLUMN_REF_NODE]->children_[COLUMN_NAME_NODE];
          if (OB_ISNULL(name_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("name node can not be null", K(ret));
          } else if (name_node->str_len_ > OB_MAX_COLUMN_NAME_LENGTH) {
            ret = OB_ERR_TOO_LONG_IDENT;
            LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)name_node->str_len_, name_node->str_value_);
          } else if (0 == name_node->str_len_) {
            ret = OB_WRONG_COLUMN_NAME;
            LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, (int)name_node->str_len_, name_node->str_value_);
          } else {
            ObString name(name_node->str_len_, name_node->str_value_);
            ObCollationType cs_type = CS_TYPE_INVALID;
            if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
              LOG_WARN("fail to get collation_connection", K(ret));
            } else if (OB_FAIL(ObSQLUtils::check_column_name(cs_type, name))) {
              LOG_WARN("fail to check column name", K(name), K(ret));
            } else {
              ObColumnNameHashWrapper column_name_key(name);
              if (OB_HASH_EXIST == column_name_set_.exist_refactored(column_name_key)) {
                ret = OB_ERR_COLUMN_DUPLICATE;
                LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, name.length(), name.ptr());
              } else {
                if (OB_FAIL(column_name_set_.set_refactored(column_name_key))) {
                  LOG_WARN("add column name to map failed", K(name), K(ret));
                }
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      column_name_set_.reset();
    }
  }
  return ret;
}

int ObCreateTableResolver::resolve_primary_key_node(const ParseNode& pk_node, ObArray<ObColumnResolveStat>& stats)
{
  int ret = OB_SUCCESS;

  const bool is_mysql_mode = !is_oracle_mode();
  if ((is_mysql_mode ? 3 < pk_node.num_child_ : 2 < pk_node.num_child_) || OB_ISNULL(pk_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the num_child of primary_node is wrong.", K(ret), K(pk_node.num_child_), K(pk_node.children_));
  } else {
    ParseNode* column_list_node = pk_node.children_[0];
    if (OB_ISNULL(column_list_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "column_list_node is null.", K(ret));
    } else if (T_COLUMN_LIST != column_list_node->type_ || column_list_node->num_child_ <= 0 ||
               OB_ISNULL(column_list_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "unexpected.", K(ret), K(column_list_node->type_), K(column_list_node->num_child_));
    } else {
      ParseNode* key_node = NULL;
      int64_t pk_data_length = 0;
      if (OB_FAIL(add_pk_key_for_oracle_temp_table(stats, pk_data_length))) {
        SQL_RESV_LOG(WARN, "add new pk key failed", K(ret));
      }
      for (int32_t i = 0; OB_SUCC(ret) && i < column_list_node->num_child_; ++i) {
        if (OB_ISNULL(column_list_node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "column_list_node->children_[i] is null.", K(ret));
        } else {
          key_node = column_list_node->children_[i];
          ObString key_name;
          if (OB_ISNULL(key_node)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "key_node is null.", K(ret));
          } else {
            key_name.assign_ptr(key_node->str_value_, static_cast<int32_t>(key_node->str_len_));
            if (OB_FAIL(add_primary_key_part(key_name, stats, pk_data_length))) {
              SQL_RESV_LOG(WARN, "add primary key part failed", K(ret), K(key_name));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && is_mysql_mode && NULL != pk_node.children_[1]) {
      ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
      if (T_USING_HASH == pk_node.children_[1]->type_) {
        create_table_stmt->set_index_using_type(share::schema::USING_HASH);
      } else {
        create_table_stmt->set_index_using_type(share::schema::USING_BTREE);
      }
    }
    if (OB_SUCC(ret) && is_mysql_mode) {
      if (NULL != pk_node.children_[2]) {
        ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
        ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
        ObString pk_comment;
        pk_comment.assign_ptr(pk_node.children_[2]->str_value_, static_cast<int32_t>(pk_node.children_[2]->str_len_));
        if (OB_FAIL(table_schema.set_pk_comment(pk_comment))) {
          LOG_WARN("fail to set primary key comment", K(pk_comment), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::get_resolve_stats_from_table_schema(
    const ObTableSchema& table_schema, ObArray<ObColumnResolveStat>& stats)
{
  int ret = OB_SUCCESS;
  ObColumnResolveStat stat;
  ObTableSchema::const_column_iterator it_begin = table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = table_schema.column_end();
  for (; OB_SUCC(ret) && it_begin != it_end; it_begin++) {
    if (OB_ISNULL(it_begin)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("it_begin should not be NULL", K(ret));
    } else if (OB_ISNULL(*it_begin)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("*it_begin should not be NULL", K(ret));
    } else {
      const ObColumnSchemaV2& column_schema = **it_begin;
      stat.reset();
      stat.column_id_ = column_schema.get_column_id();
      if (OB_FAIL(stats.push_back(stat))) {
        SQL_RESV_LOG(WARN, "fail to push back stat", K(ret));
      }
    }
  }
  return ret;
}

// in case of 'create table as select' query, this function will be called twice:
// 1st time: resolve columns only,
// 2nd time: resolve other infomation.
int ObCreateTableResolver::resolve_table_elements(const ParseNode* node, ObArray<int>& index_node_position_list,
    ObArray<int>& foreign_key_node_position_list, const int resolve_rule)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    // do nothing, create table t as select ... will come here
  } else if (OB_ISNULL(stmt_) || T_TABLE_ELEMENT_LIST != node->type_ || node->num_child_ < 1 ||
             OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_));
  } else {
    ParseNode* primary_node = NULL;
    ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    uint64_t autoinc_column_id = 0;
    int64_t first_timestamp_index = -1;
    int64_t row_data_length = 0;
    ObArray<ObColumnResolveStat> stats;
    ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
    bool has_visible_col = false;
    if (RESOLVE_NON_COL != resolve_rule) {
      table_schema.set_tenant_id(session_info_->get_effective_tenant_id());
      if (OB_FAIL(add_new_column_for_oracle_temp_table(table_schema, stats))) {
        SQL_RESV_LOG(WARN, "add new column for oracle temp table failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing ...
    } else if (RESOLVE_NON_COL == resolve_rule) {
      if (OB_FAIL(get_resolve_stats_from_table_schema(table_schema, stats))) {
        LOG_WARN("failed to generate ObColumnResolveStat array", K(ret));
      }
    } else if (OB_FAIL(check_column_name_duplicate(node))) {
      LOG_WARN("check_column_name_duplicate fail", K(ret));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ParseNode* element = node->children_[i];
      if (RESOLVE_NON_COL == resolve_rule && T_COLUMN_DEFINITION == element->type_) {
        continue;
      } else if (RESOLVE_COL_ONLY == resolve_rule && T_COLUMN_DEFINITION != element->type_) {
        continue;
      }

      if (OB_LIKELY(T_COLUMN_DEFINITION == element->type_)) {
        ObColumnSchemaV2 column;
        ObColumnResolveStat stat;
        common::ObString pk_name;
        bool is_modify_column_visibility = false;
        const bool is_create_table_as = (RESOLVE_COL_ONLY == resolve_rule);
        if (OB_INVALID_ID == column.get_column_id()) {
          column.set_column_id(gen_column_id());
        } else {
          // column id set by user sql such as "c1 int comment 'manual' id 17"
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(resolve_column_definition(column,
                       element,
                       stat,
                       is_modify_column_visibility,
                       pk_name,
                       false,
                       false,
                       is_oracle_temp_table_,
                       is_create_table_as))) {
          SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
        } else if (OB_FAIL(check_default_value(column.get_cur_default_value(),
                       session_info_->get_tz_info_wrap(),
                       session_info_->get_local_nls_formats(),
                       *allocator_,
                       table_schema,
                       column))) {
          SQL_RESV_LOG(WARN, "failed to cast default value!", K(ret));
        } else if (column.is_string_type()) {
          int64_t length = 0;
          if (OB_FAIL(column.get_byte_length(length))) {
            SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
          } else if (ob_is_string_tc(column.get_data_type()) && length > OB_MAX_VARCHAR_LENGTH) {
            ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
            LOG_USER_ERROR(
                OB_ERR_TOO_LONG_COLUMN_LENGTH, column.get_column_name(), static_cast<int32_t>(OB_MAX_VARCHAR_LENGTH));
          } else if (ob_is_text_tc(column.get_data_type())) {
            ObLength max_length = 0;
            if ((GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1470) && !ObSchemaService::g_liboblog_mode_) {
              max_length = ObAccuracy::MAX_ACCURACY_OLD[column.get_data_type()].get_length();
            } else {
              max_length = ObAccuracy::MAX_ACCURACY2[is_oracle_mode()][column.get_data_type()].get_length();
            }
            if (length > max_length) {
              ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH,
                  column.get_column_name(),
                  ObAccuracy::MAX_ACCURACY2[is_oracle_mode()][column.get_data_type()].get_length());
            } else if ((GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_1470) || ObSchemaService::g_liboblog_mode_) {
              length = min(length, OB_MAX_LOB_HANDLE_LENGTH);
            }
          }
          if (OB_SUCC(ret) && (row_data_length += length) > OB_MAX_USER_ROW_LENGTH) {
            ret = OB_ERR_TOO_BIG_ROWSIZE;
          }
        }
        if (OB_SUCC(ret) && column.is_generated_column()) {
          if (OB_FAIL(column.set_orig_default_value(column.get_cur_default_value()))) {
            LOG_WARN("set origin default value failed", K(column), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          stat.column_id_ = column.get_column_id();
          if (stat.is_primary_key_) {
            int64_t pk_data_length = 0;
            if (get_primary_key_size() > 0) {
              ret = OB_ERR_MULTIPLE_PRI_KEY;
              SQL_RESV_LOG(WARN, "Multiple primary key defined");
            } else if (stat.is_set_null_) {
              ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
            } else if (OB_FAIL(add_pk_key_for_oracle_temp_table(stats, pk_data_length))) {
              SQL_RESV_LOG(WARN, "add new pk key failed", K(ret));
            } else if (ob_is_string_tc(column.get_data_type())) {
              int64_t length = 0;
              if (OB_FAIL(column.get_byte_length(length))) {
                SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
              } else if (pk_data_length += length > OB_MAX_VARCHAR_LENGTH_KEY) {
                ret = OB_ERR_TOO_LONG_KEY_LENGTH;
                LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_VARCHAR_LENGTH_KEY);
              } else if (length <= 0) {
                ret = OB_ERR_WRONG_KEY_COLUMN;
                LOG_USER_ERROR(
                    OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name());
              } else {
                // do nothing
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(primary_keys_.push_back(column.get_column_id()))) {
                SQL_RESV_LOG(WARN, "add primary key failed");
              } else {
                column.set_rowkey_position(get_primary_key_size());
                // generate a constraint name for primary key in oracle mode.
                if (session_info_->is_oracle_compatible()) {
                  ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
                  ObSEArray<ObConstraint, 4>& csts = create_table_stmt->get_create_table_arg().constraint_list_;
                  if (OB_FAIL(resolve_pk_constraint_node(*element, pk_name, csts))) {
                    SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
                  }
                }
              }
            }
          } else {
            column.set_rowkey_position(0);
          }
        }

        if (OB_SUCC(ret)) {
          if (stat.is_unique_key_) {
            // consider column with unique_key as a special index node,
            // then resolve it in resolve_index_node()
            if (OB_MAX_INDEX_PER_TABLE == index_node_position_list.count()) {
              ret = OB_ERR_TOO_MANY_KEYS;
              LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
            } else if (OB_FAIL(index_node_position_list.push_back(i))) {
              SQL_RESV_LOG(WARN, "add index node failed", K(ret));
            } else { /*do nothing*/
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (stat.is_autoincrement_) {
            if (0 == autoinc_column_id) {
              autoinc_column_id = column.get_column_id();
            } else {
              ret = OB_ERR_WRONG_AUTO_KEY;
              LOG_USER_ERROR(OB_ERR_WRONG_AUTO_KEY);
              SQL_RESV_LOG(WARN, "only one auto-increment column permitted", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          bool is_first_timestamp = false;
          if (ObTimestampType == column.get_data_type()) {
            if (-1 == first_timestamp_index) {
              is_first_timestamp = true;
              first_timestamp_index = column.get_column_id();
            }
            if (OB_FAIL(ObResolverUtils::resolve_timestamp_node(
                    stat.is_set_null_, stat.is_set_default_value_, is_first_timestamp, session_info_, column))) {
              SQL_RESV_LOG(WARN, "fail to resolve timestamp node", K(ret), K(column));
            }
          }
        }

        if (OB_SUCC(ret)) {
          ObColumnSchemaV2* tmp_col = NULL;
          if (OB_FAIL(table_schema.add_column(column))) {
            SQL_RESV_LOG(WARN, "add column schema failed", K(ret), K(column), K(table_schema));
          } else if (OB_ISNULL(tmp_col = table_schema.get_column_schema(column.get_column_id()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get column schema failed", K(column));
          } else {
            ObColumnNameHashWrapper name_key(tmp_col->get_column_name_str());
            if (OB_FAIL(column_name_set_.set_refactored(name_key))) {
              SQL_RESV_LOG(WARN, "add column name to map failed", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
        if (OB_SUCC(ret) && share::is_oracle_mode()) {
          if (!column.is_invisible_column()) {
            has_visible_col = true;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(stats.push_back(stat))) {
            SQL_RESV_LOG(WARN, "fail to push back stat", K(ret));
          }
        }
      } else if (T_PRIMARY_KEY == element->type_) {
        if (NULL == primary_node) {
          primary_node = element;
          if (session_info_->is_oracle_compatible()) {
            ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
            ObSEArray<ObConstraint, 4>& csts = create_table_stmt->get_create_table_arg().constraint_list_;
            ObString pk_name;
            if (OB_FAIL(resolve_pk_constraint_node(*element, pk_name, csts))) {
              SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
            }
          }
        } else {
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
          SQL_RESV_LOG(WARN, "multiple primary key defined");
        }
      } else if (T_INDEX == element->type_) {
        if (OB_MAX_INDEX_PER_TABLE == index_node_position_list.count()) {
          ret = OB_ERR_TOO_MANY_KEYS;
          LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
        } else if (OB_FAIL(index_node_position_list.push_back(i))) {
          SQL_RESV_LOG(WARN, "add index node failed", K(ret));
        } else { /*do nothing*/
        }
      } else if (T_FOREIGN_KEY == element->type_) {
        if (OB_MAX_INDEX_PER_TABLE == foreign_key_node_position_list.count()) {
          ret = OB_ERR_TOO_MANY_KEYS;
          LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
        } else if (OB_FAIL(foreign_key_node_position_list.push_back(i))) {
          SQL_RESV_LOG(WARN, "add foreign key node failed", K(ret));
        } else { /*do nothing*/
        }
      } else if (T_CHECK_CONSTRAINT == element->type_) {
        ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
        ObSEArray<ObConstraint, 4>& csts = create_table_stmt->get_create_table_arg().constraint_list_;
        if (OB_FAIL(resolve_check_constraint_node(*element, csts))) {
          SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
        }
      } else {
        // won't be here
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "unexpected branch", K(ret));
      }
    }

    if (OB_SUCC(ret) && share::is_oracle_mode()) {
      if (RESOLVE_NON_COL != resolve_rule && !has_visible_col) {
        ret = OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE;
        SQL_RESV_LOG(WARN, "table must have at least one column that is not invisible", K(ret));
      }
    }

    if (OB_SUCC(ret) && share::is_oracle_mode()) {
      bool has_non_virtual_column = false;
      for (int64_t i = 0; OB_SUCC(ret) && !has_non_virtual_column && i < table_schema.get_column_count(); ++i) {
        const ObColumnSchemaV2* column = table_schema.get_column_schema_by_idx(i);
        CK(OB_NOT_NULL(column));
        if (OB_SUCC(ret) && !column->is_generated_column()) {
          has_non_virtual_column = true;
        }
      }
      if (OB_SUCC(ret) && !has_non_virtual_column) {
        ret = OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL;
        SQL_RESV_LOG(WARN, "table must have at least one column that is not virtual", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(get_primary_key_size() > 0 && NULL != primary_node)) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        SQL_RESV_LOG(WARN, "multiple primary key defined");
      } else if (NULL == primary_node) {
        // do nothing
      } else if (OB_FAIL(resolve_primary_key_node(*primary_node, stats))) {
        SQL_RESV_LOG(WARN, "resolve_primary_key_node failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        table_schema.set_first_timestamp_index(first_timestamp_index);
        table_schema.set_max_used_column_id(cur_column_id_);
        if (0 != autoinc_column_id) {
          table_schema.set_autoinc_column_id(autoinc_column_id);
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::set_nullable_for_cta_column(
    ObSelectStmt* select_stmt, ObColumnSchemaV2& column, const ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null of expr and select stmt.", K(ret));
  } else if (select_stmt->has_rollup() || select_stmt->has_grouping_sets()) {
    bool is_found = false;
    const ObIArray<ObRawExpr*>& rollup_exprs = select_stmt->get_rollup_exprs();
    const ObIArray<ObGroupingSetsItem>& groupingsets_items = select_stmt->get_grouping_sets_items();
    const ObIArray<ObMultiRollupItem>& multi_rollup_items = select_stmt->get_multi_rollup_items();
    int64_t rollup_exprs_size = rollup_exprs.count();
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < rollup_exprs_size; i++) {
      if (ObOptimizerUtil::is_sub_expr(rollup_exprs.at(i), expr)) {
        is_found = true;
        column.set_nullable(true);
      } else { /* do nothing.*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < groupingsets_items.count(); ++i) {
      const ObIArray<ObGroupbyExpr>& grouping_sets_exprs = groupingsets_items.at(i).grouping_sets_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < grouping_sets_exprs.count(); ++j) {
        const ObIArray<ObRawExpr*>& groupby_exprs = grouping_sets_exprs.at(j).groupby_exprs_;
        for (int64_t k = 0; OB_SUCC(ret) && !is_found && k < groupby_exprs.count(); ++k) {
          if (ObOptimizerUtil::is_sub_expr(groupby_exprs.at(k), expr)) {
            is_found = true;
            column.set_nullable(true);
          } else { /* do nothing.*/
          }
        }
      }
      const ObIArray<ObMultiRollupItem>& rollup_items = groupingsets_items.at(i).multi_rollup_items_;
      for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < rollup_items.count(); ++j) {
        const ObIArray<ObGroupbyExpr>& rollup_list_exprs = rollup_items.at(j).rollup_list_exprs_;
        for (int64_t k = 0; OB_SUCC(ret) && !is_found && k < rollup_list_exprs.count(); ++k) {
          const ObIArray<ObRawExpr*>& groupby_exprs = rollup_list_exprs.at(k).groupby_exprs_;
          for (int64_t m = 0; OB_SUCC(ret) && !is_found && m < groupby_exprs.count(); ++m) {
            if (ObOptimizerUtil::is_sub_expr(groupby_exprs.at(m), expr)) {
              is_found = true;
              column.set_nullable(true);
            } else { /* do nothing.*/
            }
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < multi_rollup_items.count(); ++i) {
      const ObIArray<ObGroupbyExpr>& rollup_list_exprs = multi_rollup_items.at(i).rollup_list_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < rollup_list_exprs.count(); ++j) {
        const ObIArray<ObRawExpr*>& groupby_exprs = rollup_list_exprs.at(j).groupby_exprs_;
        for (int64_t k = 0; OB_SUCC(ret) && !is_found && k < groupby_exprs.count(); ++k) {
          if (ObOptimizerUtil::is_sub_expr(groupby_exprs.at(k), expr)) {
            is_found = true;
            column.set_nullable(true);
          } else { /* do nothing.*/
          }
        }
      }
    }
    if (OB_SUCC(ret) && !is_found) {
      column.set_nullable(!expr->is_not_null());
    } else { /* do nothing.*/
    }
  } else if (share::is_mysql_mode() || expr->is_column_ref_expr()) {
    column.set_nullable(!expr->is_not_null());
  } else {
    column.set_nullable(true);
  }
  return ret;
}

int ObCreateTableResolver::resolve_table_elements_from_select(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  ParseNode* sub_sel_node = parse_tree.children_[CREATE_TABLE_AS_SEL_NUM_CHILD - 1];
  ObSelectStmt* select_stmt = NULL;
  ObSelectResolver select_resolver(params_);
  select_resolver.params_.is_from_create_view_ = true;
  // select stmt can not see upper insert stmt.
  select_resolver.set_parent_namespace_resolver(NULL);
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session_info is null.", K(session_info_), K(ret));
  } else if (OB_ISNULL(sub_sel_node) || OB_UNLIKELY(T_SELECT != sub_sel_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid select node", K(sub_sel_node));
  } else if (OB_FAIL(set_table_name(table_name_))) {
    LOG_WARN("failed to set table name", K(ret));
  } else if (OB_FAIL(select_resolver.resolve(*sub_sel_node))) {
    LOG_WARN("failed to resolve select stmt in creat table stmt", K(ret));
  } else {
    select_stmt = select_resolver.get_select_stmt();
    ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
    table_schema.set_tenant_id(session_info_->get_effective_tenant_id());
    if (OB_ISNULL(select_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid select stmt", K(select_stmt));
    } else {
      ObIArray<SelectItem>& select_items = select_stmt->get_select_items();
      ObColumnSchemaV2 column;
      create_table_stmt->set_sub_select(select_stmt);
      for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
        const SelectItem& cur_item = select_items.at(i);
        const ObString* cur_name = NULL;
        if (!cur_item.alias_name_.empty()) {
          cur_name = &cur_item.alias_name_;
        } else {
          cur_name = &cur_item.expr_name_;
        }
        if (cur_name->length() > OB_MAX_COLUMN_NAME_LENGTH) {
          ret = OB_ERR_TOO_LONG_IDENT;
          LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, cur_name->length(), cur_name->ptr());
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
          const SelectItem& pre_item = select_items.at(j);
          const ObString* prev_name = NULL;
          if (!pre_item.alias_name_.empty()) {
            prev_name = &pre_item.alias_name_;
          } else {
            prev_name = &pre_item.expr_name_;
          }
          if (ObCharset::case_compat_mode_equal(*prev_name, *cur_name)) {
            ret = OB_ERR_COLUMN_DUPLICATE;
            LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, cur_name->length(), cur_name->ptr());
          }
        }
      }
      const int64_t create_table_column_count = table_schema.get_column_count();
      // in oracle mode, if 'create table as select exprs' does not specify column name,
      // every expr must be column, or specify alias name.
      if (share::is_oracle_mode() && create_table_column_count <= 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); i++) {
          const SelectItem& select_item = select_items.at(i);
          if (OB_ISNULL(select_item.expr_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid null expr in select_item", K(ret), K(select_item.expr_));
          } else if (select_item.is_real_alias_ || T_REF_COLUMN == select_item.expr_->get_expr_type()) {
            // do nothing
          } else {
            ret = OB_NO_COLUMN_ALIAS;
            LOG_USER_ERROR(OB_NO_COLUMN_ALIAS, select_item.expr_name_.length(), select_item.expr_name_.ptr());
          }
        }
      }
      // add 2 hidden folumn for temp table of oracle mode.
      const int64_t hidden_column_num = is_oracle_temp_table_ ? 2 : 0;
      if (OB_SUCC(ret) && share::is_oracle_mode()) {
        if (create_table_column_count > 0) {
          if (create_table_column_count != select_items.count() + hidden_column_num) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("column count not equal", K(ret));
          }
        } else {
          ObArray<ObColumnResolveStat> stats;
          if (OB_FAIL(add_new_column_for_oracle_temp_table(table_schema, stats))) {
            LOG_WARN("add new column for oracle temp table failed", K(ret));
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
        const SelectItem& select_item = select_items.at(i);
        const ObRawExpr* expr = select_item.expr_;
        if (OB_UNLIKELY(NULL == expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select item expr is null", K(ret), K(i));
        } else {
          column.reset();
          if (!select_item.alias_name_.empty()) {
            column.set_column_name(select_item.alias_name_);
          } else {
            column.set_column_name(select_item.expr_name_);
          }
          if (ObResolverUtils::is_restore_user(*session_info_) &&
              ObCharset::case_insensitive_equal(column.get_column_name_str(), OB_HIDDEN_PK_INCREMENT_COLUMN_NAME)) {
            continue;
          }
          if (expr->get_result_type().is_null()) {
            if (is_oracle_mode()) {
              ret = OB_ERR_ZERO_LEN_COL;
              LOG_WARN("add column failed on oracle mode: length is zero", K(ret));
            } else {
              const ObAccuracy binary_accuracy(0);
              ObObjMeta binary_meta;
              binary_meta.set_binary();
              column.set_meta_type(binary_meta);
              column.set_accuracy(binary_accuracy);
            }
          } else {
            ObObjMeta column_meta = expr->get_result_type().get_obj_meta();
            if (column_meta.is_lob_locator()) {
              column_meta.set_type(ObLongTextType);
            }
            column.set_meta_type(column_meta);
            if (column.is_enum_or_set()) {
              if (OB_FAIL(column.set_extended_type_info(expr->get_enum_set_values()))) {
                LOG_WARN("set enum or set info failed", K(ret), K(*expr));
              }
            }
            column.set_charset_type(table_schema.get_charset_type());
            column.set_collation_type(expr->get_collation_type());
            column.set_accuracy(expr->get_accuracy());
            LOG_DEBUG("column expr debug", K(*expr));
          }
          if (OB_FAIL(ret)) {  // do nothing.
          } else if (OB_FAIL(set_nullable_for_cta_column(select_stmt, column, expr))) {
            LOG_WARN("failed to check and set nullable for cta.", K(ret));
          } else if (is_oracle_mode() && create_table_column_count > 0) {
            if (column.is_string_type()) {
              if (column.get_meta_type().is_lob()) {
                if (OB_FAIL(check_text_column_length_and_promote(column))) {
                  LOG_WARN("fail to check text or blob column length", K(ret), K(column));
                }
              } else if (OB_FAIL(check_string_column_length(column, share::is_oracle_mode()))) {
                LOG_WARN("fail to check string column length", K(ret), K(column));
              }
            } else if (ObRawType == column.get_data_type()) {
              if (OB_FAIL(ObDDLResolver::check_raw_column_length(column))) {
                LOG_WARN("failed to check raw column length", K(ret), K(column));
              }
            }
            if (OB_SUCC(ret)) {
              ObColumnSchemaV2* org_column = table_schema.get_column_schema_by_idx(i + hidden_column_num);
              org_column->set_meta_type(column.get_meta_type());
              org_column->set_charset_type(column.get_charset_type());
              org_column->set_collation_type(column.get_collation_type());
              org_column->set_accuracy(column.get_accuracy());
              if (column.is_enum_or_set()) {
                if (OB_FAIL(org_column->set_extended_type_info(column.get_extended_type_info()))) {
                  LOG_WARN("set enum or set info failed", K(ret), K(*expr));
                }
              }
            }
          } else {
            column.set_column_id(gen_column_id());
            ObColumnSchemaV2* org_column = table_schema.get_column_schema(column.get_column_name());
            if (OB_NOT_NULL(org_column)) {
              // adjust the order for compatible with mysql.
              ObColumnSchemaV2 new_column(*org_column);
              new_column.set_column_id(gen_column_id());
              new_column.set_prev_column_id(UINT64_MAX);
              new_column.set_next_column_id(UINT64_MAX);
              if (1 == table_schema.get_column_count()) {
              } else if (OB_FAIL(table_schema.delete_column(org_column->get_column_name_str()))) {
                LOG_WARN("delete column failed", K(ret), K(new_column.get_column_name_str()));
              } else if (OB_FAIL(table_schema.add_column(new_column))) {
                LOG_WARN("add column failed", K(ret), K(new_column));
              } else {
                LOG_DEBUG("reorder column successfully", K(new_column));
              }
            } else {
              if (column.is_string_type()) {
                if (column.get_meta_type().is_lob()) {
                  if (OB_FAIL(check_text_column_length_and_promote(column))) {
                    LOG_WARN("fail to check text or blob column length", K(ret), K(column));
                  }
                } else if (OB_FAIL(check_string_column_length(column, share::is_oracle_mode()))) {
                  LOG_WARN("fail to check string column length", K(ret), K(column));
                }
              } else if (ObRawType == column.get_data_type()) {
                if (OB_FAIL(ObDDLResolver::check_raw_column_length(column))) {
                  LOG_WARN("failed to check raw column length", K(ret), K(column));
                }
              }
              if (OB_FAIL(ret)) {
                // do nothing ...
              } else if (OB_FAIL(table_schema.add_column(column))) {
                LOG_WARN("add column to table_schema failed", K(ret), K(column));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::add_sort_column(const ObColumnSortItem& sort_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "variables are not inited", K(ret), KP(stmt_));
  } else {
    ObColumnSchemaV2* column_schema = NULL;
    ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema& tbl_schema = create_table_stmt->get_create_table_arg().schema_;
    share::schema::ObColumnNameWrapper column_key(sort_column.column_name_, sort_column.prefix_len_);
    bool check_prefix_len = false;
    if (NULL == (column_schema = tbl_schema.get_column_schema(sort_column.column_name_))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
          sort_column.column_name_.length(),
          sort_column.column_name_.ptr(),
          table_name_.length(),
          table_name_.ptr());
    } else if (is_column_exists(sort_column_array_, column_key, check_prefix_len)) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, sort_column.column_name_.length(), sort_column.column_name_.ptr());
    } else if (OB_FAIL(check_prefix_key(sort_column.prefix_len_, *column_schema))) {
      SQL_RESV_LOG(WARN, "Incorrect prefix key", K(ret));
    } else if (OB_FAIL(sort_column_array_.push_back(column_key))) {
      SQL_RESV_LOG(WARN, "failed to push back column key", K(ret));
    } else if (OB_FAIL(index_arg_.index_columns_.push_back(sort_column))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "add sort column to index arg failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObCreateTableResolver::get_table_schema_for_check(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  if (OB_FAIL(table_schema.assign(create_table_stmt->get_create_table_arg().schema_))) {
    SQL_RESV_LOG(WARN, "fail to assign schema", K(ret));
  }
  return ret;
}

int ObCreateTableResolver::generate_index_arg()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "variables are not inited.", K(ret), KP(stmt_));
  } else if (OB_FAIL(set_index_name())) {
    SQL_RESV_LOG(WARN, "set index name failed", K(ret), K_(index_name));
  } else if (OB_FAIL(set_index_option_to_arg())) {
    SQL_RESV_LOG(WARN, "set index option failed", K(ret));
  } else if (OB_FAIL(set_storing_column())) {
    SQL_RESV_LOG(WARN, "set storing column failed", K(ret));
  } else if (OB_FAIL(set_fulltext_columns())) {
    LOG_WARN("assign fulltext column name failed", K(ret));
  } else {
    ObIndexType type = INDEX_TYPE_IS_NOT;
    // index of temp table in oracle mode should be local always.
    if (is_oracle_temp_table_) {
      index_scope_ = LOCAL_INDEX;
    }
    global_ = (index_scope_ != LOCAL_INDEX);
    ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
    if (OB_SUCC(ret)) {
      if (!index_arg_.fulltext_columns_.empty()) {
        if (index_keyname_ != DOMAIN_KEY) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "CTXCAT without domain index");
        } else if (index_scope_ != NOT_SPECIFIED) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "GLOBAL/LOCAL option in domain ctxcat index");
        } else {
          type = INDEX_TYPE_DOMAIN_CTXCAT;
        }
        if (OB_SUCC(ret) && parser_name_.empty()) {
          index_arg_.index_option_.parser_name_ =
              common::ObString::make_string(common::OB_DEFAULT_FULLTEXT_PARSER_NAME);
        }
      } else {
        if (UNIQUE_KEY == index_keyname_) {
          if (global_) {
            type = INDEX_TYPE_UNIQUE_GLOBAL;
          } else {
            type = INDEX_TYPE_UNIQUE_LOCAL;
          }
        } else if (NORMAL_KEY == index_keyname_) {
          if (global_) {
            type = INDEX_TYPE_NORMAL_GLOBAL;
          } else {
            type = INDEX_TYPE_NORMAL_LOCAL;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      index_arg_.index_type_ = type;
      // create table with index .the status of index is available
      index_arg_.index_option_.index_status_ = INDEX_STATUS_AVAILABLE;
      index_arg_.index_option_.index_attributes_set_ = index_attributes_set_;
    }
    if (OB_FAIL(ret)) {
      // skip
    } else if (INDEX_TYPE_UNIQUE_GLOBAL == type || INDEX_TYPE_NORMAL_GLOBAL == type) {
      ObArray<ObColumnSchemaV2*> gen_columns;
      ObTableSchema& index_schema = index_arg_.index_schema_;
      index_schema.set_table_type(USER_INDEX);
      index_schema.set_index_type(index_arg_.index_type_);
      bool check_data_schema = false;
      if (OB_FAIL(share::ObIndexBuilderUtil::adjust_expr_index_args(index_arg_, table_schema, gen_columns))) {
        LOG_WARN("fail to adjust expr index args", K(ret));
      } else if (OB_FAIL(share::ObIndexBuilderUtil::set_index_table_columns(
                     index_arg_, table_schema, index_schema, check_data_schema))) {
        LOG_WARN("fail to set index table columns", K(ret));
      }
    }
  }

  return ret;
}

int ObCreateTableResolver::check_same_substr_expr(ObRawExpr& left, ObRawExpr& right, bool& same)
{
  int ret = OB_SUCCESS;
  same = true;

  if (left.get_expr_type() != T_FUN_SYS_SUBSTR || right.get_expr_type() != T_FUN_SYS_SUBSTR) {
    same = false;
  } else {
    ObSysFunRawExpr& sys_left = static_cast<ObSysFunRawExpr&>(left);
    ObSysFunRawExpr& sys_right = static_cast<ObSysFunRawExpr&>(right);
    if (sys_left.get_param_count() != sys_right.get_param_count()) {
      same = false;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_left.get_param_count() && same; ++i) {
        ObRawExpr* param_left = sys_left.get_param_expr(i);
        ObRawExpr* param_right = sys_right.get_param_expr(i);
        if (0 == i) {
          if (!param_left->is_column_ref_expr() || !param_right->is_column_ref_expr()) {
            same = false;
          } else {
            ObColumnRefRawExpr* col_left = static_cast<ObColumnRefRawExpr*>(param_left);
            ObColumnRefRawExpr* col_right = static_cast<ObColumnRefRawExpr*>(param_right);
            if (!ObCharset::case_insensitive_equal(col_left->get_column_name(), col_right->get_column_name())) {
              same = false;
            }
          }
        } else {
          if (!param_left->is_const_expr() || !param_right->is_const_expr()) {
            same = false;
          } else if (!param_left->same_as(*param_right)) {
            same = false;
          }
        }
      }
    }
  }

  return ret;
}

int ObCreateTableResolver::set_index_name()
{
  int ret = OB_SUCCESS;
  ObIndexNameHashWrapper key(index_name_);
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "allocator is null.", K(ret));
  } else if (OB_FAIL(current_index_name_set_.set_refactored(key))) {
    SQL_RESV_LOG(WARN, "set index name to current index name set failed", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, index_name_, index_arg_.index_name_))) {
    SQL_RESV_LOG(WARN, "write short index name failed", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObCreateTableResolver::set_index_option_to_arg()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "allocator is null.", K(ret));
  } else {
    index_arg_.index_option_.block_size_ = block_size_;
    if (OB_FAIL(ob_write_string(*allocator_, compress_method_, index_arg_.index_option_.compress_method_))) {
      SQL_RESV_LOG(WARN, "set compress func name failed", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_, comment_, index_arg_.index_option_.comment_))) {
      SQL_RESV_LOG(WARN, "set comment str failed", K(ret));
    } else {
      index_arg_.index_option_.parser_name_ = parser_name_;
      index_arg_.index_option_.row_store_type_ = row_store_type_;
      index_arg_.index_option_.store_format_ = store_format_;
      index_arg_.with_rowid_ = with_rowid_;
    }
  }

  return ret;
}

int ObCreateTableResolver::set_storing_column()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < store_column_names_.count(); ++i) {
    ret = index_arg_.store_columns_.push_back(store_column_names_.at(i));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < hidden_store_column_names_.count(); ++i) {
    ret = index_arg_.hidden_store_columns_.push_back(hidden_store_column_names_.at(i));
  }
  return ret;
}

int ObCreateTableResolver::set_fulltext_columns()
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt* crt_table_stmt = static_cast<ObCreateTableStmt*>(get_basic_stmt());
  if (OB_ISNULL(crt_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("crt_table_stmt is null");
  } else {
    const ObTableSchema& table_schema = crt_table_stmt->get_create_table_arg().schema_;
    for (int64_t i = 0; OB_SUCC(ret) && i < fulltext_column_names_.count(); ++i) {
      const ObString& ft_name = fulltext_column_names_.at(i);
      const ObColumnSchemaV2* ft_column = table_schema.get_column_schema(ft_name);
      if (OB_ISNULL(ft_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema failed", K(ret));
      } else if (!ft_column->is_string_type()) {
        ret = OB_ERR_BAD_FT_COLUMN;
        LOG_USER_ERROR(OB_ERR_BAD_FT_COLUMN, ft_name.length(), ft_name.ptr());
      } else if (OB_FAIL(index_arg_.fulltext_columns_.push_back(ft_name))) {
        LOG_WARN("store fulltext column name failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::set_table_option_to_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session_info is null.", K(ret));
  } else {
    bool is_oracle_mode = share::is_oracle_mode();
    table_schema.set_block_size(block_size_);
    table_schema.set_replica_num(replica_num_);
    int64_t progressive_merge_round = 0;
    int64_t tablet_size = tablet_size_;
    if (-1 == tablet_size) {
      tablet_size = common::ObServerConfig::get_instance().tablet_size;
    }
    table_schema.set_tablet_size(tablet_size);
    table_schema.set_pctfree(pctfree_);
    table_schema.set_collation_type(collation_type_);
    table_schema.set_charset_type(charset_type_);
    table_schema.set_is_use_bloomfilter(use_bloom_filter_);
    table_schema.set_auto_increment(auto_increment_);
    table_schema.set_tenant_id(session_info_->get_effective_tenant_id());
    table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
    table_schema.set_table_id(table_id_);
    table_schema.set_read_only(read_only_);
    table_schema.set_duplicate_scope(duplicate_scope_);
    table_schema.set_enable_row_movement(enable_row_movement_);
    table_schema.set_table_mode_struct(table_mode_);
    table_schema.set_dop(table_dop_);
    if (0 == progressive_merge_num_) {
      ObTenantConfigGuard tenant_config(TENANT_CONF(session_info_->get_effective_tenant_id()));
      table_schema.set_progressive_merge_num(
          tenant_config.is_valid() ? tenant_config->default_progressive_merge_num : 0);
    } else {
      table_schema.set_progressive_merge_num(progressive_merge_num_);
    }
    // set store format
    if (store_format_ == OB_STORE_FORMAT_INVALID) {
      ObString default_format;
      if (is_oracle_mode) {
        if (OB_ISNULL(GCONF.default_compress.get_value())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default oracle compress option is not set in server config", K(ret));
        } else {
          default_format = ObString::make_string(GCONF.default_compress.str());
        }
      } else {
        if (NULL == GCONF.default_row_format.get_value()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default row format is not set in server config", K(ret));
        } else {
          default_format = ObString::make_string(GCONF.default_row_format.str());
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL((ObStoreFormat::find_store_format_type(default_format, store_format_)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default compress not found!", K(ret), K_(store_format), K(default_format));
        } else if (!ObStoreFormat::is_store_format_valid(store_format_, is_oracle_mode)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected store format type", K_(store_format), K(is_oracle_mode), K(ret));
        } else {
          row_store_type_ = ObStoreFormat::get_row_store_type(store_format_);
        }
      }
    } else if (!ObStoreFormat::is_store_format_valid(store_format_, is_oracle_mode) ||
               row_store_type_ != ObStoreFormat::get_row_store_type(store_format_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected store format type or row store type",
          K_(store_format),
          K_(row_store_type),
          K(is_oracle_mode),
          K(ret));
    }

    if (OB_SUCC(ret)) {
      if (0 == progressive_merge_round) {
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2200) {
          progressive_merge_round = 0;
        } else {
          progressive_merge_round = 1;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_STORAGE_FORMAT_VERSION_INVALID == storage_format_version_) {
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2200) {
          storage_format_version_ = OB_STORAGE_FORMAT_VERSION_V2;
        } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3000) {
          storage_format_version_ = OB_STORAGE_FORMAT_VERSION_V3;
        } else {
          storage_format_version_ = OB_STORAGE_FORMAT_VERSION_V4;
        }
      }
    }

    // set compress method
    if (OB_SUCC(ret)) {
      if (is_oracle_mode) {
        const char* compress_name = NULL;
        if (OB_ISNULL(compress_name = ObStoreFormat::get_store_format_compress_name(store_format_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null compress name", K_(store_format), K(ret));
        } else {
          compress_method_ = ObString::make_string(compress_name);
        }
      } else if (compress_method_.empty()) {
        if (NULL == GCONF.default_compress_func.get_value()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default compress func name is not set in server config", K(ret));
        } else {
          bool found = false;
          for (int i = 0; i < ARRAYSIZEOF(common::compress_funcs) && !found; ++i) {
            // find again in case of case sensitive in server init parameters
            // all change to
            if (0 == ObString::make_string(common::compress_funcs[i]).case_compare(GCONF.default_compress_func.str())) {
              found = true;
              compress_method_ = ObString::make_string(common::compress_funcs[i]);
            }
          }
          if (!found) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("compress method not found!",
                K(ret),
                K_(compress_method),
                "default_compress_func",
                GCONF.default_compress_func.str());
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      table_schema.set_row_store_type(row_store_type_);
      table_schema.set_store_format(store_format_);
      table_schema.set_progressive_merge_round(progressive_merge_round);
      table_schema.set_storage_format_version(storage_format_version_);
      if (OB_FAIL(table_schema.set_expire_info(expire_info_)) ||
          OB_FAIL(table_schema.set_compress_func_name(compress_method_)) ||
          OB_FAIL(table_schema.set_comment(comment_)) || OB_FAIL(table_schema.set_tablegroup_name(tablegroup_name_)) ||
          OB_FAIL(table_schema.set_primary_zone(primary_zone_)) || OB_FAIL(table_schema.set_locality(locality_))) {
        SQL_RESV_LOG(WARN, "set table_options failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::resolve_index(const ParseNode* node, ObArray<int>& index_node_position_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    // do nothing, create table t as select ... will come here
  } else if (T_TABLE_ELEMENT_LIST != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(node->type_));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "stmt_ is null.", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(node->children_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_node_position_list.size(); ++i) {
      reset();
      index_attributes_set_ = OB_DEFAULT_INDEX_ATTRIBUTES_SET;
      index_arg_.reset();
      if (OB_UNLIKELY(index_node_position_list.at(i) >= node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(index_node_position_list.at(i)));
      } else if (OB_FAIL(resolve_index_node(node->children_[index_node_position_list.at(i)]))) {
        SQL_RESV_LOG(WARN, "resolve index node failed", K(ret));
      } else { /*do nothing*/
      }
    }
    current_index_name_set_.reset();
  }

  return ret;
}

int ObCreateTableResolver::resolve_index_node(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  ObString uk_name;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node is null.", K(ret));
  } else if ((T_INDEX != node->type_ || INDEX_NUM_CHILD != node->num_child_) &&
             (T_COLUMN_DEFINITION != node->type_ || COLUMN_DEFINITION_NUM_CHILD != node->num_child_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid arguments.", K(ret), K(node->type_), K(node->num_child_));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "stmt_ is null.", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "node->children_ is null.", K(ret));
  } else {
    index_arg_.reset();
    ObColumnSortItem sort_item;
    ObString first_column_name;
    ObColumnSchemaV2* column_schema = NULL;
    ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema& tbl_schema = create_table_stmt->get_create_table_arg().schema_;
    if (T_INDEX == node->type_) {
      // if index_name is not specified, new index_name will be generated
      // by the first_column_name, so resolve the index_column_list_node firstly.
      if (OB_SUCC(ret)) {
        if (NULL == node->children_[1] || T_INDEX_COLUMN_LIST != node->children_[1]->type_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid index column list.", K(ret));
        } else {
          int64_t index_data_length = 0;
          index_keyname_ = static_cast<INDEX_KEYNAME>(node->value_);
          ParseNode* index_column_list_node = node->children_[1];
          if (index_column_list_node->num_child_ > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
            ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
            LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
          } else if (OB_ISNULL(index_column_list_node->children_)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "children_ is null.", K(ret));
          } else { /*do nothing*/
          }

          ParseNode* index_column_node = NULL;
          bool is_ctxcat_added = false;
          if (OB_SUCC(ret) && OB_FAIL(add_new_indexkey_for_oracle_temp_table(index_column_list_node->num_child_))) {
            SQL_RESV_LOG(WARN, "add session id key failed", K(ret));
          }
          for (int32_t i = 0; OB_SUCC(ret) && i < index_column_list_node->num_child_; ++i) {
            ObString& column_name = sort_item.column_name_;
            if (NULL == index_column_list_node->children_[i] ||
                T_SORT_COLUMN_KEY != index_column_list_node->children_[i]->type_) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "invalid index_column_list_node.", K(ret));
            } else {
              index_column_node = index_column_list_node->children_[i];
              if (OB_ISNULL(index_column_node->children_) || index_column_node->num_child_ < 3 ||
                  OB_ISNULL(index_column_node->children_[0])) {
                ret = OB_ERR_UNEXPECTED;
                SQL_RESV_LOG(WARN,
                    "invalid index_column_node.",
                    K(ret),
                    K(index_column_node->num_child_),
                    K(index_column_node->children_),
                    K(index_column_node->children_[0]));
              } else {
                // column_name
                if (index_column_node->children_[0]->type_ != T_IDENT) {
                  sort_item.is_func_index_ = true;
                } else {
                  sort_item.is_func_index_ = false;
                }
                column_name.assign_ptr(const_cast<char*>(index_column_node->children_[0]->str_value_),
                    static_cast<int32_t>(index_column_node->children_[0]->str_len_));
                if (NULL != index_column_node->children_[1]) {
                  sort_item.prefix_len_ = static_cast<int32_t>(index_column_node->children_[1]->value_);
                  if (0 == sort_item.prefix_len_) {
                    ret = OB_KEY_PART_0;
                    LOG_USER_ERROR(OB_KEY_PART_0, column_name.length(), column_name.ptr());
                  }
                } else {
                  sort_item.prefix_len_ = 0;
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (sort_item.is_func_index_) {
                ObRawExpr* expr = NULL;
                if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(
                        column_name, *params_.expr_factory_, *session_info_, tbl_schema, expr, schema_checker_))) {
                  LOG_WARN("build generated column expr failed", K(ret));
                } else if (!expr->is_column_ref_expr()) {
                  // real index expr, so generate hidden generated column in data table schema
                  if (OB_FAIL(
                          ObIndexBuilderUtil::generate_ordinary_generated_column(*expr, tbl_schema, column_schema))) {
                    LOG_WARN("generate ordinary generated column failed", K(ret));
                  } else {
                    sort_item.column_name_ = column_schema->get_column_name_str();
                    sort_item.is_func_index_ = false;
                  }
                } else {
                  const ObColumnRefRawExpr* ref_expr = static_cast<const ObColumnRefRawExpr*>(expr);
                  sort_item.column_name_ = ref_expr->get_column_name();
                  sort_item.is_func_index_ = false;
                  column_schema = tbl_schema.get_column_schema(ref_expr->get_column_id());
                }
              } else {
                if (NULL == (column_schema = tbl_schema.get_column_schema(column_name))) {
                  ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
                  LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_ISNULL(session_info_)) {
                ret = OB_NOT_INIT;
                LOG_WARN("session_info_ is null");
              } else if (sort_item.prefix_len_ > column_schema->get_data_length()) {
                ret = OB_WRONG_SUB_KEY;
                SQL_RESV_LOG(WARN,
                    "prefix length is longer than column length",
                    K(sort_item),
                    K(column_schema->get_data_length()),
                    K(ret));
              } else if (ob_is_text_tc(column_schema->get_data_type())) {
                if (DOMAIN_KEY == index_keyname_) {
                  if (column_schema->get_meta_type().is_blob()) {
                    ret = OB_ERR_WRONG_KEY_COLUMN;
                    LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
                  }
                } else if (sort_item.prefix_len_ <= 0) {
                  ret = OB_ERR_WRONG_KEY_COLUMN;
                  LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
                }
              }
              if (OB_SUCC(ret) && ob_is_string_type(column_schema->get_data_type())) {
                int64_t length = 0;
                if (OB_FAIL(column_schema->get_byte_length(length))) {
                  SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
                } else if (sort_item.prefix_len_ > 0) {
                  if (0 >= column_schema->get_data_length()) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("column data length is less than or equal to zero", K(ret));
                  } else {
                    length = length * sort_item.prefix_len_ / column_schema->get_data_length();
                  }
                } else { /*do nothing*/
                }

                if (OB_SUCC(ret)) {
                  if ((index_data_length += length) > OB_MAX_USER_ROW_KEY_LENGTH) {
                    ret = OB_ERR_TOO_LONG_KEY_LENGTH;
                    LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
                  } else if (index_data_length <= 0) {
                    ret = OB_ERR_WRONG_KEY_COLUMN;
                    LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
                  } else {
                    // do nothing
                  }
                }
              }
            }
            if (OB_SUCC(ret)) {
              // column_order
              if (NULL != index_column_node->children_[2] && T_SORT_DESC == index_column_node->children_[2]->type_) {
                // sort_item.order_type_ = common::ObOrderType::DESC;
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("not support desc index now", K(ret));
              } else {
                sort_item.order_type_ = common::ObOrderType::ASC;
              }
              ObColumnNameHashWrapper column_key(column_name);
              if (OB_HASH_NOT_EXIST == column_name_set_.exist_refactored(column_key)) {
                ret = OB_ERR_BAD_FIELD_ERROR;
                LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                    column_name.length(),
                    column_name.ptr(),
                    table_name_.length(),
                    table_name_.ptr());
              } else {
                if (0 == i) {
                  first_column_name.assign_ptr(const_cast<char*>(index_column_node->children_[0]->str_value_),
                      static_cast<int32_t>(index_column_node->children_[0]->str_len_));
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(add_sort_column(sort_item))) {
                SQL_RESV_LOG(WARN, "add sort column failed", K(ret), K(sort_item));
              } else { /*do nothing*/
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (NULL != node->children_[3]) {
              if (T_USING_BTREE == node->children_[3]->type_) {
                index_arg_.index_using_type_ = USING_BTREE;
              } else {
                index_arg_.index_using_type_ = USING_HASH;
              }
            }
          }
        }
      }
    } else {
      // unique [key]
      if (T_COLUMN_DEFINITION != node->type_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(node->type_));
      } else if (NULL == node->children_[0] || T_COLUMN_REF != node->children_[0]->type_ ||
                 COLUMN_DEF_NUM_CHILD != node->children_[0]->num_child_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
      } else if (OB_ISNULL(node->children_[0]->children_) || OB_ISNULL(node->children_[0]->children_[2])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "node->ch[0]->ch[2] is null.", K(ret));
      } else {
        index_keyname_ = UNIQUE_KEY;
        ObString& column_name = sort_item.column_name_;
        column_name.assign_ptr(const_cast<char*>(node->children_[0]->children_[2]->str_value_),
            static_cast<int32_t>(node->children_[0]->children_[2]->str_len_));
        if (NULL == (column_schema = tbl_schema.get_column_schema(column_name))) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_USER_ERROR(
              OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), table_name_.length(), table_name_.ptr());
        } else if (ObTimestampTZType == column_schema->get_data_type()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
        } else if (ob_is_string_tc(column_schema->get_data_type())) {
          int64_t length = 0;
          if (OB_FAIL(column_schema->get_byte_length(length))) {
            SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
          } else if (length > OB_MAX_USER_ROW_KEY_LENGTH) {
            ret = OB_ERR_TOO_LONG_KEY_LENGTH;
            LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
          } else if (length <= 0) {
            ret = OB_ERR_WRONG_KEY_COLUMN;
            LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
          } else {
            // do nothing
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_new_indexkey_for_oracle_temp_table(0))) {
          SQL_RESV_LOG(WARN, "add session id key failed", K(ret));
        } else {
          first_column_name = sort_item.column_name_;
          sort_item.order_type_ = common::ObOrderType::ASC;
          if (OB_FAIL(add_sort_column(sort_item))) {
            SQL_RESV_LOG(WARN, "add sort column failed", K(ret), K(sort_item));
          }
        }
      }
      if (OB_SUCC(ret) && share::is_oracle_mode()) {
        ParseNode* attrs_node = node->children_[2];
        if (NULL != attrs_node && OB_UNLIKELY(4 == node->num_child_)) {
          if (OB_FAIL(resolve_uk_name_from_column_attribute(attrs_node, uk_name))) {
            SQL_RESV_LOG(WARN, "resolve uk name from column attribute", K(ret), K(sort_item));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      has_index_using_type_ = false;
      if (OB_FAIL(resolve_index_name(T_INDEX == node->type_ ? node->children_[0] : NULL,
              first_column_name,
              UNIQUE_KEY == index_keyname_ ? true : false,
              uk_name))) {
        SQL_RESV_LOG(WARN, "resolve index name failed", K(ret));
      } else if (T_INDEX == node->type_ && OB_FAIL(resolve_table_options(node->children_[2], true))) {
        SQL_RESV_LOG(WARN, "resolve index options failed", K(ret));
      } else if (tbl_schema.is_old_no_pk_table() && tbl_schema.is_partitioned_table() &&
                 OB_FAIL(store_part_key(tbl_schema, index_arg_))) {
        SQL_RESV_LOG(WARN, "failed to store part key", K(ret));
      } else if (OB_FAIL(generate_index_arg())) {
        SQL_RESV_LOG(WARN, "generate index arg failed", K(ret));
      } else {
        if (has_index_using_type_) {
          index_arg_.index_using_type_ = index_using_type_;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObCreateIndexStmt create_index_stmt;
      ObCreateIndexArg& create_index_arg = create_index_stmt.get_create_index_arg();
      ObSArray<ObPartitionResolveResult>& resolve_results = create_table_stmt->get_index_partition_resolve_results();
      ObSArray<obrpc::ObCreateIndexArg>& index_arg_list = create_table_stmt->get_index_arg_list();
      ObPartitionResolveResult resolve_result;
      if (OB_FAIL(create_index_arg.assign(index_arg_))) {
        LOG_WARN("fail to assign create index arg", K(ret));
      } else if (OB_FAIL(resolve_results.push_back(resolve_result))) {
        LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      } else if (OB_FAIL(index_arg_list.push_back(create_index_arg))) {
        LOG_WARN("fail to push back index_arg", K(ret));
      }
      //      if (T_INDEX == node->type_ &&
      //          NULL != node->children_[4] &&
      //          OB_FAIL(resolve_index_partition_node(node->children_[4], &create_index_stmt))) {
      //        LOG_WARN("fail to resolve partition option", K(ret));
      //      } else {
      //        ObPartitionResolveResult resolve_result;
      //        resolve_result.get_partition_fun_expr() = create_index_stmt.get_partition_fun_expr();
      //        resolve_result.get_range_values_exprs() = create_index_stmt.get_range_values_exprs();
      //        resolve_result.get_list_values_exprs() = create_index_stmt.get_list_values_exprs();
      //        if (OB_FAIL(resolve_results.push_back(resolve_result))) {
      //          LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      //        } else if (OB_FAIL(index_arg_list.push_back(create_index_arg))) {
      //          LOG_WARN("fail to push back index_arg", K(ret));
      //        }
      //      }
    }
  }
  return ret;
}

int ObCreateTableResolver::resolve_index_name(
    const ParseNode* node, const ObString& first_column_name, bool is_unique, ObString& uk_name)
{
  int ret = OB_SUCCESS;
  if (NULL == node) {
    if (share::is_oracle_mode() && is_unique) {
      if (NULL == uk_name.ptr()) {
        if (OB_FAIL(ObTableSchema::create_cons_name_automatically(
                index_name_, table_name_, *allocator_, CONSTRAINT_TYPE_UNIQUE_KEY))) {
          SQL_RESV_LOG(WARN, "create index name automatically failed", K(ret));
        }
      } else {
        index_name_.assign_ptr(uk_name.ptr(), uk_name.length());
      }
    } else if (share::is_oracle_mode() && !is_unique) {
      if (OB_FAIL(ObTableSchema::create_idx_name_automatically_oracle(index_name_, table_name_, *allocator_))) {
        SQL_RESV_LOG(WARN, "create index name automatically failed", K(ret));
      }
    } else {  // mysql mode
      if (OB_FAIL(generate_index_name(index_name_, current_index_name_set_, first_column_name))) {
        SQL_RESV_LOG(WARN, "generate index name failed", K(ret), K(index_name_));
      }
    }
  } else if (T_IDENT != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node_type is wrong.", K(ret));
  } else {
    int32_t len = static_cast<int32_t>(node->str_len_);
    index_name_.assign_ptr(node->str_value_, len);
    // check duplicate for index_name
    ObIndexNameHashWrapper index_key(index_name_);
    if (OB_HASH_EXIST == (ret = current_index_name_set_.exist_refactored(index_key))) {
      SQL_RESV_LOG(WARN, "duplicate index name", K(ret), K(index_name_));
      ret = OB_ERR_KEY_NAME_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_KEY_NAME_DUPLICATE, index_name_.length(), index_name_.ptr());
    } else if (0 == ObString::make_string("primary").case_compare(index_name_)) {
      // index name can not be 'primary'
      ret = OB_WRONG_NAME_FOR_INDEX;
      LOG_USER_ERROR(OB_WRONG_NAME_FOR_INDEX, index_name_.length(), index_name_.ptr());
    } else if (index_name_.empty()) {
      if (share::is_oracle_mode()) {
        ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
        SQL_RESV_LOG(WARN, "index name is empty", K(ret), K(index_name_));
      } else {
        ret = OB_WRONG_NAME_FOR_INDEX;
        SQL_RESV_LOG(WARN, "index name is empty", K(ret), K(index_name_));
        LOG_USER_ERROR(OB_WRONG_NAME_FOR_INDEX, index_name_.length(), index_name_.ptr());
      }
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t max_user_table_name_length =
        share::is_oracle_mode() ? OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
    if (index_name_.length() > max_user_table_name_length) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, index_name_.length(), index_name_.ptr());
    }
  }

  return ret;
}

int ObCreateTableResolver::resolve_table_charset_info(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  if (NULL != node) {
    if (T_TABLE_OPTION_LIST != node->type_ || node->num_child_ < 1 || OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
    } else {
      ParseNode* option_node = NULL;
      int32_t num = node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        option_node = node->children_[i];
        if (OB_ISNULL(option_node)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(option_node));
        } else if (T_CHARSET == option_node->type_ && OB_FAIL(resolve_table_option(option_node, false))) {
          SQL_RESV_LOG(WARN, "resolve failed", K(ret));
        } else if (T_COLLATION == option_node->type_ && OB_FAIL(resolve_table_option(option_node, false))) {
          SQL_RESV_LOG(WARN, "resolve failed", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (CHARSET_INVALID == charset_type_ && CS_TYPE_INVALID == collation_type_) {
      // The database character set and collation affect these aspects of server operation:
      //
      // For CREATE TABLE statements, the database character set and collation are used as default
      // values for table definitions if the table character set and collation are not specified.
      // To override this, provide explicit CHARACTER SET and COLLATE table options.
      const uint64_t tenant_id = session_info_->get_effective_tenant_id();
      ObString database_name;
      uint64_t database_id = OB_INVALID_ID;
      const ObDatabaseSchema* database_schema = NULL;
      int tmp_ret = 0;
      if (OB_SUCCESS != (tmp_ret = schema_checker_->get_database_id(tenant_id, database_name_, database_id))) {
        SQL_RESV_LOG(WARN, "fail to get database_id.", K(tmp_ret), K(database_name_), K(tenant_id));
      } else if (OB_SUCCESS !=
                 (tmp_ret = schema_checker_->get_database_schema(tenant_id, database_id, database_schema))) {
        LOG_WARN("failed to get db schema", K(tmp_ret), K(database_id));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error. db schema is null", K(ret), K(database_schema));
      } else {
        charset_type_ = database_schema->get_charset_type();
        collation_type_ = database_schema->get_collation_type();
      }
    } else if (OB_FAIL(ObCharset::check_and_fill_info(charset_type_, collation_type_))) {
      SQL_RESV_LOG(WARN, "fail to fill collation info", K(ret));
    }
  }

  return ret;
}

int ObCreateTableResolver::resolve_auto_partition(const ParseNode* partition_node)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  if (OB_ISNULL(partition_node) || T_AUTO_PARTITION != partition_node->type_ || 2 != partition_node->num_child_ ||
      OB_ISNULL(partition_node->children_[0]) || OB_ISNULL(partition_node->children_[1]) ||
      OB_ISNULL(create_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN,
        "node is unexpected",
        KR(ret),
        "type",
        get_type_name(partition_node->type_),
        "child_num",
        partition_node->num_child_);
  } else {
    const ParseNode* part_type_node = partition_node->children_[0];
    ObTableSchema& table_schema = create_table_stmt->get_create_table_arg().schema_;
    PartitionInfo part_info;
    share::schema::ObPartitionFuncType part_func_type = share::schema::PARTITION_FUNC_TYPE_RANGE;
    share::schema::ObPartitionOption* partition_option = NULL;
    part_info.part_level_ = share::schema::PARTITION_LEVEL_ONE;
    partition_option = &part_info.part_option_;
    const ParseNode* part_expr_node = part_type_node->children_[0];
    if (T_RANGE_COLUMNS_PARTITION == part_type_node->type_) {
      part_func_type = share::schema::PARTITION_FUNC_TYPE_RANGE_COLUMNS;
    } else if (T_RANGE_PARTITION == part_type_node->type_) {
      part_func_type = share::schema::PARTITION_FUNC_TYPE_RANGE;
    } else {
      ret = OB_NOT_SUPPORTED;
      SQL_RESV_LOG(WARN, "part type not supported", KR(ret), "type", get_type_name(part_type_node->type_));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(partition_option)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "partition option is null", KR(ret));
    } else {
      partition_option->set_part_func_type(part_func_type);
      partition_option->set_auto_part(true /*auto_part*/);
      int64_t part_size = -1;
      const ParseNode* part_size_node = partition_node->children_[1];
      if (T_VARCHAR == part_size_node->type_) {
        bool valid = false;
        common::ObSqlString buf;
        if (OB_FAIL(buf.append(part_size_node->str_value_, part_size_node->str_len_))) {
          SQL_RESV_LOG(WARN, "fail to assign child str", K(ret));
        } else {
          part_size = common::ObConfigCapacityParser::get(buf.ptr(), valid);
          if (!valid) {
            ret = common::OB_ERR_PARSE_SQL;
          } else if (OB_UNLIKELY(0 == part_size)) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "param, the param can't be zero", K(ret), K(buf));
          }
        }
      } else if (T_AUTO == part_size_node->type_) {
        part_size = 0;
      } else {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "part type not supported", KR(ret), "type", get_type_name(part_size_node->type_));
      }
      if (OB_SUCC(ret)) {
        partition_option->set_auto_part_size(part_size);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(part_expr_node)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("none of partition key not supported", KR(ret));
    } else {
      common::ObString func_expr_name;
      const bool is_subpartition = false;
      func_expr_name.assign_ptr(
          const_cast<char*>(part_type_node->str_value_), static_cast<int32_t>(part_type_node->str_len_));
      if (OB_FAIL(resolve_part_func(params_,
              part_expr_node,
              part_func_type,
              table_schema,
              part_info.part_func_exprs_,
              part_info.part_keys_))) {
        SQL_RESV_LOG(WARN, "resolve part func failed", KR(ret));
      } else if (OB_FAIL(partition_option->set_part_expr(func_expr_name))) {
        SQL_RESV_LOG(WARN, "set partition express string failed", KR(ret));
      } else if (OB_FAIL(set_partition_keys(table_schema, part_info.part_keys_, is_subpartition))) {
        SQL_RESV_LOG(WARN, "Failed to set partition keys", KR(ret), K(table_schema), K(is_subpartition));
      } else {
        ObPartition partition;
        common::ObArray<common::ObObj> rowkeys;
        partition.set_part_id(0);
        common::ObString part_name = common::ObString::make_string("p0");
        for (int64_t i = 0; i < part_info.part_keys_.count() && OB_SUCC(ret); ++i) {
          common::ObObj obj = ObObj::make_max_obj();
          if (OB_FAIL(rowkeys.push_back(obj))) {
            LOG_WARN("failed to push back obj", KR(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          if (0 >= rowkeys.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rowkey count can not be zero", KR(ret), K(rowkeys), "partition_key", part_info.part_keys_);
          } else {
            common::ObRowkey high_value(&rowkeys.at(0), rowkeys.count());
            if (OB_FAIL(partition.set_high_bound_val(high_value))) {
              LOG_WARN("failed to set high bound value", KR(ret), K(high_value));
            } else if (OB_FAIL(partition.set_part_name(part_name))) {
              LOG_WARN("failed to set part name", KR(ret), K(part_name));
            } else if (OB_FAIL(table_schema.add_partition(partition))) {
              LOG_WARN("failed to add partition", KR(ret), K(partition), K(table_schema));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_schema.get_part_option() = *partition_option;
      table_schema.set_part_level(share::schema::PARTITION_LEVEL_ONE);
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
