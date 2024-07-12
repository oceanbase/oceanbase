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
#include "share/ob_fts_index_builder_util.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_part_func_checker.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/ob_sql_utils.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/printer/ob_select_stmt_printer.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/resolver/cmd/ob_help_resolver.h"
#include "lib/charset/ob_template_helper.h"
#include "sql/optimizer/ob_optimizer_util.h"


namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace omt;
namespace sql
{
ObCreateTableResolver::ObCreateTableResolver(ObResolverParams &params)
    : ObCreateTableResolverBase(params),
      cur_column_id_(OB_APP_MIN_COLUMN_ID - 1),
      cur_column_group_id_(COLUMN_GROUP_START_ID),
      primary_keys_(),
      column_name_set_(),
      if_not_exist_(false),
      is_oracle_temp_table_(false),
      is_temp_table_pk_added_(false),
      index_arg_(),
      current_index_name_set_(),
      cur_udt_set_id_(0)
{
}

ObCreateTableResolver::~ObCreateTableResolver()
{
}

uint64_t ObCreateTableResolver::gen_column_id()
{
  return ++cur_column_id_;
}

uint64_t ObCreateTableResolver::gen_udt_set_id()
{
  return ++cur_udt_set_id_;
}

int64_t ObCreateTableResolver::get_primary_key_size() const
{
  return primary_keys_.count();
}

int ObCreateTableResolver::add_primary_key_part(const ObString &column_name,
                                                ObArray<ObColumnResolveStat> &stats,
                                                int64_t &pk_data_length)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  ObColumnSchemaV2 *col = NULL;
  if (OB_ISNULL(create_table_stmt)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "stmt is null", KP(create_table_stmt), K(ret));
  } else if (OB_FAIL(ObCreateTableResolverBase::add_primary_key_part(column_name,
                                                              create_table_stmt->get_create_table_arg().schema_,
                                                              primary_keys_.count(),
                                                              pk_data_length,
                                                              col))) {
    LOG_WARN("failed to add primary key part", KR(ret), K(column_name));
  } else if (OB_FAIL(primary_keys_.push_back(col->get_column_id()))) {
    SQL_RESV_LOG(WARN, "push primary key to array failed", K(ret));
  } else if (!lib::is_oracle_mode()) {
    // mysql 模式下，建表时主键列被 set null 或被 set default value = null，都要报错
    // oracle 模式下，建表时主键列被 set null 或被 set default value = null，都不会报错，所以跳过下面这个检查
    ObColumnResolveStat *stat = NULL;
    for (int64_t i = 0; NULL == stat && OB_SUCC(ret) && i < stats.count(); ++i) {
      if (stats.at(i).column_id_ == col->get_column_id()) {
        stat = &stats.at(i);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(stat)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "fail to find column stat", K(ret), K(column_name));
    } else if (stat->is_set_null_ || (stat->is_set_default_value_ && col->get_cur_default_value().is_null())) {
      ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
    }
  }
  return ret;
}

int ObCreateTableResolver::add_hidden_tablet_seq_col()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "stmt is NULL", K(stmt_), K(ret));
  } else if (0 == get_primary_key_size()) {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    ObColumnSchemaV2 hidden_pk;
    hidden_pk.reset();
    hidden_pk.set_column_id(OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
    hidden_pk.set_data_type(ObUInt64Type);
    hidden_pk.set_nullable(false);
    hidden_pk.set_is_hidden(true);
    hidden_pk.set_charset_type(CHARSET_BINARY);
    hidden_pk.set_collation_type(CS_TYPE_BINARY);
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
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "tablet seq expects to be the first primary key", K(stmt_), K(ret));
  }
  return ret;
}

int ObCreateTableResolver::add_hidden_external_table_pk_col()
{
  int ret = OB_SUCCESS;
  uint64_t COL_IDS[2] = {OB_HIDDEN_FILE_ID_COLUMN_ID, OB_HIDDEN_LINE_NUMBER_COLUMN_ID};
  const char* COL_NAMES[2] = {OB_HIDDEN_FILE_ID_COLUMN_NAME, OB_HIDDEN_LINE_NUMBER_COLUMN_NAME};
  if (OB_ISNULL(stmt_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "stmt is NULL", K(stmt_), K(ret));
  } else {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    for (int i = 0; OB_SUCC(ret) && i < array_elements(COL_IDS); i++) {
      ObColumnSchemaV2 hidden_pk;
      hidden_pk.reset();
      hidden_pk.set_column_id(COL_IDS[i]);
      hidden_pk.set_data_type(ObIntType);
      hidden_pk.set_nullable(false);
      hidden_pk.set_is_hidden(true);
      hidden_pk.set_charset_type(CHARSET_BINARY);
      hidden_pk.set_collation_type(CS_TYPE_BINARY);
      if (OB_FAIL(hidden_pk.set_column_name(COL_NAMES[i]))) {
        SQL_RESV_LOG(WARN, "failed to set column name", K(ret));
      } else if (OB_FAIL(primary_keys_.push_back(COL_IDS[i]))) {
        SQL_RESV_LOG(WARN, "failed to push_back column_id", K(ret));
      } else {
        hidden_pk.set_rowkey_position(primary_keys_.count());
        if (OB_FAIL(table_schema.add_column(hidden_pk))) {
          SQL_RESV_LOG(WARN, "add column to table_schema failed", K(ret), K(hidden_pk));
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::add_udt_hidden_column(ObTableSchema &table_schema,
                                                 ObSEArray<ObColumnSchemaV2, SEARRAY_INIT_NUM> &resolved_cols,
                                                 ObColumnSchemaV2 &udt_column)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  ObString tmp_str;
  ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (udt_column.is_xmltype()) {
    ObColumnSchemaV2 hidden_blob;
    ObSEArray<ObString, 4> gen_col_expr_arr;
    ObString tmp_str;
    char col_name[128] = {0};
    hidden_blob.reset();
    hidden_blob.set_column_id(gen_column_id());
    hidden_blob.set_data_type(ObLongTextType);
    hidden_blob.set_nullable(udt_column.is_nullable());
    hidden_blob.set_udt_set_id(udt_column.get_udt_set_id());
    hidden_blob.set_is_hidden(true);
    hidden_blob.set_charset_type(CHARSET_BINARY);
    hidden_blob.set_collation_type(CS_TYPE_BINARY);
    databuff_printf(col_name, 128, "SYS_NC%05lu$",cur_column_id_);
    if (OB_FAIL(hidden_blob.set_column_name(col_name))) {
      SQL_RESV_LOG(WARN, "failed to set column name", K(ret));
    } else if (OB_FAIL(check_default_value(udt_column.get_cur_default_value(),
                                           session_info_->get_tz_info_wrap(),
                                           &tmp_str,    // useless
                                           NULL,
                                           *allocator_,
                                           table_schema,
                                           resolved_cols,
                                           udt_column,
                                           gen_col_expr_arr,  // useless
                                           session_info_->get_sql_mode(),
                                           session_info_,
                                           true,
                                           schema_checker_))) {
      SQL_RESV_LOG(WARN, "check udt column default value failed", K(ret), K(hidden_blob));
    } else if (OB_FAIL(resolved_cols.push_back(hidden_blob))) {
      SQL_RESV_LOG(WARN, "add column to table_schema failed", K(ret), K(hidden_blob));
    }
  } else if (udt_column.is_geometry() && lib::is_oracle_mode()) {
    // oracle sdo_geometry type
    if (tenant_data_version < DATA_VERSION_4_2_2_0
    || (tenant_data_version >= DATA_VERSION_4_3_0_0 && tenant_data_version < DATA_VERSION_4_3_1_0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("sdo_geometry is not supported when data_version is below 4.2.2.0 or data_version is above 4.3.0.0 but below 4.3.1.0.", K(ret), K(tenant_data_version), K(udt_column));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2.2 or data_version is above 4.3.0.0 but below 4.3.1.0, sdo_geometry");
    } else if (OB_ISNULL(create_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null create table stmt", K(ret));
    } else if (OB_FAIL(check_udt_default_value(udt_column.get_cur_default_value(),
                                              session_info_->get_tz_info_wrap(),
                                              &tmp_str,    // useless
                                              *allocator_,
                                              table_schema,
                                              udt_column,
                                              session_info_->get_sql_mode(),
                                              session_info_,
                                              schema_checker_,
                                              create_table_stmt->get_ddl_arg()))) {
      SQL_RESV_LOG(WARN, "check udt column default value failed", K(ret), K(udt_column.get_cur_default_value()));
    }

  }
  return ret;
}

int ObCreateTableResolver::add_udt_hidden_column(ObTableSchema &table_schema,
                                                 ObColumnSchemaV2 &udt_column)
{
  int ret = OB_SUCCESS;
  if (udt_column.is_xmltype()) {
    ObColumnSchemaV2 hidden_blob;
    ObSEArray<ObString, 4> gen_col_expr_arr;
    ObString tmp_str;
    char col_name[128] = {0};
    hidden_blob.reset();
    hidden_blob.set_column_id(gen_column_id());
    hidden_blob.set_data_type(ObLongTextType);
    hidden_blob.set_nullable(udt_column.is_nullable());
    hidden_blob.set_udt_set_id(udt_column.get_udt_set_id());
    hidden_blob.set_is_hidden(true);
    hidden_blob.set_charset_type(CHARSET_BINARY);
    hidden_blob.set_collation_type(CS_TYPE_BINARY);
    databuff_printf(col_name, 128, "SYS_NC%05lu$",cur_column_id_);
    if (OB_FAIL(hidden_blob.set_column_name(col_name))) {
      SQL_RESV_LOG(WARN, "failed to set column name", K(ret));
    } else if (OB_FAIL(table_schema.add_column(hidden_blob))) {
      SQL_RESV_LOG(WARN, "add column to table_schema failed", K(ret), K(hidden_blob));
    }
  }
  return ret;
}

//为临时表做额外的信息设置
int ObCreateTableResolver::set_temp_table_info(ObTableSchema &table_schema, ParseNode *commit_option_node)
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
      table_schema.set_session_id(session_info_->get_sessid_for_table()); ////设置session_id和session创建时间, 用于清理时的判断, oracle功能不同不需要设置
    }
    table_schema.set_sess_active_time(ObTimeUtility::current_time());
  }
  LOG_DEBUG("resolve create temp table", K(session_info_->is_obproxy_mode()), K(*session_info_), K(table_schema));
  return ret;
}

 // 列定义添加(__session_id bigint, __session_create_time bigint), 如果用户表定义已经出现__session_id等后面解析时会报错...
 int ObCreateTableResolver::add_new_column_for_oracle_temp_table(ObTableSchema &table_schema,
                                                                 ObArray<ObColumnResolveStat> &stats)
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
    column.set_meta_type(meta_int);
    column.set_column_id(OB_HIDDEN_SESSION_ID_COLUMN_ID);
    column.set_is_hidden(true);
    stat.column_id_ = column.get_column_id();
    if (OB_FAIL(column.set_column_name(OB_HIDDEN_SESSION_ID_COLUMN_NAME))) {
      LOG_WARN("failed to set column name", K(ret));
    } else if (OB_FAIL(table_schema.add_column(column))) {
      SQL_RESV_LOG(WARN, "fail to add column", K(ret));
    } else if (OB_FAIL(stats.push_back(stat))) {
      SQL_RESV_LOG(WARN, "fail to push back stat", K(ret));
    } else {
      stat.reset();
      column.set_meta_type(meta_int);
      column.set_column_id(OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID);
      column.set_is_hidden(true);
      stat.column_id_ = column.get_column_id();
      if (OB_FAIL(column.set_column_name(OB_HIDDEN_SESS_CREATE_TIME_COLUMN_NAME))) {
        LOG_WARN("failed to set column name", K(ret));
      } else if (OB_FAIL(table_schema.add_column(column))) {
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

// 索引义添加__session_id并作为首列
int ObCreateTableResolver::add_new_indexkey_for_oracle_temp_table(const int32_t org_key_len)
{
  int ret = OB_SUCCESS;
  if (is_oracle_temp_table_) {
    if (org_key_len + 1 > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
      ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
      LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    } else {
      ObColumnSortItem sort_item;
      sort_item.column_name_.assign_ptr(OB_HIDDEN_SESSION_ID_COLUMN_NAME,
                                        static_cast<int32_t>(strlen(OB_HIDDEN_SESSION_ID_COLUMN_NAME)));
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

// PK定义添加__session_id并作为首列
int ObCreateTableResolver::add_pk_key_for_oracle_temp_table(ObArray<ObColumnResolveStat> &stats,
                                                            int64_t &pk_data_length)
{
  int ret = OB_SUCCESS;
  if (is_oracle_temp_table_) {
    ObString key_name(OB_HIDDEN_SESSION_ID_COLUMN_NAME);
    if (OB_FAIL(add_primary_key_part(key_name, stats, pk_data_length))) {
      SQL_RESV_LOG(WARN, "add primary key part failed", K(ret), K(key_name));
    }
    is_temp_table_pk_added_ = true;
  }
  return ret;
}

// 分区信息增加 partition by key(__session_id) partitions 16 (同__all_table的分区方式)
int ObCreateTableResolver::set_partition_info_for_oracle_temp_table(share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_temp_table_) {
    ObString partition_expr;
    common::ObSEArray<ObString, 2> partition_keys;
    char expr_str_buf[64] = {'\0'};
    int64_t pos1 = 0;
    const int64_t buf_len = 64;
    const int64_t partition_num = 16; //和__all_table的分区一致
    share::schema::ObPartitionOption *partition_option = NULL;
    share::schema::ObPartitionFuncType partition_func_type = share::schema::PARTITION_FUNC_TYPE_HASH;
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
      } else if (FALSE_IT(partition_option->set_part_func_type(partition_func_type))) {
      } else if (FALSE_IT(partition_option->set_part_num(partition_num))) {
      } else if (OB_FAIL(generate_default_hash_part(partition_num,
                                                    table_schema.get_tablespace_id(),
                                                    table_schema))) {
        LOG_WARN("failed to generate default hash part", K(ret));
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  bool is_temporary_table = false;
  bool has_dblink_node = false;
  const bool is_mysql_mode = !is_oracle_mode();
  ParseNode *create_table_node = const_cast<ParseNode*>(&parse_tree);
  CHECK_COMPATIBILITY_MODE(session_info_);
  if (OB_ISNULL(create_table_node)
      || T_CREATE_TABLE != create_table_node->type_
      || (CREATE_TABLE_NUM_CHILD != create_table_node->num_child_ &&
          CREATE_TABLE_AS_SEL_NUM_CHILD != create_table_node->num_child_)
      || OB_ISNULL(create_table_node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
  } else {
    ObCreateTableStmt *create_table_stmt = NULL;
    ObString table_name;
    char *dblink_name_ptr = NULL;
    int32_t dblink_name_len = 0;
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
    //resolve temporary option or external table option
    if (OB_SUCC(ret)) {
      if (NULL != create_table_node->children_[0]) {
          switch (create_table_node->children_[0]->type_) {
            case T_TEMPORARY:
              if (create_table_node->children_[5] != NULL) { //临时表不支持分区
                ret = OB_ERR_TEMPORARY_TABLE_WITH_PARTITION;
              } else if (lib::is_mysql_mode()) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "MySQL compatible temporary table");
              } else {
                is_temporary_table = true;
                is_oracle_temp_table_ = (is_mysql_mode == false);
              }
              break;
            case T_EXTERNAL: {
              uint64_t tenant_version = 0;
              if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_version))) {
                LOG_WARN("failed to get data version", K(ret));
              } else if (tenant_version < DATA_VERSION_4_2_0_0) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2, external table");
              } else {
                create_table_stmt->get_create_table_arg().schema_.set_table_type(EXTERNAL_TABLE);
                is_external_table_ = true;
              }
              break;
            }
            default:
              ret = OB_INVALID_ARGUMENT;
              SQL_RESV_LOG(WARN, "invalid argument.",
                           K(ret), K(create_table_node->children_[0]->type_));
            }
      }
    }
    //resolve if_not_exists
    if (OB_SUCC(ret)) {
      if (NULL != create_table_node->children_[1]) {
        if (T_IF_NOT_EXISTS != create_table_node->children_[1]->type_) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid argument.",
                       K(ret), K(create_table_node->children_[1]->type_));
        } else {
          if_not_exist_ = true;
        }
      }
    }
    //resolve table_name
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(create_table_node->children_[2])) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
      } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
        ret = OB_NOT_INIT;
        SQL_RESV_LOG(WARN, "session_info is null.", K(ret));
      } else if (OB_FAIL(resolve_table_relation_node(create_table_node->children_[2], table_name, database_name,
                                                     false, false, &dblink_name_ptr, &dblink_name_len, &has_dblink_node))) {
        SQL_RESV_LOG(WARN, "failed to resolve table relation node!", K(ret));
      } else if (has_dblink_node) { //don't care about dblink_name_len
        // Check whether the child nodes of table_node have dblink ParseNode,
        // If so, an error will be reported.
        ret = OB_ERR_DDL_ON_REMOTE_DATABASE;
        SQL_RESV_LOG(WARN, "create table on remote database by dblink.", K(ret));
        LOG_USER_ERROR(OB_ERR_DDL_ON_REMOTE_DATABASE);
      } else if ((ObString(OB_RECYCLEBIN_SCHEMA_NAME) == database_name
                  && ObSQLSessionInfo::USER_SESSION == session_info_->get_session_type())
                 || ObString(OB_PUBLIC_SCHEMA_NAME) == database_name) {
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
      } else if (OB_FAIL(ob_write_string(
                  *allocator_, database_name, create_table_stmt->get_non_const_db_name()))) {
        SQL_RESV_LOG(WARN, "Failed to deep copy database name to stmt", K(ret));
      } else if (ObCharset::case_insensitive_equal(ObString(strlen(OB_SYS_DATABASE_NAME), OB_SYS_DATABASE_NAME), database_name)) {
        uint64_t tenant_id = session_info_->get_effective_tenant_id();
        uint64_t database_id = OB_INVALID_ID;
        if (OB_ISNULL(schema_checker_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "schema_checker_ is null.", K(ret));
        } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, database_name, database_id)))  {
          SQL_RESV_LOG(WARN, "fail to get database_id.", K(ret), K(database_name), K(tenant_id));
        } else {
          create_table_stmt->set_database_id(database_id);
        }
      } else {
        //创建表时，resolver往RS层传递database_name，而不是database_id,
        //后面产生临时的schema_checker，需要用到database_id,
        //所以这里使用任意值设置database_id,
        //resolve结束时，将database_id设置成OB_INVALID_ID
        create_table_stmt->set_database_id(generate_table_id());
      }
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OZ (schema_checker_->check_ora_ddl_priv(tenant_id,
                                                session_info_->get_priv_user_id(),
                                                database_name,
                                                stmt::T_CREATE_TABLE,
                                                session_info_->get_enable_role_array()),
            session_info_->get_user_id(), database_name, session_info_->get_enable_role_array());
      }
      // string列长度及default value长度的判断逻辑依赖于列的字符集类型
      // mysql的行为是若列指定了字符集则用指定的，反之用表的字符集
      // 因此需要在resolve_table_elements之前resolve table charset&collation
      if (OB_SUCC(ret)) {
        if (OB_FAIL(resolve_table_charset_info(create_table_node->children_[4]))) {
          SQL_RESV_LOG(WARN, "fail to resolve charset and collation of table", K(ret));
        } else if (is_create_as_sel) {
          if (OB_NOT_NULL(create_table_node->children_[3]) && T_TABLE_ELEMENT_LIST != create_table_node->children_[3]->type_) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(create_table_node->children_[2]->type_));
          } else { /* do nothing */ }
        }  else if (OB_ISNULL(create_table_node->children_[3])) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
        } else if (T_TABLE_ELEMENT_LIST != create_table_node->children_[3]->type_) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(create_table_node->children_[3]->type_));
        } else {
          // do nothing
        }

        if (OB_SUCC(ret) && is_external_table_) {
          //before resolve table elements
          if (OB_FAIL(resolve_external_table_format_early(create_table_node->children_[4]))) {
            LOG_WARN("fail to resolve external file format", K(ret));
          }
        }

        // 1、 resolve table_id first for check whether is inner_table
        if (OB_SUCC(ret) && OB_FAIL(resolve_table_id_pre(create_table_node->children_[4]))) {
          SQL_RESV_LOG(WARN, "resolve_table_id_pre failed", K(ret));
        }

        //consider index can be defined before column, so column should be
        //resolved firstly;avoid to rescan table_element_list_node, use a
        //array named index_node_position_list to record the position of indexes
        ParseNode *table_element_list_node = create_table_node->children_[3];
        ObArray<int> index_node_position_list;
        ObArray<int> foreign_key_node_position_list;
        ObArray<int> table_level_constraint_list;
        if (OB_SUCC(ret)) {
          if (false == is_create_as_sel) {
            if (OB_FAIL(resolve_table_elements(table_element_list_node, index_node_position_list, foreign_key_node_position_list, table_level_constraint_list, RESOLVE_ALL))) {
              SQL_RESV_LOG(WARN, "resolve table elements failed", K(ret));
            }
          } else {
            if (OB_FAIL(resolve_table_elements(table_element_list_node, index_node_position_list, foreign_key_node_position_list, table_level_constraint_list, RESOLVE_COL_ONLY))) {
              SQL_RESV_LOG(WARN, "resolve table elements col failed", K(ret));
            } else if (OB_FAIL(resolve_insert_mode(&parse_tree))) {
              SQL_RESV_LOG(WARN, "resolve ignore_or_replace flag failed", K(ret));
            } else if (OB_FAIL(resolve_table_elements_from_select(parse_tree))) {
              SQL_RESV_LOG(WARN, "resolve table elements from select failed", K(ret));
            } else if (OB_FAIL(resolve_table_elements(table_element_list_node, index_node_position_list, foreign_key_node_position_list, table_level_constraint_list, RESOLVE_NON_COL))) {
              SQL_RESV_LOG(WARN, "resolve table elements non-col failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && lib::is_oracle_mode()) {
            if (OB_FAIL(generate_primary_key_name_array(create_table_stmt->get_create_table_arg().schema_, pk_columns_name))) {
              SQL_RESV_LOG(WARN, "generate primary_key_name_array failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            reset();
            // 当用户建表未指定主键时，内部实现为新无主键表，暂不支持用户指定TableOrganizationFormat
            if (0 == get_primary_key_size()) {
              // change default no pk to heap table
              table_mode_.organization_mode_ = TOM_HEAP_ORGANIZED;
              table_mode_.pk_mode_ = TPKM_TABLET_SEQ_PK;
            }
            if (is_oracle_temp_table_) {
              //oracle global temp table default table mode is queuing
              table_mode_.mode_flag_ = TABLE_MODE_QUEUING;
            }
            ObTenantConfigGuard tenant_config(TENANT_CONF(session_info_->get_effective_tenant_id()));
            if (OB_SUCC(ret) && OB_LIKELY(tenant_config.is_valid())) {
              const char *ptr = NULL;
              if (OB_ISNULL(ptr = tenant_config->default_auto_increment_mode.get_value())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("default auto increment mode ptr is null", K(ret));
              } else {
                table_mode_.auto_increment_mode_ =
                  (0 == ObString::make_string("order").case_compare(ptr)) ?
                    ObTableAutoIncrementMode::ORDER : ObTableAutoIncrementMode::NOORDER;
                table_mode_.rowid_mode_ = tenant_config->default_enable_extended_rowid ?
                    ObTableRowidMode::ROWID_EXTENDED : ObTableRowidMode::ROWID_NORMAL;
              }
            }
            ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
            if (!table_schema.is_sys_table()) {
              pctfree_ = 0; // set default pctfree value for non-sys table
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(resolve_table_options(create_table_node->children_[4], false))) {
              SQL_RESV_LOG(WARN, "resolve table options failed", K(ret));
            } else if (OB_FAIL(set_table_option_to_schema(table_schema))) {
              SQL_RESV_LOG(WARN, "set table option to schema failed", K(ret));
            } else if (OB_FAIL(check_max_row_data_length(table_schema))) {
              SQL_RESV_LOG(WARN, "check max row data length failed", K(ret));
            } else {
              table_schema.set_collation_type(collation_type_);
              table_schema.set_charset_type(charset_type_);
              //不再需要这个步骤。在resolve开始的时候，就直接先将表的collation/charset信息解析出来，等到resolve列的信息时，已经能够获取
              //表的collation/charset信息
              //if (OB_FAIL(table_schema.fill_column_collation_info())) {
              //  SQL_RESV_LOG(WARN, "fail to fill column collation info", K(ret), K(table_name_));
              //} else {
              //  //do nothing
              //}
            }
          }
        }

        if (OB_SUCC(ret) && is_external_table_) {
          //external table support check
          ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
          if (index_node_position_list.count() > 0
              || foreign_key_node_position_list.count() > 0
              || table_level_constraint_list.count() > 0
              || table_schema.get_constraint_count() > 0
              || is_create_as_sel) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation on external table");
          }
        }

        // !!Attention!! resolve_partition_option should always call after resolve_table_options
        if (OB_SUCC(ret)) {
          ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
          if (OB_FAIL(resolve_partition_option(
                      create_table_node->children_[5], table_schema,
                      (is_mysql_mode && 1 == create_table_node->reserved_) ? false : true))) {
            SQL_RESV_LOG(WARN, "resolve partition option failed", K(ret));
          }
        }

        if (OB_SUCC(ret) && create_table_stmt->get_create_table_arg().schema_.is_external_table()) {
          if (create_table_stmt->get_create_table_arg().schema_.get_part_level() == ObPartitionLevel::PARTITION_LEVEL_ONE) {
            OZ (create_default_partition_for_table(create_table_stmt->get_create_table_arg().schema_));
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(add_hidden_external_table_pk_col())) {
            LOG_WARN("fail to add hidden pk col for external table", K(ret));
          }
        }

        // 4.0 new heap table has hidden primary key (tablet seq)
        if (OB_SUCC(ret) && 0 == get_primary_key_size()
            && TOM_HEAP_ORGANIZED == table_mode_.organization_mode_) {
          ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
          if (OB_FAIL(add_hidden_tablet_seq_col())) {
            SQL_RESV_LOG(WARN, "failed to add hidden primary key tablet seq", K(ret));
          } else if (!is_create_as_sel && OB_FAIL(add_inner_index_for_heap_gtt())) {
            SQL_RESV_LOG(WARN, "failed to add_inner_index_for_heap_gtt", K(ret));
          }
        }

        // column group
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_column_group(create_table_node->children_[6]))) {
            SQL_RESV_LOG(WARN, "fail to resolve column group", KR(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
          if (OB_FAIL(check_skip_index(table_schema))) {
            SQL_RESV_LOG(WARN, "fail to resolve skip index", KR(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
          table_schema.set_define_user_id(session_info_->get_priv_user_id());
          create_table_stmt->set_if_not_exists(if_not_exist_);
          if (false == is_temporary_table && OB_NOT_NULL(create_table_node->children_[7])) {
            ret = OB_ERR_PARSER_SYNTAX;
            SQL_RESV_LOG(WARN, "on commit option can only be used for temp table", K(ret));
          } else if (is_temporary_table && OB_FAIL(set_temp_table_info(table_schema, create_table_node->children_[7]))) {
            SQL_RESV_LOG(WARN, "set temp table info failed", K(ret));
          } else if (OB_FAIL(table_schema.set_table_name(table_name_))) {
            SQL_RESV_LOG(WARN, "set table name failed", K(ret));
          } else {
            create_table_stmt->set_database_id(OB_INVALID_ID);
          }
          //查询建表或创建临时表T时, 记录接受请求的obs地址obs#1, 用于obs后台job清理时, 限制T仅可由当初创建时的obs#1 drop
          if (OB_SUCC(ret) && (is_temporary_table || is_create_as_sel)) {
            char create_host_str[OB_MAX_HOST_NAME_LENGTH];
            MYADDR.ip_port_to_string(create_host_str, OB_MAX_HOST_NAME_LENGTH);
            table_schema.set_create_host(create_host_str);
            if (is_temporary_table) {
              table_schema.set_sess_active_time(ObTimeUtility::current_time());
            }
          }
        }

        //放到临时表信息设置后解析, 因为涉及临时表不支持外键引用的报错检查
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_index(table_element_list_node, index_node_position_list))) {
            SQL_RESV_LOG(WARN, "resolve index failed", K(ret));
          } else if (OB_FAIL(resolve_foreign_key(table_element_list_node, foreign_key_node_position_list))) {
            SQL_RESV_LOG(WARN, "resolve foreign key failed", K(ret));
          } else if (OB_FAIL(resolve_table_level_constraint_for_mysql(table_element_list_node, table_level_constraint_list))) {
            SQL_RESV_LOG(WARN, "resolve check constraint failed", K(ret));
          } else { /* do nothing */ }
        }
        // 对foreign key 进行references 权限检查
        if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
          const ObSArray<ObCreateForeignKeyArg> &fka_list =
              create_table_stmt->get_foreign_key_arg_list();
          for (int i = 0; OB_SUCC(ret) && i < fka_list.count(); ++i) {
            const ObCreateForeignKeyArg &fka = fka_list.at(i);
            // Oracle 官方文档关于 references 权限的描述非常少。
            // 文档中比较重要的一条是“references 权限不能被授予给角色”，
            // 再加上测试的结果，所以我们推测references权限进行检查时，
            // 检查的不是当前用户是否具有references权限，而是去检查子表所在的schema有没有references的权限。
            // 所以现在的逻辑是
            //   1. 当子表和父表相同时，无需检查
            //   2. 当子表和父表同属一个schema时，也无需检查，这一点已经在oracle上验证了。
            //   所以在代码里面，当database_name_和 fka.parent_database_相同时，就 skip 检查。
            if (0 == database_name_.case_compare(fka.parent_database_)) {
            } else {
              OZ (schema_checker_->check_ora_ddl_ref_priv(tenant_id,
                                                      database_name_,
                                                      fka.parent_database_,
                                                      fka.parent_table_,
                                                      fka.parent_columns_,
                                                      static_cast<uint64_t>(ObObjectType::TABLE),
                                                      stmt::T_CREATE_TABLE,
                                                      session_info_->get_enable_role_array()));
            }
          }
        }
      }
    }
    // checking uk-pk and uk-uk duplicate in oracle mode
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      const ObSArray<obrpc::ObCreateIndexArg> &index_arg_list = create_table_stmt->get_index_arg_list();
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
    if (OB_SUCC(ret)){
      if (OB_FAIL(deep_copy_string_in_part_expr(create_table_stmt))) {
        LOG_WARN("failed to deep copy string in part expr");
      }
    }
    if (OB_SUCC(ret) && is_create_as_sel) {
      if (OB_FAIL(resolve_hints(create_table_node->children_[9],
                               *create_table_stmt,
                               create_table_stmt->get_create_table_arg().schema_))) {
        LOG_WARN("fail to resolve hint", K(ret));
      }
    }
  }
  return ret;
}

// 生成主键列名字构成的 array
int ObCreateTableResolver::generate_primary_key_name_array(const ObTableSchema &table_schema,
                                                           ObIArray<ObString> &pk_columns_name)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column = NULL;

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

// 生成有 index_arg_list 里 uk 的下标构成的 array
int ObCreateTableResolver::generate_uk_idx_array(const ObIArray<obrpc::ObCreateIndexArg> &index_arg_list,
                                                 ObIArray<int64_t> &uk_idx_in_index_arg_list)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list.count(); ++i) {
    if (INDEX_TYPE_UNIQUE_LOCAL == index_arg_list.at(i).index_type_
        || INDEX_TYPE_UNIQUE_GLOBAL == index_arg_list.at(i).index_type_
        || INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_arg_list.at(i).index_type_) {
      if (OB_FAIL(uk_idx_in_index_arg_list.push_back(i))) {
        SQL_RESV_LOG(WARN, "failed to push back to uk_idx_in_index_arg_list", K(ret));
      }
    }
  }

  return ret;
}

// 检查建表时主键和唯一索是否建到完全相同（包括顺序）的列或列族上
bool ObCreateTableResolver::is_pk_uk_duplicate(const ObIArray<ObString> &pk_columns_name,
                                               const ObIArray<obrpc::ObCreateIndexArg> &index_arg_list,
                                               const ObIArray<int64_t> &uk_idx)
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

// 检查建表时唯一索引和唯一索引是否建到完全相同（包括顺序）的列或列族上
bool ObCreateTableResolver::is_uk_uk_duplicate(const ObIArray<int64_t> &uk_idx,
                                               const ObIArray<obrpc::ObCreateIndexArg> &index_arg_list)
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
            if (0 != index_arg_list.at(i).index_columns_.at(k).column_name_.case_compare(index_arg_list.at(j).index_columns_.at(k).column_name_)) {
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

int ObCreateTableResolver::resolve_partition_option(
    ParseNode *node, ObTableSchema &table_schema, const bool is_partition_option_node_with_opt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCreateTableResolverBase::resolve_partition_option(node, table_schema, is_partition_option_node_with_opt))) {
    LOG_WARN("fail to resolve partition option", KR(ret));
  } else if (is_oracle_temp_table_ && OB_FAIL(set_partition_info_for_oracle_temp_table(table_schema))) {
    SQL_RESV_LOG(WARN, "set __sess_id as partition key failed", KR(ret));
  }
  if (OB_SUCC(ret) && (OB_NOT_NULL(node) || table_schema.is_external_table() || is_oracle_temp_table_)) {
    if (OB_FAIL(check_generated_partition_column(table_schema))) {
      LOG_WARN("Failed to check generate partiton column", KR(ret));
    } else if (OB_FAIL(table_schema.check_primary_key_cover_partition_column())) {
      SQL_RESV_LOG(WARN, "fail to check primary key cover partition column", KR(ret));
    } else if (OB_FAIL(table_schema.check_auto_partition_valid())) {
      LOG_WARN("failed to check auto partition valid", KR(ret));
    } else { }//do nothing
  }
  return ret;
}

int ObCreateTableResolver::create_default_partition_for_table(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObPartition partition;
  ObRawExpr *maxvalue_expr = NULL;
  ObConstRawExpr *c_expr = NULL;
  ObOpRawExpr *row_expr = NULL;
  if (OB_ISNULL(params_.allocator_)
      || OB_ISNULL(c_expr = (ObConstRawExpr *) params_.allocator_->alloc(sizeof(ObConstRawExpr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allcoate memory", K(ret));
  } else if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_OP_ROW, row_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(row_expr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allcoate memory", K(ret));
  } else {
    c_expr = new(c_expr) ObConstRawExpr();
    maxvalue_expr = c_expr;
    maxvalue_expr->set_data_type(common::ObMaxType);
    if (OB_FAIL(row_expr->add_param_expr(maxvalue_expr))) {
      LOG_WARN("failed add param expr", K(ret));
    }
  }
  ObDDLStmt::array_t list_values_exprs;
  ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(partition.set_part_name("P_DEFAULT"))) {
    LOG_WARN("set partition name failed", K(ret));
  } else if (OB_FAIL(table_schema.add_partition(partition))) {
    LOG_WARN("add partition failed", K(ret));
  } else if (OB_FALSE_IT(table_schema.set_part_num(1))) {
  } else if (OB_FAIL(list_values_exprs.push_back(row_expr))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(create_table_stmt->get_part_values_exprs().assign(list_values_exprs))) {
    LOG_WARN("failed to assign list values exprs", K(ret));
  }

  return ret;
}

int ObCreateTableResolver::check_external_table_generated_partition_column_sanity(ObTableSchema &table_schema,
                                                                       ObRawExpr *dependant_expr,
                                                                       ObIArray<int64_t> &external_part_idx)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> col_exprs;
  if (OB_ISNULL(dependant_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dependant expr is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(dependant_expr, col_exprs, true/*extract pseudo column*/))) {
    LOG_WARN("extract col exprs failed", K(ret));
  } else if (table_schema.is_user_specified_partition_for_external_table()) {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_exprs.count(); i++) {
      if (OB_ISNULL(col_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col expr is null", K(ret));
      } else if (col_exprs.at(i)->is_pseudo_column_expr()) {
        if (col_exprs.at(i)->get_expr_type() != T_PSEUDO_PARTITION_LIST_COL ||
            dependant_expr != col_exprs.at(i)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "user specified partition dependant expr is not metadata$external_partition pseudo column");
          LOG_WARN("user specified partition col expr contains non external partition pseudo column is not supported", K(ret));
        } else if (ObOptimizerUtil::find_item(external_part_idx, col_exprs.at(i)->get_extra())) {
          ObSqlString tmp;
          OZ (tmp.append(col_exprs.at(i)->get_expr_name()));
          OZ (tmp.append(" redefinition"));
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, to_cstring(tmp.string()));
          LOG_WARN("redefine the metadata$external_partition[i] column", K(ret));
        } else {
          OZ (external_part_idx.push_back(col_exprs.at(i)->get_extra()));
          found = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!found) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "user specified partition col expr contains no metadata$external_partition pseudo columns");
        LOG_WARN("user specified partition col expr contains no external partition pseudo column is not supported", K(ret));
      }
    }
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_exprs.count(); i++) {
      if (OB_ISNULL(col_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col expr is null", K(ret));
      } else if (col_exprs.at(i)->is_pseudo_column_expr()) {
        if (col_exprs.at(i)->get_expr_type() != T_PSEUDO_EXTERNAL_FILE_URL) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition col expr contains non metadata$fileurl pseudo column");
          LOG_WARN("partition col expr contains non metadata$fileurl pseudo column is not supported", K(ret));
        } else {
          found = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!found) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition col expr contains no metadata$fileurl pseudo columns");
        LOG_WARN("partition col expr contains no metadata$fileurln pseudo column is not supported", K(ret));
      }
    }
  }

  return ret;
}

int ObCreateTableResolver::check_generated_partition_column(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionKeyInfo &part_key_info = table_schema.get_partition_key_info();
  const ObPartitionKeyColumn *part_column = NULL;
  ObColumnSchemaV2 *column_schema = NULL;
  ObRawExpr *dependant_expr = NULL;
  ObString expr_def;
  ObArray<int64_t> external_part_idx;
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
      } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(NULL,
                                                                     expr_def,
                                                                     *params_.expr_factory_,
                                                                     *params_.session_info_,
                                                                     table_schema,
                                                                     dependant_expr,
                                                                     schema_checker_))) {
        LOG_WARN("build generated column expr failed", K(ret));
      } else if (table_schema.is_external_table()) {
        if (table_schema.is_user_specified_partition_for_external_table()
            && mocked_external_table_column_ids_.has_member(column_schema->get_column_id())) {
          ObSqlString temp_str;
          ObString new_gen_def;
          OZ (temp_str.assign_fmt("%s%ld", N_PARTITION_LIST_COL, idx + 1));
          CK (OB_NOT_NULL(allocator_));
          OZ (ob_write_string(*allocator_, temp_str.string(), new_gen_def));
          OX (column_schema->get_cur_default_value().set_varchar(new_gen_def));
          OZ (external_part_idx.push_back(idx + 1));
        } else {
          OZ (check_external_table_generated_partition_column_sanity(table_schema, dependant_expr, external_part_idx));
        }
      } /*
        if gc column is partition key, then this is no restriction
        else {
        //check Expr Function for generated column whether allowed.
        ObRawExprPartFuncChecker part_func_checker(true);
        if (OB_FAIL(gen_col_expr->preorder_accept(part_func_checker))) {
          LOG_WARN("check partition function failed", K(ret));
        }
      }*/
    } else { }//do nothing
  }

  //to check external partition valid
  if (OB_FAIL(ret)) {
  } else if (table_schema.is_external_table()) {
    if (table_schema.is_user_specified_partition_for_external_table()) {
      if (external_part_idx.count() != part_key_info.get_size()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition key number not equal to generate column with metadata$partition_list_col");
        LOG_WARN("partition key number not equal to generate column with metadata$partition_list_col is not supported", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < external_part_idx.count(); i++) {
        if (!ObOptimizerUtil::find_item(external_part_idx, i + 1)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "metadata$partition_list_col id is not continuously from 1");
          LOG_WARN("metadata$partition_list_col id is not continuously from 1 is not supported", K(ret));
        }
      }
    }
    ObArray<uint64_t> column_ids;
    ObArray<uint64_t> part_column_ids;
    OZ (table_schema.get_column_ids(column_ids));
    if (OB_SUCC(ret) && part_key_info.get_size() != 0) {
      OZ (part_key_info.get_column_ids(part_column_ids));
    }
    OZ (ObOptimizerUtil::remove_item(column_ids, part_column_ids));
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
      ObColumnSchemaV2 *column_schema = NULL;
      if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to get column schema", K(ret), K(column_ids.at(i)));
      } else if (column_schema->is_generated_column()) {
        ObArray<ObRawExpr *> col_exprs;
        if (OB_FAIL(column_schema->get_cur_default_value().get_string(expr_def))) {
          LOG_WARN("get string from current default value failed", K(ret), K(column_schema->get_cur_default_value()));
        } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(NULL,
                                                                      expr_def,
                                                                      *params_.expr_factory_,
                                                                      *params_.session_info_,
                                                                      table_schema,
                                                                      dependant_expr,
                                                                      schema_checker_))) {
          LOG_WARN("build generated column expr failed", K(ret));
        } else if (OB_ISNULL(dependant_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dependant expr is null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(dependant_expr, col_exprs, true/*extract pseudo column*/))) {
          LOG_WARN("extract col exprs failed", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < col_exprs.count(); j++) {
            if (col_exprs.at(j)->get_expr_type() == T_PSEUDO_PARTITION_LIST_COL) {
              ObSqlString tmp;
              OZ (tmp.append(col_exprs.at(j)->get_expr_name()));
              OZ (tmp.append(" defined in not partition column"));
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, to_cstring(tmp.string()));
              LOG_WARN("metadata$external_partition[i] column defined in normal column", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::check_column_name_duplicate(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node is null.", K(ret));
  } else if (OB_ISNULL(stmt_)
      || T_TABLE_ELEMENT_LIST != node->type_
      || OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node->type_), K(node->num_child_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ParseNode *element = node->children_[i];
      if (OB_ISNULL(element)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("element node is null", K(ret));
      } else if (OB_LIKELY(T_COLUMN_DEFINITION == element->type_)) {
        if (OB_USER_ROW_MAX_COLUMNS_COUNT < column_name_set_.count()) {
          ret = OB_ERR_TOO_MANY_COLUMNS;
        } else if (element->num_child_ < COLUMN_DEFINITION_NUM_CHILD ||
            OB_ISNULL(element->children_) || OB_ISNULL(element->children_[COLUMN_REF_NODE]) ||
            T_COLUMN_REF != element->children_[COLUMN_REF_NODE]->type_ ||
            COLUMN_DEF_NUM_CHILD != element->children_[COLUMN_REF_NODE]->num_child_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid parse node", K(ret));
        } else {
          ParseNode *name_node = element->children_[COLUMN_REF_NODE]->children_[COLUMN_NAME_NODE];
          if (OB_ISNULL(name_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("name node can not be null", K(ret));
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
              if (OB_HASH_EXIST  == column_name_set_.exist_refactored(column_name_key)) {
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

int ObCreateTableResolver::resolve_primary_key_node(const ParseNode &pk_node,
                                                    ObArray<ObColumnResolveStat> &stats)
{
  int ret = OB_SUCCESS;

  const bool is_mysql_mode = !is_oracle_mode();
  if ((is_mysql_mode ? 3 < pk_node.num_child_ : 2 < pk_node.num_child_ )
      || OB_ISNULL(pk_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the num_child of primary_node is wrong.",
                 K(ret), K(pk_node.num_child_), K(pk_node.children_));
  } else if (is_external_table_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Primary key constraint on external table");
  } else {
    ParseNode *column_list_node = pk_node.children_[0];
    if (OB_ISNULL(column_list_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "column_list_node is null.", K(ret));
    } else if (T_COLUMN_LIST != column_list_node->type_
               || column_list_node->num_child_ <= 0
               || OB_ISNULL(column_list_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "unexpected.",
                   K(ret), K(column_list_node->type_), K(column_list_node->num_child_));
    } else {
      ParseNode *key_node = NULL;
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
            key_name.assign_ptr(key_node->str_value_,static_cast<int32_t>(key_node->str_len_));
            if (OB_FAIL(add_primary_key_part(key_name, stats, pk_data_length))) {
              SQL_RESV_LOG(WARN, "add primary key part failed", K(ret), K(key_name));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && is_mysql_mode && NULL != pk_node.children_[1]) {
      ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
      if (T_USING_HASH == pk_node.children_[1]->type_) {
        create_table_stmt->set_index_using_type(share::schema::USING_HASH);
      } else {
        create_table_stmt->set_index_using_type(share::schema::USING_BTREE);
      }
    }
    if (OB_SUCC(ret) && is_mysql_mode) {
      if (NULL != pk_node.children_[2]) {
        ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
        ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
        ObString pk_comment;
        pk_comment.assign_ptr(pk_node.children_[2]->str_value_,static_cast<int32_t>(pk_node.children_[2]->str_len_));
        if (OB_FAIL(table_schema.set_pk_comment(pk_comment))) {
          LOG_WARN("fail to set primary key comment", K(pk_comment), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::get_resolve_stats_from_table_schema(
    const ObTableSchema &table_schema,
    ObArray<ObColumnResolveStat> &stats)
{
  int ret = OB_SUCCESS;
  ObColumnResolveStat stat;
  ObTableSchema::const_column_iterator it_begin = table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = table_schema.column_end();
  for(; OB_SUCC(ret) && it_begin != it_end; it_begin++) {
    if (OB_ISNULL(it_begin)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("it_begin should not be NULL", K(ret));
    } else if (OB_ISNULL(*it_begin)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("*it_begin should not be NULL", K(ret));
    } else {
      const ObColumnSchemaV2 &column_schema = **it_begin;
      stat.reset();
      stat.column_id_ = column_schema.get_column_id();
      if (OB_FAIL(stats.push_back(stat))) {
        SQL_RESV_LOG(WARN, "fail to push back stat", K(ret));
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::resolve_table_elements(const ParseNode *node,
                                                  ObArray<int> &index_node_position_list,
                                                  ObArray<int> &foreign_key_node_position_list,
                                                  ObArray<int> &table_level_constraint_list,
                                                  const int resolve_rule) //查询建表会调用两次, 第一次仅解析列
                                                                          //第二次解析非列
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = lib::is_oracle_mode();
  const uint64_t tenant_id = session_info_->get_effective_tenant_id();
  if (OB_ISNULL(node)) {
   // do nothing, create table t as select ... will come here
  } else if (OB_ISNULL(stmt_)
             || OB_ISNULL(session_info_)
             || T_TABLE_ELEMENT_LIST != node->type_
             || node->num_child_ < 1
             || OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument",
                 K(ret), K(node->type_), K(node->num_child_));
  } else if (static_cast<int64_t>(table_id_) > 0
             && OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
             session_info_->get_effective_tenant_id(), table_id_, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K_(table_id));
  } else {
    ObSEArray<ObString, 4> gen_col_expr_arr;
    ParseNode *primary_node = NULL;
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    uint64_t autoinc_column_id = 0;
    int64_t first_timestamp_index = -1;
    int64_t row_data_length = 0;
    ObArray<ObColumnResolveStat> stats;
    ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    table_schema.set_tenant_id(tenant_id);
    bool has_visible_col = false;
    // 将经过 resolve_column_definition 后的 column schema 存放在 resolved_cols 中
    // 为了支持生成列按照任意顺序定义，在生成全部 column_schema 之后，再按照统一存放到 table_schema 中
    ObSEArray<ObColumnSchemaV2, SEARRAY_INIT_NUM> resolved_cols;
    //列需要根据租户id区分大小写,这里先把租户id设置进table_schema
    if (RESOLVE_NON_COL != resolve_rule) {
      if (OB_FAIL(add_new_column_for_oracle_temp_table(table_schema, stats))) {
        SQL_RESV_LOG(WARN, "add new column for oracle temp table failed", K(ret));
      }
    }
    //RESOLVE_NON_COL需要将查询中的列添加到stats中以便解析PK约束等信息
    if (OB_FAIL(ret)) {
      //do nothing ...
    } else if (RESOLVE_NON_COL == resolve_rule) {
      if (OB_FAIL(get_resolve_stats_from_table_schema(table_schema, stats))) {
        LOG_WARN("failed to generate ObColumnResolveStat array", K(ret));
      }
    } else if (OB_FAIL(check_column_name_duplicate(node))) {
      LOG_WARN("check_column_name_duplicate fail", K(ret));
    }

    // 为了实现以任意顺序定义生成列，需要遍历两次 node
    // 第一次遍历，解析所有列的列名，生成 column_schema 并存储在 resolved_cols
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ParseNode *element = node->children_[i];
      CK (OB_NOT_NULL(element));
      if (OB_FAIL(ret)) {
      } else if ((RESOLVE_NON_COL == resolve_rule && T_COLUMN_DEFINITION == element->type_)
                 || (RESOLVE_COL_ONLY == resolve_rule && T_COLUMN_DEFINITION != element->type_)) {
        //continue
      } else if (OB_LIKELY(T_COLUMN_DEFINITION == element->type_)) {
        ObColumnSchemaV2 column;
        column.set_tenant_id(tenant_id);
        ObColumnResolveStat stat;
        common::ObString pk_name;
        if (OB_INVALID_ID == column.get_column_id()) {
          column.set_column_id(gen_column_id());
        } else {
          // column id set by user sql such as "c1 int comment 'manual' id 17"
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(resolve_column_name(column, element))) {
          SQL_RESV_LOG(WARN, "resolve column name failed", K(ret));
        } else {
          OZ (resolved_cols.push_back(column));
        }
      }
    }

    int64_t resolved_cols_count = resolved_cols.count();
    // 第二遍遍历，利用 resolved_cols 解析出全部 column schema 并保存到 table_schema 中
    // 并解析 index 等
    for (int32_t i = 0, ele_pos = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ParseNode *element = node->children_[i];
      CK (OB_NOT_NULL(element));
      if (OB_FAIL(ret)) {
      } else if ((RESOLVE_NON_COL == resolve_rule && T_COLUMN_DEFINITION == element->type_)
                 || (RESOLVE_COL_ONLY == resolve_rule && T_COLUMN_DEFINITION != element->type_)) {
        continue;
      } else if (OB_LIKELY(T_COLUMN_DEFINITION == element->type_)) {
        bool is_modify_column_visibility = false;
        const bool is_create_table_as = (RESOLVE_COL_ONLY == resolve_rule);
        CK (ele_pos >= 0 && ele_pos < resolved_cols_count);
        if (OB_SUCC(ret)) {
          ObColumnSchemaV2 &column = resolved_cols.at(ele_pos);
          ObColumnResolveStat stat;
          common::ObString pk_name;
          // ele_pos + 1, 指向下一个 column_schema
          ++ele_pos;
          ObString tmp_str[ObNLSFormatEnum::NLS_MAX];
          tmp_str[ObNLSFormatEnum::NLS_DATE] = session_info_->get_local_nls_date_format();
          tmp_str[ObNLSFormatEnum::NLS_TIMESTAMP] = session_info_->get_local_nls_timestamp_format();
          tmp_str[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = session_info_->get_local_nls_timestamp_tz_format();
          if (OB_FAIL(resolve_column_definition(column, element, stat,
                                                is_modify_column_visibility,
                                                pk_name,
                                                table_schema,
                                                is_oracle_temp_table_,
                                                is_create_table_as))) {
            SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
          } else if (!column.is_udt_related_column(lib::is_oracle_mode()) && // udt column will check after hidden column generated
                     OB_FAIL(check_default_value(column.get_cur_default_value(),
                                          session_info_->get_tz_info_wrap(),
                                          tmp_str,
                                          NULL,
                                          *allocator_,
                                          table_schema,
                                          resolved_cols,
                                          column,
                                          gen_col_expr_arr,
                                          session_info_->get_sql_mode(),
                                          session_info_,
                                          true, /* allow_sequence */
                                          schema_checker_,
                                          NULL == element->children_[1]))) {
            SQL_RESV_LOG(WARN, "failed to cast default value!", K(ret));
          } else if (column.is_string_type() || is_lob_storage(column.get_data_type())) {
            int64_t length = 0;
            if (OB_FAIL(column.get_byte_length(length, is_oracle_mode, false))) {
              SQL_RESV_LOG(WARN, "fail to get byte length of column", KR(ret), K(is_oracle_mode));
            } else if (ob_is_string_tc(column.get_data_type()) && length > OB_MAX_VARCHAR_LENGTH) {
              ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, column.get_column_name(), static_cast<int32_t>(OB_MAX_VARCHAR_LENGTH));
            } else if (is_lob_storage(column.get_data_type())) {
              ObLength max_length = 0;
              max_length = ObAccuracy::MAX_ACCURACY2[is_oracle_mode][column.get_data_type()].get_length();
              if (length > max_length) {
                ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
                LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, column.get_column_name(),
                    ObAccuracy::MAX_ACCURACY2[is_oracle_mode][column.get_data_type()].get_length());
              } else {
                // table lob inrow theshold has not been parsed, so use handle length check
                // will recheck after parsing table lob inrow theshold
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
          if (OB_SUCC(ret)){
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
                if (OB_FAIL(column.get_byte_length(length, is_oracle_mode, false))){
                  SQL_RESV_LOG(WARN, "fail to get byte length of column", KR(ret), K(is_oracle_mode));
                } else if (pk_data_length += length > OB_MAX_VARCHAR_LENGTH_KEY) {
                  ret = OB_ERR_TOO_LONG_KEY_LENGTH;
                  LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_VARCHAR_LENGTH_KEY);
                } else if (length <= 0) {
                  ret = OB_ERR_WRONG_KEY_COLUMN;
                  LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name());
                } else {
                  // do nothing
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(primary_keys_.push_back(column.get_column_id()))) {
                  SQL_RESV_LOG(WARN, "add primary key failed");
                } else {
                  column.set_rowkey_position(get_primary_key_size());
                  // 判断是否是 oracle 模式, oracle 模式下才需要给主键一个约束名
                  if (session_info_->is_oracle_compatible()) {
                    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
                    ObSEArray<ObConstraint, 4> &csts = create_table_stmt->get_create_table_arg().constraint_list_;
                    // 加 pk 到 cst_list 里面
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
              //consider column with unique_key as a special index node,
              //then resolve it in resolve_index_node()
              if (OB_MAX_INDEX_PER_TABLE == index_node_position_list.count()) {
                ret = OB_ERR_TOO_MANY_KEYS;
                LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
              } else if (OB_FAIL(index_node_position_list.push_back(i))){
                SQL_RESV_LOG(WARN, "add index node failed", K(ret));
              } else { /*do nothing*/ }
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
              if (OB_FAIL(ObResolverUtils::resolve_timestamp_node(stat.is_set_null_, stat.is_set_default_value_,
                                                                  is_first_timestamp, session_info_, column))) {
                SQL_RESV_LOG(WARN, "fail to resolve timestamp node", K(ret), K(column));
              }
            }
          }

          if (OB_SUCC(ret) && column.is_xmltype()) {
            column.set_udt_set_id(gen_udt_set_id());
          }

          if (OB_SUCC(ret)) {
            ObColumnSchemaV2 *tmp_col = NULL;
            LOG_DEBUG("resolve table elements mid2", K(i), K(column));
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

          if (OB_SUCC(ret) && lib::is_oracle_mode()) {
            if (!column.is_invisible_column()) {
              has_visible_col = true;
            }
            // column from resolved_cols may be invalid
            if (OB_FAIL(add_udt_hidden_column(table_schema, resolved_cols, column))) {
              LOG_WARN("generate hidden column for udt failed");
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(stats.push_back(stat))) {
              SQL_RESV_LOG(WARN, "fail to push back stat", K(ret));
            }
          }
        }
      } else if (T_PRIMARY_KEY == element->type_) {
        if (NULL == primary_node) {
          primary_node = element;
          if (session_info_->is_oracle_compatible()) {
            ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
            ObSEArray<ObConstraint, 4> &csts =
                create_table_stmt->get_create_table_arg().constraint_list_;
            ObString pk_name;
            if (OB_FAIL(resolve_pk_constraint_node(*element, pk_name, csts))) {
              SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
            }
          }
        } else {
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
          SQL_RESV_LOG(WARN, "multiple primary key defined");
        }
      } else if(ObItemType::T_INDEX == element->type_) {
        if (OB_MAX_INDEX_PER_TABLE == index_node_position_list.count()) {
          ret = OB_ERR_TOO_MANY_KEYS;
          LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
        } else if (OB_FAIL(index_node_position_list.push_back(i))){
          SQL_RESV_LOG(WARN, "add index node failed", K(ret));
        } else { /*do nothing*/ }
      } else if (T_FOREIGN_KEY == element->type_) {
        // FIXME: 外键最大数量限制和 index 数量一样
        if (OB_MAX_INDEX_PER_TABLE == foreign_key_node_position_list.count()) {
          ret = OB_ERR_TOO_MANY_KEYS;
          LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
        } else if (OB_FAIL(foreign_key_node_position_list.push_back(i))){
          SQL_RESV_LOG(WARN, "add foreign key node failed", K(ret));
        } else { /*do nothing*/ }
      } else if (T_CHECK_CONSTRAINT == element->type_) {
        if (lib::is_mysql_mode()) {
          // TODO:@xiaofeng.lby : do we also need to deal with constraints like this in oracle mode
          if (OB_FAIL(table_level_constraint_list.push_back(i))) {
            SQL_RESV_LOG(WARN, "add check constraint node failed", K(ret));
          }
        } else { // oracle mode
          ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
          ObSEArray<ObConstraint, 4>& csts = create_table_stmt->get_create_table_arg().constraint_list_;
          if (OB_FAIL(resolve_check_constraint_node(*element, csts))) {
            SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
          }
        }
      } else if (T_EMPTY == element->type_) {
        // compatible with mysql 5.7 check (expr), do nothing
      } else {
        // won't be here
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "unexpected branch", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      for (int32_t i = resolved_cols_count; i < resolved_cols.count() && OB_SUCC(ret); i++) {
        ObColumnSchemaV2 &hidden_col = resolved_cols.at(i);
        if (OB_FAIL(table_schema.add_column(hidden_col))) {
          SQL_RESV_LOG(WARN, "add udt hidden column to table_schema failed", K(ret), K(hidden_col));
        } else {
          ObColumnNameHashWrapper name_key(hidden_col.get_column_name_str());
          if (OB_FAIL(column_name_set_.set_refactored(name_key))) {
            SQL_RESV_LOG(WARN, "add column name to map failed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t identity_column_count = 0;
      if (OB_FAIL(get_identity_column_count(table_schema, identity_column_count))) {
        SQL_RESV_LOG(WARN, "get identity column count fail", K(ret));
      } else if (identity_column_count > 1) {
        ret = OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT;
        SQL_RESV_LOG(WARN, "each table can only have an identity column", K(ret));
      }
    }
    // oracle 模式下，一个表中至少有一个列为 visible 列
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      // RESOLVE_NON_COL == resolve_rule 时，只解析非列定义
      if (RESOLVE_NON_COL != resolve_rule && !has_visible_col) {
        ret = OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE;
        SQL_RESV_LOG(WARN, "table must have at least one column that is not invisible", K(ret));
      }
    }

    // Oracle 模式下，一个表至少有一个列为非 generated 列
    if (OB_SUCC(ret) && lib::is_oracle_mode() && !table_schema.is_external_table()) {
      bool has_non_virtual_column = false;
      for (int64_t i = 0;
           OB_SUCC(ret) && !has_non_virtual_column && i < table_schema.get_column_count();
           ++i) {
        const ObColumnSchemaV2 *column = table_schema.get_column_schema_by_idx(i);
        CK (OB_NOT_NULL(column));
        if (OB_SUCC(ret) && !column->is_generated_column()) {
          has_non_virtual_column = true;
        }
      }
      if (OB_SUCC(ret) && !has_non_virtual_column) {
        ret = OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL;
        SQL_RESV_LOG(WARN, "table must have at least one column that is not virtual", K(ret));
      }
    }

    if (OB_SUCC(ret) && table_schema.is_external_table()) {
      bool all_virtual_column = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
        const ObColumnSchemaV2 *column = table_schema.get_column_schema_by_idx(i);
        CK (OB_NOT_NULL(column));
        if (OB_SUCC(ret) && !column->is_generated_column()) {
          all_virtual_column = false;
          break;
        }
      }
      if (OB_SUCC(ret) && !all_virtual_column) {
        ret = OB_NOT_SUPPORTED; //[TODO EXTERNAL-TABLE]
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "normal columns");
      }
    }

    // MySQL 模式下，一个表至少有一个非 hidden 列
    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      bool has_non_hidden_column = false;
      for (int64_t i = 0;
           OB_SUCC(ret) && !has_non_hidden_column && i < table_schema.get_column_count();
           ++i) {
        const ObColumnSchemaV2 *column = table_schema.get_column_schema_by_idx(i);
        CK (OB_NOT_NULL(column));
        if (OB_SUCC(ret) && !column->is_hidden()) {
          has_non_hidden_column = true;
        }
      }
      if (OB_SUCC(ret) && !has_non_hidden_column) {
        ret = OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL;
        SQL_RESV_LOG(WARN, "table must have at least one column that is not hidden", K(ret));
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
        table_schema.set_max_used_column_id(cur_column_id_);
        if (0 != autoinc_column_id) {
          table_schema.set_autoinc_column_id(autoinc_column_id);
        }
      }
    }
    LOG_DEBUG("resolve table elements end ", K(resolve_rule), K(table_schema));
  }
  return ret;
}

int ObCreateTableResolver::set_nullable_for_cta_column(ObSelectStmt *select_stmt,
                                                       ObColumnSchemaV2& column,
                                                       const ObRawExpr *expr,
                                                       const ObString &table_name,
                                                       ObIAllocator &allocator,
                                                       ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  bool is_not_null = false;
  if (OB_ISNULL(expr) || OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null of expr and select stmt.", K(ret));
  } else if (lib::is_mysql_mode()) {
    // scope set to FROM since it will not go deduce process with context,
    // such as null reject in where condition and having condition.
    // if is_not_null true, it will pass into full scope checking at next step.
    ObNotNullContext ctx(NULL, NULL, select_stmt);
    if (OB_FAIL(ctx.generate_stmt_context(NULLABLE_SCOPE::NS_FROM))) {
      LOG_WARN("failed to generate stmt context", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx,
                                                          const_cast<ObRawExpr *>(expr),
                                                          is_not_null,
                                                          NULL))) {
      LOG_WARN("failed to check expr not null", K(ret));
    }
  } else if (expr->is_column_ref_expr()) {
    is_not_null = expr->get_result_type().has_result_flag(HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG);
  }
  if (OB_SUCC(ret) && is_not_null) {
    // deduce pre-condition: already not null
    // oracle:
    // 1. only deduce column not null, not for composed exprs(e.g, c1+c2) and const expr
    // 2. column not null depends on HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG, e.g, ctas from pk (NULL: YES)
    // mysql:
    // 1. supports composed expr not null deduce
    // 2. column not null depends on NOT_NULL_FLAG, e.g, ctas from pk (NULL: NO)
    ObNotNullContext ctx(NULL, NULL, select_stmt, lib::is_oracle_mode());
    if (OB_FAIL(ctx.generate_stmt_context(NULLABLE_SCOPE::NS_TOP))) {
      LOG_WARN("failed to generate stmt context", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx,
                                                          const_cast<ObRawExpr *>(expr),
                                                          is_not_null,
                                                          NULL))) {
      LOG_WARN("failed to check expr not null", K(ret));
    }
  }
  LOG_DEBUG("set nullable_for_cta_column", K(is_not_null), K(column));
  if (OB_SUCC(ret)) {
    if (is_oracle_mode()) {
      if (is_not_null) {
        if (OB_FAIL(add_default_not_null_constraint(column, table_name, allocator, stmt))) {
          LOG_WARN("add default not null constraint failed", K(ret));
        }
      } else {
        column.set_nullable(!is_not_null);
      }
    } else { //mysql mode
      if (expr->is_win_func_expr()) {//compatible with mysql
        const ObWinFunRawExpr *win_expr = reinterpret_cast<const ObWinFunRawExpr*>(expr);
        if (T_WIN_FUN_RANK == win_expr->get_func_type() ||
            T_WIN_FUN_DENSE_RANK == win_expr->get_func_type() ||
            T_WIN_FUN_ROW_NUMBER == win_expr->get_func_type()) {
          ObObj temp_default;
          temp_default.set_uint64(0);
          column.set_cur_default_value(temp_default);
        } else if (T_WIN_FUN_CUME_DIST == win_expr->get_func_type() ||
                   T_WIN_FUN_PERCENT_RANK == win_expr->get_func_type()) {
          ObObj temp_default;
          temp_default.set_double(0);
          column.set_cur_default_value(temp_default);
        } else {}
      } else {}
      column.set_nullable(!is_not_null);
    }
  }
  return ret;
}
int ObCreateTableResolver::resolve_insert_mode(const ParseNode *parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *flag_node = NULL;
  ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt *>(stmt_);
  ObExecContext *exec_ctx = NULL;
  ObSqlCtx *sql_ctx = NULL;
  bool is_support = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0;
  if (OB_ISNULL(parse_tree) ||
      OB_ISNULL(create_table_stmt) ||
      OB_ISNULL(params_.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error",K(parse_tree), K(create_table_stmt), K(session_info_), K(exec_ctx), K(sql_ctx), K(ret));
  } else if (lib::is_oracle_mode() || !is_support) {
    create_table_stmt->set_insert_mode(0);
  } else if (parse_tree->num_child_ != CREATE_TABLE_AS_SEL_NUM_CHILD){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child_num",K(parse_tree->num_child_), K(ret));
  } else {
    flag_node = parse_tree->children_[10];
    if (flag_node == NULL) {
      create_table_stmt->set_insert_mode(0);
    } else if (flag_node->type_ == T_IGNORE) {
      create_table_stmt->set_insert_mode(1);
    } else if (flag_node->type_ == T_REPLACE) {
      create_table_stmt->set_insert_mode(2);
    }
  }
  return ret;
}

//解析column_list和查询, 然后根据建表语句中的opt_column_list(可能无)和查询, 设置新表的列名和数据类型
int ObCreateTableResolver::resolve_table_elements_from_select(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt *>(stmt_);
  const ObTableSchema *base_table_schema = NULL;
  ParseNode *sub_sel_node = parse_tree.children_[8];
  ObSelectStmt *select_stmt = NULL;
  ObSelectResolver select_resolver(params_);
  select_resolver.params_.is_from_create_table_ = true;
  select_resolver.params_.is_specified_col_name_ = parse_tree.num_child_ > 3 &&
                                                   parse_tree.children_[3] != NULL &&
                                                   T_TABLE_ELEMENT_LIST == parse_tree.children_[3]->type_;
  //select层不应该看到上层的insert stmt的属性，所以upper scope stmt应该为空
  select_resolver.set_parent_namespace_resolver(NULL);
  if (lib::is_mysql_mode()
        && OB_NOT_NULL(params_.query_ctx_)
        && 0 != params_.query_ctx_->question_marks_count_
        && !params_.is_prepare_protocol_) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("not support questionmark in normal create.", K(ret));
  } else if (OB_UNLIKELY(parse_tree.num_child_ <= 3 ||
                         (parse_tree.children_[3] != NULL &&
                          T_TABLE_ELEMENT_LIST != parse_tree.children_[3]->type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_) || OB_ISNULL(params_.param_list_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "ObCreateTableResolver is not init", K(params_.param_list_), K(allocator_),
                                                            K(session_info_), K(ret));
  } else if (OB_ISNULL(sub_sel_node) || OB_UNLIKELY(T_SELECT != sub_sel_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid select node", K(sub_sel_node));
  } else if (OB_FAIL(set_table_name(table_name_))) {
      LOG_WARN("failed to set table name", K(ret));
  } else if (OB_FAIL(select_resolver.resolve(*sub_sel_node))) {
    LOG_WARN("failed to resolve select stmt in creat table stmt", K(ret));
  } else {
    select_stmt = select_resolver.get_select_stmt();
    ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    table_schema.set_tenant_id(session_info_->get_effective_tenant_id());
    LOG_DEBUG("resolve table select item begin", K(table_schema));
    if (OB_ISNULL(select_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid select stmt", K(select_stmt));
    } else if (OB_FAIL(params_.query_ctx_->query_hint_.init_query_hint(allocator_,
                                                                       session_info_,
                                                                       select_stmt))) {
      LOG_WARN("failed to init query hint.", K(ret));
    } else {
      ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
      ObColumnSchemaV2 column;
      create_table_stmt->set_sub_select(select_stmt);
      //检查查询项之间有无重名, 有则报错; 查询项和表定义中列名是可以重名的;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
        const SelectItem &cur_item = select_items.at(i);
        const ObString *cur_name = NULL;
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
          const SelectItem &pre_item = select_items.at(j);
          const ObString *prev_name = NULL;
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
      // oracle模式下,create table (column_names) as select expr, 如果没有显示定义column_names,
      // 则要求每一个expr必须显示指定别名或者本身是列
      if (lib::is_oracle_mode() && create_table_column_count <= 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); i++) {
          const SelectItem &select_item = select_items.at(i);
          if (OB_ISNULL(select_item.expr_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid null expr in select_item", K(ret), K(select_item.expr_));
          } else if (select_item.is_real_alias_ || T_REF_COLUMN == select_item.expr_->get_expr_type()) {
            // do nothing
          } else if (select_item.expr_->get_expr_type() == T_FUN_SYS_SEQ_NEXTVAL) {
            // do nothing
          } else {
            ret = OB_NO_COLUMN_ALIAS;
            LOG_USER_ERROR(OB_NO_COLUMN_ALIAS, select_item.expr_name_.length(),
                        select_item.expr_name_.ptr());
          }
        }
      }
      // oracle临时表会在subquery的基础上默认添加两列隐藏列
      const int64_t hidden_column_num = is_oracle_temp_table_ ? 2 : 0;
      if (OB_SUCC(ret) && lib::is_oracle_mode()) {
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
        const SelectItem &select_item = select_items.at(i);
        ObRawExpr *expr = select_item.expr_;
        ObColumnRefRawExpr *new_col_ref = static_cast<ObColumnRefRawExpr *>(expr);
        TableItem *new_table_item = select_stmt->get_table_item_by_id(new_col_ref->get_table_id());
        if (OB_UNLIKELY(NULL == expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select item expr is null", K(ret), K(i));
        } else {
          column.reset();
          if (!select_item.alias_name_.empty()) {
            OZ(column.set_column_name(select_item.alias_name_));
          } else {
            OZ(column.set_column_name(select_item.expr_name_));
          }
          if (OB_SUCC(ret) && is_mysql_mode()) {
            if (new_table_item != NULL && new_table_item->is_basic_table()) {
              if (base_table_schema == NULL &&
                  OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                            new_table_item->ref_id_, base_table_schema))) {
                LOG_WARN("get table schema failed", K(ret));
              } else if (OB_ISNULL(base_table_schema)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("NULL table schema", K(ret));
              } else {
                const ObColumnSchemaV2 *org_column = base_table_schema->get_column_schema(select_item.expr_name_);
                if (NULL != org_column &&
                    !org_column->is_generated_column() &&
                    !org_column->get_cur_default_value().is_null()) {
                    column.set_cur_default_value(org_column->get_cur_default_value());
                  }
              }
            } else if (new_table_item == NULL &&
                       (ObRawExpr::EXPR_CONST == expr->get_expr_class() ||
                        (ObRawExpr::EXPR_OPERATOR == expr->get_expr_class() &&
                         expr->is_static_const_expr())) &&
                        !expr->get_result_type().is_null()) {
              common::ObObjType result_type = expr->get_result_type().get_obj_meta().get_type();
              if (ob_is_numeric_type(result_type) || ob_is_string_tc(result_type) || ob_is_time_tc(result_type)) {
                common::ObObj zero_obj(0);
                if (OB_FAIL(column.set_cur_default_value(zero_obj))) {
                  LOG_WARN("set default value failed", K(ret));
                }
              }
            } else { /*do nothing*/ }
          }
          if (OB_SUCC(ret) && ObResolverUtils::is_restore_user(*session_info_)
              && ObCharset::case_insensitive_equal(column.get_column_name_str(), OB_HIDDEN_PK_INCREMENT_COLUMN_NAME)) {
            continue;
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (expr->get_result_type().is_null()) { //bug16503918, NULL需要替换为binary(0)
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
          } else if (lib::is_oracle_mode() && ObTinyIntType == expr->get_result_type().get_type()) {
            //can not create a column which meta type is tinyint in oracle mode
            ret = OB_ERR_INVALID_DATATYPE;
            LOG_USER_ERROR(OB_ERR_INVALID_DATATYPE);
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
            column.set_zero_fill(expr->get_result_flag() & ZEROFILL_FLAG);
            OZ (adjust_number_decimal_column_accuracy_within_max(column, lib::is_oracle_mode()));
            if (OB_SUCC(ret) && lib::is_oracle_mode() && expr->get_result_type().is_user_defined_sql_type()) {
              // udt column is varbinary used for null bitmap
              column.set_collation_type(CS_TYPE_BINARY);
              column.set_udt_set_id(gen_udt_set_id());
              if (expr->get_result_type().get_subschema_id() == ObXMLSqlType) {
                column.set_sub_data_type(T_OBJ_XML);
              } else if (!ObObjUDTUtil::ob_is_supported_sql_udt(expr->get_result_type().get_udt_id())) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("unsupported udt type for sql udt",
                         K(ret), K(expr->get_result_type()), K(expr->get_result_type().get_udt_id()));
              } else {
                column.set_sub_data_type(expr->get_result_type().get_udt_id());
              }
            }
            if (OB_SUCC(ret) && lib::is_mysql_mode() && ob_is_geometry(expr->get_result_type().get_type())) {
              column.set_geo_type(static_cast<uint64_t>(expr->get_geo_expr_result_type()));
            }
            OZ (adjust_string_column_length_within_max(column, lib::is_oracle_mode()));
            LOG_DEBUG("column expr debug", K(*expr));
          }
          if (OB_FAIL(ret)) { // do nothing.
          //create_table_column_count > 0  means the format of ctas is "create table t(c1, c2,...) as select"
          // During the first step of resolving ctas, column schemas of (c1, c2, ...) are
          // generated and added into table_schema.
          } else if (is_oracle_mode() && create_table_column_count > 0) {
            LOG_DEBUG("ctas oracle mode, create_table_column_count > 0,begin", K(create_table_column_count), K(column));
            if (column.is_string_type()) {
              if (column.get_meta_type().is_lob()) {
                if (OB_FAIL(check_text_column_length_and_promote(column, table_id_, true))) {
                  LOG_WARN("fail to check text or blob column length", K(ret), K(column));
                }
              } else if (OB_FAIL(check_string_column_length(column, lib::is_oracle_mode(), params_.is_prepare_stage_))) {
                LOG_WARN("fail to check string column length", K(ret), K(column));
              }
            } else if (ObRawType == column.get_data_type()) {
              if (OB_FAIL(ObDDLResolver::check_raw_column_length(column))) {
                LOG_WARN("failed to check raw column length", K(ret), K(column));
              }
            }
            if (OB_SUCC(ret)) {
              ObColumnSchemaV2 *org_column =
                  table_schema.get_column_schema_by_idx(i + hidden_column_num);
              org_column->set_meta_type(column.get_meta_type());
              org_column->set_charset_type(column.get_charset_type());
              org_column->set_collation_type(column.get_collation_type());
              org_column->set_accuracy(column.get_accuracy());
              // nullable property of org_column instead of column should be set.
              if (OB_FAIL(set_nullable_for_cta_column(select_stmt,
                                                      *org_column,
                                                      expr,
                                                      table_name_,
                                                      *allocator_,
                                                      stmt_))) {
                LOG_WARN("failed to check and set nullable for cta.", K(ret));
              } else if (column.is_enum_or_set()) {
                if (OB_FAIL(org_column->set_extended_type_info(column.get_extended_type_info()))) {
                  LOG_WARN("set enum or set info failed", K(ret), K(*expr));
                }
              } else if (is_oracle_mode() && column.is_xmltype()) {
                org_column->set_sub_data_type(T_OBJ_XML);
                // udt column is varbinary used for null bitmap
                org_column->set_udt_set_id(gen_udt_set_id());
                if (OB_FAIL(add_udt_hidden_column(table_schema, *org_column))) {
                  LOG_WARN("add udt hidden column to table_schema failed", K(ret), K(column));
                }
              }
            }
            LOG_DEBUG("ctas oracle mode, create_table_column_count > 0,end", K(column));
          } else {
            LOG_DEBUG("ctas mysql mode, create_table_column_count = 0,begin", K(create_table_column_count), K(column));
            column.set_column_id(gen_column_id());
            ObColumnSchemaV2 *org_column = table_schema.get_column_schema(column.get_column_name());
            if (OB_NOT_NULL(org_column)) {
              //同名列存在, 为了和mysql保持一致, 需要调整原列的顺序
              ObColumnSchemaV2 new_column;
              if (OB_FAIL(new_column.assign(*org_column))) {
                LOG_WARN("fail to assign column", KR(ret), KPC(org_column));
              } else {
                new_column.set_column_id(gen_column_id());
                new_column.set_prev_column_id(UINT64_MAX);
                new_column.set_next_column_id(UINT64_MAX);
              }
              if (OB_FAIL(ret)) {
              } else if (1 == table_schema.get_column_count()) {
                //do nothing, 只有一列就不用调整了
                if (OB_FAIL(set_nullable_for_cta_column(select_stmt, *org_column, expr, table_name_, *allocator_, stmt_))) {
                  LOG_WARN("failed to check and set nullable for cta.", K(ret));
                }
              } else if (OB_FAIL(set_nullable_for_cta_column(select_stmt,
                                                            new_column,
                                                            expr,
                                                            table_name_,
                                                            *allocator_,
                                                            stmt_))) {
                LOG_WARN("failed to check and set nullable for cta.", K(ret));
              } else if (OB_FAIL(table_schema.delete_column(org_column->get_column_name_str()))) {
                LOG_WARN("delete column failed", K(ret), K(new_column.get_column_name_str()));
              } else if (OB_FAIL(table_schema.add_column(new_column))) {
                LOG_WARN("add column failed", K(ret), K(new_column));
              } else {
                LOG_DEBUG("reorder column successfully", K(new_column));
              }
            } else {
              if (OB_FAIL(set_nullable_for_cta_column(select_stmt, column, expr, table_name_, *allocator_, stmt_))) {
                LOG_WARN("failed to check and set nullable for cta.", K(ret));
              } else if (column.is_string_type() || column.is_json() || column.is_geometry()) {
                if (column.is_geometry() && T_REF_COLUMN == select_item.expr_->get_expr_type()) {
                  column.set_srs_id((static_cast<ObColumnRefRawExpr*>(select_item.expr_))->get_srs_id());
                } else if (ObHexStringType == column.get_data_type()) {
                  column.set_data_type(ObVarcharType);
                }
                if (column.get_meta_type().is_lob() || column.get_meta_type().is_json()
                    || column.get_meta_type().is_geometry()) {
                  if (OB_FAIL(check_text_column_length_and_promote(column, table_id_, true))) {
                    LOG_WARN("fail to check text or blob column length", K(ret), K(column));
                  }
                } else if (OB_FAIL(check_string_column_length(column, lib::is_oracle_mode(), params_.is_prepare_stage_))) {
                  LOG_WARN("fail to check string column length", K(ret), K(column));
                }
              } else if (ObRawType == column.get_data_type()) {
                if (OB_FAIL(ObDDLResolver::check_raw_column_length(column))) {
                  LOG_WARN("failed to check raw column length", K(ret), K(column));
                }
              }
              if (OB_FAIL(ret)) {
                //do nothing ...
              } else if (OB_FAIL(table_schema.add_column(column))) {
                LOG_WARN("add column to table_schema failed", K(ret), K(column));
              } else if (is_oracle_mode() && column.is_extend() &&
                         OB_FAIL(add_udt_hidden_column(table_schema, column))) {
                LOG_WARN("add udt hidden column to table_schema failed", K(ret), K(column));
              } else {
                ObColumnNameHashWrapper name_key(column.get_column_name_str());
                if (OB_FAIL(column_name_set_.set_refactored(name_key))) {
                  SQL_RESV_LOG(WARN, "add column name to map failed", K(ret));
                }
              }
            }
            LOG_DEBUG("ctas mysql mode, create_table_column_count = 0,end", K(column));
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::add_sort_column(const ObColumnSortItem &sort_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "variables are not inited", K(ret), KP(stmt_));
  } else {
    ObColumnSchemaV2 *column_schema = NULL;
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema &tbl_schema = create_table_stmt->get_create_table_arg().schema_;
    share::schema::ObColumnNameWrapper column_key(sort_column.column_name_, sort_column.prefix_len_);
    bool check_prefix_len = false;
    if (NULL == (column_schema = tbl_schema.get_column_schema(sort_column.column_name_))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
          sort_column.column_name_.length(), sort_column.column_name_.ptr(),
          table_name_.length(), table_name_.ptr());
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

int ObCreateTableResolver::get_table_schema_for_check(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  if (OB_FAIL(table_schema.assign(create_table_stmt->get_create_table_arg().schema_))) {
    SQL_RESV_LOG(WARN, "fail to assign schema", K(ret));
  }
  return ret;
}

int ObCreateTableResolver::generate_index_arg()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;

  if (OB_ISNULL(stmt_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "variables are not inited.", K(ret), KP(stmt_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (OB_FAIL(set_index_name())) {
    SQL_RESV_LOG(WARN, "set index name failed", K(ret), K_(index_name));
  } else if (OB_FAIL(set_index_option_to_arg())) {
    SQL_RESV_LOG(WARN, "set index option failed", K(ret));
  } else if(OB_FAIL(set_storing_column())) {
    SQL_RESV_LOG(WARN, "set storing column failed", K(ret));
  } else {
    ObIndexType type = INDEX_TYPE_IS_NOT;
    //index默认是global的，如果不指定的话，但oracle临时表是内部转换的, 只能是局部的
    if (is_oracle_temp_table_) {
      index_scope_ = LOCAL_INDEX;
    }
    if (NOT_SPECIFIED == index_scope_) {
      // MySQL default index mode is local,
      // and Oracle default index mode is global
      global_ = lib::is_oracle_mode();
    } else {
      global_ = (GLOBAL_INDEX == index_scope_);
    }
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    if (OB_SUCC(ret)) {
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
      } else if (SPATIAL_KEY == index_keyname_) {
        if (tenant_data_version < DATA_VERSION_4_1_0_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.1, spatial index is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, spatial index");
        } else if (global_) {
          type = INDEX_TYPE_SPATIAL_GLOBAL;
        } else {
          type = INDEX_TYPE_SPATIAL_LOCAL;
        }
      } else if (FTS_KEY == index_keyname_) {
        if (tenant_data_version < DATA_VERSION_4_3_1_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.1, fulltext index not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, fulltext index");
        } else if (global_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support global fts index now", K(ret));
        } else {
          // set type to fts_doc_rowkey first, append other fts arg later
          type = INDEX_TYPE_DOC_ID_ROWKEY_LOCAL;
        }
      } else if (MULTI_KEY == index_keyname_) {
        if (tenant_data_version < DATA_VERSION_4_3_1_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.1, multivalue index is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, multivalue index");
        } else if (global_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support global fts index now", K(ret));
        } else {
          type = INDEX_TYPE_NORMAL_MULTIVALUE_LOCAL;
        }
      } else if (MULTI_UNIQUE_KEY == index_keyname_) {
        if (tenant_data_version < DATA_VERSION_4_3_1_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.1, multivalue index is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, multivalue index");
        } else if (global_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support global multivalue index now", K(ret));
        } else {
          type = INDEX_TYPE_UNIQUE_MULTIVALUE_LOCAL;
        }
      }
    }
    if(OB_SUCC(ret)) {
      index_arg_.index_type_ = type;
      //create table with index .the status of index is available
      index_arg_.index_option_.index_status_ = INDEX_STATUS_AVAILABLE;
      index_arg_.index_option_.index_attributes_set_ = index_attributes_set_;
      index_arg_.sql_mode_ = session_info_->get_sql_mode();
    }
    if (OB_FAIL(ret)) {
      // skip
    } else if (INDEX_TYPE_UNIQUE_GLOBAL == type
               || INDEX_TYPE_NORMAL_GLOBAL == type) {
      ObArray<ObColumnSchemaV2 *> gen_columns;
      ObTableSchema &index_schema = index_arg_.index_schema_;
      index_schema.set_table_type(USER_INDEX);
      index_schema.set_index_type(index_arg_.index_type_);
      index_schema.set_tenant_id(table_schema.get_tenant_id());
      bool check_data_schema = false;
      if (OB_FAIL(share::ObIndexBuilderUtil::adjust_expr_index_args(
              index_arg_, table_schema, *allocator_, gen_columns))) {
        LOG_WARN("fail to adjust expr index args", K(ret));
      } else if (OB_FAIL(share::ObIndexBuilderUtil::set_index_table_columns(
              index_arg_, table_schema, index_schema, check_data_schema))) {
        LOG_WARN("fail to set index table columns", K(ret));
      }
    }
  }

  return ret;
}

int ObCreateTableResolver::check_same_substr_expr(ObRawExpr &left, ObRawExpr &right, bool &same)
{
  int ret = OB_SUCCESS;
  same = true;

  if (left.get_expr_type() != T_FUN_SYS_SUBSTR ||
      right.get_expr_type() != T_FUN_SYS_SUBSTR) {
    same = false;
  } else {
    ObSysFunRawExpr &sys_left = static_cast<ObSysFunRawExpr &>(left);
    ObSysFunRawExpr &sys_right = static_cast<ObSysFunRawExpr &>(right);
    if (sys_left.get_param_count() != sys_right.get_param_count()) {
      same = false;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_left.get_param_count() && same; ++i) {
        ObRawExpr *param_left = sys_left.get_param_expr(i);
        ObRawExpr *param_right = sys_right.get_param_expr(i);
        if (0 == i) {
          if (!param_left->is_column_ref_expr() ||
              !param_right->is_column_ref_expr()) {
            same = false;
          } else {
            ObColumnRefRawExpr *col_left = static_cast<ObColumnRefRawExpr *>(param_left);
            ObColumnRefRawExpr *col_right = static_cast<ObColumnRefRawExpr *>(param_right);
            if (!ObCharset::case_insensitive_equal(col_left->get_column_name(),
                                                   col_right->get_column_name())) {
              same = false;
            }
          }
        } else {
          if (!param_left->is_const_raw_expr() ||
              !param_right->is_const_raw_expr()) {
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
    index_arg_.index_schema_.set_name_generated_type(name_generated_type_);
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
    if (OB_FAIL(ob_write_string(*allocator_, compress_method_,
                                index_arg_.index_option_.compress_method_))) {
      SQL_RESV_LOG(WARN, "set compress func name failed", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_, comment_,
                                       index_arg_.index_option_.comment_))) {
      SQL_RESV_LOG(WARN, "set comment str failed", K(ret));
    } else {
      index_arg_.index_option_.parser_name_ = parser_name_;
      index_arg_.index_option_.row_store_type_  = row_store_type_;
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

int ObCreateTableResolver::resolve_table_level_constraint_for_mysql(
    const ParseNode* node, ObArray<int>& constraint_position_list)
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
    ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObSEArray<ObConstraint, 4>& csts = create_table_stmt->get_create_table_arg().constraint_list_;
    for (int64_t i = 0; OB_SUCC(ret) && i < constraint_position_list.size(); ++i) {
      if (OB_UNLIKELY(constraint_position_list.at(i) >= node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(constraint_position_list.at(i)));
      } else if (OB_FAIL(resolve_check_constraint_node(*node->children_[constraint_position_list.at(i)], csts))) {
        SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::resolve_index(
    const ParseNode *node,
    ObArray<int> &index_node_position_list)
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
      } else { /*do nothing*/ }
    }
    current_index_name_set_.reset();
  }

  return ret;
}

int ObCreateTableResolver::resolve_index_node(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  ObString uk_name;
  bool is_oracle_mode = lib::is_oracle_mode();
  const uint64_t tenant_id = session_info_->get_effective_tenant_id();
  bool is_index_part_specified = false;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node is null.", K(ret));
  } else if (ObItemType::T_INDEX != node->type_ && ObItemType::T_COLUMN_DEFINITION != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid arguments.", K(ret), K(node->type_), K(node->num_child_));
  } else if (OB_ISNULL(stmt_) || OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)){
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "stmt or session_info or schema_checker is null.",
                 K(ret), KP(session_info_), KP(stmt_), K_(schema_checker));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "node->children_ is null.", K(ret));
  } else if (static_cast<int64_t>(table_id_) > 0
             && OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
             session_info_->get_effective_tenant_id(), table_id_, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K_(table_id));
  } else {
    index_arg_.reset();
    ObColumnSortItem sort_item;
    ObString first_column_name;
    ObColumnSchemaV2 *column_schema = NULL;
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObTableSchema &tbl_schema = create_table_stmt->get_create_table_arg().schema_;
    if(ObItemType::T_INDEX == node->type_) {
      //if index_name is not specified, new index_name will be generated
      //by the first_column_name, so resolve the index_column_list_node firstly.
      if (NULL == node->children_[1] || T_INDEX_COLUMN_LIST != node->children_[1]->type_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid index column list.", K(ret));
      } else {
        int64_t index_data_length = 0;
        index_keyname_ = static_cast<INDEX_KEYNAME>(node->value_);
        ParseNode *index_column_list_node = node->children_[1];
        if (index_column_list_node->num_child_ > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
          ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
          LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
        } else if (OB_ISNULL(index_column_list_node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "children_ is null.", K(ret));
        } else { /*do nothing*/ }

        ParseNode *index_column_node = NULL;
        bool is_ctxcat_added = false;
        if (OB_SUCC(ret) && OB_FAIL(add_new_indexkey_for_oracle_temp_table(index_column_list_node->num_child_))) {
          SQL_RESV_LOG(WARN, "add session id key failed", K(ret));
        }
        bool cnt_func_index_mysql = false;
        bool is_multi_value_index = false;
        for (int32_t i = 0; OB_SUCC(ret) && i < index_column_list_node->num_child_; ++i) {
          ObString &column_name = sort_item.column_name_;
          if (NULL == index_column_list_node->children_[i]
              || T_SORT_COLUMN_KEY != index_column_list_node->children_[i]->type_) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "invalid index_column_list_node.", K(ret));
          } else {
            index_column_node = index_column_list_node->children_[i];
            if (OB_ISNULL(index_column_node->children_)
                || index_column_node->num_child_ < 3
                || OB_ISNULL(index_column_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "invalid index_column_node.", K(ret),
                           K(index_column_node->num_child_),
                           K(index_column_node->children_),
                           K(index_column_node->children_[0]));
            } else {
              //column_name
              if (index_column_node->children_[0]->type_ != T_IDENT) {
                sort_item.is_func_index_ = true;
                cnt_func_index_mysql = true;
              } else {
                sort_item.is_func_index_ = false;
              }
              column_name.assign_ptr(
                  const_cast<char *>(index_column_node->children_[0]->str_value_),
                  static_cast<int32_t>(index_column_node->children_[0]->str_len_));
              if (OB_FAIL(ObMulValueIndexBuilderUtil::adjust_index_type(column_name,
                                                                        is_multi_value_index,
                                                                        reinterpret_cast<int*>(&index_keyname_)))) {
                LOG_WARN("failed to resolve index type", K(ret));
              } else if (NULL != index_column_node->children_[1]) {
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
              ObRawExpr *expr = NULL;
              if (is_multi_value_index) {
                ObColumnSchemaV2 *budy_column_schema = NULL;
                bool force_rebuild = true;
                if (OB_FAIL(ObMulValueIndexBuilderUtil::build_and_generate_multivalue_column(
                                                                                  sort_item,
                                                                                  *params_.expr_factory_,
                                                                                  *session_info_,
                                                                                  tbl_schema,
                                                                                  schema_checker_,
                                                                                  force_rebuild,
                                                                                  column_schema,
                                                                                  budy_column_schema))) {
                  LOG_WARN("failed to build index schema failed", K(ret));
                } else if (OB_ISNULL(column_schema) || OB_ISNULL(budy_column_schema)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("multivalue index generate column, or budy column is null.",
                    K(ret), KP(column_schema), KP(budy_column_schema));
                } else {
                  ObColumnNameHashWrapper column_name_key(column_schema->get_column_name_str());
                  if (OB_FAIL(column_name_set_.set_refactored(column_name_key))) {
                    LOG_WARN("add column name to map failed", K(column_schema->get_column_name_str()), K(ret));
                  } else {
                    ObColumnSortItem budy_sort_item;
                    budy_sort_item.is_func_index_ = true;
                    budy_sort_item.column_name_ = budy_column_schema->get_column_name_str();

                    ObColumnNameHashWrapper budy_column_name_key(budy_column_schema->get_column_name_str());
                    if (OB_FAIL(column_name_set_.set_refactored(budy_column_name_key))) {
                      LOG_WARN("add column name to map failed", K(budy_column_schema->get_column_name_str()), K(ret));
                    } else if (OB_FAIL(add_sort_column(budy_sort_item))) {
                      LOG_WARN("failed to add sort item", K(ret));
                    }
                  }
                }
              } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(NULL,
                                                                      column_name,
                                                                      *params_.expr_factory_,
                                                                      *session_info_,
                                                                      tbl_schema,
                                                                      expr,
                                                                      schema_checker_,
                                                                      ObResolverUtils::CHECK_FOR_FUNCTION_INDEX))) {
                LOG_WARN("build generated column expr failed", K(ret));
              } else if (!expr->is_column_ref_expr()) {
                //real index expr, so generate hidden generated column in data table schema
                if (ob_is_geometry(expr->get_data_type()) || static_cast<int64_t>(INDEX_KEYNAME::SPATIAL_KEY) == node->value_) {
                  ret = OB_ERR_SPATIAL_FUNCTIONAL_INDEX;
                  LOG_WARN("Spatial functional index is not supported.", K(ret), K(column_name));
                } else if (OB_FAIL(ObIndexBuilderUtil::generate_ordinary_generated_column(*expr,
                                                                                   session_info_->get_sql_mode(),
                                                                                   tbl_schema,
                                                                                   column_schema,
                                                                                   schema_checker_->get_schema_guard()))) {
                  LOG_WARN("generate ordinary generated column failed", K(ret));
                } else {
                  ObColumnNameHashWrapper column_name_key(column_schema->get_column_name_str());
                  sort_item.column_name_ = column_schema->get_column_name_str();
                  sort_item.is_func_index_ = false;
                  if (OB_FAIL(column_name_set_.set_refactored(column_name_key))) {
                    LOG_WARN("add column name to map failed", K(column_schema->get_column_name_str()), K(ret));
                  }
                }
              } else if (is_oracle_mode) {
                const ObColumnRefRawExpr *ref_expr = static_cast<const ObColumnRefRawExpr*>(expr);
                sort_item.column_name_ = ref_expr->get_column_name();
                sort_item.is_func_index_ = false;
                column_schema = tbl_schema.get_column_schema(ref_expr->get_column_id());
              } else {
                ret = OB_ERR_FUNCTIONAL_INDEX_ON_FIELD;
                LOG_WARN("Functional index on a column is not supported.", K(ret), K(*expr));
              }
            } else {
              if (NULL == (column_schema = tbl_schema.get_column_schema(column_name))) {
                ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
                LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_ISNULL(session_info_)) {
                ret = OB_NOT_INIT;
                LOG_WARN("session_info_ is null");
              }  else if (sort_item.prefix_len_ > column_schema->get_data_length()) {
                ret = OB_WRONG_SUB_KEY;
                SQL_RESV_LOG(WARN, "prefix length is longer than column length", K(sort_item), K(column_schema->get_data_length()), K(ret));
              } else if (ob_is_text_tc(column_schema->get_data_type())
                  && static_cast<int64_t>(INDEX_KEYNAME::FTS_KEY) != node->value_) {
                if (column_schema->is_hidden()) {
                  //functional index in mysql mode
                  ret = OB_ERR_FUNCTIONAL_INDEX_ON_LOB;
                  LOG_WARN("Cannot create a functional index on an expression that returns a BLOB or TEXT.", K(ret));
                } else if(sort_item.prefix_len_ <= 0) {
                  ret = OB_ERR_WRONG_KEY_COLUMN;
                  LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
                }
              } else if (OB_FAIL(resolve_spatial_index_constraint(*column_schema,
                  index_column_list_node->num_child_, node->value_, is_oracle_mode,
                  NULL != index_column_node->children_[2] && 1 != index_column_node->children_[2]->is_empty_))) {
                SQL_RESV_LOG(WARN, "fail to resolve spatial index constraint", K(ret), K(column_name));
              } else if (OB_FAIL(resolve_fts_index_constraint(*column_schema,
                                                              node->value_))) {
                SQL_RESV_LOG(WARN, "fail to resolve fts index constraint", K(ret), K(column_name));
              } else if (OB_FAIL(resolve_multivalue_index_constraint(*column_schema, index_keyname_))) {
                SQL_RESV_LOG(WARN, "fail to resolve multivalue index constraint", K(ret), K(column_name));
              }

              if (OB_SUCC(ret) && ob_is_string_type(column_schema->get_data_type())) {
                int64_t length = 0;
                if (OB_FAIL(column_schema->get_byte_length(length, is_oracle_mode, false))) {
                  SQL_RESV_LOG(WARN, "fail to get byte length of column", KR(ret), K(is_oracle_mode));
                } else if (sort_item.prefix_len_ > 0) {
                  length = length * sort_item.prefix_len_ / column_schema->get_data_length();
                } else { /*do nothing*/ }

                if (OB_SUCC(ret)) {
                  if ((index_data_length += length) > OB_MAX_USER_ROW_KEY_LENGTH
                      && static_cast<int64_t>(INDEX_KEYNAME::FTS_KEY) != node->value_) {
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
              //column_order
              if (is_oracle_mode && NULL != index_column_node->children_[2]
                  && T_SORT_DESC == index_column_node->children_[2]->type_) {
                // sort_item.order_type_ = common::ObOrderType::DESC;
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("not support desc index now", K(ret));
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "desc index");
              } else {
                //兼容mysql5.7, 降序索引不生效且不报错
                sort_item.order_type_ = common::ObOrderType::ASC;
              }
              ObColumnNameHashWrapper column_key(column_name);
              if (OB_HASH_NOT_EXIST == column_name_set_.exist_refactored(column_key)) {
                ret = OB_ERR_BAD_FIELD_ERROR;
                LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(),
                               table_name_.length(), table_name_.ptr());
              } else {
                if (0 == i) {
                  first_column_name.assign_ptr(
                      const_cast<char *>(index_column_node->children_[0]->str_value_),
                      static_cast<int32_t>(index_column_node->children_[0]->str_len_));
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(add_sort_column(sort_item))) {
                SQL_RESV_LOG(WARN, "add sort column failed", K(ret), K(sort_item));
              } else { /*do nothing*/ }
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

        if (OB_SUCC(ret) && cnt_func_index_mysql) {
          uint64_t tenant_data_version = 0;
          if (OB_ISNULL(session_info_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret));
          } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
            LOG_WARN("get tenant data version failed", K(ret));
          } else if (tenant_data_version < DATA_VERSION_4_2_0_0){
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("tenant version is less than 4.2, functional index is not supported in mysql mode", K(ret), K(tenant_data_version));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2, functional index in mysql mode not supported");
          }
        }

        if (OB_SUCC(ret) && cnt_func_index_mysql) {
          first_column_name = ObString::make_string("functional_index");
        }
      }
    } else {
      //unique [key]
      if (T_COLUMN_DEFINITION != node->type_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(node->type_));
      } else if (NULL == node->children_[0]
                 || T_COLUMN_REF != node->children_[0]->type_
                 || COLUMN_DEF_NUM_CHILD != node->children_[0]->num_child_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
      } else if (OB_ISNULL(node->children_[0]->children_) || OB_ISNULL(node->children_[0]->children_[2])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "node->ch[0]->ch[2] is null.", K(ret));
      } else {
        index_keyname_ = UNIQUE_KEY;
        ObString &column_name = sort_item.column_name_;
        column_name.assign_ptr(
            const_cast<char *>(node->children_[0]->children_[2]->str_value_),
            static_cast<int32_t>(node->children_[0]->children_[2]->str_len_));
        if (NULL == (column_schema = tbl_schema.get_column_schema(column_name))) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(),
              table_name_.length(), table_name_.ptr());
        } else if (ObTimestampTZType == column_schema->get_data_type()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
        } else if (ob_is_string_tc(column_schema->get_data_type())) {
          int64_t length = 0;
          if (OB_FAIL(column_schema->get_byte_length(length, is_oracle_mode, false))) {
            SQL_RESV_LOG(WARN, "fail to get byte length of column", KR(ret), K(is_oracle_mode));
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
      if (OB_SUCC(ret) && lib::is_oracle_mode()) {
        ParseNode *attrs_node = node->children_[2];
        if (NULL != attrs_node && OB_UNLIKELY(4 == node->num_child_)) {
          if (OB_FAIL(resolve_uk_name_from_column_attribute(attrs_node, uk_name))) {
            SQL_RESV_LOG(WARN, "resolve uk name from column attribute", K(ret), K(sort_item));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      has_index_using_type_ = false;
      if (OB_FAIL(resolve_index_name(
          ObItemType::T_INDEX == node->type_ ? node->children_[0] : NULL,
          first_column_name,
          (UNIQUE_KEY == index_keyname_ || MULTI_UNIQUE_KEY == index_keyname_) ? true : false,
          uk_name))) {
        SQL_RESV_LOG(WARN, "resolve index name failed", K(ret));
      } else if (ObItemType::T_INDEX == node->type_ && OB_FAIL(resolve_table_options(node->children_[2], true))) {
        SQL_RESV_LOG(WARN, "resolve index options failed", K(ret));
      }
      if (OB_SUCC(ret) && lib::is_mysql_mode()) {
        if (ObItemType::T_INDEX == node->type_ && NULL != node->children_[4]) {
          if (1 != node->children_[4]->num_child_ || T_PARTITION_OPTION != node->children_[4]->type_) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "column vertical partition for index");
          } else if (OB_ISNULL(node->children_[4]->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("node is null", K(ret));
          } else if (LOCAL_INDEX == index_scope_) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify partition option of local index");
          } else if (NOT_SPECIFIED == index_scope_) {
            index_scope_ = GLOBAL_INDEX;
          }
          is_index_part_specified = true;
        }
      }

      // index column_group
      if (OB_SUCC(ret) && lib::is_mysql_mode()) { //only mysql support create table with index
        if (node->num_child_ < 6) {
          // no cg, ignore
        } else if (ObItemType::T_INDEX == node->type_ && NULL != node->children_[5]) {
          if (T_COLUMN_GROUP != node->children_[5]->type_ || node->children_[5]->num_child_ <= 0) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid argument", KR(ret), K(node->type_), K(node->num_child_));
          } else if (OB_ISNULL(node->children_[5]->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("node is null", K(ret));
          } else if (OB_FAIL(resolve_index_column_group(node->children_[5], index_arg_))) {
            SQL_RESV_LOG(WARN, "resolve index column group failed", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_index_arg())) {
        SQL_RESV_LOG(WARN, "generate index arg failed", K(ret));
      } else if (tbl_schema.is_partitioned_table()
          && INDEX_TYPE_SPATIAL_GLOBAL == index_arg_.index_type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "spatial global index");
      } else {
        if (has_index_using_type_) {
          index_arg_.index_using_type_ = index_using_type_;
        }
      }
    }
    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      if (OB_FAIL(set_index_tablespace(tbl_schema, index_arg_))) {
        LOG_WARN("fail to set index tablespace", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      HEAP_VARS_2((ObCreateIndexStmt, create_index_stmt), (ObPartitionResolveResult, resolve_result))  {
        ObCreateIndexArg &create_index_arg = create_index_stmt.get_create_index_arg();
        ObSArray<ObPartitionResolveResult> &resolve_results = create_table_stmt->get_index_partition_resolve_results();
        ObSArray<obrpc::ObCreateIndexArg> &index_arg_list = create_table_stmt->get_index_arg_list();
        if (OB_FAIL(create_index_arg.assign(index_arg_))) {
          LOG_WARN("fail to assign create index arg", K(ret));
        } else if (is_index_part_specified) {
          if (OB_FAIL(resolve_index_partition_node(node->children_[4]->children_[0], &create_index_stmt))) {
            LOG_WARN("fail to resolve partition option", K(ret));
          } else {
            resolve_result.get_part_fun_exprs() = create_index_stmt.get_part_fun_exprs();
            resolve_result.get_part_values_exprs() = create_index_stmt.get_part_values_exprs();
            resolve_result.get_subpart_fun_exprs() = create_index_stmt.get_subpart_fun_exprs();
            resolve_result.get_template_subpart_values_exprs() = create_index_stmt.get_template_subpart_values_exprs();
            resolve_result.get_individual_subpart_values_exprs() = create_index_stmt.get_individual_subpart_values_exprs();
          }
        }
        if (OB_SUCC(ret)) {
          if (is_fts_index(index_arg_.index_type_)) {
            if (OB_FAIL(ObDDLResolver::append_fts_args(resolve_result,
                                                       create_index_arg,
                                                       have_generate_fts_arg_,
                                                       resolve_results,
                                                       index_arg_list,
                                                       allocator_))) {
              LOG_WARN("failed to append fts args", K(ret));
            }
          } else if (is_multivalue_index(index_arg_.index_type_)) {
            if (OB_FAIL(ObDDLResolver::append_multivalue_args(resolve_result,
                                                              create_index_arg,
                                                              have_generate_fts_arg_,
                                                              resolve_results,
                                                              index_arg_list,
                                                              allocator_))) {
              LOG_WARN("failed to append fts args", K(ret));
            }
          } else {
            if (OB_FAIL(resolve_results.push_back(resolve_result))) {
              LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
            } else if (OB_FAIL(index_arg_list.push_back(create_index_arg))) {
              LOG_WARN("fail to push back index_arg", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableResolver::resolve_index_name(
    const ParseNode *node,
    const ObString &first_column_name,
    bool is_unique,
    ObString &uk_name)
{
  int ret =OB_SUCCESS;
  name_generated_type_ = GENERATED_TYPE_USER;
  if (NULL == node) {
    if (lib::is_oracle_mode() && is_unique) {
      if (NULL == uk_name.ptr()) {
        if (OB_FAIL(ObTableSchema::create_cons_name_automatically(index_name_, table_name_, *allocator_, CONSTRAINT_TYPE_UNIQUE_KEY, lib::is_oracle_mode()))) {
          SQL_RESV_LOG(WARN, "create index name automatically failed", K(ret));
        } else {
          name_generated_type_ = GENERATED_TYPE_SYSTEM;
        }
      } else {
        index_name_.assign_ptr(uk_name.ptr(), uk_name.length());
      }
    } else if (lib::is_oracle_mode() && !is_unique) {
      if (OB_FAIL(ObTableSchema::create_idx_name_automatically_oracle(index_name_, table_name_, *allocator_))) {
        SQL_RESV_LOG(WARN, "create index name automatically failed", K(ret));
      } else {
        name_generated_type_ = GENERATED_TYPE_SYSTEM;
      }
    } else { // mysql mode
      if (OB_FAIL(generate_index_name(index_name_, current_index_name_set_, first_column_name))) {
        SQL_RESV_LOG(WARN, "generate index name failed", K(ret), K(index_name_));
      } else {
        name_generated_type_ = GENERATED_TYPE_SYSTEM;
      }
    }
  } else if (T_IDENT != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node_type is wrong.", K(ret));
  } else {
    int32_t len = static_cast<int32_t>(node->str_len_);
    index_name_.assign_ptr(node->str_value_, len);
    //check duplicate for index_name
    ObIndexNameHashWrapper index_key(index_name_);
    if (OB_HASH_EXIST == (ret = current_index_name_set_.exist_refactored(index_key))) {
      SQL_RESV_LOG(WARN, "duplicate index name", K(ret), K(index_name_));
      ret = OB_ERR_KEY_NAME_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_KEY_NAME_DUPLICATE,
                     index_name_.length(),
                     index_name_.ptr());
    } else if (0 == ObString::make_string("primary").case_compare(index_name_)) {
    //index name can not be 'primary'
      ret = OB_WRONG_NAME_FOR_INDEX;
      LOG_USER_ERROR(OB_WRONG_NAME_FOR_INDEX, index_name_.length(), index_name_.ptr());
    } else if (index_name_.empty()) {
      if (lib::is_oracle_mode()) {
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
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_UNLIKELY(NULL == session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session if NULL", K(ret));
    } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
      LOG_WARN("fail to get collation connection", K(ret));
    } else if (OB_FAIL(ObSQLUtils::check_index_name(cs_type, index_name_))) {
      LOG_WARN("fail to check index name", K(ret), K(index_name_));
    }
  }

  return ret;
}

int ObCreateTableResolver::resolve_external_table_format_early(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "format");
  } else {
    if (T_TABLE_OPTION_LIST != node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument.", K(ret));
    } else {
      ParseNode *option_node = NULL;
      int32_t num = node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        option_node = node->children_[i];
        if (OB_NOT_NULL(option_node) && T_EXTERNAL_FILE_FORMAT == option_node->type_) {
          ObExternalFileFormat format;
          for (int32_t j = 0; OB_SUCC(ret) && j < option_node->num_child_; ++j) {
            if (OB_NOT_NULL(option_node->children_[j])
                && T_EXTERNAL_FILE_FORMAT_TYPE == option_node->children_[j]->type_) {
              if (OB_FAIL(resolve_file_format(option_node->children_[j], format))) {
                LOG_WARN("fail to resolve file format", K(ret));
              } else {
                external_table_format_type_ = format.format_type_;
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && external_table_format_type_ >= ObExternalFileFormat::PARQUET_FORMAT) {
    uint64_t data_version = 0;
    CK (OB_NOT_NULL(session_info_));
    OZ (GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), data_version));
    OV (DATA_VERSION_4_3_2_0 <= data_version, OB_NOT_SUPPORTED, data_version);
  }
  return ret;
}

int ObCreateTableResolver::resolve_table_charset_info(const ParseNode *node) {
  int ret = OB_SUCCESS;
  if (NULL != node) {
    if (T_TABLE_OPTION_LIST != node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
    } else {
      ParseNode *option_node = NULL;
      int32_t num = node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        option_node = node->children_[i];
        if (OB_ISNULL(option_node)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid argument.", K(ret), K(option_node));
        } else if (T_CHARSET == option_node->type_
            && OB_FAIL(resolve_table_option(option_node, false))) {
          SQL_RESV_LOG(WARN, "resolve failed", K(ret));
        } else if (T_COLLATION == option_node->type_
                   && OB_FAIL(resolve_table_option(option_node, false))) {
          SQL_RESV_LOG(WARN, "resolve failed", K(ret));
        } else { /*do nothing*/ }
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
      const ObDatabaseSchema *database_schema = NULL;
      if (OB_FAIL(schema_checker_->get_database_id(tenant_id, database_name_, database_id)))  {
        SQL_RESV_LOG(WARN, "fail to get database_id.", K(ret), K(database_name_), K(tenant_id));
      } else if (OB_FAIL(schema_checker_->get_database_schema(tenant_id, database_id, database_schema))) {
        LOG_WARN("failed to get db schema", K(ret), K(database_id));
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

int ObCreateTableResolver::resolve_auto_partition(const ParseNode *partition_node)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt *create_table_stmt =
      static_cast<ObCreateTableStmt *>(stmt_);
  if (OB_ISNULL(partition_node)
      || T_AUTO_PARTITION != partition_node->type_
      || 2 != partition_node->num_child_
      || OB_ISNULL(partition_node->children_[0])
      || OB_ISNULL(partition_node->children_[1])
      || OB_ISNULL(create_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "node is unexpected", KR(ret),
                       "type", get_type_name(partition_node->type_),
                       "child_num", partition_node->num_child_);
  } else {
    const ParseNode *part_type_node = partition_node->children_[0];
    ObTableSchema &table_schema =
        create_table_stmt->get_create_table_arg().schema_;
    PartitionInfo part_info;
    share::schema::ObPartitionFuncType part_func_type = share::schema::PARTITION_FUNC_TYPE_RANGE;
    share::schema::ObPartitionOption *partition_option = NULL;
    part_info.part_level_ = share::schema::PARTITION_LEVEL_ONE;
    partition_option = &part_info.part_option_;
    const ParseNode *part_expr_node = part_type_node->children_[0];
    if (T_RANGE_COLUMNS_PARTITION == part_type_node->type_) {
      part_func_type = share::schema::PARTITION_FUNC_TYPE_RANGE_COLUMNS;
    } else if (T_RANGE_PARTITION == part_type_node->type_) {
      part_func_type = share::schema::PARTITION_FUNC_TYPE_RANGE;
    } else {
      ret = OB_NOT_SUPPORTED;
      SQL_RESV_LOG(WARN, "part type not supported", KR(ret),
                   "type", get_type_name(part_type_node->type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "specified part type");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(partition_option)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "partition option is null", KR(ret));
    } else {
      partition_option->set_part_func_type(part_func_type);
      partition_option->set_auto_part(true/*auto_part*/);
      int64_t part_size = -1;
      const ParseNode *part_size_node = partition_node->children_[1];
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
        SQL_RESV_LOG(WARN, "part type not supported", KR(ret),
                           "type", get_type_name(part_size_node->type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specified part type");
      }
      if (OB_SUCC(ret)) {
        partition_option->set_auto_part_size(part_size);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(part_expr_node)) {
      //暂时不支持不指定分区键
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("none of partition key not supported", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "none of partition key");
    } else {
      common::ObString func_expr_name;
      const bool is_subpartition = false;
      func_expr_name.assign_ptr(
          const_cast<char*>(part_type_node->str_value_),
          static_cast<int32_t>(part_type_node->str_len_));
      if (OB_FAIL(resolve_part_func(params_,
                                    part_expr_node,
                                    part_func_type,
                                    table_schema,
                                    part_info.part_func_exprs_,
                                    part_info.part_keys_))) {
        SQL_RESV_LOG(WARN, "resolve part func failed", KR(ret));
      } else if (OB_FAIL(partition_option->set_part_expr(func_expr_name))) {
        SQL_RESV_LOG(WARN, "set partition express string failed", KR(ret));
      } else if (OB_FAIL(set_partition_keys(table_schema, part_info.part_keys_,
                                            is_subpartition))) {
        SQL_RESV_LOG(WARN, "Failed to set partition keys", KR(ret),
                     K(table_schema), K(is_subpartition));
      } else {
        //填充max_value
        ObPartition partition;
        common::ObArray<common::ObObj> rowkeys;
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
            LOG_WARN("rowkey count can not be zero", KR(ret), K(rowkeys),
                      "partition_key", part_info.part_keys_);
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

uint64_t ObCreateTableResolver::gen_column_group_id()
{
  return ++cur_column_group_id_;
}

/*
* only when default columns store is column_store
* have to add each column group
*/
int ObCreateTableResolver::resolve_column_group(const ParseNode *cg_node)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt *>(stmt_);
  ObArray<uint64_t> column_ids; // not include virtual column
  uint64_t compat_version = 0;
  ObTableStoreType table_store_type = OB_TABLE_STORE_INVALID;

  if (OB_ISNULL(create_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "create_table_stmt should not be null", KR(ret));
  } else {
    ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const int64_t column_cnt = table_schema.get_column_count();
    if (OB_FAIL(column_ids.reserve(column_cnt))) {
      LOG_WARN("fail to reserve", KR(ret), K(column_cnt));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    } else if (!(compat_version >= DATA_VERSION_4_3_0_0)) {
      if (OB_NOT_NULL(cg_node) && (T_COLUMN_GROUP == cg_node->type_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can't support column store if version less than 4_1_0_0", KR(ret), K(compat_version));
      }
    } else {
      table_schema.set_column_store(true);
      bool is_each_cg_exist = false;
      if (OB_NOT_NULL(cg_node)) {
        if (OB_FAIL(parse_column_group(cg_node, table_schema, table_schema))) {
          LOG_WARN("fail to parse column group", K(ret));
        }
      }

      /* build column group when cg node is null && tenant cg valid*/
      ObTenantConfigGuard tenant_config(TENANT_CONF(session_info_->get_effective_tenant_id()));
      if (OB_FAIL(ret)) {
      } else if ( OB_LIKELY(tenant_config.is_valid()) && nullptr == cg_node) {
        /* force to build each cg*/
        if (!ObSchemaUtils::can_add_column_group(table_schema)) {
        } else if (OB_FAIL(ObTableStoreFormat::find_table_store_type(
                    tenant_config->default_table_store_format.get_value_string(),
                    table_store_type))) {
          LOG_WARN("fail to get table store format", K(ret), K(table_store_type));
        } else if (ObTableStoreFormat::is_with_column(table_store_type)) {
          /* for default is column store, must add each column group*/
          if (OB_FAIL(ObSchemaUtils::build_add_each_column_group(table_schema, table_schema))) {
            LOG_WARN("fail to add each column group", K(ret));
          }
        }

        /* force to build all cg*/
        ObColumnGroupSchema all_cg;
        if (OB_FAIL(ret)) {
        } else if (!ObSchemaUtils::can_add_column_group(table_schema)) {
        } else if (ObTableStoreFormat::is_row_with_column_store(table_store_type)) {
          if (OB_FAIL(ObSchemaUtils::build_all_column_group(table_schema, table_schema.get_tenant_id(),
                                                                   table_schema.get_max_used_column_group_id() + 1, all_cg))) {
            LOG_WARN("fail to add all column group", K(ret));
          } else if (OB_FAIL(table_schema.add_column_group(all_cg))) {
            LOG_WARN("fail to build all column group", K(ret));
          }
        }
      }

      // add default_type column_group, build a empty and then use alter_deafult_cg
      if (OB_SUCC(ret)) {
        ObColumnGroupSchema tmp_cg;
        column_ids.reuse();
        if (OB_FAIL(build_column_group(table_schema, ObColumnGroupType::DEFAULT_COLUMN_GROUP,
            OB_DEFAULT_COLUMN_GROUP_NAME, column_ids, DEFAULT_TYPE_COLUMN_GROUP_ID, tmp_cg))) {
          LOG_WARN("fail to build default type column_group", KR(ret), K(table_store_type),
                   "table_id", table_schema.get_table_id());
        } else if (OB_FAIL(table_schema.add_column_group(tmp_cg))) {
          LOG_WARN("fail to add default column group", KR(ret), "table_id", table_schema.get_table_id());
        } else if (OB_FAIL(ObSchemaUtils::alter_rowkey_column_group(table_schema))) {
          LOG_WARN("fail to adjust rowkey column group when add column group", K(ret));
        } else if (OB_FAIL(ObSchemaUtils::alter_default_column_group(table_schema))) {
          LOG_WARN("fail to adjust default column group", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObCreateTableResolver::add_inner_index_for_heap_gtt() {
  int ret = OB_SUCCESS;
  if (is_oracle_temp_table_) {
    if (OB_ISNULL(stmt_) || OB_ISNULL(allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "stmt is NULL", K(stmt_), K(ret));
    } else {
      ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
      ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
      ObSqlString temp_str;

      reset();
      index_attributes_set_ = OB_DEFAULT_INDEX_ATTRIBUTES_SET;
      index_arg_.reset();

      OZ (temp_str.append_fmt("IDX_FOR_HEAP_GTT_%.*s", table_name_.length(), table_name_.ptr()));
      OZ (ob_write_string(*allocator_, temp_str.string(), index_name_));

      OZ (add_new_indexkey_for_oracle_temp_table(0));

      for (int i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
        const ObColumnSchemaV2 *column = table_schema.get_column_schema_by_idx(i);
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column schema", K(ret));
        } else {
          if (!column->is_rowkey_column()
              && column->get_column_id() != OB_HIDDEN_SESSION_ID_COLUMN_ID
              && column->get_column_id() != OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID) {
            ObString column_name = column->get_column_name_str();
            bool has_invalid_types = false;
            if (OB_FAIL(add_storing_column(column_name,
                                           true, /*check_column_exist*/
                                           false, /*is_hidden*/
                                           &has_invalid_types))) {
              if (has_invalid_types) {
                ret = OB_SUCCESS;
                //ignore invalid columns
              } else {
                LOG_WARN("fail to add storing column", K(ret));
              }
            }
          }
        }
      }

      OZ (generate_index_arg());
      OZ (create_table_stmt->get_index_arg_list().push_back(index_arg_));
      OZ (create_table_stmt->get_index_partition_resolve_results().push_back(ObPartitionResolveResult()));
    }
  }
  return ret;
}

int ObCreateTableResolver::check_max_row_data_length(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t row_data_length = 0;
  bool is_oracle_mode = lib::is_oracle_mode();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
    int64_t length = 0;
    const ObColumnSchemaV2 *column = table_schema.get_column_schema_by_idx(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret), K(table_schema));
    } else if (! column->is_string_type() && ! is_lob_storage(column->get_data_type()) ) { // skip non string or lob storage type
    } else if (OB_FAIL(column->get_byte_length(length, is_oracle_mode, false))) {
      SQL_RESV_LOG(WARN, "fail to get byte length of column", KR(ret), K(is_oracle_mode));
    } else if (ob_is_string_tc(column->get_data_type()) && length > OB_MAX_VARCHAR_LENGTH) {
      ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, column->get_column_name(), static_cast<int32_t>(OB_MAX_VARCHAR_LENGTH));
    } else if (is_lob_storage(column->get_data_type())) {
      ObLength max_length = 0;
      max_length = ObAccuracy::MAX_ACCURACY2[is_oracle_mode][column->get_data_type()].get_length();
      if (length > max_length) {
        ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, column->get_column_name(),
            ObAccuracy::MAX_ACCURACY2[is_oracle_mode][column->get_data_type()].get_length());
      } else {
        length = min(length, max(table_schema.get_lob_inrow_threshold(), OB_MAX_LOB_HANDLE_LENGTH));
      }
    }
    if (OB_SUCC(ret) && (row_data_length += length) > OB_MAX_USER_ROW_LENGTH) {
      ret = OB_ERR_TOO_BIG_ROWSIZE;
      SQL_RESV_LOG(WARN, "too big rowsize", KR(ret), K(is_oracle_mode), K(i), K(row_data_length), K(length));
    }
  }
  return ret;
}

}//end namespace sql
}//end namespace oceanbase
