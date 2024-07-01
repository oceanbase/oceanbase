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
#include "sql/resolver/ddl/ob_create_index_resolver.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{
ObCreateIndexResolver::ObCreateIndexResolver(ObResolverParams &params)
   : ObDDLResolver(params),is_oracle_temp_table_(false), is_spec_block_size(false)
{
}

ObCreateIndexResolver::~ObCreateIndexResolver()
{
}

// child 0 of root node, resolve index name
int ObCreateIndexResolver::resolve_index_name_node(
    ParseNode *index_name_node,
    ObCreateIndexStmt *crt_idx_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == index_name_node)
      || OB_UNLIKELY(NULL == crt_idx_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_name_node), KP(crt_idx_stmt));
  } else if (index_name_node->num_child_ < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), "child_num", index_name_node->num_child_);
  } else if (NULL == index_name_node->children_[1]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (NULL != index_name_node->children_[0]) { // database name not null
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    uint64_t database_id = OB_INVALID_ID;
    const ObString &database_name = crt_idx_stmt->get_database_name();
    uint64_t spec_database_id = OB_INVALID_ID;
    ObString spec_database_name(index_name_node->children_[0]->str_len_,
                                index_name_node->children_[0]->str_value_);
    if (OB_FAIL(schema_checker_->get_database_id(
            tenant_id, database_name, database_id))) {
      LOG_WARN("fail to get database_id", K(ret), K(database_name), K(tenant_id));
    } else if (OB_FAIL(schema_checker_->get_database_id(
            tenant_id, spec_database_name, spec_database_id))) {
      LOG_WARN("fail to get database id", K(ret));
    } else if (spec_database_id != database_id) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("should specify the database name of data table for index",
               K(ret), K(spec_database_name), K(database_name));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Index name including database name is");
    } else {} // no more to do
  }
  if (OB_SUCC(ret)) {
    int32_t len = static_cast<int32_t>(index_name_node->children_[1]->str_len_);
    ObString index_name(len, index_name_node->children_[1]->str_value_);
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_UNLIKELY(NULL == session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session if NULL", K(ret));
    } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
      LOG_WARN("fail to get collation connection", K(ret));
    } else if (OB_FAIL(ObSQLUtils::check_index_name(cs_type, index_name))) {
      LOG_WARN("fail to check index name", K(ret), K(index_name));
    } else {
      crt_idx_stmt->set_index_name(index_name);
      index_keyname_ = static_cast<INDEX_KEYNAME>(index_name_node->value_);
    }
  }
  return ret;
}

// child 1 of root node, resolve table name of this index
int ObCreateIndexResolver::resolve_index_table_name_node(
    ParseNode *index_table_name_node,
    ObCreateIndexStmt *crt_idx_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == index_table_name_node)
      || OB_UNLIKELY(NULL == crt_idx_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_table_name_node), KP(crt_idx_stmt));
  } else {
    ObString table_name;
    ObString database_name;
    if (OB_FAIL(resolve_table_relation_node(index_table_name_node, table_name, database_name))) {
      LOG_WARN("fail to resolve table relation node", K(ret));
    } else if (OB_FAIL(set_database_name(database_name))) {
      LOG_WARN("fail to set database name", K(ret));
    } else {
      crt_idx_stmt->set_database_name(database_name);
      crt_idx_stmt->set_table_name(table_name);
      crt_idx_stmt->set_name_generated_type(GENERATED_TYPE_USER);
      crt_idx_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    }
  }
  return ret;
}

// 索引义添加__session_id并作为首列
int ObCreateIndexResolver::add_new_indexkey_for_oracle_temp_table()
{
  int ret = OB_SUCCESS;
  if (is_oracle_temp_table_) {
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
  return ret;
}

// child 2 of root node, resolve index column
int ObCreateIndexResolver::resolve_index_column_node(
    ParseNode *index_column_node,
    const int64_t index_keyname_value,
    ParseNode *table_option_node,
    ObCreateIndexStmt *crt_idx_stmt,
    const ObTableSchema *tbl_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 8> input_index_columns_name;
  if (OB_ISNULL(index_column_node) || OB_ISNULL(crt_idx_stmt) || OB_ISNULL(tbl_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_column_node), KP(crt_idx_stmt), KP(tbl_schema));
  } else if (T_INDEX_COLUMN_LIST != index_column_node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to check node type", K(ret));
  } else {
    if (OB_FAIL(add_new_indexkey_for_oracle_temp_table())) {
      SQL_RESV_LOG(WARN, "add session id key failed", K(ret));
    }
    bool cnt_func_index = false;
    for (int32_t i = 0; OB_SUCC(ret) && i < index_column_node->num_child_; ++i) {
      ParseNode *col_node = index_column_node->children_[i];
      ObColumnSortItem sort_item;
      if (OB_UNLIKELY(NULL == col_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column node is null", K(ret));
      } else if (T_SORT_COLUMN_KEY != col_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to check node type", K(ret));
      } else {
        //如果此node类型不是identifier,那么认为是函数索引.
        if (col_node->children_[0]->type_ != T_IDENT) {
          sort_item.is_func_index_ = true;
          cnt_func_index = true;
        }
        sort_item.column_name_.assign_ptr(const_cast<char *>(col_node->children_[0]->str_value_),
                                          static_cast<int32_t>(col_node->children_[0]->str_len_));
        bool is_multivalue_index = false;
        if (OB_FAIL(ObMulValueIndexBuilderUtil::is_multivalue_index_type(sort_item.column_name_,
                                                                         is_multivalue_index))) {
          LOG_WARN("failed to resolve index type", K(ret));
        } else if (is_multivalue_index) {
          // not support dynamic create multi-value index
          // todo: weiyouchao.wyc
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support dynaimic create multivlaue index", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "not support dynaimic create multivlaue index");
        }
      }
      // 前缀索引的前缀长度
      if (OB_FAIL(ret)) {
      } else if (NULL != col_node->children_[1]) {
        sort_item.prefix_len_ = static_cast<int32_t>(col_node->children_[1]->value_);
        if (0 == sort_item.prefix_len_) {
          ret = OB_KEY_PART_0;
          LOG_WARN("index prefix len invalid", K(ret), "prefix_len", sort_item.prefix_len_);
          LOG_USER_ERROR(OB_KEY_PART_0, sort_item.column_name_.length(), sort_item.column_name_.ptr());
        }
      } else {
        sort_item.prefix_len_ = 0;
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (index_keyname_ == FTS_KEY) {
        if (!GCONF._enable_add_fulltext_index_to_existing_table) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("build fulltext index afterward is experimental feature", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "experimental feature: build fulltext index afterward");
        } else if (OB_FAIL(resolve_fts_index_constraint(*tbl_schema,
                                                 sort_item.column_name_,
                                                 index_keyname_value))) {
          SQL_RESV_LOG(WARN, "check fts index constraint fail",K(ret),
              K(sort_item.column_name_));
        }
      } else { // spatial index, NOTE resolve_spatial_index_constraint() will set index_keyname
        bool is_explicit_order = (NULL != col_node->children_[2]
            && 1 != col_node->children_[2]->is_empty_);
        if (OB_FAIL(resolve_spatial_index_constraint(*tbl_schema, sort_item.column_name_,
            index_column_node->num_child_, index_keyname_value, is_explicit_order, sort_item.is_func_index_))) {
          LOG_WARN("fail to resolve spatial index constraint", K(ret), K(sort_item.column_name_));
        }
      }

      // 索引排序方式
      if (OB_FAIL(ret)) {
      } else if (is_oracle_mode() && col_node->children_[2]
                 && col_node->children_[2]->type_ == T_SORT_DESC) {
        // sort_item.order_type_ = common::ObOrderType::DESC;
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support desc index now", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create desc index");
      } else {
        //兼容mysql5.7, 降序索引不生效且不报错
        sort_item.order_type_ = common::ObOrderType::ASC;
      }

      if (OB_FAIL(ret)) {
        //do nothing
      } else if (col_node->num_child_ <= 3) {
        //no id specified, do nothing
      } else if (col_node->children_[3] &&
                 col_node->children_[3]->type_ == T_COLUMN_ID) {
        ParseNode *id_node = col_node->children_[3];
        bool is_sync_ddl_user = false;
        if (id_node->num_child_ != 1
            || OB_ISNULL(id_node->children_[0])
            || T_INT != id_node->children_[0]->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid syntax. a column number expected after id", K(ret));
        } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
          LOG_WARN("Failed to check sync_ddl_user", K(ret));
        } else if (!is_sync_ddl_user) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("Only support for sync ddl user to specify column id", K(ret), K(session_info_->get_user_name()));
        } else {
          sort_item.column_id_ = static_cast<int32_t>(id_node->children_[0]->value_);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(add_sort_column(sort_item))) {
        LOG_WARN("fail to add index column", K(ret));
      } else { /*do nothing*/ }
      if (OB_SUCC(ret) && lib::is_oracle_mode()) {
        if (OB_FAIL(input_index_columns_name.push_back(sort_item.column_name_))) {
          SQL_RESV_LOG(WARN, "add column name to input_index_columns_name failed",K(sort_item.column_name_), K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && lib::is_mysql_mode() && cnt_func_index) {
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

    if (OB_SUCC(ret) && cnt_func_index) {
      if (OB_UNLIKELY(tbl_schema->mv_container_table())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("use functional index on materialized view not supported", K(ret), KPC(tbl_schema));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "use functional index on materialized view");
      }
    }

    // In oracle mode, we need to check if the new index is on the same cols with old indexes.
    CHECK_COMPATIBILITY_MODE(session_info_);
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      bool has_other_indexes_on_same_cols = true;
      SMART_VAR(ObCreateIndexArg, create_index_arg) {
        if (OB_ISNULL(stmt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema checker or stmt is NULL", K(ret));
        } else if (OB_FAIL(create_index_arg.assign(static_cast<ObCreateIndexStmt*>(stmt_)->get_create_index_arg()))){
          SQL_RESV_LOG(WARN, "fail to assign create index arg", K(ret));
        }
        if (OB_SUCC(ret)) {
          bool has_same_index_name = false;
          if (OB_FAIL(check_index_name_duplicate(*tbl_schema,
                                                create_index_arg,
                                                *schema_checker_,
                                                has_same_index_name))) {
            SQL_RESV_LOG(WARN, "check index name duplicate failed", K(ret));
          } else if (has_same_index_name) {
            ret = OB_OBJ_ALREADY_EXIST;
            SQL_RESV_LOG(WARN, "index name is already used by an existing index", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_indexes_on_same_cols(*tbl_schema,
                                                create_index_arg,
                                                *schema_checker_,
                                                has_other_indexes_on_same_cols))) {
            SQL_RESV_LOG(WARN, "check indexes on same cols failed", K(ret));
          } else if (has_other_indexes_on_same_cols) {
            ret = OB_ERR_COLUMN_LIST_ALREADY_INDEXED;
            SQL_RESV_LOG(WARN, "has other indexes on the same cols", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          bool is_pk_idx_on_same_cols = false;
          if (OB_FAIL(ObResolverUtils::check_pk_idx_duplicate(*tbl_schema,
                                                              create_index_arg,
                                                              input_index_columns_name,
                                                              is_pk_idx_on_same_cols))) {
            SQL_RESV_LOG(WARN, "check if pk and idx on same cols failed", K(ret));
          } else if (is_pk_idx_on_same_cols) {
            ret = OB_ERR_COLUMN_LIST_ALREADY_INDEXED;
            SQL_RESV_LOG(WARN, "uk and pk is duplicate", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// child 3 of root node, resolve index option node
int ObCreateIndexResolver::resolve_index_option_node(
    ParseNode *index_option_node,
    ObCreateIndexStmt *crt_idx_stmt,
    const ObTableSchema *tbl_schema,
    bool is_partitioned)
{
  int ret = OB_SUCCESS;
  const bool is_index = true;
  if (OB_ISNULL(crt_idx_stmt) || OB_ISNULL(tbl_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(crt_idx_stmt), KP(tbl_schema), K(ret));
  } else if (NULL != index_option_node) {
    if (OB_FAIL(resolve_table_options(index_option_node, is_index))) {
      LOG_WARN("fail to resolve table options", K(ret));
    }

    // index table dop
    if (OB_SUCC(ret)) {
      // 如果没有指定dop，table_dop_的默认值为1
      crt_idx_stmt->set_index_dop(table_dop_);
    }

    // block_size
    if (OB_SUCC(ret)) {
      if(T_TABLE_OPTION_LIST != index_option_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse node", K(ret));
      } else {
        int64_t num = index_option_node->num_child_;
        for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
          ParseNode *option_node = NULL;
          if (OB_ISNULL(option_node = index_option_node->children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "node is null", K(ret));
          } else if (T_BLOCK_SIZE == option_node->type_) {
            is_spec_block_size = true;
            break;
          }
        }
      }
    }
  }

  // storing column
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < store_column_names_.count(); ++i) {
      if (OB_FAIL(crt_idx_stmt->add_storing_column(store_column_names_.at(i)))) {
        LOG_WARN("fail to add store column to create index stmt", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < hidden_store_column_names_.count(); ++i) {
      if (OB_FAIL(crt_idx_stmt->add_hidden_storing_column(hidden_store_column_names_.at(i)))) {
        LOG_WARN("fail to add store column to create index stmt", K(ret));
      }
    }
  }

  // in mysql mode, index and data table are always in the same tablespace
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    tablespace_id_ = tbl_schema->get_tablespace_id();
    if (OB_FAIL(set_encryption_name(tbl_schema->get_encryption_str()))) {
      LOG_WARN("fail to set encryption name to create index stmt", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (has_index_using_type_) {
      crt_idx_stmt->set_index_using_type(index_using_type_);
    }
    if (OB_FAIL(set_table_option_to_stmt(is_partitioned))) {
      LOG_WARN("fail to set table option to stmt", K(ret));
    } else if (tbl_schema->is_partitioned_table()
        && INDEX_TYPE_SPATIAL_GLOBAL == crt_idx_stmt->get_create_index_arg().index_type_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "spatial global index");
    }
  }
  return ret;
}

// child 4 of root node, resolve index method node
int ObCreateIndexResolver::resolve_index_method_node(
    ParseNode *index_method_node,
    ObCreateIndexStmt *crt_idx_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == index_method_node)
      || OB_UNLIKELY(NULL == crt_idx_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_method_node), KP(crt_idx_stmt));
  } else {
    if (T_USING_HASH == index_method_node->type_) {
      crt_idx_stmt->set_index_using_type(USING_HASH);
    } else {
      crt_idx_stmt->set_index_using_type(USING_BTREE);
    }
  }
  return ret;
}

/**
 * @brief 将session中的一些信息添入到arg中
 * @param session 当前session
 * @param crt_idx_stmt stmt
 * @return ret
 */
int ObCreateIndexResolver::fill_session_info_into_arg(const sql::ObSQLSessionInfo *session,
                                                      ObCreateIndexStmt *crt_idx_stmt)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session));
  CK (OB_NOT_NULL(crt_idx_stmt));
  if (OB_SUCC(ret)) {
    ObCreateIndexArg &arg = crt_idx_stmt->get_create_index_arg();
    arg.nls_date_format_ = session->get_local_nls_date_format();
    arg.nls_timestamp_format_ = session->get_local_nls_timestamp_format();
    arg.nls_timestamp_tz_format_ = session->get_local_nls_timestamp_tz_format();
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(session->get_effective_tenant_id(), tenant_data_version))) {
      LOG_WARN("get tenant data version failed", K(ret));
    } else if (tenant_data_version < DATA_VERSION_4_2_2_0) {
      //do nothing
    } else if (OB_FAIL(arg.local_session_var_.load_session_vars(session))) {
      LOG_WARN("fail to fill session info into local_session_var", K(ret));
    }
  }
  return ret;
}

int ObCreateIndexResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateIndexStmt *crt_idx_stmt = NULL;
  ParseNode &parse_node = const_cast<ParseNode &>(parse_tree);
  ParseNode *if_not_exist_node = NULL;
  const ObTableSchema *tbl_schema = NULL;
  const ObTableSchema *data_tbl_schema = NULL;
  bool has_synonym = false;
  ObString new_db_name;
  ObString new_tbl_name;

  if (OB_UNLIKELY(T_CREATE_INDEX != parse_tree.type_)
      || OB_UNLIKELY(CREATE_INDEX_CHILD_NUM != parse_tree.num_child_)
      || OB_UNLIKELY(NULL == parse_tree.children_[0])
      || OB_UNLIKELY(NULL == parse_tree.children_[1])
      || OB_UNLIKELY(NULL == parse_tree.children_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(parse_tree.type_), K(parse_tree.num_child_),
             "index_name_node", parse_tree.children_[0],
             "table_name_node", parse_tree.children_[1],
             "index_column_node", parse_tree.children_[2]);
  } else if (OB_UNLIKELY(NULL == (crt_idx_stmt = create_stmt<ObCreateIndexStmt>()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("create index stmt failed", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", K(ret));
  } else {
    stmt_ = crt_idx_stmt;
    if_not_exist_node = parse_tree.children_[7];
  }

  // 将session中的信息添写到 stmt 的 arg 中
  // 包括 nls_xx_format
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_session_info_into_arg(session_info_, crt_idx_stmt))) {
      LOG_WARN("fill_session_info_into_arg failed", K(ret));
    }
  }

  if (FAILEDx(resolve_index_table_name_node(parse_node.children_[1], crt_idx_stmt))) {
    LOG_WARN("fail to resolve index table name node", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema_with_synonym(session_info_->get_effective_tenant_id(),
                                                       crt_idx_stmt->get_database_name(),
                                                       crt_idx_stmt->get_table_name(),
                                                       false/*not index table*/,
                                                       has_synonym,
                                                       new_db_name,
                                                       new_tbl_name,
                                                       tbl_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(crt_idx_stmt->get_create_index_arg().database_name_),
          to_cstring(crt_idx_stmt->get_create_index_arg().table_name_));
      LOG_WARN("table not exist", K(ret),
          "database_name", crt_idx_stmt->get_create_index_arg().database_name_,
          "table_name", crt_idx_stmt->get_create_index_arg().table_name_);
    } else {
      LOG_WARN("fail to get table schema", K(ret));
    }
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (tbl_schema->is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation on external table");
  } else if (tbl_schema->is_mlog_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create index on materialized view log is not supported",
        KR(ret), K(tbl_schema->get_table_name()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create index on materialized view log is");
  } else if (tbl_schema->is_materialized_view()) {
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    const uint64_t mv_container_table_id = tbl_schema->get_data_table_id();
    const ObTableSchema *mv_container_table_schema = nullptr;
    ObString mv_container_table_name;
    if (OB_FAIL(get_mv_container_table(tenant_id,
                                       mv_container_table_id,
                                       mv_container_table_schema,
                                       mv_container_table_name))) {
      LOG_WARN("fail to get mv container table", KR(ret), K(tenant_id), K(mv_container_table_id));
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_ERR_UNEXPECTED; // rewrite errno
      }
    } else {
      is_oracle_temp_table_ = (tbl_schema->is_oracle_tmp_table());
      ObTableSchema &index_schema = crt_idx_stmt->get_create_index_arg().index_schema_;
      index_schema.set_tenant_id(session_info_->get_effective_tenant_id());
      crt_idx_stmt->set_table_id(mv_container_table_schema->get_table_id());
      crt_idx_stmt->set_table_name(mv_container_table_name);
      data_tbl_schema = mv_container_table_schema;
    }
  } else {
    is_oracle_temp_table_ = (tbl_schema->is_oracle_tmp_table());
    ObTableSchema &index_schema = crt_idx_stmt->get_create_index_arg().index_schema_;
    index_schema.set_tenant_id(session_info_->get_effective_tenant_id());
    crt_idx_stmt->set_table_id(tbl_schema->get_table_id());
    data_tbl_schema = tbl_schema;
  }
  if (OB_SUCC(ret) && has_synonym) {
    ObString tmp_new_db_name;
    ObString tmp_new_tbl_name;
    // related issue :
    if (OB_FAIL(deep_copy_str(new_db_name, tmp_new_db_name))) {
      LOG_WARN("failed to deep copy new_db_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(new_tbl_name, tmp_new_tbl_name))) {
      LOG_WARN("failed to deep copy new_tbl_name", K(ret));
    } else {
      crt_idx_stmt->set_database_name(tmp_new_db_name);
      crt_idx_stmt->set_table_name(tmp_new_tbl_name);
      crt_idx_stmt->set_name_generated_type(GENERATED_TYPE_USER);
    }
  }

  if (FAILEDx(resolve_index_name_node(parse_node.children_[0], crt_idx_stmt))) {
    LOG_WARN("fail to resolve index name node", K(ret));
  } else if (OB_FAIL(resolve_index_column_node(parse_node.children_[2],
                                               parse_tree.children_[0]->value_,
                                               parse_tree.children_[3],
                                               crt_idx_stmt,
                                               data_tbl_schema))) {
    LOG_WARN("fail to resolve index column node", K(ret));
  } else if (NULL != parse_node.children_[4]
      && OB_FAIL(resolve_index_method_node(parse_node.children_[4], crt_idx_stmt))) {
    LOG_WARN("fail to resolve index method node", K(ret));
  } else if (OB_FAIL(resolve_index_option_node(parse_node.children_[3],
                                               crt_idx_stmt,
                                               data_tbl_schema,
                                               NULL != parse_node.children_[5]))) {
    LOG_WARN("fail to resolve index option node", K(ret));
  } else if (global_ && OB_FAIL(generate_global_index_schema(crt_idx_stmt))) {
    LOG_WARN("fail to generate index schema", K(ret));
  } else {
    if (NULL != parse_node.children_[5]) {
      // 0: 普通分区node // 1: 垂直分区node, 不支持建全局索引时指定垂直分区
      if (1 != parse_node.children_[5]->num_child_
          || T_PARTITION_OPTION != parse_node.children_[5]->type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "column vertical partition for index table");
        LOG_WARN("node is invalid", K(ret));
      } else if (NULL == parse_node.children_[5]->children_[0]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret));
      } else {
        ParseNode *index_partition_node = parse_node.children_[5]->children_[0]; // 普通分区partition node
        if (OB_FAIL(resolve_index_partition_node(index_partition_node, crt_idx_stmt))) {
          LOG_WARN("fail to resolve index partition node", K(ret));
        }
      }
    }

    // index column_group
    if (OB_FAIL(ret)) {
    } else if (NULL != parse_node.children_[6]) {
      if (T_COLUMN_GROUP != parse_node.children_[6]->type_ || parse_node.children_[6]->num_child_ <= 0) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(parse_node.children_[6]->type_), K(parse_node.children_[6]->num_child_));
      } else if (OB_ISNULL(parse_node.children_[6]->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret));
      } else if (OB_FAIL(resolve_index_column_group(parse_node.children_[6], crt_idx_stmt->get_create_index_arg()))) {
        SQL_RESV_LOG(WARN, "resolve index column group failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      crt_idx_stmt->set_if_not_exists(NULL != if_not_exist_node);
      // 设置block size, 如果未指定block size，则使用主表block size
      // 否则使用默认block_size
      if (!is_spec_block_size) {
        ObCreateIndexArg &index_arg = crt_idx_stmt->get_create_index_arg();
        index_arg.index_option_.block_size_ = tbl_schema->get_block_size();
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ParseNode *parallel_node = parse_tree.children_[8];
    if (OB_FAIL(resolve_hints(parse_tree.children_[8], *crt_idx_stmt, *tbl_schema))) {
      LOG_WARN("resolve hints failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            crt_idx_stmt->get_database_name(),
                                            crt_idx_stmt->get_table_id(),
                                            static_cast<uint64_t>(ObObjectType::TABLE),
                                            stmt::T_CREATE_INDEX,
                                            session_info_->get_enable_role_array()));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(crt_idx_stmt->get_create_index_arg().
                based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(
                    tbl_schema->get_table_id(),
                    TABLE_SCHEMA,
                    tbl_schema->get_schema_version())))) {
      SQL_RESV_LOG(WARN, "failed to add based_schema_object_info to arg",
                   K(ret), K(tbl_schema->get_table_id()),
                   K(tbl_schema->get_schema_version()));
    }
  }
  DEBUG_SYNC(HANG_BEFORE_RESOLVER_FINISH);

  return ret;
}

int ObCreateIndexResolver::add_sort_column(const ObColumnSortItem &sort_column)
{
  int ret = OB_SUCCESS;
  ObCreateIndexStmt *create_index_stmt = NULL;
  ObColumnNameWrapper column_key(sort_column.column_name_, sort_column.prefix_len_);
  bool check_prefix_len = false;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker or stmt is NULL", K(ret));
  } else {
    create_index_stmt = static_cast<ObCreateIndexStmt*>(stmt_);
  }
  if (OB_FAIL(ret)) {
    //empty
  } else if (is_column_exists(sort_column_array_, column_key, check_prefix_len)) {
    ret = OB_ERR_COLUMN_DUPLICATE;
    LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, sort_column.column_name_.length(), sort_column.column_name_.ptr());
    LOG_WARN("Duplicate sort column name", K(sort_column), K(ret));
   } else if (OB_FAIL(sort_column_array_.push_back(column_key))) {
    LOG_WARN("failed to push back column key", K(sort_column), K(ret));
  } else if (OB_FAIL(create_index_stmt->add_sort_column(sort_column))) {
    LOG_WARN("add sort column to create index stmt failed", K(sort_column), K(ret));
  }
  return ret;
}

int ObCreateIndexResolver::set_table_option_to_stmt(bool is_partitioned)
{
  int ret = OB_SUCCESS;
  ObCreateIndexStmt *create_index_stmt = static_cast<ObCreateIndexStmt*>(stmt_);
  if (OB_ISNULL(create_index_stmt) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create_index_stmt can not be null", K(ret));
  } else if (is_oracle_temp_table_ && GLOBAL_INDEX == index_scope_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Create global index on temp table");
  } else {
    ObCreateIndexArg &index_arg = create_index_stmt->get_create_index_arg();
    if (is_oracle_temp_table_) { //oracle临时表是系统内部转成了分区表, 索引创建时全部是local
      index_scope_ = LOCAL_INDEX;
    }
    index_arg.tenant_id_ = session_info_->get_effective_tenant_id();
    index_arg.index_option_.index_status_= INDEX_STATUS_UNAVAILABLE;
    if (NOT_SPECIFIED == index_scope_) {
      // partitioned index must be global,
      // MySQL default index mode is local,
      // and Oracle default index mode is global
      global_ = is_partitioned || lib::is_oracle_mode();
    } else {
      global_ = (GLOBAL_INDEX == index_scope_);
    }
    if (UNIQUE_KEY == index_keyname_) {
      if (global_) {
        index_arg.index_type_ = INDEX_TYPE_UNIQUE_GLOBAL;
      } else {
        index_arg.index_type_ = INDEX_TYPE_UNIQUE_LOCAL;
      }
    } else if (NORMAL_KEY == index_keyname_) {
      if (global_) {
        index_arg.index_type_ = INDEX_TYPE_NORMAL_GLOBAL;
      } else {
        index_arg.index_type_ = INDEX_TYPE_NORMAL_LOCAL;
      }
    } else if (SPATIAL_KEY == index_keyname_) {
      if (global_) {
        index_arg.index_type_ = INDEX_TYPE_SPATIAL_GLOBAL;
      } else {
        index_arg.index_type_ = INDEX_TYPE_SPATIAL_LOCAL;
      }
    } else if (FTS_KEY == index_keyname_) {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
      LOG_WARN("get tenant data version failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_3_2_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant data version is less than 4.3.2, create fulltext index on existing table not supported", K(ret), K(tenant_data_version));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.2, fulltext index");
      } else if (global_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support global fts index now", K(ret));
      } else {
        // set type to fts_index_aux first, append other fts arg later
        index_arg.index_type_ = INDEX_TYPE_FTS_INDEX_LOCAL;
      }
    }
    index_arg.data_table_id_ = data_table_id_;
    index_arg.index_table_id_ = index_table_id_;
    index_arg.index_option_.block_size_ = block_size_;
    index_arg.index_option_.replica_num_ = replica_num_;
    index_arg.index_option_.use_bloom_filter_ = use_bloom_filter_;
    index_arg.index_option_.progressive_merge_num_ = progressive_merge_num_;
    index_arg.index_option_.index_attributes_set_ = index_attributes_set_;
    index_arg.with_rowid_ = with_rowid_;
    index_arg.index_schema_.set_data_table_id(data_table_id_);
    index_arg.index_schema_.set_table_id(index_table_id_);
    index_arg.sql_mode_ = session_info_->get_sql_mode();
    create_index_stmt->set_comment(comment_);
    create_index_stmt->set_tablespace_id(tablespace_id_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_index_stmt->set_encryption_str(encryption_))) {
      LOG_WARN("fail to set encryption str", K(ret));
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
