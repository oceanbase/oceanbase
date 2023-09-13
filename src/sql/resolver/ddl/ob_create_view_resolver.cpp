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

#define USING_LOG_PREFIX  SQL_RESV

#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h" // share CREATE TABLE stmt
#include "sql/resolver/dml/ob_select_stmt.h" // resolve select clause
#include "sql/resolver/dml/ob_dml_stmt.h" // PartExprItem
#include "sql/ob_sql_context.h"
#include "sql/ob_select_stmt_printer.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ddl/ob_create_table_resolver.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{
ObCreateViewResolver::ObCreateViewResolver(ObResolverParams &params) : ObDDLResolver(params)
{
}

ObCreateViewResolver::~ObCreateViewResolver()
{
}

int ObCreateViewResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt *stmt = NULL;
  bool is_sync_ddl_user = false;
  if (OB_UNLIKELY(T_CREATE_VIEW != parse_tree.type_)
      || OB_UNLIKELY(ROOT_NUM_CHILD != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse_tree", K(parse_tree.type_), K(parse_tree.num_child_), K(ret));
  } else if (OB_ISNULL(parse_tree.children_) || OB_ISNULL(parse_tree.children_[VIEW_NODE])
             || OB_ISNULL(allocator_) || OB_ISNULL(session_info_)
             || OB_ISNULL(params_.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parse_tree.children_),
                                    K(parse_tree.children_[VIEW_NODE]),
                                    K(allocator_), K(session_info_),
                                    K(params_.query_ctx_));
  } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
    LOG_WARN("Failed to check sync_dll_user", K(ret));
  } else if (OB_UNLIKELY(NULL == (stmt = create_stmt<ObCreateTableStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create view stmt failed", K(ret));
  } else {
    ObString db_name;
    ObString view_name;
    char *dblink_name_ptr = NULL;
    int32_t dblink_name_len = 0;
    ObString view_define;
    const bool is_force_view = NULL != parse_tree.children_[FORCE_VIEW_NODE];
    stmt->set_allocator(*allocator_);
    stmt_ = stmt;
    stmt->set_is_view_stmt(true);
    ObCreateTableArg &create_arg = stmt->get_create_table_arg();
    ObTableSchema &table_schema = create_arg.schema_;
    ObSelectStmt *select_stmt = NULL;
    // 原来兼容mysql，先resolve view_definition 再 resolve view_name
    // resolve view_name不依赖view_definition， 但是resolve view_definition检查循环依赖时需要view_name，
    // 因此交换两个resolve的位置
    // resolve view name; create view [ or replace] view <view_name>[column_list] [table_id]
    create_arg.if_not_exist_ = NULL != parse_tree.children_[IF_NOT_EXISTS_NODE]
                               || 1 == parse_tree.reserved_;
    create_arg.is_alter_view_ = (1 == parse_tree.reserved_);
    table_schema.set_force_view(is_force_view);
    table_schema.set_tenant_id(session_info_->get_effective_tenant_id());
    table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
    table_schema.set_define_user_id(session_info_->get_priv_user_id());
    table_schema.set_view_created_method_flag((ObViewCreatedMethodFlag)(create_arg.if_not_exist_ || is_force_view));
    ParseNode *table_id_node = parse_tree.children_[TABLE_ID_NODE];
    const int64_t max_user_table_name_length = lib::is_oracle_mode()
                  ? OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    bool perserve_lettercase = false; // lib::is_oracle_mode() ? true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
    ObArray<ObString> column_list;
    bool has_dblink_node = false;
    if (OB_FAIL(resolve_table_relation_node(parse_tree.children_[VIEW_NODE],
                                            view_name, db_name,
                                            false, false, &dblink_name_ptr, &dblink_name_len, &has_dblink_node))) {
      LOG_WARN("failed to resolve table relation node!", K(ret));
    } else if (has_dblink_node) { //don't care about dblink_name_len
      ret = OB_ERR_MISSING_KEYWORD;
      LOG_WARN("missing keyword when create view", K(ret));
      LOG_USER_ERROR(OB_ERR_MISSING_KEYWORD);
    } else if (OB_FAIL(normalize_table_or_database_names(view_name))) {
      LOG_WARN("fail to normalize table name", K(view_name), K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_, db_name,
                                        stmt->get_non_const_db_name()))) {
      LOG_WARN("failed to deep copy database name", K(ret), K(db_name));
    } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
      LOG_WARN("fail to get name case mode", K(ret), K(mode));
    } else if (FALSE_IT(perserve_lettercase = lib::is_oracle_mode() ? true : (mode != OB_LOWERCASE_AND_INSENSITIVE))) {
    } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(CS_TYPE_UTF8MB4_GENERAL_CI, perserve_lettercase, view_name))) {
      LOG_WARN("fail to check and convert view_name", K(ret), K(view_name));
    } else if (OB_FAIL(table_schema.set_table_name(view_name))) {
      LOG_WARN("fail to set table_name", K(view_name), K(ret));
    } else if (OB_UNLIKELY(NULL != table_id_node && (T_TABLE_ID != table_id_node->type_
                                                     || 1 != table_id_node->num_child_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to resolve table_id", K(ret));
    } else {
      table_schema.set_table_id(table_id_node ?
                                static_cast<uint64_t>(table_id_node->children_[0]->value_) :
                                OB_INVALID_ID);
    }

    if (OB_SUCC(ret)) {
      // resolve select stmt: create ... as <select ...>
      ObViewTableResolver view_table_resolver(params_, stmt->get_database_name(),
                                              table_schema.get_table_name());
      view_table_resolver.params_.is_from_create_view_ = true;
      view_table_resolver.params_.is_specified_col_name_ = parse_tree.children_[VIEW_COLUMNS_NODE] != NULL;
      view_table_resolver.set_current_view_level(1);
      view_table_resolver.set_is_create_view(true);
      // set ObViewSchema.materialized_ in RS
      view_table_resolver.set_materialized(parse_tree.children_[MATERIALIZED_NODE] ? true : false);
      ParseNode *select_stmt_node = parse_tree.children_[SELECT_STMT_NODE];
      ParseNode *view_columns_node = parse_tree.children_[VIEW_COLUMNS_NODE];
      ObString sql_str(select_stmt_node->str_len_, select_stmt_node->str_value_);
      view_define = sql_str;
      if (OB_ISNULL(select_stmt_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select_stmt_node should not be NULL", K(ret));
      } else if (OB_UNLIKELY(T_SELECT != select_stmt_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node type is not T_SELECT", K(select_stmt_node->type_), K(ret));
      } else if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                           *allocator_, session_info_->get_dtc_params(), view_define,
                           ObCharset::COPY_STRING_ON_SAME_CHARSET))) {
        LOG_WARN("write view define failed", K(ret));
      } else if (OB_FAIL(view_table_resolver.resolve(*select_stmt_node))) {
        if (is_force_view) {
          // create force view, ignore resolve error
          if (OB_FAIL(try_add_error_info(ret, create_arg.error_info_))) {
            LOG_WARN("failed to add error info to for force view", K(ret));
          }
        } else if (is_sync_ddl_user && session_info_->is_inner()
                   && !session_info_->is_user_session()
                   && (OB_TABLE_NOT_EXIST == ret || OB_ERR_BAD_FIELD_ERROR == ret
                       || OB_ERR_KEY_DOES_NOT_EXISTS == ret)) {
          // ret: OB_TABLE_NOT_EXIST || OB_ERR_BAD_FIELD_ERROR
          // resolve select_stmt_mode可能会出现表或者列不存在，这里做规避
          LOG_WARN("resolve select in create view failed", K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("resolve select in create view failed", K(select_stmt_node), K(ret));
        }
      } else if (OB_FAIL(params_.query_ctx_->query_hint_.init_query_hint(params_.allocator_,
                                                                         params_.session_info_,
                                                          view_table_resolver.get_select_stmt()))) {
        LOG_WARN("failed to init query hint.", K(ret));
      }
      // specify view related flags
      if (table_schema.is_sys_table()) {
        table_schema.set_table_type(SYSTEM_VIEW);
      } else if (table_schema.is_user_table()) {
        table_schema.set_table_type(USER_VIEW);
      }
      if (OB_FAIL(ret)) {
      } else if (!is_force_view && !is_sync_ddl_user && OB_FAIL(resolve_column_list(parse_tree.children_[VIEW_COLUMNS_NODE], column_list, table_schema))) {
        LOG_WARN("fail to resolve view columns", K(ret));
      } else if (OB_ISNULL(select_stmt = view_table_resolver.get_select_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(view_table_resolver.get_select_stmt()));
      } else if (OB_ISNULL(select_stmt->get_real_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get real stmt", K(ret), K(*select_stmt));
      } else if (OB_FAIL(check_view_columns(*select_stmt, view_columns_node,
                                            create_arg.error_info_, is_force_view))) {
        LOG_WARN("failed to check view columns", K(ret));
      } else if (OB_FAIL(add_column_infos(session_info_->get_effective_tenant_id(), *select_stmt, table_schema, *allocator_, *session_info_, column_list))) {
        LOG_WARN("failed to add column infos", K(ret));
      } else if (OB_FAIL(collect_dependency_infos(params_.query_ctx_, create_arg))) {
        LOG_WARN("failed to collect dependency infos", K(ret));
      } else {
        bool is_updatable = true;
        ViewCheckOption check_option = VIEW_CHECK_OPTION_NONE;
        if (NULL != parse_tree.children_[WITH_OPT_NODE]) {
          if (is_oracle_mode()) {
            if (1 == parse_tree.children_[WITH_OPT_NODE]->value_) {
              // WITH READ ONLY
              is_updatable = false;
              table_schema.set_read_only(true);
            } else if (2 == parse_tree.children_[WITH_OPT_NODE]->value_) {
              check_option = VIEW_CHECK_OPTION_CASCADED;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected parser node value", K(ret),
                        K(parse_tree.children_[WITH_OPT_NODE]->value_));
            }
          } else {
            int64_t node_value = parse_tree.children_[WITH_OPT_NODE]->value_;
            if (OB_UNLIKELY(node_value < 0 ||
                  node_value >= static_cast<int64_t>(VIEW_CHECK_OPTION_MAX))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected parser node value", K(ret),
                        K(parse_tree.children_[WITH_OPT_NODE]->value_));
            } else {
              check_option = static_cast<ViewCheckOption>(node_value);
            }
          }
        }
        ObViewSchema &view_schema = table_schema.get_view_schema();
        view_schema.set_view_definition(view_define);
        view_schema.set_view_check_option(check_option);
        view_schema.set_view_is_updatable(is_updatable);
        ObCharsetType cs_client_type = CHARSET_INVALID;
        ObCollationType coll_connection_type = CS_TYPE_INVALID;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(session_info_->get_character_set_client(cs_client_type))) {
          LOG_WARN("get character set client failed", K(ret));
        } else if (OB_FAIL(session_info_->get_collation_connection(coll_connection_type))) {
          LOG_WARN("get collation connection failed", K(ret));
        } else {
          bool with_check_option = VIEW_CHECK_OPTION_NONE != check_option;
          view_schema.set_character_set_client(cs_client_type);
          view_schema.set_collation_connection(coll_connection_type);
          // check whether view is allowed to be with check option.
          // In mysql mode, only support create view as ... with check option in syntax.
          // In oracle mode, support create view v as (select * from (select * from t with check option))
          // so we have to check in oracle mode even if check_option of view_schema is NONE.
          if ((with_check_option || lib::is_oracle_mode())
              && OB_FAIL(ObResolverUtils::view_with_check_option_allowed(select_stmt,
                                                                          with_check_option))) {
            LOG_WARN("view with check option not allowed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && !is_force_view && !is_sync_ddl_user) {
      // 前面用建view的sql直接设置过table_schema.set_view_definition
      // 基线备份时create view必须都用show create view里面的view definition
      // create force view use origin view_define
      ObString expanded_view;
      if (OB_FAIL(stmt_print(select_stmt, 0 == column_list.count() ? NULL : &column_list,
                                    expanded_view))) {
        LOG_WARN("fail to expand view definition", K(ret));
      } else if (OB_FAIL(table_schema.set_view_definition(expanded_view))) {
        LOG_WARN("fail to set view definition", K(expanded_view), K(ret));
      }
    }

    // 权限添加需要拿到完整stmt信息，慎重调整本段代码位置
    if (OB_SUCC(ret) && !(is_sync_ddl_user && session_info_->is_inner())
        && OB_FAIL(check_privilege_needed(*stmt, *select_stmt, is_force_view))) {
      LOG_WARN("fail to check privilege needed", K(ret));
    }
  }

  return ret;
}

int ObCreateViewResolver::try_add_error_info(const uint64_t error_number,
                                             share::schema::ObErrorInfo &error_info)
{
  int ret = OB_SUCCESS;
  if (ERROR_STATUS_HAS_ERROR == error_info.get_error_status()) {
    /* do nothing */
  } else {
    ObString err_txt(common::ob_oracle_strerror(error_number));
    error_info.set_error_number(error_number);
    error_info.set_error_status(ERROR_STATUS_HAS_ERROR);
    if (err_txt.empty()) {
      ObString err_unknown("unknown error message. the system doesn't declare this msg.");
      error_info.set_text_length(err_unknown.length());
      ret = error_info.set_text(err_unknown);
    } else {
      error_info.set_text_length(err_txt.length());
      ret = error_info.set_text(err_txt);
    }
    LOG_USER_WARN(OB_ERR_RESOLVE_SQL);
  }
  return ret;
}

int ObCreateViewResolver::check_view_columns(ObSelectStmt &select_stmt,
                                             ParseNode *view_columns_node,
                                             share::schema::ObErrorInfo &error_info,
                                             const bool is_force_view)
{
  int ret = OB_SUCCESS;
  // oracle 模式下, create view时要求每一个select expr有明确的别名
  // 1. expr 本身是一个列
  // 2. expr是计算表达式，但是有用户显示指定的别名
  // 3. create veiw (c1,c2,c3) as select，view定义中指定了列名
  // 并且oracle规定view的列名不能重复
  bool is_col_dup = false;
  ObString dup_col_name;
  hash::ObHashSet<ObString> view_col_names;
  int64_t select_item_size = select_stmt.get_select_item_size();
  if (NULL != view_columns_node && view_columns_node->num_child_ > 0) {
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_UNLIKELY(!is_force_view && select_item_size != view_columns_node->num_child_)) {
      ret = OB_ERR_VIEW_WRONG_LIST;
      LOG_WARN("view columns is not equal with select columns", K(select_item_size),
                                                      K(view_columns_node->num_child_));
    } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation_connection", K(ret));
    } else if (OB_FAIL(view_col_names.create(view_columns_node->num_child_))) {
      LOG_WARN("failed to init hashset", K(ret), K(select_stmt.get_select_items().count()));
    } else if (is_force_view && select_item_size != view_columns_node->num_child_) {
      if (OB_FAIL(try_add_error_info(OB_ERR_VIEW_WRONG_LIST, error_info))) {
        LOG_WARN("failed to add error info to for force view", K(ret));
      } else {
        LOG_TRACE("force view columns is not equal with select columns", K(select_item_size),
                                                              K(view_columns_node->num_child_));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_col_dup && i < view_columns_node->num_child_; i++) {
      if (OB_ISNULL(view_columns_node->children_[i])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null children", K(ret), K(view_columns_node->children_[i]));
      } else if (FALSE_IT(dup_col_name = ObString::make_string(view_columns_node->children_[i]->str_value_))) {
      } else if (OB_FAIL(ObCharset::tolower(cs_type, dup_col_name, dup_col_name, *allocator_))) {
        LOG_WARN("fail to lower string", K(ret));
      } else if (OB_HASH_EXIST == (ret = view_col_names.set_refactored(dup_col_name, 0))) {
        is_col_dup = true;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to set hashset", K(ret));
      } else { /* do nothing */ }
    }
  } else if (OB_UNLIKELY(is_force_view && 0 == select_item_size)) {
    if (OB_FAIL(try_add_error_info(OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE, error_info))) {
      LOG_WARN("failed to add error info to for force view", K(ret));
    } else {
      LOG_TRACE("force view must have at least one column that is not invisible",
                                                    K(OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE));
    }
  } else if (OB_FAIL(view_col_names.create(select_item_size))) {
    LOG_WARN("failed to init hashset", K(ret), K(select_item_size));
  } else if (lib::is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_col_dup
                                     && i < select_stmt.get_select_items().count(); i++) {
      SelectItem &select_item = select_stmt.get_select_items().at(i);
      if (OB_ISNULL(select_item.expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null expr in select item", K(ret), K(select_item.expr_));
      } else if (OB_UNLIKELY(!select_item.is_real_alias_ &&
                             T_REF_COLUMN != select_item.expr_->get_expr_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support behavior while creating view in oracle", K(ret),
                                                      K(select_item.is_real_alias_),
                                                      K(select_item.expr_->get_expr_type()),
                                                      K(select_stmt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "expression without a column alias");
      } else if (OB_HASH_EXIST == (ret = view_col_names.set_refactored(select_item.alias_name_, 0))) {// flag = 0不覆盖原有的值
        is_col_dup = true;
        dup_col_name = select_item.alias_name_;
        ret = OB_SUCCESS; // 覆盖错误码
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to set hashset", K(ret), K(select_item.alias_name_));
      } else { /* do nothing */ }
    }
  } else if (lib::is_mysql_mode()) {
    ObArray<int64_t> index_array;
    if (OB_FAIL(check_view_stmt_col_name(select_stmt,
                                         index_array,
                                         view_col_names))) {
      SQL_RESV_LOG(WARN, "check select stmt col name failed", K(ret));
    } else if (OB_FAIL(create_alias_names_auto(index_array, &select_stmt, view_col_names))) {
      SQL_RESV_LOG(WARN, "check and create alias name failed", K(ret), K(index_array));
    }
  }

  if (OB_FAIL(ret) || OB_LIKELY(!is_col_dup)) {
    /* do nothing */
  } else if (lib::is_oracle_mode()) {
    ret = OB_NON_UNIQ_ERROR;
    ObString scope_name = ObString::make_string(get_scope_name(T_FIELD_LIST_SCOPE));
    LOG_USER_ERROR(OB_NON_UNIQ_ERROR,
                    dup_col_name.length(),
                    dup_col_name.ptr(),
                    scope_name.length(),
                    scope_name.ptr());
  } else if (lib::is_mysql_mode()) {
    ret = OB_ERR_COLUMN_DUPLICATE;
    LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, dup_col_name.length(), dup_col_name.ptr());
  } else { /* do nothing */ }
  return ret;
}

// get all tables/view in subquery.
int ObCreateViewResolver::get_sel_priv_tables_in_subquery(const ObSelectStmt *select_stmt,
                                         hash::ObHashMap<int64_t, const TableItem *> &select_tables)
{
  int ret = OB_SUCCESS;
  ObString info_schema(OB_INFORMATION_SCHEMA_NAME);
  if (NULL != select_stmt) {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_table_items().count(); i++) {
      const TableItem *table_item = select_stmt->get_table_item(i);
      const TableItem *dummy_item = NULL;
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_item is NULL ptr", K(ret));
      } else if (!(table_item->is_basic_table() || table_item->is_view_table_) ||
                 (table_item->database_name_.empty() && !table_item->dblink_name_.empty())) {
        /* do nothing */
      } else if (OB_FAIL(select_tables.get_refactored(table_item->ref_id_, dummy_item))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get refactor", K(ret));
        } else {
          ret = OB_SUCCESS;
          bool is_database_name_equal = false;
          if (OB_FAIL(ObResolverUtils::name_case_cmp(session_info_, table_item->database_name_,
                                                     info_schema, OB_TABLE_NAME_CLASS,
                                                     is_database_name_equal))) {
          } else if (is_database_name_equal) {
            //do nothing
          } else if (OB_FAIL(select_tables.set_refactored(table_item->ref_id_, table_item))) {
            LOG_WARN("failed to set refacted", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      // subquery + generated table in child_stmts
      ObSEArray<ObSelectStmt *, 4> child_stmts;
      if (OB_FAIL(select_stmt->get_child_stmts(child_stmts))) {
        LOG_WARN("get child stmt failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
          if (OB_ISNULL(child_stmts.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child stmt is NULL", K(ret));
          } else if (SMART_CALL(get_sel_priv_tables_in_subquery(child_stmts.at(i), select_tables))) {
            LOG_WARN("failed to get need privs in child stmt", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

/* select (expr or sub_query) from (table/view/generated table) elsewhere (expr or subquery)
 * - tables will appear at select expr, select sub_query, from item, elsewhere expr
 * - only pure column ref expr to a table/view in select expr need any privileges
 * - select sub_query, from item, elsewhere expr need select privileges
 */
int ObCreateViewResolver::get_need_priv_tables(ObSelectStmt &root_stmt,
                                         hash::ObHashMap<int64_t, const TableItem *> &select_tables,
                                         hash::ObHashMap<int64_t, const TableItem *> &any_tables)
{
  int ret = OB_SUCCESS;
  ObRelIds select_table_ids;  // select priv tables in root_stmt
  ObString info_schema(OB_INFORMATION_SCHEMA_NAME);
  const TableItem *dummy_item = NULL;
  // 1. get tables in select_item, need select privilege
  for (int64_t i = 0; OB_SUCC(ret) && i < root_stmt.get_select_item_size(); i++) {
    const ObRawExpr *expr = root_stmt.get_select_item(i).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get NULL ptr", K(ret));
    } else if (!expr->is_column_ref_expr()) {
      if (OB_FAIL(select_table_ids.add_members(expr->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      } else { /* do nothing */ }
    }
  }
  // 2. get tables in elsewhere, need select privilege
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr *, 4> else_exprs; // exprs in elsewhere
    ObStmtExprGetter visitor;
    visitor.set_relation_scope();
    visitor.remove_scope(SCOPE_SELECT);
    visitor.set_recursive(false);
    if (OB_FAIL(root_stmt.get_relation_exprs(else_exprs, visitor))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < else_exprs.count(); i++) {
        const ObRawExpr *expr = else_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get NULL ptr", K(ret));
        } else if (OB_FAIL(select_table_ids.add_members(expr->get_relation_ids()))) {
          LOG_WARN("failed to add members", K(ret));
        } else { /* do nothing */ }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // subquery + generated table in child_stmts
    ObSEArray<ObSelectStmt *, 4> child_stmts;
    if (OB_FAIL(root_stmt.get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
        if (OB_ISNULL(child_stmts.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child stmt is NULL", K(ret));
        } else if (SMART_CALL(get_sel_priv_tables_in_subquery(child_stmts.at(i), select_tables))) {
          LOG_WARN("failed to get need privs in child stmt", K(ret));
        }
      }
    }
  }
  // 4. get tables at from_scope tables/views, need any privileges
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < root_stmt.get_table_items().count(); i++) {
      const TableItem *table_item = root_stmt.get_table_item(i);
      bool is_database_name_equal = false;
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null");
      } else if (!(table_item->is_basic_table() || table_item->is_view_table_) ||
                 (table_item->database_name_.empty() && !table_item->dblink_name_.empty())) {
        /* do nothing */
      } else if (OB_FAIL(select_tables.get_refactored(table_item->ref_id_, dummy_item))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get refactored", K(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(ObResolverUtils::name_case_cmp(session_info_, table_item->database_name_,
                                       info_schema, OB_TABLE_NAME_CLASS, is_database_name_equal))) {
          } else if (is_database_name_equal) {
            /* do nothing */
          } else if (select_table_ids.has_member(root_stmt.get_table_bit_index(table_item->table_id_))) {
            if (OB_FAIL(select_tables.set_refactored(table_item->ref_id_, table_item))) {
              LOG_WARN("failed to set refactor", K(ret));
            }
          } else {
            if (OB_FAIL(any_tables.get_refactored(table_item->ref_id_, dummy_item))) {
              if (OB_HASH_NOT_EXIST != ret) {
                LOG_WARN("failed to get refactored", K(ret));
              } else {
                ret = OB_SUCCESS;
                if (OB_FAIL(any_tables.set_refactored(table_item->ref_id_, table_item))) {
                  LOG_WARN("failed to set refactor", K(ret));
                }
              }
            }
          }
        }
      } else { /* do nothing */ }
    }
  }
  return ret;
}

// MySQL5.7 privilege check refrence https://dev.mysql.com/doc/refman/5.7/en/create-view.html
int ObCreateViewResolver::check_privilege_needed(ObCreateTableStmt &stmt,
                                                 ObSelectStmt &select_stmt,
                                                 const bool is_force_view)
{
  int ret = OB_SUCCESS;
  const bool is_sys_table = false;
  int64_t table_size = select_stmt.get_table_size();
  common::hash::ObHashMap<int64_t, const TableItem *> select_tables;
  common::hash::ObHashMap<int64_t, const TableItem *> any_tables;
  ObNeedPriv need_priv(stmt.get_database_name(), stmt.get_table_name(), OB_PRIV_TABLE_LEVEL,
                       OB_PRIV_DROP, is_sys_table);
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ should not be NULL", K(ret));
  } else if (OB_FAIL(select_tables.create(8, "DDLResolver"))) {
    LOG_WARN("failed to create a hashmap", K(ret));
  } else if (OB_FAIL(any_tables.create(8, "DDLResolver"))) {
    LOG_WARN("failed to create a hashmap", K(ret));
  } else if (!is_force_view &&
             OB_FAIL(get_need_priv_tables(select_stmt, select_tables, any_tables))) {
    LOG_WARN("failed to get need priv tables", K(ret));
  }
  if (OB_SUCC(ret)) {
    int64_t need_privs_size = is_force_view ? 2 : 2 + select_tables.size() + any_tables.size();
    if (OB_FAIL(stmt.get_view_need_privs().reserve(need_privs_size))) {
      LOG_WARN("fail to reserve view need privs array", K(ret));
    } else if (stmt.get_create_table_arg().if_not_exist_ &&
               OB_FAIL(stmt.add_view_need_priv(need_priv))) {
      LOG_WARN("Fail to add need_priv", K(ret));
    } else if (OB_FALSE_IT(need_priv.priv_set_ = OB_PRIV_CREATE_VIEW)) {
    } else if (OB_FAIL(stmt.add_view_need_priv(need_priv))) {
      LOG_WARN("Fail to add need_priv", K(ret));
    } else if (!is_force_view) {
      if (!any_tables.empty()) {
        hash::ObHashMap<int64_t, const TableItem *>::iterator iter = any_tables.begin();
        for (; OB_SUCC(ret) && iter != any_tables.end(); iter++) {
          ObString database_name;
          ObString table_name;
          const TableItem *table_item = iter->second;
          if (OB_ISNULL(table_item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table item is null");
          } else if (OB_FAIL(ob_write_string(*allocator_, table_item->database_name_,
                                             database_name))) {
            LOG_WARN("Write string database name error", K(ret));
          } else if (OB_FAIL(ob_write_string(*allocator_, table_item->table_name_, table_name))) {
            LOG_WARN("Write table name error", K(table_item->table_name_), K(ret));
          } else {
            ObNeedPriv need_priv_else(database_name, table_name, OB_PRIV_TABLE_LEVEL,
                                      OB_PRIV_SELECT | OB_PRIV_INSERT | OB_PRIV_UPDATE | OB_PRIV_DELETE,
                                      table_item->is_system_table_, table_item->for_update_,
                                      OB_PRIV_CHECK_ANY);
            if (OB_FAIL(stmt.add_view_need_priv(need_priv_else))) {
              LOG_WARN("Fail to add need_priv", K(ret), K(need_priv_else));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !select_tables.empty()) {
        hash::ObHashMap<int64_t, const TableItem *>::iterator iter = select_tables.begin();
        for (; OB_SUCC(ret) && iter != select_tables.end(); iter++) {
          ObString database_name;
          ObString table_name;
          const TableItem *table_item = iter->second;
          if (OB_ISNULL(table_item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table item is null");
          } else if (OB_FAIL(ob_write_string(*allocator_, table_item->database_name_,
                                             database_name))) {
            LOG_WARN("Write string database name error", K(ret));
          } else if (OB_FAIL(ob_write_string(*allocator_, table_item->table_name_, table_name))) {
            LOG_WARN("Write table name error", K(table_item->table_name_), K(ret));
          } else {
            ObNeedPriv need_priv_else(database_name, table_name, OB_PRIV_TABLE_LEVEL,
                                      OB_PRIV_SELECT, table_item->is_system_table_,
                                      table_item->for_update_);
            if (OB_FAIL(stmt.add_view_need_priv(need_priv_else))) {
              LOG_WARN("Fail to add need_priv", K(ret), K(need_priv_else));
            }
          }
        }
      }
    }
  }
  if (select_tables.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = select_tables.destroy()))) {
      LOG_WARN("failed to destroy select_tables map", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  if (any_tables.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = any_tables.destroy()))) {
      LOG_WARN("failed to destroy any_tables map", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OX (stmt.set_view_define(&select_stmt));
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            stmt.get_database_name(),
                                            stmt::T_CREATE_VIEW,
                                            session_info_->get_enable_role_array()),
        session_info_->get_effective_tenant_id(), session_info_->get_user_id(),
        stmt.get_database_name());
    // 当user没有创建view的权限时，需要进一步检查user对于create view语句中select的table是否具有
    // access的权限。如果存在某一个table是user无法access的，
    // 那么优先对针对这个table报告出 OB_TABLE_NOT_EXIST
    if (OB_ERR_NO_PRIVILEGE == ret || OB_ERR_NO_SYS_PRIVILEGE == ret) {
      int no_priv_error = ret;
      ret = OB_SUCCESS;
      for (int i = 0; OB_SUCC(ret) && i < table_size; ++i) {
        bool accessible = false;
        const TableItem *table_item = select_stmt.get_table_item(i);
        CK (OB_NOT_NULL(table_item));
        OZ (schema_checker_->check_access_to_obj(session_info_->get_effective_tenant_id(),
                                                  session_info_->get_priv_user_id(),
                                                  table_item->ref_id_,
                                                  stmt::T_CREATE_VIEW,
                                                  session_info_->get_enable_role_array(),
                                                  accessible),
            session_info_->get_effective_tenant_id(), session_info_->get_user_id(),
            stmt.get_database_name());
        if (!accessible) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(stmt.get_database_name()),
              to_cstring(table_item->table_name_));
        }
      }
      if (OB_SUCC(ret)) {
        ret = no_priv_error;
      } else if (!is_force_view) {
        /* do nothing */
      } else if (OB_FAIL(try_add_error_info(ret, stmt.get_create_table_arg().error_info_))) {
        LOG_WARN("failed to add error info to for force view", K(ret));
      } else {
        ret = no_priv_error;
      }
    }
  }
  return ret;
}
int ObCreateViewResolver::stmt_print(const ObSelectStmt *stmt,
                                     common::ObIArray<common::ObString> *column_list,
                                     common::ObString &expanded_view)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  bool is_set_subquery = false;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_ISNULL(params_.query_ctx_) || OB_ISNULL(params_.schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx or schema checker is null", K(ret));
  } else {
    do {
      pos = 0;
      ObObjPrintParams obj_print_params(params_.query_ctx_->get_timezone_info());
      obj_print_params.print_origin_stmt_ = true;
      ObSelectStmtPrinter stmt_printer(buf, buf_len, &pos, stmt,
                                      params_.schema_checker_->get_schema_guard(),
                                      obj_print_params, true);
      stmt_printer.set_column_list(column_list);
      stmt_printer.set_is_first_stmt_for_hint(true);  // need print global hint
      if (OB_FAIL(stmt_printer.do_print())) {
        if (OB_SIZE_OVERFLOW == ret && buf_len < OB_MAX_PACKET_LENGTH) {
          buf_len = std::min(buf_len * 2, OB_MAX_PACKET_LENGTH);
          if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(buf_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret));
          }
        } else {
          LOG_WARN("fail to expand view definition", K(ret));
        }
      } else {
        expanded_view.assign_ptr(buf, pos);
      }
    } while (OB_SIZE_OVERFLOW == ret && buf_len < OB_MAX_PACKET_LENGTH);
  }
  return ret;
}

int ObCreateViewResolver::check_view_stmt_col_name(
    ObSelectStmt &select_stmt,
    ObArray<int64_t> &index_array,
    common::hash::ObHashSet<ObString> &view_col_names)
{
  /*
  * for real_alis name report error, otherwise auto gen col name
  * 1. column name is to long
  * 2. column name end with space
  * 3. column name already exists
  */
  int ret = OB_SUCCESS;
  int hash_ret = OB_HASH_NOT_EXIST;
  ObCollationType cs_type = CS_TYPE_INVALID;
  bool need_gen_name = false;
  int64_t select_item_size = select_stmt.get_select_item_size();
  if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation_connection", K(ret));
  }
  /*
  *check real alias name first
  *create view v as select 'K ','k','c' as 'k';
  *->| Name_exp_1 | Name_exp_2 | k |
  */
  for (int64_t i = 0; OB_SUCC(ret) && i < select_item_size; ++i) {
    ObString dup_col_name;
    ObString col_name = select_stmt.get_select_item(i).alias_name_;
    SelectItem& select_item = select_stmt.get_select_item(i);
    bool is_real_alias_ = select_item.is_real_alias_ ||
                          ObRawExprUtils::is_column_ref_skip_implicit_cast(select_item.expr_);
    if (is_real_alias_) {
      if (OB_FAIL(ObCharset::tolower(cs_type, col_name, dup_col_name, *allocator_))) {
        LOG_WARN("fail to lower string", K(ret));
      } else if (dup_col_name.length() > static_cast<size_t>(OB_MAX_VIEW_COLUMN_NAME_LENGTH_MYSQL)) {
        ret = OB_WRONG_COLUMN_NAME;
        LOG_WARN("view col_name is too long", K(col_name), K(ret));
        LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, col_name.length(), col_name.ptr());
      } else if (OB_FAIL(ObSQLUtils::check_column_name(cs_type, dup_col_name))) {
        LOG_WARN("fail to check_column_name", K(col_name), K(ret));
      } else if ((OB_HASH_EXIST == (hash_ret = view_col_names.exist_refactored(dup_col_name)))) {
        ret = OB_ERR_COLUMN_DUPLICATE;
        LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, dup_col_name.length(), dup_col_name.ptr());
        LOG_WARN("view col_name is real_alias and duplicated", K(col_name), K(ret));
      } else if (OB_FAIL(view_col_names.set_refactored(dup_col_name, 0))) {
        SQL_RESV_LOG(WARN, "set column name to hash set failed", K(ret), K(col_name));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_item_size; ++i) {
    ObString dup_col_name;
    ObString col_name = select_stmt.get_select_item(i).alias_name_;
    SelectItem& select_item = select_stmt.get_select_item(i);
    bool need_gen_name = false;
    bool is_real_alias_ = select_item.is_real_alias_ ||
                          ObRawExprUtils::is_column_ref_skip_implicit_cast(select_item.expr_);
    if (!is_real_alias_) {
      if (OB_FAIL(ObCharset::tolower(cs_type, col_name, dup_col_name, *allocator_))) {
        LOG_WARN("fail to lower string", K(ret));
      } else if (dup_col_name.length() > static_cast<size_t>(OB_MAX_VIEW_COLUMN_NAME_LENGTH_MYSQL)) {
          need_gen_name = true;
      } else if (OB_FAIL(ObSQLUtils::check_column_name(cs_type, dup_col_name))) {
        if (ret == OB_WRONG_COLUMN_NAME) {
          need_gen_name = true;
          ret = OB_SUCCESS;
          LOG_TRACE("view column name end with space is not real_alias will auto gen col name");
        } else {
          LOG_WARN("fail to check column name", K(col_name), K(ret));
        }
      } else if ((OB_HASH_EXIST == (hash_ret = view_col_names.exist_refactored(dup_col_name)))) {
        need_gen_name = true;
        ret = OB_SUCCESS;
        LOG_TRACE("view column name end with space is not real_alias will auto gen col name");
      } else if (OB_FAIL(view_col_names.set_refactored(dup_col_name, 0))) {
        SQL_RESV_LOG(WARN, "set column name to hash set failed", K(ret), K(col_name));
      }
      if (OB_SUCC(ret) && need_gen_name) {
        if (OB_FAIL(add_var_to_array_no_dup(index_array, i))){
          SQL_RESV_LOG(WARN, "add var failed", K(ret), K(i), K(col_name));
        }
      }
    }
  }
  return ret;
}

// 这个函数用于当非列别名的列名超过 64 时，将列名置为为系统自动生成的列名，eg: Name_exp_1
int ObCreateViewResolver::create_alias_names_auto(
    ObArray<int64_t> &index_array,
    ObSelectStmt *select_stmt,
    common::hash::ObHashSet<ObString> &view_col_names)
{
  int ret = OB_SUCCESS;
  int64_t long_col_name_num = index_array.size();
  uint64_t auto_name_id = 1;
  ObString tmp_col_name;
  int hash_ret = OB_HASH_EXIST;
  ObCollationType cs_type = CS_TYPE_INVALID;
  bool need_gen_name = false;
  ObString dup_col_name;
  if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation_connection", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < long_col_name_num; ++j) {
    // 创建系统自动生成的列名，并检查冲突
    hash_ret = OB_HASH_EXIST;
    char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
    while (OB_SUCC(ret) && OB_HASH_EXIST == hash_ret) {
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), "Name_exp_%ld", auto_name_id) < 0) {
        ret = OB_SIZE_OVERFLOW;SQL_RESV_LOG(WARN, "failed to generate buffer for temp_str_buf", K(ret));
      }
      if (OB_SUCC(ret)) {
        tmp_col_name = ObString::make_string(temp_str_buf);
        if (OB_FAIL(ObCharset::tolower(cs_type, tmp_col_name, dup_col_name, *allocator_))) {
          LOG_WARN("fail to lower string", K(ret));
        }
      }
      if (OB_HASH_EXIST == (hash_ret = view_col_names.exist_refactored(dup_col_name))) {
        ++auto_name_id;
      }
    }
    if (OB_SUCC(ret)) {
      ObString col_name;
      if (OB_FAIL(ob_write_string(*allocator_, tmp_col_name, col_name))) {
        SQL_RESV_LOG(WARN, "Can not malloc space for constraint name", K(ret));
      } else {
        select_stmt->get_select_item(index_array[j]).alias_name_.assign_ptr(col_name.ptr(), col_name.length());
        // 向 hash set 插入 col_name
        if (OB_FAIL(ObCharset::tolower(cs_type, col_name, dup_col_name, *allocator_))) {
          LOG_WARN("fail to lower string", K(ret));
        } else if (OB_FAIL(view_col_names.set_refactored(dup_col_name, 0))) {
          SQL_RESV_LOG(WARN, "set column name to hash set failed", K(ret), K(col_name));
        }
      }
    }
  }

  return ret;
}

int ObCreateViewResolver::resolve_column_list(ParseNode *view_columns_node,
                                              ObIArray<ObString> &column_list,
                                              ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is NULL", K(ret));
  } else if (NULL != view_columns_node) {
    ObColumnSchemaV2 column;
    if (OB_ISNULL(view_columns_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children_ of view_columns_node is NULL", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < view_columns_node->num_child_; ++i) {
        ParseNode *column_node = view_columns_node->children_[i];
        if (OB_ISNULL(column_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column node should not be NULL", K(ret));
        } else {
          column.reset();
          ObString column_name;
          if (OB_FAIL(resolve_column_name(column_name, column_node))) {
            LOG_WARN("fail to resolve column definition", K(ret));
          } else if (OB_FAIL(column.set_column_name(column_name))) {
            LOG_WARN("set column name failed", K(ret));
          } else {
            if (OB_FAIL(ob_write_string(*allocator_, column.get_column_name_str(), column_name))) {
              LOG_WARN("Failed to deep copy column_name", K(column.get_column_name_str()), K(ret));
            } else if (OB_FAIL(column_list.push_back(column_name))) {
              LOG_WARN("fail to push back column name", K(column_name), K(ret));
            } else {}
          }
        }
      }
    }
  } else {
    //if view_columns_nodes == NULL, just skip it
  }
  return ret;
}

int ObCreateViewResolver::collect_dependency_infos(ObQueryCtx *query_ctx,
                                                   ObCreateTableArg &create_arg)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  int64_t max_ref_obj_schema_version = -1;
  CK (OB_NOT_NULL(query_ctx));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(create_arg.schema_.get_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version >= DATA_VERSION_4_1_0_0) {
    OZ (ObDependencyInfo::collect_dep_infos(query_ctx->reference_obj_tables_,
                                            create_arg.dep_infos_,
                                            ObObjectType::VIEW,
                                            OB_INVALID_ID,
                                            max_ref_obj_schema_version));
    OX (create_arg.schema_.set_max_dependency_version(max_ref_obj_schema_version));
  } else {
    ObReferenceObjTable::ObDependencyObjItem *dep_obj_item = nullptr;
    ObString dummy;
    if (query_ctx->reference_obj_tables_.is_inited()) {
      OZ (query_ctx->reference_obj_tables_.get_dep_obj_item(
        OB_INVALID_ID, OB_INVALID_ID, ObObjectType::VIEW, dep_obj_item));
      CK (OB_NOT_NULL(dep_obj_item));
      OZ (ObDependencyInfo::collect_dep_infos(dep_obj_item->get_ref_obj_versions(),
                                              create_arg.dep_infos_,
                                              ObObjectType::VIEW,
                                               0, dummy, dummy, false/* is_pl */));
      OX (create_arg.schema_.set_max_dependency_version(dep_obj_item->max_ref_obj_schema_version_));
    }
  }
  return ret;
}

int ObCreateViewResolver::add_column_infos(const uint64_t tenant_id,
                                           ObSelectStmt &select_stmt,
                                           ObTableSchema &table_schema,
                                           ObIAllocator &alloc,
                                           ObSQLSessionInfo &session_info,
                                           const ObIArray<ObString> &column_list)
{
  int ret = OB_SUCCESS;
  ObIArray<SelectItem> &select_items = select_stmt.get_select_items();
  ObColumnSchemaV2 column;
  int64_t cur_column_id = OB_APP_MIN_COLUMN_ID;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version >= DATA_VERSION_4_1_0_0) {
    if (!column_list.empty() && OB_UNLIKELY(column_list.count() != select_items.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get wrong column count", K(ret), K(column_list.count()), K(select_items.count()), K(table_schema), K(select_stmt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      const SelectItem &select_item = select_items.at(i);
      const ObRawExpr *expr = select_item.expr_;
      if (OB_UNLIKELY(NULL == expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item expr is null", K(ret), K(i));
      } else {
        column.reset();
        if (!column_list.empty()) {
          column.set_column_name(column_list.at(i));
        } else if (!select_item.alias_name_.empty()) {
          column.set_column_name(select_item.alias_name_);
        } else {
          column.set_column_name(select_item.expr_name_);
        }
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
        if (column_meta.is_xml_sql_type()) {
          column.set_sub_data_type(T_OBJ_XML);
        }
        column.set_charset_type(table_schema.get_charset_type());
        column.set_collation_type(expr->get_collation_type());
        column.set_accuracy(expr->get_accuracy());
        column.set_zero_fill(expr->get_result_type().has_result_flag(ZEROFILL_FLAG));
        OZ (adjust_string_column_length_within_max(column, lib::is_oracle_mode()));
        if (lib::is_mysql_mode()) { // oracle mode has default expr value, not support now
          OZ (resolve_column_default_value(&select_stmt, select_item, column, alloc, session_info));
        }
        if (OB_FAIL(ret)) {
          // do nothing.
        } else {
          column.set_column_id(cur_column_id++);
          ObColumnSchemaV2 *org_column = table_schema.get_column_schema(column.get_column_name());
          if (OB_NOT_NULL(org_column)) {
            ObColumnSchemaV2 new_column(*org_column);
            new_column.set_column_id(cur_column_id++);
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
            if (column.is_string_type() || column.is_json()) {
              if (column.get_meta_type().is_lob() || column.get_meta_type().is_json()) {
                if (OB_FAIL(check_text_column_length_and_promote(column, table_schema.get_table_id(), true))) {
                  LOG_WARN("fail to check text or blob column length", K(ret), K(column));
                }
              }
            }
            if (OB_FAIL(ret)) {
              //do nothing ...
            } else if (OB_FAIL(table_schema.add_column(column))) {
              LOG_WARN("add column to table_schema failed", K(ret), K(column));
            }
          }
          LOG_DEBUG("ctas mysql mode, create_table_column_count = 0,end", K(column));
        }
      }
    }
  }
  return ret;
}

int ObCreateViewResolver::resolve_column_default_value(const sql::ObSelectStmt *select_stmt,
                                                       const sql::SelectItem &select_item,
                                                       ObColumnSchemaV2 &column_schema,
                                                       ObIAllocator &alloc,
                                                       ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObObj casted_cell;
  ObObj &res_obj = column_schema.get_cur_default_value();
  res_obj.reset();
  UNUSED(session_info);
  const ObObj *res_cell = NULL;
  ColumnItem column_item;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get select stmt", K(ret));
  } else if (OB_FAIL(ObResolverUtils::resolve_default_value_and_expr_from_select_item(select_item, column_item, select_stmt))) {
    LOG_WARN("failed to resolve default value", K(ret));
  } else if (OB_FAIL(ob_write_obj(alloc, column_item.default_value_, res_obj))) {
    LOG_WARN("failed to write obj", K(ret));
  } else if (ob_is_enum_or_set_type(column_item.default_value_.get_type())) {
    if (OB_FAIL(column_schema.set_extended_type_info(select_item.expr_->get_enum_set_values()))) {
      LOG_WARN("failed to set extended type info", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
