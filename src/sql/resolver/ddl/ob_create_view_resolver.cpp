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

#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"  // share CREATE TABLE stmt
#include "sql/resolver/dml/ob_select_stmt.h"        // resolve select clause
#include "sql/resolver/dml/ob_dml_stmt.h"           // PartExprItem
#include "sql/ob_sql_context.h"
#include "sql/ob_select_stmt_printer.h"
#include "sql/session/ob_sql_session_info.h"

#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "lib/hash/ob_hashset.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql {
ObCreateViewResolver::ObCreateViewResolver(ObResolverParams& params) : ObDDLResolver(params)
{}

ObCreateViewResolver::~ObCreateViewResolver()
{}

int ObCreateViewResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CREATE_VIEW != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type of parse_tree", K(parse_tree.type_), K(ret));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be NULL", K(ret));
  } else if (OB_UNLIKELY(ROOT_NUM_CHILD != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected num_child of parse_tree", K(parse_tree.num_child_), K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ should not be NULL", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ should not be NULL", K(ret));
  } else if (OB_ISNULL(params_.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_ctx should not be NULL", K(ret));
  } else {
    ObString view_name;
    ObString synonym_name;
    ObString view_define;
    ObCreateTableStmt* stmt = NULL;

    if (OB_UNLIKELY(NULL == (stmt = create_stmt<ObCreateTableStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create view stmt failed", K(ret));
    } else {
      stmt->set_allocator(*allocator_);
      stmt_ = stmt;
      stmt->set_is_view_stmt(true);
    }

    bool is_sync_ddl_user = false;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
        LOG_WARN("Failed to check sync_dll_user", K(ret));
      }
    }

    ObSelectStmt* select_stmt = NULL;
    // resolve view name; create view [ or replace] view <view_name>[column_list] [table_id]
    if (OB_SUCC(ret)) {
      ObString db_name;
      if (OB_UNLIKELY(T_CREATE_VIEW != parse_tree.type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parse_tree type should be T_CREATE_VIEW", K(parse_tree.type_), K(ret));
      } else {
        ObCreateTableArg& create_arg = stmt->get_create_table_arg();
        ObTableSchema& table_schema = create_arg.schema_;
        create_arg.if_not_exist_ = (NULL != parse_tree.children_[IF_NOT_EXISTS_NODE]);  // TODO

        ParseNode* view_node = parse_tree.children_[VIEW_NODE];
        if (OB_ISNULL(view_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view node should not be NULL", K(ret));
        } else {
          // uint64_t database_id = OB_INVALID_ID;
          // if (OB_FAIL(resolve_table_relation_factor(view_node, database_id, view_name, synonym_name, db_name))) {
          //   if (OB_TABLE_NOT_EXIST == ret) {
          //     ret = OB_SUCCESS;
          //   } else {
          //     LOG_WARN("resolve table relation failed", K(ret));
          //   }
          // }
          if (OB_FAIL(resolve_table_relation_node(view_node, view_name, db_name))) {
            LOG_WARN("failed to resolve table relation node!", K(ret));
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(normalize_table_or_database_names(view_name))) {
              LOG_WARN("fail to normalize table name", K(view_name), K(ret));
            } else if (OB_FAIL(ob_write_string(*allocator_, db_name, stmt->get_non_const_db_name()))) {
              LOG_WARN("failed to deep copy database name", K(ret), K(db_name));
            } else {
            }
          }

          if (OB_SUCC(ret)) {
            const int64_t max_user_table_name_length =
                share::is_oracle_mode() ? OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
            if (view_name.length() > max_user_table_name_length) {
              ret = OB_ERR_TOO_LONG_IDENT;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, view_name.length(), view_name.ptr());
            } else {
              table_schema.set_tenant_id(session_info_->get_effective_tenant_id());
              table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
              //              table_schema.set_database_id(database_id);
              ParseNode* table_id_node = parse_tree.children_[TABLE_ID_NODE];
              if (OB_UNLIKELY(NULL != table_id_node &&
                              (T_TABLE_ID != table_id_node->type_ || 1 != table_id_node->num_child_))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to resolve table_id", K(ret));
              } else {
                table_schema.set_table_id(
                    table_id_node ? static_cast<uint64_t>(table_id_node->children_[0]->value_) : OB_INVALID_ID);
                if (OB_FAIL(table_schema.set_table_name(view_name))) {
                  LOG_WARN("fail to set table_name", K(view_name), K(ret));
                }
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      // resolve select stmt: create ... as <select ...>
      ObResolverParams& resolver_ctx = params_;
      ObCreateTableArg& create_arg = stmt->get_create_table_arg();
      ObTableSchema& table_schema = create_arg.schema_;
      // ctx NULL resolve_ctx query-xtx
      if (OB_ISNULL(resolver_ctx.query_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resolver_ctx.query_ctx_ is null", K(ret));
      } else {
        ObViewTableResolver view_table_resolver(resolver_ctx, stmt->get_database_name(), table_schema.get_table_name());
        view_table_resolver.params_.is_from_create_view_ = true;
        view_table_resolver.set_is_create_view(true);
        // set ObViewSchema.materialized_ in RS
        view_table_resolver.set_materialized(parse_tree.children_[MATERIALIZED_NODE] ? true : false);

        ParseNode* select_stmt_node = parse_tree.children_[SELECT_STMT_NODE];
        ObString sql_str(select_stmt_node->str_len_, select_stmt_node->str_value_);
        if (OB_ISNULL(select_stmt_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select_stmt_node should not be NULL", K(ret));
        } else if (OB_UNLIKELY(T_SELECT != select_stmt_node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node type is not T_SELECT", K(select_stmt_node->type_), K(ret));
        } else if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(*allocator_,
                       sql_str,
                       view_define,
                       session_info_->get_local_collation_connection(),
                       ObCharset::get_system_collation()))) {
          LOG_WARN("write view define failed", K(ret));
        } else {
          bool specify_view_col = false;
          bool specify_alias_or_is_col = true;
          bool is_expr_or_col_dup = false;
          bool is_view_col_dup = false;
          ObString dup_col_name;
          hash::ObHashSet<ObString> view_col_names;

          ObCreateTableArg& create_arg = stmt->get_create_table_arg();
          ObTableSchema& table_schema = create_arg.schema_;
          // parse and resolve view defination
          if (OB_FAIL(view_table_resolver.resolve(*select_stmt_node))) {
            if (is_sync_ddl_user && session_info_->is_inner()) {
              // ret: OB_TABLE_NOT_EXIST || OB_ERR_BAD_FIELD_ERROR
              if (OB_TABLE_NOT_EXIST == ret || OB_ERR_BAD_FIELD_ERROR == ret || OB_ERR_KEY_DOES_NOT_EXISTS == ret) {
                LOG_WARN("resolve select in create view failed", K(ret));
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("resolve select in create view failed", K(ret));
              }
            } else {
              LOG_WARN("resolve select in create view failed", K(select_stmt_node), K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            select_stmt = view_table_resolver.get_select_stmt();
            if (OB_ISNULL(select_stmt)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("select_stmt is null", K(ret));
            } else if (share::is_oracle_mode() && !(is_sync_ddl_user && session_info_->is_inner())) {
              if (OB_FAIL(view_col_names.create(select_stmt->get_select_items().count()))) {
                LOG_WARN("failed to init hashset", K(ret), K(select_stmt->get_select_items().count()));
              }

              for (int64_t i = 0; OB_SUCC(ret) && specify_alias_or_is_col && !is_expr_or_col_dup &&
                                  i < select_stmt->get_select_items().count();
                   i++) {
                SelectItem& select_item = select_stmt->get_select_items().at(i);
                if (OB_ISNULL(select_item.expr_)) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("invalid null expr in select item", K(ret), K(select_item.expr_));
                } else if (select_item.is_real_alias_ || T_REF_COLUMN == select_item.expr_->get_expr_type()) {
                  if (OB_HASH_EXIST == (ret = view_col_names.set_refactored(select_item.alias_name_, 0))) {
                    is_expr_or_col_dup = true;
                    dup_col_name = select_item.alias_name_;
                    ret = OB_SUCCESS;
                  } else if (OB_SUCCESS != ret) {
                    LOG_WARN("failed to set hashset", K(ret), K(select_item.alias_name_));
                  } else {
                    // do nothing
                  }
                } else {
                  specify_alias_or_is_col = false;
                }
              }
            } else if (share::is_mysql_mode() && !(is_sync_ddl_user && session_info_->is_inner())) {
              ObArray<int64_t> index_array;  // to record too long expr column index
              int64_t select_item_size = select_stmt->get_select_item_size();
              if (OB_FAIL(view_col_names.create(select_item_size))) {
                LOG_WARN("failed to init hashset", K(ret), K(select_item_size));
              }
              for (int64_t i = 0; OB_SUCC(ret) && i < select_item_size; ++i) {
                if (OB_FAIL(
                        check_select_stmt_col_name(select_stmt->get_select_item(i), index_array, i, view_col_names))) {
                  SQL_RESV_LOG(WARN,
                      "check select stmt col name failed",
                      K(ret),
                      K(select_stmt->get_select_item(i).alias_name_));
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(create_alias_names_auto(index_array, select_stmt, view_col_names))) {
                  SQL_RESV_LOG(WARN, "check and create alias name failed", K(ret), K(index_array));
                }
              }
            }
            // select_stmt->remove_alias_ref_exprs();
          }
          if (OB_SUCC(ret) && !(is_sync_ddl_user && session_info_->is_inner())) {
            ParseNode* view_columns_node = parse_tree.children_[VIEW_COLUMNS_NODE];
            // view_columns_node may be NULL
            if (NULL != view_columns_node && view_columns_node->num_child_ > 0) {
              // check the view columns size is same with select columns
              int64_t select_item_size = select_stmt->get_select_item_size();
              int64_t view_children_num = view_columns_node->num_child_;
              if (OB_UNLIKELY(select_item_size != view_children_num)) {
                ret = OB_ERR_VIEW_WRONG_LIST;
                LOG_WARN("view columns is not equal with select columns", K(select_item_size), K(view_children_num));
              } else if (share::is_oracle_mode()) {  // need check if any dup. names in oracle mode
                is_expr_or_col_dup = false;
                specify_view_col = true;
                dup_col_name.reset();
                view_col_names.clear();
                for (int64_t i = 0; OB_SUCC(ret) && !is_view_col_dup && i < view_columns_node->num_child_; i++) {
                  if (OB_ISNULL(view_columns_node->children_[i])) {
                    ret = OB_INVALID_ARGUMENT;
                    LOG_WARN("invalid null children", K(ret), K(view_columns_node->children_[i]));
                  } else if (FALSE_IT(
                                 dup_col_name = ObString::make_string(view_columns_node->children_[i]->str_value_))) {
                    // do nothing
                  } else if (OB_HASH_EXIST == (ret = view_col_names.set_refactored(dup_col_name, 0))) {
                    is_view_col_dup = true;
                    ret = OB_SUCCESS;
                  } else if (OB_SUCCESS != ret) {
                    LOG_WARN("failed to set hashset", K(ret));
                  } else {
                    // do nothing
                  }
                }
              } else {
                // do nothing
              }
            }
          }

          if (share::is_oracle_mode() && !(is_sync_ddl_user && session_info_->is_inner())) {
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (!specify_view_col && !specify_alias_or_is_col) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support behavior while creating view in oracle",
                  K(ret),
                  K(specify_view_col),
                  K(specify_alias_or_is_col));
              LOG_USER_ERROR(OB_NOT_SUPPORTED,
                  "expression without a column alias");  // ORACLE_STR_USER_ERROR[-OB_NOT_SUPPORTED] = "internal error
                                                         // code, arguments: -4007, %msg not supported";
            } else if (is_expr_or_col_dup || is_view_col_dup) {
              ret = OB_NON_UNIQ_ERROR;
              ObString scope_name = ObString::make_string(get_scope_name(T_FIELD_LIST_SCOPE));
              LOG_USER_ERROR(
                  OB_NON_UNIQ_ERROR, dup_col_name.length(), dup_col_name.ptr(), scope_name.length(), scope_name.ptr());
            } else {
              // do nothing
            }
          }

          // specify view related flags
          if (OB_SUCC(ret)) {
            if (table_schema.is_sys_table()) {
              table_schema.set_table_type(SYSTEM_VIEW);
            } else if (table_schema.is_user_table()) {
              table_schema.set_table_type(USER_VIEW);
            }
            bool is_updatable = true;
            if (NULL != parse_tree.children_[WITH_OPT_NODE] && 1 == parse_tree.children_[WITH_OPT_NODE]->value_) {
              // WITH READ ONLY
              is_updatable = false;
            }
            ObViewSchema& view_schema = table_schema.get_view_schema();
            view_schema.set_view_definition(view_define);
            view_schema.set_view_check_option(VIEW_CHECK_OPTION_NONE);
            view_schema.set_view_is_updatable(is_updatable);
            ObCharsetType cs_client_type = CHARSET_INVALID;
            ObCollationType coll_connection_type = CS_TYPE_INVALID;
            if (OB_FAIL(session_info_->get_character_set_client(cs_client_type))) {
              LOG_WARN("get character set client failed", K(ret));
            } else if (OB_FAIL(session_info_->get_collation_connection(coll_connection_type))) {
              LOG_WARN("get collation connection failed", K(ret));
            } else {
              view_schema.set_character_set_client(cs_client_type);
              view_schema.set_collation_connection(coll_connection_type);
            }
          }

          // materialized view
          if (OB_SUCC(ret) && view_table_resolver.get_materialized()) {
            ObSelectStmt* select_stmt = view_table_resolver.get_select_stmt();
            if (OB_ISNULL(select_stmt)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL ptr", K(ret), K(select_stmt));
            } else {
              ObMaterializedViewContext mv_ctx;
              if (OB_FAIL(ObResolverUtils::find_base_and_depend_table(*schema_checker_, *select_stmt, mv_ctx))) {
                LOG_WARN("find base and depend table failed", K(ret));
              } else if (OB_INVALID_ID == mv_ctx.base_table_id_ || OB_INVALID_ID == mv_ctx.depend_table_id_) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected table id", K(mv_ctx.base_table_id_), K(mv_ctx.depend_table_id_), K(ret));
              } else {
                select_stmt->set_base_table_id(mv_ctx.base_table_id_);
                select_stmt->set_depend_table_id(mv_ctx.depend_table_id_);
                if (OB_FAIL(ObResolverUtils::check_materialized_view_limitation(*select_stmt, mv_ctx))) {
                  LOG_WARN("fail to check materialized view limitattion", K(ret));
                } else if (OB_FAIL(ObResolverUtils::update_materialized_view_schema(
                               mv_ctx, *schema_checker_, table_schema))) {
                  LOG_WARN("fail to update materialized view schema", K(ret));
                } else {
                  ObTableSchema tmp_schema;
                  ;
                  if (OB_FAIL(tmp_schema.assign(table_schema))) {
                    LOG_WARN("fail to assign schema", K(ret));
                  } else if (OB_FAIL(ObResolverUtils::make_columns_for_materialized_view(
                                 mv_ctx, *schema_checker_, tmp_schema))) {
                    LOG_WARN("fail to make columns for mv", K(ret));
                  }
                }
              }
            }
          }
        }
      }
    }

    ObString expanded_view;
    if (OB_SUCC(ret)) {
      ObArray<ObString> column_list;
      ParseNode* view_columns_node = parse_tree.children_[VIEW_COLUMNS_NODE];
      // view_columns_node may be NULL
      if (OB_FAIL(resolve_column_list(view_columns_node, column_list))) {
        LOG_WARN("fail to resolve view columns", K(ret));
      } else {
        ObTableSchema& table_schema = stmt->get_create_table_arg().schema_;
        // expand view definition
        if (OB_FAIL(stmt_print(select_stmt, 0 == column_list.count() ? NULL : &column_list, expanded_view))) {
          LOG_WARN("fail to expand view definition", K(ret));
        } else {
          if (is_sync_ddl_user) {
          } else if (OB_FAIL(table_schema.set_view_definition(expanded_view))) {
            LOG_WARN("fail to set view definition", K(expanded_view), K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t table_size = select_stmt->get_table_size();
      if (OB_FAIL(stmt->get_view_need_privs().reserve(table_size + 2))) {
        LOG_WARN("fail to reserve view need privs array", K(ret));
      }
    }
    // get privilege needed
    if (OB_SUCC(ret) && !(is_sync_ddl_user && session_info_->is_inner())) {
      if (stmt->get_create_table_arg().if_not_exist_) {
        const bool is_sys_table = false;
        ObNeedPriv drop_priv(
            stmt->get_database_name(), stmt->get_table_name(), OB_PRIV_TABLE_LEVEL, OB_PRIV_DROP, is_sys_table);
        if (OB_FAIL(stmt->add_view_need_priv(drop_priv))) {
          LOG_WARN("Fail to add need_priv", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const bool is_sys_table = false;
        ObNeedPriv need_priv(
            stmt->get_database_name(), stmt->get_table_name(), OB_PRIV_TABLE_LEVEL, OB_PRIV_CREATE_VIEW, is_sys_table);
        if (OB_FAIL(stmt->add_view_need_priv(need_priv))) {
          LOG_WARN("Fail to add need_priv", K(ret));
        } else {
          int64_t table_size = select_stmt->get_table_size();
          for (int64_t i = 0; OB_SUCC(ret) && i < table_size; ++i) {
            ObString database_name;
            ObString table_name;
            const TableItem* table_item = select_stmt->get_table_item(i);
            if (OB_ISNULL(table_item)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table item is null");
            } else if (TableItem::BASE_TABLE == table_item->type_ || TableItem::ALIAS_TABLE == table_item->type_) {
              // no check for information_schema select
              ObString info_schema("information_schema");
              bool is_table_name_equal = false;
              if (OB_FAIL(ObResolverUtils::name_case_cmp(session_info_,
                      table_item->database_name_,
                      info_schema,
                      OB_TABLE_NAME_CLASS,
                      is_table_name_equal))) {
              } else if (is_table_name_equal) {
                // do nothing
              } else if (OB_FAIL(ob_write_string(*allocator_, table_item->database_name_, database_name))) {
                LOG_WARN("Write string database name error", K(ret));
              } else if (OB_FAIL(ob_write_string(*allocator_, table_item->table_name_, table_name))) {
                LOG_WARN("Write table name error", K(table_item->table_name_), K(ret));
              } else {
                need_priv.db_ = database_name;
                need_priv.table_ = table_name;
                need_priv.priv_set_ = OB_PRIV_SELECT;
                need_priv.is_sys_table_ = table_item->is_system_table_;
                need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
                if (OB_FAIL(stmt->add_view_need_priv(need_priv))) {
                  LOG_WARN("Fail to add need_priv", K(ret), K(need_priv));
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OX(stmt->set_view_define(select_stmt));
        OZ(schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
               session_info_->get_priv_user_id(),
               stmt->get_database_name(),
               stmt::T_CREATE_VIEW,
               session_info_->get_enable_role_array()),
            session_info_->get_effective_tenant_id(),
            session_info_->get_user_id(),
            stmt->get_database_name());
        if (OB_ERR_NO_PRIVILEGE == ret || OB_ERR_NO_SYS_PRIVILEGE == ret) {
          int no_priv_error = ret;
          ret = OB_SUCCESS;
          int64_t table_size = select_stmt->get_table_size();
          for (int i = 0; OB_SUCC(ret) && i < table_size; ++i) {
            bool accessible = false;
            const TableItem* table_item = select_stmt->get_table_item(i);
            CK(OB_NOT_NULL(table_item));
            OZ(schema_checker_->check_access_to_obj(session_info_->get_effective_tenant_id(),
                   session_info_->get_priv_user_id(),
                   table_item->table_id_,
                   stmt::T_CREATE_VIEW,
                   session_info_->get_enable_role_array(),
                   accessible),
                session_info_->get_effective_tenant_id(),
                session_info_->get_user_id(),
                stmt->get_database_name());
            if (!accessible) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_USER_ERROR(
                  OB_TABLE_NOT_EXIST, to_cstring(stmt->get_database_name()), to_cstring(table_item->table_name_));
            }
          }
          if (OB_SUCC(ret)) {
            ret = no_priv_error;
          }
        }
      }
    }
  }

  return ret;
}

int ObCreateViewResolver::stmt_print(
    const ObSelectStmt* stmt, common::ObIArray<common::ObString>* column_list, common::ObString& expanded_view)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  bool is_set_subquery = false;
  if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    do {
      pos = 0;
      ObObjPrintParams obj_print_params(stmt->tz_info_);
      obj_print_params.is_show_create_view_ = true;
      ObSelectStmtPrinter stmt_printer(buf, buf_len, &pos, stmt, stmt->tz_info_, column_list, is_set_subquery);
      stmt_printer.set_print_params(obj_print_params);
      if (OB_FAIL(stmt_printer.do_print())) {
        if (OB_SIZE_OVERFLOW == ret && buf_len < OB_MAX_PACKET_LENGTH) {
          buf_len = std::min(buf_len * 2, OB_MAX_PACKET_LENGTH);
          if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
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

// check if alias too long or duplicate
int ObCreateViewResolver::check_select_stmt_col_name(SelectItem& select_item, ObArray<int64_t>& index_array,
    int64_t pos, common::hash::ObHashSet<ObString>& view_col_names)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_HASH_NOT_EXIST;
  bool len_is_legal = false;
  ObString col_name = select_item.alias_name_;

  if (select_item.is_real_alias_) {
    // alias name less than OB_MAX_VIEW_COLUMN_NAME_LENGTH_MYSQL
    if (col_name.length() > static_cast<size_t>(OB_MAX_VIEW_COLUMN_NAME_LENGTH_MYSQL)) {
      ret = OB_WRONG_COLUMN_NAME;
      LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, col_name.length(), col_name.ptr());
    } else {
      len_is_legal = true;
    }
  } else {
    if (col_name.length() > static_cast<size_t>(OB_MAX_VIEW_COLUMN_NAME_LENGTH_MYSQL)) {
      if (OB_FAIL(index_array.push_back(pos))) {
        SQL_RESV_LOG(WARN, "push_back failed", K(ret), K(pos), K(col_name));
      }
    } else {
      len_is_legal = true;
    }
  }
  if (OB_SUCC(ret) && len_is_legal) {
    // check if dup
    if (OB_HASH_EXIST == (hash_ret = view_col_names.exist_refactored(col_name))) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, col_name.length(), col_name.ptr());
    } else {
      if (OB_FAIL(view_col_names.set_refactored(col_name, 0))) {
        SQL_RESV_LOG(WARN, "set column name to hash set failed", K(ret), K(col_name));
      }
    }
  }

  return ret;
}

int ObCreateViewResolver::create_alias_names_auto(
    ObArray<int64_t>& index_array, ObSelectStmt* select_stmt, common::hash::ObHashSet<ObString>& view_col_names)
{
  int ret = OB_SUCCESS;

  int64_t long_col_name_num = index_array.size();
  uint64_t auto_name_id = 1;
  ObString tmp_col_name;
  int hash_ret = OB_HASH_EXIST;
  for (int64_t j = 0; OB_SUCC(ret) && j < long_col_name_num; ++j) {
    hash_ret = OB_HASH_EXIST;
    while (OB_SUCC(ret) && OB_HASH_EXIST == hash_ret) {
      char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), "Name_exp_%ld", auto_name_id) < 0) {
        ret = OB_SIZE_OVERFLOW;
        SQL_RESV_LOG(WARN, "failed to generate buffer for temp_str_buf", K(ret));
      }
      if (OB_SUCC(ret)) {
        tmp_col_name = ObString::make_string(temp_str_buf);
      }
      if (OB_HASH_EXIST == (hash_ret = view_col_names.exist_refactored(tmp_col_name))) {
        ++auto_name_id;
      }
    }
    if (OB_SUCC(ret)) {
      ObString col_name;
      if (OB_FAIL(ob_write_string(*allocator_, tmp_col_name, col_name))) {
        SQL_RESV_LOG(WARN, "Can not malloc space for constraint name", K(ret));
      } else {
        select_stmt->get_select_item(index_array[j]).alias_name_.assign_ptr(col_name.ptr(), col_name.length());
        if (OB_FAIL(view_col_names.set_refactored(col_name, 0))) {
          SQL_RESV_LOG(WARN, "set column name to hash set failed", K(ret), K(col_name));
        }
      }
    }
  }

  return ret;
}

int ObCreateViewResolver::resolve_column_list(ParseNode* view_columns_node, ObIArray<ObString>& column_list)
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
        ParseNode* column_node = view_columns_node->children_[i];
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
            } else {
            }
          }
        }
      }
    }
  } else {
    // if view_columns_nodes == NULL, just skip it
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
