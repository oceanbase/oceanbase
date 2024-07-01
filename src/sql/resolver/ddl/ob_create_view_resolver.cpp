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
#include "sql/printer/ob_select_stmt_printer.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ddl/ob_create_table_resolver.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "lib/hash/ob_hashset.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "observer/virtual_table/ob_table_columns.h"
#include "sql/rewrite/ob_transformer_impl.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{
ObCreateViewResolver::ObCreateViewResolver(ObResolverParams &params) : ObCreateTableResolverBase(params)
{
}

ObCreateViewResolver::~ObCreateViewResolver()
{
}

int ObCreateViewResolver::add_hidden_tablet_seq_col(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "stmt is NULL", K(stmt_), KR(ret));
  } else {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    ObColumnSchemaV2 hidden_pk;
    hidden_pk.reset();
    hidden_pk.set_column_id(OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
    hidden_pk.set_data_type(ObUInt64Type);
    hidden_pk.set_nullable(false);
    hidden_pk.set_is_hidden(true);
    hidden_pk.set_charset_type(CHARSET_BINARY);
    hidden_pk.set_collation_type(CS_TYPE_BINARY);
    if (OB_FAIL(hidden_pk.set_column_name(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME))) {
      SQL_RESV_LOG(WARN, "failed to set column name", KR(ret));
    } else {
      hidden_pk.set_rowkey_position(1);
      if (OB_FAIL(table_schema.add_column(hidden_pk))) {
        SQL_RESV_LOG(WARN, "add column to table_schema failed", KR(ret), K(hidden_pk));
      }
    }
  }
  return ret;
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
    ObString expanded_view;
    int64_t view_definition_start_pos = 0;
    int64_t view_definition_end_pos = 0;
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
    bool is_materialized_view = 2 == parse_tree.reserved_;
    table_schema.get_view_schema().set_materialized(is_materialized_view);
    table_schema.set_force_view(is_force_view);
    table_schema.set_tenant_id(session_info_->get_effective_tenant_id());
    //table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
    table_schema.set_define_user_id(session_info_->get_priv_user_id());
    table_schema.set_view_created_method_flag((ObViewCreatedMethodFlag)(create_arg.if_not_exist_ || is_force_view));
    ParseNode *table_id_node = parse_tree.children_[TABLE_ID_NODE];
    const int64_t max_user_table_name_length = lib::is_oracle_mode()
                  ? OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    bool perserve_lettercase = false; // lib::is_oracle_mode() ? true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
    ObArray<ObString> column_list;
    ObArray<ObString> comment_list;
    bool has_dblink_node = false;
    ParseNode *mv_primary_key_node = NULL;
    share::schema::ObSchemaGetterGuard *schema_guard = NULL;
    uint64_t database_id = OB_INVALID_ID;
    ObString old_database_name;
    uint64_t old_database_id = session_info_->get_database_id();
    bool resolve_succ = true;
    bool can_expand_star = true;
    if (is_materialized_view) {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
        LOG_WARN("get tenant data version failed", KR(ret));
      } else if (tenant_data_version < DATA_VERSION_4_3_0_0){
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant version is less than 4.3, materialized view is not supported", KR(ret), K(tenant_data_version));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.3, materialized view is not supported");
      }
    }
    bool add_undefined_columns = false;
    ParseNode *select_stmt_node = NULL;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(resolve_table_relation_node(parse_tree.children_[VIEW_NODE],
                                                view_name, db_name,
                                                false, false, &dblink_name_ptr, &dblink_name_len, &has_dblink_node))) {
      LOG_WARN("failed to resolve table relation node!", K(ret));
    } else if (OB_FAIL(set_database_name(db_name))) {
      SQL_RESV_LOG(WARN, "set database name failes", KR(ret));
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
    } else if (OB_ISNULL(schema_checker_)
               || OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(schema_guard->get_database_id(session_info_->get_effective_tenant_id(),
                                                     stmt->get_database_name(),
                                                     database_id))) {
      LOG_WARN("failed to get database id", K(ret));
    } else if (OB_FALSE_IT(table_schema.set_database_id(database_id))) {
      //never reach
    } else if (OB_FAIL(ob_write_string(*allocator_,
                                       session_info_->get_database_name(),
                                       old_database_name))) {
      LOG_WARN("failed to write string", K(ret));
    } else {
      table_schema.set_table_id(table_id_node ?
                                static_cast<uint64_t>(table_id_node->children_[0]->value_) :
                                OB_INVALID_ID);
      if (is_oracle_mode()) {
        if (OB_FAIL(session_info_->set_default_database(stmt->get_database_name()))) {
          LOG_WARN("failed to set default database name", K(ret));
        } else {
          session_info_->set_database_id(database_id);
        }
      }
    }

    if (OB_SUCC(ret)) {
      // resolve select stmt: create ... as <select ...>
      ObViewTableResolver view_table_resolver(params_, stmt->get_database_name(),
                                              table_schema.get_table_name());
      view_table_resolver.params_.is_from_create_view_ = true;
      view_table_resolver.params_.is_from_create_mview_ = is_materialized_view;
      view_table_resolver.params_.is_specified_col_name_ = parse_tree.children_[VIEW_COLUMNS_NODE] != NULL;
      view_table_resolver.set_current_view_level(1);
      view_table_resolver.set_is_top_stmt(true);
      view_table_resolver.set_has_resolved_field_list(false);
      view_table_resolver.set_is_create_view(true);
      // set ObViewSchema.materialized_ in RS
      view_table_resolver.set_materialized(parse_tree.children_[MATERIALIZED_NODE] ? true : false);
      select_stmt_node = parse_tree.children_[SELECT_STMT_NODE];
      ParseNode *view_columns_node = parse_tree.children_[VIEW_COLUMNS_NODE];
      ObString sql_str(select_stmt_node->str_len_, select_stmt_node->str_value_);
      view_define = sql_str;
      view_definition_start_pos = select_stmt_node->stmt_loc_.first_column_;
      view_definition_end_pos = min(select_stmt_node->stmt_loc_.last_column_,
                                    params_.cur_sql_.length() - 1);
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
        resolve_succ = false;
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
      } else if (OB_FAIL(view_table_resolver.check_auto_gen_column_names())) {
        LOG_WARN("fail to check auto gen column names", K(ret));
      } else if (OB_FAIL(params_.query_ctx_->query_hint_.init_query_hint(params_.allocator_,
                                                                          params_.session_info_,
                                                          view_table_resolver.get_select_stmt()))) {
        LOG_WARN("failed to init query hint.", K(ret));
      }
      if (is_oracle_mode()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = session_info_->set_default_database(old_database_name))) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret; // 不覆盖错误码
          LOG_ERROR("failed to reset default database", K(ret), K(tmp_ret), K(old_database_name));
        } else {
          session_info_->set_database_id(old_database_id);
        }
      }
      // specify view related flags
      if (table_schema.is_sys_table()) {
        table_schema.set_table_type(SYSTEM_VIEW);
      } else if (table_schema.is_user_table()) {
        if (is_materialized_view) {
          table_schema.set_table_type(MATERIALIZED_VIEW);
        } else {
          table_schema.set_table_type(USER_VIEW);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(resolve_column_list(view_columns_node,
                                             column_list,
                                             mv_primary_key_node))) {
        LOG_WARN("fail to resolve view columns", K(ret));
      } else if (OB_UNLIKELY(!is_materialized_view && NULL != mv_primary_key_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected primary key node for non materialized view", K(ret));
      } else if (OB_ISNULL(select_stmt = view_table_resolver.get_select_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(view_table_resolver.get_select_stmt()));
      } else if (OB_ISNULL(select_stmt->get_real_stmt())) {
        if (!resolve_succ) {
          //if the first child stmt of set query resolve failed, real stmt is null
          ObArray<SelectItem> select_items;
          if (!column_list.empty() && OB_FAIL(add_undefined_column_infos(
                                                          session_info_->get_effective_tenant_id(),
                                                          select_items,
                                                          table_schema,
                                                          column_list))) {
            if (OB_FAIL(try_add_error_info(ret, create_arg.error_info_))) {
              LOG_WARN("failed to add error info to for force view", K(ret));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get real stmt", K(ret), KPC(select_stmt));
        }
      } else if (OB_FAIL(check_view_columns(*select_stmt,
                                            view_columns_node,
                                            create_arg.error_info_,
                                            is_force_view,
                                            can_expand_star,
                                            add_undefined_columns))) {
        LOG_WARN("failed to check view columns", K(ret));
      } else if ((lib::is_mysql_mode() || (resolve_succ && !add_undefined_columns))
                 && OB_FAIL(add_column_infos(session_info_->get_effective_tenant_id(),
                                             *select_stmt,
                                             table_schema,
                                             *allocator_,
                                             *session_info_,
                                             column_list,
                                             comment_list,
                                             params_.is_from_create_mview_))) {
        LOG_WARN("failed to add column infos", K(ret));
      } else if ((!resolve_succ || add_undefined_columns)
                  && is_force_view && lib::is_oracle_mode()
                  && OB_FAIL(try_add_undefined_column_infos(session_info_->get_effective_tenant_id(),
                                                            view_table_resolver.has_resolved_field_list(),
                                                            select_stmt_node,
                                                            *select_stmt,
                                                            table_schema,
                                                            column_list))) {
        LOG_WARN("failed to add undefined column infos", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(collect_dependency_infos(params_.query_ctx_, create_arg))) {
        LOG_WARN("failed to collect dependency infos", K(ret));
      } else if (is_force_view && (!resolve_succ || add_undefined_columns)
                 && FALSE_IT(table_schema.set_object_status(ObObjectStatus::INVALID))) {
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
          if ((with_check_option || lib::is_oracle_mode()) && !(select_stmt == NULL && !resolve_succ)
              && OB_FAIL(ObResolverUtils::view_with_check_option_allowed(select_stmt,
                                                                          with_check_option))) {
            LOG_WARN("view with check option not allowed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && is_materialized_view) {
      ObMVAdditionalInfo *mv_ainfo = NULL;
      ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
      ObSEArray<ObConstraint,4> &csts = create_table_stmt->get_create_table_arg().constraint_list_;
      if (OB_FAIL(ObResolverUtils::check_schema_valid_for_mview(table_schema))) {
        LOG_WARN("failed to check schema valid for mview", KR(ret), K(table_schema));
      } else if (OB_FAIL(resolve_table_options(parse_tree.children_[TABLE_OPTION_NODE], false))) {
        LOG_WARN("fail to resolve table options", KR(ret));
      } else if (OB_ISNULL(mv_ainfo = create_arg.mv_ainfo_.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Allocate ObMVAdditionalInfo from array error", K(ret));
      } else if (OB_FAIL(mv_ainfo->container_table_schema_.assign(table_schema))) {
        LOG_WARN("fail to assign table schema", KR(ret));
      } else if (OB_FAIL(resolve_materialized_view_container_table(parse_tree.children_[PARTITION_NODE],
                                                                   mv_primary_key_node,
                                                                   mv_ainfo->container_table_schema_,
                                                                   csts))) {
        LOG_WARN("fail do resolve for materialized view", K(ret));
      } else if (OB_FAIL(resolve_mv_options(select_stmt,
                                            parse_tree.children_[MVIEW_NODE],
                                            mv_ainfo->mv_refresh_info_,
                                            table_schema))) {
        LOG_WARN("fail to resolve mv options", K(ret));
      } else if (OB_FAIL(resolve_hints(parse_tree.children_[HINT_NODE], *stmt, mv_ainfo->container_table_schema_))) {
        LOG_WARN("resolve hints failed", K(ret));
      } else {
        mv_ainfo->mv_refresh_info_.parallel_ = stmt->get_parallelism();
      }
    }

    if (OB_SUCC(ret)) {
      uint64_t compat_version = OB_INVALID_VERSION;
      if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), compat_version))) {
        LOG_WARN("get min data_version failed", K(ret), K(session_info_->get_effective_tenant_id()));
      } else if (lib::is_oracle_mode() && resolve_succ && can_expand_star && !is_materialized_view
                 && sql::ObSQLUtils::is_data_version_ge_422_or_431(compat_version)) {
        if (OB_FAIL(print_star_expanded_view_stmt(expanded_view,
                                                  view_definition_start_pos,
                                                  view_definition_end_pos))) {
          LOG_WARN("failed to print stmt", K(ret));
        } else if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                           *allocator_, session_info_->get_dtc_params(), expanded_view,
                           ObCharset::COPY_STRING_ON_SAME_CHARSET))) {
          LOG_WARN("failed to convert expanded view", K(ret));
        } else if (OB_FAIL(table_schema.set_view_definition(expanded_view))) {
          LOG_WARN("fail to set view definition", K(expanded_view), K(ret));
        }
      } else if (!is_force_view && !is_sync_ddl_user) {
        // 前面用建view的sql直接设置过table_schema.set_view_definition
        // 基线备份时create view必须都用show create view里面的view definition
        // create force view use origin view_define
        if (OB_FAIL(print_rebuilt_view_stmt(select_stmt,
                                            0 == column_list.count() ? NULL : &column_list,
                                            expanded_view))) {
          LOG_WARN("fail to expand view definition", K(ret));
        } else if (OB_FAIL(table_schema.set_view_definition(expanded_view))) {
          LOG_WARN("fail to set view definition", K(expanded_view), K(ret));
        }
      }
    }

    // 权限添加需要拿到完整stmt信息，慎重调整本段代码位置
    if (OB_SUCC(ret) && !(is_sync_ddl_user && session_info_->is_inner())
        && !(select_stmt == NULL && !resolve_succ)
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

int ObCreateViewResolver::resolve_materialized_view_container_table(ParseNode *partition_node,
                                                                    ParseNode *mv_primary_key_node,
                                                                    ObTableSchema &container_table_schema,
                                                                    ObSEArray<ObConstraint,4>& csts)
{
  int ret = OB_SUCCESS;
  container_table_schema.set_table_type(ObTableType::USER_TABLE);
  container_table_schema.get_view_schema().reset();
  container_table_schema.set_max_dependency_version(OB_INVALID_VERSION);
  pctfree_ = 0; // set default pctfree value for non-sys table
  if (OB_FAIL(resolve_partition_option(partition_node, container_table_schema, true))) {
    LOG_WARN("fail to resolve_partition_option", KR(ret));
  } else if (OB_FAIL(set_table_option_to_schema(container_table_schema))) {
    SQL_RESV_LOG(WARN, "set table option to schema failed", KR(ret));
  } else if (NULL != mv_primary_key_node
             && OB_FAIL(resolve_primary_key_node(*mv_primary_key_node, container_table_schema))) {
    LOG_WARN("failed to resolve primary key node", K(ret));
  } else if (0 < container_table_schema.get_rowkey_column_num()) {  // create mv with primary key
    container_table_schema.set_table_pk_mode(ObTablePKMode::TPKM_OLD_NO_PK);
    container_table_schema.set_table_organization_mode(ObTableOrganizationMode::TOM_INDEX_ORGANIZED);
    if (is_oracle_mode() && OB_FAIL(resolve_pk_constraint_node(*mv_primary_key_node, ObString::make_empty_string(), csts))) {
      LOG_WARN("failed to add pk constraint for oracle mode", KR(ret));
    }
  } else if (OB_FAIL(add_hidden_tablet_seq_col(container_table_schema))) {
    LOG_WARN("fail to add hidden pk", KR(ret));
  } else {  // create mv without primary key
    container_table_schema.set_table_pk_mode(TPKM_TABLET_SEQ_PK);
    container_table_schema.set_table_organization_mode(TOM_HEAP_ORGANIZED);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(container_table_schema.check_primary_key_cover_partition_column())) {
    SQL_RESV_LOG(WARN, "fail to check primary key cover partition column", KR(ret));
  } else {
    container_table_schema.set_collation_type(collation_type_);
    container_table_schema.set_charset_type(charset_type_);
    container_table_schema.set_mv_container_table(IS_MV_CONTAINER_TABLE);
  }
  return ret;
}

int ObCreateViewResolver::resolve_primary_key_node(ParseNode &pk_node,
                                                   ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ParseNode *cur_node = NULL;
  if (OB_UNLIKELY(2 > pk_node.num_child_) || OB_ISNULL(cur_node = pk_node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(pk_node.num_child_), K(cur_node));
  } else if (OB_UNLIKELY(T_COLUMN_LIST != cur_node->type_ || cur_node->num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node", K(ret), K(get_type_name(cur_node->type_)), K(cur_node->num_child_));
  } else {
    ParseNode *key_node = NULL;
    int64_t pk_data_length = 0;
    ObColumnSchemaV2 *col = NULL;
    for (int32_t i = 0; OB_SUCC(ret) && i < cur_node->num_child_; ++i) {
      if (OB_ISNULL(key_node = cur_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i));
      } else if (OB_FAIL(ObCreateTableResolverBase::add_primary_key_part(ObString(key_node->str_len_, key_node->str_value_),
                                                                         table_schema, i,
                                                                         pk_data_length, col))) {
        LOG_WARN("failed to add primary key part", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret) || is_oracle_mode()) {
    } else if (OB_UNLIKELY(3 != pk_node.num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected params", K(ret), K(pk_node.num_child_));
    } else {
      if (NULL != (cur_node = pk_node.children_[1])) {
        table_schema.set_index_using_type(T_USING_HASH == cur_node->type_ ? share::schema::USING_HASH
                                                                          : share::schema::USING_BTREE);
      }
      if (NULL != (cur_node = pk_node.children_[2])) {
        if (OB_FAIL(table_schema.set_pk_comment(ObString(cur_node->str_len_, cur_node->str_value_)))) {
          LOG_WARN("fail to set primary key comment", K(ret), K(ObString(cur_node->str_len_, cur_node->str_value_)));
        }
      }
    }
  }
  return ret;
}

int ObCreateViewResolver::check_view_columns(ObSelectStmt &select_stmt,
                                             ParseNode *view_columns_node,
                                             share::schema::ObErrorInfo &error_info,
                                             const bool is_force_view,
                                             bool &can_expand_star,
                                             bool &add_undefined_columns)
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
  bool has_view_columns_node = false;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if ((!is_force_view || select_item_size > 0)
      && OB_FAIL(view_col_names.create(select_item_size))) {
    LOG_WARN("failed to init hashset", K(ret), K(select_item_size));
  } else if (NULL == view_columns_node || view_columns_node->num_child_ <= 0) {
    /* do nothing */
  } else if (is_force_view && select_item_size != view_columns_node->num_child_) {
    has_view_columns_node = true;
    if (OB_FAIL(try_add_error_info(OB_ERR_VIEW_WRONG_LIST, error_info))) {
      LOG_WARN("failed to add error info to for force view", K(ret));
    } else {
      can_expand_star = false;
      add_undefined_columns = true;
      LOG_TRACE("force view columns is not equal with select columns", K(select_item_size),
                                                            K(view_columns_node->num_child_));
    }
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else {
    ParseNode *child_node = NULL;
    int64_t col_cnt_from_node = 0;
    for (int64_t i = 0; OB_SUCC(ret) && !is_col_dup && i < view_columns_node->num_child_; i++) {
      if (OB_ISNULL(child_node = view_columns_node->children_[i])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null children", K(ret), K(i), K(child_node));
      } else if (T_PRIMARY_KEY == child_node->type_) {
        /* do nothing */
      } else if (FALSE_IT(dup_col_name = ObString::make_string(child_node->str_value_))) {
      } else if (OB_FAIL(ObCharset::tolower(cs_type, dup_col_name, dup_col_name, *allocator_))) {
        LOG_WARN("fail to lower string", K(ret));
      } else if (OB_HASH_EXIST == (ret = view_col_names.set_refactored(dup_col_name, 0))) {
        ++col_cnt_from_node;
        is_col_dup = true;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to set hashset", K(ret));
      } else {
        ++col_cnt_from_node;
      }
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode() && can_expand_star && select_item_size > 0) {
      hash::ObHashSet<ObString> select_item_names;
      if (OB_FAIL(select_item_names.create(select_item_size))) {
        LOG_WARN("failed to init hashset", K(ret), K(select_item_size));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && can_expand_star
                                         && i < select_stmt.get_select_items().count(); i++) {
          SelectItem &select_item = select_stmt.get_select_items().at(i);
          if (OB_HASH_EXIST == (ret = select_item_names.set_refactored(select_item.alias_name_, 0))) {
            can_expand_star = false;
            ret = OB_SUCCESS;
          } else if (OB_FAIL(ret)) {
            LOG_WARN("failed to set hashset", K(ret), K(select_item.alias_name_));
          } else { /* do nothing */ }
        }
      }
    }
    if (OB_FAIL(ret) || 0 == col_cnt_from_node) {
    } else if (OB_UNLIKELY(select_item_size != col_cnt_from_node)) {
      ret = OB_ERR_VIEW_WRONG_LIST;
      LOG_WARN("view columns is not equal with select columns", K(select_item_size),
                                      K(col_cnt_from_node), K(view_columns_node->num_child_));
    } else {
      has_view_columns_node = true;
    }
  }

  if (OB_FAIL(ret) || has_view_columns_node) {
  } else if (OB_UNLIKELY(is_force_view && 0 == select_item_size)) {
    if (OB_FAIL(try_add_error_info(OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE, error_info))) {
      LOG_WARN("failed to add error info to for force view", K(ret));
    } else {
      LOG_TRACE("force view must have at least one column that is not invisible",
                                                    K(OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE));
    }
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
          } else if (OB_FAIL(SMART_CALL(get_sel_priv_tables_in_subquery(child_stmts.at(i), select_tables)))) {
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
        } else if (OB_FAIL(SMART_CALL(get_sel_priv_tables_in_subquery(child_stmts.at(i), select_tables)))) {
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
                                                 table_item->database_name_,
                                                 stmt::T_CREATE_VIEW,
                                                 session_info_->get_enable_role_array(),
                                                 accessible),
            session_info_->get_effective_tenant_id(), session_info_->get_user_id(),
            stmt.get_database_name());
        if (OB_SUCC(ret) && !accessible) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(table_item->database_name_),
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
int ObCreateViewResolver::print_rebuilt_view_stmt(const ObSelectStmt *stmt,
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

int ObCreateViewResolver::print_star_expanded_view_stmt(common::ObString &expanded_view,
                                                        const int64_t view_definition_start_pos,
                                                        const int64_t view_definition_end_pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t last_end_pos = view_definition_start_pos;
  int64_t start_pos = 0;
  int64_t end_pos = 0;
  ObSqlString expanded_str;
  ObString substr;
  ObString orig_str = params_.cur_sql_;
  ObString table_name;
  for (int64_t i = 0; OB_SUCC(ret) && i < params_.star_expansion_infos_.count(); ++i) {
    ObArray<ObString> &column_name_list = params_.star_expansion_infos_.at(i).column_name_list_;
    start_pos = params_.star_expansion_infos_.at(i).start_pos_;
    end_pos = params_.star_expansion_infos_.at(i).end_pos_;
    // from last_end_pos to start_pos
    if (OB_FAIL(ob_sub_str(*allocator_, orig_str, last_end_pos, start_pos - 1, substr))) {
      LOG_WARN("failed to get sub string", K(ret));
    } else if (OB_FAIL(expanded_str.append(substr))) {
      LOG_WARN("failed to append sub string", K(ret));
    }
    // from start_pos to end_pos
    // * or t.*
    if (OB_SUCC(ret) && start_pos != end_pos
        && OB_FAIL(ob_sub_str(*allocator_, orig_str, start_pos, end_pos - 1, table_name))) {
      LOG_WARN("failed to get table_name", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < column_name_list.count(); ++j) {
      if (j > 0 && OB_FAIL(expanded_str.append(","))) {
        LOG_WARN("failed to append comma", K(ret));
      } else {
        ObSqlString column_name;
        if (start_pos != end_pos && OB_FAIL(expanded_str.append(table_name))) {
          LOG_WARN("failed to append table_name", K(ret));
        } else if (OB_FAIL(column_name.append("\""))) {
          LOG_WARN("failed to append quote", K(ret));
        } else if (OB_FAIL(column_name.append(column_name_list.at(j)))) {
          LOG_WARN("failed to append column name", K(ret));
        } else if (OB_FAIL(column_name.append("\""))) {
          LOG_WARN("failed to append quote", K(ret));
        } else if (OB_FAIL(expanded_str.append(column_name.string()))) {
          LOG_WARN("failed to append column name", K(ret));
        }
      }
    }
    last_end_pos = end_pos + 1;
  }
  // from last_end_pos to sql_end_pos
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ob_sub_str(*allocator_, orig_str, last_end_pos, view_definition_end_pos, substr))) {
    LOG_WARN("failed to get sub string", K(ret));
  } else if (OB_FAIL(expanded_str.append(substr))) {
    LOG_WARN("failed to append sub string", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, expanded_str.string(), expanded_view, true))) {
    LOG_WARN("failed to write string", K(ret));
  } else {
    LOG_DEBUG("expanded view definition", K(expanded_view));
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

int ObCreateViewResolver::resolve_mv_options(const ObSelectStmt *stmt,
                                             ParseNode *options_node,
                                             ObMVRefreshInfo &refresh_info,
                                             ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  refresh_info.refresh_method_ = ObMVRefreshMethod::FORCE; //default method is force
  refresh_info.refresh_mode_ = ObMVRefreshMode::DEMAND; //default mode is demand
  if (NULL == options_node) {
    /* do nothing */
  } else if (OB_UNLIKELY(T_MV_OPTIONS != options_node->type_ || 1 != options_node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(options_node));
  } else if (OB_FAIL(resolve_mv_refresh_info(options_node->children_[0], refresh_info))) {
    LOG_WARN("fail to resolve mv refresh info", KR(ret));
  } else {
    const int64_t on_query_computation_flag = 1;
    const int64_t query_rewrite_flag = 1 << 1;
    if (options_node->value_ & on_query_computation_flag) {
      table_schema.set_mv_on_query_computation(ObMVOnQueryComputationFlag::IS_MV_ON_QUERY_COMPUTATION);
    }
    if (options_node->value_ & query_rewrite_flag) {
      table_schema.set_mv_enable_query_rewrite(ObMVEnableQueryRewriteFlag::IS_MV_ENABLE_QUERY_REWRITE);
    }
  }
  if (OB_FAIL(ret)) {
  } else if ((table_schema.mv_on_query_computation() || ObMVRefreshMethod::FAST == refresh_info.refresh_method_)
             && OB_FAIL(ObMVChecker::check_mv_fast_refresh_valid(stmt, params_.stmt_factory_,
                                                                 params_.expr_factory_,
                                                                 params_.session_info_))) {
    LOG_WARN("fail to check fast refresh valid", K(ret));
  } else if (table_schema.mv_on_query_computation()
             && OB_FAIL(check_on_query_computation_supported(stmt))) {
    LOG_WARN("fail to check on query computation mv column type", K(ret));
  }
  return ret;
}

int ObCreateViewResolver::check_on_query_computation_supported(const ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObTransformerImpl::StmtFunc func;
  if (OB_FAIL(ObTransformerImpl::check_stmt_functions(stmt, func))) {
    LOG_WARN("failed to check stmt functions", K(ret));
  } else if (OB_UNLIKELY(func.contain_enum_set_values_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "on query computation mview use enum type");
    LOG_WARN("not support on query computation mview use enum type", KR(ret), K(func.contain_enum_set_values_));
  }
  return ret;
}

int ObCreateViewResolver::resolve_mv_refresh_info(ParseNode *refresh_info_node,
                                              ObMVRefreshInfo &refresh_info)
{
  int ret = OB_SUCCESS;
  if (allocator_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", KR(ret));
  }
  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t pos = 0;
  OZ (ObExecEnv::gen_exec_env(*session_info_, buf, OB_MAX_PROC_ENV_LENGTH, pos));
  OX (refresh_info.exec_env_.assign(buf, pos));
  OZ (ob_write_string(*allocator_, refresh_info.exec_env_, refresh_info.exec_env_));
  if (OB_SUCC(ret) && refresh_info_node != nullptr) {
    if (refresh_info_node->int32_values_[0] == 1) { //never refresh
      refresh_info.refresh_method_ = ObMVRefreshMethod::NEVER;
      refresh_info.refresh_mode_ = ObMVRefreshMode::NEVER;
    } else if ((2 == refresh_info_node->num_child_)
               && OB_NOT_NULL(refresh_info_node->children_)) {
      int32_t refresh_method = refresh_info_node->int32_values_[1];
      ParseNode *refresh_on_clause = refresh_info_node->children_[0];
      ParseNode *refresh_interval_node = refresh_info_node->children_[1];

      switch (refresh_method) {
        case 0:
          refresh_info.refresh_method_ = ObMVRefreshMethod::FAST;
          break;
        case 1:
          refresh_info.refresh_method_ = ObMVRefreshMethod::COMPLETE;
          break;
        case 2:
          refresh_info.refresh_method_ = ObMVRefreshMethod::FORCE;
          break;
      }

      if (refresh_on_clause != nullptr) {
        ParseNode *refresh_mode_node = refresh_on_clause->children_[0];
        if (refresh_mode_node != nullptr) {
          switch (refresh_mode_node->value_) {
            case 0:
              refresh_info.refresh_mode_ = ObMVRefreshMode::DEMAND;
              break;
            case 1:
              refresh_info.refresh_mode_ = ObMVRefreshMode::COMMIT;
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview refresh on commit");
              break;
            case 2:
              refresh_info.refresh_mode_ = ObMVRefreshMode::STATEMENT;
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview refresh on statement");
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid refresh mode", K(refresh_mode_node->value_));
              break;
          }
        }
      }

      if (OB_SUCC(ret) && refresh_interval_node != nullptr
          && 2 == refresh_interval_node->num_child_
          && (OB_NOT_NULL(refresh_interval_node->children_[0])
              || OB_NOT_NULL(refresh_interval_node->children_[1]))) {
        if (refresh_info.refresh_mode_ == ObMVRefreshMode::COMMIT) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ON COMMIT attribute followed by start with/next clause is not supported", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ON COMMIT attribute followed by start with/next clause is");
        } else if (refresh_info.refresh_mode_ == ObMVRefreshMode::STATEMENT) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ON STATEMENT attribute followed by start with/next clause is not supported", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ON STATEMENT attribute followed by start with/next clause is");
        } else {
          ParseNode *start_date = refresh_interval_node->children_[0];
          ParseNode *next_date = refresh_interval_node->children_[1];
          int64_t current_time = ObTimeUtility::current_time() / 1000000L * 1000000L; // ignore micro seconds
          int64_t start_time = OB_INVALID_TIMESTAMP;

          if (OB_NOT_NULL(start_date)
              && (T_MV_REFRESH_START_EXPR == start_date->type_)
              && (1 == start_date->num_child_)
              && (OB_NOT_NULL(start_date->children_))
              && (OB_NOT_NULL(start_date->children_[0]))) {
            if (OB_FAIL(ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(params_,
                *session_info_, *(start_date->children_[0]), *allocator_, start_time))) {
              LOG_WARN("failed to resolve date expr to timestamp", KR(ret));
            } else if (start_time < current_time) {
              ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
              LOG_WARN("the parameter start date must evaluate to a time in the future",
                  KR(ret), K(current_time), K(start_time));
              LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "start date");
            }
          }

          if (OB_SUCC(ret) && OB_NOT_NULL(next_date)) {
            int64_t next_time = OB_INVALID_TIMESTAMP;
            if (OB_FAIL(ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(params_,
                *session_info_, *next_date, *allocator_, next_time))) {
              LOG_WARN("fail to resolve date expr to timestamp", KR(ret));
            } else if (next_time < current_time) {
              ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
              LOG_WARN("the parameter next date must evaluate to a time in the future",
                  KR(ret), K(current_time), K(next_time));
              LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "next date");
            } else if (OB_INVALID_TIMESTAMP == start_time) {
              start_time = next_time;
            }

            if (OB_SUCC(ret)) {
              ObString next_date_str(next_date->str_len_, next_date->str_value_);
              if (OB_FAIL(ob_write_string(*allocator_, next_date_str, refresh_info.next_time_expr_))) {
                LOG_WARN("fail to write string", KR(ret));
              }
            }
          }

          if (OB_SUCC(ret)) {
            refresh_info.start_time_.set_timestamp(start_time);
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateViewResolver::resolve_column_list(ParseNode *view_columns_node,
                                              ObIArray<ObString> &column_list,
                                              ParseNode *&mv_primary_key_node)
{
  int ret = OB_SUCCESS;
  mv_primary_key_node = NULL;
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
        } else if (T_PRIMARY_KEY == column_node->type_) {
          if (OB_UNLIKELY(NULL != mv_primary_key_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("more than one primary key node", K(ret));
          } else {
            mv_primary_key_node = column_node;
          }
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
int ObCreateViewResolver::try_add_undefined_column_infos(const uint64_t tenant_id,
                                                         bool has_resolved_field_list,
                                                         ParseNode *select_stmt_node,
                                                         ObSelectStmt &select_stmt,
                                                         ObTableSchema &table_schema,
                                                         const common::ObIArray<ObString> &column_list)
{
  int ret = OB_SUCCESS;
  bool add_undefined_columns = false;
  ObArray<SelectItem> select_items;
  if (OB_ISNULL(select_stmt_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse node is null", K(ret));
  } else if (select_stmt_node->children_[PARSE_SELECT_SET] != NULL && column_list.empty()) {
    // do nothing
  } else if (has_resolved_field_list) {
    if (OB_FAIL(add_undefined_column_infos(tenant_id,
                                           select_stmt.get_select_items(),
                                           table_schema,
                                           column_list))) {
      LOG_WARN("failed to add undefined column infos", K(ret));
    }
  } else if (column_list.empty()
             && OB_FAIL(resolve_select_node_for_force_view(add_undefined_columns,
                                                           select_stmt_node,
                                                           select_items))) {
    LOG_WARN("failed to resolve select node", K(ret));
  } else if ((add_undefined_columns || !column_list.empty())
             && OB_FAIL(add_undefined_column_infos(tenant_id,
                                                   select_items,
                                                   table_schema,
                                                   column_list))) {
    LOG_WARN("failed to add undefined column infos", K(ret));
  }
  return ret;
}
int ObCreateViewResolver::resolve_select_node_for_force_view(bool &add_undefined_columns,
                                                             ParseNode *select_stmt_node,
                                                             ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  ParseNode *field_list_node = NULL;
  bool has_star = false;
  ParseNode *project_node = NULL;
  SelectItem select_item;
  if (OB_ISNULL(select_stmt_node)
      || OB_ISNULL(field_list_node = select_stmt_node->children_[PARSE_SELECT_SELECT])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse node is null", K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && !has_star && i < field_list_node->num_child_; ++i) {
    if (OB_ISNULL(project_node = field_list_node->children_[i]->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parse node is null", K(ret));
    } else if (project_node->type_ == T_STAR
               || (project_node->type_ == T_COLUMN_REF
                   && project_node->children_[2]->type_ == T_STAR)) {
      has_star = true;
    } else {
      select_item.expr_name_.assign_ptr(field_list_node->children_[i]->str_value_,
                          static_cast<int32_t>(field_list_node->children_[i]->str_len_));
      if (project_node->type_ == T_ALIAS) {
        ParseNode *alias_node = project_node->children_[1];
        select_item.alias_name_.assign_ptr(const_cast<char *>(alias_node->str_value_),
                                            static_cast<int32_t>(alias_node->str_len_));
        if (OB_UNLIKELY(alias_node->str_len_ > OB_MAX_COLUMN_NAME_LENGTH)) {
          ret = OB_ERR_TOO_LONG_IDENT;
          LOG_WARN("alias name too long", K(ret), K(select_item.alias_name_));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(select_items.push_back(select_item))) {
        LOG_WARN("failed to push back select item");
      }
    }
  }
  if (!has_star && OB_SUCC(ret)) {
    add_undefined_columns = true;
  }
  return ret;
}

int ObCreateViewResolver::add_undefined_column_infos(const uint64_t tenant_id,
                                                     ObIArray<SelectItem> &select_items,
                                                     ObTableSchema &table_schema,
                                                     const common::ObIArray<ObString> &column_list)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 column;
  int64_t cur_column_id = OB_APP_MIN_COLUMN_ID;
  int64_t column_count = !column_list.empty() ? column_list.count() : select_items.count();
  if (lib::is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      column.reset();
      if (!column_list.empty()) {
        OZ(column.set_column_name(column_list.at(i)));
      } else if (!select_items.at(i).alias_name_.empty()) {
        OZ(column.set_column_name(select_items.at(i).alias_name_));
      } else {
        OZ(column.set_column_name(select_items.at(i).expr_name_));
      }
      if (OB_FAIL(ret)) {
      } else {
        column.set_column_id(cur_column_id++);
        //ObExtendType only used internal, we user it to describe UNDEFINED type
        column.set_data_type(ObObjType::ObExtendType);
        column.set_data_length(0);
        column.set_data_precision(-1);
        column.set_data_scale(OB_MIN_NUMBER_SCALE - 1);
        if (OB_FAIL(table_schema.add_column(column))) {
          LOG_WARN("add column to table_schema failed", K(ret), K(column));
        }
      }
    }
  }
  return ret;
}

int ObCreateViewResolver::add_column_infos(const uint64_t tenant_id,
                                           ObSelectStmt &select_stmt,
                                           ObTableSchema &table_schema,
                                           ObIAllocator &alloc,
                                           ObSQLSessionInfo &session_info,
                                           const ObIArray<ObString> &column_list,
                                           const ObIArray<ObString> &comment_list,
                                           bool is_from_create_mview /* =false */)
{
  int ret = OB_SUCCESS;
  ObIArray<SelectItem> &select_items = select_stmt.get_select_items();
  ObColumnSchemaV2 column;
  int64_t cur_column_id = OB_APP_MIN_COLUMN_ID;
  uint64_t data_version = 0;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version >= DATA_VERSION_4_1_0_0) {
    if ((!column_list.empty() && OB_UNLIKELY(column_list.count() != select_items.count()))
        || (!comment_list.empty() && OB_UNLIKELY(comment_list.count() != select_items.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get wrong column count", K(ret), K(column_list.count()), K(comment_list.count()),
                                           K(select_items.count()), K(table_schema), K(select_stmt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      const SelectItem &select_item = select_items.at(i);
      const ObRawExpr *expr = select_item.expr_;
      column.reset();
      column.set_column_id(cur_column_id++);
      if (!column_list.empty()) {
        OZ(column.set_column_name(column_list.at(i)));
      } else if (!select_item.alias_name_.empty()) {
        OZ(column.set_column_name(select_item.alias_name_));
      } else {
        OZ(column.set_column_name(select_item.expr_name_));
      }
      if (OB_SUCC(ret) && !comment_list.empty()) {
        OZ(column.set_comment(comment_list.at(i)));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(NULL == expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item expr is null", K(ret), K(i));
      } else if (OB_FAIL(fill_column_meta_infos(*expr,
                                                table_schema.get_charset_type(),
                                                table_schema.get_table_id(),
                                                column,
                                                is_from_create_mview))) {
        LOG_WARN("failed to fill column meta infos", K(ret), K(column));
      } else if (lib::is_mysql_mode() &&
                 OB_FAIL(resolve_column_default_value(&select_stmt, select_item, column, alloc, session_info))) {
        // oracle mode has default expr value, not support now
        LOG_WARN("add column to table_schema failed", K(ret), K(column));
      } else if (OB_FAIL(table_schema.add_column(column))) {
        LOG_WARN("add column to table_schema failed", K(ret), K(column));
      } else {
        LOG_DEBUG("ctas mysql mode, create_table_column_count = 0,end", K(column));
      }
    }
  }
  return ret;
}

int ObCreateViewResolver::fill_column_meta_infos(const ObRawExpr &expr,
                                                 const ObCharsetType charset_type,
                                                 const uint64_t table_id,
                                                 ObColumnSchemaV2 &column,
                                                 bool is_from_create_mview /* =false */)
{
  int ret = OB_SUCCESS;
  ObObjMeta column_meta = expr.get_result_type().get_obj_meta();
  if (column_meta.is_lob_locator()) {
    column_meta.set_type(ObLongTextType);
  }
  if (column_meta.is_xml_sql_type()) {
    column.set_sub_data_type(T_OBJ_XML);
  }
  column.set_meta_type(column_meta);
  column.set_charset_type(charset_type);
  column.set_collation_type(expr.get_collation_type());
  column.set_accuracy(expr.get_accuracy());
  column.set_zero_fill(expr.get_result_type().has_result_flag(ZEROFILL_FLAG));
  if (is_from_create_mview) {
    // bug fix for
    // mview should not set not null
    column.set_nullable(true);
  } else {
    column.set_nullable(expr.get_result_type().is_not_null_for_read() ? false : true);
  }
  if (OB_FAIL(ret)) {
  } else if (column.is_enum_or_set() && OB_FAIL(column.set_extended_type_info(expr.get_enum_set_values()))) {
    LOG_WARN("set enum or set info failed", K(ret), K(expr));
  } else if (OB_FAIL(adjust_string_column_length_within_max(column, lib::is_oracle_mode()))) {
    LOG_WARN("failed to adjust string column length within max", K(ret), K(expr));
  } else if (OB_FAIL(adjust_number_decimal_column_accuracy_within_max(column, lib::is_oracle_mode()))) {
    LOG_WARN("failed to adjust number decimal column accuracy within max", K(ret), K(expr));
  } else if ((column.is_string_type() || column.is_json())
              && (column.get_meta_type().is_lob() || column.get_meta_type().is_json())
              && OB_FAIL(check_text_column_length_and_promote(column, table_id, true))) {
    LOG_WARN("fail to check text or blob column length", K(ret), K(column));
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

int ObCreateViewResolver::resolve_columns_nullable_value(const sql::ObSelectStmt *select_stmt,
                                                         const ObTableSchema &table_schema,
                                                         const sql::SelectItem &select_item,
                                                         ObColumnSchemaV2 &column_schema,
                                                         ObIAllocator &alloc,
                                                         ObSQLSessionInfo &session_info,
                                                         share::schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  observer::ObTableColumns::ColumnAttributes column_attributes;
  if (OB_FAIL(observer::ObTableColumns::deduce_column_attributes(is_oracle_mode(),
                                                                 table_schema,
                                                                 select_stmt,
                                                                 select_item,
                                                                 schema_guard,
                                                                 &session_info,
                                                                 NULL,
                                                                 0,
                                                                 column_attributes,
                                                                 true,
                                                                 alloc))) {
    LOG_WARN("failed to resolve nullable value", K(ret), K(table_schema));
  } else {
    column_schema.set_nullable(column_attributes.null_ == "YES");
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
