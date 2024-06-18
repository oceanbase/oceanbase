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
#include "sql/resolver/dml/ob_view_table_resolver.h"
#include "share/schema/ob_table_schema.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/parser/ob_parser.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
int ObViewTableResolver::do_resolve_set_query(const ParseNode &parse_tree,
                                              ObSelectStmt *&child_stmt,
                                              const bool is_left_child) /*default false*/
{
  int ret = OB_SUCCESS;
  child_stmt = NULL;
  ObViewTableResolver child_resolver(params_, view_db_name_, view_name_);

  child_resolver.set_current_level(current_level_);
  child_resolver.set_current_view_level(current_view_level_);
  child_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
  child_resolver.set_current_view_item(current_view_item);
  child_resolver.set_parent_view_resolver(parent_view_resolver_);
  child_resolver.set_calc_found_rows(is_left_child && has_calc_found_rows_);
  child_resolver.set_is_top_stmt(is_top_stmt());
  
  if (OB_FAIL(add_cte_table_to_children(child_resolver))) {
    LOG_WARN("failed to add cte table to children", K(ret));
  } else if (OB_FAIL(child_resolver.resolve_child_stmt(parse_tree))) {
    LOG_WARN("failed to resolve child stmt", K(ret));
  } else if (OB_ISNULL(child_stmt = child_resolver.get_child_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null child stmt", K(ret));
  }
  return ret;
}

int ObViewTableResolver::expand_view(TableItem &view_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is null");
  } else if (OB_FAIL(check_view_circular_reference(view_item))) {
    LOG_WARN("check view circular reference failed", K(ret));
  } else {
    // expand view as subquery which use view name as alias
    const ObTableSchema *view_schema = NULL;
    share::schema::ObSchemaGetterGuard *schema_guard = NULL;
    uint64_t database_id = OB_INVALID_ID;
    ObString old_database_name;
    uint64_t old_database_id = session_info_->get_database_id();
    if (OB_ISNULL(schema_checker_)
        || OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(schema_guard->get_database_id(session_info_->get_effective_tenant_id(),
                                                     view_item.database_name_,
                                                     database_id))) {
      LOG_WARN("failed to get database id", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                  view_item.ref_id_,
                                                  view_schema))) {
      LOG_WARN("get table schema failed", K(view_item));
    } else if (OB_FAIL(ob_write_string(*allocator_,
                                       session_info_->get_database_name(),
                                       old_database_name))) {
      LOG_WARN("failed to write string", K(ret));
    } else {
      ObViewTableResolver view_resolver(params_, view_db_name_, view_name_);
      view_resolver.set_current_level(current_level_);
      view_resolver.set_current_view_level(current_view_level_ + 1);
      view_resolver.set_view_ref_id(view_item.ref_id_);
      view_resolver.set_current_view_item(view_item);
      view_resolver.set_parent_view_resolver(this);
      view_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
      if (is_oracle_mode()) {
        if (OB_FAIL(session_info_->set_default_database(view_item.database_name_))) {
          LOG_WARN("failed to set default database name", K(ret));
        } else {
          session_info_->set_database_id(database_id);
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(do_expand_view(view_item, view_resolver))) {
        LOG_WARN("do expand view failed", K(ret));
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
    }
  }
  return ret;
}

int ObViewTableResolver::check_view_circular_reference(const TableItem &view_item)
{
  int ret = OB_SUCCESS;
  // 检查逻辑 :不断往上走，逐层判断view_item是否相同(db_name && tbl_name)
  // -- is_view_stmt() : 判断当前stmt是不是view展开的
  // -- get_view_item() : 展开成当前stmt的view (TableItem)
  // -- get_view_upper_scope_stmt() : view子查询的uppe_scope_stmt
  // 如果存在相互引用，mysql的行为是对最上层的view报错, 因此
  // exist_circular_reference = true 时也会继续循环, 比如：
  // v3引用v2，而v1<->v2相互引用，select * from v3 结果是对v3报错
  ObViewTableResolver *cur_resolver = this;
  if (OB_UNLIKELY(! view_db_name_.empty() && ! view_name_.empty()
                  && 0 == view_db_name_.compare(view_item.database_name_)
                  && 0 == view_name_.compare(view_item.table_name_))) {
    if (is_oracle_mode()) {
      ret = OB_ERR_VIEW_RECURSIVE;
    } else {
      ret = OB_ERR_VIEW_RECURSIVE;
      LOG_USER_ERROR(OB_ERR_VIEW_RECURSIVE, view_db_name_.length(), view_db_name_.ptr(),
                     view_name_.length(), view_name_.ptr());
    }
  } else {
    // 原来的检测逻辑存在一个问题，对于开头的这个例子，不应该在创建v3时报错，或者说v1和v2相互引用的情况就不应该出现
    // 而是应该在create or replace v1/v2导致v1和v2相互引用时报错。
    // 虽然现在加了前面这个检测逻辑，在创建视图v时检查定义展开后没有出现v可以避免出现相互引用，
    // 但是原来的检测逻辑也要保留。如果升级前创建了存在循环的视图，升级后select from这个视图，在下面报错。
    do {
      if (OB_UNLIKELY(view_item.ref_id_ == cur_resolver->current_view_item.ref_id_)) {
        ret = OB_ERR_VIEW_RECURSIVE;
        const ObString &db_name = cur_resolver->current_view_item.database_name_;
        const ObString &tbl_name = cur_resolver->current_view_item.table_name_;
        LOG_USER_ERROR(OB_ERR_VIEW_RECURSIVE, db_name.length(), db_name.ptr(),
                       tbl_name.length(), tbl_name.ptr());
      } else {
        cur_resolver = cur_resolver->parent_view_resolver_;
      }
    } while(OB_SUCC(ret) && cur_resolver != NULL);
  }
  return ret;
}

int ObViewTableResolver::resolve_generate_table(const ParseNode &table_node, const ParseNode *alias_node, TableItem *&table_item)
{
  int ret = OB_SUCCESS;
  ObViewTableResolver view_table_resolver(params_, view_db_name_, view_name_);
  //from子查询和当前查询属于平级，因此current level和当前保持一致
  view_table_resolver.set_current_level(current_level_);
  view_table_resolver.set_current_view_level(current_view_level_);
  view_table_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
  view_table_resolver.set_parent_view_resolver(parent_view_resolver_);
  view_table_resolver.set_current_view_item(current_view_item);
  if (OB_FAIL(view_table_resolver.set_cte_ctx(cte_ctx_, true, true))) {
    LOG_WARN("set cte ctx to child resolver failed", K(ret));
  } else if (OB_FAIL(add_cte_table_to_children(view_table_resolver))) {
    LOG_WARN("add CTE table to children failed", K(ret));
  } else if (OB_FAIL(do_resolve_generate_table(table_node, alias_node, view_table_resolver, table_item))) {
    LOG_WARN("do resolve generate table failed", K(ret));
  }
  return ret;
}

// use_sys_tenant 标记是否需要以系统租户的身份获取schema
int ObViewTableResolver::check_need_use_sys_tenant(bool &use_sys_tenant) const
{
  int ret = OB_SUCCESS;

  // 若当前已经是系统租户, 则忽略
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is null");
  } else if (OB_SYS_TENANT_ID == session_info_->get_effective_tenant_id()) {
    use_sys_tenant = false;
  } else {
    use_sys_tenant = true;
  }

  // 若当前stmt不是系统视图展开的, 则忽略
  if (OB_SUCC(ret) && use_sys_tenant) {
    if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(), current_view_item.ref_id_, table_schema))) {
      LOG_WARN("fail to get table_schema", K(ret));
    } else if (NULL == table_schema) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table_schema should not be NULL", K(ret));
    } else if (!table_schema->is_sys_view()) {
      use_sys_tenant = false;
    }
  }

  return ret;
}

int ObViewTableResolver::check_in_sysview(bool &in_sysview) const
{
  int ret = OB_SUCCESS;
  in_sysview = is_sys_view(current_view_item.ref_id_);
  return ret;
}

// construct select item from select_expr
int ObViewTableResolver::set_select_item(SelectItem &select_item, bool is_auto_gen)
{
  int ret = OB_SUCCESS;
  ObCollationType cs_type = CS_TYPE_INVALID;
  ObSelectStmt *select_stmt = get_select_stmt();

  if (OB_ISNULL(select_stmt) || OB_ISNULL(session_info_) || OB_ISNULL(select_item.expr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("select stmt is null", K_(session_info), K(select_stmt), K_(select_item.expr));
  } else if (is_create_view_ && !select_item.is_real_alias_) {
    if (OB_FAIL(ObSelectResolver::set_select_item(select_item, is_auto_gen))) {
      LOG_WARN("set select item failed", K(ret));
    }
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (select_item.is_real_alias_
             && OB_FAIL(ObSQLUtils::check_column_name(cs_type, select_item.alias_name_, true))) {
    // Only check real alias here,
    // auto generated alias will be checked in ObSelectResolver::check_auto_gen_column_names().
    LOG_WARN("fail to make field name", K(ret));
  } else if (OB_FAIL(select_stmt->add_select_item(select_item))) {
    LOG_WARN("add select item to select stmt failed", K(ret));
  }
  return ret;
}

int ObViewTableResolver::resolve_subquery_info(const ObIArray<ObSubQueryInfo> &subquery_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is null");
  } else if (current_level_ + 1 >= OB_MAX_SUBQUERY_LAYER_NUM && subquery_info.count() > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "too many levels of subqueries");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_info.count(); i++) {
    const ObSubQueryInfo &info = subquery_info.at(i);
    ObViewTableResolver subquery_resolver(params_, view_db_name_, view_name_);
    subquery_resolver.set_current_level(current_level_ + 1);
    subquery_resolver.set_current_view_level(current_view_level_);
    subquery_resolver.set_parent_namespace_resolver(this);
    subquery_resolver.set_parent_view_resolver(parent_view_resolver_);
    subquery_resolver.set_current_view_item(current_view_item);
    set_query_ref_exec_params(info.ref_expr_ == NULL ? NULL : &info.ref_expr_->get_exec_params());
    if (OB_FAIL(add_cte_table_to_children(subquery_resolver))) {
      LOG_WARN("add CTE table to children failed", K(ret));
    } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
      subquery_resolver.set_parent_aggr_level(info.parents_expr_info_.has_member(IS_AGG) ?
          current_level_ : parent_aggr_level_);
    }
    if (OB_SUCC(ret) && OB_FAIL(do_resolve_subquery_info(info, subquery_resolver))) {
      LOG_WARN("do resolve subquery info failed", K(ret));
    }
    set_query_ref_exec_params(NULL);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
