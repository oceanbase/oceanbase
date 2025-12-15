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
#include "sql/rewrite/ob_expand_aggregate_utils.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "lib/utility/ob_template_utils.h"
#include "sql/resolver/mv/ob_mv_provider.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObMVChecker::check_mv_fast_refresh_type(const ObSelectStmt *view_stmt,
                                            ObIAllocator *allocator,
                                            ObSchemaChecker *schema_checker,
                                            ObStmtFactory *stmt_factory,
                                            ObRawExprFactory *expr_factory,
                                            ObSQLSessionInfo *session_info,
                                            const ObTableSchema &container_table_schema,
                                            const bool need_on_query_computation,
                                            ObMVRefreshableType &refresh_type,
                                            FastRefreshableNotes &note,
                                            ObIArray<std::pair<ObRawExpr*, int64_t>> &fast_refresh_dependent_columns,
                                            ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *copied_stmt = NULL;
  refresh_type = OB_MV_REFRESH_INVALID;
  ObTableReferencedColumnsInfo table_referenced_columns_info;
  if (OB_ISNULL(view_stmt) || OB_ISNULL(stmt_factory)
      || OB_ISNULL(expr_factory) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(view_stmt), K(expr_factory), K(session_info));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*stmt_factory, *expr_factory,
                                                      view_stmt, copied_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(ObMVProvider::transform_mv_def_stmt(copied_stmt,
                                                         allocator,
                                                         schema_checker,
                                                         session_info,
                                                         expr_factory,
                                                         stmt_factory))) {
    LOG_WARN("failed to transform mv stmt", K(ret));
  } else if (OB_FAIL(table_referenced_columns_info.init())) {
    LOG_WARN("failed to init table referenced columns info", KR(ret));
  } else {
    ObMVChecker checker(*static_cast<ObSelectStmt *>(copied_stmt), *expr_factory, session_info,
                        container_table_schema, need_on_query_computation, note,
                        fast_refresh_dependent_columns,&table_referenced_columns_info);
    if (OB_FAIL(pre_process_view_stmt(expr_factory, session_info, checker.get_gen_cols(), *static_cast<ObSelectStmt *>(copied_stmt)))) {
      LOG_WARN("failed to pre process view stmt", K(ret));
    } else if (OB_FAIL(checker.check_mv_refresh_type())) {
      LOG_WARN("failed to check mv refresh type", K(ret));
    } else if (OB_FAIL(table_referenced_columns_info.convert_to_required_columns_infos(required_columns_infos))) {
      LOG_WARN("failed to convert to required columns infos", K(ret));
    } else {
      refresh_type = checker.get_refersh_type();
      LOG_INFO("check mv fast refresh type", KR(ret), K(refresh_type), K(required_columns_infos));
    }
  }
  return ret;
}

void ObMVChecker::reset()
{
  refresh_type_ = OB_MV_REFRESH_INVALID;
  mlog_tables_.reuse();
  expand_aggrs_.reuse();
  marker_idx_ = OB_INVALID_INDEX;
  child_refresh_types_.reuse();
  refresh_dep_columns_.reuse();
  stmt_idx_ = OB_INVALID_INDEX;
}

int ObMVChecker::check_mv_refresh_type()
{
  reset();
  return check_mv_stmt_refresh_type(stmt_, refresh_type_);
}

int ObMVChecker::check_mv_stmt_refresh_type(const ObSelectStmt &stmt,
                                            ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  refresh_type = OB_MV_REFRESH_INVALID;
  bool is_valid = false;
  if (OB_FAIL(check_mv_stmt_refresh_type_basic(stmt, is_valid))) {
    LOG_WARN("failed to check mv refresh type basic", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (stmt.is_set_stmt()) {
    if (OB_FAIL(check_union_all_refresh_type(stmt, refresh_type))) {
      LOG_WARN("failed to check mav refresh type", K(ret));
    }
  } else if (stmt.has_group_by()) {
    if (OB_FAIL(check_mav_refresh_type(stmt, refresh_type))) {
      LOG_WARN("failed to check mav refresh type", K(ret));
    }
  } else if (OB_FAIL(check_mjv_refresh_type(stmt, refresh_type))) {
    LOG_WARN("failed to check mjv refresh type", K(ret));
  }
  if (OB_SUCC(ret) && !IS_VALID_FAST_REFRESH_TYPE(refresh_type)) {
    refresh_dep_columns_.reuse();
  }
  LOG_TRACE("finish check mv refresh type", K(refresh_type));
  return ret;
}

int ObMVChecker::check_mv_stmt_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_rowid = false;
  bool is_deterministic_query = false;
  bool has_cur_time = false;
  bool table_type_valid = false;
  bool has_non_proctime_table = false;
  is_valid = true;
  if (stmt.is_set_stmt() && is_child_stmt(&stmt)) {
    // child stmt is set stmt, has multi-level set stmt
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with set operators UNION/INTERSECT/EXCEPT/MINUS is not supported");
  } else if (stmt.has_subquery()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with subquery is not supported");
  } else if (stmt.is_distinct()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with DISTINCT operator is not supported");
  } else if (stmt.has_order_by() || stmt.has_limit()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with order by or limit operators is not supported");
  } else if ((stmt.has_rollup() || stmt.has_cube() || stmt.has_grouping_sets() ||
              stmt.get_having_expr_size() > 0)) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with rollup/grouping sets/cube/having operators is not supported");
  } else if (stmt.is_hierarchical_query()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("hierarchical query is not supported");
  } else if (stmt.has_window_function()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with window functions is not supported");
  } else if (stmt.is_contains_assignment()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with assignment operators is not supported");
  } else if (OB_FAIL(stmt.has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (OB_FAIL(check_has_rowid_exprs(&stmt, has_rowid))) {
    LOG_WARN("failed to check stmt has rowid exprs", K(ret));
  } else if (has_rownum || has_rowid || stmt.has_ora_rowscn()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with rownum/rowid/ora_rowscn pseudo columns is not supported");
  } else if (OB_FAIL(stmt.is_query_deterministic(is_deterministic_query))) {
    LOG_WARN("failed to check mv stmt use special expr", K(ret));
  } else if (!is_deterministic_query) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("non-deterministic query is not supported");
  } else if (OB_FAIL(stmt.has_special_expr(CNT_CUR_TIME, has_cur_time))) {
    LOG_WARN("failed to check stmt has special expr", K(ret));
  } else if (has_cur_time) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with current time expression is not supported");
  } else if (OB_FAIL(check_mv_table_type_valid(stmt, table_type_valid))) {
    LOG_WARN("failed to check mv table mlog", K(ret));
  } else if (!table_type_valid) {
    is_valid = false;
  } else if (is_child_stmt(&stmt)) {
    // do nothing, check below is ONLY for ROOT STMT
  } else if (OB_FAIL(check_mv_has_non_proctime_table(stmt, has_non_proctime_table))) {
    LOG_WARN("failed to check mv has non proctime table", K(ret));
  } else if (!has_non_proctime_table) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with all tables are AS OF PROCTIME() is not supported");
  }
  return ret;
}

int ObMVChecker::check_mv_table_type_valid(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObIArray<TableItem*> &tables = stmt.get_table_items();
  const TableItem *table = NULL;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < tables.count(); ++i) {
    if (OB_ISNULL(table = tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", K(ret), KPC(table));
    } else if (OB_UNLIKELY(!(table->is_basic_table() || (table->is_generated_table() && table->is_mv_proctime_table_)))) {
      is_valid = false;
      fast_refreshable_error_.assign_fmt("query with %s is not supported, only basic table "
                                         "and materialized view are supported",
                                         get_table_type_str(table->type_));
    } else if (OB_UNLIKELY(!(ObTableType::USER_TABLE == table->table_type_
                             || ObTableType::MATERIALIZED_VIEW == table->table_type_
                             || table->is_generated_table()))) {
      is_valid = false;
      fast_refreshable_error_.assign_fmt("query with %s is not supported, only basic table "
                                         "and materialized view are supported",
                                         ob_table_type_str(table->table_type_));
    } else if (OB_NOT_NULL(table->flashback_query_expr_)) {
      is_valid = false;
      fast_refreshable_error_.assign_fmt("flashback query is not supported");
    } else if (OB_UNLIKELY(!table->part_ids_.empty())) {
      is_valid = false;
      fast_refreshable_error_.assign_fmt("query with partition specification for table is not supported");
    } else if (need_on_query_computation_ && table->table_type_ == ObTableType::MATERIALIZED_VIEW) {
      is_valid = false;
      fast_refreshable_error_.assign_fmt("query with enable on query computation for nested mview is not supported");
    }
  }
  return ret;
}

int ObMVChecker::check_mv_has_non_proctime_table(const ObSelectStmt &stmt, bool &has_non_proctime_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  has_non_proctime_table = false;
  if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_non_proctime_table && i < stmt.get_table_size(); ++i) {
    const TableItem *table = stmt.get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table", K(ret), K(i));
    } else if (!table->is_mv_proctime_table_) {
      has_non_proctime_table = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_non_proctime_table && i < child_stmts.count(); ++i) {
    const ObSelectStmt *child_stmt = child_stmts.at(i);
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child stmt", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(check_mv_has_non_proctime_table(*child_stmt, has_non_proctime_table)))) {
      LOG_WARN("failed to check mv has non proctime table", K(ret));
    }
  }
  return ret;
}

int ObMVChecker::check_has_rowid_exprs(const ObDMLStmt *stmt, bool &has_rowid)
{
  int ret = OB_SUCCESS;
  has_rowid = false;
  ObSEArray<ObRawExpr*, 16> exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt->get_relation_exprs(exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  while (OB_SUCC(ret) && !has_rowid && !exprs.empty()) {
    ObRawExpr *expr = NULL;
    if (OB_FAIL(exprs.pop_back(expr))) {
      LOG_WARN("failed to pop back", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret));
    } else if (expr->has_flag(IS_ROWID)) {
      has_rowid = true;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_FAIL(exprs.push_back(expr->get_param_expr(i)))) {
          LOG_WARN("failed to push back param expr", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

const char *ObMVChecker::get_table_type_str(const TableItem::TableType type)
{
  const char *type_str = NULL;
  switch (type) {
  case TableItem::BASE_TABLE:
    type_str = "BASE TABLE";
    break;
  case TableItem::ALIAS_TABLE:
    type_str = "ALIAS TABLE";
    break;
  case TableItem::GENERATED_TABLE:
    type_str = "GENERATED TABLE";
    break;
  case TableItem::JOINED_TABLE:
    type_str = "JOIN TABLE";
    break;
  case TableItem::CTE_TABLE:
    type_str = "CTE TABLE";
    break;
  case TableItem::FUNCTION_TABLE:
    type_str = "FUNCTION TABLE";
    break;
  case TableItem::TRANSPOSE_TABLE:
    type_str = "TRANSPOSE TABLE";
    break;
  case TableItem::TEMP_TABLE:
    type_str = "TEMP TABLE";
    break;
  case TableItem::LINK_TABLE:
    type_str = "LINK TABLE";
    break;
  case TableItem::JSON_TABLE:
    type_str = "JSON TABLE";
    break;
  case TableItem::VALUES_TABLE:
    type_str = "VALUES TABLE";
    break;
  case TableItem::LATERAL_TABLE:
    type_str = "LATERAL TABLE";
    break;
  default:
    type_str = "UNKNOWN TABLE TYPE";
    break;
  }

  return type_str;
}

int ObMVChecker::check_mv_dependency_mlog_tables(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObSqlSchemaGuard *sql_schema_guard = NULL;
  const uint64_t tenant_id = MTL_ID();
  uint64_t data_version = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (OB_ISNULL(stmt.get_query_ctx()) || OB_ISNULL(sql_schema_guard = &stmt.get_query_ctx()->sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(sql_schema_guard));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (stmt.get_table_size() == 0) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("table size is 0");
  } else {
    is_valid = true;
    const ObIArray<TableItem*> &tables = stmt.get_table_items();
    const share::schema::ObTableSchema *table_schema = NULL;
    const share::schema::ObTableSchema *mlog_schema = NULL;
    const TableItem *table = NULL;
    ObSEArray<ColumnItem, 4> table_columns;
    ObSEArray<uint64_t, 4> table_col_ids;
    bool enable_auto_build_mlog = false;
    if (((data_version >= MOCK_DATA_VERSION_4_3_5_4 && data_version < DATA_VERSION_4_4_0_0)
         || (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0)
         || (data_version >= DATA_VERSION_4_5_1_0))
        && tenant_config.is_valid() && tenant_config->enable_mlog_auto_maintenance) {
      enable_auto_build_mlog = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < tables.count(); ++i) {
      table_columns.reuse();
      table_col_ids.reuse();
      if (OB_ISNULL(table = tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table", K(ret), KPC(table));
      } else if (table->is_mv_proctime_table_) {
        // do nothing, no need to check mlog table for proctime table
      } else if (OB_NOT_NULL(table_referenced_columns_info_) && enable_auto_build_mlog) {
        for (int64_t j = 0; OB_SUCC(ret) && j < refresh_dep_columns_.count(); ++j) {
          if (OB_FAIL(ObOptimizerUtil::extract_column_ids(refresh_dep_columns_.at(j).first,
                                                          table->table_id_,
                                                          table_col_ids))) {
            LOG_WARN("failed to extract dependent column id", K(ret), KPC(refresh_dep_columns_.at(j).first));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(stmt.get_column_ids(table->table_id_, table_col_ids))) {
          LOG_WARN("failed to get column items", K(ret));
        } else if (OB_FAIL(table_referenced_columns_info_->record_table_referenced_columns(table->ref_id_, table_col_ids))) {
          LOG_WARN("failed to record table referenced columns", K(ret), KPC(table));
        }
      } else if (OB_FAIL(sql_schema_guard->get_table_schema(table->ref_id_, table_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(table_schema));
      } else if (OB_FAIL(sql_schema_guard->get_table_mlog_schema(table->ref_id_, mlog_schema))
                 || OB_ISNULL(mlog_schema) || !mlog_schema->is_available_mlog()) {
        if (OB_TABLE_NOT_EXIST == ret || OB_ERR_TABLE_NO_MLOG == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to get mlog schema", K(ret), K(table_schema->get_table_name()));
        } else {
          is_valid = false;
          fast_refreshable_error_.assign_fmt("base table %.*s doesn't have mlog table",
                                             table->get_table_name().length(),
                                             table->get_table_name().ptr());
        }
      } else if (OB_FAIL(stmt.get_column_items(table->table_id_, table_columns))) {
        LOG_WARN("failed to get column items", K(ret));
      } else if (OB_FAIL(check_mlog_table_valid(table_schema,
                                                table,
                                                table_columns,
                                                *mlog_schema,
                                                sql_schema_guard->get_schema_guard(),
                                                is_valid))) {
        LOG_WARN("failed to get and check mlog table", K(ret));
      } else if (!is_valid) {
        // do nothing
      } else if (OB_FAIL(mlog_tables_.push_back(std::make_pair(table, mlog_schema)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

// get mlog table schema, check columns exists in mlog table
bool ObMVChecker::check_mlog_table_valid(const share::schema::ObTableSchema *table_schema,
                                         const TableItem *table_item,
                                         const ObIArray<ColumnItem> &columns,
                                         const share::schema::ObTableSchema &mlog_schema,
                                         ObSchemaGetterGuard *schema_guard,
                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  uint64_t mlog_cid = OB_INVALID_ID;
  ObSEArray<uint64_t, 4> unique_col_ids;
  if (OB_ISNULL(table_schema) || OB_ISNULL(table_item) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema), K(schema_guard), K(table_item));
  } else if (OB_FAIL(get_table_rowkey_ids(table_schema, schema_guard, unique_col_ids))) {
    LOG_WARN("failed to get table rowkey ids", K(ret), KPC(table_schema));
  }
  for (int i = 0; is_valid && OB_SUCC(ret) && i < unique_col_ids.count(); ++i) {
    is_valid = NULL != mlog_schema.get_column_schema(unique_col_ids.at(i))
               || OB_HIDDEN_PK_INCREMENT_COLUMN_ID == unique_col_ids.at(i);
    if (!is_valid) {
      ObString column_name;
      bool column_exist = false;
      table_schema->get_column_name_by_column_id(unique_col_ids.at(i), column_name, column_exist);
      if (OB_UNLIKELY(!column_exist)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column name", K(ret), K(unique_col_ids.at(i)), KPC(table_schema));
      } else {
        fast_refreshable_error_.assign_fmt("all primary keys and partition keys of table %.*s are required in the corresponding mlog table, missing column %s",
                                           table_item->get_table_name().length(), table_item->get_table_name().ptr(), column_name.ptr());
      }
    }
    LOG_DEBUG("check mlog_table unique column is valid", K(is_valid), K(i), K(mlog_cid), K(unique_col_ids));
  }
  for (int i = 0; is_valid && OB_SUCC(ret) && i < columns.count(); ++i) {
    mlog_cid = ObTableSchema::gen_mlog_col_id_from_ref_col_id(columns.at(i).base_cid_);
    is_valid = NULL != mlog_schema.get_column_schema(mlog_cid);
    if (!is_valid) {
      fast_refreshable_error_.assign_fmt("column %s of table %.*s used in mv is required in the corresponding mlog table", columns.at(i).column_name_.ptr(), table_item->get_table_name().length(), table_item->get_table_name().ptr());
    }
    LOG_DEBUG("check mlog_table column is valid", K(is_valid), K(i), K(mlog_cid), K(columns));
  }
  return ret;
}

int ObMVChecker::check_mav_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool has_outer_join = false;
  refresh_type = OB_MV_REFRESH_INVALID;
  if (OB_FAIL(check_mav_refresh_type_basic(stmt, is_valid))) {
    LOG_WARN("failed to check refresh type basic", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (OB_FAIL(check_and_expand_mav_aggrs(stmt, expand_aggrs_, is_valid))) {
    LOG_WARN("failed to check mav aggr valid", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (!stmt.is_single_table_stmt() &&
             OB_FAIL(check_join_mv_fast_refresh_valid(stmt, true, is_valid, has_outer_join))) {
    LOG_WARN("failed to check join mv fast refresh valid", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (OB_FAIL(check_mv_dependency_mlog_tables(stmt, is_valid))) {
    LOG_WARN("failed to check mv table mlog", KR(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (stmt.is_single_table_stmt()) { // single table MAV
    refresh_type = OB_MV_FAST_REFRESH_SIMPLE_MAV;
  } else { // join MAV
    refresh_type = has_outer_join ? OB_MV_FAST_REFRESH_OUTER_JOIN_MAV : OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV;
  }
  return ret;
}

int ObMVChecker::check_mav_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const ObAggFunRawExpr *default_count = NULL;  // count(*) need for non scalar group by
  if (!stmt.is_scala_group_by() && OB_FAIL(get_mav_default_count(stmt.get_aggr_items(), default_count))) {
    LOG_WARN("failed to check target aggr exist", K(ret));
  } else if (!stmt.is_scala_group_by() && NULL == default_count) {
    fast_refreshable_error_.assign_fmt("a count(*) item is required to be added to the select item list");
    is_valid = false;
  } else if (lib::is_mysql_mode() && OB_FAIL(check_is_standard_group_by(stmt, is_valid))) {
    LOG_WARN("failed to check is standard group by", K(ret));
  } else if (!is_valid) {
    fast_refreshable_error_.assign_fmt("the select item list contains columns that are not in the group by clause");
  } else if (is_valid) {
    // check group by exprs exists in select list
    const ObIArray<ObRawExpr*> &group_exprs = stmt.get_group_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs.count(); ++i) {
      ObRawExpr *group_expr = group_exprs.at(i);
      if (OB_ISNULL(group_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i), K(group_exprs));
      } else if (stmt.check_is_select_item_expr(group_expr)) {
        // do nothing
      } else if (OB_FAIL(add_dep_columns_no_dup(group_expr))) {
        LOG_WARN("failed to push back dependent column", K(ret));
      }
    }
  }
  return ret;
}

// for mysql mode, check is standard group by
int ObMVChecker::check_is_standard_group_by(const ObSelectStmt &stmt, bool &is_standard)
{
  int ret = OB_SUCCESS;
  is_standard = true;
  hash::ObHashSet<uint64_t> expr_set;
  if (OB_FAIL(expr_set.create(32))) {
    LOG_WARN("failed to create expr set", K(ret));
  } else {
    const ObIArray<ObRawExpr*> &group_exprs = stmt.get_group_exprs();
    const ObIArray<SelectItem> &select_items = stmt.get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs.count(); ++i) {
      uint64_t key = reinterpret_cast<uint64_t>(group_exprs.at(i));
      if (OB_FAIL(expr_set.set_refactored(key, 0))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to add expr into set", K(ret));
        }
      }
    }
    for (int64_t i = 0; is_standard && OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (OB_FAIL(is_standard_select_in_group_by(expr_set, select_items.at(i).expr_, is_standard))) {
        LOG_WARN("failed to push back null safe equal expr", K(ret));
      } else if (!is_standard) {
        LOG_TRACE("expr can not use in select for group by", K(is_standard), K(i), KPC(select_items.at(i).expr_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr_set.destroy())) {
        LOG_WARN("failed to destroy stmt expr set", K(ret));
      }
    }
  }
  return ret;
}

int ObMVChecker::is_standard_select_in_group_by(const hash::ObHashSet<uint64_t> &expr_set,
                                                const ObRawExpr *expr,
                                                bool &is_standard)
{
  int ret = OB_SUCCESS;
  is_standard = true;
  int tmp_ret = OB_SUCCESS;
  uint64_t key = reinterpret_cast<uint64_t>(expr);
  bool is_const_inherit = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr));
  } else if (expr->is_aggr_expr() || !expr->has_flag(CNT_COLUMN)) {
    /* do nothing */
  } else if (OB_HASH_EXIST == (tmp_ret = expr_set.exist_refactored(key))) {
    /* do nothing */
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
    ret = tmp_ret;
    LOG_WARN("failed to check hash set exists", K(ret));
  } else if (expr->is_column_ref_expr()) {
    is_standard = false;
  } else if (OB_FAIL(expr->is_const_inherit_expr(is_const_inherit, true))) {
    LOG_WARN("failed to check is const inherit expr", K(ret));
  } else if (!is_const_inherit) {
    is_standard = false;
  } else {
    for (int64_t i = 0; is_standard && OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_standard_select_in_group_by(expr_set, expr->get_param_expr(i), is_standard)))) {
        LOG_WARN("failed to visit first", K(ret));
      }
    }
  }
  return ret;
}

int ObMVChecker::check_and_expand_mav_aggrs(const ObSelectStmt &stmt,
                                            ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObAggFunRawExpr*, 8> all_aggrs;
  const ObIArray<ObAggFunRawExpr*> &aggrs = stmt.get_aggr_items();
  if (OB_FAIL(all_aggrs.assign(aggrs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    bool need_check_min_max_aggr = true;
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < aggrs.count(); ++i) {
      if (OB_FAIL(check_and_expand_mav_aggr(stmt, aggrs.at(i), all_aggrs, expand_aggrs,
                                            need_check_min_max_aggr,
                                            is_valid))) {
        LOG_WARN("failed to check and expand mav aggr", K(ret));
      }
    }
  }
  return ret;
}

// fast refres can not support for these funs if there is only insert dml on base table.
int ObMVChecker::check_and_expand_mav_aggr(const ObSelectStmt &stmt,
                                           ObAggFunRawExpr *aggr,
                                           ObIArray<ObAggFunRawExpr*> &all_aggrs,
                                           ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs,
                                           bool &need_check_min_max_aggr,
                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(aggr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(aggr));
  } else if (aggr->is_param_distinct() || aggr->in_inner_stmt()) { // 这里判断完全没有has_nested_aggr_的时候in_inner_stmt 才会为true
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with nested aggregate functions or distinct keyword is not supported");
  } else {
    const int64_t orig_aggr_count = all_aggrs.count();
    switch (aggr->get_expr_type()) {
      case T_FUN_COUNT: {
        if (!stmt.check_is_select_item_expr(aggr)) {
          fast_refreshable_error_.assign_fmt("a standalone count expression is required in the select item list when using expressions that derive from that count operation");
          LOG_WARN("need count item", KPC(aggr));
        } else {
          is_valid = true;
        }
        break;
      }
      case T_FUN_SUM: {
        const ObAggFunRawExpr *dependent_aggr = NULL;
        if (!stmt.check_is_select_item_expr(aggr)) {
          fast_refreshable_error_.assign_fmt("a standalone sum expression is required in the select item list when using expressions that derive from that sum operation");
        } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(stmt, aggr->get_param_expr(0), dependent_aggr))) {
          LOG_WARN("failed to check sum aggr fast refresh valid", K(ret));
        } else if (NULL == dependent_aggr) {
          fast_refreshable_error_.assign_fmt("when using sum/avg/stddev/variance functions, a standalone count function of the corresponding column is required in the select item list");
        } else {
          is_valid = true;
        }
        break;
      }
      case T_FUN_AVG:
      case T_FUN_STDDEV:
      case T_FUN_VARIANCE:  {
        ObRawExpr *replace_expr = NULL;
        ObExpandAggregateUtils expand_aggr_utils(expr_factory_, session_info_);
        expand_aggr_utils.set_expand_for_mv();
        if (OB_FAIL(expand_aggr_utils.expand_common_aggr_expr(aggr, replace_expr, all_aggrs))) {
          LOG_WARN("failed to expand common aggr expr", K(ret));
        } else if (all_aggrs.count() != orig_aggr_count
                   && OB_FAIL(try_replace_equivalent_count_aggr(stmt, orig_aggr_count, all_aggrs, replace_expr))) {
          LOG_WARN("failed to try replace equivalent count aggr ", K(ret));
        } else if (all_aggrs.count() != orig_aggr_count) {
          /* expand aggr generate new aggr, can not fast refresh */
          is_valid = false;
          LOG_TRACE("aggr can not fast refresh", KPC(aggr), KPC(replace_expr), K(orig_aggr_count), K(all_aggrs));
          fast_refreshable_error_.assign_fmt("when using sum/avg/stddev/variance functions, a standalone count function of the corresponding column is required in the select item list");
          ObOptimizerUtil::revert_items(all_aggrs, orig_aggr_count);
        } else if (OB_FAIL(expand_aggrs.push_back(std::make_pair(aggr, replace_expr)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          /* need not check this aggr in select item, expand aggr will check when call this function by itself */
          is_valid = true;
        }
        break;
      }
      case T_FUN_MAX:
      case T_FUN_MIN: {
        if (!need_check_min_max_aggr) {
          is_valid = true;
        } else if (OB_FAIL(check_min_max_aggr_fast_refresh_valid(stmt, is_valid))) {
          LOG_WARN("failed to check min max aggr fast refresh valid", K(ret));
        } else {
          need_check_min_max_aggr = false;
        }
        break;
      }
      default : {
        is_valid = false;
        fast_refreshable_error_.assign_fmt("the aggregate function type is not supported yet (only count/sum/avg/stddev/variance/min/max is supported)");
        break;
      }
    }
  }
  return ret;
}

int ObMVChecker::check_min_max_aggr_fast_refresh_valid(const ObSelectStmt &stmt,
                                                       bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObQueryCtx *query_ctx = NULL;
  const TableItem *table_item = NULL;
  const ObTableSchema *index_schema = NULL;
  ObSEArray<uint64_t, 8> group_by_col_ids;
  if (OB_ISNULL(query_ctx = stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(query_ctx));
  } else if (!stmt.is_single_table_stmt()) {
    fast_refreshable_error_.assign_fmt("min/max aggregation function only support for single table query");
  } else if (stmt.is_scala_group_by()) {
    fast_refreshable_error_.assign_fmt("min/max aggregation function not support for scala group by");
  } else if (OB_ISNULL(table_item = stmt.get_table_item(0))
             || OB_UNLIKELY(!table_item->is_basic_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), KPC(table_item));
  } else if (OB_FAIL(get_group_by_column_ids(table_item->table_id_,
                                             stmt.get_group_exprs(),
                                             get_gen_cols(),
                                             group_by_col_ids))) {
    LOG_WARN("failed to get group by column ids", K(ret));
  } else if (group_by_col_ids.empty()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("there is no valid index for fast refresh min/max aggregation function");
  } else if (OB_FAIL(get_valid_index_for_cols(query_ctx->sql_schema_guard_,
                                              table_item->ref_id_,
                                              group_by_col_ids,
                                              index_schema))) {
    LOG_WARN("failed to get valid index for cols", K(ret));
  } else if (NULL == index_schema) {
    fast_refreshable_error_.assign_fmt("there is no valid index for fast refresh min/max aggregation function");
  } else {
    is_valid = true;
  }
  return ret;
}

int ObMVChecker::get_group_by_column_ids(const uint64_t log_table_id,
                                         const ObIArray<ObRawExpr*> &group_by_exprs,
                                         const ObIArray<ObColumnRefRawExpr*> &gen_cols,
                                         ObIArray<uint64_t> &group_by_col_ids)
{
  int ret = OB_SUCCESS;
  group_by_col_ids.reuse();
  bool is_valid = true;
  ObRawExpr *gby_expr = NULL;
  const bool skip_extract_real_dep_expr = false;
  for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < group_by_exprs.count(); ++i) {
    if (OB_ISNULL(gby_expr = group_by_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(group_by_exprs));
    } else if (gby_expr->is_column_ref_expr()) {
      ret = add_var_to_array_no_dup(group_by_col_ids, static_cast<ObColumnRefRawExpr*>(gby_expr)->get_column_id());
    } else {
      is_valid = false;
      ObRawExpr *target_expr = gby_expr;
      ObRawExpr *dep_expr = NULL;
      for (int64_t j = 0; !is_valid && OB_SUCC(ret) && j < gen_cols.count(); ++j) {
        if (OB_ISNULL(gen_cols.at(j)) || OB_ISNULL(dep_expr = gen_cols.at(j)->get_dependant_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL", K(ret), K(j), K(gen_cols));
        } else if (OB_FAIL(ObOptimizerUtil::check_and_extract_matched_gen_col_exprs(dep_expr,
                                                                                    target_expr,
                                                                                    is_valid,
                                                                                    NULL,
                                                                                    skip_extract_real_dep_expr))) {
          LOG_WARN("fail to check and extract matched gen col exprs", K(ret));
        } else if (!is_valid) {
          /* do nothing */
        } else if (OB_FAIL(add_var_to_array_no_dup(group_by_col_ids, gen_cols.at(j)->get_column_id()))) {
          LOG_WARN("fail to add var to array no dup", K(ret));
        } else {
          is_valid = true;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !is_valid) {
    group_by_col_ids.reuse();
  }
  return ret;
}

int ObMVChecker::get_valid_index_for_cols(ObSqlSchemaGuard &sql_schema_guard,
                                          const uint64_t phy_table_id,
                                          const ObIArray<uint64_t> &group_by_col_ids,
                                          const ObTableSchema *&index_schema)
{
  int ret = OB_SUCCESS;
  index_schema = NULL;
  uint64_t tids[OB_MAX_AUX_TABLE_PER_MAIN_TABLE + 1];
  int64_t table_index_aux_count = OB_MAX_AUX_TABLE_PER_MAIN_TABLE + 1;
  if (OB_FAIL(sql_schema_guard.get_can_read_index_array(phy_table_id,
                                                        tids,
                                                        table_index_aux_count,
                                                        true   /*mv*/,
                                                        true   /*global index*/,
                                                        false  /*domain index*/,
                                                        false  /*spatial index*/,
                                                        false  /*vector index*/))) {
    LOG_WARN("failed to get can read index", K(ret));
  } else if (table_index_aux_count > OB_MAX_AUX_TABLE_PER_MAIN_TABLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table index or index aux count is invalid", K(ret), K(phy_table_id), K(table_index_aux_count));
  }

  for (int64_t i = -1; NULL == index_schema && OB_SUCC(ret) && i < table_index_aux_count; ++i) {
    uint64_t index_id = -1 == i ? phy_table_id : tids[i];
    bool is_valid = false;
    const ObTableSchema *cur_index_schema = NULL;
    uint64_t column_id = OB_INVALID_ID;
    if (OB_FAIL(sql_schema_guard.get_table_schema(index_id, cur_index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(cur_index_schema)) {
      /* ignore empty index schema */
    } else if (cur_index_schema->get_rowkey_info().get_size() < group_by_col_ids.count()) {
      /* do nothing */
    } else {
      is_valid = true;
      for (int64_t j = 0; is_valid && OB_SUCC(ret) && j < group_by_col_ids.count(); ++j) {
        column_id = OB_INVALID_ID;
        if (OB_FAIL(cur_index_schema->get_rowkey_info().get_column_id(j, column_id))) {
          LOG_WARN("Failed to get column_id from rowkey_info", K(ret));
        } else if (ObOptimizerUtil::find_item(group_by_col_ids, column_id)) {
          /* do nothing */
        } else {
          is_valid = false;
        }
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      index_schema = cur_index_schema;
    }
  }
  return ret;
}

int ObMVChecker::try_replace_equivalent_count_aggr(const ObSelectStmt &stmt,
                                                   const int64_t orig_aggr_count,
                                                   ObIArray<ObAggFunRawExpr*> &all_aggrs,
                                                   ObRawExpr *&replace_expr)
{
  int ret = OB_SUCCESS;
  const ObAggFunRawExpr *aggr = NULL;
  const ObAggFunRawExpr *equal_aggr = NULL;
  bool aggr_not_support = false;
  ObRawExprCopier copier(expr_factory_);
  for (int64_t i = orig_aggr_count; !aggr_not_support && OB_SUCC(ret) && i < all_aggrs.count(); ++i) {
    if (OB_ISNULL(aggr = all_aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(aggr));
    } else if (T_FUN_COUNT != aggr->get_expr_type() || 1 != aggr->get_real_param_count()) {
      aggr_not_support = true;
    } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(stmt, aggr->get_param_expr(0), equal_aggr))) {
      LOG_WARN("failed to get equivalent count aggr", K(ret));
    } else if (NULL == equal_aggr) {
      aggr_not_support = true;
    } else if (OB_FAIL(copier.add_replaced_expr(aggr, equal_aggr))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }
  if (OB_SUCC(ret) && !aggr_not_support) {
    ObRawExpr *new_replace_expr = NULL;
    ObOptimizerUtil::revert_items(all_aggrs, orig_aggr_count);
    if (OB_FAIL(copier.copy_on_replace(replace_expr, new_replace_expr))) {
      LOG_WARN("failed to generate group by exprs", K(ret));
    } else {
      replace_expr = new_replace_expr;
    }
  }
  return ret;
}

bool ObMVChecker::is_basic_aggr(const ObItemType aggr_type)
{
  return  T_FUN_COUNT == aggr_type || T_FUN_SUM == aggr_type;
}

//  count(c1) is needed for refresh sum(c1)
int ObMVChecker::get_dependent_aggr_of_fun_sum(const ObSelectStmt &stmt,
                                               const ObRawExpr *sum_param,
                                               const ObAggFunRawExpr *&dep_aggr)
{
  int ret = OB_SUCCESS;
  dep_aggr = NULL;
  const ObRawExpr *check_param = NULL;
  if (OB_FAIL(get_equivalent_null_check_param(sum_param, check_param))) {
    LOG_WARN("failed to get null check param", K(ret));
  } else {
    const ObIArray<ObAggFunRawExpr*> &aggrs = stmt.get_aggr_items();
    const ObAggFunRawExpr *cur_aggr = NULL;
    const ObRawExpr *cur_check_param = NULL;
    for (int64_t i = 0; NULL == dep_aggr && OB_SUCC(ret) && i < aggrs.count(); ++i) {
      if (OB_ISNULL(cur_aggr = aggrs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (T_FUN_COUNT != cur_aggr->get_expr_type()
                 || cur_aggr->is_param_distinct()
                 || 1 != cur_aggr->get_real_param_count()) {
        /* do nothing */
      } else if (OB_FAIL(get_equivalent_null_check_param(cur_aggr->get_param_expr(0),
                                                         cur_check_param))) {
        LOG_WARN("failed to get null check param", K(ret));
      } else if (cur_check_param->same_as(*check_param)) {
        dep_aggr = cur_aggr;
      }
    }
  }
  return ret;
}

//  We need calculate sum(c1*c1) to get the value of stddev(c1).
//  To refrsh sum(c1*c1) and avoid calculate count(c1*c1), count(c1) can also used to refrsh sum(c1*c1).
//  Here try to get c1 as equivalent aggr param of c1*c1 only.
int ObMVChecker::get_equivalent_null_check_param(const ObRawExpr *param_expr,
                                                 const ObRawExpr *&check_param)
{
  int ret = OB_SUCCESS;
  check_param = NULL;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(param_expr));
  } else if (T_FUN_SYS_CAST == param_expr->get_expr_type()) {
    //  sum(cast(c1_varchar as int)) can use count(c1_varchar)
    if (OB_FAIL(SMART_CALL(get_equivalent_null_check_param(param_expr->get_param_expr(0), check_param)))) {
      LOG_WARN("failed to smart call get null check param", K(ret));
    }
  } else if (T_OP_MUL == param_expr->get_expr_type()) {
    //  sum(c1*c1) can use count(c1)
    const ObRawExpr *l_expr = NULL;
    const ObRawExpr *r_expr = NULL;
    if (OB_ISNULL(l_expr = param_expr->get_param_expr(0))
        || OB_ISNULL(r_expr = param_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(l_expr), K(r_expr));
    } else if (!l_expr->same_as(*r_expr)) {
      check_param = param_expr;
    } else if (OB_FAIL(SMART_CALL(get_equivalent_null_check_param(l_expr, check_param)))) {
      LOG_WARN("failed to smart call get null check param", K(ret));
    }
  } else {
    check_param = param_expr;
  }
  return ret;
}

int ObMVChecker::get_mav_default_count(const ObIArray<ObAggFunRawExpr*> &aggrs,
                                       const ObAggFunRawExpr *&count_aggr)
{
  int ret = OB_SUCCESS;
  count_aggr = NULL;
  const ObAggFunRawExpr *aggr = NULL;
  for (int64_t i = 0; NULL == count_aggr && OB_SUCC(ret) && i < aggrs.count(); ++i) {
    if (OB_ISNULL(aggr = aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (T_FUN_COUNT == aggr->get_expr_type() && 0 == aggr->get_real_param_count()) {
      count_aggr = aggr;
    }
  }
  return ret;
}

int ObMVChecker::check_mjv_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  refresh_type = OB_MV_COMPLETE_REFRESH;
  bool mlog_valid = true;
  bool match_major_refresh = false;
  bool has_outer_join = false;
  bool is_valid = false;
  uint64_t tenant_id = MTL_ID();
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (OB_FAIL(check_join_mv_fast_refresh_valid(stmt, false, is_valid, has_outer_join))) {
    LOG_WARN("failed to check join mv fast refresh valid", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (data_version >= DATA_VERSION_4_3_4_0 && !has_outer_join &&
             OB_FAIL(check_match_major_refresh_mv(stmt, match_major_refresh))) {
    LOG_WARN("failed to check match major refresh mv", KR(ret));
  } else if (match_major_refresh) {
    refresh_type = OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV;
  } else if (OB_FAIL(check_mv_dependency_mlog_tables(stmt, mlog_valid))) {
    LOG_WARN("failed to check mv dependency mlog tables", KR(ret));
  } else if (mlog_valid) {
    refresh_type = has_outer_join ? OB_MV_FAST_REFRESH_OUTER_JOIN_MJV : OB_MV_FAST_REFRESH_SIMPLE_MJV;
  }
  return ret;
}

int ObMVChecker::check_join_mv_fast_refresh_valid(const ObSelectStmt &stmt,
                                                  const bool for_join_mav,
                                                  bool &is_valid,
                                                  bool &has_outer_join)
{
  int ret = OB_SUCCESS;
  bool is_valid_join = 0;
  is_valid = false;
  has_outer_join = false;
  if (stmt.get_table_size() <= 0) {
    fast_refreshable_error_.assign_fmt("materialized view with empty FROM clause is not supported");
  // } else if (stmt.get_table_size() > 5) {
  //   append_fast_refreshable_note("join table size more than 5 not support");
  } else if (OB_FAIL(check_mv_join_type(stmt, is_valid_join, has_outer_join))) {
    LOG_WARN("failed to check mv join type", K(ret));
  } else if (!is_valid_join) {
    // do nothing
  } else if (need_on_query_computation_ && has_outer_join) {
    fast_refreshable_error_.assign_fmt("on query computation is not supported for materialized view with OUTER JOIN");
  } else if (for_join_mav) {
    is_valid = true;
  } else if (OB_FAIL(collect_tables_primary_key_for_select(stmt, has_outer_join))) {
    LOG_WARN("failed to collect tables primary key for select", K(ret));
  } else if (has_outer_join && OB_FAIL(collect_all_single_columns_for_select(stmt))) {
    LOG_WARN("failed to collect all single columns for select", K(ret));
  } else {
    is_valid = true;
  }
  return ret;
}

int ObMVChecker::check_mv_join_type(const ObSelectStmt &stmt, bool &is_valid_join, bool &has_outer_join)
{
  int ret = OB_SUCCESS;
  is_valid_join = true;
  const ObIArray<JoinedTable*> &joined_tables = stmt.get_joined_tables();
  bool null_side_has_non_proctime_table = false;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < joined_tables.count(); ++i) {
    bool table_is_valid = false;
    bool table_has_outer_join = false;
    bool table_null_side_has_non_proctime = false;
    ObRelIds null_side_tables;
    if (OB_FAIL(is_mv_join_type_valid(stmt,
                                      joined_tables.at(i),
                                      null_side_tables,
                                      table_is_valid,
                                      table_has_outer_join,
                                      table_null_side_has_non_proctime))) {
      LOG_WARN("failed to check mv join type valid", K(ret));
    } else {
      is_valid_join &= table_is_valid;
      has_outer_join |= table_has_outer_join;
      null_side_has_non_proctime_table |= table_null_side_has_non_proctime;
    }
  }
  if (OB_FAIL(ret) || !is_valid_join || !null_side_has_non_proctime_table) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < stmt.get_table_size(); ++i) {
      if (OB_ISNULL(stmt.get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (stmt.get_table_item(i)->is_generated_table()) {
        fast_refreshable_error_.assign_fmt("materialized view that contains both VIEW and OUTER JOIN only supports PROCTIME() table on the right side of left join (or the left side of right join)");
        is_valid_join = false;
      }
    }
  }
  return ret;
}

int ObMVChecker::is_mv_join_type_valid(const ObSelectStmt &stmt,
                                       const TableItem *table,
                                       ObRelIds &null_side_tables,
                                       bool &is_valid_join,
                                       bool &has_outer_join,
                                       bool &null_side_has_non_proctime_table)
{
  int ret = OB_SUCCESS;
  bool is_child_has_outer_join = false;
  bool is_child_null_side_has_non_proctime_table = false;
  const JoinedTable *joined_table = NULL;
  is_valid_join = true;
  has_outer_join = false;
  null_side_has_non_proctime_table = false;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table));
  } else if (table->is_basic_table() || table->is_generated_table()) {
    // do nothing
  } else if (!table->is_joined_table()) {
    fast_refreshable_error_.assign_fmt("table type is not supported");
    is_valid_join = false;
  } else if (OB_FALSE_IT(joined_table = static_cast<const JoinedTable*>(table))) {
  } else if (OB_FAIL(SMART_CALL(is_mv_join_type_valid(stmt,
                                                      joined_table->left_table_,
                                                      null_side_tables,
                                                      is_valid_join,
                                                      is_child_has_outer_join,
                                                      is_child_null_side_has_non_proctime_table)))) {
    LOG_WARN("failed to check left child", K(ret), KPC(joined_table));
  } else if (!is_valid_join) {
    // do nothing
  } else if (OB_FALSE_IT(has_outer_join |= is_child_has_outer_join)) {
  } else if (OB_FALSE_IT(null_side_has_non_proctime_table |= is_child_null_side_has_non_proctime_table)) {
  } else if (OB_FAIL(SMART_CALL(is_mv_join_type_valid(stmt,
                                                      joined_table->right_table_,
                                                      null_side_tables,
                                                      is_valid_join,
                                                      is_child_has_outer_join,
                                                      is_child_null_side_has_non_proctime_table)))) {
    LOG_WARN("failed to check right child", K(ret), KPC(joined_table));
  } else if (!is_valid_join) {
    // do nothing
  } else if (OB_FALSE_IT(has_outer_join |= is_child_has_outer_join)) {
  } else if (OB_FALSE_IT(null_side_has_non_proctime_table |= is_child_null_side_has_non_proctime_table)) {
  } else if (joined_table->is_inner_join()) {
    if (!has_outer_join) {
      // do nothing, is valid
    } else if (joined_table->left_table_->is_joined_table() &&
               joined_table->right_table_->is_joined_table()) {
      // not a left deep tree
      fast_refreshable_error_.assign_fmt("only support left deep join tree for materialized view with OUTER JOIN");
      is_valid_join = false;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->get_join_conditions().count(); ++i) {
        const ObRawExpr *cond = joined_table->get_join_conditions().at(i);
        if (OB_ISNULL(cond)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(cond));
        } else if (cond->get_relation_ids().overlap(null_side_tables)) {
          fast_refreshable_error_.assign_fmt("inner join can not reference null side tables for materialized view with OUTER JOIN");
          is_valid_join = false;
          break;
        }
      }
    }
  } else if (joined_table->is_left_join() || joined_table->is_right_join()) {
    has_outer_join = true;
    const TableItem *null_side_table = joined_table->is_left_join() ? joined_table->right_table_ : joined_table->left_table_;
    int64_t null_side_table_relid = stmt.get_table_bit_index(null_side_table->table_id_);
    ObRelIds null_side_table_relid_set;
    bool join_cond_contain_other_table = false;
    ObSEArray<const ObRawExpr*, 4> col_exprs;
    null_side_has_non_proctime_table |= !null_side_table->is_mv_proctime_table_;
    if (!(null_side_table->is_basic_table() || null_side_table->is_generated_table())) {
      // not a left deep tree
      fast_refreshable_error_.assign_fmt("only support left deep join tree for materialized view with OUTER JOIN");
      is_valid_join = false;
    } else if (OB_FAIL(null_side_table_relid_set.add_member(null_side_table_relid))) {
      LOG_WARN("failed to add member", K(ret));
    }
    // check whether where conditions contain null side tables
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < stmt.get_condition_size(); ++i) {
      const ObRawExpr *cond = stmt.get_condition_expr(i);
      if (OB_ISNULL(cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (cond->get_relation_ids().has_member(null_side_table_relid)) {
        fast_refreshable_error_.assign_fmt("WHERE conditions should not contain any null side table");
        is_valid_join = false;
      }
    }
    // check whether join conditions contain other table
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < joined_table->get_join_conditions().count(); ++i) {
      const ObRawExpr *cond = joined_table->get_join_conditions().at(i);
      if (OB_ISNULL(cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i), KPC(joined_table));
      } else if (cond->get_relation_ids().num_members() > 2) {
        // only for performance considerations, can be removed if necessary
        fast_refreshable_error_.assign_fmt("can not contain more than two tables in one OUTER JOIN condition");
        is_valid_join = false;
        break;
      } else if (!null_side_table_relid_set.is_superset(cond->get_relation_ids())) {
        join_cond_contain_other_table = true;
        // break; // can not break, continue to check whether other join conditions contain more than two tables
      }
    }
    if (OB_FAIL(ret) || !is_valid_join) {
      // do nothing
    } else if (!join_cond_contain_other_table) {
      fast_refreshable_error_.assign_fmt("OUTER JOIN condition should contain at least one table from each side");
      is_valid_join = false;
    }
    // check whether join conditions are null reject for other null side tables
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < stmt.get_table_size(); ++i) {
      col_exprs.reuse();
      if (!null_side_tables.has_member(i + 1)) {
        // do nothing
      } else if (OB_ISNULL(stmt.get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(i), K(stmt.get_table_size()));
      } else if (OB_FAIL(stmt.get_column_exprs(stmt.get_table_item(i)->table_id_, col_exprs))) {
        LOG_WARN("failed to get table column exprs", K(ret), KPC(stmt.get_table_item(i)));
      } else if (OB_FAIL(check_null_reject_or_not_contain(joined_table->get_join_conditions(),
                                                          col_exprs,
                                                          i + 1,
                                                          is_valid_join))) {
        LOG_WARN("failed to check null reject or not contain", K(ret));
      } else if (!is_valid_join) {
        fast_refreshable_error_.assign_fmt("join condition should be null reject for the other tables");
        break;
      }
    }
    if (OB_FAIL(ret) || !is_valid_join) {
      // do nothing
    } else if (OB_FAIL(null_side_tables.add_member(null_side_table_relid))) {
      LOG_WARN("failed to add member", K(ret));
    }
  } else {
    fast_refreshable_error_.assign_fmt("query join type is not supported");
    is_valid_join = false;
  }
  return ret;
}

int ObMVChecker::check_null_reject_or_not_contain(const ObIArray<ObRawExpr*> &conditions,
                                                  const ObIArray<const ObRawExpr*> &table_col_exprs,
                                                  const int64_t table_rel_id,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool has_null_reject = false;
  bool contain_cur_table = false;
  is_valid = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_null_reject && i < conditions.count(); ++i) {
    const ObRawExpr *cond = conditions.at(i);
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (!cond->get_relation_ids().has_member(table_rel_id)) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_condition(cond,
                                                                  table_col_exprs,
                                                                  has_null_reject))) {
      LOG_WARN("failed to check null reject condition", K(ret), KPC(cond));
    } else {
      contain_cur_table = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!contain_cur_table || has_null_reject) {
    is_valid = true;
  }
  return ret;
}

int ObMVChecker::collect_tables_primary_key_for_select(const ObSelectStmt &stmt,
                                                       const bool has_outer_join)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSEArray<uint64_t, 8> pk_ids;
  if (OB_ISNULL(query_ctx = stmt.get_query_ctx())
      || OB_ISNULL(schema_guard = query_ctx->sql_schema_guard_.get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_ctx), K(schema_guard));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    const TableItem *table_item = NULL;
    const ObTableSchema *table_schema = NULL;
    pk_ids.reuse();
    if (OB_ISNULL(table_item = stmt.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(i));
    } else if (table_item->is_generated_table()
               || (table_item->is_mv_proctime_table_ && !has_outer_join)) {
      // do nothing
    } else if (OB_UNLIKELY(!table_item->is_basic_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected table type", K(ret), K(i), KPC(table_item));
    } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_table_schema(table_item->ref_id_,
                                                                     table_schema))) {
      LOG_WARN("table schema not found", K(ret), KPC(table_item));
    } else if (OB_FAIL(get_table_rowkey_ids(table_schema, schema_guard, pk_ids))) {
      LOG_WARN("failed to get table rowkey ids", K(ret), KPC(table_schema));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < stmt.get_select_item_size(); ++j) {
        const ObRawExpr *expr = NULL;
        if (OB_ISNULL(expr = stmt.get_select_item(j).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(j), K(stmt.get_select_item(j)));
        } else if (expr->is_column_ref_expr()) {
          const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(expr);
          if (col_expr->get_table_id() != table_item->table_id_) {
            // do nothing
          } else if (OB_FAIL(ObOptimizerUtil::remove_item(pk_ids, col_expr->get_column_id()))) {
            LOG_WARN("failed to remove column id in select", K(ret), KPC(col_expr));
          }
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < pk_ids.count(); ++j) {
        const ColumnItem *col_item = stmt.get_column_item(table_item->table_id_, pk_ids.at(j));
        ObColumnRefRawExpr *col_expr = NULL;
        const ObColumnSchemaV2 *col_schema = NULL;
        if (NULL != col_item) {
          if (OB_ISNULL(col_expr = col_item->expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column expr is null", K(ret), KPC(table_item), K(pk_ids.at(j)), KPC(col_item));
          }
        } else if (OB_ISNULL(col_schema = table_schema->get_column_schema(pk_ids.at(j)))) {
          ret = OB_ERR_COLUMN_NOT_FOUND;
          LOG_WARN("column not found", K(ret), K(pk_ids.at(j)), K(table_schema));
        } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory_, *col_schema, session_info_, col_expr))) {
          LOG_WARN("failed to create column ref raw expr", K(ret), K(pk_ids.at(j)), KPC(col_schema));
        } else if (OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr is null", K(ret), KPC(table_item), K(pk_ids.at(j)), KPC(col_item));
        } else {
          col_expr->set_ref_id(table_item->table_id_, col_schema->get_column_id());
          col_expr->get_relation_ids().reuse();
          col_expr->set_column_attr(table_item->get_table_name(), col_schema->get_column_name_str());
          col_expr->set_database_name(table_item->database_name_);
          if (!table_item->alias_name_.empty()) {
            col_expr->set_table_alias_name();
          }
          if (OB_FAIL(col_expr->formalize(session_info_))) {
            LOG_WARN("formalize col_expr failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(add_dep_columns_no_dup(col_expr))) {
          LOG_WARN("failed to push back dependent column", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMVChecker::get_table_rowkey_ids(const ObTableSchema *table_schema,
                                      ObSchemaGetterGuard *schema_guard,
                                      ObIArray<uint64_t> &rowkey_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> ori_rowkey_ids;
  bool has_logic_pk = false;
  if (OB_ISNULL(table_schema) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema), K(schema_guard));
  } else if (OB_FAIL(table_schema->is_table_with_logic_pk(*schema_guard, has_logic_pk))) {
    LOG_WARN("failed to check table with logic pk", K(ret), KPC(table_schema));
  } else if (has_logic_pk) {
    if (OB_FAIL(table_schema->get_logic_pk_column_ids(schema_guard, ori_rowkey_ids))) {
      LOG_WARN("failed to get logic pk column ids", K(ret));
    }
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(ori_rowkey_ids))) {
    LOG_WARN("failed to get table rowkey ids", K(ret), K(has_logic_pk), KPC(table_schema));
  } else if (table_schema->get_partition_key_info().is_valid() &&
             OB_FAIL(table_schema->get_partition_key_info().get_column_ids(ori_rowkey_ids))) {
    LOG_WARN("failed to add part column ids", K(ret));
  } else if (table_schema->get_subpartition_key_info().is_valid() &&
             OB_FAIL(table_schema->get_subpartition_key_info().get_column_ids(ori_rowkey_ids))) {
    LOG_WARN("failed to add subpart column ids", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ori_rowkey_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty rowkey id array", K(ret));
  } else if (OB_FAIL(append_array_no_dup(rowkey_ids, ori_rowkey_ids))) {
    LOG_WARN("failed to append column ids no dup", K(ret));
  }
  return ret;
}

int ObMVChecker::collect_all_single_columns_for_select(const ObSelectStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> single_columns_in_select;
  ObSEArray<ObRawExpr*, 8> all_columns;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = stmt.get_select_items().at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null select expr", K(ret), K(i), K(stmt.get_select_items()));
    } else if (expr->is_column_ref_expr()) {
      if (OB_FAIL(single_columns_in_select.push_back(expr))) {
        LOG_WARN("failed to push back select expr", K(ret));
      }
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, all_columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObOptimizerUtil::remove_item(all_columns, single_columns_in_select))) {
    LOG_WARN("failed to remove single column exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_columns.count(); ++i) {
    if (OB_ISNULL(all_columns.at(i)) || OB_UNLIKELY(!all_columns.at(i)->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected column expr", K(ret), K(i), K(all_columns));
    } else if (OB_FAIL(add_dep_columns_no_dup(all_columns.at(i)))) {
      LOG_WARN("failed to push back dependent column", K(ret));
    }
  }
  return ret;
}

int ObMVChecker::check_match_major_refresh_mv(const ObSelectStmt &stmt, bool &is_match)
{
  int ret = OB_SUCCESS;
  const ObIArray<JoinedTable *> &joined_tables = stmt.get_joined_tables();
  const JoinedTable *joined_table = NULL;
  const ObTableSchema *left_table_schema = NULL;
  const ObTableSchema *right_table_schema = NULL;
  ObSqlSchemaGuard &sql_schema_guard = stmt.get_query_ctx()->sql_schema_guard_;
  bool cnt_proctime_table = false;
  is_match = true;
  if (stmt.get_table_size() != 2 || joined_tables.count() != 1) {
    is_match = false;
    LOG_INFO("[MAJ_REF_MV] join table size not valid", KR(ret), "table_size", stmt.get_table_size(),
             "joined_table_count", joined_tables.count());
  } else if (FALSE_IT(joined_table = joined_tables.at(0))) {
  } else if (OB_ISNULL(joined_table) ||
             OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("joined_table is null", KR(ret));
  } else if (!refresh_dep_columns_.empty()) {
    is_match = false;
    LOG_INFO("[MAJ_REF_MV] additional refresh dependent columns is not empty", K(refresh_dep_columns_));
  } else if (OB_FAIL(stmt.is_contains_mv_proctime_table(cnt_proctime_table))) {
    LOG_WARN("failed to check contains mv proctime table", KR(ret));
  } else if (cnt_proctime_table) {
    is_match = false;
    LOG_INFO("[MAJ_REF_MV] mv proctime table is not supported", KPC(joined_table->left_table_), KPC(joined_table->right_table_));
  } else if (OB_FAIL(sql_schema_guard.get_table_schema(joined_table->left_table_->ref_id_,
                                                       left_table_schema))) {
    LOG_WARN("left table schema not found", KR(ret), KPC(joined_table->left_table_));
  } else if (OB_FAIL(sql_schema_guard.get_table_schema(joined_table->right_table_->ref_id_,
                                                       right_table_schema))) {
    LOG_WARN("right table schema not found", KR(ret), KPC(joined_table->right_table_));
  } else if (OB_ISNULL(left_table_schema) || OB_ISNULL(right_table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("table schema is null", KR(ret), K(left_table_schema), K(right_table_schema));
  } else if (OB_FAIL(check_right_table_join_key_valid(stmt, joined_table, right_table_schema,
                                                      is_match))) {
    LOG_WARN("failed to check join key valid", KR(ret));
  } else if (is_match && OB_FAIL(check_left_table_partition_rule_valid(
                             stmt, joined_table->left_table_, left_table_schema, is_match))) {
    LOG_WARN("failed to check partition rule valid", KR(ret));
  } else if (is_match && OB_FAIL(check_select_item_valid(stmt, joined_table->get_join_conditions(),
                                                         joined_table->left_table_->table_id_,
                                                         is_match))) {
    LOG_WARN("failed to check select item valid", KR(ret));
  } else if (is_match && OB_FAIL(check_left_table_rowkey_valid(stmt, left_table_schema, is_match))) {
    LOG_WARN("failed to check left table rowkey valid", KR(ret));
  } else if (is_match && OB_FAIL(check_broadcast_table_valid(stmt, right_table_schema, is_match))) {
    LOG_WARN("failed to check broadcast table valid", KR(ret));
  } else if (is_match && OB_FAIL(check_column_store_valid(stmt, left_table_schema, right_table_schema, is_match))) {
    LOG_WARN("failed to check column store valid", KR(ret));
  } else if (is_match && FALSE_IT(is_match = !GCTX.is_shared_storage_mode())) {
  }

  LOG_INFO("[MAJ_REF_MV] check match major refresh mv", K(is_match));

  return ret;
}

int ObMVChecker::check_select_item_valid(const ObSelectStmt &stmt,
                                         const ObIArray<ObRawExpr*> &on_conds,
                                         const uint64_t left_table_id,
                                         bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  ObSEArray<ObRawExpr*, 8> exprs;
  const ObColumnRefRawExpr *col_expr = NULL;
  const ObIArray<SelectItem> &select_items = stmt.get_select_items();
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(stmt.get_condition_exprs(), exprs))
      || OB_FAIL(ObRawExprUtils::extract_column_exprs(on_conds, exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret), K(on_conds), K(stmt.get_condition_exprs()));
  }
  for(int64_t i = 0; is_match && OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (OB_ISNULL(select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(select_items));
    } else {
      is_match = select_items.at(i).expr_->is_column_ref_expr();
    }
  }
  for (int64_t i = 0; is_match && OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(col_expr = dynamic_cast<ObColumnRefRawExpr*>(exprs.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(exprs));
    } else if (col_expr->get_table_id() != left_table_id) {
      /* do nothing */
    } else {
      is_match = stmt.check_is_select_item_expr(col_expr);
    }
  }
  return ret;
}

int ObMVChecker::check_right_table_join_key_valid(const ObSelectStmt &stmt,
                                                  const JoinedTable *joined_table,
                                                  const ObTableSchema *right_table_schema,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObRelIds right_table_set;
  is_valid = true;

  if (OB_ISNULL(joined_table) || OB_ISNULL(joined_table->right_table_) || OB_ISNULL(right_table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("null input", KR(ret), K(joined_table), K(right_table_schema));
  } else if (OB_FAIL(stmt.get_table_rel_ids(*joined_table->right_table_, right_table_set))) {
    LOG_WARN("failed to get table rel ids", KR(ret));
  } else {
    const ObIArray<ObRawExpr *> &join_conditions = joined_table->get_join_conditions();
    ObSEArray<uint64_t, 4> right_table_keys;
    ObSEArray<uint64_t, 4> right_table_join_keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions.count(); ++i) {
      const ObRawExpr *expr = join_conditions.at(i);
      const ObRawExpr *l_expr = NULL;
      const ObRawExpr *r_expr = NULL;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("expr is null", KR(ret));
      } else if (2 != expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect expr param count", KR(ret), K(expr->get_param_count()));
      } else if (OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
                 OB_ISNULL(r_expr = expr->get_param_expr(1))) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("expr is null", KR(ret), K(l_expr), K(r_expr));
      } // TODO: expr's table_id is not valid, we can only compare table_name. maybe set and check table_id later
      else if (l_expr->is_column_ref_expr() &&
               l_expr->get_relation_ids().is_subset(right_table_set) &&
               OB_FAIL(right_table_join_keys.push_back(
                   static_cast<const ObColumnRefRawExpr *>(l_expr)->get_column_id()))) {
        LOG_WARN("failed to push join key", KR(ret));
      } else if (r_expr->is_column_ref_expr() &&
                 r_expr->get_relation_ids().is_subset(right_table_set) &&
                 OB_FAIL(right_table_join_keys.push_back(
                     static_cast<const ObColumnRefRawExpr *>(r_expr)->get_column_id()))) {
        LOG_WARN("failed to push join key", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(right_table_schema->get_rowkey_info().get_column_ids(right_table_keys))) {
        LOG_WARN("failed to get table keys", KR(ret));
      } else {
        lib::ob_sort(right_table_keys.begin(), right_table_keys.end());
        lib::ob_sort(right_table_join_keys.begin(), right_table_join_keys.end());
        if (!is_array_equal(right_table_keys, right_table_join_keys)) {
          is_valid = false;
          LOG_INFO("[MAJ_REF_MV] right table join key is not valid", K(right_table_keys),
                   K(right_table_join_keys));
        }
      }
    }
  }

  return ret;
}

int ObMVChecker::check_left_table_partition_rule_valid(const ObSelectStmt &stmt,
                                                       const TableItem *left_table,
                                                       const ObTableSchema *left_table_schema,
                                                       bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObRelIds left_table_set;
  is_valid = true;
  if (OB_ISNULL(left_table) || OB_ISNULL(left_table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("left table schema is null", KR(ret), K(left_table), K(left_table_schema));
  } else if (OB_FAIL(stmt.get_table_rel_ids(*left_table, left_table_set))) {
    LOG_WARN("failed to get table rel ids", KR(ret));
  } else {
    const schema::ObPartitionOption &left_partop = left_table_schema->get_part_option();
    const schema::ObPartitionOption &mv_partop = mv_container_table_schema_.get_part_option();
    // TODO for now only works for collect_mv, we could relax the rule later: partition type can
    // be other type, partition func expr should consider about column name alias, etc.
    if (!left_partop.is_hash_part() || !mv_partop.is_hash_part()) {
      is_valid = false;
      LOG_INFO("[MAJ_REF_MV] is not hash partition", K(left_partop), K(mv_partop));
    } else if (left_partop.get_part_num() != mv_partop.get_part_num()) {
      is_valid = false;
      LOG_INFO("[MAJ_REF_MV] part num doesn't match", K(left_partop), K(mv_partop));
    } else {
      ObString left_part_str = left_partop.get_part_func_expr_str();
      ObString mv_part_str = mv_partop.get_part_func_expr_str();
      bool found_item = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_items().count(); ++i) {
        const ObRawExpr *expr = NULL;
        int64_t idx = OB_INVALID_INDEX;
        if (OB_ISNULL(expr = stmt.get_select_items().at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", KR(ret), K(i), K(stmt.get_select_items()));
        } else if (!expr->is_column_ref_expr() ||
                   !expr->get_relation_ids().is_subset(left_table_set)) {
        } else {
          const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(expr);
          ObString format_mv_part_str = mv_part_str;
          ObString format_left_part_str = left_part_str;
          // remove ` or " from part_func_expr_str
          if (format_mv_part_str.length() > 2 &&
              ((format_mv_part_str[format_mv_part_str.length() - 1] == '`' && format_mv_part_str[0] == '`') ||
               (format_mv_part_str[format_mv_part_str.length() - 1] == '"' && format_mv_part_str[0] == '"'))) {
            format_mv_part_str.assign_ptr(format_mv_part_str.ptr() + 1, format_mv_part_str.length() - 2);
          }
          if (format_left_part_str.length() > 2 &&
              ((format_left_part_str[format_left_part_str.length() - 1] == '`' && format_left_part_str[0] == '`') ||
               (format_left_part_str[format_left_part_str.length() - 1] == '"' && format_left_part_str[0] == '"'))) {
            format_left_part_str.assign_ptr(format_left_part_str.ptr() + 1, format_left_part_str.length() - 2);
          }
          if (!col_expr->get_alias_column_name().empty() &&
              format_mv_part_str.case_compare(col_expr->get_alias_column_name()) != 0) {
          } else if (col_expr->get_alias_column_name().empty() &&
                     format_mv_part_str.case_compare(col_expr->get_column_name()) != 0) {
          } else if (format_left_part_str.case_compare(col_expr->get_column_name()) != 0) {
          } else {
            found_item = true;
            break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!found_item) {
        is_valid = false;
        LOG_INFO("[MAJ_REF_MV] hash expr doesn't match", K(left_partop), K(mv_partop));
      }
    }
  }

  return ret;
}

int ObMVChecker::check_left_table_rowkey_valid(const ObSelectStmt &stmt,
                                               const ObTableSchema *left_table_schema,
                                               bool &is_valid)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(left_table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("left table schema is null", KR(ret), K(left_table_schema));
  } else {
    common::ObArray<uint64_t> left_table_rowkey_column_ids;
    common::ObArray<uint64_t> mv_table_rowkey_column_ids;
    if (OB_FAIL(left_table_schema->get_rowkey_info().get_column_ids(left_table_rowkey_column_ids))) {
      LOG_WARN("failed to get table keys", KR(ret));
    } else if (OB_FAIL(mv_container_table_schema_.get_rowkey_info().get_column_ids(
                   mv_table_rowkey_column_ids))) {
      LOG_WARN("failed to get table keys", KR(ret));
    } else {
      lib::ob_sort(left_table_rowkey_column_ids.begin(), left_table_rowkey_column_ids.end());
      lib::ob_sort(mv_table_rowkey_column_ids.begin(), mv_table_rowkey_column_ids.end());
      if (!is_array_equal(left_table_rowkey_column_ids, mv_table_rowkey_column_ids)) {
        is_valid = false;
        LOG_INFO("[MAJ_REF_MV] rowkey doesn't match", K(left_table_rowkey_column_ids),
                 K(mv_table_rowkey_column_ids));
      }
    }
  }

  return ret;
}

int ObMVChecker::check_broadcast_table_valid(const ObSelectStmt &stmt,
                                             const ObTableSchema *right_table_schema,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;

  if (OB_ISNULL(right_table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("right table schema is null", KR(ret));
  } else if (!right_table_schema->is_broadcast_table()) {
    is_valid = false;
    LOG_INFO("[MAJ_REF_MV] right table is not broadcast table", K(*right_table_schema));
  }

  return ret;
}

int ObMVChecker::check_column_store_valid(const ObSelectStmt &stmt,
                                          const ObTableSchema *left_table_schema,
                                          const ObTableSchema *right_table_schema,
                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_column_store = false;
  is_valid = true;

  if (OB_ISNULL(left_table_schema) || OB_ISNULL(right_table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("table schema is null", KR(ret), K(left_table_schema), K(right_table_schema));
  } else if (OB_FAIL(mv_container_table_schema_.get_is_column_store(is_column_store))) {
    LOG_WARN("failed to get is column store", KR(ret));
  } else if (is_column_store) {
    is_valid = false;
    LOG_INFO("[MAJ_REF_MV] mv container table is column store table",
             K(mv_container_table_schema_.get_table_name()));
  } else if (OB_FAIL(left_table_schema->get_is_column_store(is_column_store))) {
    LOG_WARN("failed to get is column store", KR(ret));
  } else if (is_column_store) {
    is_valid = false;
    LOG_INFO("[MAJ_REF_MV] left table is column store table",
             K(left_table_schema->get_table_name()));
  } else if (OB_FAIL(right_table_schema->get_is_column_store(is_column_store))) {
    LOG_WARN("failed to get is column store", KR(ret));
  } else if (is_column_store) {
    is_valid = false;
    LOG_INFO("[MAJ_REF_MV] right table is column store table",
             K(right_table_schema->get_table_name()));
  }

  return ret;
}

bool ObMVChecker::is_child_refresh_type_supported(const ObMVRefreshableType refresh_type)
{
  bool bret = OB_MV_FAST_REFRESH_SIMPLE_MAV == refresh_type ||
              OB_MV_FAST_REFRESH_SIMPLE_MJV == refresh_type ||
              OB_MV_FAST_REFRESH_OUTER_JOIN_MJV == refresh_type ||
              OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV == refresh_type ||
              OB_MV_FAST_REFRESH_OUTER_JOIN_MAV == refresh_type;
  return bret;
}

int ObMVChecker::check_union_all_refresh_type(const ObSelectStmt &stmt,
                                              ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  refresh_type = OB_MV_REFRESH_INVALID;
  bool is_valid = true;
  const ObIArray<ObSelectStmt*> &set_queries = stmt.get_set_query();
  ObMVRefreshableType child_refresh_type = OB_MV_REFRESH_INVALID;
  if (ObSelectStmt::UNION != stmt.get_set_op() || stmt.is_set_distinct()) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("query with set operators UNION/INTERSECT/EXCEPT/MINUS is not supported");
  } else if (need_on_query_computation_) {
    is_valid = false;
    fast_refreshable_error_.assign_fmt("on query computation is not supported for materialized view with UNION ALL");
  } else if (OB_FAIL(check_union_all_mv_marker_column_valid(stmt, is_valid))) {
    LOG_WARN("failed to check union all mv marker column valid", K(ret));
  } else if (!is_valid) {
    fast_refreshable_error_.assign_fmt("UNION ALL query without valid marker select item");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < set_queries.count(); ++i) {
      stmt_idx_ = i;
      if (OB_ISNULL(set_queries.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i), K(stmt));
      } else if (OB_FAIL(check_mv_stmt_refresh_type(*set_queries.at(i), child_refresh_type))) {
        LOG_WARN("failed to check mv stmt refresh type", K(ret));
      } else if (!is_child_refresh_type_supported(child_refresh_type)) {
        is_valid = false;
        if (fast_refreshable_error_.empty()) {
          if (OB_MV_COMPLETE_REFRESH >= child_refresh_type) {
            fast_refreshable_error_.assign_fmt("fast refresh is not supported");
          } else {
            fast_refreshable_error_.assign_fmt("refresh type(%d) is not supported for materialized view with union all", child_refresh_type);
          }
        }
        fast_refreshable_error_.append_fmt(" in the %ld-th set child query", i+1);
      } else if (OB_FAIL(child_refresh_types_.push_back(child_refresh_type))) {
        LOG_WARN("failed to push back", K(ret));
      }
      stmt_idx_ = OB_INVALID_INDEX;
    }
  }

  if (OB_SUCC(ret)) {
    refresh_type = is_valid ? OB_MV_FAST_REFRESH_UNION_ALL
                            : OB_MV_COMPLETE_REFRESH;
  }
  return ret;
}

int ObMVChecker::check_union_all_mv_marker_column_valid(const ObSelectStmt &stmt,
                                                        bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObIArray<ObSelectStmt*> &set_queries = stmt.get_set_query();
  const int64_t sel_size = stmt.get_select_item_size();
  ObSEArray<ObRawExpr*, 4> marker_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < sel_size; ++i) {
    bool cur_sel_is_valid = true;
    ObRawExpr *expr = NULL;
    marker_exprs.reuse();
    for (int64_t j = 0; OB_SUCC(ret) && cur_sel_is_valid && j < set_queries.count(); ++j) {
      if (OB_ISNULL(set_queries.at(j)) || OB_ISNULL(expr = set_queries.at(j)->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i), K(j), K(stmt));
      } else if (!expr->is_const_raw_expr()) {
        // marker column is const value and can not be null.
        // 1. this case, need not check the null value because of the cast above null.
        //  select 1 marker ... union all select null ...
        // 2. this case, need not check the null value because of the equal null value.
        //  select null marker ... union all select null ...
        cur_sel_is_valid = false;
      } else if (ObOptimizerUtil::find_equal_expr(marker_exprs, expr)) {
        cur_sel_is_valid = false;
      } else if (OB_FAIL(marker_exprs.push_back(expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret) && cur_sel_is_valid) {
      is_valid = true;
      marker_idx_ = i;
    }
  }
  return ret;
}

int ObMVChecker::pre_process_view_stmt(ObRawExprFactory *expr_factory,
                                       ObSQLSessionInfo *session_info,
                                       ObIArray<ObColumnRefRawExpr*> &gen_cols,
                                       ObSelectStmt &view_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(adjust_set_stmt_sel_item(view_stmt))) {
    LOG_WARN("failed to pre process view stmt", K(ret));
  } else if (OB_FAIL(get_generated_columns_recursively(&view_stmt, gen_cols))) {
    LOG_WARN("failed to get generated columns recursively", K(ret));
  } else if (OB_FAIL(view_stmt.formalize_stmt_expr_reference(expr_factory, session_info, true))) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else if (OB_FAIL(reset_ref_query_stmt_id_recursively(&view_stmt))) {
    LOG_WARN("failed to reset ref query stmt id recursively", K(ret));
  }
  return ret;
}

int ObMVChecker::adjust_set_stmt_sel_item(ObSelectStmt &view_stmt)
{
  int ret = OB_SUCCESS;
  if (view_stmt.is_set_stmt()) {
    ObIArray<ObSelectStmt*> &set_queries = view_stmt.get_set_query();
    const int64_t sel_size = view_stmt.get_select_item_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_size; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < set_queries.count(); ++j) {
        if (OB_ISNULL(set_queries.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(i), K(j), K(view_stmt));
        } else if (0 < j) {
          set_queries.at(j)->get_select_item(i).alias_name_ = set_queries.at(0)->get_select_item(i).alias_name_;
        }
      }
    }
  }
  return ret;
}

int ObMVChecker::get_generated_columns_recursively(ObDMLStmt *stmt,
                                                   ObIArray<ObColumnRefRawExpr*> &gen_cols)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KPC(stmt));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else {
    const ObIArray<ColumnItem> &column_items = stmt->get_column_items();
    ObColumnRefRawExpr *col_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      if (OB_ISNULL(col_expr = column_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(i), K(column_items));
      } else if (!col_expr->is_generated_column() || NULL == col_expr->get_dependant_expr()) {
        /* do nothing */
      } else if (OB_FAIL(gen_cols.push_back(col_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_generated_columns_recursively(child_stmts.at(i), gen_cols)))) {
        LOG_WARN("failed to get generated columns recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObMVChecker::reset_ref_query_stmt_id_recursively(ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KPC(stmt));
  } else if (OB_FALSE_IT(stmt->reset_stmt_id())) {
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(reset_ref_query_stmt_id_recursively(child_stmts.at(i))))) {
        LOG_WARN("failed to reset ref query stmt id recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObMVChecker::add_dep_columns_no_dup(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < refresh_dep_columns_.count(); ++i) {
    if (OB_ISNULL(refresh_dep_columns_.at(i).first)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i), K(refresh_dep_columns_.at(i).first));
    } else if (stmt_idx_ == refresh_dep_columns_.at(i).second
               && expr->same_as(*refresh_dep_columns_.at(i).first)) {
      found = true;
    }
  }
  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(refresh_dep_columns_.push_back(std::make_pair(expr, stmt_idx_)))) {
      LOG_WARN("failed to push back dependent column", K(ret));
    }
  }
  return ret;
}

int ObTableReferencedColumnsInfo::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(table_referenced_columns_map_.create(16, lib::ObMemAttr(MTL_ID(), "TableRefCol")))) {
    LOG_WARN("failed to create table referenced columns map", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObTableReferencedColumnsInfo::destroy()
{
  FOREACH(it, table_referenced_columns_map_) {
    common::hash::ObHashSet<uint64_t> *table_referenced_columns = it->second;
    if (OB_NOT_NULL(table_referenced_columns)) {
      table_referenced_columns->destroy();
    }
  }
  table_referenced_columns_map_.destroy();
  inner_alloc_.reset();
  is_inited_ = false;
}

int ObTableReferencedColumnsInfo::record_table_referenced_columns(const uint64_t table_ref_id,
                                                                  const ObIArray<uint64_t> &col_ids)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> *table_referenced_columns = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SUCCESS != (ret = table_referenced_columns_map_.get_refactored(table_ref_id, table_referenced_columns))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      table_referenced_columns = OB_NEWx(common::hash::ObHashSet<uint64_t>, (&inner_alloc_));
      if (OB_ISNULL(table_referenced_columns)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_FAIL(table_referenced_columns->create(16, lib::ObMemAttr(MTL_ID(), "TableRefCol")))) {
        LOG_WARN("failed to create table referenced columns", K(ret));
      } else if (OB_FAIL(table_referenced_columns_map_.set_refactored(table_ref_id, table_referenced_columns))) {
        LOG_WARN("failed to set table referenced columns map", K(ret));
      }
    } else {
      LOG_WARN("failed to get table referenced columns map", K(ret));
    }
  } else if (OB_ISNULL(table_referenced_columns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_ref_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); ++i) {
    if (OB_SUCCESS != (ret = table_referenced_columns->set_refactored(col_ids.at(i)))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to set table referenced columns", K(ret));
      }
    }
  }
  return ret;
}

int ObTableReferencedColumnsInfo::convert_to_required_columns_infos(ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos)
{
  int ret = OB_SUCCESS;
  required_columns_infos.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    FOREACH(it, table_referenced_columns_map_) {
      const uint64_t table_id = it->first;
      const common::hash::ObHashSet<uint64_t> *table_referenced_columns = it->second;
      ObSEArray<uint64_t, 16> required_columns;
      if (OB_ISNULL(table_referenced_columns)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret), K(table_referenced_columns));
      } else {
        FOREACH(it2, *table_referenced_columns) {
          const uint64_t column_id = it2->first;
          if (OB_FAIL(required_columns.push_back(column_id))) {
            LOG_WARN("failed to push back", KR(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(required_columns_infos.push_back(
                     obrpc::ObMVRequiredColumnsInfo(table_id,
                                                    required_columns)))) {
        LOG_WARN("failed to push back", KR(ret));
      }
    }
  }

  return ret;
}

int ObTableReferencedColumnsInfo::append_to_table_referenced_columns(const uint64_t table_id,
                                                                     common::hash::ObHashSet<uint64_t> &table_referenced_columns)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> *referenced_columns = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(table_referenced_columns_map_.get_refactored(table_id, referenced_columns))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get table referenced columns", KR(ret));
    }
  } else if (OB_ISNULL(referenced_columns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(referenced_columns));
  } else {
    FOREACH(it, *referenced_columns) {
      if (OB_FAIL(table_referenced_columns.set_refactored(it->first))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to set table referenced columns", KR(ret));
        }
      }
    }
  }

  return ret;
}

} // end of namespace sql
}//end of namespace oceanbase
