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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_late_materialization.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_join.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObTransformLateMaterialization::transform_one_stmt(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                       ObDMLStmt *&stmt,
                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool force_trans = false;
  bool force_no_trans = false;
  bool can_trans = false;
  const ObSelectStmt *select_stmt = NULL;
  ObLateMaterializationInfo info;
  ObCostBasedLateMaterializationCtx check_ctx;
  trans_happened = false;
  OPT_TRACE_BEGIN_SECTION;
  OPT_TRACE("try to perform late materialization");
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (!stmt->is_select_stmt()) {
    /* do nothing */
  } else if (OB_FAIL(check_hint_validity(*stmt, force_trans, force_no_trans))) {
    LOG_WARN("failed to check hint validity", K(ret));
  } else if (force_no_trans) {
    OPT_TRACE("hint reject transform");
  } else if (FALSE_IT(select_stmt = static_cast<const ObSelectStmt*>(stmt))) {
  } else if (OB_FAIL(check_stmt_need_late_materialization(*select_stmt, force_trans, can_trans))) {
    LOG_WARN("failed to check stmt need late materialization", K(ret));
  } else if (!can_trans) {
    OPT_TRACE("there is no need to late materialization");
  } else if (OB_FAIL(generate_late_materialization_info(*select_stmt, info, check_ctx))) {
    LOG_WARN("failed to check if table need late materialization", K(ret));
  } else if (info.candi_indexs_.empty() && !info.is_allow_column_table_) {
    /* do nothing */
  } else if (OB_FAIL(inner_accept_transform(parent_stmts, stmt, force_trans, info, check_ctx,
                                            trans_happened))) {
    LOG_WARN("failed to do inner accept transform", K(ret));
  } else if (trans_happened && OB_FAIL(add_transform_hint(*stmt, &info))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else {
    LOG_TRACE("succeed to do late materialization", K(trans_happened));
  }
  OPT_TRACE_END_SECTION;
  return ret;
}

int ObTransformLateMaterialization::check_hint_validity(const ObDMLStmt &stmt,
                                                        bool &force_trans,
                                                        bool &force_no_trans)
{
  int ret = OB_SUCCESS;
  force_trans = false;
  force_no_trans = false;
  const ObQueryHint *query_hint = NULL;
  const ObHint *trans_hint = NULL;
  if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(query_hint));
  } else if (stmt.get_stmt_hint().enable_no_rewrite()) {
    force_no_trans = true;
  } else if (query_hint->has_outline_data() &&
             NULL != (trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_))) {
    force_trans = trans_hint->get_hint_type() == T_USE_LATE_MATERIALIZATION;
  } else if (NULL != (trans_hint = stmt.get_stmt_hint().get_normal_hint(T_USE_LATE_MATERIALIZATION))) {
    force_no_trans = trans_hint->is_disable_hint();
    force_trans = trans_hint->is_enable_hint();
  } else {
    /* it deals with T_FULL_HINT because we can also handle a large column with full path*/
  }
  return ret;
}

int ObTransformLateMaterialization::check_stmt_need_late_materialization(const ObSelectStmt &stmt,
                                                                         const bool force_accept,
                                                                         bool &need)
{
  int ret = OB_SUCCESS;
  int64_t child_stmt_size = 0;
  const TableItem *table_item = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  bool contain_enumset_rowkey = false;
  need = false;
  if (OB_ISNULL(stmt.get_query_ctx()) ||
      OB_ISNULL(ctx_) || OB_ISNULL(schema_guard = ctx_->sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(COMPAT_VERSION_4_2_5 > stmt.get_query_ctx()->optimizer_features_enable_version_) ||
             OB_UNLIKELY(COMPAT_VERSION_4_3_3 > stmt.get_query_ctx()->optimizer_features_enable_version_ &&
                         COMPAT_VERSION_4_3_0 <= stmt.get_query_ctx()->optimizer_features_enable_version_)) {
    /* need_transform = false; */
    LOG_TRACE("not late materialization stmt, optimizer feature enable is lower than 4_2_5 or lower than 4_3_3");
  } else if (OB_FAIL(stmt.get_child_stmt_size(child_stmt_size))) {
    LOG_WARN("failed to get child stmt size", K(ret));
  } else if (stmt.has_hierarchical_query() ||
             stmt.has_group_by()||
             stmt.has_rollup() ||
             stmt.has_having() ||
             stmt.has_window_function() ||
             stmt.has_distinct() ||
             stmt.is_unpivot_select() ||
             1 != stmt.get_from_item_size() ||
             1 != stmt.get_table_size() ||
             NULL == stmt.get_table_item(0) ||
             !stmt.get_table_item(0)->is_basic_table() ||
             stmt.get_table_item(0)->is_system_table_ ||
             0 != child_stmt_size ||
             stmt.is_calc_found_rows() ||
             !stmt.has_limit()) {
    /* need_transform = false; */
  }  else if (OB_ISNULL(table_item = stmt.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_item->ref_id_, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_item->ref_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is NULL", K(ret));
  } else if (!table_schema->get_rowkey_info().is_valid()) {
    /* need_transform = false; */
  } else if (!(stmt.has_order_by()) &&
             !(force_accept && table_schema->get_part_level() != PARTITION_LEVEL_ZERO &&
               stmt.has_limit() && (NULL != stmt.get_offset_expr() || NULL != stmt.get_limit_percent_expr()))) {
    /* need_transform = false; */
  } else if (OB_FAIL(contain_enum_set_rowkeys(table_schema->get_rowkey_info(),
                                              contain_enumset_rowkey))) {
    LOG_WARN("check contain enumset rowkey failed", K(ret));
  } else if (contain_enumset_rowkey) {
    //if there are enumset rowkeys, don't use late materialization since 'enumset_col = ?' cannot be used to extract query ranges
  } else {
    need = true;
  }
  return ret;
}

int ObTransformLateMaterialization::generate_late_materialization_info(
                                                       const ObSelectStmt &select_stmt,
                                                       ObLateMaterializationInfo &info,
                                                       ObCostBasedLateMaterializationCtx &check_ctx)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObSEArray<uint64_t, 4> key_col_ids;
  ObSEArray<uint64_t, 4> filter_col_ids;
  ObSEArray<uint64_t, 4> orderby_col_ids;
  ObSEArray<uint64_t, 4> select_col_ids;
  ObSEArray<uint64_t, 4> index_column_ids;
  ObSEArray<uint64_t, 4> common_select_cols;
  if (OB_ISNULL(ctx_) || OB_ISNULL(schema_guard = ctx_->sql_schema_guard_) ||
      OB_ISNULL(table_item = select_stmt.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_item->ref_id_, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_item->ref_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is NULL", K(ret));
  } else if (OB_FAIL(extract_transform_column_ids(select_stmt, *table_schema, key_col_ids,
                                                  filter_col_ids, orderby_col_ids,
                                                  select_col_ids))) {
    LOG_WARN("failed to extract column ids", K(ret));
  } else if (OB_FAIL(gen_trans_info_for_row_store(select_stmt, key_col_ids,
                                                  filter_col_ids, orderby_col_ids,
                                                  select_col_ids, table_item,
                                                  table_schema, info, check_ctx))) {
    LOG_WARN("failed to gen trans info for row store", K(ret));
  } else if (!info.candi_indexs_.empty()) {
    /* do nothing */
  } else if (OB_FAIL(gen_trans_info_for_column_store(select_stmt, key_col_ids, filter_col_ids,
                                                     orderby_col_ids, select_col_ids, table_item,
                                                     table_schema, info, check_ctx))) {
    LOG_WARN("failed to gen trans info for column store", K(ret));
  }
  return ret;
}

int ObTransformLateMaterialization::gen_trans_info_for_row_store(const ObSelectStmt &stmt,
                                                                 const ObIArray<uint64_t> &key_col_ids,
                                                                 const ObIArray<uint64_t> &filter_col_ids,
                                                                 const ObIArray<uint64_t> &orderby_col_ids,
                                                                 const ObIArray<uint64_t> &select_col_ids,
                                                                 const TableItem *table_item,
                                                                 const ObTableSchema *table_schema,
                                                                 ObLateMaterializationInfo &info,
                                                                 ObCostBasedLateMaterializationCtx &check_ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> index_column_ids;
  ObSEArray<uint64_t, 4> common_select_cols;
  ObSEArray<const ObTableSchema *, 4> index_schemas;
  if (OB_FAIL(common_select_cols.assign(select_col_ids))) {
    LOG_WARN("failed to assign predicate col in view", K(ret));
  } else if (OB_FAIL(get_accessible_index(stmt, *table_item, index_schemas))) {
    LOG_WARN("get valid index schema", K(ret));
  } else {
    bool is_partition_table = table_schema->get_part_level() != PARTITION_LEVEL_ZERO;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_schemas.count(); ++i) {
      const ObTableSchema *index_schema = NULL;
      bool is_match = false;
      ObString index_name;
      index_column_ids.reuse();
      if (OB_ISNULL(index_schema = index_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got null ptr", K(ret));
      } else if (OB_FAIL(index_schema->get_column_ids(index_column_ids))) {
        LOG_WARN("failed to get column ids", K(ret));
      } else if (OB_FAIL(check_index_match_late_materialization(index_schema->get_table_id(),
                                                    index_column_ids, key_col_ids, filter_col_ids,
                                                    orderby_col_ids, select_col_ids,
                                                    table_item->ref_id_, check_ctx, is_match))) {
        LOG_WARN("failed to check index match", K(ret));
      } else if (!is_match) {
        /* do nothing */
      } else if (OB_FAIL(info.candi_indexs_.push_back(index_schema->get_table_id()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
        LOG_WARN("failed to get index name", K(ret));
      } else if (OB_FAIL(info.candi_index_names_.push_back(index_name))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (stmt.has_order_by() &&
                 !(is_partition_table && index_schema->is_global_index_table()) &&
                 OB_FAIL(check_ctx.check_sort_indexs_.push_back(index_schema->get_table_id()))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        for (int64_t i = common_select_cols.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
          if (!ObOptimizerUtil::find_item(index_column_ids, common_select_cols.at(i)) &&
              OB_FAIL(common_select_cols.remove(i))) {
            LOG_WARN("failed to remove", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (info.candi_indexs_.empty()) {
        OPT_TRACE("candidate index is empty");
        LOG_TRACE("there is no candidate index to late materialize");
      } else if (OB_FAIL(info.project_col_in_view_.assign(key_col_ids))) {
        LOG_WARN("failed to assign predicate col in view", K(ret));
      } else if (OB_FAIL(append_array_no_dup(info.project_col_in_view_, orderby_col_ids))) {
        LOG_WARN("failed to append array no dup", K(ret));
      } else if (OB_FAIL(append_array_no_dup(info.project_col_in_view_, common_select_cols))) {
        LOG_WARN("failed to append array no dup", K(ret));
      } else {
        info.is_allow_column_table_ = false;
        check_ctx.late_table_id_ = table_item->table_id_;
      }
    }
  }
  return ret;
}

int ObTransformLateMaterialization::extract_transform_column_ids(const ObSelectStmt &select_stmt,
                                                                const ObTableSchema &table_schema,
                                                                ObIArray<uint64_t> &key_col_ids,
                                                                ObIArray<uint64_t> &filter_col_ids,
                                                                ObIArray<uint64_t> &orderby_col_ids,
                                                                ObIArray<uint64_t> &select_col_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> temp_exprs;
  ObSEArray<uint64_t, 4> part_col_ids;
  if (OB_FAIL(table_schema.get_rowkey_info().get_column_ids(key_col_ids))) {
    LOG_WARN("get rowkey column ids failed", K(ret));
  } else if (table_schema.get_partition_key_info().is_valid() &&
             OB_FAIL(table_schema.get_partition_key_info().get_column_ids(part_col_ids))) {
    LOG_WARN("get partition column ids failed", K(ret));
  } else if (table_schema.get_subpartition_key_info().is_valid() &&
             OB_FAIL(table_schema.get_subpartition_key_info().get_column_ids(part_col_ids))) {
    LOG_WARN("get subpartition column ids failed", K(ret));
  } else if (OB_FAIL(append_array_no_dup(key_col_ids, part_col_ids))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(select_stmt.get_condition_exprs(),
                                                        filter_col_ids))) {
    LOG_WARN("failed to extract column ids", K(ret));
  } else if (OB_FAIL(select_stmt.get_order_exprs(temp_exprs))) {
    LOG_WARN("ger order exprs failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(temp_exprs, orderby_col_ids))) {
    LOG_WARN("failed to extract column ids", K(ret));
  } else if (FALSE_IT(temp_exprs.reuse())) {
  } else if (OB_FAIL(select_stmt.get_select_exprs(temp_exprs))) {
    LOG_WARN("get select exprs failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(temp_exprs, select_col_ids))) {
    LOG_WARN("failed to get stored column ids", K(ret));
  }
  return ret;
}

/*
 * index > full, index > no_index
 * full > use_late_materialization
*/
int ObTransformLateMaterialization::get_accessible_index(const ObSelectStmt &select_stmt,
                                                     const TableItem &table_item,
                                                     ObIArray<const ObTableSchema *> &index_schemas)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 2> hint_index_ids;
  ObSEArray<uint64_t, 2> hint_no_index_ids;
  ObSEArray<uint64_t, 4> index_ids;
  ObSEArray<const ObTableSchema *, 2> tmp_index_schemas;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObQueryHint *query_hint = NULL;
  bool has_full_hint = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(schema_guard = ctx_->sql_schema_guard_) ||
      OB_ISNULL(query_hint = select_stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_vaild_index_id(schema_guard, &select_stmt, &table_item,
                                                          index_ids))) {
    LOG_WARN("fail to get vaild index id", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
      const ObTableSchema *index_schema = NULL;
      if (OB_FAIL(schema_guard->get_table_schema(index_ids.at(i), index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(index_ids.at(i)));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null index schema", K(ret));
      } else if (!index_schema->get_rowkey_info().is_valid() ||
                 !index_schema->is_index_table()) {
        /* do nothing */
      } else {
        ObIndexType index_type = index_schema->get_index_type();
        if (INDEX_TYPE_NORMAL_LOCAL == index_type ||
            INDEX_TYPE_UNIQUE_LOCAL == index_type ||
            INDEX_TYPE_NORMAL_GLOBAL == index_type ||
            INDEX_TYPE_UNIQUE_GLOBAL == index_type ||
            INDEX_TYPE_PRIMARY == index_type ||
            INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type ||
            INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type ||
            INDEX_TYPE_HEAP_ORGANIZED_TABLE_PRIMARY == index_type) {
          if (OB_FAIL(tmp_index_schemas.push_back(index_schema))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
    }
  }
  const ObIArray<ObHint*> &opt_hints = select_stmt.get_stmt_hint().other_opt_hints_;
  for (int64_t i = 0; OB_SUCC(ret) && i < opt_hints.count(); ++i) {
    ObIndexHint *index_hint = NULL;
    int64_t index_schema_i = 0;
    if (OB_ISNULL(opt_hints.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (!opt_hints.at(i)->is_access_path_hint()) {
      /* do nothing */
    } else if (FALSE_IT(index_hint = static_cast<ObIndexHint *>(opt_hints.at(i)))) {
      /* do nothing */
    } else if (T_INDEX_HINT == index_hint->get_hint_type() ||
               T_INDEX_ASC_HINT == index_hint->get_hint_type() ||
               T_INDEX_DESC_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_ASC_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_DESC_HINT == index_hint->get_hint_type()) {
      if (match_index_name(tmp_index_schemas, table_item, *index_hint, query_hint->cs_type_, index_schema_i)) {
        if (OB_FAIL(hint_index_ids.push_back(tmp_index_schemas.at(index_schema_i)->get_table_id()))) {
          LOG_WARN("push back hint index name failed", K(ret));
        }
      }
    } else if (T_NO_INDEX_HINT == index_hint->get_hint_type()) {
      if (match_index_name(tmp_index_schemas, table_item, *index_hint, query_hint->cs_type_, index_schema_i)) {
        if (OB_FAIL(hint_no_index_ids.push_back(tmp_index_schemas.at(index_schema_i)->get_table_id()))) {
          LOG_WARN("push back hint index name failed", K(ret));
        }
      }
    } else if (T_FULL_HINT == index_hint->get_hint_type()) {
      has_full_hint = true;
    }
  }
  if (OB_SUCC(ret) && hint_index_ids.empty() && has_full_hint) {
    tmp_index_schemas.reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_index_schemas.count(); ++i) {
    const ObTableSchema *index_schema = tmp_index_schemas.at(i);
    if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null index schema", K(ret));
    } else if (!hint_index_ids.empty() &&
               !ObOptimizerUtil::find_item(hint_index_ids, index_schema->get_table_id())) {
      /* do nothing */
    } else if (!hint_no_index_ids.empty() &&
               ObOptimizerUtil::find_item(hint_no_index_ids, index_schema->get_table_id()) &&
               !ObOptimizerUtil::find_item(hint_index_ids, index_schema->get_table_id())) {
      /* do nothing */
    } else if (OB_FAIL(index_schemas.push_back(index_schema))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

bool ObTransformLateMaterialization::match_index_name(
                                               const ObIArray<const ObTableSchema *> &index_schemas,
                                               const TableItem &table_item,
                                               const ObIndexHint &index_hint,
                                               const ObCollationType cs_type,
                                               int64_t &id)
{
  bool match = false;
  id = -1;
  for (int64_t i = 0; !match && i < index_schemas.count(); ++i) {
    if (index_hint.is_match_index(cs_type, table_item, *index_schemas.at(i))) {
      id = i;
      match = true;
    }
  }
  return match;
}

int ObTransformLateMaterialization::check_index_match_late_materialization(
                                                         const uint64_t index_id,
                                                         const ObIArray<uint64_t> &index_column_ids,
                                                         const ObIArray<uint64_t> &key_col_ids,
                                                         const ObIArray<uint64_t> &filter_col_ids,
                                                         const ObIArray<uint64_t> &orderby_col_ids,
                                                         const ObIArray<uint64_t> &select_col_ids,
                                                         const uint64_t ref_table_id,
                                                         ObCostBasedLateMaterializationCtx &check_ctx,
                                                         bool &is_match)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> tmp_index_column_ids;
  ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(schema_guard = ctx_->sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (ObOptimizerUtil::is_subset(select_col_ids, index_column_ids) ||
             !ObOptimizerUtil::is_subset(key_col_ids, index_column_ids) ||
             !ObOptimizerUtil::is_subset(orderby_col_ids, index_column_ids)) {
    /* do nothing */
  } else if (ObOptimizerUtil::is_subset(filter_col_ids, index_column_ids)) {
    is_match = true;
    if (OB_FAIL(check_ctx.late_material_indexs_.push_back(index_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } else {
    // it may match function index.
    const int64_t cnt = index_column_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      const ObColumnSchemaV2 *index_col = NULL;
      if (OB_FAIL(schema_guard->get_column_schema(ref_table_id, index_column_ids.at(i), index_col))) {
        LOG_WARN("failed to get column schema", K(ret));
      } else if (NULL == index_col) {
        /* do nothing : unique key will has a shadow_pk_0 column which no used here */
      } else if (index_col->is_func_idx_column()) {
        if (OB_FAIL(index_col->get_cascaded_column_ids(tmp_index_column_ids))) {
          LOG_WARN("failed to get column ref ids", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !tmp_index_column_ids.empty()) {
      if (OB_FAIL(append(tmp_index_column_ids, index_column_ids))) {
        LOG_WARN("failed to append array", K(ret));
      } else if (ObOptimizerUtil::is_subset(filter_col_ids, tmp_index_column_ids)) {
        is_match = true;
      }
    }
  }
  return ret;
}

int ObTransformLateMaterialization::evaluate_stmt_cost(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                       ObDMLStmt *&stmt,
                                                       bool is_trans_stmt,
                                                       double &plan_cost,
                                                       bool &is_expected,
                                                       ObCostBasedLateMaterializationCtx &check_ctx)
{
  int ret = OB_SUCCESS;
  ObEvalCostHelper eval_cost_helper;
  ObDMLStmt *root_stmt = NULL;
  is_expected = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid()) ||
      OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx()) ||
      OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory()) ||
      OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(ctx_));
  } else if (OB_FAIL(eval_cost_helper.fill_helper(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                  *stmt->get_query_ctx(), *ctx_))) {
    LOG_WARN("failed to fill eval cost helper", K(ret));
  } else if (OB_FAIL(prepare_eval_cost_stmt(parent_stmts, *stmt, root_stmt, is_trans_stmt))) {
    LOG_WARN("failed to prepare eval cost stmt", K(ret));
  } else {
    ctx_->eval_cost_ = true;
    lib::ContextParam param;
    param.set_mem_attr(ctx_->session_info_->get_effective_tenant_id(),
                       "CostBasedRewrit",
                       ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL)
         .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    CREATE_WITH_TEMP_CONTEXT(param) {
      ObRawExprFactory tmp_expr_factory(CURRENT_CONTEXT->get_arena_allocator());
      HEAP_VAR(ObOptimizerContext,
               optctx,
               ctx_->session_info_,
               ctx_->exec_ctx_,
               ctx_->sql_schema_guard_,
               ctx_->opt_stat_mgr_,
               CURRENT_CONTEXT->get_arena_allocator(),
               &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
               *ctx_->self_addr_,
               GCTX.srv_rpc_proxy_,
               root_stmt->get_query_ctx()->get_global_hint(),
               tmp_expr_factory,
               root_stmt,
               false,
               ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx()) {
        ObOptimizer optimizer(optctx);
        ObLogPlan *plan = NULL;
        if (OB_FAIL(optimizer.get_optimization_cost(*root_stmt, plan, plan_cost))) {
          LOG_WARN("failed to get optimization cost", K(ret));
        } else if (OB_FAIL(is_expected_plan(plan, &check_ctx, is_trans_stmt, is_expected))) {
          LOG_WARN("failed to check transformed plan", K(ret));
        } else if (OB_FAIL(eval_cost_helper.recover_context(
                                              *ctx_->exec_ctx_->get_physical_plan_ctx(),
                                              *ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx(),
                                              *ctx_))) {
          LOG_WARN("failed to recover context", K(ret));
        } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, root_stmt))) {
          LOG_WARN("failed to free stmt", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformLateMaterialization::inner_accept_transform(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                           ObDMLStmt *&stmt,
                                                           const bool force_accept,
                                                           ObLateMaterializationInfo &info,
                                                           ObCostBasedLateMaterializationCtx &check_ctx,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  double trans_stmt_cost = -1;
  double base_stmt_cost = -1;
  bool is_expected = false;
  bool is_base_expected = false;
  ObDMLStmt *tmp1 = NULL;
  ObDMLStmt *tmp2 = NULL;
  ObDMLStmt *trans_stmt = NULL;
  ObTryTransHelper try_trans_helper;
  cost_based_trans_tried_ = true;
  trans_happened = false;
  BEGIN_OPT_TRACE_EVA_COST;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("context is null", K(ret), K(ctx_), K(stmt));
  } else if (ctx_->in_accept_transform_) {
    LOG_TRACE("not accept transform because already in one accepct transform");
  } else {
    ctx_->in_accept_transform_ = true;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_trans_helper.fill_helper(stmt->get_query_ctx()))) {
        LOG_WARN("failed to fill try trans helper", K(ret));
      } else if (OB_FAIL(generate_late_materialization_stmt(info, stmt, trans_stmt))) {
        LOG_WARN("perform late materialization failed", K(ret));
      } else if (force_accept && stmt->get_stmt_hint().query_hint_->has_outline_data()) {
        trans_happened = true;
        LOG_TRACE("force accept to use late materialization");
      } else if (OB_FAIL(evaluate_stmt_cost(parent_stmts, trans_stmt, true, trans_stmt_cost,
                                            is_expected, check_ctx))) {
        LOG_WARN("failed to evaluate cost for the transformed stmt", K(ret));
      } else if (!is_expected) {
        trans_happened = false;
      } else if (!force_accept && OB_FAIL(evaluate_stmt_cost(parent_stmts, stmt, false,
                                                             base_stmt_cost, is_base_expected,
                                                             check_ctx))) {
        LOG_WARN("failed to evaluate cost of select_stmt");
      } else {
        trans_happened = force_accept ||
                         (base_stmt_cost > 0 && trans_stmt_cost > 0 && trans_stmt_cost < base_stmt_cost) ||
                         // we always consider best late_materialization index is better to the full path
                         (check_ctx.base_index_ != common::OB_INVALID_ID &&
                          ObOptimizerUtil::find_item(check_ctx.late_material_indexs_, check_ctx.base_index_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(try_trans_helper.finish(trans_happened, stmt->get_query_ctx(), ctx_))) {
        LOG_WARN("failed to finish try trans helper", K(ret));
      }
    }
    ctx_->in_accept_transform_ = false;
  }
  END_OPT_TRACE_EVA_COST;

  if (OB_FAIL(ret)) {
  } else if (!trans_happened) {
    OPT_TRACE("reject transform because the cost is increased or the query plan is unexpected.");
    OPT_TRACE("before transform cost:", base_stmt_cost);
    OPT_TRACE("after transform cost:", trans_stmt_cost);
    OPT_TRACE("is expected plan:", is_expected);
    LOG_TRACE("reject transform because the cost is increased or the query plan is unexpected",
              K(base_stmt_cost), K(trans_stmt_cost), K(is_expected), K(check_ctx));
  } else if (OB_FAIL(adjust_transformed_stmt(parent_stmts, trans_stmt, tmp1, tmp2))) {
    LOG_WARN("failed to adjust transformed stmt", K(ret));
  } else if (force_accept) {
    OPT_TRACE("hint or rule force cost based transform apply.");
    LOG_TRACE("succeed force accept transform because hint/rule");
    stmt = trans_stmt;
    reset_stmt_cost();
  } else {
    OPT_TRACE("accept transform because the cost is decreased.");
    OPT_TRACE("before transform cost:", base_stmt_cost);
    OPT_TRACE("after transform cost:", trans_stmt_cost);
    LOG_TRACE("accept transform because the cost is decreased", K(base_stmt_cost), K(trans_stmt_cost),
              K(check_ctx));
    stmt = trans_stmt;
    reset_stmt_cost();
  }
  return ret;
}

int ObTransformLateMaterialization::replace_expr_skip_part(ObSelectStmt &select_stmt,
                                                           ObIArray<ObRawExpr *> &old_col_exprs,
                                                           ObIArray<ObRawExpr *> &new_col_exprs) {
  int ret = OB_SUCCESS;
  ObStmtExprReplacer replacer;
  ObSEArray<ObRawExpr *, 8> part_exprs;
  const ObIArray<ObDMLStmt::PartExprItem> &part_items = select_stmt.get_part_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < part_items.count(); ++i) {
    if (part_items.at(i).part_expr_ != NULL &&
        OB_FAIL(part_exprs.push_back(part_items.at(i).part_expr_))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (part_items.at(i).subpart_expr_ != NULL &&
               OB_FAIL(part_exprs.push_back(part_items.at(i).subpart_expr_))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    replacer.set_relation_scope();
    replacer.set_recursive(false);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replacer.add_replace_exprs(old_col_exprs, new_col_exprs, &part_exprs))) {
      LOG_WARN("failed to add replace exprs", K(ret));
    } else if (OB_FAIL(select_stmt.iterate_stmt_expr(replacer))) {
      LOG_WARN("failed to iterate stmt expr", K(ret));
    }
  }
  return ret;
}

int ObTransformLateMaterialization::generate_late_materialization_stmt(
                                                              const ObLateMaterializationInfo &info,
                                                              ObDMLStmt *stmt,
                                                              ObDMLStmt *&trans_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> old_col_exprs;
  ObSEArray<ObRawExpr*, 4> new_col_exprs;
  const TableItem *table_item = NULL;
  TableItem *view_table = NULL;
  ObSelectStmt *view_stmt = NULL;
  ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                      stmt, trans_stmt))) {
    LOG_WARN("failed to deep copy select stmt", K(ret));
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(trans_stmt))) {
  } else if (OB_ISNULL(table_item = select_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is NULL", K(ret));
  } else if (OB_FAIL(generate_late_materialization_view(info.project_col_in_view_,
                                                        select_stmt, view_stmt, view_table))) {
    LOG_WARN("failed to generate late materialization view", K(ret));
  } else if (OB_FAIL(extract_replace_column_exprs(*select_stmt, *view_stmt, table_item->table_id_,
                                                  view_table->table_id_, old_col_exprs,
                                                  new_col_exprs))) {
    LOG_WARN("failed to extract replace column exprs", K(ret));
  } else if (OB_FAIL(replace_expr_skip_part(*select_stmt, old_col_exprs, new_col_exprs))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  } else if (OB_FAIL(generate_pk_join_conditions(table_item->ref_id_, table_item->table_id_,
                                                 old_col_exprs, new_col_exprs, *select_stmt))) {
    LOG_WARN("failed generate pk join condition", K(ret));
  } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*select_stmt))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  } else if (OB_FAIL(select_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(select_stmt->formalize_stmt_expr_reference(ctx_->expr_factory_,
                                                                ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt expr reference", K(ret));
  } else if (OB_FAIL(generate_late_materialization_hint(info, *table_item, *view_table,
                                                        *select_stmt, *view_stmt))) {
    LOG_WARN("generate late materialization hint failed", K(ret));
  } else {
    LOG_TRACE("got late materialization info", K(info));
  }
  return ret;
}

int ObTransformLateMaterialization::generate_late_materialization_view(
                                                           const ObIArray<uint64_t> &select_col_ids,
                                                           ObSelectStmt *select_stmt,
                                                           ObSelectStmt *&view_stmt,
                                                           TableItem *&view_item)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item_inner = NULL;
  ObDMLStmt *tmp_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> select_col_exprs;
  ObSEArray<ObRawExpr*, 4> columns;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_UNLIKELY(select_col_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected param", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                      select_stmt, tmp_stmt))) {
    LOG_WARN("failed to deep copy select stmt", K(ret));
  } else if (FALSE_IT(view_stmt = static_cast<ObSelectStmt*>(tmp_stmt))) {
  } else if (OB_FAIL(view_stmt->adjust_statement_id(ctx_->allocator_, ctx_->src_qb_name_, ctx_->src_hash_val_))) {
    LOG_WARN("failed to recursive adjust statement id", K(ret));
  } else if (OB_FAIL(view_stmt->update_stmt_table_id(ctx_->allocator_, *select_stmt))) {
    LOG_WARN("failed to update table id", K(ret));
  } else if (OB_ISNULL(table_item_inner = view_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    view_stmt->get_select_items().reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_col_ids.count(); i++) {
      ObRawExpr *raw_expr = view_stmt->get_column_expr_by_id(table_item_inner->table_id_,
                                                             select_col_ids.at(i));
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(select_col_exprs.push_back(raw_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                          select_col_exprs,
                                                          view_stmt))) {
    LOG_WARN("failed to create select items", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, select_stmt, view_stmt, view_item))) {
    LOG_WARN("failed to add new table item", K(ret));
  } else if (OB_FAIL(select_stmt->add_from_item(view_item->table_id_))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(select_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuid table hash", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *view_item, select_stmt, columns))) {
    LOG_WARN("failed to create columns for view", K(ret));
  } else {
    select_stmt->get_condition_exprs().reset();
    select_stmt->set_limit_offset(NULL, NULL);
    select_stmt->set_limit_percent_expr(NULL);
    select_stmt->set_fetch_with_ties(false);
    select_stmt->set_has_fetch(false);
  }
  return ret;
}

int ObTransformLateMaterialization::extract_replace_column_exprs(const ObSelectStmt &select_stmt,
                                                                const ObSelectStmt &view_stmt,
                                                                const uint64_t table_id,
                                                                const uint64_t view_id,
                                                                ObIArray<ObRawExpr*> &old_col_exprs,
                                                                ObIArray<ObRawExpr*> &new_col_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *col_expr = NULL;
  ObRawExpr *old_col_expr = NULL;
  ObRawExpr *new_col_expr = NULL;
  old_col_exprs.reset();
  new_col_exprs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < view_stmt.get_select_item_size(); ++i) {
    if (OB_ISNULL(col_expr = view_stmt.get_select_item(i).expr_) ||
        OB_UNLIKELY(!col_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(old_col_exprs.push_back(col_expr))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_ISNULL(new_col_expr = select_stmt.get_column_expr_by_id(view_id,
                                                                       i + OB_APP_MIN_COLUMN_ID))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(new_col_exprs.push_back(new_col_expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_col_exprs.count(); ++i) {
    if (OB_ISNULL(old_col_exprs.at(i)) ||
        OB_ISNULL(old_col_expr = select_stmt.get_column_expr_by_id(table_id,
                         static_cast<ObColumnRefRawExpr*>(old_col_exprs.at(i))->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got unexpected param", K(ret));
    } else {
      old_col_exprs.at(i) = old_col_expr;
    }
  }
  return ret;
}

int ObTransformLateMaterialization::generate_pk_join_conditions(const uint64_t ref_table_id,
                                                          const uint64_t table_id,
                                                          const ObIArray<ObRawExpr*> &old_col_exprs,
                                                          const ObIArray<ObRawExpr*> &new_col_exprs,
                                                          ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> key_col_ids;
  ObSEArray<uint64_t, 4> part_col_ids;
  const bool is_mysql_mode = lib::is_mysql_mode();
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(schema_guard = ctx_->sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is NULL", K(ret));
  } else if (OB_FAIL(table_schema->get_rowkey_info().get_column_ids(key_col_ids))) {
    LOG_WARN("get rowkey column ids failed", K(ret));
  } else if (table_schema->get_partition_key_info().is_valid() &&
             OB_FAIL(table_schema->get_partition_key_info().get_column_ids(part_col_ids))) {
    LOG_WARN("get partition column ids failed", K(ret));
  } else if (table_schema->get_subpartition_key_info().is_valid() &&
             OB_FAIL(table_schema->get_subpartition_key_info().get_column_ids(part_col_ids))) {
    LOG_WARN("get subpartition column ids failed", K(ret));
  } else if (OB_FAIL(append_array_no_dup(key_col_ids, part_col_ids))) {
    LOG_WARN("failed to append", K(ret));
  } else {
    ObNotNullContext not_null_ctx(*ctx_, &select_stmt);
    if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_FROM))){
      LOG_WARN("failed to generate not null context", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < key_col_ids.count(); ++i) {
      uint64_t column_id = key_col_ids.at(i);
      ObRawExpr *equal_expr = NULL;
      ObRawExpr *view_col = NULL;
      ObRawExpr *table_col = NULL;
      int64_t idx = -1;
      bool is_not_null = false;
      ObArray<ObRawExpr *> constraints;
      if (OB_ISNULL(table_col = select_stmt.get_column_expr_by_id(table_id, column_id)) ||
          OB_UNLIKELY(!ObOptimizerUtil::find_item(old_col_exprs, table_col, &idx)) ||
          OB_UNLIKELY(idx < 0 || idx >= new_col_exprs.count()) ||
          OB_ISNULL(view_col = new_col_exprs.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx, view_col,
                                                            is_not_null, &constraints))) {
            LOG_WARN("failed to check expr not null", K(ret));
      } else if (is_not_null) {
        if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_, ctx_->session_info_,
                                                      view_col, table_col, equal_expr))) {
          LOG_WARN("failed to create equal expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
          LOG_WARN("failed to add param not null constraint", K(ret));
        } else if (OB_FAIL(select_stmt.get_condition_exprs().push_back(equal_expr))) {
          LOG_WARN("push back join condition failed", K(ret));
        }
      } else {
        if (OB_FAIL(ObRawExprUtils::create_null_safe_equal_expr(*ctx_->expr_factory_,
                            ctx_->session_info_, is_mysql_mode, table_col, view_col, equal_expr))) {
          LOG_WARN("failed to create null safe equal expr", K(ret));
        } else if (OB_FAIL(select_stmt.get_condition_exprs().push_back(equal_expr))) {
          LOG_WARN("push back join condition failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformLateMaterialization::generate_late_materialization_hint(
                                                              const ObLateMaterializationInfo &info,
                                                              const TableItem &table_item,
                                                              const TableItem &view_table,
                                                              ObSelectStmt &select_stmt,
                                                              ObSelectStmt &view_stmt)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item_inner = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObSEArray<ObItemType, 4> conflict_hints;
  ObString parent_qb_name;
  ObString view_qb_name;
  if (OB_ISNULL(ctx_) || OB_ISNULL(schema_guard = ctx_->sql_schema_guard_) ||
      OB_ISNULL(select_stmt.get_stmt_hint().query_hint_) ||
      OB_ISNULL(table_item_inner = view_stmt.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (select_stmt.get_stmt_hint().query_hint_->has_outline_data()) {
    /* do nothing */
  } else if (OB_FAIL(select_stmt.get_qb_name(parent_qb_name))) {
    LOG_WARN("get qb name failed", K(ret));
  } else if (OB_FAIL(view_stmt.get_qb_name(view_qb_name))) {
    LOG_WARN("get view qb name failed", K(ret));
  } else {
    // nlj_hint
    if (OB_SUCC(ret)) {
      ObJoinHint *join_hint = NULL;
      ObTableInHint table_in_hint(table_item.qb_name_, table_item.database_name_, table_item.get_object_name());
      if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_USE_NL, join_hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_ISNULL(join_hint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(join_hint->get_tables().push_back(table_in_hint))) {
        LOG_WARN("failde to push back", K(ret));
      } else {
        join_hint->set_qb_name(parent_qb_name);
        join_hint->set_trans_added(true);
        if (OB_FAIL(select_stmt.get_stmt_hint().merge_hint(*join_hint,
                                                          HINT_DOMINATED_EQUAL,
                                                          conflict_hints))) {
          LOG_WARN("merge join hint failed", K(ret));
        }
      }
    }
    // leading_hint
    if (OB_SUCC(ret)) {
      ObJoinOrderHint *join_order_hint = NULL;
      ObLeadingTable *leading_table = NULL;
      ObLeadingTable *left_leading_table = NULL;
      ObLeadingTable *right_leading_table = NULL;
      if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_LEADING, join_order_hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_ISNULL(join_order_hint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(ObQueryHint::create_leading_table(ctx_->allocator_, left_leading_table))) {
        LOG_WARN("failed to create leading table", K(ret));
      } else if (OB_FAIL(ObQueryHint::create_leading_table(ctx_->allocator_, right_leading_table))) {
        LOG_WARN("failed to create leading table", K(ret));
      } else if (OB_ISNULL(left_leading_table) || OB_ISNULL(right_leading_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(ObQueryHint::create_hint_table(ctx_->allocator_, left_leading_table->table_))) {
        LOG_WARN("fail to create hint table", K(ret));
      } else if (OB_FAIL(ObQueryHint::create_hint_table(ctx_->allocator_, right_leading_table->table_))) {
        LOG_WARN("fail to create hint table", K(ret));
      } else if (OB_ISNULL(left_leading_table->table_) || OB_ISNULL(right_leading_table->table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        left_leading_table->table_->qb_name_ = view_table.qb_name_;
        left_leading_table->table_->db_name_ = view_table.database_name_;
        left_leading_table->table_->table_name_ = view_table.get_object_name();
        right_leading_table->table_->qb_name_ = table_item.qb_name_;
        right_leading_table->table_->db_name_ = table_item.database_name_;
        right_leading_table->table_->table_name_ = table_item.get_object_name();
        join_order_hint->get_table().left_table_ = left_leading_table;
        join_order_hint->get_table().right_table_ = right_leading_table;
        join_order_hint->set_qb_name(parent_qb_name);
        join_order_hint->set_trans_added(true);
        if (OB_FAIL(select_stmt.get_stmt_hint().merge_hint(*join_order_hint,
                                                           HINT_DOMINATED_EQUAL,
                                                           conflict_hints))) {
          LOG_WARN("merge join hint failed", K(ret));
        }
      }
    }
    // index hint
    if (OB_SUCC(ret) && !info.is_allow_column_table_) {
      ObIArray<ObHint*> &opt_hints = select_stmt.get_stmt_hint().other_opt_hints_;
      for (int64_t i = opt_hints.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        ObHint* hint = opt_hints.at(i);
        if (OB_ISNULL(hint)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), KP(hint));
        } else if (hint->is_access_path_hint()) {
          ObIndexHint *index_hint = static_cast<ObIndexHint *>(hint);
          if ((T_INDEX_HINT == index_hint->get_hint_type() ||
               T_INDEX_ASC_HINT == index_hint->get_hint_type() ||
               T_INDEX_DESC_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_ASC_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_DESC_HINT == index_hint->get_hint_type() ||
               T_NO_INDEX_HINT == index_hint->get_hint_type() ||
               T_FULL_HINT == index_hint->get_hint_type())) {
            if (OB_FAIL(opt_hints.remove(i))) {
              LOG_WARN("failed to remove opt hint");
            }
          }
        }
      }
      ObIArray<ObHint*> &view_opt_hints = view_stmt.get_stmt_hint().other_opt_hints_;
      for (int64_t i = view_opt_hints.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        ObHint* hint = view_opt_hints.at(i);
        if (OB_ISNULL(hint)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), KP(hint));
        } else if (hint->is_access_path_hint()) {
          ObIndexHint *index_hint = static_cast<ObIndexHint *>(hint);
          if ((T_INDEX_HINT == index_hint->get_hint_type() ||
               T_INDEX_ASC_HINT == index_hint->get_hint_type() ||
               T_INDEX_DESC_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_ASC_HINT == index_hint->get_hint_type() ||
               T_INDEX_SS_DESC_HINT == index_hint->get_hint_type() ||
               T_NO_INDEX_HINT == index_hint->get_hint_type() ||
               T_FULL_HINT == index_hint->get_hint_type())) {
            if (OB_FAIL(view_opt_hints.remove(i))) {
              LOG_WARN("failed to remove opt hint");
            }
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < info.candi_indexs_.count(); ++i) {
        ObTableInHint table_in_hint(table_item_inner->qb_name_,
                                    table_item_inner->database_name_,
                                    table_item_inner->get_object_name());
        ObIndexHint *index_hint = NULL;
        if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_INDEX_HINT, index_hint))) {
          LOG_WARN("failed to create hint", K(ret));
        } else if (OB_FAIL(index_hint->get_table().assign(table_in_hint))) {
          LOG_WARN("assign table in hint failed", K(ret));
        } else {
          index_hint->get_index_name() = info.candi_index_names_.at(i);
          index_hint->set_qb_name(view_qb_name);
          index_hint->set_trans_added(true);
          if (OB_FAIL(view_stmt.get_stmt_hint().merge_hint(*index_hint,
                                                           HINT_DOMINATED_EQUAL,
                                                           conflict_hints))) {
            LOG_WARN("merge index hint failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && info.is_allow_column_table_) {
      ObTableInHint table_in_hint(table_item_inner->qb_name_,
                                  table_item_inner->database_name_,
                                  table_item_inner->get_object_name());
      ObIndexHint *index_hint = NULL;
      if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_USE_COLUMN_STORE_HINT, index_hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(index_hint->get_table().assign(table_in_hint))) {
        LOG_WARN("assign table in hint failed", K(ret));
      } else {
        index_hint->set_qb_name(view_qb_name);
        index_hint->set_trans_added(true);
        if (OB_FAIL(view_stmt.get_stmt_hint().merge_hint(*index_hint,
                                                        HINT_DOMINATED_EQUAL,
                                                        conflict_hints))) {
          LOG_WARN("merge index hint failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // ObTransHint *trans_hint = NULL;
      ObViewMergeHint *no_merge_hint = NULL;
      // view_merge_hint->set_is_used_query_push_down(hint_node.value_ == 1);
      if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_NO_MERGE_HINT, no_merge_hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else {
        no_merge_hint->set_parent_qb_name(parent_qb_name);
        no_merge_hint->set_qb_name(view_qb_name);
        if (OB_FAIL(select_stmt.get_stmt_hint().merge_hint(*no_merge_hint,
                                                           HINT_DOMINATED_EQUAL,
                                                           conflict_hints))) {
          LOG_WARN("merge no expand hint failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformLateMaterialization::is_expected_plan(ObLogPlan *plan,
                                                     void *check_ctx,
                                                     bool is_trans_plan,
                                                     bool &is_expected)
{
  int ret = OB_SUCCESS;
  ObCostBasedLateMaterializationCtx *local_ctx =
                                         static_cast<ObCostBasedLateMaterializationCtx*>(check_ctx);
  is_expected = false;
  if (OB_ISNULL(plan) || OB_ISNULL(local_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (is_trans_plan) {
    if (OB_FAIL(check_transform_plan_expected(plan->get_plan_root(), *local_ctx, is_expected))) {
      LOG_WARN("failed to check transform plan expected", K(ret));
    }
  } else if (!is_trans_plan) {
    if (OB_FAIL(get_index_of_base_stmt_path(plan->get_plan_root(), *local_ctx))) {
      LOG_WARN("failed to get base stmt best index", K(ret));
    } else {
      is_expected = true;
    }
  }
  return ret;
}

int ObTransformLateMaterialization::get_index_of_base_stmt_path(ObLogicalOperator *top,
                                                             ObCostBasedLateMaterializationCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *limit_op = NULL;
  ObLogicalOperator *sort_op = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(top));
  } else {
    while (OB_SUCC(ret) && 1 <= top->get_num_of_child() && log_op_def::LOG_TABLE_SCAN != top->get_type()) {
      if (log_op_def::LOG_LIMIT == top->get_type()) {
        limit_op = top;
      } else if (log_op_def::LOG_SORT == top->get_type()) {
        sort_op = top;
      }
      if (OB_ISNULL(top = top->get_child(ObLogicalOperator::first_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), KP(top));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(log_op_def::LOG_TABLE_SCAN != top->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the deeppest operator should be table scan", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObLogTableScan *table_scan = static_cast<ObLogTableScan *>(top);
      if (table_scan->is_index_scan() && (NULL != limit_op || NULL != sort_op)) {
        ctx.base_index_ = table_scan->get_index_table_id();
      } else {
        // otherwise table full scan or can do filter、 sort、limt on index table
      }
    }
  }
  return ret;
}

int ObTransformLateMaterialization::check_transform_plan_expected(ObLogicalOperator* top,
                                                             ObCostBasedLateMaterializationCtx &ctx,
                                                             bool &is_expected)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *full_table_scan = NULL;
  ObLogicalOperator *join_left_branch = NULL;
  is_expected = false;
  if (OB_ISNULL(top) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(top));
  } else {
    while (OB_SUCC(ret) && log_op_def::LOG_JOIN != top->get_type() && 1 == top->get_num_of_child()) {
      if (OB_ISNULL(top = top->get_child(ObLogicalOperator::first_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), KP(top));
      }
    }
    if (OB_SUCC(ret) && log_op_def::LOG_JOIN == top->get_type()) {
      ObLogJoin *join_op = static_cast<ObLogJoin*>(top);
      if (NESTED_LOOP_JOIN != join_op->get_join_algo()) {
        LOG_TRACE("not nlj, reject transform", K(join_op->get_join_algo()));
      } else if (OB_ISNULL(full_table_scan = join_op->get_child(ObLogicalOperator::second_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("operator is null", K(ret), KP(full_table_scan));
      } else if (log_op_def::LOG_TABLE_SCAN == full_table_scan->get_type() &&
                 ctx.late_table_id_ == static_cast<ObLogTableScan *>(full_table_scan)->get_table_id()) {
        if (OB_ISNULL(join_left_branch = join_op->get_child(ObLogicalOperator::first_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), KP(join_left_branch));
        } else {
          is_expected = true;
        }
      }
    }
    if (OB_SUCC(ret) && is_expected) {
      ObLogTableScan *index_scan = NULL;
      ObLogSort *sort_op = NULL;
      while (OB_SUCC(ret) && log_op_def::LOG_TABLE_SCAN != join_left_branch->get_type() &&
                             1 == join_left_branch->get_num_of_child()) {
        if (log_op_def::LOG_SORT == join_left_branch->get_type()) {
          sort_op = static_cast<ObLogSort *>(join_left_branch);
        }
        if (OB_ISNULL(join_left_branch = join_left_branch->get_child(ObLogicalOperator::first_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), KP(join_left_branch));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (log_op_def::LOG_TABLE_SCAN != join_left_branch->get_type()) {
        is_expected = false;
        LOG_TRACE("not table scan, reject transform");
      } else if (FALSE_IT(index_scan = static_cast<ObLogTableScan *>(join_left_branch))) {
      } else if (!ctx.check_column_store_) {
        // row store table
        if (!index_scan->is_index_scan()) {
          is_expected = false;
          LOG_TRACE("not index scan, reject transform", K(index_scan->is_index_scan()));
        } else if (NULL == sort_op &&
                   ObOptimizerUtil::find_item(ctx.check_sort_indexs_, index_scan->get_index_table_id())) {
          is_expected = false;
          LOG_TRACE("check sort op, reject transform");
        } else if (index_scan->get_index_back()) {
          if (ObOptimizerUtil::find_item(ctx.late_material_indexs_, index_scan->get_index_table_id())) {
            is_expected = false;
            LOG_TRACE("index back, reject transform");
          } else {
            // inside evaluate_cost, the index back tag is inaccurate. Then do double check here
            ObSEArray<ObRawExpr*, 4> temp_exprs;
            ObSEArray<uint64_t, 4> used_column_ids;
            const ObTableSchema *index_schema = NULL;
            ObSqlSchemaGuard *schema_guard = ctx_->sql_schema_guard_;
            ObSEArray<uint64_t, 4> index_column_ids;
            if (sort_op != NULL && OB_FAIL(sort_op->get_sort_exprs(temp_exprs))) {
              LOG_WARN("failed to get sort exprs", K(ret));
            } else if (index_scan != NULL && OB_FAIL(append(temp_exprs, index_scan->get_filter_exprs()))) {
              LOG_WARN("failed to get sort exprs", K(ret));
            } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(temp_exprs, used_column_ids))) {
              LOG_WARN("failed to extract column ids", K(ret));
            } else if (OB_FAIL(schema_guard->get_table_schema(index_scan->get_index_table_id(), index_schema))) {
              LOG_WARN("fail to get index schema", K(ret));
            } else if (OB_FAIL(index_schema->get_column_ids(index_column_ids))) {
              LOG_WARN("failed to get column ids", K(ret));
            } else if (ObOptimizerUtil::is_subset(used_column_ids, index_column_ids)) {
              is_expected = true;
            } else {
              is_expected = false;
              LOG_TRACE("index back, reject transform");
            }
          }
        }
        if (OB_SUCC(ret) && is_expected && NULL != index_scan) {
          // check table range scan
          bool is_get = false;
          if (OB_FAIL(index_scan->is_table_get(is_get))) {
            LOG_WARN("failed to consider is table get", K(ret));
          } else if (!is_get && index_scan->get_range_conditions().empty()) {
            // if there is no range, then there is no need to late material
            is_expected = false;
            LOG_TRACE("there is no range", K(ret));
          }
        }
      } else {
        // column store table
        if (!index_scan->use_column_store()) {
          is_expected = false;
          LOG_TRACE("not use column store, reject transform");
        } else if (NULL == sort_op) {
          is_expected = false;
          LOG_TRACE("check sort op, reject transform");
        }
      }
    }
  }
  return ret;
}

int ObTransformLateMaterialization::contain_enum_set_rowkeys(const ObRowkeyInfo &rowkey_info,
                                                             bool &contain)
{
  int ret = OB_SUCCESS;
  const ObRowkeyColumn *col = NULL;
  contain = false;
  for (int64_t i = 0; OB_SUCC(ret) && !contain && i < rowkey_info.get_size(); ++i) {
    if (OB_ISNULL(col = rowkey_info.get_column(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ob_is_enumset_tc(col->get_meta_type().get_type())) {
      contain = true;
    }
  }
  return ret;
}

/* select * from t1 where c1 > 10 order by c2 limit 3;
    -->
    select t1.* from (select / *+use_column_table(t1)* /key_col_ids from t1 where c1 > 10 order by c2 limit 3) v,
                    t1
                where v.pk = t1.pk order by t1.c2;
*/
int ObTransformLateMaterialization::gen_trans_info_for_column_store(const ObSelectStmt &stmt,
                                                                    const ObIArray<uint64_t> &key_col_ids,
                                                                    const ObIArray<uint64_t> &filter_col_ids,
                                                                    const ObIArray<uint64_t> &orderby_col_ids,
                                                                    const ObIArray<uint64_t> &select_col_ids,
                                                                    const TableItem *table_item,
                                                                    const ObTableSchema *table_schema,
                                                                    ObLateMaterializationInfo &info,
                                                                    ObCostBasedLateMaterializationCtx &check_ctx)
{
  int ret = OB_SUCCESS;
  bool is_allow_column_store = false;
  if (OB_FAIL(check_is_allow_column_store(stmt, table_item, table_schema, is_allow_column_store))) {
    LOG_WARN("failed to check is allow column store", K(ret));
  } else if (!is_allow_column_store) {
    OPT_TRACE("not allow column store late materialization");
  } else if (OB_FAIL(info.project_col_in_view_.assign(key_col_ids))) {
    LOG_WARN("failed to assign predicate col in view", K(ret));
  } else {
    info.is_allow_column_table_ = true;
    check_ctx.late_table_id_ = table_item->table_id_;
    check_ctx.check_column_store_ = true;
  }
  return ret;
}

int ObTransformLateMaterialization::check_is_allow_column_store(const ObSelectStmt &stmt,
                                                                const TableItem *table_item,
                                                                const ObTableSchema *table_schema,
                                                                bool &is_allow)
{
  int ret = OB_SUCCESS;
  int64_t route_policy_type = 0;
  bool has_all_column_group = false;
  bool has_normal_column_group = false;
  const ObQueryHint *query_hint = NULL;
  is_allow = true;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(table_item) ||
      OB_ISNULL(table_schema) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected NULL ptr", K(ret));
  } else if (table_item->is_link_table()) {
    is_allow = false;
  } else if (OB_FAIL(table_schema->has_all_column_group(has_all_column_group))) {
    LOG_WARN("failed to check has row store", K(ret));
  } else if (OB_FAIL(table_schema->get_is_column_store(has_normal_column_group))) {
    LOG_WARN("failed to get is column store", K(ret));
  } else if (!ctx_->session_info_->is_enable_column_store()) {
    if (has_all_column_group) {
      is_allow = false;
    }
  } else {
    is_allow = has_normal_column_group;
  }
  if (OB_SUCC(ret) && is_allow) {
    const ObIArray<ObHint*> &opt_hints = stmt.get_stmt_hint().other_opt_hints_;
    for (int64_t i = 0; OB_SUCC(ret) && i < opt_hints.count(); ++i) {
      ObIndexHint *index_hint = NULL;
      int64_t index_schema_i = 0;
      if (OB_ISNULL(opt_hints.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (!opt_hints.at(i)->is_access_path_hint()) {
        /* do nothing */
      } else if (FALSE_IT(index_hint = static_cast<ObIndexHint *>(opt_hints.at(i)))) {
        /* do nothing */
      } else if (T_NO_USE_COLUMN_STORE_HINT == index_hint->get_hint_type()) {
        if (index_hint->get_table().is_match_table_item(query_hint->cs_type_, *table_item)) {
          is_allow = false;
        }
      }
    }
  }
  return ret;
}

}
}
