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

#include "ob_transform_temp_table.h"
#include "sql/optimizer/ob_log_plan.h"
#include "ob_transform_min_max.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_subplan_scan.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

typedef ObDMLStmt::TempTableInfo TempTableInfo;

ObTransformTempTable::~ObTransformTempTable()
{
  if (OB_NOT_NULL(trans_param_)) {
    trans_param_->~TempTableTransParam();
    trans_param_ = NULL;
  }
}

int ObTransformTempTable::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                             ObDMLStmt *&stmt,
                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  //当前stmt是root stmt时才改写
  if (parent_stmts.empty()) {
    is_happened = false;
    void *buf = NULL;
    if (OB_UNLIKELY(NULL != trans_param_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans param", K(ret));
    } else if(OB_ISNULL(buf = allocator_.alloc(sizeof(TempTableTransParam)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      trans_param_ = new(buf)TempTableTransParam;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_with_clause(stmt, is_happened))) {
        LOG_WARN("failed to generate with clause", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("generate with clause:", is_happened);
        LOG_TRACE("succeed to generate with clause",  K(is_happened));
      }
    }

    ObArray<TempTableInfo> temp_table_infos;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->collect_temp_table_infos(temp_table_infos))) {
        LOG_WARN("failed to collect temp table infos", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(project_pruning(temp_table_infos, is_happened))) {
        LOG_WARN("failed to do project pruning for temp table", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("project pruning for temp table:", is_happened);
        LOG_TRACE("succeed to do project pruning for temp table", K(temp_table_infos),  K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_inline_temp_table(stmt, temp_table_infos, is_happened))) {
        LOG_WARN("failed to inline temp table", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("inline temp table:", is_happened);
        LOG_TRACE("succeed to inline temp table",  K(is_happened));
      }
    }
  }
  return ret;
}

int ObTransformTempTable::check_stmt_size(ObDMLStmt *stmt, int64_t &total_size, bool &stmt_oversize)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check stmt size failed", K(ret));
  } else if (stmt->is_select_stmt() &&
      static_cast<const ObSelectStmt *>(stmt)->is_set_stmt() &&
        OB_FAIL(static_cast<const ObSelectStmt *>(stmt)->get_set_stmt_size(size))) {
    LOG_WARN("failed to get set stm size", K(ret));
  } else if (OB_FALSE_IT(total_size = total_size + size)) {
  } else if (total_size > common::OB_MAX_SET_STMT_SIZE) {
    stmt_oversize = true;
  } else {
    ObSEArray<ObSelectStmt*, 16> child_stmts;
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !stmt_oversize && i < child_stmts.count(); i++) {
      if (OB_FAIL(SMART_CALL(check_stmt_size(child_stmts.at(i), total_size, stmt_oversize)))) {
        LOG_WARN("check stmt size failed", K(ret));
      }
    }
  }
  return ret;
}
int ObTransformTempTable::generate_with_clause(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 8> child_stmts;
  ObSEArray<ObSelectStmt*, 8> non_correlated_stmts;
  ObSEArray<ObSelectStmt*, 8> view_stmts;
  ObArray<TempTableInfo> temp_table_infos;
  hash::ObHashMap<uint64_t, ObParentDMLStmt> parent_map;
  trans_happened = false;
  bool enable_temp_table_transform = false;
  bool has_hint = false;
  bool is_hint_enabled = false;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx()) ||
      OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ctx_), K(ret));
  } else if (OB_FAIL(session_info->is_temp_table_transformation_enabled(enable_temp_table_transform))) {
    LOG_WARN("failed to check temp table transform enabled", K(ret));
  } else if (OB_FAIL(stmt->get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
              ObOptParamHint::XSOLAPI_GENERATE_WITH_CLAUSE, is_hint_enabled, has_hint))) {
    LOG_WARN("failed to check has opt param", K(ret));
  } else if (has_hint) {
    enable_temp_table_transform = is_hint_enabled;
  }
  if (OB_FAIL(ret)) {
  } else if (ctx_->eval_cost_) {
    OPT_TRACE("disable CTE extraction during cost evaluation");
  } else if (ctx_->is_set_stmt_oversize_) {
    OPT_TRACE("stmt containt oversize set stmt");
  } else if (!enable_temp_table_transform || ctx_->is_force_inline_) {
    OPT_TRACE("session variable disable temp table transform");
  } else if (stmt->has_for_update()) {
    OPT_TRACE("stmt has for update, can not extract CTE");
  } else if (OB_FAIL(parent_map.create(128, "TempTable"))) {
    LOG_WARN("failed to init stmt map", K(ret));
  } else if (!ObOptimizerUtil::find_item(ctx_->temp_table_ignore_stmts_, stmt) &&
             OB_FAIL(ObTransformUtils::get_all_child_stmts(stmt, child_stmts, &parent_map, &ctx_->temp_table_ignore_stmts_))) {
    LOG_WARN("failed to get all child stmts", K(ret));
  } else if (stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_5, COMPAT_VERSION_4_3_0, COMPAT_VERSION_4_3_5)) {
    if (OB_FAIL(get_all_view_stmts(stmt, view_stmts))) {
      LOG_WARN("failed to get non correlated subquery", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::intersect(child_stmts, view_stmts, child_stmts))) {
      LOG_WARN("failed to intersect child stmts", K(ret));
    }
  } else {
    if (OB_FAIL(get_non_correlated_subquery(stmt, non_correlated_stmts))) {
      LOG_WARN("failed to get non correlated subquery", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::intersect(child_stmts, non_correlated_stmts, child_stmts))) {
      LOG_WARN("failed to intersect child stmts", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(extract_common_table_expression(stmt, child_stmts, parent_map, trans_happened))) {
    LOG_WARN("failed to extract common subquery as cte", K(ret));
  } else if (OB_FAIL(parent_map.destroy())) {
    LOG_WARN("failed to destroy map", K(ret));
  }
  return ret;
}

/**
 * @brief expand_temp_table
 * 尝试基于规则和代价的 inline 判定
 * 规则：单表无聚合，只被引用一次，不满足物化条件...
 */
int ObTransformTempTable::try_inline_temp_table(ObDMLStmt *stmt,
                                                ObIArray<TempTableInfo> &temp_table_info,
                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) ||
      OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ctx_), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_info.count(); ++i) {
    TempTableInfo &helper = temp_table_info.at(i);
    bool can_materia = false;
    bool force_materia = false;
    bool force_inline = false;
    bool is_oversize_stmt = false;
    bool has_for_update = false;
    int64_t stmt_size = 0;
    bool need_inline = false;
    bool can_inline = true;
    bool need_check_cost = false;
    bool in_blacklist = false;
    OPT_TRACE("try to inline temp table:", helper.temp_table_query_);
    if (OB_ISNULL(helper.temp_table_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", K(helper), K(ret));
    } else if (OB_FAIL(check_stmt_size(helper.temp_table_query_, stmt_size, is_oversize_stmt))) {
      LOG_WARN("check stmt size failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_inline_temp_table_valid(helper.temp_table_query_, can_inline))) {
      LOG_WARN("failed to check inline temp table valid", K(ret));
    } else if (OB_FAIL(helper.temp_table_query_->is_query_deterministic(can_inline))) {
      LOG_WARN("failed to check stmt is deterministic", K(ret));
    } else if (OB_FAIL(check_has_for_update(helper, has_for_update))) {
      LOG_WARN("failed to check has for update", K(ret));
    } else if (!can_inline) {
      // do nothing
      OPT_TRACE("CTE can not be inlined");
    } else if (OB_FAIL(check_hint_allowed_trans(*helper.temp_table_query_,
                                                force_inline,
                                                force_materia))) {
      LOG_WARN("failed to check force materialize", K(ret));
    } else if (force_inline) {
      need_inline = true;
      OPT_TRACE("hint force inline CTE");
    } else if (lib::is_oracle_mode() && has_for_update) {
      need_inline = true;
      OPT_TRACE("stmt has FOR UPDATE, force inline CTE");
    } else if (force_materia) {
      //do nothing
      OPT_TRACE("hint force materialize CTE");
    } else if (is_oversize_stmt) {
      //do nothing
      OPT_TRACE("CTE too large to inline");
    } else if (ctx_->is_force_materialize_) {
      //do nothing
      OPT_TRACE("system variable force materialize CTE");
    } else if (ctx_->is_force_inline_) {
      need_inline = true;
      OPT_TRACE("system variable force inline CTE");
    } else if (1 == helper.table_items_.count()) {
      need_inline = true;
      OPT_TRACE("CTE`s refer once, force inline");
    } else if (OB_FAIL(check_stmt_can_materialize(helper.temp_table_query_, true, can_materia))) {
      LOG_WARN("failed to check extract cte valid", K(ret));
    } else if (!can_materia) {
      need_inline = true;
      OPT_TRACE("transform rule force inline CTE");
    } else if (OB_FAIL(check_stmt_in_blacklist(helper.temp_table_query_,
                                               ctx_->inline_blacklist_,
                                               in_blacklist))) {
      LOG_WARN("failed to check cte in blacklist", K(ret));
    } else if (in_blacklist) {
      OPT_TRACE("reject inline CTE due to blacklist");
    } else if (ctx_->eval_cost_ || helper.temp_table_query_->is_recursive_union()) {
      // only try inline by rule
    } else {
      need_check_cost = true;
    }

    if (OB_FAIL(ret)) {
    } else if (stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_5, COMPAT_VERSION_4_3_0, COMPAT_VERSION_4_3_5) &&
               need_check_cost &&
               OB_FAIL(check_inline_temp_table_by_cost(stmt, helper, need_inline))) {
      LOG_WARN("failed to check inline temp table by cost", K(ret));
    } else if (!need_inline) {
      // do nothing
    } else {
      //深拷贝每一份查询，还原成generate table
      ObDMLStmt *orig_stmt = helper.temp_table_query_;
      if (OB_FAIL(ObTransformUtils::inline_temp_table(ctx_, helper))) {
        LOG_WARN("failed to extend temp table", K(ret));
      } else if (OB_FAIL(add_normal_temp_table_trans_hint(*orig_stmt, T_INLINE))) {
        LOG_WARN("failed to add transform hint", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformTempTable::check_stmt_can_materialize(ObSelectStmt *stmt, bool is_existing_cte, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  bool has_cross_product = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret));
  } else if (0 == stmt->get_table_items().count() &&
             !stmt->is_set_stmt()) {
    //expression stmt不允许物化
    is_valid = false;
    OPT_TRACE("expression stmt can not materialize")
  } else if (1 == stmt->get_table_items().count()) {
    if (is_existing_cte) {
      TableItem *table = stmt->get_table_item(0);
      if (stmt->has_group_by() ||
          stmt->has_limit() ||
          stmt->has_window_function() ||
          table->is_generated_table()) {
        is_valid = true;
      } else {
        is_valid = false;
        OPT_TRACE("single table cte will not be materialized");
      }
    } else {
      // Currently, we will not push `limit` in stmt into cte
      bool can_use_fast_min_max = false;
      STOP_OPT_TRACE;
      if (OB_FAIL(ObTransformMinMax::check_transform_validity(*ctx_, stmt, can_use_fast_min_max))) {
        LOG_WARN("failed to check fast min max", K(ret));
      }
      RESUME_OPT_TRACE;
      if (OB_FAIL(ret)) {
      } else if (can_use_fast_min_max) {
        is_valid = false;
        OPT_TRACE("fast min max query will not be materialized");
      } else if (stmt->has_group_by() ||
                 stmt->has_window_function()) {
        is_valid = true;
      } else {
        is_valid = false;
        OPT_TRACE("single table query without aggr/win will not be materialized");
      }
    }
  } else if (OB_FAIL(check_stmt_has_cross_product(stmt, has_cross_product))) {
    LOG_WARN("failed to check has cross product", K(ret));
  } else if (has_cross_product) {
    is_valid = false;
    OPT_TRACE("stmt has cross produce, will not be materialized");
  }
  return ret;
}

int ObTransformTempTable::check_stmt_has_cross_product(ObSelectStmt *stmt, bool &has_cross_product)
{
  int ret = OB_SUCCESS;
  has_cross_product = false;
  ObSEArray<ObRawExpr*, 4> on_conditions;
  ObSqlBitSet<> table_ids;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (stmt->is_set_stmt()) {
    ObIArray<ObSelectStmt*> &set_query = stmt->get_set_query();
    //继续检查set op的每个分支
    for (int64_t i = 0; OB_SUCC(ret) && !has_cross_product && i < set_query.count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_stmt_has_cross_product(set_query.at(i),
                                                          has_cross_product)))) {
        LOG_WARN("failed to check stmt condition", K(ret));
      }
    }
  } else if (stmt->is_hierarchical_query()) {
    //层次查询在post process之前都是笛卡尔积，可忽略
  } else if (0 == stmt->get_table_items().count()) {
    has_cross_product = true;
  } else if (1 == stmt->get_table_items().count()) {
    //do nothing
  } else if (OB_FAIL(ObTransformUtils::get_on_conditions(*stmt, on_conditions))) {
    LOG_WARN("failed to get on conditions", K(ret));
  } else {
    ObIArray<ObRawExpr*> &where_conditions = stmt->get_condition_exprs();
    //收集连接条件引用的所有表
    for (int64_t i = 0; OB_SUCC(ret) && i < where_conditions.count(); ++i) {
      ObRawExpr *expr = where_conditions.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!expr->has_flag(IS_JOIN_COND)) {
        //do nothing
      } else if (OB_FAIL(table_ids.add_members(expr->get_relation_ids()))) {
        LOG_WARN("failed to add relation ids", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < on_conditions.count(); ++i) {
      ObRawExpr *expr = on_conditions.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (expr->get_relation_ids().num_members() < 2) {
        //do nothing
      } else if (OB_FAIL(table_ids.add_members(expr->get_relation_ids()))) {
        LOG_WARN("failed to add relation ids", K(ret));
      }
    }
    const ObIArray<SemiInfo*> &semi_infos = stmt->get_semi_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      const SemiInfo *info = semi_infos.at(i);
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null semi info", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < info->semi_conditions_.count(); ++j) {
        const ObRawExpr *expr = info->semi_conditions_.at(j);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (expr->get_relation_ids().num_members() < 2) {
          //do nothing
        } else if (OB_FAIL(table_ids.add_members(expr->get_relation_ids()))) {
          LOG_WARN("failed to add relation ids", K(ret));
        }
      }
    }
    //如果有表没有被连接条件引用，说明有笛卡尔积出现
    for (int64_t i = 0; OB_SUCC(ret) && !has_cross_product && i < stmt->get_table_items().count(); ++i) {
      TableItem *table = stmt->get_table_item(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!table_ids.has_member(stmt->get_table_bit_index(table->table_id_))) {
        //has cross product
        has_cross_product = true;
      } else if (!table->is_generated_table()) {
        //do nothing
      } else if (OB_FAIL(SMART_CALL(check_stmt_has_cross_product(table->ref_query_,
                                                                 has_cross_product)))) {
        LOG_WARN("failed to check stmt condition", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformTempTable::check_has_for_update(const ObDMLStmt::TempTableInfo &helper,
                                               bool &has_for_update)
{
  int ret = OB_SUCCESS;
  has_for_update = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < helper.upper_stmts_.count(); ++i) {
    const ObDMLStmt *upper_stmt = helper.upper_stmts_.at(i);
    if (OB_ISNULL(upper_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upper stmt is NULL", K(ret), K(i));
    } else if (upper_stmt->has_for_update()) {
      has_for_update = true;
      break;
    }
  }
  return ret;
}

/**
 * @brief extract_common_table_expression
 * 比较当前stmt的所有child stmt，
 * 把所有相似的stmt分为一组，抽离最大的公共部分作为temp table
 */
int ObTransformTempTable::extract_common_table_expression(ObDMLStmt *stmt,
                                                         ObIArray<ObSelectStmt*> &stmts,
                                                         hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                                         bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<StmtClassifyHelper, 8> stmt_groups;
  const ObQueryHint *query_hint = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(trans_param_)
      || OB_ISNULL(query_hint = stmt->get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(remove_simple_stmts(stmts))) {
    LOG_WARN("failed to remove simple stmts", K(ret));
  } else if (OB_FAIL(classify_stmts(stmts, stmt_groups))) {
    LOG_WARN("failed to sort stmts", K(ret));
  }
  //对每一组stmt抽离公共部分
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_groups.count(); ++i) {
    bool is_happened = false;
    if (OB_FAIL(inner_extract_common_table_expression(*stmt,
                                                     stmt_groups.at(i).stmts_, 
                                                     parent_map,
                                                     is_happened))) {
      LOG_WARN("failed to convert temp table", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  if (OB_SUCC(ret) && trans_happened) {
    trans_param_->trans_type_ = T_MATERIALIZE;
    if (OB_FAIL(add_transform_hint(*stmt, trans_param_))) {
      LOG_WARN("failed to add hint", K(ret));
    } else if (query_hint->has_outline_data()) {
      ++ctx_->trans_list_loc_;
    }
  }
  return ret;
}

/**
 * @brief inner_extract_common_table_expression
 * stmt之间两两比较，分成多个相似组，
 * 对每组相似stmt创建temp table
 */
int ObTransformTempTable::inner_extract_common_table_expression(ObDMLStmt &root_stmt,
                                                               ObIArray<ObSelectStmt*> &stmts,
                                                               hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObStmtMapInfo map_info;
  QueryRelation relation;
  ObSEArray<StmtCompareHelper*, 8> compare_infos;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); ++i) {
    bool find_similar = false;
    ObSelectStmt *stmt = stmts.at(i);
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt ", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !find_similar && j < compare_infos.count(); ++j) {
      bool has_stmt = false;
      bool is_valid = false;
      StmtCompareHelper *helper = compare_infos.at(j);
      map_info.reset();
      if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got unexpected NULL ptr", K(ret));
      } else if (OB_FAIL(check_has_stmt(helper->similar_stmts_, stmt, parent_map, has_stmt))) {
        LOG_WARN("failed to check has stmt", K(ret));
      } else if (has_stmt) {
        //do nothing
      } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(helper->stmt_,
                                                                stmt,
                                                                map_info,
                                                                relation))) {
        LOG_WARN("failed to check stmt containment", K(ret));
      } else if (OB_FAIL(check_stmt_can_extract_temp_table(helper->stmt_,
                                                           stmt,
                                                           map_info,
                                                           relation,
                                                           !helper->hint_force_stmt_set_.empty() &&
                                                           helper->hint_force_stmt_set_.has_qb_name(stmt),
                                                           is_valid))) {
        LOG_WARN("failed to check is similar stmt");
      } else if (!is_valid) {
        //do nothing
      } else if (OB_FAIL(helper->similar_stmts_.push_back(stmt))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else if (OB_FAIL(helper->stmt_map_infos_.push_back(map_info))) {
        LOG_WARN("failed to push back map info", K(ret));
      } else {
        find_similar = true;
      }
    }
    if (OB_SUCC(ret) && !find_similar) {
      StmtCompareHelper *new_helper = NULL;
      bool force_no_trans = false;
      QbNameList qb_names;
      map_info.reset();
      if (OB_FAIL(get_hint_force_set(root_stmt, *stmt, qb_names, force_no_trans))) {
        LOG_WARN("failed to get hint set", K(ret));
      } else if (force_no_trans) {
        //do nothing
        OPT_TRACE("hint reject materialize:", stmt);
      } else if (OB_FAIL(StmtCompareHelper::alloc_compare_helper(*ctx_->allocator_, new_helper))) {
        LOG_WARN("failed to alloc compare helper", K(ret));
      } else if (OB_ISNULL(new_helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null compare helper", K(ret));
      } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(stmt, stmt, map_info, relation))) {
        LOG_WARN("failed to check stmt containment", K(ret));
      } else if (OB_FAIL(new_helper->similar_stmts_.push_back(stmt))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else if (OB_FAIL(new_helper->stmt_map_infos_.push_back(map_info))) {
        LOG_WARN("failed to push back map info", K(ret));
      } else if (OB_FAIL(new_helper->hint_force_stmt_set_.assign(qb_names))) {
        LOG_WARN("failed to assign qb names", K(ret));
      } else if (OB_FALSE_IT(new_helper->stmt_ = stmt)) {
      } else if (OB_FAIL(compare_infos.push_back(new_helper))) {
        LOG_WARN("failed to push back compare info", K(ret));
      }
    }
  }
  //对每组相似stmt创建temp table
  for (int64_t i = 0; OB_SUCC(ret) && i < compare_infos.count(); ++i) {
    StmtCompareHelper *helper = compare_infos.at(i);
    bool is_happened = false;
    if (OB_ISNULL(helper)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got unexpected NULL ptr", K(ret));
    } else {
      OPT_TRACE("try to materialize:", helper->stmt_);
      if (!helper->hint_force_stmt_set_.empty() &&
          !helper->hint_force_stmt_set_.is_subset(helper->similar_stmts_)) {
        //hint forbid, do nothing
        OPT_TRACE("hint reject transform");
      } else if (helper->hint_force_stmt_set_.empty() && helper->similar_stmts_.count() < 2) {
        //do nothing
        OPT_TRACE("no other similar stmts");
      } else if (OB_FAIL(create_temp_table(root_stmt, *helper, parent_map, is_happened))) {
        LOG_WARN("failed to create temp table", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    if (NULL != compare_infos.at(i)) {
      compare_infos.at(i)->~StmtCompareHelper();
      compare_infos.at(i) = NULL;
    }
  }
  return ret;
}

int ObTransformTempTable::add_materialize_stmts(const ObIArray<ObSelectStmt*> &stms)
{
  int ret = OB_SUCCESS;
  MaterializeStmts *new_stmts = NULL;
  if (OB_ISNULL(trans_param_) ||
      OB_ISNULL(new_stmts = (MaterializeStmts *) allocator_.alloc(sizeof(MaterializeStmts)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate stmts array", K(ret));
  } else {
    new_stmts = new (new_stmts) MaterializeStmts();
    if (OB_FAIL(new_stmts->assign(stms))) {
      LOG_WARN("failed to assign array", K(ret));
    } else if (OB_FAIL(trans_param_->materialize_stmts_.push_back(new_stmts))) {
      LOG_WARN("failed to push back stmts", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::check_has_stmt(ObSelectStmt *left_stmt,
                                         ObSelectStmt *right_stmt,
                                         hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                         bool &has_stmt)
{
  int ret = OB_SUCCESS;
  has_stmt = false;
  ObDMLStmt *current = left_stmt;
  ObParentDMLStmt parent_stmt;
  bool get_temp_table = false;
  while (OB_SUCC(ret) && current != right_stmt && NULL != current && !get_temp_table) {
    uint64_t key = reinterpret_cast<uint64_t>(current);
    if (OB_FAIL(parent_map.get_refactored(key, parent_stmt))) {
      if (ret == OB_HASH_NOT_EXIST) {
        current = NULL;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get value", K(ret));
      }
    } else if (NULL != parent_stmt.stmt_) {
      current = parent_stmt.stmt_;
    } else {
      // current is a temp table query, and might has multi parents
      get_temp_table = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (current == right_stmt) {
      has_stmt = true;
    } else if (get_temp_table) {
      // reverse the search direction
      // ret = check_has_stmt(right_stmt, left_stmt, has_stmt);
      ObDMLStmt *tmp = NULL;
      if (OB_ISNULL(right_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null stmt", K(ret));
      } else if (OB_FAIL(right_stmt->get_stmt_by_stmt_id(current->get_stmt_id(), tmp))) {
        LOG_WARN("failed to get stmt by stmt id", K(ret), K(current->get_stmt_id()), KPC(right_stmt));
      } else if (current == tmp) {
        has_stmt = true;
      }
    }

  }
  return ret;
}

int ObTransformTempTable::check_has_stmt(const ObIArray<ObSelectStmt *> &stmts,
                                         ObSelectStmt *right_stmt,
                                         hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                         bool &has_stmt)
{
  int ret = OB_SUCCESS;
  has_stmt = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_stmt && i < stmts.count(); i ++) {
    if (OB_FAIL(check_has_stmt(stmts.at(i), right_stmt, parent_map, has_stmt))) {
      LOG_WARN("failed to check has stmt", K(ret));
    } else if (has_stmt) {
      //do nothing
    } else if (OB_FAIL(check_has_stmt(right_stmt, stmts.at(i), parent_map, has_stmt))) {
      LOG_WARN("failed to check has stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::check_stmt_can_extract_temp_table(ObSelectStmt *first,
                                                            ObSelectStmt *second,
                                                            const ObStmtMapInfo &map_info,
                                                            QueryRelation relation,
                                                            bool check_basic_similarity,
                                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (first->is_scala_group_by() ^ second->is_scala_group_by()) {
    is_valid = false;
  } else if (second->is_set_stmt()) {
    is_valid = QueryRelation::QUERY_EQUAL == relation && map_info.is_order_equal_;
  } else if (second->get_table_size() < 2) {
    if (second->get_group_expr_size() > 0 ||
        second->get_rollup_expr_size() > 0) {
      is_valid = map_info.is_group_equal_ && map_info.is_cond_equal_;
    } else if (second->get_aggr_item_size() > 0) {
      is_valid = map_info.is_table_equal_ && map_info.is_from_equal_ && map_info.is_semi_info_equal_ && map_info.is_cond_equal_;
    }
  } else if (map_info.is_table_equal_ && map_info.is_from_equal_ && map_info.is_semi_info_equal_) {
    is_valid = true;
    if (map_info.is_cond_equal_ || check_basic_similarity) {
      // do nothing
    } else if (OB_FAIL(check_equal_join_condition_match(*first, *second, map_info, is_valid))) {
      LOG_WARN("failed to check condition", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(check_index_condition_match(*first, *second, map_info, is_valid))) {
      LOG_WARN("failed to check condition", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::check_equal_join_condition_match(ObSelectStmt &first,
                                                           ObSelectStmt &second,
                                                           const ObStmtMapInfo &map_info,
                                                           bool &is_match)
{
  int ret = OB_SUCCESS;
  // Check equal join condition
  ObSqlBitSet<> map_join_conds;
  is_match = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_match && i < first.get_condition_size(); ++i) {
    ObRawExpr *expr = first.get_condition_expr(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (expr->has_flag(IS_JOIN_COND)) {
      int64_t cond_pos_in_other = map_info.cond_map_.at(i);
      if (OB_INVALID_ID == cond_pos_in_other) {
        // Equal join conds of first stmt not in second stmt
        is_match = false;
      } else if (OB_FAIL(map_join_conds.add_member(cond_pos_in_other))) {
        LOG_WARN("failed to add member", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_match && i < second.get_condition_size(); ++i) {
    ObRawExpr *expr = second.get_condition_expr(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (expr->has_flag(IS_JOIN_COND) && !map_join_conds.has_member(i)) {
      // Equal join conds of second stmt not in first stmt
      is_match = false;
    }
  }
  return ret;
}

int ObTransformTempTable::check_index_condition_match(ObSelectStmt &first,
                                                      ObSelectStmt &second,
                                                      const ObStmtMapInfo &map_info,
                                                      bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  ObSqlBitSet<> map_index_conds;
  for (int64_t i = 0; OB_SUCC(ret) && is_match && i < first.get_condition_size(); ++i) {
    ObRawExpr *cond = NULL;
    bool index_match = false;
    if (OB_ISNULL(cond = first.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cond->has_flag(IS_SIMPLE_COND) ||
               cond->has_flag(IS_RANGE_COND) ||
               T_OP_IS == cond->get_expr_type()) {
      ObSEArray<ObRawExpr*, 2> column_exprs;
      ObColumnRefRawExpr *col_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(cond, column_exprs))) {
        LOG_WARN("failed to extrace column exprs", K(ret));
      } else if (1 != column_exprs.count()) {
        //do nothing
      } else if (OB_ISNULL(column_exprs.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!column_exprs.at(0)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect column ref expr", K(*column_exprs.at(0)), K(ret));
      } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(0)))) {
      } else if (OB_ISNULL(first.get_table_item_by_id(col_expr->get_table_id()))) {
        //do nothing
      } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                          &first,
                                                          col_expr,
                                                          index_match))) {
        LOG_WARN("failed to check is match index", K(ret));
      } else if (index_match) {
        int64_t cond_pos_in_other = map_info.cond_map_.at(i);
        if (OB_INVALID_ID == cond_pos_in_other) {
          // Index conds of first stmt not in second stmt
          is_match = false;
        } else if (OB_FAIL(map_index_conds.add_member(cond_pos_in_other))) {
          LOG_WARN("failed to add member", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_match && i < second.get_condition_size(); ++i) {
    ObRawExpr *cond = NULL;
    bool index_match = false;
    if (OB_ISNULL(cond = second.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cond->has_flag(IS_SIMPLE_COND) ||
               cond->has_flag(IS_RANGE_COND) ||
               T_OP_IS == cond->get_expr_type()) {
      ObSEArray<ObRawExpr*, 2> column_exprs;
      ObColumnRefRawExpr *col_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(cond, column_exprs))) {
        LOG_WARN("failed to extrace column exprs", K(ret));
      } else if (1 != column_exprs.count()) {
        //do nothing
      } else if (OB_ISNULL(column_exprs.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!column_exprs.at(0)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect column ref expr", K(*column_exprs.at(0)), K(ret));
      } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(0)))) {
      } else if (OB_ISNULL(second.get_table_item_by_id(col_expr->get_table_id()))) {
        //do nothing
      } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                          &second,
                                                          col_expr,
                                                          index_match))) {
        LOG_WARN("failed to check is match index", K(ret));
      } else if (index_match && !map_index_conds.has_member(i)) {
        is_match = false;
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformTempTable::get_non_correlated_subquery
 * @param stmt
 * @param non_correlated_stmts
 * @return
 */
int ObTransformTempTable::get_non_correlated_subquery(ObDMLStmt *stmt, ObIArray<ObSelectStmt *> &non_correlated_stmts)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<uint64_t, uint64_t> param_level;
  uint64_t min_param_level = 0;
  ObArray<TempTableInfo> temp_table_infos;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(param_level.create(128, "TempTable"))) {
    LOG_WARN("failed to init expr map", K(ret));
  } else if (OB_FAIL(get_non_correlated_subquery(stmt, 0, param_level, non_correlated_stmts, min_param_level))) {
    LOG_WARN("failed to get non correlated subquery", K(ret));
  } else if (OB_FAIL(stmt->collect_temp_table_infos(temp_table_infos))) {
    LOG_WARN("failed to collect temp table infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); i ++) {
    min_param_level = 0;
    if (OB_FAIL(param_level.reuse())) {
      LOG_WARN("failed to reuse hash map", K(ret));
    } else if (OB_FAIL(get_non_correlated_subquery(temp_table_infos.at(i).temp_table_query_,
                                                   0,
                                                   param_level,
                                                   non_correlated_stmts,
                                                   min_param_level))) {
      LOG_WARN("failed to get non correlated subquery", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(param_level.destroy())) {
    LOG_WARN("failed to destroy map", K(ret));
  }
  return ret;
}

int ObTransformTempTable::get_non_correlated_subquery(ObDMLStmt *stmt,
                                                      const uint64_t recursive_level,
                                                      hash::ObHashMap<uint64_t, uint64_t> &param_level,
                                                      ObIArray<ObSelectStmt *> &non_correlated_stmts,
                                                      uint64_t &min_param_level)
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt *> child_stmts;
  ObArray<ObRawExpr *> relation_exprs;
  min_param_level = recursive_level;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    ObRawExpr *expr = relation_exprs.at(i);
    if (OB_FAIL(check_exec_param_level(expr, param_level, min_param_level))) {
      LOG_WARN("failed to check exec param level", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
    TableItem *table_item = stmt->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (!table_item->is_lateral_table()) {
      // do nothing
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < table_item->exec_params_.count(); ++j) {
        ObRawExpr *exec_param = table_item->exec_params_.at(j);
        uint64_t key = reinterpret_cast<uint64_t>(exec_param);
        if (OB_ISNULL(exec_param)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("exec param is null", K(ret));
        } else if (OB_FAIL(param_level.set_refactored(key, recursive_level))) {
          if (ret == OB_HASH_EXIST) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to add exec param into map", K(ret));
          }
        }
      }
    }

  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_subquery_expr_size(); ++i) {
    ObQueryRefRawExpr *query_ref = stmt->get_subquery_exprs().at(i);
    if (OB_ISNULL(query_ref)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ref is null", K(ret), K(query_ref));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < query_ref->get_exec_params().count(); ++j) {
      ObRawExpr *exec_param = query_ref->get_exec_params().at(j);
      uint64_t key = reinterpret_cast<uint64_t>(exec_param);
      if (OB_ISNULL(exec_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec param is null", K(ret));
      } else if (OB_FAIL(param_level.set_refactored(key, recursive_level))) {
        if (ret == OB_HASH_EXIST) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to add exec param into map", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    uint64_t child_min_param_level = recursive_level + 1;
    if (OB_FAIL(SMART_CALL(get_non_correlated_subquery(child_stmts.at(i),
                                                       recursive_level + 1,
                                                       param_level,
                                                       non_correlated_stmts,
                                                       child_min_param_level)))) {
      LOG_WARN("failed to get non correlated subquery", K(ret));
    } else if (child_min_param_level < min_param_level) {
      min_param_level = child_min_param_level;
    }
  }
  if (OB_SUCC(ret) && min_param_level == recursive_level && stmt->is_select_stmt()) {
    if (OB_FAIL(non_correlated_stmts.push_back(static_cast<ObSelectStmt *>(stmt)))) {
      LOG_WARN("failed to push back non correlated stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::check_exec_param_level(const ObRawExpr *expr,
                                                 const hash::ObHashMap<uint64_t, uint64_t> &param_level,
                                                 uint64_t &min_param_level)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_exec_param_expr()) {
    uint64_t key = reinterpret_cast<uint64_t>(expr);
    uint64_t level = UINT64_MAX;
    if (OB_FAIL(param_level.get_refactored(key, level))) {
      LOG_WARN("failed to get level", K(ret), K(*expr));
    } else if (level < min_param_level) {
      min_param_level = level;
    }
  } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_exec_param_level(expr->get_param_expr(i),
                                                    param_level,
                                                    min_param_level)))) {
        LOG_WARN("failed to check exec param level", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformTempTable::remove_simple_stmts(ObIArray<ObSelectStmt*> &stmts)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 8> new_stmts;
  bool has_rownum = false;
  bool is_valid = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); ++i) {
    ObSelectStmt *subquery = stmts.at(i);
    bool force_inline = false;
    bool force_materia = false;
    if (OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(check_hint_allowed_trans(*subquery,
                                                force_inline,
                                                force_materia))) {
      LOG_WARN("failed to check force materialize", K(ret));
    } else if (force_inline) {
      // do nothing
    } else if (OB_FAIL(subquery->has_rownum(has_rownum))) {
      LOG_WARN("failed to check has rownum", K(ret));
    } else if (has_rownum) {
      //do nothing
    } else if (OB_FAIL(check_stmt_can_materialize(subquery, false, is_valid))) {
      LOG_WARN("failed to check stmt is valid", K(ret));
    } else if (!is_valid) {
      //do nothing
    } else if (OB_FAIL(new_stmts.push_back(subquery))) {
      LOG_WARN("failed to push back stmt", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmts.assign(new_stmts))) {
      LOG_WARN("failed to assign stmts", K(ret));
    }
  }
  return ret;
}

/**
 * @classify_stmts
 * 为了降低stmt比较的代价，
 * 把stmt按照table size、generate table size分组，
 * 每组stmt的basic table item size和generate table size相同
 * 因为不同table item的stmt之间一定不相似
 */
int ObTransformTempTable::classify_stmts(ObIArray<ObSelectStmt*> &stmts,
                                        ObIArray<StmtClassifyHelper> &stmt_groups)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); ++i) {
    ObSelectStmt *stmt = stmts.at(i);
    int64_t table_size = 0;
    int64_t generate_table_size = 0;
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else {
      table_size = stmt->get_table_size();
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < table_size; ++j) {
      const TableItem *table_item = stmt->get_table_item(j);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_item is null", K(j));
      } else if (table_item->is_generated_table()) {
        ++generate_table_size;
      }
    }
    bool find = false;
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < stmt_groups.count(); ++j) {
      if (stmt_groups.at(j).table_size_ == table_size &&
          stmt_groups.at(j).generate_table_size_ == generate_table_size) {
        if (OB_FAIL(stmt_groups.at(j).stmts_.push_back(stmt))) {
          LOG_WARN("failed to push back stmt", K(ret));
        } else {
          find = true;
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      StmtClassifyHelper helper;
      helper.table_size_ = table_size;
      helper.generate_table_size_ = generate_table_size;
      if (OB_FAIL(helper.stmts_.push_back(stmt))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else if (OB_FAIL(stmt_groups.push_back(helper))) {
        LOG_WARN("failed to push back stmt", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @create_temp_table
 * 把相似stmt的公共部分抽离成temp table
 */

int ObTransformTempTable::create_temp_table(ObDMLStmt &root_stmt,
                                            StmtCompareHelper& compare_info,
                                            hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObStmtMapInfo common_map_info;
  TableItem *table = NULL;
  TableItem *temp_table = NULL;
  ObSelectStmt *temp_table_query = NULL;
  ObSEArray<ObSelectStmt *, 2> origin_stmts;
  ObSEArray<ObSelectStmt *, 2> trans_stmts;
  ObSEArray<int64_t, 2> compare_info_map;
  ObTryTransHelper try_trans_helper;
  if (OB_ISNULL(compare_info.stmt_) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)
      || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(try_trans_helper.fill_helper(root_stmt.get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else if (OB_FAIL(compute_common_map_info(compare_info.stmt_map_infos_, common_map_info))) {
    LOG_WARN("failed to compute common map info", K(ret));
  } else if (compare_info.stmt_map_infos_.count() != compare_info.similar_stmts_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect compare info", K(compare_info), K(ret));
  }
  // check whether the stmt is valid
  bool is_valid = true;
  // int valid_stmt_cnt = compare_info.similar_stmts_.count();
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < compare_info.similar_stmts_.count(); ++i) {
    ObSelectStmt *similar_stmt = compare_info.similar_stmts_.at(i);
    ObDMLStmt *current_stmt = NULL;
    if (OB_ISNULL(similar_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(root_stmt.get_stmt_by_stmt_id(similar_stmt->get_stmt_id(), current_stmt))) {
      LOG_WARN("failed to get stmt by stmt id", K(ret), K(similar_stmt->get_stmt_id()), KPC(similar_stmt));
    } else if (similar_stmt != current_stmt) {
      // similar stmt might has been rewrite,
      // and do not exists in root stmt
      // e.g.
      //    select * from
      //      (select * from t1,t2 where a=b union select * from t1,t2 where c=d) A,
      //      (select * from t1,t2 where a=b union select * from t1,t2 where c=d) B
      // => with cte1 as (select * from t1,t2 where a=b union select * from t1,t2 where c=d)
      //       select * from cte1 A, cte1 B;
      //   Stmt (select * from t1,t2 where a=b) is different after extract cte1
      is_valid = false;
      OPT_TRACE("Similar stmts are invalid");
    } else if (!compare_info.hint_force_stmt_set_.empty() &&
               !compare_info.hint_force_stmt_set_.has_qb_name(similar_stmt)) {
      // not in hint, do not transform
    } else if (OB_FAIL(origin_stmts.push_back(similar_stmt))) {
      LOG_WARN("failed to push back stmt", K(ret));
    } else if (OB_FAIL(compare_info_map.push_back(i))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (is_valid && compare_info.hint_force_stmt_set_.empty() && origin_stmts.count() <= 1) {
    is_valid = false;
    OPT_TRACE("Only one valid stmt, do not transform");
  }
  //把stmt的公共部分封装成generate table
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < origin_stmts.count(); ++i) {
    ObSelectStmt *similar_stmt = origin_stmts.at(i);
    ObDMLStmt *dml_stmt = NULL;
    if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                 similar_stmt, dml_stmt))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else if (FALSE_IT(similar_stmt = static_cast<ObSelectStmt *>(dml_stmt))) {
    } else if (OB_FAIL(trans_stmts.push_back(similar_stmt))) {
      LOG_WARN("failed to push back stmt", K(ret));
    } else if (OB_FAIL(inner_create_temp_table(similar_stmt,
                                        compare_info.stmt_map_infos_.at(compare_info_map.at(i)),
                                        common_map_info))) {
      LOG_WARN("failed to replace temp table", K(ret));
    }
  }
  //把generate table转成temp table
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < trans_stmts.count(); ++i) {
    ObSelectStmt *stmt = trans_stmts.at(i);
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (1 != stmt->get_table_size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect one table item in stmt", KPC(stmt), K(ret));
    } else if (OB_ISNULL(table = stmt->get_table_item(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_generated_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect generate table item", KPC(table), K(ret));
    } else if (OB_ISNULL(table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", K(ret));
    } else {
      if (0 == i) {
        temp_table = table;
        ObDMLStmt *temp_table_stmt = NULL;
        if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                     table->ref_query_, temp_table_stmt))) {
          LOG_WARN("failed to deep copy stmt", K(ret));
        } else if (OB_FAIL(temp_table_stmt->update_stmt_table_id(ctx_->allocator_, *table->ref_query_))) {
          LOG_WARN("failed to update table id", K(ret));
        } else if (OB_FAIL(stmt->generate_view_name(*ctx_->allocator_,
                                            temp_table->table_name_,
                                            true))) {
          LOG_WARN("failed to generate view name", K(ret));
        } else {
          temp_table_query = static_cast<ObSelectStmt *>(temp_table_stmt);
          table->ref_query_ = temp_table_query;
        }
      } else if (OB_FAIL(apply_temp_table(stmt,
                                          table,
                                          temp_table_query,
                                          compare_info.stmt_map_infos_.at(compare_info_map.at(i))))) {
        LOG_WARN("failed to apply temp table", K(ret));
      } else {
        table->ref_query_ = temp_table_query;
        table->table_name_ = temp_table->table_name_;
      }
      table->type_ = TableItem::TEMP_TABLE;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < trans_stmts.count(); ++i) {
    ObSelectStmt *stmt = trans_stmts.at(i);
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt_expr_reference(ctx_->expr_factory_,
                                                           ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt reference", K(ret));
    }
  }
  
  if (OB_SUCC(ret) && is_valid && OB_NOT_NULL(temp_table_query)) {
    if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*temp_table_query))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else if (OB_FAIL(temp_table_query->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(temp_table_query->formalize_stmt_expr_reference(ctx_->expr_factory_,
                                                                       ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt reference", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_valid) {
    trans_happened = false;
    common::ObSEArray<ObSelectStmt *, 2> accept_stmts;
    ObString temp_query_name;
    bool use_new_accept_func = root_stmt.get_query_ctx()->check_opt_compat_version(
                          COMPAT_VERSION_4_2_5, COMPAT_VERSION_4_3_0, COMPAT_VERSION_4_3_5);
    if (!use_new_accept_func &&
        OB_FAIL(accept_cte_transform(root_stmt, temp_table,
                                    origin_stmts, trans_stmts,
                                    accept_stmts, parent_map,
                                    !compare_info.hint_force_stmt_set_.empty(),
                                    trans_happened))) {
      LOG_WARN("failed to accept transform", K(ret));
    } else if (use_new_accept_func &&
               OB_FAIL(accept_cte_transform_v2(root_stmt, temp_table,
                                              origin_stmts, trans_stmts,
                                              accept_stmts, parent_map,
                                              !compare_info.hint_force_stmt_set_.empty(),
                                              trans_happened))) {
      LOG_WARN("failed to accept transform", K(ret));
    } else if (OB_FAIL(try_trans_helper.finish(trans_happened, root_stmt.get_query_ctx(), ctx_))) {
      LOG_WARN("failed to finish try_trans_helper", K(ret));
    } else if (!trans_happened) {
    } else if (OB_FAIL(append(ctx_->equal_param_constraints_, common_map_info.equal_param_map_))) {
      LOG_WARN("failed to append equal param constraints", K(ret));
    } else if (OB_FAIL(add_materialize_stmts(accept_stmts))) {
      LOG_WARN("failed to add stmts", K(ret));
    } else if (OB_FAIL(temp_table_query->get_qb_name(temp_query_name))) {
      LOG_WARN("failed to get qb name", K(ret));
    } else if (OB_FAIL(ctx_->inline_blacklist_.push_back(temp_query_name))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < compare_info.stmt_map_infos_.count(); i ++) {
        if (OB_FAIL(append(ctx_->equal_param_constraints_,
                           compare_info.stmt_map_infos_.at(i).equal_param_map_))) {
          LOG_WARN("failed to append equal param constraints", K(ret));
        }
      }
      LOG_TRACE("succeed to create temp table", KPC(temp_table_query));
    }
  }

  return ret;
}

/**
 * @brief compute_common_map_info
 * 计算相似stmt的最大公共部分
 */
int ObTransformTempTable::compute_common_map_info(ObIArray<ObStmtMapInfo>& map_infos,
                                                  ObStmtMapInfo &common_map_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < map_infos.count(); ++i) {
    ObStmtMapInfo &map_info = map_infos.at(i);
    if (0 == i) {
      if (OB_FAIL(common_map_info.assign(map_info))) {
        LOG_WARN("failed to assign map info", K(ret));
      }
    } else if (OB_FAIL(append(common_map_info.equal_param_map_, map_info.equal_param_map_))) {
      LOG_WARN("failed to append equal param", K(ret));
    } else {
      //compute common condi map
      if (OB_FAIL(compute_common_map(map_info.cond_map_, common_map_info.cond_map_))) {
        LOG_WARN("failed to compute common map info", K(ret));
      } else {
        common_map_info.is_cond_equal_ &= map_info.is_cond_equal_;
      }
      //compute common group by map
      if (OB_SUCC(ret)) {
        //只有当where condition完全相同时才能考虑下压group by到temp table
        //TODO:当前不相同的condition可以推迟到having执行时也可以下压group by
        if (common_map_info.is_cond_equal_) {
          if (OB_FAIL(compute_common_map(map_info.group_map_, common_map_info.group_map_))) {
            LOG_WARN("failed to compute common map info", K(ret));
          } else {
            common_map_info.is_group_equal_ &= map_info.is_group_equal_;
          }
        } else {
          common_map_info.group_map_.reset();
          common_map_info.having_map_.reset();
          common_map_info.select_item_map_.reset();
          common_map_info.is_distinct_equal_ = false;
        }
      }
      //compute common having map
      if (OB_SUCC(ret)) {
        if (common_map_info.is_group_equal_) {
          if (OB_FAIL(compute_common_map(map_info.having_map_, common_map_info.having_map_))) {
            LOG_WARN("failed to compute common map info", K(ret));
          } else {
            common_map_info.is_having_equal_ &= map_info.is_having_equal_;
          }
        } else {
          common_map_info.having_map_.reset();
          common_map_info.select_item_map_.reset();
          common_map_info.is_distinct_equal_ = false;
        }
      }
      //compute common select item map
      if (OB_SUCC(ret)) {
        if (common_map_info.is_having_equal_) {
          if (OB_FAIL(compute_common_map(map_info.select_item_map_, common_map_info.select_item_map_))) {
            LOG_WARN("failed to compute common map info", K(ret));
          } else {
            common_map_info.is_select_item_equal_ &= map_info.is_select_item_equal_;
          }
        } else {
          common_map_info.select_item_map_.reset();
          common_map_info.is_distinct_equal_ = false;
        }
      }
      //compute common distinct map
      if (OB_SUCC(ret)) {
        if (common_map_info.is_select_item_equal_) {
          common_map_info.is_distinct_equal_ = map_info.is_distinct_equal_;
        }
      }
    }
  }
  return ret;
}

int ObTransformTempTable::compute_common_map(ObIArray<int64_t> &source_map,
                                             ObIArray<int64_t> &common_map)
{
  int ret = OB_SUCCESS;
  if (source_map.count() != common_map.count()) {
    common_map.reset();
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < source_map.count(); ++i) {
      if (OB_INVALID_ID == source_map.at(i)) {
        common_map.at(i) = OB_INVALID_ID;
      }
    }
  }
  return ret;
}

/**
 * @brief inner_create_temp_table
 * 把stmt的公共部分封装在generate table内
 */
int ObTransformTempTable::inner_create_temp_table(ObSelectStmt *parent_stmt,
                                                  ObStmtMapInfo& map_info,
                                                  ObStmtMapInfo& common_map_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (parent_stmt->is_set_stmt()) {
    if (OB_FAIL(ObTransformUtils::pack_stmt(ctx_, parent_stmt))) {
      LOG_WARN("failed to create temp table for set stmt", K(ret));
    } else {
      LOG_TRACE("succeed to create temp table", KPC(parent_stmt));
    }
  } else {
    TableItem *view_table = NULL;
    ObSEArray<TableItem *, 8> from_tables;
    ObSEArray<SemiInfo *, 4> semi_infos;
    ObSEArray<ObRawExpr *, 8> pushdown_select;
    ObSEArray<ObRawExpr *, 8> pushdown_where;
    ObSEArray<ObRawExpr *, 8> pushdown_groupby;
    ObSEArray<ObRawExpr *, 8> pushdown_rollup;
    ObSEArray<ObRawExpr *, 8> pushdown_having;

    if (parent_stmt->get_condition_size() > 0 &&
              OB_FAIL(pushdown_conditions(parent_stmt,
                                          map_info.cond_map_,
                                          common_map_info.cond_map_,
                                          pushdown_where))) {
      LOG_WARN("failed to pushdown conditions", K(ret));
    } else if (!common_map_info.is_cond_equal_ ||
              !common_map_info.is_group_equal_) {
      //do nothing
      //下压group by
    } else if (parent_stmt->has_group_by() &&
              OB_FAIL(ObTransformUtils::pushdown_group_by(parent_stmt,
                                                          pushdown_groupby,
                                                          pushdown_rollup,
                                                          pushdown_select))) {
      LOG_WARN("failed to pushdown group by", K(ret));
      //下压having
    } else if (parent_stmt->get_having_expr_size() > 0 &&
              OB_FAIL(pushdown_having_conditions(parent_stmt,
                                                  map_info.having_map_,
                                                  common_map_info.having_map_,
                                                  pushdown_having))) {
      LOG_WARN("failed to pushdown having conditions", K(ret));
    }

    ObSEArray<TableItem *, 8> origin_tables;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransformUtils::pushdown_pseudo_column_like_exprs(*parent_stmt,
                                                                           false,
                                                                           pushdown_select))) {
      LOG_WARN("failed to pushdown pseudo column like exprs", K(ret));
    } else if (OB_FAIL(origin_tables.assign(parent_stmt->get_table_items()))) {
      LOG_WARN("failed to get table items", K(ret));
    } else if (OB_FAIL(parent_stmt->get_from_tables(from_tables))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(semi_infos.assign(parent_stmt->get_semi_infos()))) {
      LOG_WARN("failed to assign semi info", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                                 parent_stmt,
                                                                 view_table,
                                                                 from_tables,
                                                                 &semi_infos))) {
      LOG_WARN("failed to create empty view", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                            parent_stmt,
                                                            view_table,
                                                            from_tables,
                                                            &pushdown_where,
                                                            &semi_infos,
                                                            &pushdown_select,
                                                            &pushdown_groupby,
                                                            &pushdown_rollup,
                                                            &pushdown_having))) {
      LOG_WARN("failed to create inline view", K(ret));
    } else if (OB_ISNULL(view_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null view query", K(ret));

    // recover the order of table items,
    // the table_map in ObStmtMapInfo will be used in apply_temp_table
    } else if (OB_FAIL(view_table->ref_query_->get_table_items().assign(origin_tables))) {
      LOG_WARN("failed to adjust table map", K(ret));
    } else if (OB_FAIL(view_table->ref_query_->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(view_table->ref_query_->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item by id", K(ret));
    } else if (OB_FAIL(view_table->ref_query_->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

/**
 * @brief pushdown_conditions
 * 把公共的where condition重命名后下压到视图内
 * 不同的where condition保留在原stmt中，等待谓词推导下压
 */
int ObTransformTempTable::pushdown_conditions(ObSelectStmt *parent_stmt,
                                              const ObIArray<int64_t> &cond_map,
                                              const ObIArray<int64_t> &common_cond_map,
                                              ObIArray<ObRawExpr*> &pushdown_conds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> keep_conds;
  if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (cond_map.count() != common_cond_map.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect map info", K(cond_map), K(common_cond_map), K(ret));
  } else {
    ObIArray<ObRawExpr*> &conditions = parent_stmt->get_condition_exprs();
    //找到相同的condition
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_map.count(); ++i) {
      int64_t idx = cond_map.at(i);
      if (OB_INVALID_ID == common_cond_map.at(i)) {
        //do nothing
      } else if (idx < 0 || idx > conditions.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect cond index", K(idx), K(ret));
      } else if (OB_FAIL(pushdown_conds.push_back(conditions.at(idx)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    //找到不同的condition
    for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
      if (ObOptimizerUtil::find_item(pushdown_conds, conditions.at(i))) {
        //do nothing
      } else if (OB_FAIL(keep_conds.push_back(conditions.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && !pushdown_conds.empty()) {
      if (OB_FAIL(parent_stmt->get_condition_exprs().assign(keep_conds))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief pushdown_having_conditions
 * 下推相同的having condition到视图中
 */
int ObTransformTempTable::pushdown_having_conditions(ObSelectStmt *parent_stmt,
                                                    const ObIArray<int64_t> &having_map,
                                                    const ObIArray<int64_t> &common_having_map,
                                                    ObIArray<ObRawExpr*> &pushdown_conds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> keep_conds;

  if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (having_map.count() != common_having_map.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect map info", K(having_map), K(common_having_map), K(ret));
  } else {
    ObIArray<ObRawExpr*> &conditions = parent_stmt->get_having_exprs();
    //找到相同的having condition
    for (int64_t i = 0; OB_SUCC(ret) && i < having_map.count(); ++i) {
      int64_t idx = having_map.at(i);
      if (OB_INVALID_ID == common_having_map.at(i)) {
        //do nothing
      } else if (idx < 0 || idx > conditions.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect cond index", K(idx), K(ret));
      } else if (OB_FAIL(pushdown_conds.push_back(conditions.at(idx)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    //找到不同的having condition
    for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
      if (ObOptimizerUtil::find_item(pushdown_conds, conditions.at(i))) {
        //do nothing
      } else if (OB_FAIL(keep_conds.push_back(conditions.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && !conditions.empty()) {
      parent_stmt->get_having_exprs().reset();
      if (OB_FAIL(append(parent_stmt->get_condition_exprs(), keep_conds))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief apply_temp_table
 * 把视图view替换成temp table query，
 * 已知条件：view与temp table query仅仅只是select item不同
 * view与temp table query的基表映射关系存在于map info中
 * 只需要把view中与temp table query不同的select item转换成temp table的select item
 * 并且更新parent stmt的column item，引用temp table 的select item
 * 如果有聚合函数，需要添加到temp table query中
 */
int ObTransformTempTable::apply_temp_table(ObSelectStmt *parent_stmt,
                                           TableItem *view_table,
                                           ObSelectStmt *temp_table_query,
                                           ObStmtMapInfo& map_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> old_view_columns;
  ObSEArray<ObRawExpr*, 4> new_temp_columns;
  ObSelectStmt *view = NULL;
  SMART_VAR(ObStmtCompareContext, context) {
    if (OB_ISNULL(parent_stmt) || OB_ISNULL(parent_stmt->get_query_ctx()) ||
        OB_ISNULL(temp_table_query) || OB_ISNULL(view_table) ||
        OB_UNLIKELY(!view_table->is_generated_table()) ||
        OB_ISNULL(view = view_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(ret), KP(parent_stmt), KP(temp_table_query), KP(view_table));
    } else if (OB_FAIL(context.init(temp_table_query, view, map_info,
                                    &parent_stmt->get_query_ctx()->calculable_items_))) {
      LOG_WARN("failed to init context", K(ret));
    } else if (OB_FAIL(apply_temp_table_columns(context, map_info, temp_table_query, view,
                                                old_view_columns, new_temp_columns))) {
      LOG_WARN("failed to apply temp table columns", K(ret));
    } else if (OB_FAIL(apply_temp_table_select_list(context, map_info, parent_stmt,
                                                    temp_table_query, view, view_table,
                                                    old_view_columns, new_temp_columns))) {
      LOG_WARN("failed to apply temp table select list", K(ret));
    }
  } // end smart var
  return ret;
}

/*
build a mapping relations between columns of view and columns of temp_table_query.
But if there is no corresponding columns in temp_table_query, then add a new ObColumnItem into temp_table_query
*/
int ObTransformTempTable::apply_temp_table_columns(ObStmtCompareContext &context,
                                                   const ObStmtMapInfo& map_info,
                                                   ObSelectStmt *temp_table_query,
                                                   ObSelectStmt *view,
                                                   ObIArray<ObRawExpr *> &view_column_list,
                                                   ObIArray<ObRawExpr *> &new_column_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> temp_table_column_list;
  if (OB_ISNULL(view) || OB_ISNULL(temp_table_query) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(view->get_column_exprs(view_column_list))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(temp_table_query->get_column_exprs(temp_table_column_list))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < view_column_list.count(); ++i) {
      ObRawExpr *view_column = view_column_list.at(i);
      bool find = false;
      if (OB_ISNULL(view_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null column expr", K(ret));
      }
      //column item是否存在于temp table中
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < temp_table_column_list.count(); ++j) {
        ObRawExpr *temp_table_column = temp_table_column_list.at(j);
        if (OB_ISNULL(temp_table_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null column expr", K(ret));
        } else if (!temp_table_column->same_as(*view_column, &context)) {
          //do nothing
        } else if (OB_FAIL(new_column_list.push_back(temp_table_column))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          find = true;
        }
      }
      //不存在于temp table中的column需要添加到temp table中
      if (OB_SUCC(ret) && !find) {
        TableItem *table = NULL;
        ColumnItem *column_item = NULL;
        ObColumnRefRawExpr *col_ref = static_cast<ObColumnRefRawExpr*>(view_column);
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        if (OB_ISNULL(column_item = view->get_column_item_by_id(col_ref->get_table_id(),
                                                                col_ref->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null column item", K(ret));
        } else if (OB_FAIL(ObStmtComparer::get_map_column(map_info, view, temp_table_query,
                                                          col_ref->get_table_id(),
                                                          col_ref->get_column_id(),
                                                          true,
                                                          table_id, column_id))) {
          LOG_WARN("failed to get map column", K(ret));
        } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) ||
                   OB_UNLIKELY(OB_INVALID_ID == column_id) ||
                   OB_ISNULL(table = temp_table_query->get_table_item_by_id(table_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null column id", K(ret));
        } else if (table->is_generated_table() && OB_ISNULL(table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null column id", K(ret));
        } else {
          col_ref->set_ref_id(table_id, column_id);
          col_ref->set_table_name(table->get_table_name());
          if (table->is_generated_table()) {
            const SelectItem &select_item = table->ref_query_->get_select_item(column_id - OB_APP_MIN_COLUMN_ID);
            col_ref->set_column_name(select_item.alias_name_);
          }
          column_item->table_id_ = table_id;
          column_item->column_id_ = column_id;
          if (OB_FAIL(temp_table_query->add_column_item(*column_item))) {
            LOG_WARN("failed to add column item", K(ret));
          } else if (OB_FAIL(new_column_list.push_back(col_ref))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformTempTable::apply_temp_table_select_list(ObStmtCompareContext &context,
                                                       const ObStmtMapInfo& map_info,
                                                       ObSelectStmt *parent_stmt,
                                                       ObSelectStmt *temp_table_query,
                                                       ObSelectStmt *view,
                                                       TableItem *view_table,
                                                       ObIArray<ObRawExpr *> &old_view_columns,
                                                       ObIArray<ObRawExpr *> &new_temp_columns)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> view_select_list;
  ObSEArray<ObRawExpr*, 4> temp_table_select_list;
  ObSEArray<ObRawExpr*, 4> new_select_list;
  ObSEArray<ObRawExpr*, 4> old_column_exprs;  // used in parent stmt
  ObSEArray<ObRawExpr*, 4> new_column_exprs;  // used in parent stmt
  ObSEArray<ObAggFunRawExpr*, 2> aggr_items;
  ObSEArray<ObWinFunRawExpr*, 2> win_func_exprs;
  if (OB_ISNULL(view) || OB_ISNULL(temp_table_query) || OB_ISNULL(parent_stmt) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(view->get_select_exprs(view_select_list))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(temp_table_query->get_select_exprs(temp_table_select_list))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else {
    // use select_expr in temp_table_query directly
    for (int64_t i = 0; OB_SUCC(ret) && i < view_select_list.count(); ++i) {
      ObRawExpr *view_select = view_select_list.at(i);
      ObColumnRefRawExpr *col_expr = NULL;
      bool find = false;
      if (OB_ISNULL(view_select)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null select expr", K(ret));
      } else if (NULL == (col_expr = parent_stmt->get_column_expr_by_id(view_table->table_id_,
                                                                        i + OB_APP_MIN_COLUMN_ID))) {
        // unused select item, skip following procedure
        find = true;
      } else if (OB_FAIL(old_column_exprs.push_back(col_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
      //select item是否存在于temp table中
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < temp_table_select_list.count(); ++j) {
        ObRawExpr *temp_table_select = temp_table_select_list.at(j);
        if (OB_ISNULL(temp_table_select)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null select expr", K(ret));
        } else if (!temp_table_select->same_as(*view_select, &context)) {
          //do nothing
        } else if (OB_FAIL(new_select_list.push_back(temp_table_select))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          find = true;
        }
      }
      //不存在于temp table中的select expr需要转换成temp table的select item
      if (OB_SUCC(ret) && !find) {
        aggr_items.reset();
        win_func_exprs.reset();
        if (OB_FAIL(ObTransformUtils::replace_expr(old_view_columns, new_temp_columns, view_select))) {
          LOG_WARN("failed to replace expr", K(ret));
        } else if (OB_FAIL(new_select_list.push_back(view_select))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::extract_aggr_expr(view_select, aggr_items))) {
          LOG_WARN("failed to extract aggr expr", K(ret));
        } else if (OB_FAIL(append(temp_table_query->get_aggr_items(), aggr_items))) {
          LOG_WARN("failed to append aggr items", K(ret));
        } else if (OB_FAIL(ObTransformUtils::extract_winfun_expr(view_select, win_func_exprs))) {
          LOG_WARN("failed to extract win func exprs", K(ret));
        } else if (OB_FAIL(append(temp_table_query->get_window_func_exprs(), win_func_exprs))) {
          LOG_WARN("failed to append win func exprs", K(ret));
        }
      }
    }
  }
  // Create a new select item for the temp table and replace the reference to the parent stmt
  if (OB_SUCC(ret)) {
    view_table->ref_query_ = temp_table_query;
    parent_stmt->clear_column_items();
    if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *view_table, parent_stmt,
                                                          new_select_list,
                                                          new_column_exprs))) {
      LOG_WARN("failed to create column for view", K(ret));
    } else if (OB_FAIL(parent_stmt->replace_relation_exprs(old_column_exprs, new_column_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(temp_table_query->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::project_pruning(ObIArray<TempTableInfo> &temp_table_infos,
                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSqlBitSet<> removed_idx;
  bool is_valid = false;
  if (OB_ISNULL(trans_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null trans param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); i++) {
    removed_idx.reuse();
    TempTableInfo &info = temp_table_infos.at(i);
    trans_param_->trans_stmt_ = info.temp_table_query_;
    trans_param_->trans_type_ = T_PROJECT_PRUNE;
    OPT_TRACE("try to prune project for:",info.temp_table_query_);
    if (OB_ISNULL(info.temp_table_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(check_hint_allowed_trans(*info.temp_table_query_,
                                                T_PROJECT_PRUNE,
                                                is_valid))) {
      LOG_WARN("failed to check hint allowed prune", K(ret));
    } else if (!is_valid) {
      //do nothing
      OPT_TRACE("hint reject transform");
    } else if (OB_FAIL(ObTransformUtils::check_project_pruning_validity(*info.temp_table_query_,
                                                                        is_valid))) {
      LOG_WARN("failed to check project pruning valid", K(ret));
    } else if (!is_valid) {
      //do nothing
      OPT_TRACE("can not prune project");
    } else if (OB_FAIL(get_remove_select_item(info,
                                              removed_idx))) {
      LOG_WARN("failed to get remove select item", K(ret));
    } else if (removed_idx.is_empty()) {
      //do nothing
    } else if (OB_FAIL(remove_select_items(info, removed_idx))) {
      LOG_WARN("failed to rempve select item", K(ret));
    } else if (OB_FAIL(add_normal_temp_table_trans_hint(*info.temp_table_query_, T_PROJECT_PRUNE))) {
      LOG_WARN("failed to add transform hint", K(ret));
    } else if (OB_FAIL(info.temp_table_query_->formalize_stmt_expr_reference(ctx_->expr_factory_,
                                                                             ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt reference", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformTempTable::get_remove_select_item(TempTableInfo &info,
                                                 ObSqlBitSet<> &removed_idx)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> column_ids;
  if (OB_ISNULL(info.temp_table_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < info.table_items_.count(); ++i) {
    ObSqlBitSet<> table_column_ids;
    if (OB_ISNULL(info.table_items_.at(i)) ||
        OB_ISNULL(info.upper_stmts_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null info", K(ret));
    } else if (OB_FAIL(info.upper_stmts_.at(i)->get_column_ids(info.table_items_.at(i)->table_id_,
                                                               table_column_ids))) {
      LOG_WARN("failed to get column ids", K(ret));
    } else if (OB_FAIL(column_ids.add_members(table_column_ids))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < info.temp_table_query_->get_select_item_size(); i++) {
    bool need_remove = false;
    if (column_ids.has_member(i + OB_APP_MIN_COLUMN_ID)) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::check_select_item_need_remove(info.temp_table_query_,
                                                                       i,
                                                                       need_remove))) {
      LOG_WARN("fail to check column in set ordrt by", K(ret));
    } else if (need_remove) {
      ret = removed_idx.add_member(i);
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformTempTable::remove_select_items(TempTableInfo &info,
                                              ObSqlBitSet<> &removed_idxs)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ObSEArray<uint64_t, 16> new_column_ids;
  ObArray<SelectItem> new_select_items;
  ObSEArray<ColumnItem, 16> new_column_items;
  ObSelectStmt *child_stmt = info.temp_table_query_;
  if (OB_ISNULL(ctx_) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument invalid", K(ctx_), K(ret));
  } else if (OB_FAIL(new_column_ids.prepare_allocate(child_stmt->get_select_item_size()))) {
    LOG_WARN("failed to preallocate", K(ret));
  }
  //计算老的column id对应的新column id关系
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_select_item_size(); i++) {
    new_column_ids.at(i) =  OB_INVALID_ID;
    if (!removed_idxs.has_member(i) ) {
      if (OB_FAIL(new_select_items.push_back(child_stmt->get_select_item(i)))) {
        LOG_WARN("failed to push back select item", K(ret));
      } else {
        new_column_ids.at(i) = count + OB_APP_MIN_COLUMN_ID;
        count++;
      }
    }
  }

  //更新upper stmt的column item
  for (int64_t i = 0; OB_SUCC(ret) && i < info.table_items_.count(); ++i) {
    new_column_items.reuse();
    ObDMLStmt *upper_stmt = info.upper_stmts_.at(i);
    TableItem *table = info.table_items_.at(i);
    if (OB_ISNULL(upper_stmt) || OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(ret));
    } else if (OB_FAIL(upper_stmt->get_column_items(table->table_id_, new_column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < new_column_items.count(); ++j) {
      ColumnItem &column = new_column_items.at(j);
      uint64_t column_id = column.column_id_;
      if (column_id - OB_APP_MIN_COLUMN_ID >= new_column_ids.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect column id", K(column), K(ret));
      } else {
        column.set_ref_id(table->table_id_, new_column_ids.at(column_id - OB_APP_MIN_COLUMN_ID));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(upper_stmt->remove_column_item(table->table_id_))) {
      LOG_WARN("failed to remove column item", K(ret));
    } else if (OB_FAIL(upper_stmt->add_column_item(new_column_items))) {
      LOG_WARN("failed to add column item", K(ret));
    }
  }
  //消除child stmt的select item
  if (OB_SUCC(ret)) {
    if (child_stmt->is_set_stmt()) {
      if (OB_FAIL(ObTransformUtils::remove_select_items(ctx_,
                                                        *child_stmt,
                                                        removed_idxs))) {
        LOG_WARN("failed to remove select item", K(ret));
      }
    } else if (OB_FAIL(child_stmt->get_select_items().assign(new_select_items))) {
      LOG_WARN("failed to assign select item", K(ret));
    } else if (child_stmt->get_select_items().empty() &&
              OB_FAIL(ObTransformUtils::create_dummy_select_item(*child_stmt, ctx_))) {
      LOG_WARN("failed to create dummy select item", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

// add hint about temp table transform: expand temp table, project pruning, filter pushdown
int ObTransformTempTable::add_normal_temp_table_trans_hint(ObDMLStmt &stmt, ObItemType type)
{
  int ret = OB_SUCCESS;
  ObString qb_name;
  const ObQueryHint *query_hint = NULL;
  ObMaterializeHint *hint = NULL;
  ObItemType real_type = T_INLINE == type ? T_MATERIALIZE : type;
  const ObHint *used_hint = stmt.get_stmt_hint().get_normal_hint(real_type);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (OB_FAIL(stmt.get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret), K(stmt.get_stmt_id()));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, type, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (NULL != used_hint && OB_FAIL(ctx_->add_used_trans_hint(used_hint))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else if (OB_FAIL(ctx_->add_src_hash_val(qb_name))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else if (OB_FAIL(stmt.adjust_qb_name(ctx_->allocator_,
                                         ctx_->src_qb_name_,
                                         ctx_->src_hash_val_))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else {
    ctx_->src_hash_val_.pop_back();
    hint->set_qb_name(qb_name);
    if (query_hint->has_outline_data()) {
      ++ctx_->trans_list_loc_;
    }
  }
  return ret;
}

//  create and add T_MATERIALIZE hint
int ObTransformTempTable::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObMaterializeHint *hint = NULL;
  TempTableTransParam *params = static_cast<TempTableTransParam *>(trans_params);
  if (OB_ISNULL(params) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(params), K(ctx_));
  } else if (OB_UNLIKELY(T_MATERIALIZE != params->trans_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect transform type", K(ret), "type", get_type_name(params->trans_type_));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_MATERIALIZE, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(sort_materialize_stmts(params->materialize_stmts_))) {
    LOG_WARN("failed to sort stmts", K(ret));
  } else {
    Ob2DArray<MaterializeStmts *> &child_stmts = params->materialize_stmts_;
    ObSelectStmt* subquery = NULL;
    bool use_hint = false;
    const ObMaterializeHint *myhint = static_cast<const ObMaterializeHint*>(get_hint(stmt.get_stmt_hint()));
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      MaterializeStmts *subqueries = child_stmts.at(i);
      QbNameList qb_names;
      if (OB_ISNULL(subqueries)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmts", K(ret));
      }
      for (int j = 0; OB_SUCC(ret) && j < subqueries->count(); ++j) {
        ObString subquery_qb_name;
        ObSelectStmt *subquery = NULL;
        if (OB_ISNULL(subquery = subqueries->at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(subquery));
        } else if (OB_FAIL(subquery->get_qb_name(subquery_qb_name))) {
          LOG_WARN("failed to get qb name", K(ret), K(stmt.get_stmt_id()));
        } else if (OB_FAIL(qb_names.qb_names_.push_back(subquery_qb_name))) {
          LOG_WARN("failed to push back qb name", K(ret));
        } else if (OB_FAIL(ctx_->add_src_hash_val(subquery_qb_name))) {
          LOG_WARN("failed to add src hash val", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(hint->add_qb_name_list(qb_names))) {
        LOG_WARN("failed to add qb names", K(ret));
      } else if (NULL != myhint && myhint->enable_materialize_subquery(qb_names.qb_names_)) {
        use_hint = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
      LOG_WARN("failed to push back hint", K(ret));
    } else if (use_hint && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else {
      hint->set_qb_name(ctx_->src_qb_name_);
    }
  }
  return ret;
}

int ObTransformTempTable::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                          const int64_t current_level,
                                          const ObDMLStmt &stmt,
                                          bool &need_trans)
{
  int ret = OB_SUCCESS;
  need_trans = false;
  const ObQueryHint *query_hint = NULL;
  const ObHint *trans_hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!parent_stmts.empty() || current_level != 0 ||
             is_normal_disabled_transform(stmt)) {
    need_trans = false;
  } else if (!query_hint->has_outline_data()) {
    // TODO: sean.yyj make the priority of rule hint higher than cost based hint
    if (OB_FAIL(ObTransformUtils::is_cost_based_trans_enable(ctx_, query_hint->global_hint_,
                                                             need_trans))) {
      LOG_WARN("failed to check cost based transform enable", K(ret));
    }
  } else if (NULL == (trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_))) {
    /*do nothing*/
    OPT_TRACE("outline reject transform");
  } else {
    const ObItemType hint_type = trans_hint->get_hint_type();
    need_trans = T_MATERIALIZE == hint_type
                 || T_PUSH_PRED_CTE == hint_type
                 || T_PROJECT_PRUNE == hint_type;
  }
  return ret;
}

// check hint T_MATERIALIZE for expand temp table
int ObTransformTempTable::check_hint_allowed_trans(const ObSelectStmt &subquery,
                                                   bool &force_inline,
                                                   bool &force_materialize) const
{
  int ret = OB_SUCCESS;
  force_inline = false;
  force_materialize = false;
  const ObQueryHint *query_hint = NULL;
  const ObMaterializeHint *myhint = static_cast<const ObMaterializeHint*>(get_hint(subquery.get_stmt_hint()));
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(query_hint = subquery.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    if (NULL == myhint) {
      /* do nothing */
    } else if (OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else {
      force_inline = myhint->enable_inline();
      force_materialize = myhint->enable_materialize();
    }
  } else {
    force_inline = NULL != myhint && myhint->enable_inline()
                   && query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, myhint);
    force_materialize = !force_inline;
  }
  return ret;
}

int ObTransformTempTable::get_hint_force_set(const ObDMLStmt &stmt,
                                             const ObSelectStmt &subquery,
                                             QbNameList &qb_names,
                                             bool &hint_force_no_trans)

{
  int ret = OB_SUCCESS;
  hint_force_no_trans = false;
  const ObQueryHint *query_hint = NULL;
  ObString qb_name;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (OB_FAIL(subquery.get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else {
    const ObHint *myhint = get_hint(stmt.get_stmt_hint());
    const ObMaterializeHint *hint = static_cast<const ObMaterializeHint*>(myhint);
    if (!query_hint->has_outline_data()) {
      if (NULL == myhint ||
          !hint->has_qb_name_list()) {
        const ObHint *no_rewrite_hint = stmt.get_stmt_hint().get_no_rewrite_hint();
        if (NULL != no_rewrite_hint) {
          if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite_hint))) {
            LOG_WARN("failed to add used transform hint", K(ret));
          } else {
            hint_force_no_trans = true;
          }
        }
      } else if (OB_FAIL(hint->get_qb_name_list(qb_name, qb_names))) {
        LOG_WARN("failed to get qb name list", K(ret));
      }
    } else {
      bool is_valid = query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                                        myhint);
      if (!is_valid) {
        hint_force_no_trans = true;
      } else if (OB_ISNULL(myhint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null hint", K(ret));
      } else if (OB_FAIL(hint->get_qb_name_list(qb_name, qb_names))) {
        LOG_WARN("failed to get qb name list", K(ret));
      } else if (qb_names.empty()) {
        hint_force_no_trans = true;
      }
    }
  }
  return ret;
}

int ObTransformTempTable::sort_materialize_stmts(Ob2DArray<MaterializeStmts *> &materialize_stmts)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<int, int>, 4> index_map;
  Ob2DArray<MaterializeStmts *> new_stmts;
  auto cmp_func1 = [](ObSelectStmt* l_stmt, ObSelectStmt* r_stmt){
    if (OB_ISNULL(l_stmt) || OB_ISNULL(r_stmt)) {
      return false;
    } else {
      return l_stmt->get_stmt_id() < r_stmt->get_stmt_id();
    }
  };
  auto cmp_func2 = [](std::pair<int,int> &lhs, std::pair<int,int> &rhs){
    return lhs.second < rhs.second;
  };
  for (int64_t i = 0; OB_SUCC(ret) && i < materialize_stmts.count(); ++i) {
    MaterializeStmts *subqueries = materialize_stmts.at(i);
    if (OB_ISNULL(subqueries)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmts", K(ret));
    } else {
      lib::ob_sort(subqueries->begin(), subqueries->end(), cmp_func1);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < materialize_stmts.count(); ++i) {
    MaterializeStmts *subqueries = materialize_stmts.at(i);
    if (OB_ISNULL(subqueries)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmts", K(ret));
    } else if (subqueries->empty() || OB_ISNULL(subqueries->at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmts", K(ret));
    } else if (OB_FAIL(index_map.push_back(std::pair<int,int>(i, subqueries->at(0)->get_stmt_id())))) {
      LOG_WARN("failed to push back index", K(ret));
    }
  }
  lib::ob_sort(index_map.begin(), index_map.end(), cmp_func2);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_map.count(); ++i) {
    int index = index_map.at(i).first;
    if (index < 0 || index >= materialize_stmts.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index out of range", K(ret));
    } else if (OB_FAIL(new_stmts.push_back(materialize_stmts.at(index)))) {
      LOG_WARN("failed to push back stmts", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(materialize_stmts.assign(new_stmts))) {
      LOG_WARN("failed to assign array", K(ret));
    }
  }
  return ret;
}

// check hint for T_PUSH_PRED_CTE and T_PROJECT_PRUNE
int ObTransformTempTable::check_hint_allowed_trans(const ObSelectStmt &ref_query,
                                                   const ObItemType check_hint_type,
                                                   bool &allowed) const
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = NULL;
  const ObHint *myhint = ref_query.get_stmt_hint().get_normal_hint(check_hint_type);
  bool is_enable = (NULL != myhint && myhint->is_enable_hint());
  bool is_disable = (NULL != myhint && myhint->is_disable_hint());
  allowed = false;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(query_hint = ref_query.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (OB_UNLIKELY(T_PUSH_PRED_CTE != check_hint_type
                         && T_PROJECT_PRUNE != check_hint_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect hint type", K(ret), "type", get_type_name(check_hint_type));
  } else if (!query_hint->has_outline_data()) {
    const ObHint *no_rewrite_hint = ref_query.get_stmt_hint().get_no_rewrite_hint();
    if (is_enable) {
      allowed = true;
    } else if (NULL != no_rewrite_hint || is_disable) {
      if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite_hint))) {
        LOG_WARN("failed to add used transform hint", K(ret));
      } else if (is_disable && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
        LOG_WARN("failed to add used transform hint", K(ret));
      }
    } else {
      allowed = true;
    }
  } else if (query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, myhint)) {
    allowed = true;
  }
  return ret;
}

int ObTransformTempTable::get_stmt_pointers(ObDMLStmt &root_stmt,
                                            ObIArray<ObSelectStmt *> &stmts,
                                            hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                            ObIArray<ObSelectStmtPointer> &stmt_ptrs)
{
  int ret = OB_SUCCESS;
  ObArray<TempTableInfo> temp_table_infos;
  if (OB_FAIL(root_stmt.collect_temp_table_infos(temp_table_infos))) {
    LOG_WARN("failed to collect temp table infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); i ++) {
    uint64_t key = reinterpret_cast<uint64_t>(stmts.at(i));
    ObSelectStmtPointer stmt_ptr;
    ObParentDMLStmt parent_stmt;
    bool is_find = false;
    if (OB_FAIL(parent_map.get_refactored(key, parent_stmt))) {
      LOG_WARN("failed to get value", K(ret));
    } else if (NULL != parent_stmt.stmt_) {
      int64_t pos = 0;
      ObDMLStmt* stmt = parent_stmt.stmt_;
      if (stmt->is_select_stmt()) {
        ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
        if (parent_stmt.pos_ < sel_stmt->get_set_query().count()) {
          is_find = true;
          OZ(stmt_ptr.add_ref(&sel_stmt->get_set_query().at(parent_stmt.pos_)));
        } else {
          pos += sel_stmt->get_set_query().count();
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < stmt->get_table_size(); ++j) {
        TableItem *table_item = stmt->get_table_item(j);
        if (OB_ISNULL(table_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table_item is null", K(i));
        } else if (table_item->is_generated_table() ||
                   table_item->is_lateral_table()) {
          if (parent_stmt.pos_ == pos) {
            is_find = true;
            OZ(stmt_ptr.add_ref(&table_item->ref_query_));
          } else {
            pos++;
          }
        } else { /*do nothing*/ }
      }
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < stmt->get_subquery_expr_size(); ++j) {
        ObQueryRefRawExpr *subquery_ref = stmt->get_subquery_exprs().at(j);
        if (OB_ISNULL(subquery_ref)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subquery reference is null", K(subquery_ref));
        } else if (parent_stmt.pos_ == pos) {
          is_find = true;
          OZ(stmt_ptr.add_ref(&subquery_ref->get_ref_stmt()));
        } else {
          pos++;
        }
      }
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < temp_table_infos.count(); ++j) {
        if (stmts.at(i) == temp_table_infos.at(j).temp_table_query_) {
          is_find = true;
          for (int64_t k = 0; OB_SUCC(ret) && k < temp_table_infos.at(j).table_items_.count(); ++k) {
            OZ(stmt_ptr.add_ref(&temp_table_infos.at(j).table_items_.at(k)->ref_query_));
          }
        }
      }
    }
    if (!is_find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not find stmt", K(parent_stmt), KPC(stmts.at(i)));
    }
    OZ(stmt_ptrs.push_back(stmt_ptr));
  }

  return ret;
}

int ObTransformTempTable::adjust_transformed_stmt(ObIArray<ObSelectStmtPointer> &stmt_ptrs,
                                                  ObIArray<ObSelectStmt *> &stmts,
                                                  ObIArray<ObSelectStmt *> *origin_stmts)
{
  int ret = OB_SUCCESS;
  if (stmts.count() != stmt_ptrs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(ret));
  } else if (origin_stmts != NULL) {
    origin_stmts->reuse();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_ptrs.count(); i ++) {
    ObSelectStmt *origin_stmt = NULL;
    if (OB_ISNULL(stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null stmt", K(ret));
    } else if (NULL != origin_stmts && OB_FAIL(stmt_ptrs.at(i).get(origin_stmt))) {
      LOG_WARN("failed to get ptr", K(ret));
    } else if (NULL != origin_stmts && OB_FAIL(origin_stmts->push_back(origin_stmt))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(stmt_ptrs.at(i).set(stmts.at(i)))) {
      LOG_WARN("failed to set ptr", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::accept_cte_transform(ObDMLStmt &origin_root_stmt,
                                               TableItem *temp_table,
                                               common::ObIArray<ObSelectStmt *> &origin_stmts,
                                               common::ObIArray<ObSelectStmt *> &trans_stmts,
                                               common::ObIArray<ObSelectStmt *> &accept_stmts,
                                               hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                               bool force_accept,
                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  cost_based_trans_tried_ = true;
  ObSEArray<double, 2> origin_costs;
  ObSEArray<double, 2> trans_costs;
  ObSEArray<bool, 2> do_use_cte;
  ObSEArray<ObSelectStmtPointer, 2> stmt_ptrs;
  double temp_table_costs = 0.0;
  double dummy = 0.0;
  double temp_table_profit = 0.0;
  BEGIN_OPT_TRACE_EVA_COST;
  if (OB_ISNULL(ctx_) || OB_UNLIKELY(origin_stmts.count() != trans_stmts.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), K(ctx_));
  } else if (OB_FAIL(do_use_cte.prepare_allocate(origin_stmts.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else if (OB_FAIL(get_stmt_pointers(origin_root_stmt, origin_stmts, parent_map, stmt_ptrs))) {
    LOG_WARN("failed to get stmt pointers", K(ret));
  } else if (force_accept) {
    trans_happened = true;
  } else if (ctx_->is_set_stmt_oversize_) {
    LOG_TRACE("not accept transform because large set stmt", K(ctx_->is_set_stmt_oversize_));
  } else if (OB_FAIL(evaluate_cte_cost(origin_root_stmt, false, origin_stmts, stmt_ptrs, origin_costs, NULL, dummy))) {
    LOG_WARN("failed to evaluate cost for the origin stmt", K(ret));
  } else if (OB_FAIL(evaluate_cte_cost(origin_root_stmt, true, trans_stmts, stmt_ptrs, trans_costs, temp_table, temp_table_costs))) {
    LOG_WARN("failed to evaluate cost for the transform stmt", K(ret));
  } else if (OB_UNLIKELY(origin_costs.count() != trans_costs.count()) ||
             OB_UNLIKELY(origin_costs.count() != do_use_cte.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array size", K(origin_costs), K(trans_costs), K(trans_happened));
  }
  END_OPT_TRACE_EVA_COST;
  if (OB_SUCC(ret)) {
    if (!force_accept) {
      // Only consider stmt whose cost is reduced after extracting cte.
      int64_t accept_cte_cnt = 0;
      for (int64_t i = 0; i < origin_costs.count(); i ++) {
        if (origin_costs.at(i) > trans_costs.at(i)) {
          do_use_cte.at(i) = true;
          temp_table_profit += origin_costs.at(i) - trans_costs.at(i);
          accept_cte_cnt ++;
        }
      }

      // Extract CTE if the profit is greater than the cost
      trans_happened = (temp_table_profit > temp_table_costs) && (accept_cte_cnt > 1);
    } else {
      for (int64_t i = 0; i < do_use_cte.count(); i ++) {
        do_use_cte.at(i) = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!trans_happened) {
    OPT_TRACE("reject transform because the cost is increased");
    OPT_TRACE("The cost of extract cte :", temp_table_costs);
    OPT_TRACE("The profit of extract cte :", temp_table_profit);
    LOG_TRACE("reject transform because the cost is increased",
                     K_(ctx_->is_set_stmt_oversize), K(temp_table_costs),
                     K(temp_table_profit), K(origin_costs), K(trans_costs));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_stmts.count(); i ++) {
      if (do_use_cte.at(i)) {
        // root stmt will not extract cte
        if (OB_FAIL(stmt_ptrs.at(i).set(trans_stmts.at(i)))) {
          LOG_WARN("failed to set ptr", K(ret));
        } else if (OB_FAIL(accept_stmts.push_back(origin_stmts.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
        OPT_TRACE("Materialize stmt :", trans_stmts.at(i));
      }
    }
    if (force_accept) {
      OPT_TRACE("accept cte because of the hint");
    } else {
      OPT_TRACE("accept cte because the cost is decreased");
      OPT_TRACE("The cost of extract cte :", temp_table_costs);
      OPT_TRACE("The profit of extract cte :", temp_table_profit);
      LOG_TRACE("accept transform because the cost is decreased",
                     K_(ctx_->is_set_stmt_oversize), K(temp_table_costs),
                     K(temp_table_profit), K(origin_costs), K(trans_costs));
    }
  }
  return ret;
}

int ObTransformTempTable::evaluate_cte_cost(ObDMLStmt &root_stmt,
                                            bool is_trans_stmt,
                                            ObIArray<ObSelectStmt *> &stmts,
                                            ObIArray<ObSelectStmtPointer> &stmt_ptrs,
                                            ObIArray<double> &costs,
                                            TableItem *temp_table,
                                            double &temp_table_cost)
{
  int ret = OB_SUCCESS;
  ObEvalCostHelper eval_cost_helper;
  temp_table_cost = 0.0;
  if (OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid())
      || OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())
      || OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory())
      || OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx())
      || OB_ISNULL(root_stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(root_stmt));
  } else if (OB_FAIL(eval_cost_helper.fill_helper(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                  *root_stmt.get_query_ctx(), *ctx_))) {
    LOG_WARN("failed to fill eval cost helper", K(ret));
  } else {
    ctx_->eval_cost_ = true;
    ParamStore &param_store = ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store_for_update();
    lib::ContextParam param;
    ObArray<ObParentDMLStmt> dummy;
    ObDMLStmt *copy_root_stmt = NULL;
    ObSEArray<ObSelectStmt*, 2> origin_stmts;
    param.set_mem_attr(ctx_->session_info_->get_effective_tenant_id(),
                       "TempTableCost",
                       ObCtxIds::DEFAULT_CTX_ID)
       .set_properties(lib::USE_TL_PAGE_OPTIONAL)
       .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSelectStmt * copy_cte_stmt = NULL;
    common::ObSEArray<ObSelectStmt *, 2> copy_stmts;
    if (OB_FAIL(prepare_eval_cte_cost_stmt(root_stmt,
                                           stmts,
                                           NULL != temp_table ? temp_table->ref_query_ : NULL,
                                           stmt_ptrs,
                                           copy_root_stmt,
                                           copy_stmts,
                                           copy_cte_stmt,
                                           is_trans_stmt))) {
      LOG_WARN("failed to prepare eval cost stmt", K(ret));
    } else {
      CREATE_WITH_TEMP_CONTEXT(param) {
        ObRawExprFactory tmp_expr_factory(CURRENT_CONTEXT->get_arena_allocator());
        HEAP_VAR(ObOptimizerContext, optctx,
                ctx_->session_info_,
                ctx_->exec_ctx_,
                ctx_->sql_schema_guard_,
                ctx_->opt_stat_mgr_,
                CURRENT_CONTEXT->get_arena_allocator(),
                &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
                *ctx_->self_addr_,
                GCTX.srv_rpc_proxy_,
                root_stmt.get_query_ctx()->get_global_hint(),
                tmp_expr_factory,
                copy_root_stmt,
                false,
                ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx()) {
          // optctx.set_only_ds_basic_stat(true);
          ObOptimizer optimizer(optctx);
          if (OB_FAIL(optimizer.get_cte_optimization_cost(*copy_root_stmt,
                                                          copy_cte_stmt,
                                                          copy_stmts,
                                                          temp_table_cost,
                                                          costs))) {
            LOG_WARN("failed to get cost", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(eval_cost_helper.recover_context(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                      *ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx(),
                                                      *ctx_))) {
            LOG_WARN("failed to recover context", K(ret));
          } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, copy_root_stmt))) {
            LOG_WARN("failed to free stmt", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformTempTable::accept_cte_transform_v2(ObDMLStmt &origin_root_stmt,
                                                  TableItem *temp_table,
                                                  common::ObIArray<ObSelectStmt *> &origin_stmts,
                                                  common::ObIArray<ObSelectStmt *> &trans_stmts,
                                                  common::ObIArray<ObSelectStmt *> &accept_stmts,
                                                  hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                                  bool force_accept,
                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  cost_based_trans_tried_ = true;
  ObDMLStmt *copy_inline_root;
  ObDMLStmt *copy_materialize_root;
  ObSEArray<ObSelectStmt*, 4> copy_inline_stmts;
  ObSEArray<ObSelectStmt*, 4> copy_materialize_stmts;
  ObSEArray<ObSelectStmtPointer, 2> stmt_ptrs;
  ObSelectStmt *dummy_stmt = NULL;
  ObSelectStmt *copy_cte_query = NULL;
  ObTryTransHelper inline_trans_helper;
  ObTryTransHelper materialize_trans_helper;
  ObSEArray<int64_t, 4> choosed_inline_idxs;
  ObSEArray<int64_t, 4> choosed_materialize_idxs;
  if (OB_ISNULL(ctx_) || OB_UNLIKELY(origin_stmts.count() != trans_stmts.count()) ||
      OB_ISNULL(temp_table) || OB_ISNULL(temp_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), K(ctx_));
  } else if (OB_FAIL(get_stmt_pointers(origin_root_stmt, origin_stmts, parent_map, stmt_ptrs))) {
    LOG_WARN("failed to get stmt pointers", K(ret));
  } else if (origin_stmts.count() != stmt_ptrs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt pointers", K(ret), K(origin_stmts.count()), K(stmt_ptrs.count()));
  } else if (force_accept) {
    OPT_TRACE("accept cte because of the hint");
    trans_happened = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_stmts.count(); i++) {
      if (OB_FAIL(stmt_ptrs.at(i).set(trans_stmts.at(i)))) {
        LOG_WARN("failed to set ptr", K(ret));
      } else if (OB_FAIL(accept_stmts.push_back(origin_stmts.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
      OPT_TRACE("Materialize stmt :", trans_stmts.at(i));
    }
  } else if (ctx_->is_set_stmt_oversize_) {
    LOG_TRACE("not accept transform because large set stmt", K(ctx_->is_set_stmt_oversize_));
  } else if (OB_FAIL(pick_out_stmts_in_blacklist(ctx_->materialize_blacklist_,
                                                 origin_stmts,
                                                 trans_stmts,
                                                 stmt_ptrs))) {
    LOG_WARN("failed to pick out stmts in blacklist", K(ret));
  } else if (origin_stmts.count() < 2) {
    OPT_TRACE("reject materialize CTE due to blacklist");
    trans_happened = false;
  } else if (OB_FAIL(inline_trans_helper.fill_helper(origin_root_stmt.get_query_ctx()))) {
    LOG_WARN("failed to fill helper", K(ret));
  } else if (OB_FAIL(materialize_trans_helper.fill_helper(origin_root_stmt.get_query_ctx()))) {
    LOG_WARN("failed to fill helper", K(ret));
  } else if (OB_FAIL(copy_and_replace_trans_root(origin_root_stmt,
                                                origin_stmts,
                                                NULL,
                                                stmt_ptrs,
                                                copy_inline_root,
                                                copy_inline_stmts,
                                                dummy_stmt,
                                                false))) {
    LOG_WARN("failed to prepare eval cte cost stmt", K(ret));
  } else if (OB_FAIL(copy_and_replace_trans_root(origin_root_stmt,
                                                trans_stmts,
                                                temp_table->ref_query_,
                                                stmt_ptrs,
                                                copy_materialize_root,
                                                copy_materialize_stmts,
                                                copy_cte_query,
                                                true))) {
    LOG_WARN("failed to prepare eval cte cost stmt", K(ret));
  } else if (OB_FAIL(evaluate_inline_materialize_costs(&origin_root_stmt,
                                                      true,
                                                      copy_inline_root,
                                                      copy_inline_stmts,
                                                      copy_materialize_root,
                                                      copy_materialize_stmts,
                                                      copy_cte_query,
                                                      choosed_inline_idxs,
                                                      choosed_materialize_idxs))) {
    LOG_WARN("failed to evaluate inline materialize costs", K(ret));
  } else if (choosed_materialize_idxs.count() > 1) {
    OPT_TRACE("accept materialize cte due to cost");
    trans_happened = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < choosed_materialize_idxs.count(); i++) {
      int64_t stmt_idx = choosed_materialize_idxs.at(i);
      if (OB_UNLIKELY(stmt_idx < 0 || stmt_idx >= origin_stmts.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected idx", K(ret), K(stmt_idx));
      } else if (OB_FAIL(stmt_ptrs.at(stmt_idx).set(trans_stmts.at(stmt_idx)))) {
        LOG_WARN("failed to set ptr", K(ret));
      } else if (OB_FAIL(accept_stmts.push_back(origin_stmts.at(stmt_idx)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  } else {
    OPT_TRACE("reject materialize cte due to cost");
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_stmts.count(); i++) {
      ObString qb_name;
      if (OB_ISNULL(origin_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null stmt", K(ret));
      } else if (OB_FAIL(origin_stmts.at(i)->get_qb_name(qb_name))) {
        LOG_WARN("failed to get qb name", K(ret));
      } else if (OB_FAIL(ctx_->materialize_blacklist_.push_back(qb_name))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformTempTable::evaluate_cte_cost_partially(ObDMLStmt *root_stmt,
                                                      ObIArray<ObSelectStmt *> &stmts,
                                                      ObIArray<double> &costs,
                                                      ObSelectStmt *cte_query,
                                                      double &temp_table_cost)
{
  int ret = OB_SUCCESS;
  ObEvalCostHelper eval_cost_helper;
  temp_table_cost = 0.0;
  bool can_eval = false;
  if (OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid()) || OB_ISNULL(root_stmt)
      || OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())
      || OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory())
      || OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx())
      || OB_ISNULL(root_stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(root_stmt));
  } else if (OB_FAIL(eval_cost_helper.fill_helper(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                  *root_stmt->get_query_ctx(), *ctx_))) {
    LOG_WARN("failed to fill eval cost helper", K(ret));
  } else {
    BEGIN_OPT_TRACE_EVA_COST;
    ctx_->eval_cost_ = true;
    ParamStore &param_store = ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store_for_update();
    lib::ContextParam param;
    ObTransformerImpl trans(ctx_);
    bool trans_happended = false;
    uint64_t trans_rules = ALL_EXPR_LEVEL_HEURISTICS_RULES;
    param.set_mem_attr(ctx_->session_info_->get_effective_tenant_id(),
                       "TempTableCost",
                       ObCtxIds::DEFAULT_CTX_ID)
       .set_properties(lib::USE_TL_PAGE_OPTIONAL)
       .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(trans.transform_rule_set(root_stmt,
                                        trans_rules,
                                        trans.get_max_iteration_count(),
                                        trans_happended))) {
      LOG_WARN("failed to transform heuristic rule", K(ret));
    } else if (OB_FAIL(check_evaluate_after_transform(root_stmt, stmts, can_eval))) {
      LOG_WARN("failed to check after transform", K(ret));
    } else if (!can_eval) {
      temp_table_cost = std::numeric_limits<double>::max();
      for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); i++) {
        if (OB_FAIL(costs.push_back(0.0))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(eval_cost_helper.recover_context(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                    *ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx(),
                                                    *ctx_))) {
          LOG_WARN("failed to recover context", K(ret));
        } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, root_stmt))) {
          LOG_WARN("failed to free stmt", K(ret));
        }
      }
    } else {
      CREATE_WITH_TEMP_CONTEXT(param) {
        ObRawExprFactory tmp_expr_factory(CURRENT_CONTEXT->get_arena_allocator());
        HEAP_VAR(ObOptimizerContext, optctx,
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
          // optctx.set_only_ds_basic_stat(true);
          ObOptimizer optimizer(optctx);
          if (OB_FAIL(optimizer.get_cte_optimization_cost(*root_stmt,
                                                          cte_query,
                                                          stmts,
                                                          temp_table_cost,
                                                          costs))) {
            LOG_WARN("failed to get cost", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(eval_cost_helper.recover_context(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                      *ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx(),
                                                      *ctx_))) {
            LOG_WARN("failed to recover context", K(ret));
          } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, root_stmt))) {
            LOG_WARN("failed to free stmt", K(ret));
          }
        }
      }
    }
    END_OPT_TRACE_EVA_COST;
  }
  return ret;
}

int ObTransformTempTable::evaluate_cte_cost_globally(ObDMLStmt *origin_root,
                                                     ObDMLStmt *root_stmt,
                                                     ObIArray<int64_t> &semi_join_stmt_ids,
                                                     bool need_check_plan,
                                                     double &global_cost)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *copy_cte_stmt = NULL;
  ObEvalCostHelper eval_cost_helper;
  common::ObSEArray<ObSelectStmt *, 2> copy_stmts;
  if (OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid()) || OB_ISNULL(root_stmt) ||
      OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx()) ||
      OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory()) ||
      OB_ISNULL(ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_));
  } else if (OB_FAIL(eval_cost_helper.fill_helper(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                  *root_stmt->get_query_ctx(), *ctx_))) {
    LOG_WARN("failed to fill eval cost helper", K(ret));
  } else {
    BEGIN_OPT_TRACE_EVA_COST;
    ctx_->eval_cost_ = true;
    ParamStore &param_store = ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store_for_update();
    lib::ContextParam param;
    ObTransformerImpl trans(ctx_);
    bool trans_happended = false;
    uint64_t trans_rules = ALL_HEURISTICS_RULES | (1L << TEMP_TABLE_OPTIMIZATION);
    param.set_mem_attr(ctx_->session_info_->get_effective_tenant_id(),
                       "TempTableCost",
                       ObCtxIds::DEFAULT_CTX_ID)
       .set_properties(lib::USE_TL_PAGE_OPTIONAL)
       .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(add_semi_to_inner_hint_if_need(root_stmt, semi_join_stmt_ids))) {
      LOG_WARN("failed to add semi to inner hint", K(ret));
    } else if (OB_FAIL(trans.transform_rule_set(root_stmt,
                                                trans_rules,
                                                trans.get_max_iteration_count(),
                                                trans_happended))) {
      LOG_WARN("failed to transform heuristic rule", K(ret));
    } else {
      CREATE_WITH_TEMP_CONTEXT(param) {
        ObRawExprFactory tmp_expr_factory(CURRENT_CONTEXT->get_arena_allocator());
        HEAP_VAR(ObOptimizerContext, optctx,
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
          if (OB_FAIL(optimizer.get_optimization_cost(*root_stmt, plan, global_cost))) {
            LOG_WARN("failed to get optimization cost", K(ret));
          } else if (need_check_plan && OB_NOT_NULL(plan)) {
            ObSEArray<uint64_t, 4> blacklist;
            if (OB_FAIL(gather_materialize_blacklist(plan->get_plan_root(), blacklist))) {
              LOG_WARN("failed to gather materialize blacklist", K(ret));
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < blacklist.count(); ++i) {
              ObDMLStmt *stmt = NULL;
              ObString qb_name;
              if (OB_FAIL(origin_root->get_stmt_by_stmt_id(blacklist.at(i), stmt))) {
                LOG_WARN("failed to get stmt by stmt id", K(ret));
              } else if (OB_ISNULL(stmt)) {
                // do nothing, may generate new stmt after rewrite
              } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
                LOG_WARN("failed to get qb name", K(ret));
              } else if (OB_FAIL(ctx_->materialize_blacklist_.push_back(qb_name))) {
                LOG_WARN("failed to push back", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(eval_cost_helper.recover_context(*ctx_->exec_ctx_->get_physical_plan_ctx(),
                                                       *ctx_->exec_ctx_->get_stmt_factory()->get_query_ctx(),
                                                       *ctx_))) {
            LOG_WARN("failed to recover context", K(ret));
          } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, root_stmt))) {
            LOG_WARN("failed to free stmt", K(ret));
          }
        }
      }
    }
    END_OPT_TRACE_EVA_COST;
  }
  return ret;
}

int ObTransformTempTable::need_check_global_cte_cost(const ObDMLStmt *root_stmt,
                                                     const ObIArray<ObSelectStmt *> &origin_stmts,
                                                     const hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                                     const ObSelectStmt *temp_table_query,
                                                     ObIArray<int64_t> &semi_join_stmt_ids,
                                                     bool &check_global_cost)
{
  int ret = OB_SUCCESS;
  check_global_cost = false;
  if (OB_ISNULL(root_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->sql_schema_guard_) ||
      OB_ISNULL(temp_table_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret));
  } else {
    // 1. can pushdown dynamic filter
    for (int64_t i = 0; OB_SUCC(ret) && !check_global_cost && i < origin_stmts.count(); ++i) {
      const ObSelectStmt *stmt = origin_stmts.at(i);
      ObSEArray<int64_t, 4> sel_idxs;
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret));
      } else {
        // collect idxs of select items that can match index
        for (int64_t j = 0; OB_SUCC(ret) && j < stmt->get_select_item_size(); j++) {
          bool match_index = false;
          if (OB_FAIL(ObTransformUtils::check_select_item_match_index(root_stmt,
                                                                      stmt,
                                                                      ctx_->sql_schema_guard_,
                                                                      j,
                                                                      match_index))) {
            LOG_WARN("failed to check select item match index", K(ret));
          } else if (!match_index) {
            // do nothing
          } else if (OB_FAIL(sel_idxs.push_back(j))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
      // check the opportunity of pushing down dynamic filter to view through NLJ inner path
      if (OB_SUCC(ret) && sel_idxs.count() > 0) {
        bool use_for_join = false;
        if (OB_FAIL(check_projected_cols_used_for_join(stmt,
                                                       parent_map,
                                                       sel_idxs,
                                                       semi_join_stmt_ids,
                                                       use_for_join))) {
          LOG_WARN("failed to check projected cols used for join", K(ret));
        } else if (use_for_join) {
          check_global_cost = true;
        }
      }
    }
    // 2. current temp table references another temp table（may inline after extraction）
    for (int64_t i = 0; OB_SUCC(ret) && !check_global_cost && i < temp_table_query->get_table_size(); ++i) {
      const TableItem *table_item = temp_table_query->get_table_items().at(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (table_item->is_temp_table()) {
        check_global_cost = true;
      }
    }
  }
  return ret;
}


/**
 * @brief check_projected_cols_used_for_join
 * check whether select items projected from the generated table are used for joins (pushdown dynamic filter)
 * @param semi_join_stmt_ids refers to stmts that use target generated table as left table of semi join
 */
int ObTransformTempTable::check_projected_cols_used_for_join(const ObSelectStmt *stmt,
                                                             const hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                                             const ObIArray<int64_t> &sel_idxs,
                                                             ObIArray<int64_t> &semi_join_stmt_ids,
                                                             bool &used_for_join)
{
  int ret = OB_SUCCESS;
  uint64_t cur_table_id = OB_INVALID_ID;
  ObDMLStmt *parent_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret));
  } else if (sel_idxs.count() == 0 || used_for_join) {
    // do nothing
  } else if (OB_FAIL(get_parent_stmt(stmt, parent_map, cur_table_id, parent_stmt))) {
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (OB_ISNULL(parent_stmt)) {
    // do nothing
  } else if (parent_stmt->is_set_stmt()) {
    if (OB_FAIL(SMART_CALL(check_projected_cols_used_for_join(static_cast<const ObSelectStmt*>(parent_stmt),
                                                              parent_map,
                                                              sel_idxs,
                                                              semi_join_stmt_ids,
                                                              used_for_join)))) {
      LOG_WARN("failed to check projected cols used for join", K(ret));
    }
  } else if (cur_table_id != OB_INVALID_ID) {
    ObStmtExprGetter visitor;
    ObSEArray<ObRawExpr*, 4> tmp_conds;
    ObSEArray<ObColumnRefRawExpr*, 4> join_col_exprs;
    ObSEArray<ObColumnRefRawExpr*, 4> mapped_col_exprs;
    bool can_filter_pushdown = false;
    visitor.remove_all();
    visitor.add_scope(SCOPE_JOINED_TABLE);
    visitor.add_scope(SCOPE_WHERE);
    visitor.add_scope(SCOPE_SEMI_INFO);
    parent_stmt->get_relation_exprs(tmp_conds, visitor);
    // convert select item idxs to columns of parent stmt
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_idxs.count(); ++i) {
      int64_t col_id = sel_idxs.at(i) + OB_APP_MIN_COLUMN_ID;
      ColumnItem *col_item = NULL;
      ObColumnRefRawExpr *col_expr = NULL;
      if (OB_ISNULL(col_item = parent_stmt->get_column_item(cur_table_id, col_id))) {
        // do nothing, unused cols are not in the stmt
      } else if (OB_ISNULL(col_expr = col_item->get_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is NULL", K(ret));
      } else if (OB_FAIL(mapped_col_exprs.push_back(col_expr))) {
        LOG_WARN("failed to push back column expr", K(ret));
      }
    }
    // collect cols used in join conditions
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_conds.count(); ++i) {
      ObRawExpr *cond = NULL;
      if (OB_ISNULL(cond = tmp_conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else if (cond->get_relation_ids().num_members() < 2 &&
                 !cond->has_flag(CNT_SUB_QUERY)) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::get_simple_filter_column(parent_stmt,
                                                                    cond,
                                                                    cur_table_id,
                                                                    join_col_exprs))) {
        LOG_WARN("failed to get simple filter column", K(ret));
      } else if ((cond->has_flag(IS_WITH_ANY) || cond->has_flag(IS_WITH_ALL)) &&
                 IS_SUBQUERY_COMPARISON_OP(cond->get_expr_type())) {
        ObColumnRefRawExpr *col = NULL;
        if (cond->get_param_count() != 2 || OB_ISNULL(cond->get_param_expr(0)) ||
            OB_ISNULL(cond->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr", K(ret));
        } else if (cond->get_param_expr(0)->is_column_ref_expr()) {
          col = static_cast<ObColumnRefRawExpr*>(cond->get_param_expr(0));
          if (col->get_table_id() != cur_table_id) {
            // do nothing
          } else if (OB_FAIL(join_col_exprs.push_back(col))) {
            LOG_WARN("failed to push back column expr", K(ret));
          }
        }
      }
    }
    // collect cols from candi join condition (subquery unnest)
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_stmt->get_subquery_expr_size(); ++i) {
      ObQueryRefRawExpr *expr = parent_stmt->get_subquery_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else if (OB_FAIL(extract_pushdown_cols(*expr, cur_table_id, join_col_exprs))) {
        LOG_WARN("failed to extract pushdown cols", K(ret));
      }
    }
    // check overlap of join_col_exprs and mapped_col_exprs
    for (int64_t i = 0; OB_SUCC(ret) && !used_for_join && i < join_col_exprs.count(); ++i) {
      ObColumnRefRawExpr* col = join_col_exprs.at(i);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null column expr", K(ret));
      } else if (ObOptimizerUtil::find_item(mapped_col_exprs, col)) {
        used_for_join = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (used_for_join) {
      if (OB_FAIL(collect_semi_join_stmt_ids(*parent_stmt,
                                             mapped_col_exprs,
                                             cur_table_id,
                                             semi_join_stmt_ids))) {
        LOG_WARN("failed to collect semi join stmt ids", K(ret));
      }
    } else if (parent_stmt->is_select_stmt()) {
      // recursively check parent stmt if need
      const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt*>(parent_stmt);
      ObSEArray<int64_t,4> new_sel_idxs;
      if (OB_FAIL(can_push_dynamic_filter_to_cols(sel_stmt,
                                                mapped_col_exprs,
                                                cur_table_id,
                                                new_sel_idxs,
                                                can_filter_pushdown))) {
        LOG_WARN("failed to check filter pushdown to cols", K(ret));
      } else if (!can_filter_pushdown) {
        // do nothing
      } else if (OB_FAIL(SMART_CALL(check_projected_cols_used_for_join(sel_stmt,
                                                                       parent_map,
                                                                       new_sel_idxs,
                                                                       semi_join_stmt_ids,
                                                                       used_for_join)))) {
        LOG_WARN("failed to check projected cols used for join", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformTempTable::extract_pushdown_cols(const ObQueryRefRawExpr &query_ref,
                                                uint64_t cur_table_id,
                                                ObIArray<ObColumnRefRawExpr*> &pushdown_cols)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *col = NULL;
  for (int64_t j = 0; OB_SUCC(ret) && j < query_ref.get_exec_params().count(); j++) {
    ObExecParamRawExpr *exec_param = query_ref.get_exec_params().at(j);
    if (OB_ISNULL(exec_param) || OB_ISNULL(exec_param->get_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is NULL", K(ret));
    } else if (!exec_param->get_ref_expr()->is_column_ref_expr()) {
      // do nothing
    } else if (OB_FALSE_IT(col = static_cast<ObColumnRefRawExpr*>(exec_param->get_ref_expr()))) {
    } else if (col->get_table_id() != cur_table_id) {
      // do nothing
    } else if (OB_FAIL(pushdown_cols.push_back(col))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::collect_semi_join_stmt_ids(const ObDMLStmt &parent_stmt,
                                                     const ObIArray<ObColumnRefRawExpr*> &mapped_col_exprs,
                                                     uint64_t cur_table_id,
                                                     ObIArray<int64_t> &semi_join_stmt_ids)
{
  int ret = OB_SUCCESS;
  // collect stmt id if generated table is on left side of semi join
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < parent_stmt.get_semi_infos().count(); ++i) {
    SemiInfo *semi_info = parent_stmt.get_semi_infos().at(i);
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is NULL", K(ret));
    } else if (!semi_info->is_semi_join()) {
      // do nothing
    } else if (!ObOptimizerUtil::find_item(semi_info->left_table_ids_, cur_table_id)) {
      // do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(semi_join_stmt_ids, parent_stmt.stmt_id_))) {
      LOG_WARN("failed to add var to array no dup", K(ret));
    } else {
      found = true;
    }
  }
  // collect stmt id from any/all/exists subquery (candidate semi join)
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < parent_stmt.get_condition_size(); i++) {
    ObRawExpr *cond = const_cast<ObRawExpr*>(parent_stmt.get_condition_expr(i));
    ObQueryRefRawExpr *query_ref = NULL;
    ObSEArray<ObColumnRefRawExpr*, 2> candi_cols;
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cond is NULL", K(ret));
    } else if (cond->get_expr_type() == T_OP_EXISTS) {
      if (cond->get_param_count() != 1 || OB_ISNULL(cond->get_param_expr(0)) ||
          !cond->get_param_expr(0)->is_query_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", K(ret));
      } else if (OB_FALSE_IT(query_ref = static_cast<ObQueryRefRawExpr*>(cond->get_param_expr(0)))) {
      } else if (OB_FAIL(extract_pushdown_cols(*query_ref, cur_table_id, candi_cols))) {
        LOG_WARN("failed to extract pushdown cols", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !found && j < candi_cols.count(); ++j) {
          if (ObOptimizerUtil::find_item(mapped_col_exprs, candi_cols.at(j))) {
            found = true;
            if (OB_FAIL(add_var_to_array_no_dup(semi_join_stmt_ids, parent_stmt.stmt_id_))) {
              LOG_WARN("failed to add var to array no dup", K(ret));
            }
          }
        }
      }
    } else if ((cond->has_flag(IS_WITH_ANY) || cond->has_flag(IS_WITH_ALL)) &&
                IS_SUBQUERY_COMPARISON_OP(cond->get_expr_type())) {
      ObColumnRefRawExpr *col = NULL;
      if (cond->get_param_count() != 2 || OB_ISNULL(cond->get_param_expr(0)) ||
          OB_ISNULL(cond->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", K(ret));
      } else if (cond->get_param_expr(0)->is_column_ref_expr() &&
                 OB_FALSE_IT(col = static_cast<ObColumnRefRawExpr*>(cond->get_param_expr(0))) &&
                 OB_FAIL(candi_cols.push_back(col))) {
        LOG_WARN("failed to push back column expr", K(ret));
      } else if (!cond->get_param_expr(1)->is_query_ref_expr()) {
        // do nothing
      } else if (OB_FALSE_IT(query_ref = static_cast<ObQueryRefRawExpr*>(cond->get_param_expr(1)))) {
      } else if (OB_FAIL(extract_pushdown_cols(*query_ref, cur_table_id, candi_cols))) {
        LOG_WARN("failed to extract pushdown cols", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !found && j < candi_cols.count(); ++j) {
          if (ObOptimizerUtil::find_item(mapped_col_exprs, candi_cols.at(j))) {
            found = true;
            if (OB_FAIL(add_var_to_array_no_dup(semi_join_stmt_ids, parent_stmt.stmt_id_))) {
              LOG_WARN("failed to add var to array no dup", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

/**
 * @brief can_push_dynamic_filter_to_cols
 * check if any dynamic filter of select item can be pushed down to the input column set.
 * NOTE: This is not a strict check, will miss some scenarios with group by and window functions. (fd, equal sets ...)
 * @param col_exprs input column (sub)set of table item
 * @param sel_idxs idxs of select items that can pushdown dynamic filter to input column set
 */
int ObTransformTempTable::can_push_dynamic_filter_to_cols(const ObSelectStmt *stmt,
                                                        const ObIArray<ObColumnRefRawExpr*> &col_exprs,
                                                        uint64_t table_id,
                                                        ObIArray<int64_t> &sel_idxs,
                                                        bool &can_filter_pushdown)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool on_null_side = false;
  ObSEArray<ObRawExpr*, 4> tmp_col_exprs;
  can_filter_pushdown = true;
  sel_idxs.reuse();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(stmt, table_id, on_null_side))) {
    LOG_WARN("failed to check table on null side", K(ret));
  } else if (stmt->is_hierarchical_query() || on_null_side || has_rownum || stmt->has_limit() ||
             stmt->has_sequence() || stmt->is_contains_assignment() || stmt->is_scala_group_by()) {
    can_filter_pushdown = false;
  } else if (OB_FAIL(append(tmp_col_exprs, col_exprs))) {
    LOG_WARN("failed to assign column exprs", K(ret));
  } else if (stmt->get_group_exprs().count() > 0 &&
             OB_FAIL(ObOptimizerUtil::intersect_exprs(tmp_col_exprs, stmt->get_group_exprs(), tmp_col_exprs))) {
    LOG_WARN("failed to intersect exprs", K(ret));
  } else if (stmt->has_window_function()) {
    for (int64_t i = 0; OB_SUCC(ret) && !tmp_col_exprs.empty() && i < stmt->get_window_func_count(); ++i) {
      const ObWinFunRawExpr *win_expr = NULL;
      if (OB_ISNULL(win_expr = stmt->get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function expr is null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::intersect_exprs(tmp_col_exprs,
                                                          win_expr->get_partition_exprs(),
                                                          tmp_col_exprs))) {
        LOG_WARN("failed to intersect expr array", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && can_filter_pushdown && !tmp_col_exprs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_items().count(); ++i) {
      const SelectItem &sel_item = stmt->get_select_item(i);
      if (OB_ISNULL(sel_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item expr is NULL", K(ret));
      } else if (ObOptimizerUtil::find_item(tmp_col_exprs, sel_item.expr_)) {
        if (OB_FAIL(sel_idxs.push_back(i))) {
          LOG_WARN("failed to push back select item idx", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    can_filter_pushdown &= !sel_idxs.empty();
  }
  return ret;
}

/**
 * @brief get_parent_stmt
 * @param table_id refers to the corresponding table id in parent_stmt, if stmt is a generated table query.
 *                 otherwise, it equals to OB_INVALID_ID.
 * @param parent_stmt returns NULL if stmt has no parent
 */
int ObTransformTempTable::get_parent_stmt(const ObSelectStmt *stmt,
                                          const hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                          uint64_t &table_id,
                                          ObDMLStmt *&parent_stmt)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  parent_stmt = NULL;
  ObParentDMLStmt parent;
  uint64_t key = 0;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FALSE_IT(key = reinterpret_cast<uint64_t>(stmt))) {
  } else if (OB_FAIL(parent_map.get_refactored(key, parent))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get value", K(ret));
    }
  } else if (OB_ISNULL(parent_stmt = parent.stmt_)) {
    // parent_stmt is null means stmt is a temp table query
    // do nothing
  } else {
    // find generated table id
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < parent_stmt->get_table_size(); ++i) {
      const TableItem *table_item = NULL;
      if (OB_ISNULL(table_item = parent_stmt->get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is NULL", K(ret));
      } else if (table_item->is_generated_table() && table_item->ref_query_ == stmt) {
        table_id = table_item->table_id_;
        found = true;
      }
    }
  }
  return ret;
}

int ObTransformTempTable::add_semi_to_inner_hint_if_need(ObDMLStmt *copy_root_stmt,
                                                         ObIArray<int64_t> &semi_join_stmt_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(copy_root_stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy_root_stmt is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_join_stmt_ids.count(); ++i) {
      ObDMLStmt *dml_stmt = NULL;
      ObSelectStmt *sub_stmt = NULL;
      ObString qb_name;
      ObSemiToInnerHint *hint = NULL;
      const ObQueryHint *query_hint = NULL;
      if (OB_FAIL(copy_root_stmt->get_stmt_by_stmt_id(semi_join_stmt_ids.at(i), dml_stmt))) {
        LOG_WARN("failed to get stmt by stmt id", K(ret));
      } else if (OB_ISNULL(dml_stmt) || !dml_stmt->is_select_stmt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected stmt", K(ret));
      } else if (OB_FALSE_IT(sub_stmt = static_cast<ObSelectStmt *>(dml_stmt))) {
      } else if (OB_ISNULL(query_hint = sub_stmt->get_stmt_hint().query_hint_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(query_hint));
      } else if (query_hint->has_outline_data() || query_hint->has_user_def_outline()) {
        // do nothing
      } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_SEMI_TO_INNER, hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_ISNULL(hint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(sub_stmt->get_qb_name(qb_name))) {
        LOG_WARN("failed to get qb name", K(ret));
      } else if (OB_FAIL(sub_stmt->get_stmt_hint().normal_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformTempTable::check_inline_temp_table_by_cost(ObDMLStmt *root_stmt,
                                                          TempTableInfo &temp_table_info,
                                                          bool &need_inline)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> inline_stmts;
  ObSEArray<ObSelectStmt*, 4> copy_inline_stmts;
  ObDMLStmt *inline_root = NULL;
  ObSEArray<ObSelectStmt*, 4> materialize_stmts;
  ObSEArray<ObSelectStmt*, 4> copy_materialize_stmts;
  ObSelectStmt *packed_temp_table_query = NULL;
  ObDMLStmt *materialize_root = NULL;
  ObSelectStmt *copy_cte_stmt = NULL;
  ObTryTransHelper trans_helper;
  ObSelectStmt *dummy = NULL;
  ObSEArray<ObSelectStmtPointer, 4> stmt_ptrs;
  ObSEArray<int64_t, 4> choosed_inline_idxs;
  ObSEArray<int64_t, 4> choosed_materialize_idxs;
  OPT_TRACE("try cost based inline");
  if (OB_ISNULL(root_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) ||
      OB_ISNULL(temp_table_info.temp_table_query_) || OB_ISNULL(ctx_->expr_factory_) ||
      temp_table_info.table_items_.count() != temp_table_info.upper_stmts_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpect", K(ret));
  } else if (OB_FAIL(trans_helper.fill_helper(root_stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill helper", K(ret));
  } else if(OB_FAIL(prepare_inline_materialize_stmts(root_stmt,
                                                     temp_table_info,
                                                     stmt_ptrs,
                                                     inline_stmts,
                                                     materialize_stmts,
                                                     packed_temp_table_query))) {
    LOG_WARN("failed to prepare inline materialize stmts", K(ret));
  }
  // to facilitate the copying and replacement of inline_stmts and materialize_stmts,
  // temporarily change the table type from TEMP_TABLE to GENERATED_TABLE.
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_info.table_items_.count(); i++) {
    if (OB_ISNULL(temp_table_info.table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else {
      temp_table_info.table_items_.at(i)->type_ = TableItem::GENERATED_TABLE;
    }
  }
  // deep copy root and replace inline_stmts and materialize_stmts
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copy_and_replace_trans_root(*root_stmt,
                                                inline_stmts,
                                                NULL,
                                                stmt_ptrs,
                                                inline_root,
                                                copy_inline_stmts,
                                                dummy,
                                                false))) {
    LOG_WARN("failed to prepare eval cte cost stmt", K(ret));
  } else if (OB_FAIL(copy_and_replace_trans_root(*root_stmt,
                                                materialize_stmts,
                                                packed_temp_table_query,
                                                stmt_ptrs,
                                                materialize_root,
                                                copy_materialize_stmts,
                                                copy_cte_stmt,
                                                false))) {
    LOG_WARN("failed to prepare eval cte cost stmt", K(ret));
  }
  // evaluate cost
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(evaluate_inline_materialize_costs(root_stmt,
                                                      false,
                                                      inline_root,
                                                      copy_inline_stmts,
                                                      materialize_root,
                                                      copy_materialize_stmts,
                                                      copy_cte_stmt,
                                                      choosed_inline_idxs,
                                                      choosed_materialize_idxs))) {
    LOG_WARN("failed to evaluate inline materialize cost", K(ret));
  } else if (choosed_inline_idxs.count() > 0) {
    need_inline = true;
    OPT_TRACE("accept inline CTE due to cost");
  } else {
    need_inline = false;
    OPT_TRACE("reject inline CTE due to cost");
    ObString qb_name;
    if (OB_FAIL(temp_table_info.temp_table_query_->get_qb_name(qb_name))) {
      LOG_WARN("failed to get qb name", K(ret));
    } else if (OB_FAIL(ctx_->inline_blacklist_.push_back(qb_name))) {
      LOG_WARN("failed to push back qb name", K(ret));
    }
  }
  // recover origin table item type back to TEMP_TABLE
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_info.table_items_.count(); i++) {
    if (OB_ISNULL(temp_table_info.table_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else {
      temp_table_info.table_items_.at(i)->type_ = TableItem::TEMP_TABLE;
    }
  }
  // recover context
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans_helper.recover(root_stmt->get_query_ctx()))) {
    LOG_WARN("failed to recover helper", K(ret));
  }
  // free inline_stmts and materialize_stmts
  // note: inline_root, materialize_root, copy_inline_stmts and copy_materialize_stmts
  //       are already freed after cost evaluation
  for (int64_t i = 0; OB_SUCC(ret) && i < inline_stmts.count(); ++i) {
    if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, inline_stmts.at(i)))) {
      LOG_WARN("failed to free stmt", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < materialize_stmts.count(); ++i) {
    if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, materialize_stmts.at(i)))) {
      LOG_WARN("failed to free stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::prepare_inline_materialize_stmts(ObDMLStmt *root_stmt,
                                                           TempTableInfo &temp_table_info,
                                                           ObIArray<ObSelectStmtPointer> &stmt_ptrs,
                                                           ObIArray<ObSelectStmt *> &inline_stmts,
                                                           ObIArray<ObSelectStmt *> &materialize_stmts,
                                                           ObSelectStmt *&packed_temp_table_query)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *copied_temp_stmt = NULL;
  ObSelectStmt *temp_view = NULL;
  TableItem *temp_table = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->allocator_) || OB_ISNULL(temp_table_info.temp_table_query_) || OB_ISNULL(root_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  }

  // prepare materialize stmts
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                      *ctx_->expr_factory_,
                                                      temp_table_info.temp_table_query_,
                                                      copied_temp_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_ISNULL(copied_temp_stmt) || !copied_temp_stmt->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect stmt", K(ret));
  } else if (OB_FALSE_IT(temp_view = static_cast<ObSelectStmt *>(copied_temp_stmt))) {
    // 在 temp table query 上封一层视图（用以获取 temp table access 的代价）
  } else if (ObTransformUtils::pack_stmt(ctx_, temp_view)) {
    LOG_WARN("failed to create simple view", K(ret));
  } else if (1 != temp_view->get_table_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect one table item in stmt", KPC(temp_view), K(ret));
  } else if (OB_ISNULL(temp_table = temp_view->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!temp_table->is_generated_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect generate table item", KPC(temp_table), K(ret));
  } else if (OB_ISNULL(temp_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(ret));
  } else {
    temp_table->type_ = TableItem::TEMP_TABLE;
    packed_temp_table_query = temp_table->ref_query_;
    if (OB_FAIL(temp_view->generate_view_name(*ctx_->allocator_,
                                              temp_table->table_name_,
                                              true))) {
      LOG_WARN("failed to generate view name", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_info.table_items_.count(); ++i) {
      TableItem *table_item = temp_table_info.table_items_.at(i);
      ObDMLStmt *materialize_stmt = NULL;
      if (i == 0) {
        materialize_stmt = temp_view;
        if (OB_FAIL(materialize_stmts.push_back(static_cast<ObSelectStmt *>(materialize_stmt)))) {
          LOG_WARN("failed to push back stmt", K(ret));
        }
      } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                          *ctx_->expr_factory_,
                                                          temp_view,
                                                          materialize_stmt))) {
        LOG_WARN("failed to deep copy stmt", K(ret));
      } else if (OB_ISNULL(materialize_stmt) || !materialize_stmt->is_select_stmt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect stmt", K(ret));
      } else if (OB_FAIL(materialize_stmt->recursive_adjust_statement_id(ctx_->allocator_,
                                                                        ctx_->src_hash_val_,
                                                                        i))) {
        LOG_WARN("failed to recursive adjust statement id", K(ret));
      } else if (OB_FAIL(materialize_stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      } else if (OB_FAIL(materialize_stmt->update_stmt_table_id(ctx_->allocator_, *temp_view))) {
        LOG_WARN("failed to update table id", K(ret));
      } else if (OB_FAIL(materialize_stmt->formalize_stmt_expr_reference(ctx_->expr_factory_,
                                                                        ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt reference", K(ret));
      } else if (OB_FAIL(materialize_stmts.push_back(static_cast<ObSelectStmt *>(materialize_stmt)))) {
        LOG_WARN("failed to push back stmt", K(ret));
      }
    }
  }

  // prepare inline stmts
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_info.table_items_.count(); ++i) {
    TableItem *table_item = temp_table_info.table_items_.at(i);
    ObSelectStmtPointer stmt_ptr;
    ObDMLStmt *materialize_stmt = NULL;
    ObDMLStmt *inline_stmt = NULL;
    if (OB_ISNULL(table_item) || OB_ISNULL(table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null", K(ret));
    } else if (OB_FAIL(stmt_ptr.add_ref(&table_item->ref_query_))) {
      LOG_WARN("failed to add ref query", K(ret));
    } else if (OB_FAIL(stmt_ptrs.push_back(stmt_ptr))) {
      LOG_WARN("failed to push back stmt ptr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                        *ctx_->expr_factory_,
                                                        temp_table_info.temp_table_query_,
                                                        inline_stmt))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else if (OB_ISNULL(inline_stmt) || !inline_stmt->is_select_stmt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect stmt", K(ret));
    } else if (OB_FAIL(inline_stmt->recursive_adjust_statement_id(ctx_->allocator_,
                                                                  ctx_->src_hash_val_,
                                                                  i))) {
      LOG_WARN("failed to recursive adjust statement id", K(ret));
    } else if (OB_FAIL(inline_stmt->update_stmt_table_id(ctx_->allocator_, *temp_table_info.temp_table_query_))) {
      LOG_WARN("failed to update table id", K(ret));
    } else if (OB_FAIL(inline_stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(inline_stmt->formalize_stmt_expr_reference(ctx_->expr_factory_,
                                                                  ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt reference", K(ret));
    } else if (OB_FAIL(inline_stmts.push_back(static_cast<ObSelectStmt *>(inline_stmt)))) {
      LOG_WARN("failed to push back stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::evaluate_inline_materialize_costs(ObDMLStmt *origin_root,
                                                            bool need_check_plan,
                                                            ObDMLStmt *inline_root,
                                                            ObIArray<ObSelectStmt *> &copy_inline_stmts,
                                                            ObDMLStmt *materialize_root,
                                                            ObIArray<ObSelectStmt *> &copy_materialize_stmts,
                                                            ObSelectStmt *copy_cte_stmt,
                                                            ObIArray<int64_t> &choosed_inline_idxs,
                                                            ObIArray<int64_t> &choosed_materialize_idxs)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<uint64_t, ObParentDMLStmt> parent_map;
  ObSEArray<ObSelectStmt*, 8> dummy;
  ObSEArray<int64_t, 4> semi_join_stmt_ids;
  bool check_global_cost = false;
  if (OB_ISNULL(inline_root) || OB_ISNULL(materialize_root) || OB_ISNULL(ctx_) ||
      OB_ISNULL(copy_cte_stmt) || OB_UNLIKELY(copy_inline_stmts.count() != copy_materialize_stmts.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect error", K(ret));
  } else if (OB_FAIL(parent_map.create(128, "TempTable"))) {
    LOG_WARN("failed to init stmt map", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_all_child_stmts(inline_root, dummy, &parent_map))) {
    LOG_WARN("failed to get all child stmts", K(ret));
  } else if (OB_FAIL(need_check_global_cte_cost(inline_root,
                                                copy_inline_stmts,
                                                parent_map,
                                                copy_cte_stmt,
                                                semi_join_stmt_ids,
                                                check_global_cost))) {
    LOG_WARN("failed to check need global cost check", K(ret));
  } else if (OB_FAIL(parent_map.destroy())) {
    LOG_WARN("failed to destroy map", K(ret));
  }
  // evaluate cost globally
  if (OB_SUCC(ret) && check_global_cost) {
    double inline_global_cost = 0.0;
    double materialize_global_cost = 0.0;
    if (OB_FAIL(evaluate_cte_cost_globally(origin_root,
                                           inline_root,
                                           semi_join_stmt_ids,
                                           need_check_plan,
                                           inline_global_cost))) {
      LOG_WARN("failed to evaluate cost for the origin stmt", K(ret));
    } else if (OB_FAIL(evaluate_cte_cost_globally(origin_root,
                                                  materialize_root,
                                                  semi_join_stmt_ids,
                                                  false,
                                                  materialize_global_cost))) {
      LOG_WARN("failed to evaluate cost for the transform stmt", K(ret));
    } else {
      OPT_TRACE("The global cost of inline stmt:", inline_global_cost);
      OPT_TRACE("The global cost of materialize stmt:", materialize_global_cost);
      if (materialize_global_cost >= inline_global_cost) {
        for (int64_t i = 0; OB_SUCC(ret) && i < copy_inline_stmts.count(); ++i) {
          if (OB_FAIL(choosed_inline_idxs.push_back(i))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        OPT_TRACE("choose `inline` due to global cost check");
        LOG_TRACE("choose `inline` due to global cost check", K(inline_global_cost), K(materialize_global_cost));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < copy_materialize_stmts.count(); ++i) {
          if (OB_FAIL(choosed_materialize_idxs.push_back(i))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        OPT_TRACE("choose `materialize` due to global cost check");
        LOG_TRACE("choose `materialize` due to global cost check", K(inline_global_cost), K(materialize_global_cost));
      }
    }
  }
  // evaluate cost partially
  if (OB_SUCC(ret) && !check_global_cost) {
    ObSEArray<double, 2> inline_costs;
    ObSEArray<double, 2> materialize_costs;
    double dummy = 0.0;
    double cte_cost = 0.0;
    double cte_profit = 0.0;
    if (OB_FAIL(evaluate_cte_cost_partially(inline_root,
                                            copy_inline_stmts,
                                            inline_costs,
                                            NULL,
                                            dummy))) {
      LOG_WARN("failed to evaluate cost for the origin stmt", K(ret));
    } else if (OB_FAIL(evaluate_cte_cost_partially(materialize_root,
                                                  copy_materialize_stmts,
                                                  materialize_costs,
                                                  copy_cte_stmt,
                                                  cte_cost))) {
      LOG_WARN("failed to evaluate cost for the transform stmt", K(ret));
    } else if (OB_UNLIKELY(inline_costs.count() != materialize_costs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected array size", K(inline_costs), K(materialize_costs));
    } else {
      // accumulate materialize profit
      for (int64_t i = 0; OB_SUCC(ret) && i < inline_costs.count(); i++) {
        if (inline_costs.at(i) > materialize_costs.at(i)) {
          cte_profit += inline_costs.at(i) - materialize_costs.at(i);
          if (OB_FAIL(choosed_materialize_idxs.push_back(i))) {
            LOG_WARN("failed to push back", K(ret));
          }
        } else if (OB_FAIL(choosed_inline_idxs.push_back(i))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      OPT_TRACE("The cost of materialize cte :", cte_cost);
      OPT_TRACE("The profit of materialize cte :", cte_profit);
      // inline all references if the profit of materialize cte is less than the cost
      if (OB_FAIL(ret)) {
      } else if (cte_profit <= cte_cost || choosed_materialize_idxs.count() <= 1) {
        if (OB_FAIL(append(choosed_inline_idxs, choosed_materialize_idxs))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          choosed_materialize_idxs.reset();
          OPT_TRACE("choose `inline` due to partial cost check");
          LOG_TRACE("choose `inline` due to partial cost check", K(cte_cost), K(cte_profit));
        }
      } else {
        OPT_TRACE("choose `materialize` due to partial cost check");
        LOG_TRACE("choose `materialize` due to partial cost check", K(cte_cost), K(cte_profit));
      }
    }
  }
  return ret;
}

int ObTransformTempTable::prepare_eval_cte_cost_stmt(ObDMLStmt &root_stmt,
                                                     ObIArray<ObSelectStmt *> &trans_stmts,
                                                     ObSelectStmt *cte_query,
                                                     ObIArray<ObSelectStmtPointer> &stmt_ptrs,
                                                     ObDMLStmt *&copied_stmt,
                                                     ObIArray<ObSelectStmt *> &copied_trans_stmts,
                                                     ObSelectStmt *&copied_cte_query,
                                                     bool is_trans_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 2> old_temp_table_stmts;
  ObSEArray<ObSelectStmt*, 2> new_temp_table_stmts;
  ObSEArray<ObSelectStmt*, 2> origin_stmts;
  ObDMLStmt *copied_trans_stmt = NULL;
  ObString cur_qb_name;
  ObDMLStmt *temp = NULL;
  hash::ObHashMap<uint64_t, ObDMLStmt *> copy_stmt_map;
  ObDMLStmt *dml_stmt_val = NULL;
  if (OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid()) || OB_ISNULL(root_stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(root_stmt.get_query_ctx()));
  } else if (OB_FAIL(adjust_transformed_stmt(stmt_ptrs, trans_stmts, &origin_stmts))) {
    LOG_WARN("failed to adjust transformed stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                      &root_stmt, copied_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(deep_copy_temp_table(*copied_stmt,
                                          *ctx_->stmt_factory_,
                                          *ctx_->expr_factory_,
                                          old_temp_table_stmts,
                                          new_temp_table_stmts))) {
    LOG_WARN("failed to deep copy temp table", K(ret));
  } else if (OB_FAIL(copy_stmt_map.create(128, "TempTable"))) {
    LOG_WARN("failed to init stmt map", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_stmt_map_after_copy(&root_stmt, copied_stmt, copy_stmt_map))) {
    LOG_WARN("failed to get stmt map", K(ret));
  } else if (NULL != cte_query) {
    uint64_t key = reinterpret_cast<uint64_t>(cte_query);
    if (OB_FAIL(copy_stmt_map.get_refactored(key, dml_stmt_val))) {
      LOG_WARN("failed to get hash map", K(ret));
    } else if (OB_ISNULL(dml_stmt_val) || OB_UNLIKELY(!dml_stmt_val->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), KPC(dml_stmt_val));
    } else {
      copied_cte_query = static_cast<ObSelectStmt *>(dml_stmt_val);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_stmts.count(); i ++) {
    dml_stmt_val = NULL;
    uint64_t key = reinterpret_cast<uint64_t>(trans_stmts.at(i));
    if (OB_FAIL(copy_stmt_map.get_refactored(key, dml_stmt_val))) {
      LOG_WARN("failed to get stmt from hash map", K(ret));
    } else if (OB_ISNULL(dml_stmt_val) || OB_UNLIKELY(!dml_stmt_val->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), KPC(dml_stmt_val));
    } else if (OB_FAIL(copied_trans_stmts.push_back(static_cast<ObSelectStmt *>(dml_stmt_val)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copy_stmt_map.destroy())) {
    LOG_WARN("failed to destroy map", K(ret));
  } else if (OB_FAIL(adjust_transformed_stmt(stmt_ptrs, origin_stmts, NULL))) {
      LOG_WARN("failed to adjust transformed stmt", K(ret));
  } else if (is_trans_stmt) {
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_stmts.count(); i ++) {
      ObSelectStmt *stmt = trans_stmts.at(i);
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null stmt", K(ret));
      } else if (OB_FAIL(stmt->get_qb_name(cur_qb_name))) {
        LOG_WARN("failed to get qb name", K(ret));
      } else if (OB_FAIL(ObTransformRule::construct_transform_hint(*stmt, NULL))) {
        // To get happended transform rule by outline_trans_hints_ during evaluating cost,
        // here construct and add hint for cost based transform rule.
        // Added hint only filled qb name parameter.
        LOG_WARN("failed to construct transform hint", K(ret), K(stmt->get_stmt_id()),
                                                      K(ctx_->src_qb_name_), K(get_transformer_type()));
      } else if (cur_qb_name != ctx_->src_qb_name_) {
        ctx_->src_qb_name_ = cur_qb_name;
      } else if (OB_FAIL(copied_stmt->get_stmt_by_stmt_id(stmt->get_stmt_id(), copied_trans_stmt))) {
        LOG_WARN("failed to get stmt by stmt id", K(ret), K(stmt->get_stmt_id()), K(*copied_stmt));
      } else if(OB_ISNULL(copied_trans_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret), K(stmt->get_stmt_id()), K(*copied_stmt));
      } else if (OB_FAIL(copied_trans_stmt->adjust_qb_name(ctx_->allocator_,
                                                           ctx_->src_qb_name_,
                                                           ctx_->src_hash_val_))) {
        LOG_WARN("failed to adjust statement id", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copied_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(copied_stmt->formalize_stmt_expr_reference(ctx_->expr_factory_,
                                                                ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformTempTable::copy_and_replace_trans_root(ObDMLStmt &root_stmt,
                                                      ObIArray<ObSelectStmt *> &trans_stmts,
                                                      ObSelectStmt *cte_query,
                                                      ObIArray<ObSelectStmtPointer> &stmt_ptrs,
                                                      ObDMLStmt *&copied_stmt,
                                                      ObIArray<ObSelectStmt *> &copied_trans_stmts,
                                                      ObSelectStmt *&copied_cte_query,
                                                      bool is_trans_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 2> old_temp_table_stmts;
  ObSEArray<ObSelectStmt*, 2> new_temp_table_stmts;
  ObSEArray<ObSelectStmt*, 2> origin_stmts;
  ObDMLStmt *copied_trans_stmt = NULL;
  ObString cur_qb_name;
  ObDMLStmt *temp = NULL;
  hash::ObHashMap<uint64_t, ObDMLStmt *> copy_stmt_map;
  ObDMLStmt *dml_stmt_val = NULL;
  if (OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid()) || OB_ISNULL(root_stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(root_stmt.get_query_ctx()));
  } else if (OB_FAIL(adjust_transformed_stmt(stmt_ptrs, trans_stmts, &origin_stmts))) {
    LOG_WARN("failed to adjust transformed stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                      &root_stmt, copied_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(deep_copy_temp_table(*copied_stmt,
                                          *ctx_->stmt_factory_,
                                          *ctx_->expr_factory_,
                                          old_temp_table_stmts,
                                          new_temp_table_stmts))) {
    LOG_WARN("failed to deep copy temp table", K(ret));
  } else if (OB_FAIL(copy_stmt_map.create(128, "TempTable"))) {
    LOG_WARN("failed to init stmt map", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_stmt_map_after_copy(&root_stmt, copied_stmt, copy_stmt_map))) {
    LOG_WARN("failed to get stmt map", K(ret));
  } else if (NULL != cte_query) {
    uint64_t key = reinterpret_cast<uint64_t>(cte_query);
    if (OB_FAIL(copy_stmt_map.get_refactored(key, dml_stmt_val))) {
      LOG_WARN("failed to get hash map", K(ret));
    } else if (OB_ISNULL(dml_stmt_val) || OB_UNLIKELY(!dml_stmt_val->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), KPC(dml_stmt_val));
    } else {
      copied_cte_query = static_cast<ObSelectStmt *>(dml_stmt_val);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_stmts.count(); i ++) {
    dml_stmt_val = NULL;
    uint64_t key = reinterpret_cast<uint64_t>(trans_stmts.at(i));
    if (OB_FAIL(copy_stmt_map.get_refactored(key, dml_stmt_val))) {
      LOG_WARN("failed to get stmt from hash map", K(ret));
    } else if (OB_ISNULL(dml_stmt_val) || OB_UNLIKELY(!dml_stmt_val->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), KPC(dml_stmt_val));
    } else if (OB_FAIL(copied_trans_stmts.push_back(static_cast<ObSelectStmt *>(dml_stmt_val)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copy_stmt_map.destroy())) {
    LOG_WARN("failed to destroy map", K(ret));
  } else if (OB_FAIL(adjust_transformed_stmt(stmt_ptrs, origin_stmts, NULL))) {
      LOG_WARN("failed to adjust transformed stmt", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copied_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(copied_stmt->formalize_stmt_expr_reference(ctx_->expr_factory_,
                                                                ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformTempTable::pick_out_stmts_in_blacklist(const ObIArray<ObString> &blacklist,
                                                      ObIArray<ObSelectStmt *> &origin_stmts,
                                                      ObIArray<ObSelectStmt *> &trans_stmts,
                                                      ObIArray<ObSelectStmtPointer> &stmt_ptrs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> picked_origin_stmts;
  ObSEArray<ObSelectStmt*, 4> picked_trans_stmts;
  ObSEArray<ObSelectStmtPointer, 4> picked_stmt_ptrs;
  if (OB_UNLIKELY(origin_stmts.count() != trans_stmts.count() ||
                  origin_stmts.count() != stmt_ptrs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt count", K(ret), K(origin_stmts.count()), K(trans_stmts.count()), K(stmt_ptrs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_stmts.count(); i++) {
    ObSelectStmt *origin_stmt = origin_stmts.at(i);
    ObString qb_name;
    if (OB_ISNULL(origin_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(origin_stmt->get_qb_name(qb_name))) {
      LOG_WARN("failed to get qb name", K(ret));
    } else if (ObOptimizerUtil::find_item(blacklist, qb_name)) {
      //do nothing
    } else if (OB_FAIL(picked_origin_stmts.push_back(origin_stmts.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(picked_trans_stmts.push_back(trans_stmts.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(picked_stmt_ptrs.push_back(stmt_ptrs.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_FAIL(ret) || origin_stmts.count() == picked_origin_stmts.count()) {
    // do nothing
  } else if (OB_FAIL(origin_stmts.assign(picked_origin_stmts))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(trans_stmts.assign(picked_trans_stmts))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(stmt_ptrs.assign(picked_stmt_ptrs))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ObTransformTempTable::gather_materialize_blacklist(ObLogicalOperator *op,
                                                       ObIArray<uint64_t> &blacklist)
{
  int ret = OB_SUCCESS;
  ObLogJoin *join_op = NULL;
  ObLogSubPlanScan *subplan_scan_op = NULL;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (log_op_def::LOG_JOIN != op->get_type()) {
    // do nothing
  } else if (OB_FALSE_IT(join_op = static_cast<ObLogJoin*>(op))) {
  } else if (!join_op->is_nlj_with_param_down()) {
    // do nothing
  } else if (OB_ISNULL(join_op->get_right_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (join_op->get_right_table()->get_type() != log_op_def::LOG_SUBPLAN_SCAN) {
    // do nothing
  } else if (OB_FALSE_IT(subplan_scan_op = static_cast<ObLogSubPlanScan*>(join_op->get_right_table()))) {
  } else if (OB_FAIL(blacklist.push_back(subplan_scan_op->get_subquery_id()))) {
    LOG_WARN("failed to push back", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    if (OB_FAIL(SMART_CALL(gather_materialize_blacklist(op->get_child(i), blacklist)))) {
      LOG_WARN("failed to inner gather materialize blacklist", K(ret));
    }
  }
  return ret;
}

int ObTransformTempTable::get_all_view_stmts(ObDMLStmt *stmt,
                                             ObIArray<ObSelectStmt*> &view_stmts)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (stmt->is_set_stmt()) {
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    } else if (OB_FAIL(append_array_no_dup(view_stmts, child_stmts))) {
      LOG_WARN("failed to append array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_all_view_stmts(child_stmts.at(i), view_stmts)))) {
        LOG_WARN("failed to get all view stmts", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); i++) {
      if (OB_ISNULL(stmt->get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!stmt->get_table_item(i)->is_generated_table() &&
                 !stmt->get_table_item(i)->is_temp_table()) {
        // do nothing
      } else if (OB_FAIL(add_var_to_array_no_dup(view_stmts, stmt->get_table_item(i)->ref_query_))) {
        LOG_WARN("failed to add var to array", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(child_stmts, stmt->get_table_item(i)->ref_query_))) {
        LOG_WARN("failed to add var to array", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_all_view_stmts(child_stmts.at(i), view_stmts)))) {
        LOG_WARN("failed to get all view stmts", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformTempTable::check_stmt_in_blacklist(const ObSelectStmt *stmt,
                                                 const ObIArray<ObString> &blacklist,
                                                 bool &in_blacklist)
{
  int ret = OB_SUCCESS;
  in_blacklist = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else {
    const ObIArray<QbNames> &stmt_id_map = stmt->get_query_ctx()->get_query_hint().stmt_id_map_;
    if (stmt->get_stmt_id() < 0 || stmt->get_stmt_id() >= stmt_id_map.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect stmt id", K(ret), K(stmt->get_stmt_id()), K(stmt_id_map.count()));
    } else {
      const ObIArray<ObString> &qb_names = stmt_id_map.at(stmt->get_stmt_id()).qb_names_;
      for (int64_t i = 0; OB_SUCC(ret) && !in_blacklist && i < qb_names.count(); i++) {
        if (ObOptimizerUtil::find_item(blacklist, qb_names.at(i))) {
          in_blacklist = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformTempTable::check_evaluate_after_transform(ObDMLStmt *root_stmt,
                                                         ObIArray<ObSelectStmt *> &stmts,
                                                         bool &can_eval)
{
  int ret = OB_SUCCESS;
  ObSEArray<TempTableInfo, 4> root_cte_infos;
  ObSEArray<ObSelectStmt*, 4> all_ctes;
  can_eval = true;
  if (OB_ISNULL(root_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(root_stmt->collect_temp_table_infos(root_cte_infos))) {
    LOG_WARN("failed to collect temp table infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < root_cte_infos.count(); ++i) {
      if (OB_FAIL(all_ctes.push_back(root_cte_infos.at(i).temp_table_query_))) {
        LOG_WARN("failed to add var to array", K(ret));
      }
    }
  }
  for (int64_t i = 0; can_eval && OB_SUCC(ret) && i < stmts.count(); ++i) {
    ObSEArray<TempTableInfo, 4> tmp_cte_infos;
    if (OB_ISNULL(stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(stmts.at(i)->collect_temp_table_infos(tmp_cte_infos))) {
      LOG_WARN("failed to collect temp table infos", K(ret));
    } else {
      for (int64_t j = 0; can_eval && OB_SUCC(ret) && j < tmp_cte_infos.count(); ++j) {
        if (!ObOptimizerUtil::find_item(all_ctes, tmp_cte_infos.at(j).temp_table_query_)) {
          can_eval = false;
        }
      }
    }
  }
  return ret;
}

}//namespace sql
}//namespace oceanbase
