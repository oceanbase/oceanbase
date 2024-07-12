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
#include "lib/allocator/ob_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_common_utility.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_predicate_deduce.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_log_plan.h"
#include "ob_transform_min_max.h"

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
      if (OB_FAIL(expand_temp_table(temp_table_infos, is_happened))) {
        LOG_WARN("failed to expand temp table", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("expand temp table:", is_happened);
        LOG_TRACE("succeed to expand temp table",  K(is_happened));
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
  ObArray<TempTableInfo> temp_table_infos;
  hash::ObHashMap<uint64_t, ObParentDMLStmt> parent_map;
  trans_happened = false;
  bool enable_temp_table_transform = false;
  bool force_temp_table_inline = false;
  bool has_hint = false;
  bool is_hint_enabled = false;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx()) ||
      OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ctx_), K(ret));
  } else if (OB_FAIL(session_info->is_temp_table_transformation_enabled(enable_temp_table_transform))) {
    LOG_WARN("failed to check temp table transform enabled", K(ret));
  } else if (OB_FAIL(session_info->is_force_temp_table_inline(force_temp_table_inline))) {
    LOG_WARN("failed to check temp table force inline", K(ret));
  } else if (OB_FAIL(stmt->get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
              ObOptParamHint::XSOLAPI_GENERATE_WITH_CLAUSE, is_hint_enabled, has_hint))) {
    LOG_WARN("failed to check has opt param", K(ret));
  } else if (has_hint) {
    enable_temp_table_transform = is_hint_enabled;
  }
  if (OB_FAIL(ret)) {
  } else if (ctx_->is_set_stmt_oversize_) {
    OPT_TRACE("stmt containt oversize set stmt");
  } else if (!enable_temp_table_transform || force_temp_table_inline) {
    OPT_TRACE("session variable disable temp table transform");
  } else if (stmt->has_for_update()) {
    OPT_TRACE("stmt has for update, can not extract CTE");
  } else if (OB_FAIL(parent_map.create(128, "TempTable"))) {
    LOG_WARN("failed to init stmt map", K(ret));
  } else if (!ObOptimizerUtil::find_item(ctx_->temp_table_ignore_stmts_, stmt) &&
             OB_FAIL(ObTransformUtils::get_all_child_stmts(stmt, child_stmts, &parent_map, &ctx_->temp_table_ignore_stmts_))) {
    LOG_WARN("failed to get all child stmts", K(ret));
  } else if (OB_FAIL(get_non_correlated_subquery(stmt, non_correlated_stmts))) {
    LOG_WARN("failed to get non correlated subquery", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::intersect(child_stmts, non_correlated_stmts, child_stmts))) {
    LOG_WARN("failed to intersect child stmts", K(ret));
  } else if (OB_FAIL(extract_common_subquery_as_cte(stmt, child_stmts, parent_map, trans_happened))) {
    LOG_WARN("failed to extract common subquery as cte", K(ret));
  } else if (OB_FAIL(parent_map.destroy())) {
    LOG_WARN("failed to destroy map", K(ret));
  }
  return ret;
}

/**
 * @brief expand_temp_table
 * 如果temp table只被引用一次或者temp table是一个简单的查询
 * 例如单表查询，那么需要展开temp table，还原成generate table
 */
int ObTransformTempTable::expand_temp_table(ObIArray<TempTableInfo> &temp_table_info,
                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool system_force_inline_cte = false;
  bool system_force_materialize_cte = false;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) ||
      OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ctx_), K(ret));
  } else if (OB_FAIL(session_info->is_force_temp_table_inline(system_force_inline_cte))) {
    LOG_WARN("failed to check temp table force inline", K(ret));
  } else if (OB_FAIL(session_info->is_force_temp_table_materialize(system_force_materialize_cte))) {
    LOG_WARN("failed to check temp table force materialize", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_info.count(); ++i) {
    TempTableInfo &helper = temp_table_info.at(i);
    bool can_materia = false;
    bool force_materia = false;
    bool force_inline = false;
    bool is_oversize_stmt = false;
    bool has_for_update = false;
    int64_t stmt_size = 0;
    bool need_expand = false;
    bool can_expand = true;
    OPT_TRACE("try to expand temp table:", helper.temp_table_query_);
    if (OB_ISNULL(helper.temp_table_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", K(helper), K(ret));
    } else if (OB_FAIL(check_stmt_size(helper.temp_table_query_, stmt_size, is_oversize_stmt))) {
      LOG_WARN("check stmt size failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_expand_temp_table_valid(helper.temp_table_query_, can_expand))) {
      LOG_WARN("failed to check expand temp table valid", K(ret));
    } else if (OB_FAIL(check_has_for_update(helper, has_for_update))) {
      LOG_WARN("failed to check has for update", K(ret));
    } else if (!can_expand) {
      // do nothing
      OPT_TRACE("CTE can not be expanded");
    } else if (OB_FAIL(check_hint_allowed_trans(*helper.temp_table_query_,
                                                force_inline,
                                                force_materia))) {
      LOG_WARN("failed to check force materialize", K(ret));
    } else if (force_inline) {
      need_expand = true;
      OPT_TRACE("hint force inline CTE");
    } else if (lib::is_oracle_mode() && has_for_update) {
      need_expand = true;
      OPT_TRACE("stmt has FOR UPDATE, force inline CTE");
    } else if (force_materia) {
      //do nothing
      OPT_TRACE("hint force materialize CTE");
    } else if (is_oversize_stmt) {
      //do nothing
      OPT_TRACE("CTE too large to expand");
    } else if (system_force_materialize_cte) {
      //do nothing
      OPT_TRACE("system variable force materialize CTE");
    } else if (system_force_inline_cte) {
      need_expand = true;
      OPT_TRACE("system variable force inline CTE");
    } else if (1 == helper.table_items_.count()) {
      need_expand = true;
      OPT_TRACE("CTE`s refer once, force inline");
    } else if (OB_FAIL(check_stmt_can_materialize(helper.temp_table_query_, true, can_materia))) {
      LOG_WARN("failed to check extract cte valid", K(ret));
    } else if (!can_materia) {
      need_expand = true;
      OPT_TRACE("transform rule force inline CTE");
    }
    if (OB_SUCC(ret) && need_expand) {
      //深拷贝每一份查询，还原成generate table
      ObDMLStmt *orig_stmt = helper.temp_table_query_;
      if (OB_FAIL(ObTransformUtils::expand_temp_table(ctx_, helper))) {
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
 * @brief extract_common_subquery_as_cte
 * 比较当前stmt的所有child stmt，
 * 把所有相似的stmt分为一组，抽离最大的公共部分作为temp table
 */
int ObTransformTempTable::extract_common_subquery_as_cte(ObDMLStmt *stmt,
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
    if (OB_FAIL(inner_extract_common_subquery_as_cte(*stmt,
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
 * @brief inner_extract_common_subquery_as_cte
 * stmt之间两两比较，分成多个相似组，
 * 对每组相似stmt创建temp table
 */
int ObTransformTempTable::inner_extract_common_subquery_as_cte(ObDMLStmt &root_stmt,
                                                               ObIArray<ObSelectStmt*> &stmts,
                                                               hash::ObHashMap<uint64_t, ObParentDMLStmt> &parent_map,
                                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObStmtMapInfo map_info;
  QueryRelation relation;
  typedef ObSEArray<StmtCompareHelper, 8> StmtCompareHelperArray;
  SMART_VAR(StmtCompareHelperArray, compare_info) {
    //计算相似stmt分组
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
      for (int64_t j = 0; OB_SUCC(ret) && !find_similar && j < compare_info.count(); ++j) {
        map_info.reset();
        bool has_stmt = false;
        bool is_valid = false;
        StmtCompareHelper &helper = compare_info.at(j);
        bool check_basic_similarity = !helper.hint_force_stmt_set_.empty() &&
                                      helper.hint_force_stmt_set_.has_qb_name(stmt);
        if (OB_FAIL(check_has_stmt(helper.similar_stmts_,
                                   stmt,
                                   parent_map,
                                   has_stmt))) {
          LOG_WARN("failed to check has stmt", K(ret));
        } else if (has_stmt) {
          //do nothing
        } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(helper.stmt_,
                                                                  stmt,
                                                                  map_info,
                                                                  relation))) {
          LOG_WARN("failed to check stmt containment", K(ret));
        } else if (OB_FAIL(check_stmt_can_extract_temp_table(helper.stmt_,
                                                             stmt,
                                                             map_info,
                                                             relation,
                                                             check_basic_similarity,
                                                             is_valid))) {
          LOG_WARN("failed to check is similar stmt");
        } else if (!is_valid) {
          //do nothing
        } else if (OB_FAIL(helper.similar_stmts_.push_back(stmt))) {
          LOG_WARN("failed to push back stmt", K(ret));
        } else if (OB_FAIL(helper.stmt_map_infos_.push_back(map_info))) {
          LOG_WARN("failed to push back map info", K(ret));
        } else {
          find_similar = true;
        }
      }
      if (OB_SUCC(ret) && !find_similar) {
        SMART_VAR(StmtCompareHelper, helper) {
          map_info.reset();
          bool force_no_trans = false;
          QbNameList qb_names;
          if (OB_FAIL(get_hint_force_set(root_stmt,
                                         *stmt, 
                                         qb_names, 
                                         force_no_trans))) {
            LOG_WARN("failed to get hint set", K(ret));
          } else if (force_no_trans) {
            //do nothing
            OPT_TRACE("hint reject materialize:", stmt);
          } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(stmt,
                                                                    stmt,
                                                                    map_info,
                                                                    relation))) {
            LOG_WARN("failed to check stmt containment", K(ret));
          } else if (OB_FAIL(helper.similar_stmts_.push_back(stmt))) {
            LOG_WARN("failed to push back stmt", K(ret));
          } else if (OB_FAIL(helper.stmt_map_infos_.push_back(map_info))) {
            LOG_WARN("failed to push back map info", K(ret));
          } else if (OB_FAIL(helper.hint_force_stmt_set_.assign(qb_names))) {
            LOG_WARN("failed to assign qb names", K(ret));
          } else if (OB_FALSE_IT(helper.stmt_ = stmt)) {
          } else if (OB_FAIL(compare_info.push_back(helper))) {
            LOG_WARN("failed to push back compare info", K(ret));
          }
        }
      }
    }
    //对每组相似stmt创建temp table
    for (int64_t i = 0; OB_SUCC(ret) && i < compare_info.count(); ++i) {
      StmtCompareHelper &helper = compare_info.at(i);
      bool is_happened = false;
      OPT_TRACE("try to materialize:", helper.stmt_);
      if (!helper.hint_force_stmt_set_.empty() &&
          !helper.hint_force_stmt_set_.is_subset(helper.similar_stmts_)) {
        //hint forbid, do nothing
        OPT_TRACE("hint reject transform");
      } else if (helper.hint_force_stmt_set_.empty() && 
                (helper.similar_stmts_.count() < 2)) {
        OPT_TRACE("no other similar stmts");
        //do nothing
      } else if (OB_FAIL(create_temp_table(root_stmt, helper, parent_map, is_happened))) {
        LOG_WARN("failed to create temp table", K(ret));
      } else {
        trans_happened |= is_happened;
      }
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
    if (OB_FAIL(accept_cte_transform(root_stmt, temp_table,
                                     origin_stmts, trans_stmts,
                                     accept_stmts, parent_map,
                                     !compare_info.hint_force_stmt_set_.empty(),
                                     trans_happened))) {
      LOG_WARN("failed to accept transform", K(ret));
    } else if (!trans_happened) {
      if (OB_FAIL(try_trans_helper.recover(root_stmt.get_query_ctx()))) {
        LOG_WARN("failed to recover params", K(ret));
      }
    } else if (OB_FAIL(append(ctx_->equal_param_constraints_, common_map_info.equal_param_map_))) {
      LOG_WARN("failed to append equal param constraints", K(ret));
    } else if (OB_FAIL(add_materialize_stmts(accept_stmts))) {
      LOG_WARN("failed to add stmts", K(ret));
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
  //视图的select items
  ObSEArray<ObRawExpr*, 16> view_select_list;
  //视图的select items转换为temp table对应的select items
  ObSEArray<ObRawExpr*, 16> new_select_list;
  //temp table的select items
  ObSEArray<ObRawExpr*, 16> temp_table_select_list;
  //视图的column items
  ObSEArray<ObRawExpr*, 16> view_column_list;
  //temp table的column items
  ObSEArray<ObRawExpr*, 16> temp_table_column_list;
  //视图的column item转换为temp table对应的column items
  ObSEArray<ObRawExpr*, 16> new_column_list;
  //不存在于temp table中的column item
  ObSEArray<ColumnItem, 16> new_column_items;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSelectStmt *view = NULL;
  SMART_VAR(ObStmtCompareContext, context) {
    if (OB_ISNULL(parent_stmt) || OB_ISNULL(temp_table_query) ||
        OB_ISNULL(view_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(ret));
    } else if (!view_table->is_generated_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect generate table", KPC(view_table), K(ret));
    } else if (OB_ISNULL(view = view_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", KPC(view_table), K(ret));
    } else if (OB_FAIL(view->get_select_exprs(view_select_list))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(view->get_column_exprs(view_column_list))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(temp_table_query->get_select_exprs(temp_table_select_list))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(temp_table_query->get_column_exprs(temp_table_column_list))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else {
      context.init(temp_table_query, view, map_info,
                  &parent_stmt->get_query_ctx()->calculable_items_);
    }
    //找到对应的column item，不存在于temp table的column需要添加到temp table
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
        if (!view_column->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect column ref expr", KPC(view_column), K(ret));
        } else if (OB_ISNULL(column_item = view->get_column_item_by_id(col_ref->get_table_id(),
                                                                      col_ref->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null column item", K(ret));
        } else if (OB_FAIL(get_map_table_id(view,
                                            temp_table_query,
                                            map_info,
                                            col_ref->get_table_id(),
                                            table_id))) {
          LOG_WARN("failed to get map table id", K(ret));
        } else if (OB_FALSE_IT(column_item->table_id_ = table_id)) {
        } else if (OB_FALSE_IT(col_ref->set_table_id(table_id))) {
        } else if (OB_FAIL(new_column_items.push_back(*column_item))) {
          LOG_WARN("failed to push back column item", K(ret));
        } else if (OB_FAIL(new_column_list.push_back(view_column))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_ISNULL(table = temp_table_query->get_table_item_by_id(table_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null table item", K(ret));
        } else {
          col_ref->set_table_name(table->get_table_name());
        }
      }
    }
    //添加新的column item
    if (OB_SUCC(ret) && !new_column_items.empty()) {
      if (OB_FAIL(temp_table_query->add_column_item(new_column_items))) {
        LOG_WARN("failed to add table item", K(ret));
      }
    }
    //找到不同的select item
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
        ObSEArray<ObAggFunRawExpr*, 8> aggr_items;
        ObSEArray<ObWinFunRawExpr*, 8> win_func_exprs;
        if (OB_FAIL(ObTransformUtils::replace_expr(view_column_list, new_column_list, view_select))) {
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
    //为temp table创建新的select item，并替换parent stmt的引用
    if (OB_SUCC(ret)) {
      ObSEArray<ObRawExpr*, 16> new_column_exprs;
      view_table->ref_query_ = temp_table_query;
      if (OB_FALSE_IT(parent_stmt->clear_column_items())) {
      } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                                  *view_table,
                                                                  parent_stmt,
                                                                  new_select_list,
                                                                  new_column_exprs))) {
        LOG_WARN("failed to create column for view", K(ret));
      } else if (OB_FAIL(parent_stmt->replace_relation_exprs(old_column_exprs, new_column_exprs))) {
        LOG_WARN("failed to replace inner stmt expr", K(ret));
      } else if (OB_FAIL(temp_table_query->adjust_subquery_list())) {
        LOG_WARN("failed to adjust subquery list", K(ret));
      }
    }
  } // end smart var
  return ret;
}

/**
 * @brief get_map_table_id
 * 找到视图中的table id对应temp table中的table id
 */
int ObTransformTempTable::get_map_table_id(ObSelectStmt *view,
                                          ObSelectStmt *temp_table_query,
                                          ObStmtMapInfo& map_info,
                                          const uint64_t &view_table_id,
                                          uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(view) || OB_ISNULL(temp_table_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  bool find = false;
  int64_t idx = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < view->get_table_size(); ++i) {
    TableItem *table = view->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (view_table_id == table->table_id_) {
      find =  true;
      idx = i;
    }
  }
  if (OB_SUCC(ret) && (!find || OB_INVALID_ID == idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table shoud be found in view" ,K(view_table_id), K(ret));
  }
  find = false;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < map_info.table_map_.count(); ++i) {
    if (idx == map_info.table_map_.at(i)) {
      idx = i;
      find = true;
    }
  }
  if (OB_SUCC(ret) && (!find || OB_INVALID_ID == idx ||
      idx < 0 || idx > temp_table_query->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorrect table idx" ,K(idx), K(ret));
  }
  if (OB_SUCC(ret)) {
    TableItem *table = temp_table_query->get_table_item(idx);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else {
      table_id = table->table_id_;
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
    need_trans = true;
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

}//namespace sql
}//namespace oceanbase
