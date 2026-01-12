/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_JO
#include "sql/optimizer/ob_join_order_enum.h"
#include "sql/optimizer/ob_log_plan.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using oceanbase::share::schema::ObTableSchema;
using oceanbase::share::schema::ObColumnSchemaV2;
using oceanbase::share::schema::ObColDesc;
using share::schema::ObSchemaGetterGuard;
using common::ObArray;


ObJoinOrderEnum::ObJoinOrderEnum(ObLogPlan &plan, const common::ObIArray<ObRawExpr*> &quals):
  plan_(plan),
  quals_(quals),
  table_depend_infos_(),
  bushy_tree_infos_(),
  generator_(get_allocator(),
             plan.get_optimizer_context().get_expr_factory(),
             plan.get_optimizer_context().get_session_info(),
             plan.get_onetime_copier(),
             true, /* should_pushdown_const_filters */
             table_depend_infos_,
             plan.get_push_subq_exprs(),
             bushy_tree_infos_,
             plan.get_new_or_quals(),
             plan.get_optimizer_context().get_query_ctx()),
  conflict_detectors_(),
  output_join_order_(nullptr),
  path_cnt_(0) {}

int ObJoinOrderEnum::init()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObSEArray<ObRawExpr*,4>, 8> baserel_filters;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(stmt), K(ret));
  } else if (OB_FAIL(get_base_table_items(stmt))) {
    LOG_WARN("failed to flatten table items", K(ret));
  } else if (OB_FAIL(generate_base_level_join_order(base_table_items_,
                                                    base_level_))) {
    LOG_WARN("fail to generate base level join order", K(ret));
  } else if (OB_FAIL(init_function_table_depend_info(base_table_items_))) {
    LOG_WARN("failed to init function table depend infos", K(ret));
  } else if (OB_FAIL(init_json_table_depend_info(base_table_items_))) {
    LOG_WARN("failed to init json table depend infos", K(ret));
  } else if (OB_FAIL(init_lateral_table_depend_info(base_table_items_))) {
    LOG_WARN("failed to init lateral table depend info", K(ret));
  } else if (OB_FALSE_IT(conflict_detectors_.reuse())) {
  } else if (OB_FAIL(generator_.generate_conflict_detectors(get_stmt(),
                                                            from_table_items_,
                                                            stmt->get_semi_infos(),
                                                            quals_,
                                                            baserel_filters,
                                                            conflict_detectors_))) {
    LOG_WARN("failed to generate conflict detectors", K(ret));
  } else if (OB_FAIL(distribute_filters_to_baserels(base_level_, baserel_filters))) {
    LOG_WARN("failed to distribute filters to baserels", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    SemiInfo *semi_info = NULL;
    if (OB_ISNULL(semi_info = stmt->get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null semi info", K(ret), K(i));
    } else {
      TableItem *table = stmt->get_table_item_by_id(semi_info->right_table_id_);
      if (OB_FAIL(from_table_items_.push_back(table))) {
        LOG_WARN("failed to push back");
      }
    }
  }
  if (FAILEDx(init_bushy_tree_info(from_table_items_))) {
    LOG_WARN("failed to init bushy tree infos", K(ret));
  }

  OPT_TRACE_TITLE("GENERATE BASE PATH");
  for (int64_t i = 0; OB_SUCC(ret) && i < base_level_.count(); ++i) {
    if (OB_ISNULL(base_level_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("base_level_.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(mock_base_rel_detectors(base_level_.at(i)))) {
      LOG_WARN("failed to mock base rel detectors", K(ret));
    } else {
      OPT_TRACE("create base path for ", base_level_.at(i));
      OPT_TRACE_BEGIN_SECTION;
      ret = base_level_.at(i)->generate_base_paths();
      OPT_TRACE_MEM_USED;
      OPT_TRACE_END_SECTION;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(get_optimizer_context().get_deduce_info(),
                              base_level_.at(i)->get_deduce_info()))) {
      LOG_WARN("failed to append deduce info", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnum::enumerate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init())) {
    LOG_WARN("failed to init join order enum", K(ret));
  } else {
    OPT_TRACE_TITLE("BASIC TABLE STATISTICS");
    OPT_TRACE_STATIS(get_stmt(), get_plan().get_basic_table_metas());
    OPT_TRACE_TITLE("UPDATE TABLE STATISTICS");
    OPT_TRACE_STATIS(get_stmt(), get_plan().get_update_table_metas());
    OPT_TRACE_TITLE("START GENERATE JOIN ORDER");
  }
  if (FAILEDx(inner_enumerate())) {
    LOG_WARN("failed to do enumerate", K(ret));
  } else if (OB_ISNULL(output_join_order_)) {
    ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
    LOG_WARN("failed to generate any join order", K(ret));
  } else if (OB_UNLIKELY(output_join_order_->get_interesting_paths().empty())) {
    ret = OB_ERR_NO_PATH_GENERATED;
    LOG_WARN("failed to generate any join path", K(ret));
  }
  return ret;
}

int ObJoinOrderEnum::get_base_table_items(const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(stmt->get_from_tables(from_table_items_))) {
    LOG_WARN("failed to get table items", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_table_items_.count(); ++i) {
    TableItem *item = from_table_items_.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table item", K(ret));
    } else if (!item->is_joined_table()) {
      ret = base_table_items_.push_back(item);
    } else {
      JoinedTable *joined_table = static_cast<JoinedTable*>(item);
      for (int64_t j = 0; OB_SUCC(ret) && j < joined_table->single_table_ids_.count(); ++j) {
        TableItem *table = stmt->get_table_item_by_id(joined_table->single_table_ids_.at(j));
        ret = base_table_items_.push_back(table);
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    SemiInfo *semi_info = NULL;
    if (OB_ISNULL(semi_info = stmt->get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null semi info", K(ret), K(i));
    } else {
      TableItem *table = stmt->get_table_item_by_id(semi_info->right_table_id_);
      if (OB_FAIL(base_table_items_.push_back(table))) {
        LOG_WARN("failed to push back");
      }
    }
  }
  return ret;
}

int ObJoinOrderEnum::generate_base_level_join_order(const ObIArray<TableItem*> &table_items,
                                                    ObIArray<ObJoinOrder*> &base_level)
{
  int ret = OB_SUCCESS;
  ObJoinOrder *this_jo = NULL;
  //首先加入基表
  int64_t N = table_items.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_ISNULL(table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(ret), K(i));
    } else if (OB_ISNULL(this_jo = create_join_order(ACCESS))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate an ObJoinOrder", K(ret));
    } else if (OB_FAIL(this_jo->init_base_join_order(table_items.at(i)))) {
      LOG_WARN("fail to generate the base rel", K(ret), K(*table_items.at(i)));
      this_jo->~ObJoinOrder();
      this_jo = NULL;
    } else if (OB_FAIL(base_level.push_back(this_jo))) {
      LOG_WARN("fail to push back an ObJoinOrder", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

ObJoinOrder* ObJoinOrderEnum::create_join_order(PathType type)
{
  void *ptr = NULL;
  ObJoinOrder *join_order = NULL;
  if (!recycled_join_orders_.empty()) {
    recycled_join_orders_.pop_back(join_order);
    if (OB_NOT_NULL(join_order)) {
      join_order->set_type(type);
    }
  } else if (OB_NOT_NULL((ptr = get_allocator().alloc(sizeof(ObJoinOrder))))) {
    join_order = new (ptr) ObJoinOrder(*this, type);
  }
  return join_order;
}

int ObJoinOrderEnum::free(ObJoinOrder *&join_order)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(join_order)) {
    if (OB_FAIL(recycled_join_orders_.push_back(join_order))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      join_order->reuse();
      join_order = nullptr;
    }
  }
  return ret;
}

JoinPath* ObJoinOrderEnum::alloc_join_path()
{
  void *ptr = NULL;
  JoinPath *join_path = NULL;
  if (!recycled_join_paths_.empty()) {
    recycled_join_paths_.pop_back(join_path);
  } else {
    ptr = get_allocator().alloc(sizeof(JoinPath));
    if (OB_NOT_NULL(ptr)) {
      join_path = new(ptr) JoinPath();
    }
  }
  return join_path;
}

int ObJoinOrderEnum::free(Path *&path)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(path)) {
    if (path->is_join_path()) {
      JoinPath *join_path = static_cast<JoinPath *>(path);
      if (OB_FAIL(recycled_join_paths_.push_back(join_path))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        join_path->reuse();
      }
    } else {
      path->~Path();
      get_allocator().free(path);
    }
    path = nullptr;
  }
  return ret;
}

int ObJoinOrderEnum::check_join_hint(const ObRelIds &left_set,
                                     const ObRelIds &right_set,
                                     bool &match_hint,
                                     bool &is_legal,
                                     bool &is_strict_order)
{
  int ret = OB_SUCCESS;
  const ObRelIds &leading_tables = get_leading_tables();
  if (!left_set.overlap(leading_tables) && !right_set.overlap(leading_tables)) {
    //没有涉及leading hint的表，不需要额外的检查
    match_hint = false;
    is_legal = true;
    is_strict_order = true;
  } else if (left_set.is_subset(leading_tables) && right_set.is_subset(leading_tables)) {
    //正在枚举leading hint内部表
    bool found = false;
    //查找是否有满足的hint
    const ObIArray<LeadingInfo *> &leading_infos = get_log_plan_hint().join_order_.leading_infos_;
    for (int64_t i = 0; !found && i < leading_infos.count(); ++i) {
      const LeadingInfo *info = leading_infos.at(i);
      if (OB_ISNULL(info)) {
        // do nothing
      } else if (left_set.equal(info->left_table_set_) && right_set.equal(info->right_table_set_)) {
        is_strict_order = true;
        found = true;
      } else if (right_set.equal(info->left_table_set_) && left_set.equal(info->right_table_set_)) {
        is_strict_order = false;
        found = true;
      }
    }
    if (!found) {
      //枚举的join order尝试打乱leading hint
      is_legal = false;
    } else {
      match_hint = true;
      is_legal = true;
    }
  } else if (leading_tables.is_subset(left_set)) {
    //处理完所有leading hint表之后的枚举过程
    match_hint = true;
    is_legal = true;
    is_strict_order = true;
  } else {
    //使用部分leading hint的表，非法枚举
    is_legal = false;
  }
  return ret;
}


/**
 * Remove redundant join conditions and extract equal join conditions
 *
 * Try keep EQ preds that join same two tables.
 * For example, we will keep the two preds which join t1 and t3 in the below case.
 * (t1 join t2 on t1.c1 = t2.c1)
 *   join
 * (t3 join t4 on t3.c2 = t4.c2)
 *   on t1.c1 = t3.c1 and t2.c1 = t3.c1 and t1.c2 = t3.c2 and t1.c2 = t4.c2
 * =>
 * (t1 join t2 on t1.c1 = t2.c1)
 *   join
 * (t3 join t4 on t3.c2 = t4.c2)
 *   on t1.c1 = t3.c1 and t1.c2 = t3.c2
 *
 * Remove preds which is equation between two exprs in the same equal sets
 * (t1 where c1 = 1) join (t2 where c2 = 1) on t1.c1 = t2.c1
 *  => (t1 where c1 = 1) join (t2 where c2 = 1) on true
 * */
int ObJoinOrderEnum::process_join_pred(ObJoinOrder *left_tree,
                                       ObJoinOrder *right_tree,
                                       JoinInfo &join_info)
{
  int ret = OB_SUCCESS;
  // remove redundancy pred
  if (INNER_JOIN == join_info.join_type_ ||
      LEFT_SEMI_JOIN == join_info.join_type_ ||
      LEFT_ANTI_JOIN == join_info.join_type_ ||
      RIGHT_SEMI_JOIN == join_info.join_type_ ||
      RIGHT_ANTI_JOIN == join_info.join_type_ ||
      LEFT_OUTER_JOIN == join_info.join_type_ ||
      RIGHT_OUTER_JOIN == join_info.join_type_) {
    ObIArray<ObRawExpr*> &join_pred = IS_NOT_INNER_JOIN(join_info.join_type_) ?
                                      join_info.on_conditions_ :
                                      join_info.where_conditions_;
    TemporaryEqualSets input_equal_sets;
    ObArenaAllocator allocator;
    if (OB_FAIL(ObEqualAnalysis::merge_equal_set(&allocator,
                                                 left_tree->get_output_equal_sets(),
                                                 right_tree->get_output_equal_sets(),
                                                 input_equal_sets))) {
      LOG_WARN("failed to compute equal sets for inner join", K(ret));
    } else if (OB_FAIL(inner_remove_redundancy_pred(&allocator,
                                                    join_pred,
                                                    input_equal_sets,
                                                    left_tree,
                                                    right_tree))) {
      LOG_WARN("failed to inner remove redundancy pred", K(ret));
    }
  }
  // extract equal join conditions
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (INNER_JOIN == join_info.join_type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < join_info.where_conditions_.count(); ++i) {
      ObRawExpr *qual = join_info.where_conditions_.at(i);
      if (OB_ISNULL(qual)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null join qual", K(ret));
      } else if (!qual->has_flag(IS_JOIN_COND)) {
        //do nothing
      } else if (OB_FAIL(join_info.equal_join_conditions_.push_back(qual))) {
        LOG_WARN("failed to push back join qual", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < join_info.on_conditions_.count(); ++i) {
      ObRawExpr *qual = join_info.on_conditions_.at(i);
      if (OB_ISNULL(qual)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null join qual", K(ret));
      } else if (!qual->has_flag(IS_JOIN_COND)) {
        //do nothing
      } else if (OB_FAIL(join_info.equal_join_conditions_.push_back(qual))) {
        LOG_WARN("failed to push back join qual", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnum::join_side_from_one_table(ObJoinOrder &child_tree,
                                              ObIArray<ObRawExpr*> &join_pred,
                                              bool &is_valid,
                                              ObRelIds &intersect_rel_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRelIds, 4> eq_sets_rel_ids;
  ObRawExpr *expr_in_tree = NULL;
  intersect_rel_ids.reuse();
  is_valid = true;
  if (OB_FAIL(intersect_rel_ids.add_members(child_tree.get_tables()))){
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::build_rel_ids_by_equal_sets(child_tree.get_output_equal_sets(),
                                                                  eq_sets_rel_ids))) {
    LOG_WARN("failed to build rel ids", K(ret));
  }
  const ObRelIds &tree_rel_ids = child_tree.get_tables();
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < join_pred.count(); i++) {
    ObRawExpr *cur_expr = join_pred.at(i);
    ObRawExpr *left_expr = NULL;
    ObRawExpr *right_expr = NULL;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_expr));
    } else if (T_OP_EQ == cur_expr->get_expr_type()) {
      if (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
          OB_ISNULL(right_expr = cur_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), KPC(cur_expr), K(left_expr), K(right_expr));
      } else if (tree_rel_ids.is_superset(left_expr->get_relation_ids())) {
        expr_in_tree = left_expr;
      } else if (tree_rel_ids.is_superset(right_expr->get_relation_ids())) {
        expr_in_tree = right_expr;
      } else {
        is_valid = false;
      }
      if (OB_SUCC(ret) && is_valid) {
        const ObRelIds &expr_rel_ids = expr_in_tree->get_relation_ids();
        int64_t eq_set_idx = OB_INVALID_ID;
        if (expr_rel_ids.num_members() != 1) {
          is_valid = false;
        } else if (OB_FAIL(ObOptimizerUtil::find_expr_in_equal_sets(child_tree.get_output_equal_sets(),
                                                                    expr_in_tree,
                                                                    eq_set_idx))) {
          LOG_WARN("failed to find expr", K(ret));
        } else {
          const ObRelIds &to_intersect = (eq_set_idx == OB_INVALID_ID) ?
                                          expr_rel_ids :
                                          eq_sets_rel_ids.at(eq_set_idx);
          if (OB_FAIL(intersect_rel_ids.intersect_members(to_intersect))) {
            LOG_WARN("failed to intersect", K(ret));
          } else if (intersect_rel_ids.is_empty()) {
            is_valid = false;
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) || !is_valid) {
  } else if (OB_FAIL(intersect_rel_ids.reserve_first())) {
    LOG_WARN("failed to reserve first", K(ret));
  }

  return ret;
}

// Check if there are some predicates that were lost
// for situations where predicate derivation did not happen or predicate derivation was incomplete
int ObJoinOrderEnum::re_add_necessary_predicate(ObIArray<ObRawExpr*> &join_pred,
                                                ObIArray<ObRawExpr*> &new_join_pred,
                                                ObIArray<bool> &skip,
                                                EqualSets &equal_sets)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(join_pred.count() != skip.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < join_pred.count(); i++) {
    ObRawExpr *cur_expr= join_pred.at(i);
    ObRawExpr *left_expr = NULL;
    ObRawExpr *right_expr = NULL;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_expr));
    } else if (skip.at(i)) {
      // skip
    } else if (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
               OB_ISNULL(right_expr = cur_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(left_expr), K(right_expr));
    } else if (ObOptimizerUtil::is_expr_equivalent(left_expr,
                                                   right_expr,
                                                   equal_sets)) {
      // remove preds which is equation between two exprs in the same equal sets
      OPT_TRACE("remove redundancy join condition:", cur_expr);
    } else {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < new_join_pred.count(); j++) {
        ObRawExpr *cur_new_expr = new_join_pred.at(j);
        ObRawExpr *left_new_expr = NULL;
        ObRawExpr *right_new_expr = NULL;
        if (OB_ISNULL(cur_new_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (cur_expr->get_expr_type() != cur_new_expr->get_expr_type()) {
          // skip
        } else if(OB_ISNULL(left_new_expr = cur_new_expr->get_param_expr(0)) ||
                  OB_ISNULL(right_new_expr = cur_new_expr->get_param_expr(1))) {
          LOG_WARN("get unexpected null", K(ret), K(left_new_expr), K(right_new_expr));
        } else if (ObOptimizerUtil::is_expr_equivalent(left_expr,
                                                       left_new_expr,
                                                       equal_sets) &&
                   ObOptimizerUtil::is_expr_equivalent(right_expr,
                                                       right_new_expr,
                                                       equal_sets)) {
          find = true;
        } else if (ObOptimizerUtil::is_expr_equivalent(left_expr,
                                                       right_new_expr,
                                                       equal_sets) &&
                   ObOptimizerUtil::is_expr_equivalent(right_expr,
                                                       left_new_expr,
                                                       equal_sets)) {
          find = true;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!find && OB_FAIL(new_join_pred.push_back(cur_expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (find) {
        // remove preds which do not join the given two tables
        OPT_TRACE("remove redundancy join condition:", cur_expr);
      }
    }
  }
  return ret;
}

int ObJoinOrderEnum::inner_remove_redundancy_pred(ObIAllocator *allocator,
                                                  ObIArray<ObRawExpr*> &join_pred,
                                                  EqualSets &equal_sets,
                                                  ObJoinOrder *left_tree,
                                                  ObJoinOrder *right_tree)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_join_pred;
  ObSEArray<bool, 4> has_checked;
  bool join_two_tables = true;
  ObRelIds left_table, right_table;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(join_side_from_one_table(*left_tree,
                                              join_pred,
                                              join_two_tables,
                                              left_table))) {
    LOG_WARN("failed to check there is only one left table", K(ret));
  } else if (join_two_tables &&
             OB_FAIL(join_side_from_one_table(*right_tree,
                                              join_pred,
                                              join_two_tables,
                                              right_table))) {
    LOG_WARN("failed to check there is only one right table", K(ret));
  } else if (join_two_tables &&
             (OB_UNLIKELY(left_table.num_members() != 1) ||
              OB_UNLIKELY(right_table.num_members() != 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(left_table), K(right_table));
  } else if (OB_FAIL(has_checked.prepare_allocate(join_pred.count(), false))) {
    LOG_WARN("failed to allocate", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < join_pred.count(); i++) {
    ObRawExpr *cur_expr = join_pred.at(i);
    ObRawExpr *left_expr = NULL;
    ObRawExpr *right_expr = NULL;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_expr));
    } else if (T_OP_EQ == cur_expr->get_expr_type() &&
               2 == cur_expr->get_param_count() &&
               cur_expr->get_param_expr(0) != cur_expr->get_param_expr(1)) {
      TemporaryEqualSets tmp_equal_sets;
      if (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
          OB_ISNULL(right_expr = cur_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), KPC(cur_expr), K(left_expr), K(right_expr));
      } else if (!join_two_tables) {
        // do nothing
      } else if (left_tree->get_tables().is_superset(left_expr->get_relation_ids()) &&
          right_tree->get_tables().is_superset(right_expr->get_relation_ids())) {
        // do nothing
      } else if (left_tree->get_tables().is_superset(right_expr->get_relation_ids()) &&
          right_tree->get_tables().is_superset(left_expr->get_relation_ids())) {
        std::swap(left_expr, right_expr);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expression", K(ret), KPC(cur_expr));
      }
      if (OB_FAIL(ret)) {
      } else if (join_two_tables &&
                 !(left_table.is_superset(left_expr->get_relation_ids()) &&
                   right_table.is_superset(right_expr->get_relation_ids()))) {
        // the pred does not join the given two tables,
        // decide whether remove this qual later
      } else if (ObOptimizerUtil::in_same_equalset(left_expr,
                                                   right_expr,
                                                   equal_sets)) {
        // remove preds which is equation between two exprs in the same equal sets
        has_checked.at(i) = true;
        OPT_TRACE("remove redundancy join condition:", cur_expr);
      } else if (OB_FAIL(tmp_equal_sets.assign(equal_sets))) {
        LOG_WARN("failed to append fd equal set", K(ret));
      } else if (FALSE_IT(equal_sets.reuse())) {
      } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator,
                                                            cur_expr,
                                                            tmp_equal_sets,
                                                            equal_sets))) {
        LOG_WARN("failed to compute equal sets for inner join", K(ret));
      } else if (OB_FAIL(new_join_pred.push_back(cur_expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        has_checked.at(i) = true;
      }
    } else if (OB_FAIL(new_join_pred.push_back(cur_expr))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      has_checked.at(i) = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (join_two_tables &&
             OB_FAIL(re_add_necessary_predicate(join_pred, new_join_pred,
                                                has_checked, equal_sets))) {
    LOG_WARN("failed to re-add preds", K(ret));
  } else if (new_join_pred.count() == join_pred.count()) {
  } else if (OB_UNLIKELY(new_join_pred.count() > join_pred.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pred count", K(ret), K(new_join_pred), K(join_pred));
  } else if (OB_FAIL(join_pred.assign(new_join_pred))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  OPT_TRACE("output join conditions:", join_pred);
  return ret;
}

int ObJoinOrderEnum::init_function_table_depend_info(const ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem *table = table_items.at(i);
    TableDependInfo info;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_function_table()) {
      //do nothing
    } else if (OB_ISNULL(table->function_table_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null function table expr", K(ret));
    } else if (table->function_table_expr_->get_relation_ids().is_empty()) {
      //do thing
    } else if (OB_FAIL(info.depend_table_set_.add_members(table->function_table_expr_->get_relation_ids()))) {
      LOG_WARN("failed to assign table ids", K(ret));
    } else if (OB_FALSE_IT(info.table_idx_ = stmt->get_table_bit_index(table->table_id_))) {
    } else if (OB_FAIL(table_depend_infos_.push_back(info))) {
      LOG_WARN("failed to push back info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to init function table depend info", K(table_depend_infos_));
  }
  return ret;
}

int ObJoinOrderEnum::init_default_val_json(ObRelIds& depend_table_set,
                                           ObRawExpr*& default_expr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(default_expr)) {
    if (default_expr->get_relation_ids().is_empty()) {
      //do nothing
    } else if (OB_FAIL(depend_table_set.add_members(default_expr->get_relation_ids()))) {
      LOG_WARN("failed to assign table ids", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnum::init_json_table_column_depend_info(ObRelIds& depend_table_set,
                                                        TableItem* json_table,
                                                        const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ColumnItem* column_item = NULL;
  common::ObArray<ColumnItem> stmt_column_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(json_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_column_items(json_table->table_id_, stmt_column_items))) {
    LOG_WARN("fail to get column_items", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_column_items.count(); i++) {
    if (json_table->table_id_ != stmt_column_items.at(i).table_id_) {
    } else if (OB_NOT_NULL(stmt_column_items.at(i).default_value_expr_)
                && OB_FAIL(init_default_val_json(depend_table_set, stmt_column_items.at(i).default_value_expr_))) {
      LOG_WARN("fail to init error default value depend info", K(ret));
    } else if (OB_NOT_NULL(stmt_column_items.at(i).default_empty_expr_)
                && OB_FAIL(init_default_val_json(depend_table_set, stmt_column_items.at(i).default_empty_expr_))) {
      LOG_WARN("fail to init error default value depend info", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnum::init_json_table_depend_info(const ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem *table = table_items.at(i);
    TableDependInfo info;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_json_table()) {
      //do nothing
    } else if (OB_ISNULL(table->json_table_def_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null function table expr", K(ret));
    } else {
      bool is_all_relation_id_empty = true;
      for (int64_t j = 0; OB_SUCC(ret) && j < table->json_table_def_->doc_exprs_.count(); ++j) {
        if (OB_ISNULL(table->json_table_def_->doc_exprs_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("doc_expr in json_table_def is null", K(ret));
        } else if (table->json_table_def_->doc_exprs_.at(j)->get_relation_ids().is_empty()) {
          //do nothing
        } else if (OB_FAIL(info.depend_table_set_.add_members(table->json_table_def_->doc_exprs_.at(j)->get_relation_ids()))) {
          LOG_WARN("failed to assign table ids", K(ret));
        } else {
          is_all_relation_id_empty = false;
        }
        if (OB_FAIL(ret) || is_all_relation_id_empty) {
        } else if (OB_FAIL(init_json_table_column_depend_info(info.depend_table_set_, table, stmt))) { // deal column items default value
          LOG_WARN("fail to init json table default value depend info", K(ret));
        } else if (OB_FALSE_IT(info.table_idx_ = stmt->get_table_bit_index(table->table_id_))) {
        } else if (OB_FAIL(table_depend_infos_.push_back(info))) {
          LOG_WARN("failed to push back info", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to init function table depend info", K(table_depend_infos_));
  }
  return ret;
}

int ObJoinOrderEnum::init_lateral_table_depend_info(const ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem *table = table_items.at(i);
    TableDependInfo info;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_lateral_table() ||
                table->exec_params_.empty()) {
      //do nothing
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < table->exec_params_.count(); ++j) {
        if (OB_ISNULL(table->exec_params_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(info.depend_table_set_.add_members(
                  table->exec_params_.at(j)->get_ref_expr()->get_relation_ids()))) {
          LOG_WARN("failed to add table ids", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(info.table_idx_ = stmt->get_table_bit_index(table->table_id_))) {
      } else if (OB_FAIL(table_depend_infos_.push_back(info))) {
        LOG_WARN("failed to push back info", K(ret));
      }
    }

  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to init function table depend info", K(table_depend_infos_));
  }
  return ret;
}

int ObJoinOrderEnum::distribute_filters_to_baserels(ObIArray<ObJoinOrder*> &base_level,
                                                    ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < base_level.count(); ++i) {
    ObJoinOrder *cur_rel= base_level.at(i);
    ObSEArray<int64_t, 1> rel_id;
    if (OB_ISNULL(cur_rel)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (OB_FAIL(cur_rel->get_tables().to_array(rel_id))) {
      LOG_WARN("failed to get rel id", K(ret));
    } else if (OB_UNLIKELY(1 != rel_id.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rel id count", K(ret), K(rel_id.count()));
    } else if (OB_UNLIKELY(rel_id.at(0) < 1 || rel_id.at(0) > baserel_filters.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rel id", K(ret), K(rel_id.at(0)), K(baserel_filters.count()));
    } else if (OB_FAIL(append(cur_rel->get_restrict_infos(), baserel_filters.at(rel_id.at(0) - 1)))) {
      LOG_WARN("failed to append restrict infos", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnum::mock_base_rel_detectors(ObJoinOrder *&base_rel)
{
  int ret = OB_SUCCESS;
  ObConflictDetector *detector = NULL;
  // mock a conflict detector whose join info's where_condition
  // is base rel's base table pushdown filter, and add it into
  // used_conflict_detector, which will be used for width est.
  // see ObJoinOrder::est_join_width() condition exclusion for detail
  if (OB_ISNULL(base_rel)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (base_rel->get_restrict_infos().empty() ||
             !base_rel->get_conflict_detectors().empty()) {
    // do nothing
  } else if (OB_FAIL(ObConflictDetector::build_detector(get_allocator(), detector))) {
    LOG_WARN("failed to build conflict detector", K(ret));
  } else if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (OB_FAIL(append_array_no_dup(detector->get_join_info().where_conditions_,
                                         base_rel->get_restrict_infos()))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(base_rel->get_conflict_detectors(), detector))) {
    LOG_WARN("failed to append into used conflict detectors", K(ret));
  } else {/*do nothing*/}
  return ret;
}


int ObJoinOrderEnum::init_bushy_tree_info(const ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  const ObIArray<LeadingInfo *> &leading_infos = get_log_plan_hint().join_order_.leading_infos_;
  for (int64_t i = 0; OB_SUCC(ret) && i < leading_infos.count(); ++i) {
    const LeadingInfo *info = leading_infos.at(i);
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(leading_infos));
    } else if (info->left_table_set_.num_members() > 1 &&
               info->right_table_set_.num_members() > 1) {
      ret = bushy_tree_infos_.push_back(info->table_set_);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    if (OB_FAIL(init_bushy_tree_info_from_joined_tables(table_items.at(i)))) {
      LOG_WARN("failed to init bushy tree info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to get bushy infos", K(bushy_tree_infos_));
  }
  return ret;
}

int ObJoinOrderEnum::init_bushy_tree_info_from_joined_tables(TableItem *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item or stmt", K(ret), K(table), K(get_stmt()));
  } else if (!table->is_joined_table()) {
    //do nothing
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    if (OB_ISNULL(joined_table->left_table_) ||
        OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (joined_table->left_table_->is_joined_table() &&
               joined_table->right_table_->is_joined_table()) {
      ObRelIds table_ids;
      if (OB_FAIL(get_stmt()->get_table_rel_ids(*table, table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (OB_FAIL(bushy_tree_infos_.push_back(table_ids))) {
        LOG_WARN("failed to push back table ids", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(SMART_CALL(init_bushy_tree_info_from_joined_tables(joined_table->left_table_)))) {
      LOG_WARN("failed to init bushy tree infos", K(ret));
    } else if (OB_FAIL(SMART_CALL(init_bushy_tree_info_from_joined_tables(joined_table->right_table_)))) {
      LOG_WARN("failed to init bushy tree infos", K(ret));
    }
  }
  return ret;
}