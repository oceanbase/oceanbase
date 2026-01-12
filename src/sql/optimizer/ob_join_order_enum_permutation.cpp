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
#include "sql/optimizer/ob_join_order_enum_permutation.h"
#include "sql/optimizer/ob_log_plan.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

static inline double round_rows(double x)
{
  return trunc(x * (1 << 13)) / ((1 << 13));
}
static inline double round_cost(double cost)
{
  return round_rows(cost);
}

ObJoinOrderEnumPermutation::ObJoinOrderEnumPermutation(ObLogPlan &plan,
                                                       const common::ObIArray<ObRawExpr*> &quals):
  ObJoinOrderEnum(plan, quals),
  leading_tree_(nullptr),
  use_leading_(false),
  size_(0),
  cur_top_tree_(nullptr),
  ignore_hint_(false),
  global_contain_normal_nl_(true),
  global_has_none_equal_join_(true),
  global_has_expansion_join_(true),
  eval_level_(-1),
  normal_cost_info_(plan.get_allocator()),
  pipeline_cost_info_(plan.get_allocator()) {}

int ObJoinOrderEnumPermutation::init()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(stmt));
  } else if (OB_FAIL(ObJoinOrderEnum::init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(leading_table_depend_infos_.prepare_allocate(base_table_items_.count() + 1))) {
    LOG_WARN("failed to prepare allocate");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_depend_infos_.count(); i ++) {
    const TableDependInfo &info = table_depend_infos_.at(i);
    if (OB_UNLIKELY(info.table_idx_ < 0 || info.table_idx_ >= leading_table_depend_infos_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param", K(info), K(leading_table_depend_infos_));
    } else if (OB_FAIL(leading_table_depend_infos_.at(info.table_idx_).add_members(info.depend_table_set_))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_table_items_.count(); i ++) {
    if (OB_FAIL(init_outer_join_info(from_table_items_.at(i)))) {
      LOG_WARN("failed to init join info", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    SemiInfo *semi_info = NULL;
    ObRelIds depend_set;
    int64_t idx = OB_INVALID_INDEX;
    if (OB_ISNULL(semi_info = stmt->get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null semi info", K(ret), K(i));
    } else if (semi_info->left_table_ids_.empty()) {
      // do nothing
    } else if (OB_FAIL(stmt->get_table_rel_ids(semi_info->left_table_ids_, depend_set))) {
      LOG_WARN("failed to assign table ids", K(ret));
    } else {
      idx = stmt->get_table_bit_index(semi_info->right_table_id_);
      if (OB_UNLIKELY(idx < 0 || idx >= leading_table_depend_infos_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected param", KPC(semi_info), K(leading_table_depend_infos_));
      } else if (OB_FAIL(leading_table_depend_infos_.at(idx).add_members(depend_set))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::init_outer_join_info(TableItem *table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KPC(table_item));
  } else if (table_item->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable *>(table_item);
    TableItem *left_table = joined_table->left_table_;
    TableItem *right_table = joined_table->right_table_;
    if (OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KPC(left_table), KPC(right_table));
    } else if (joined_table->is_left_join()) {
      if (OB_FAIL(init_join_depend_info(*right_table, *left_table))) {
        LOG_WARN("failed to init join depend info", K(ret));
      }
    } else if (joined_table->is_right_join()) {
      if (OB_FAIL(init_join_depend_info(*left_table, *right_table))) {
        LOG_WARN("failed to init join depend info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(SMART_CALL(init_outer_join_info(left_table)))) {
        LOG_WARN("failed to init left child outer join info", KPC(left_table));
      } else if (OB_FAIL(SMART_CALL(init_outer_join_info(right_table)))) {
        LOG_WARN("failed to init right child outer join info", KPC(right_table));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::init_join_depend_info(const TableItem &table, const TableItem &depend_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> table_ids;
  ObRelIds depend_set;
  int64_t table_idx = OB_INVALID_INDEX;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KPC(get_stmt()));
  } else if (OB_FAIL(get_stmt()->get_table_rel_ids(depend_table, depend_set))) {
    LOG_WARN("failed to assign table ids", K(ret));
  } else if (table.is_joined_table()) {
    const JoinedTable &joined_table = static_cast<const JoinedTable &>(table);
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table.single_table_ids_.count(); i ++) {
      table_idx = get_stmt()->get_table_bit_index(joined_table.single_table_ids_.at(i));
      if (OB_UNLIKELY(table_idx < 0 || table_idx >= leading_table_depend_infos_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected param", K(joined_table), K(leading_table_depend_infos_));
      } else if (OB_FAIL(leading_table_depend_infos_.at(table_idx).add_members(depend_set))) {
        LOG_WARN("failed to add member", K(ret));
      }
    }
  } else {
    table_idx = get_stmt()->get_table_bit_index(table.table_id_);
    if (OB_UNLIKELY(table_idx < 0 || table_idx >= leading_table_depend_infos_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param", K(table), K(leading_table_depend_infos_));
    } else if (OB_FAIL(leading_table_depend_infos_.at(table_idx).add_members(depend_set))) {
      LOG_WARN("failed to add member", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::inner_enumerate()
{
  int ret = OB_SUCCESS;
  OPT_TRACE("ENUMERATE PERMUTATIONS");
  ignore_hint_ = false;
  output_join_order_ = nullptr;
  if (has_join_order_hint() && OB_FAIL(do_enumerate())) {
    LOG_WARN("failed to do enumerate with hint", K(ret));
    if (OB_ERR_NO_JOIN_ORDER_GENERATED == ret ||
        OB_ERR_NO_PATH_GENERATED == ret) {
      output_join_order_ = nullptr;
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && nullptr == output_join_order_) {
    ignore_hint_ = true;
    if (OB_FAIL(do_enumerate())) {
      LOG_WARN("failed to do enumerate without hint", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::do_enumerate()
{
  int ret = OB_SUCCESS;
  if (ignore_hint_) {
    OPT_TRACE("GENEARATE JOIN ORDER WITHOUT HINT");
  } else {
    OPT_TRACE("GENEARATE JOIN ORDER WITH HINT");
  }
  output_join_order_ = nullptr;
  ObSEArray<ObJoinOrder *, 10> flattened_base_levels;
  if (OB_FAIL(flatten_base_join_orders(flattened_base_levels))) {
    LOG_WARN("failed to flatten base join orders", K(ret));
  } else if (OB_FAIL(reduce_join_orders(flattened_base_levels,
                                        output_join_order_,
                                        !ignore_hint_))) {
    LOG_WARN("failed to reduce join orders", K(ret));
  }
  return ret;
}

int ObJoinOrderEnumPermutation::flatten_base_join_orders(ObIArray<ObJoinOrder *> &base_levels)
{
  int ret = OB_SUCCESS;
  leading_tree_ = nullptr;
  /*
  * 仅支持 hint 生成的 join order 与最外层的其他 join order 间有合法的 ZigZag 树
  */
  if (!ignore_hint_ &&
      OB_FAIL(init_join_tree_from_hint(leading_tree_))) {
    LOG_WARN("failed to init join tree", K(ret));
  } else if (nullptr != leading_tree_ &&
             OB_FAIL(base_levels.push_back(leading_tree_))) {
    LOG_WARN("failed to push back", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_table_items_.count(); i ++) {
    if (OB_FAIL(recursive_append_base_levels(from_table_items_.at(i), base_levels))) {
      LOG_WARN("failed to append base levels", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::init_join_tree_from_hint(ObJoinOrder *&leading_join_order)
{
  int ret = OB_SUCCESS;
  leading_join_order = nullptr;
  if (!has_join_order_hint()) {
    OPT_TRACE("NO JOIN ORDER HINT");
  } else if (NULL == get_leading_table_tree()) {
    // only one leading table
    if (OB_FAIL(find_base_level_join_rel(get_leading_tables(), leading_join_order))) {
      LOG_WARN("failed to find base level join tree", K(ret), K(get_log_plan_hint().join_order_));
    }
  } else if (OB_FAIL(recursive_init_join_tree_from_hint(get_leading_table_tree(), leading_join_order))) {
    LOG_WARN("failed to recursive init from hint", K(ret), K(get_log_plan_hint().join_order_));
  }
  if (OB_SUCC(ret)) {
    if (nullptr == leading_join_order) {
      OPT_TRACE("FAILED TO GENERATE LEADING JOIN ORDER BY HINT");
    } else {
      OPT_TRACE("SUCCEED TO GENERATE LEADING JOIN ORDER TREE BY HINT:", leading_join_order);
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::recursive_init_join_tree_from_hint(const LeadingInfo *leading_info,
                                                                   ObJoinOrder *&leading_join_order)
{
  int ret = OB_SUCCESS;
  leading_join_order = nullptr;
  ObJoinOrder *left_tree = nullptr;
  ObJoinOrder *right_tree = nullptr;
  if (OB_ISNULL(leading_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", KPC(leading_info));
  } else if (leading_info->left_info_ == nullptr) {
    if (OB_FAIL(find_base_level_join_rel(leading_info->left_table_set_, left_tree))) {
      LOG_WARN("failed to find base level join tree", KPC(leading_info));
    }
  } else if (OB_FAIL(SMART_CALL(recursive_init_join_tree_from_hint(leading_info->left_info_,
                                                                   left_tree)))) {
    LOG_WARN("failed to recursive init from hint", KPC(leading_info->left_info_));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(left_tree)) {
    if (leading_info->right_info_ == nullptr) {
      if (OB_FAIL(find_base_level_join_rel(leading_info->right_table_set_, right_tree))) {
        LOG_WARN("failed to find base level join tree", KPC(leading_info));
      }
    } else if (OB_FAIL(SMART_CALL(recursive_init_join_tree_from_hint(leading_info->right_info_,
                                                                     right_tree)))) {
      LOG_WARN("failed to recursive init from hint", KPC(leading_info->right_info_));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(left_tree) && OB_NOT_NULL(right_tree)) {
    OPT_TRACE("Generate leading join tree by hint:", left_tree, " join ", right_tree);
    if (OB_FAIL(try_join_two_tree(left_tree,
                                  right_tree,
                                  leading_join_order))) {
      LOG_WARN("failed to join two tree", K(ret));
    } else if (nullptr == leading_join_order) {
      ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
      LOG_WARN("failed to join left and right", K(ret), K(left_tree->get_tables()), K(right_tree->get_tables()));
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::recursive_append_base_levels(TableItem *table_item,
                                                             ObIArray<ObJoinOrder *> &base_levels)
{
  int ret = OB_SUCCESS;
  bool ignore_table = false;
  ObJoinOrder *single_join_order = nullptr;
  if (OB_ISNULL(table_item) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KPC(table_item));
  } else if (use_leading_hint()) {
    ObRelIds table_set;
    const ObRelIds leading_set = leading_tree_->get_tables();
    if (OB_FAIL(get_stmt()->get_table_rel_ids(*table_item, table_set))) {
      LOG_WARN("failed to get table set", K(ret));
    } else if (table_set.is_subset(leading_set)) {
      ignore_table = true;
    }
  }

  if (OB_FAIL(ret) || ignore_table) {
  } else if (table_item->is_joined_table()) {
    // 对于 outer join 的补 Null 侧，单独枚举 join order
    // 非补 null 侧，与外层的 inner join 一起枚举
    JoinedTable *joined_table = static_cast<JoinedTable *>(table_item);
    TableItem *left_table = joined_table->left_table_;
    TableItem *right_table = joined_table->right_table_;
    ObSEArray<ObJoinOrder *, 4> child_levels;
    if (OB_SUCC(ret)) {
      if (joined_table->is_left_join() || joined_table->is_inner_join()) {
        if (OB_FAIL(SMART_CALL(recursive_append_base_levels(left_table, child_levels)))) {
          LOG_WARN("failed to recursive append left", KPC(table_item));
        }
      } else {
        if (OB_FAIL(SMART_CALL(recursive_enumerate(left_table, single_join_order)))) {
          LOG_WARN("failed to recursive enumerate left", KPC(table_item));
        } else if (nullptr != single_join_order &&
                   OB_FAIL(child_levels.push_back(single_join_order))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (joined_table->is_right_join() || joined_table->is_inner_join()) {
        if (OB_FAIL(SMART_CALL(recursive_append_base_levels(right_table, child_levels)))) {
          LOG_WARN("failed to recursive append right", KPC(table_item));
        }
      } else {
        if (OB_FAIL(SMART_CALL(recursive_enumerate(right_table, single_join_order)))) {
          LOG_WARN("failed to recursive enumerate right", KPC(table_item));
        } else if (nullptr != single_join_order &&
                   OB_FAIL(child_levels.push_back(single_join_order))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !child_levels.empty()) {
      if (!joined_table->is_inner_join() &&
          !joined_table->is_left_join() &&
          !joined_table->is_right_join()) {
        if (OB_FAIL(reduce_join_orders(child_levels, single_join_order))) {
          LOG_WARN("failed to reduce join orders", K(ret));
        } else if (OB_FAIL(base_levels.push_back(single_join_order))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        if (OB_FAIL(append(base_levels, child_levels))) {
          LOG_WARN("failed to append", K(ret));
        }
      }
    }
  } else if (OB_FAIL(find_base_level_join_rel(table_item, single_join_order))) {
    LOG_WARN("failed to find join order", K(ret));
  } else if (OB_FAIL(base_levels.push_back(single_join_order))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

// generate the join tree for one Table Item
int ObJoinOrderEnumPermutation::recursive_enumerate(TableItem *table_item, ObJoinOrder *&top_level)
{
  int ret = OB_SUCCESS;
  top_level = nullptr;
  ObSEArray<ObJoinOrder *, 4> base_levels;
  bool ignore_table = false;
  if (use_leading_hint()) {
    ObRelIds table_set;
    const ObRelIds leading_set = leading_tree_->get_tables();
    if (OB_FAIL(get_stmt()->get_table_rel_ids(*table_item, table_set))) {
      LOG_WARN("failed to get table set", K(ret));
    } else if (table_set.is_subset(leading_set)) {
      ignore_table = true;
    }
  }
  if (OB_FAIL(ret) || ignore_table) {
  } else if (OB_FAIL(SMART_CALL(recursive_append_base_levels(table_item,
                                                             base_levels)))) {
    LOG_WARN("failed to recursive enumerate", KPC(table_item));
  } else if (OB_FAIL(reduce_join_orders(base_levels, top_level))) {
    LOG_WARN("failed to reduce join order", K(ret));
  }
  return ret;
}

// generate the join tree for multi base level join orders
int ObJoinOrderEnumPermutation::reduce_join_orders(const ObIArray<ObJoinOrder *> &base_levels,
                                                   ObJoinOrder *&top_level,
                                                   bool use_leading)
{
  int ret = OB_SUCCESS;
  if (base_levels.count() == 1) {
    top_level = base_levels.at(0);
  } else if (OB_UNLIKELY(base_levels.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected base join orders", K(base_levels));
  } else {
    OPT_TRACE("Enumerate join order for", base_levels);
    GreedyParamMinJoinCardinality min_join_card;
    reuse_permutation_infos();
    use_leading_ = use_leading;
    size_ = base_levels.count();
    if (OB_FAIL(generate_initial_join_tree(base_levels,
                                           min_join_card))) {
      LOG_WARN("failed to recursive init permutaition", K(ret));
    } else if (OB_FAIL(initial_permutation_status())) {
      LOG_WARN("failed to init permutation status", K(ret));
    } else if (OB_FAIL(enumerate_permutations())) {
      LOG_WARN("failed to enumerate permutations", K(ret));
    } else {
      top_level = cur_top_tree_;
    }
  }
  return ret;
}

// generate the initial join tree by some greedy method
int ObJoinOrderEnumPermutation::generate_initial_join_tree(const ObIArray<ObJoinOrder *> &candidate_base_levels,
                                                           const GreedyParam &greedy_method)
{
  int ret = OB_SUCCESS;
  ObSEArray<JoinHelper, 4> priority_next_join_helpers;
  ObArray<ObJoinOrder *> candidate_leading;
  if (OB_FAIL(join_helpers_.prepare_allocate(candidate_base_levels.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else if (OB_FAIL(get_candidate_leading_base_levels(candidate_base_levels,
                                                       candidate_leading))) {
    LOG_WARN("failed to get candidate leading", K(ret));
  } else if (OB_FAIL(get_current_join_node(candidate_leading,
                                           greedy_method,
                                           nullptr,
                                           0,
                                           join_helpers_.at(0)))) {
    LOG_WARN("failed to get next join priority", K(ret));
  }
  for (int64_t cur_level = 1; OB_SUCC(ret) && cur_level < join_helpers_.count(); cur_level ++) {
    JoinHelper &pre_join_helper = join_helpers_.at(cur_level - 1);
    JoinHelper &cur_join_helper = join_helpers_.at(cur_level);
    if (OB_FAIL(get_current_join_node(candidate_base_levels,
                                      greedy_method,
                                      pre_join_helper.join_tree_,
                                      cur_level,
                                      cur_join_helper))) {
      LOG_WARN("failed to get next join priority", K(ret));
    } else if (OB_UNLIKELY(!cur_join_helper.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected join", K(cur_level));
    } else if (OB_FAIL(cur_join_helper.generate_join_paths())) {
      LOG_WARN("failed to generate join paths", K(ret), K(cur_join_helper));
    } else if (OB_UNLIKELY(cur_join_helper.join_tree_->get_interesting_paths().empty())) {
      ret = OB_ERR_NO_PATH_GENERATED;
      LOG_WARN("no join path generated", K(ret), K(cur_join_helper));
    } else if (OB_FAIL(cur_join_helper.compute_join_property())) {
      LOG_WARN("failed to compute join property", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::get_candidate_leading_base_levels(const ObIArray<ObJoinOrder *> &base_levels,
                                                                  ObIArray<ObJoinOrder *> &leading_base_levels)
{
  int ret = OB_SUCCESS;
  leading_base_levels.reuse();
  bool has_leading_hint_tree = false;
  if (use_leading_hint()) {
    for (int64_t i = 0; OB_SUCC(ret) && !has_leading_hint_tree && i < base_levels.count(); i ++) {
      if (base_levels.at(i) == leading_tree_) {
        has_leading_hint_tree = true;
        if (OB_FAIL(leading_base_levels.push_back(leading_tree_))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !has_leading_hint_tree) {
    ObRelIds all_tables;
    for (int64_t i = 0; OB_SUCC(ret) && i < base_levels.count(); i ++) {
      if (OB_ISNULL(base_levels.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(all_tables.add_members(base_levels.at(i)->get_tables()))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < base_levels.count(); i ++) {
      bool has_depend = false;
      ObJoinOrder* base_level = base_levels.at(i);
      const ObRelIds &table_set = base_level->get_tables();
      for (int64_t j = 1; OB_SUCC(ret) && !has_depend && j < leading_table_depend_infos_.count(); j ++) {
        ObRelIds other_depend;
        if (table_set.has_member(j) && !leading_table_depend_infos_.at(j).is_empty()) {
          if (OB_FAIL(other_depend.except(leading_table_depend_infos_.at(j), table_set))) {
            LOG_WARN("failed to except", K(ret));
          } else {
            has_depend = other_depend.overlap(all_tables);
          }
        }
      }
      if (OB_SUCC(ret) && !has_depend) {
        if (OB_FAIL(leading_base_levels.push_back(base_level))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(leading_base_levels.empty())) {
    ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
    LOG_WARN("failed to find any leading tables", K(ret));
  }
  return ret;
}

int ObJoinOrderEnumPermutation::get_current_join_node(const ObIArray<ObJoinOrder *> &candi_nodes,
                                                      const GreedyParam &greedy_method,
                                                      ObJoinOrder *pre_join_tree,
                                                      int64_t cur_level,
                                                      JoinHelper &cur_join_helper)
{
  int ret = OB_SUCCESS;
  ObSEArray<JoinHelper, 10> tmp_helpers;
  int64_t best_idx = 0;
  bool need_free = cur_level > 0 && cur_level < size_ - 1;
  if (nullptr == pre_join_tree) {
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_nodes.count(); i ++) {
      JoinHelper *helper = nullptr;
      if (OB_ISNULL(helper = tmp_helpers.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate", K(ret));
      } else {
        helper->right_tree_ = candi_nodes.at(i);
        helper->join_tree_ = candi_nodes.at(i);
      }
    }
  } else {
    JoinHelper join_helper;
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_nodes.count(); i ++) {
      ObJoinOrder *joined_node = candi_nodes.at(i);
      if (OB_FAIL(fill_join_helper(pre_join_tree,
                                   joined_node,
                                   cur_level,
                                   join_helper))) {
        LOG_WARN("failed to check join valid", K(ret));
      } else if (!join_helper.is_valid()) {
        // do nothing
      } else if (OB_FAIL(join_helper.calc_cardinality())) {
        LOG_WARN("failed to calc cardinality", K(ret));
      } else if (OB_FAIL(tmp_helpers.push_back(join_helper))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && tmp_helpers.empty()) {
    ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
    LOG_WARN("failed to get next valid join", K(cur_level), KPC(pre_join_tree));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(greedy_best_join_node(tmp_helpers, greedy_method, best_idx))) {
      LOG_WARN("failed to find best join node", K(ret));
    } else if (OB_FAIL(cur_join_helper.assign(tmp_helpers.at(best_idx)))) {
      LOG_WARN("failed to assign");
    } else if (need_free) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_helpers.count(); i ++) {
        if (i != best_idx &&
            OB_FAIL(free(tmp_helpers.at(i).join_tree_))) {
          LOG_WARN("failed to free", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::greedy_best_join_node(const ObIArray<JoinHelper> &candi_helpers,
                                                      const GreedyParam &greedy_method,
                                                      int64_t &best_idx)
{
  int ret = OB_SUCCESS;
  best_idx = -1;
  for (int64_t i = 0; i < candi_helpers.count(); i ++) {
    if (OB_UNLIKELY(!candi_helpers.at(i).is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid join helper", K(candi_helpers.at(i)));
    } else if (best_idx < 0) {
      best_idx = i;
    } else if (greedy_method.compare(candi_helpers.at(i), candi_helpers.at(best_idx))) {
      best_idx = i;
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::initial_permutation_status()
{
  int ret = OB_SUCCESS;
  unaccepted_levels_.clear_all();
  if (OB_UNLIKELY(join_helpers_.empty()) || OB_UNLIKELY(size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected join helpers", K(ret));
  } else if (OB_FAIL(initial_permutation_.prepare_allocate(size_)) ||
             OB_FAIL(cur_permutation_.prepare_allocate(size_))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else if (OB_FAIL(normal_cost_info_.init(size_)) ||
             OB_FAIL(pipeline_cost_info_.init(size_))) {
    LOG_WARN("failed to init", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size_; i ++) {
    cur_permutation_.at(i) = i;
    initial_permutation_.at(i) = join_helpers_.at(i).right_tree_;
  }
  for (eval_level_ = 0; OB_SUCC(ret) && eval_level_ < size_; eval_level_ ++) {
    if (OB_FAIL(update_best_cost(join_helpers_.at(eval_level_).join_tree_, eval_level_))) {
      LOG_WARN("failed to update best cost", K(ret));
    } else if (OB_FAIL(update_internal_cost(join_helpers_.at(eval_level_).join_tree_))) {
      LOG_WARN("failed to update internal cost", K(ret));
    }
  }
  OPT_TRACE_TITLE("SUCCEED TO INITIALIZE JOIN TREE");
  for (int64_t i = 0; OB_SUCC(ret) && i < initial_permutation_.count(); i ++) {
    OPT_TRACE(initial_permutation_.at(i), "#", i,
              "CARDINALITY:", join_helpers_.at(i).right_tree_->get_output_rows(),
              "BEST COST:", join_helpers_.at(i).right_tree_->get_best_cost());
  }
  trace_best_info();
  OPT_TRACE_TIME_USED;
  OPT_TRACE_MEM_USED;
  return ret;
}

bool ObJoinOrderEnumPermutation::GreedyParamMinJoinCardinality::compare(const JoinHelper &left, const JoinHelper &right) const
{
  bool bret = false;
  int64_t left_type = left.get_join_type_priority();
  int64_t right_type = right.get_join_type_priority();
  // To avoid precision errors caused by the calculation of selectivity, convert to integers for comparison.
  double left_output_rows = round_rows(left.join_tree_->get_output_rows());
  double right_output_rows = round_rows(right.join_tree_->get_output_rows());
  double left_table_rows = round_rows(left.right_tree_->get_output_rows());
  double right_table_rows = round_rows(right.right_tree_->get_output_rows());
  if (left_type != right_type) {
    // we prefer equal join
    bret = left_type < right_type;
  } else if (left_output_rows != right_output_rows) {
    bret = left_output_rows < right_output_rows;
  } else if (left_table_rows != right_table_rows) {
    bret = left_table_rows < right_table_rows;
  } else {
    bret = left.right_tree_->get_best_cost() < right.right_tree_->get_best_cost();
  }
  return bret;
}

// generate next level join tree if the join is legal
int ObJoinOrderEnumPermutation::fill_join_helper(ObJoinOrder *cur_tree,
                                                 ObJoinOrder *next_node,
                                                 const int64_t level,
                                                 JoinHelper &join_helper)
{
  int ret = OB_SUCCESS;
  join_helper.reuse();
  ObJoinOrder *left_tree = cur_tree;
  ObJoinOrder *right_tree = next_node;
  bool match_hint = false;
  bool is_legal = true;
  bool is_strict_order = true;
  bool allow_cross_join = global_contain_normal_nl_;
  ObSEArray<ObConflictDetector *, 4> valid_detectors;
  if (OB_ISNULL(cur_tree) || OB_ISNULL(next_node) ||
      OB_UNLIKELY(cur_tree->get_tables().is_empty()) ||
      OB_UNLIKELY(next_node->get_tables().is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KPC(cur_tree), KPC(next_node));
  } else if (cur_tree->get_tables().overlap(next_node->get_tables())) {
    is_legal = false;
  } else if (ignore_hint_ ||
             (!left_tree->get_tables().overlap(get_leading_tables()) &&
              !right_tree->get_tables().overlap(get_leading_tables()))) {
    // do nothing
  } else if (left_tree->get_tables().is_subset(get_leading_tables()) &&
             right_tree->get_tables().is_subset(get_leading_tables())) {
    match_hint = true;
    is_strict_order = true;
    allow_cross_join = true;
  } else if (left_tree->get_tables().is_superset(get_leading_tables())) {
    match_hint = true;
    is_strict_order = true;
  } else if (right_tree->get_tables().is_superset(get_leading_tables())) {
    match_hint = true;
    is_strict_order = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected join tree", K(ret), KPC(left_tree), KPC(right_tree), K(get_leading_tables()));
  }
  if (OB_SUCC(ret) && is_legal && match_hint && !is_strict_order) {
    std::swap(left_tree, right_tree);
  }
  if (OB_FAIL(ret) || !is_legal) {
  } else if (OB_FAIL(ObConflictDetector::choose_detectors(left_tree->get_tables(),
                                                          right_tree->get_tables(),
                                                          left_tree->get_conflict_detectors(),
                                                          right_tree->get_conflict_detectors(),
                                                          table_depend_infos_,
                                                          conflict_detectors_,
                                                          valid_detectors,
                                                          !allow_cross_join/*delay_cross_product*/,
                                                          is_strict_order))) {
    LOG_WARN("failed to choose join info", K(ret));
  } else if (valid_detectors.empty()) {
    // do nothing
    OPT_TRACE("no valid join info for left tree", left_tree, "join right tree", right_tree);
  } else if (OB_FAIL(ObConflictDetector::merge_join_info(valid_detectors,
                                                         join_helper.join_info_))) {
    LOG_WARN("failed to merge join info", K(ret));
  } else {
    if (!is_strict_order) {
      if (!match_hint) {
        std::swap(left_tree, right_tree);
      } else {
        //如果leading hint指定了join order，但是合法的join order与
        //leading hint指定的join order相反，那么应该把连接类型取反
        join_helper.join_info_.join_type_ = get_opposite_join_type(join_helper.join_info_.join_type_);
      }
    }

    OPT_TRACE_TITLE("Now", left_tree, "join", right_tree, join_helper.join_info_);
    bool is_top = (level == size_ - 1 && level >= 0);
    join_helper.left_tree_ = left_tree;
    join_helper.right_tree_ = right_tree;
    join_helper.force_order_ = match_hint;
    if (FAILEDx(process_join_pred(left_tree,
                                  right_tree,
                                  join_helper.join_info_))) {
      LOG_WARN("failed to preocess join pred", K(ret));
    } else if (is_top && OB_NOT_NULL(cur_top_tree_)) {
      join_helper.join_tree_ = cur_top_tree_;
    } else if (OB_ISNULL(join_helper.join_tree_ = create_join_order(JOIN))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create join tree", K(ret));
    } else if (OB_FAIL(join_helper.join_tree_->init_join_order(left_tree,
                                                               right_tree,
                                                               join_helper.join_info_,
                                                               valid_detectors))) {
      LOG_WARN("failed to init", K(ret));
    } else if (is_top) {
      cur_top_tree_ = join_helper.join_tree_;
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::try_join_two_tree(ObJoinOrder *left_tree,
                                                  ObJoinOrder *right_tree,
                                                  ObJoinOrder *&new_join_tree)
{
  int ret = OB_SUCCESS;
  new_join_tree = nullptr;
  JoinHelper join_helper;
  if (OB_FAIL(fill_join_helper(left_tree,
                               right_tree,
                               /*level = */ -1,
                               join_helper))) {
    LOG_WARN("failed to check join valid", KPC(left_tree), KPC(right_tree));
  } else if (!join_helper.is_valid()) {
    // do nothing
  } else if (OB_FAIL(join_helper.calc_cardinality())) {
    LOG_WARN("failed to calc cardinality", K(ret));
  } else if (OB_FAIL(join_helper.generate_join_paths())) {
    LOG_WARN("failed to generate join paths", K(ret));
  } else if (OB_FAIL(join_helper.compute_join_property())) {
    LOG_WARN("failed to compute join property", K(ret));
  } else {
    new_join_tree = join_helper.join_tree_;
  }
  return ret;
}

int ObJoinOrderEnumPermutation::find_base_level_join_rel(TableItem *table, ObJoinOrder *&join_order)
{
  int ret = OB_SUCCESS;
  join_order = nullptr;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KPC(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && nullptr == join_order && i < base_level_.count(); i ++) {
    ObJoinOrder *rel = base_level_.at(i);
    if (OB_ISNULL(rel)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(base_level_));
    } else if (table->table_id_ == rel->get_table_id()) {
      join_order = rel;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(join_order)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table item", KPC(table), K(base_level_));
  }
  return ret;
}

int ObJoinOrderEnumPermutation::find_base_level_join_rel(const ObRelIds &table_set, ObJoinOrder *&join_order)
{
  int ret = OB_SUCCESS;
  join_order = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && nullptr == join_order && i < base_level_.count(); i ++) {
    ObJoinOrder *rel = base_level_.at(i);
    if (OB_ISNULL(rel)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(base_level_));
    } else if (table_set == rel->get_tables()) {
      join_order = rel;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(join_order)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table", K(table_set), K(base_level_));
  }
  return ret;
}

int ObJoinOrderEnumPermutation::enumerate_permutations()
{
  int ret = OB_SUCCESS;
  int64_t same_prefix = size_;
  int64_t pruning_prefx = size_;
  int64_t enum_cnt = 0;
  int64_t max_permutation = get_plan().get_optimizer_context().get_max_permutation();
  if (OB_UNLIKELY(size_ != cur_permutation_.count()) ||
      OB_UNLIKELY(size_ != join_helpers_.count()) ||
      OB_UNLIKELY(size_ != initial_permutation_.count()) ||
      OB_UNLIKELY(size_ != normal_cost_info_.best_cardinality_.count()) ||
      OB_UNLIKELY(size_ != normal_cost_info_.best_cost_.count()) ||
      OB_UNLIKELY(size_ != pipeline_cost_info_.best_cardinality_.count()) ||
      OB_UNLIKELY(size_ != pipeline_cost_info_.best_cost_.count()) ||
      OB_ISNULL(cur_top_tree_) ||
      OB_UNLIKELY(cur_top_tree_->get_interesting_paths().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), K(size_), KPC(cur_top_tree_));
  } else {
    const double global_best_cost = cur_top_tree_->get_best_cost();
    max_permutation = std::min(max_permutation, static_cast<int64_t>(global_best_cost));
    OPT_TRACE("The Max Enumeration Count is", max_permutation);
  }
  while (OB_SUCC(ret) &&
         get_next_permutation(pruning_prefx, same_prefix) &&
         enum_cnt < max_permutation) {
    enum_cnt ++;
    OPT_TRACE_TITLE("ENUMERATE AND EVAL PERMUTATION #", enum_cnt, ":", cur_permutation_);
    OPT_TRACE_BEGIN_SECTION;
    if (enum_cnt % 20 == 0 &&
        OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status fail", K(ret));
    } else if (OB_FAIL(eval_cur_permutation(same_prefix, pruning_prefx))) {
      LOG_WARN("failed to eval cur permuatetion", K(ret), K(same_prefix), K(enum_cnt));
    }
    OPT_TRACE_END_SECTION;
    OPT_TRACE_TIME_USED;
    OPT_TRACE_MEM_USED;
  }
  if (OB_NOT_NULL(cur_top_tree_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_top_tree_->get_interesting_paths().count(); i ++) {
      Path *path = cur_top_tree_->get_interesting_paths().at(i);
      if (OB_FAIL(check_path_sanity(path))) {
        LOG_WARN("failed to check path", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::check_path_sanity(const Path* path) const
{
  int ret = OB_SUCCESS;
  ObJoinOrder *parent = nullptr;
  if (OB_ISNULL(path) || OB_ISNULL(parent = path->parent_) ||
      OB_UNLIKELY(parent->get_tables().is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(parent), KPC(path), KPC(parent));
  } else if (path->is_join_path()) {
    const JoinPath *join_path = static_cast<const JoinPath *>(path);
    const Path* left_path = join_path->left_path_;
    const Path* right_path = join_path->right_path_;
    ObRelIds tmp_ids;
    if (OB_FAIL(SMART_CALL(check_path_sanity(left_path)))) {
      LOG_WARN("failed to check left path", KPC(path));
    } else if (OB_FAIL(SMART_CALL(check_path_sanity(right_path)))) {
      LOG_WARN("failed to check right path", KPC(path));
    } else if (OB_FAIL(tmp_ids.add_members(left_path->parent_->get_tables())) ||
               OB_FAIL(tmp_ids.add_members(right_path->parent_->get_tables()))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_UNLIKELY(left_path->parent_->get_tables().overlap(right_path->parent_->get_tables())) ||
               OB_UNLIKELY(!tmp_ids.equal(parent->get_tables()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tables", KPC(path), KPC(parent), KPC(left_path->parent_), KPC(right_path->parent_));
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::eval_cur_permutation(int64_t same_prefix, int64_t &pruning_prefx)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  pruning_prefx = cur_permutation_.count();
  eval_level_ = std::min(eval_level_, same_prefix);
  if (OB_UNLIKELY(same_prefix < 0) || OB_UNLIKELY(same_prefix >= size_ - 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), K(same_prefix));
  }
  // free unused join orders
  for (int64_t level = same_prefix; OB_SUCC(ret) && level < size_ - 1; level ++) {
    bool need_free = 0 < level && level < size_ - 1 && unaccepted_levels_.has_member(level);
    if (need_free) {
      if (OB_FAIL(free(get_join_tree(level)))) {
        LOG_WARN("failed to free", K(ret));
      } else {
        join_helpers_.at(level).right_tree_ = nullptr;
      }
   }
  }

  // evaluate new join orders
  for (; OB_SUCC(ret) && is_valid && eval_level_ < size_; eval_level_ ++) {
    ObJoinOrder *next_node = nullptr;
    JoinHelper &join_helper = join_helpers_.at(eval_level_);
    bool is_top = (eval_level_ == size_ - 1);
    OPT_TRACE("EVAL LEVEL", eval_level_, " JOIN TABLE #", cur_permutation_.at(eval_level_));
    if (OB_FAIL(initial_permutation_.at(cur_permutation_.at(eval_level_), next_node))) {
      LOG_WARN("failed to get next node", K(ret), K(cur_permutation_));
    } else if (0 == eval_level_) {
      join_helper.right_tree_ = next_node;
      join_helper.join_tree_ = next_node;
    } else if (OB_FAIL(fill_join_helper(get_join_tree(eval_level_-1),
                                        next_node,
                                        eval_level_,
                                        join_helper))) {
      LOG_WARN("failed to check join valid", K(ret));
    } else if (!join_helpers_.at(eval_level_).is_valid()) {
      is_valid = false;
      pruning_prefx = eval_level_ + 1;
    } else if (OB_FAIL(unaccepted_levels_.add_member(eval_level_))) {
      LOG_WARN("failed to add member", K(eval_level_));
    } else if (OB_FAIL(join_helper.calc_cardinality())) {
      LOG_WARN("failed to calc cardinality", K(ret));
    } else if (OB_FAIL(join_helper.generate_join_paths())) {
      LOG_WARN("failed to generate join paths", K(ret));
    } else if (join_helper.join_tree_->get_latest_interesting_paths().empty()) {
      pruning_prefx = eval_level_ + 1;
      is_valid = false;
    } else if (!is_top && OB_FAIL(join_helper.compute_join_property())) {
      LOG_WARN("failed to compute join property", K(ret));
    } else if (OB_FAIL(update_internal_cost(join_helper.join_tree_))) {
      LOG_WARN("failed to update internal cost", K(ret));
    }
    OPT_TRACE_TIME_USED;
    OPT_TRACE_MEM_USED;
  }

  if (OB_SUCC(ret)) {
    if (is_valid) {
      if (OB_FAIL(update_best_cost())) {
        LOG_WARN("failed to update best cost", K(ret));
      }
      unaccepted_levels_.clear_all();
      OPT_TRACE("accept permutation", cur_permutation_);
    } else {
      OPT_TRACE("ignore permutations begin with", ObArrayWrap<int64_t>(&cur_permutation_.at(0), pruning_prefx));
    }
  }
  trace_best_info();

  return ret;
}

void ObJoinOrderEnumPermutation::trace_best_info()
{
  CHECK_TRACE_ENABLED {
    for (int64_t i = 1; i < size_; i ++) {
      OPT_TRACE("LEVEL #", i);
      OPT_TRACE_BEGIN_SECTION;
      if (normal_cost_info_.best_cardinality_.at(i) < INFINITY) {
        OPT_TRACE("BEST CARDINALITY:", normal_cost_info_.best_cardinality_.at(i));
      }
      if (normal_cost_info_.best_cost_.at(i) < INFINITY) {
        OPT_TRACE("BEST COST:", normal_cost_info_.best_cost_.at(i));
      }
      if (pipeline_cost_info_.best_cardinality_.at(i) < INFINITY) {
        OPT_TRACE("BEST PIPELINE CARDINALITY:", pipeline_cost_info_.best_cardinality_.at(i));
      }
      if (pipeline_cost_info_.best_cost_.at(i) < INFINITY) {
        OPT_TRACE("BEST PIPELINE COST:", pipeline_cost_info_.best_cost_.at(i));
      }
      OPT_TRACE_END_SECTION;
    }
  }
}

int ObJoinOrderEnumPermutation::update_best_cost()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 1; OB_SUCC(ret) && i < size_ ; i ++) {
    ret = update_best_cost(get_join_tree(i), i);
  }
  return ret;
}

int ObJoinOrderEnumPermutation::update_best_cost(const ObJoinOrder *join_tree, int64_t level)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KPC(join_tree));
  } else {
    const ObIArray<Path *> &interesting_paths = join_tree->get_latest_interesting_paths();
    for (int64_t i = 0; OB_SUCC(ret) && i < interesting_paths.count(); i ++) {
      if (OB_FAIL(update_best_cost(interesting_paths.at(i), level))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to update best cost", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::update_best_cost(const Path *path, int64_t level)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(level >= size_) ||
      OB_UNLIKELY(level < 0) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", KPC(path), K(level), K(get_stmt()));
  } else if (path->parent_ == join_helpers_.at(level).join_tree_) {
    bool is_top_level = (size_ - 1 == level);
    JoinOrderCostInfo &cost_info = get_cost_info(path);
    double &best_cost = cost_info.best_cost_.at(level);
    double &best_cardinality = cost_info.best_cardinality_.at(level);
    best_cost = std::min(best_cost, path->get_cost());
    best_cardinality = std::min(best_cardinality, path->get_path_output_rows());
    if (is_top_level) {
      global_contain_normal_nl_ &= static_cast<const JoinPath *>(path)->contain_normal_nl();
      global_has_none_equal_join_ &= static_cast<const JoinPath *>(path)->has_none_equal_join();
      global_has_expansion_join_ &= static_cast<const JoinPath *>(path)->contain_expansion_join();
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::update_internal_cost(const ObJoinOrder *join_order)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_order)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", KPC(join_order));
  } else if (JOIN == join_order->get_type()) {
    const ObIArray<Path *> &interesting_paths = join_order->get_latest_interesting_paths();
    for (int64_t i = 0; OB_SUCC(ret) && i < interesting_paths.count(); i ++) {
      const JoinPath *path = static_cast<const JoinPath *>(interesting_paths.at(i));
      if (OB_ISNULL(path)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KPC(path));
      } else {
        JoinOrderCostInfo &cost_info = get_cost_info(path);
        double *cost_ptr = nullptr;
        if (OB_UNLIKELY(!cost_info.id_cost_map_.created()) &&
            OB_FAIL(cost_info.id_cost_map_.create(RELORDER_HASHBUCKET_SIZE,
                                                  &cost_info.id_cost_map_allocer_,
                                                  &cost_info.bucket_allocator_wrapper_))) {
          LOG_WARN("create hash map failed", K(ret));
        } else if (FALSE_IT(cost_ptr = cost_info.id_cost_map_.get(join_order->get_tables()))) {
        } else if (nullptr == cost_ptr) {
          if (OB_FAIL(cost_info.id_cost_map_.set_refactored(join_order->get_tables(), path->get_cost()))) {
            LOG_WARN("failed to set refactored", K(ret));
          }
        } else if (*cost_ptr > path->get_cost()) {
          *cost_ptr = path->get_cost();
        }
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::prune_path_global(const Path &path, bool &should_add)
{
  int ret = OB_SUCCESS;
  should_add = true;
  if (eval_level_ <= 0 || size_ - 1 <= eval_level_) {
    // do nothing
  } else if (OB_UNLIKELY(size_ <= 1) || OB_ISNULL(path.parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(size_), K(path.parent_), K(ret));
  } else if (OB_UNLIKELY(get_plan().get_optimizer_context().generate_random_plan())) {
    ObQueryCtx* query_ctx = get_plan().get_optimizer_context().get_query_ctx();
    bool random_flag = !OB_ISNULL(query_ctx) && (query_ctx->rand_gen_.get(0, 1) == 1);
    should_add = OB_ISNULL(cur_top_tree_) ||
                 cur_top_tree_->get_interesting_paths().empty() ||
                 random_flag;
    OPT_TRACE("path#", path.path_number_, " is pruned because of random plan");
  } else if (path.is_join_path()) {
    const JoinPath& join_path = static_cast<const JoinPath&>(path);
    JoinOrderCostInfo &cost_info = get_cost_info(&path);
    const double best_cost = cost_info.best_cost_.at(eval_level_);
    const double best_cardinality = cost_info.best_cardinality_.at(eval_level_);
    const double global_best_cost = cost_info.best_cost_.at(size_ - 1);
    const bool not_prune_by_heuristic = !join_path.contain_expansion_join() && global_has_expansion_join_;
    OPT_TRACE("Try prune level", eval_level_, "path#", path.path_number_,
               "global (card:", path.get_path_output_rows(), ", cost:", path.get_cost(), ")");
    if (not_prune_by_heuristic) {
      OPT_TRACE("The join path will not be pruned by heuristic rule");
    }
    if (round_cost(path.get_cost()) >= round_cost(global_best_cost)) {
      should_add = false;
      OPT_TRACE("The join path has a worse cost than the top level");
    } else if (!global_contain_normal_nl_ && join_path.contain_normal_nl()) {
      should_add = false;
      OPT_TRACE("The join path is pruned because of normal nl");
    } else if (!global_has_none_equal_join_ && join_path.has_none_equal_join()) {
      should_add = false;
      OPT_TRACE("The join path is pruned because of none equal join");
    } else if (round_rows(path.get_path_output_rows()) >= round_rows(best_cardinality) &&
               round_cost(path.get_cost()) >= round_cost(best_cost) &&
               !not_prune_by_heuristic) {
      should_add = false;
      OPT_TRACE("The join path has a worse cost and a worse cardinality");
    } else if (cost_info.id_cost_map_.created()) {
      double *cost_ptr = cost_info.id_cost_map_.get(path.parent_->get_tables());
      if (nullptr != cost_ptr && round_cost(path.get_cost()) >= round_cost(*cost_ptr)) {
        should_add = false;
        OPT_TRACE("The join path has a worse cost than cached cost", *cost_ptr);
      }
    }
  }
  return ret;
}

/**
* Get the next permutation in lexicographical order, ignore permutations which has the same pruning_prefix
* same_prefix : return the same prefix as the next permutation
*
* e.g for input: cur_permutation_ = [2,3,6,5,4,1], pruning_prefix = 6
*     the output is : permutation = [2,4,1,3,5,6], same_prefx = 1
*
*     for input: cur_permutation_ = [2,4,1,3,5,6], pruning_prefix = 2
*     the output is : permutation = [2,5,1,3,4,6], same_prefx = 1
*/
bool ObJoinOrderEnumPermutation::get_next_permutation(const int64_t pruning_prefix, int64_t &same_prefix)
{
  bool bret = true;
  same_prefix = 0;
  if (cur_permutation_.count() > 2) {
    if (pruning_prefix < cur_permutation_.count()) {
      ob_sort(&cur_permutation_.at(0) + pruning_prefix,
              &cur_permutation_.at(0) + cur_permutation_.count(),
              std::greater<int64_t>());
    }
    int i = cur_permutation_.count() - 2;
    // 从右向左找第一个升序对 (arr[i] < arr[i+1])
    while (i >= 0 && cur_permutation_[i] >= cur_permutation_[i + 1]) {
      i--;
    }
    if (i < 0) {
      // 已是最大排列
      bret = false;
    } else {
      // 从右向左找第一个大于第 i 位的元素
      int j = cur_permutation_.count() - 1;
      while (cur_permutation_[j] <= cur_permutation_[i]) {
        j--;
      }
      std::swap(cur_permutation_[i], cur_permutation_[j]);
      // 反转右侧子序列为升序
      int left = i + 1, right = cur_permutation_.count() - 1;
      while (left < right) {
        std::swap(cur_permutation_[left++], cur_permutation_[right--]);
      }
      same_prefix = i;
    }

    if (bret && i == 0) {
      // Ignore permutations where the second digit is less than the first digit
      // e.g. the next of [4,7,6,5,3,2,1] will be [5,6,1,2,3,4,7]

      // 从左向右找第一个大于首位的元素
      int j = 1;
      while (j < cur_permutation_.count() &&
             cur_permutation_[j] <= cur_permutation_[0]) {
        j++;
      }
      if (j == cur_permutation_.count()) {
        bret = false;
      } else {
        // 将该元素移动到首位后
        int64_t tmp = cur_permutation_[j];
        while (j > 1) {
          cur_permutation_[j] = cur_permutation_[j-1];
          j --;
        }
        cur_permutation_[j] = tmp;
      }
    }
  } else {
    bret = false;
  }
  // check leading hint
  if (bret && use_leading_ && same_prefix < 1) {
    bret = false;
  }
  return bret;
}

int ObJoinOrderEnumPermutation::JoinHelper::calc_cardinality()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret));
  } else if (NULL == left_tree_) {
    // do nothing
  } else if (OB_FAIL(join_tree_->calc_cardinality(left_tree_,
                                                  right_tree_,
                                                  join_info_))) {
    LOG_WARN("failed to calc cardinality", K(ret));
  }
  return ret;
}

int ObJoinOrderEnumPermutation::JoinHelper::compute_join_property()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret));
  } else if (NULL == left_tree_) {
    // do nothing
  } else if (OB_FAIL(join_tree_->compute_join_property(left_tree_,
                                                       right_tree_,
                                                       join_info_))) {
    LOG_WARN("failed to compute join property", K(ret));
  }
  return ret;
}

int ObJoinOrderEnumPermutation::JoinHelper::generate_join_paths()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret));
  } else if (NULL == left_tree_) {
    // do nothing
  } else {
    bool mock_row_size = join_tree_->get_output_row_size() < 0;
    if (mock_row_size) {
      // mock an unused row size for check
      join_tree_->set_output_row_size(0);
    }
    OPT_TRACE_TITLE("Now generate join path for ", left_tree_, "join", right_tree_, join_info_);
    if (OB_FAIL(join_tree_->generate_join_paths(*left_tree_,
                                                *right_tree_,
                                                join_info_,
                                                force_order_))) {
      LOG_WARN("failed to generate join paths for left_tree",  K(ret));
    }
    if (mock_row_size) {
      join_tree_->set_output_row_size(-1);
    }
  }
  return ret;
}

int ObJoinOrderEnumPermutation::JoinOrderCostInfo::init(int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(best_cardinality_.prepare_allocate(size)) ||
      OB_FAIL(best_cost_.prepare_allocate(size))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size; i ++) {
    best_cardinality_.at(i) = INFINITY;
    best_cost_.at(i) = INFINITY;
  }
  return ret;
}