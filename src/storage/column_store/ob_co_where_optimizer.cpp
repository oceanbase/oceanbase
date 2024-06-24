/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_co_where_optimizer.h"
#include "ob_column_oriented_sstable.h"
#include "ob_column_store_util.h"
#include "sql/engine/basic/ob_pushdown_filter.h"

namespace oceanbase
{
namespace storage
{

ObCOWhereOptimizer::ObCOWhereOptimizer(
    ObCOSSTableV2 &co_sstable,
    sql::ObPushdownFilterExecutor &filter)
    : co_sstable_(co_sstable)
    , filter_(filter)
    , filter_conditions_()
{
}

int ObCOWhereOptimizer::analyze()
{
    return analyze_impl(filter_);
}

int ObCOWhereOptimizer::analyze_impl(sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  sql::ObPushdownFilterExecutor **children = filter.get_childs();
  const uint32_t child_cnt = filter.get_child_count();

  if (filter.is_logic_op_node()) {
    for (uint32_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      if (OB_FAIL(analyze_impl(*children[i]))) {
        LOG_WARN("Failed to analyze filter tree", K(ret), K(i), KP(children[i]));
      }
    }
  }

  if (OB_SUCC(ret) && filter.is_logic_and_node()) {
    bool reorder = true;
    for (uint32_t i = 0; reorder && i < child_cnt; ++i) {
        sql::ObPushdownFilterExecutor &child_filter = *children[i];
        // if (child_filter.is_filter_dynamic_node()) {
        //     reorder = false;
        //     break;
        // }

        if (!child_filter.is_filter_node()) {
            reorder = false;
            break;
        }
    }

    if (reorder) {
      sql::ObPushdownFilterExecutor *best_filter = nullptr;
      if (OB_FAIL(filter_conditions_.prepare_allocate(child_cnt))) {
        LOG_WARN("Failed to prepare allocate filter conditions", K(ret), K(child_cnt));
      } else {
        for (uint32_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
          sql::ObPushdownFilterExecutor &child_filter = *children[i];
          ObFilterCondition &filter_condition = filter_conditions_[i];
          filter_condition.idx_ = i;
          if (OB_FAIL(collect_filter_info(child_filter, filter_condition))) {
            LOG_WARN("Failed to collect filter into", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          lib::ob_sort(&filter_conditions_[0], &filter_conditions_[0] + child_cnt);
          const uint64_t best_filter_idx = filter_conditions_[0].idx_;
          best_filter = children[best_filter_idx];
          if (0 == best_filter_idx ||
              !can_choose_best_filter(&filter_conditions_[0], *best_filter, filter)) {
          } else {
            for (uint32_t i = best_filter_idx; i > 0; i--) {
              children[i] = children[i - 1];
            }
            children[0] = best_filter;
          }
        }
      }
    }
  }

  return ret;
}

int ObCOWhereOptimizer::collect_filter_info(
    sql::ObPushdownFilterExecutor &filter,
    ObFilterCondition &filter_condition)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<uint32_t> &cg_idxes = filter.get_cg_idxs();
  const int64_t column_cnt = cg_idxes.count();
  filter_condition.columns_cnt_ = column_cnt;
  const ObCOSSTableMeta &cs_meta = co_sstable_.get_cs_meta();

  uint64_t columns_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i)
  {
    const uint32_t cg_idx = cg_idxes.at(i);
    ObSSTableWrapper cg_table_wrapper;
    if (is_virtual_cg(cg_idx) || cg_idx >= cs_meta.column_group_cnt_) {
    } else if (OB_FAIL(co_sstable_.fetch_cg_sstable(cg_idx, cg_table_wrapper))) {
      LOG_WARN("Failed to fetch cg sstable", K(ret));
    } else {
      columns_size += cg_table_wrapper.get_sstable()->get_total_macro_block_count();
    }
  }

  if (OB_SUCC(ret)) {
    filter_condition.columns_size_ = columns_size;
    if (0 == columns_size) {
      filter_condition.columns_cnt_ = 0;
    }
    filter_condition.execution_cost_ = estimate_execution_cost(filter);
  }

  return ret;
}

uint64_t ObCOWhereOptimizer::estimate_execution_cost(sql::ObPushdownFilterExecutor &filter)
{
  using namespace sql;
  uint64_t execution_cost;
  if (!filter.is_filter_white_node()) {
    execution_cost = UINT64_MAX;
  } else if (filter.is_filter_dynamic_node()) {
    ObDynamicFilterExecutor &dynamic_filter = static_cast<ObDynamicFilterExecutor &>(filter);
    if (dynamic_filter.get_filter_node().get_dynamic_filter_type()
        == DynamicFilterType::PD_TOPN_FILTER) {
      execution_cost = 1;
    } else {
      execution_cost = UINT64_MAX;
    }
  } else {
    ObWhiteFilterExecutor &white_filter = static_cast<ObWhiteFilterExecutor &>(filter);
    switch (white_filter.get_op_type()) {
      case WHITE_OP_EQ:
      case WHITE_OP_LE:
      case WHITE_OP_LT:
      case WHITE_OP_GE:
      case WHITE_OP_GT:
      case WHITE_OP_NE:
        if (!is_lob_storage(white_filter.get_filter_node().column_exprs_.at(0)->obj_meta_.get_type())
            && white_filter.get_datums().at(0).len_ <= sizeof(uint64_t)) {
          execution_cost = 1;
        } else {
          execution_cost = UINT64_MAX;
        }
        break;
      case WHITE_OP_NU:
      case WHITE_OP_NN:
        execution_cost = 1;
        break;
      default:
        execution_cost = UINT64_MAX;
        break;
    }
  }

  return execution_cost;
}

bool ObCOWhereOptimizer::can_choose_best_filter(
    ObFilterCondition *best_filter_condition,
    sql::ObPushdownFilterExecutor &best_filter,
    sql::ObPushdownFilterExecutor &parent_filter)
{
  bool ret = true;
  sql::ObPushdownFilterExecutor **children = parent_filter.get_childs();
  const uint32_t child_cnt = parent_filter.get_child_count();
  const uint32_t best_cg_idx = best_filter.get_cg_idxs().at(0);
  ObFilterCondition *second_best_filter_condition = best_filter_condition + 1;

  if (best_filter_condition->execution_cost_ < second_best_filter_condition->execution_cost_
      || best_filter_condition->columns_size_ * 2 < second_best_filter_condition->columns_size_
      || best_filter_condition->columns_cnt_ * 2 < second_best_filter_condition->columns_cnt_) {
    for (uint32_t i = 1; i < child_cnt; ++i) {
      sql::ObPushdownFilterExecutor *filter = children[best_filter_condition[i].idx_];
      const common::ObIArray<uint32_t> &cg_idxes = filter->get_cg_idxs();
      if (is_contain(cg_idxes, best_cg_idx)) {
        ret = false;
        break;
      }
    }
  } else {
    ret = false;
  }

  return ret;
}

}
}
