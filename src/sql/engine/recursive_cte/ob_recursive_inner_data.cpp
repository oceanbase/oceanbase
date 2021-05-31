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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_double_children_phy_operator.h"
#include "sql/engine/set/ob_merge_set_operator.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_recursive_inner_data.h"

namespace oceanbase {
namespace sql {

int ObRecursiveInnerData::init(const ObIArray<int64_t>& cte_pseudo_column_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cte_pseudo_column_row_desc_.init(ObCTEPseudoColumn::CTE_PSEUDO_COLUMN_CNT))) {
    LOG_WARN("Failed to init cte_pseudo_column_row_desc_", K(ret));
  } else if (ObCTEPseudoColumn::CTE_PSEUDO_COLUMN_CNT != cte_pseudo_column_row_desc.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrong cte pseudo column row desc", K(ret), K(cte_pseudo_column_row_desc.count()));
  } else if (OB_FAIL(cte_pseudo_column_row_desc_.assign(cte_pseudo_column_row_desc))) {
    LOG_WARN("Failed to assign pseudo row desc", K(ret));
  } else if (OB_FAIL(dfs_pump_.init())) {
    LOG_WARN("Failed to init depth first search pump", K(ret));
  }
  return ret;
}

int ObRecursiveInnerData::add_search_column(ObSortColumn col)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(search_by_col_lists_.push_back(col))) {
    LOG_WARN("Add search col info failed", K(ret));
  } else if (OB_FAIL(dfs_pump_.add_sort_column(col))) {
    LOG_WARN("Add search col info failed", K(ret));
  } else if (OB_FAIL(bfs_pump_.add_sort_column(col))) {
    LOG_WARN("Add search col info failed", K(ret));
  }
  return ret;
}

int ObRecursiveInnerData::add_cycle_column(common::ObColumnInfo col)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cycle_by_col_lists_.push_back(col))) {
    LOG_WARN("Add cycle col info failed", K(ret));
  } else if (OB_FAIL(dfs_pump_.add_cycle_by_column(col))) {
    LOG_WARN("Add cycle col info failed", K(ret));
  } else if (OB_FAIL(bfs_pump_.add_cycle_by_column(col))) {
    LOG_WARN("Add cycle col info failed", K(ret));
  }
  return ret;
}

int ObRecursiveInnerData::get_all_data_from_left_child(ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = nullptr;
  uint64_t left_rows_count = 0;
  if (OB_ISNULL(left_op_) || OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left_op_ or right_op_ is nullptr", K(ret), K(left_op_), K(right_op_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(left_op_->get_next_row(exec_ctx, input_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Failed to get next row", K(ret));
        }
      } else {
        LOG_DEBUG("Get rows from left op: ", KPC(input_row), K(left_op_->get_type()));
        ++left_rows_count;
        if (SearchStrategyType::BREADTH_FRIST == search_type_) {
          if (OB_FAIL(bfs_pump_.add_row(input_row))) {
            LOG_WARN("Failed to add row", K(ret));
          }
        } else if (SearchStrategyType::DEPTH_FRIST == search_type_) {
          if (OB_FAIL(dfs_pump_.add_row(input_row))) {
            LOG_WARN("Failed to add row", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected search strategy", K(ret), K(search_type_));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = (left_rows_count == 0) ? OB_ITER_END : OB_SUCCESS;
  }
  return ret;
}

int ObRecursiveInnerData::get_all_data_from_right_child(ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = nullptr;
  if (OB_ISNULL(left_op_) || OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left_op_ or right_op_ is nullptr", K(ret), K(left_op_), K(right_op_));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(right_op_->get_next_row(exec_ctx, input_row))) {
      LOG_DEBUG("Get rows from right op: ", K(ret), KPC(input_row), K(right_op_->get_type()));
      if (OB_ITER_END == ret) {
      } else if (OB_SUCC(ret) && SearchStrategyType::BREADTH_FRIST == search_type_) {
        LOG_DEBUG("Get rows from right op: ", KPC(input_row));
        if (OB_FAIL(bfs_pump_.add_row(input_row))) {
          LOG_WARN("Failed to add row", K(ret));
        }
      } else if (OB_SUCC(ret) && SearchStrategyType::DEPTH_FRIST == search_type_) {
        LOG_DEBUG("Get rows from right op: ", KPC(input_row));
        if (OB_FAIL(dfs_pump_.add_row(input_row))) {
          LOG_WARN("Failed to add row", K(ret));
        }
      } else {
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObRecursiveInnerData::try_format_output_row(const ObNewRow*& output_row)
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  // try to get row again
  if (!result_output_.empty()) {
    if (OB_FAIL(result_output_.pop_front(result_node))) {
      LOG_WARN("Get result output failed", K(ret));
    } else if (OB_ISNULL(result_node.row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get a null result output", K(ret));
    }
  } else {
    ret = OB_ITER_END;
  }
  // try format row
  if (OB_SUCC(ret)) {
    LOG_DEBUG("Result row", KPC(result_node.row_));
    if (OB_FAIL(assign_to_cur_row(*cur_row_, *result_node.row_))) {
      LOG_WARN("Failed to assign input row to cur row", K(ret));
    } else if (OB_FAIL(add_pseudo_column(cur_row_, result_node.is_cycle_))) {
      LOG_WARN("Add pseudo column failed", K(ret));
    } else {
      output_row = cur_row_;
      LOG_DEBUG("Format row", K(cte_pseudo_column_row_desc_));
    }
  }
  return ret;
}

int ObRecursiveInnerData::depth_first_union(ObExecContext& exec_ctx, const bool sort /*=true*/)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  if (OB_FAIL(dfs_pump_.finish_add_row(sort))) {
    LOG_WARN("Failed to add row", K(ret));
  } else if (OB_FAIL(set_fake_cte_table_empty(exec_ctx))) {
    LOG_WARN("Set fake cte table to empty failed", K(ret));
  } else if (OB_FAIL(dfs_pump_.get_next_non_cycle_node(result_output_, node))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Failed to get next non cycle node", K(ret));
    }
  } else if (OB_FAIL(fake_cte_table_add_row(exec_ctx, node))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  }
  return ret;
}

int ObRecursiveInnerData::fake_cte_table_add_row(ObExecContext& exec_ctx, ObTreeNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fake cte table op can not be nullptr", K(ret));
  } else if (OB_FAIL(pump_operator_->add_row(exec_ctx, node.row_))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  } else if (SearchStrategyType::BREADTH_FRIST == search_type_ && OB_FAIL(bfs_pump_.update_parent_node(node))) {
    LOG_WARN("Failed to update last bst node stask", K(ret), K(node));
  } else if (SearchStrategyType::DEPTH_FRIST == search_type_ && OB_FAIL(dfs_pump_.adjust_stack(node))) {
    LOG_WARN("Failed to adjust stask", K(ret), K(node));
  }
  return ret;
}

int ObRecursiveInnerData::breadth_first_union(ObExecContext& exec_ctx, bool left_branch, bool& continue_search)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  if (OB_FAIL(set_fake_cte_table_empty(exec_ctx))) {
    LOG_WARN("Set fake cte table to empty failed", K(ret));
  } else if (OB_FAIL(bfs_pump_.add_result_rows())) {
    LOG_WARN("Failed to finish add row", K(ret));
  } else if (OB_FAIL(bfs_pump_.get_next_non_cycle_node(result_output_, node))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      continue_search = false;
      if (OB_FAIL(start_new_level(exec_ctx, left_branch))) {
        LOG_WARN("Failed to start new level", K(ret));
      }
    } else {
      LOG_WARN("Failed to get next non cycle node", K(ret));
    }
  } else if (OB_FAIL(fake_cte_table_add_row(exec_ctx, node))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  } else if (search_by_col_lists_.empty()) {
    /**
     *  when sort_collations_ is not empty, we need to sort all sibling nodes,
     *  so continue_search is true.
     *  when sort_collations_ is empty, we can output each node when it's generated,
     *  so continue_search is false.
     */
    continue_search = false;
  }
  return ret;
}

int ObRecursiveInnerData::start_new_level(ObExecContext& exec_ctx, bool left_branch)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  if (OB_FAIL(bfs_pump_.finish_add_row(!left_branch))) {
    LOG_WARN("Failed to finish add row", K(ret));
  } else if (OB_FAIL(bfs_pump_.get_next_non_cycle_node(result_output_, node))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      state_ = RecursiveUnionState::R_UNION_END;
    } else {
      LOG_WARN("Failed to get next non cycle node", K(ret));
    }
  } else if (OB_FAIL(fake_cte_table_add_row(exec_ctx, node))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  }
  return ret;
}

int ObRecursiveInnerData::try_get_left_rows(ObExecContext& exec_ctx, const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  const bool sort = false;
  if (OB_FAIL(get_all_data_from_left_child(exec_ctx))) {
    if (OB_ITER_END == ret) {
      // do nothing
    } else {
      LOG_WARN("Get row from left child failed", K(ret));
    }
  } else {
    if (SearchStrategyType::BREADTH_FRIST == search_type_) {
      bool continue_search = false;
      if (OB_FAIL(breadth_first_union(exec_ctx, true, continue_search))) {
        LOG_WARN("Breadth first union failed", K(ret));
      }
    } else if (SearchStrategyType::DEPTH_FRIST == search_type_) {
      if (OB_FAIL(depth_first_union(exec_ctx, sort))) {
        LOG_WARN("Depth first union failed", K(ret));
      }
    } else {
      // never get there
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_format_output_row(row))) {
      LOG_WARN("Failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObRecursiveInnerData::try_get_right_rows(ObExecContext& exec_ctx, const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  if (SearchStrategyType::DEPTH_FRIST == search_type_) {
    if (OB_FAIL(get_all_data_from_right_child(exec_ctx))) {
      LOG_WARN("Get row from right child failed", K(ret));
    } else if (OB_FAIL(right_op_->rescan(exec_ctx))) {
      LOG_WARN("Recursive union right children rescan failed", K(ret));
    } else if (dfs_pump_.empty()) {
      // do nothing
    } else if (OB_FAIL(depth_first_union(exec_ctx))) {
      LOG_WARN("Depth first union failed", K(ret));
    }
  } else if (SearchStrategyType::BREADTH_FRIST == search_type_) {
    bool continue_search = true;
    while (OB_SUCC(ret) && continue_search) {
      if (OB_FAIL(get_all_data_from_right_child(exec_ctx))) {
        LOG_WARN("Get row from right child failed", K(ret));
      } else if (OB_FAIL(right_op_->rescan(exec_ctx))) {
        LOG_WARN("Recursive union right children rescan failed", K(ret));
      } else if (bfs_pump_.empty()) {
        break;
      } else if (OB_FAIL(breadth_first_union(exec_ctx, false, continue_search))) {
        LOG_WARN("Breadth first union failed", K(ret));
      }
    }
  }
  // try to get row again
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_format_output_row(row))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Failed to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObRecursiveInnerData::get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!result_output_.empty()) {
    if (OB_FAIL(try_format_output_row(row))) {
      LOG_WARN("Format output row failed", K(ret));
    } else {
    }
  } else if (RecursiveUnionState::R_UNION_READ_LEFT == state_) {
    if (OB_FAIL(try_get_left_rows(exec_ctx, row))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get left rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else {
      state_ = R_UNION_READ_RIGHT;
    }
  } else if (RecursiveUnionState::R_UNION_READ_RIGHT == state_) {
    if (OB_FAIL(try_get_right_rows(exec_ctx, row))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get right rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else {
    }
  } else if (RecursiveUnionState::R_UNION_END == state_) {
    ret = OB_ITER_END;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), K(state_));
  }
  return ret;
}

int ObRecursiveInnerData::add_pseudo_column(const ObNewRow* output_row, bool cycle /*default false*/)
{
  int ret = OB_SUCCESS;
  if (!search_by_col_lists_.empty()) {
    if (OB_ISNULL(output_row) || OB_UNLIKELY(!output_row->is_valid()) || OB_ISNULL(calc_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid parameter", K(ret), KPC(output_row));
    } else if (OB_UNLIKELY(ObCTEPseudoColumn::CTE_PSEUDO_COLUMN_CNT != cte_pseudo_column_row_desc_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid pseudo_column_row_desc", K(ret));
    } else if (ObMergeSetOperator::UNUSED_POS != cte_pseudo_column_row_desc_[ObCTEPseudoColumn::CTE_SEARCH]) {
      int64_t cell_idx = cte_pseudo_column_row_desc_[ObCTEPseudoColumn::CTE_SEARCH];
      if (cell_idx > output_row->count_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid cell idx", K(ret), K(cell_idx), K(output_row->count_));
      } else {
        ObObj val;
        int64_t cur_level = ordering_column_;
        common::number::ObNumber num;
        if (OB_FAIL(num.from(cur_level, *calc_buf_))) {
          LOG_WARN("Failed to create ObNumber", K(ret));
        } else {
          val.set_number(num);
        }
        output_row->cells_[cell_idx] = val;
        ++ordering_column_;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, search by clause without pseudo info", K(ret));
    }
  }
  if (OB_SUCC(ret) && !cycle_by_col_lists_.empty()) {
    if (OB_ISNULL(output_row) || OB_UNLIKELY(!output_row->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid parameter", K(ret), KPC(output_row));
    } else if (OB_UNLIKELY(ObCTEPseudoColumn::CTE_PSEUDO_COLUMN_CNT != cte_pseudo_column_row_desc_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid pseudo_column_row_desc", K(ret));
    } else if (ObMergeSetOperator::UNUSED_POS != cte_pseudo_column_row_desc_[ObCTEPseudoColumn::CTE_CYCLE]) {
      int64_t cell_idx = cte_pseudo_column_row_desc_[ObCTEPseudoColumn::CTE_CYCLE];
      if (cell_idx > output_row->count_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid cell idx", K(cell_idx), K(output_row->count_), K(ret));
      } else {
        output_row->cells_[cell_idx] = cycle ? cycle_value_ : non_cycle_value_;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, cycle by clause without pseudo info", K(ret));
    }
  }
  return ret;
}

int ObRecursiveInnerData::rescan(ObExecContext& ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  state_ = R_UNION_READ_LEFT;
  result_output_.reset();
  ordering_column_ = 1;
  dfs_pump_.reuse();
  bfs_pump_.reuse();
  return ret;
}

int ObRecursiveInnerData::set_fake_cte_table_empty(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The fake cte table is null", K(ret));
  } else if (OB_FAIL(pump_operator_->set_empty(ctx))) {
    LOG_WARN("Set fake cte table empty failed", K(ret));
  }
  return ret;
}

void ObRecursiveInnerData::destroy()
{
  stored_row_buf_.reset();
}

int ObRecursiveInnerData::assign_to_cur_row(ObNewRow& cur_row, const ObNewRow& input_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_row.count_ < input_row.projector_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "Cur column count is not enough to store child row", K(ret), K_(cur_row.count), K_(input_row.projector_size));
  } else {
    for (int64_t i = 0; i < input_row.get_count(); ++i) {
      cur_row.cells_[i] = input_row.get_cell(i);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
