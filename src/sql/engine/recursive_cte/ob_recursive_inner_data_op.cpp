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
#include "ob_recursive_union_all_op.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_recursive_inner_data_op.h"

namespace oceanbase {
namespace sql {

int ObRecursiveInnerDataOp::init(const ObExpr* search_expr, const ObExpr* cycle_expr)
{
  int ret = OB_SUCCESS;
  search_expr_ = search_expr;
  cycle_expr_ = cycle_expr;
  if (OB_FAIL(dfs_pump_.init())) {
    LOG_WARN("Failed to init depth first search pump", K(ret));
  }
  return ret;
}

int ObRecursiveInnerDataOp::get_all_data_from_left_child()
{
  int ret = OB_SUCCESS;
  uint64_t left_rows_count = 0;
  if (OB_ISNULL(left_op_) || OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left_op_ or right_op_ is nullptr", K(ret), K(left_op_), K(right_op_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(left_op_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Failed to get next row", K(ret));
        }
      } else {
        ++left_rows_count;
        if (SearchStrategyType::BREADTH_FRIST == search_type_) {
          if (OB_FAIL(bfs_pump_.add_row(left_op_->get_spec().output_, eval_ctx_))) {
            LOG_WARN("Failed to add row", K(ret));
          }
        } else if (SearchStrategyType::DEPTH_FRIST == search_type_) {
          if (OB_FAIL(dfs_pump_.add_row(left_op_->get_spec().output_, eval_ctx_))) {
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
    // no rows from left child, end immediately.
    ret = (left_rows_count == 0) ? OB_ITER_END : OB_SUCCESS;
  }
  return ret;
}

int ObRecursiveInnerDataOp::get_all_data_from_right_child()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_op_) || OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left_op_ or right_op_ is nullptr", K(ret), K(left_op_), K(right_op_));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(right_op_->get_next_row())) {
      if (OB_ITER_END == ret) {
      } else if (OB_SUCC(ret) && SearchStrategyType::BREADTH_FRIST == search_type_) {
        if (OB_FAIL(bfs_pump_.add_row(right_op_->get_spec().output_, eval_ctx_))) {  // add stored row
          LOG_WARN("Failed to add row", K(ret));
        }
      } else if (OB_SUCC(ret) && SearchStrategyType::DEPTH_FRIST == search_type_) {
        if (OB_FAIL(dfs_pump_.add_row(right_op_->get_spec().output_, eval_ctx_))) {
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

int ObRecursiveInnerDataOp::try_format_output_row()
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  // try to get row again
  if (!result_output_.empty()) {
    if (OB_FAIL(result_output_.pop_front(result_node))) {
      LOG_WARN("Get result output failed", K(ret));
    } else if (OB_ISNULL(result_node.stored_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get a null result output", K(ret));
    }
  } else {
    ret = OB_ITER_END;
  }
  // try format row
  if (OB_SUCC(ret)) {
    if (OB_FAIL(assign_to_cur_row(result_node.stored_row_))) {
      LOG_WARN("Failed to assign input row to cur row", K(ret));
    } else if (OB_FAIL(add_pseudo_column(result_node.is_cycle_))) {
      LOG_WARN("Add pseudo column failed", K(ret));
    }
  }
  return ret;
}

int ObRecursiveInnerDataOp::depth_first_union(const bool sort /*=true*/)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  if (OB_FAIL(dfs_pump_.finish_add_row(sort))) {
    LOG_WARN("Failed to add row", K(ret));
  } else if (OB_FAIL(set_fake_cte_table_empty())) {
    LOG_WARN("Set fake cte table to empty failed", K(ret));
  } else if (OB_FAIL(dfs_pump_.get_next_non_cycle_node(result_output_, node))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Failed to get next non cycle node", K(ret));
    }
  } else if (OB_FAIL(fake_cte_table_add_row(node))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  }
  return ret;
}

int ObRecursiveInnerDataOp::fake_cte_table_add_row(ObTreeNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fake cte table op can not be nullptr", K(ret));
  } else if (OB_FAIL(pump_operator_->add_row(node.stored_row_))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  } else if (SearchStrategyType::BREADTH_FRIST == search_type_ && OB_FAIL(bfs_pump_.update_parent_node(node))) {
    LOG_WARN("Failed to update last bst node stask", K(ret), K(node));
  } else if (SearchStrategyType::DEPTH_FRIST == search_type_ && OB_FAIL(dfs_pump_.adjust_stack(node))) {
    LOG_WARN("Failed to adjust stask", K(ret), K(node));
  }
  return ret;
}

int ObRecursiveInnerDataOp::breadth_first_union(bool left_branch, bool& continue_search)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  if (OB_FAIL(set_fake_cte_table_empty())) {
    LOG_WARN("Set fake cte table to empty failed", K(ret));
  } else if (OB_FAIL(bfs_pump_.add_result_rows())) {
    LOG_WARN("Failed to finish add row", K(ret));
  } else if (OB_FAIL(bfs_pump_.get_next_non_cycle_node(result_output_, node))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      continue_search = false;
      if (OB_FAIL(start_new_level(left_branch))) {
        LOG_WARN("Failed to start new level", K(ret));
      }
    } else {
      LOG_WARN("Failed to get next non cycle node", K(ret));
    }
  } else if (OB_FAIL(fake_cte_table_add_row(node))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  } else if (sort_collations_.empty()) {
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

int ObRecursiveInnerDataOp::start_new_level(bool left_branch)
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
  } else if (OB_FAIL(fake_cte_table_add_row(node))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  }
  return ret;
}

int ObRecursiveInnerDataOp::try_get_left_rows()
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  const bool sort = false;
  if (OB_FAIL(get_all_data_from_left_child())) {
    if (OB_ITER_END == ret) {
      // do nothing
    } else {
      LOG_WARN("Get row from left child failed", K(ret));
    }
  } else {
    if (SearchStrategyType::BREADTH_FRIST == search_type_) {
      bool continue_search = false;
      if (OB_FAIL(breadth_first_union(true, continue_search))) {
        LOG_WARN("Breadth first union failed", K(ret));
      }
    } else if (SearchStrategyType::DEPTH_FRIST == search_type_) {
      if (OB_FAIL(depth_first_union(sort))) {
        LOG_WARN("Depth first union failed", K(ret));
      }
    } else {
      // never get there
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_format_output_row())) {
      LOG_WARN("Failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObRecursiveInnerDataOp::try_get_right_rows()
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  if (SearchStrategyType::DEPTH_FRIST == search_type_) {
    if (OB_FAIL(get_all_data_from_right_child())) {
      LOG_WARN("Get row from right child failed", K(ret));
    } else if (OB_FAIL(right_op_->rescan())) {
      LOG_WARN("Recursive union right children rescan failed", K(ret));
    } else if (dfs_pump_.empty()) {
      // do nothing
    } else if (OB_FAIL(depth_first_union())) {
      LOG_WARN("Depth first union failed", K(ret));
    }
  } else if (SearchStrategyType::BREADTH_FRIST == search_type_) {
    bool continue_search = true;
    while (OB_SUCC(ret) && continue_search) {
      if (OB_FAIL(get_all_data_from_right_child())) {
        LOG_WARN("Get row from right child failed", K(ret));
      } else if (OB_FAIL(right_op_->rescan())) {
        LOG_WARN("Recursive union right children rescan failed", K(ret));
      } else if (bfs_pump_.empty()) {
        break;
      } else if (OB_FAIL(breadth_first_union(false, continue_search))) {
        LOG_WARN("Breadth first union failed", K(ret));
      }
    }
  }
  // try to get row again
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_format_output_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Failed to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObRecursiveInnerDataOp::get_next_row()
{
  int ret = OB_SUCCESS;
  if (!result_output_.empty()) {
    if (OB_FAIL(try_format_output_row())) {
      LOG_WARN("Format output row failed", K(ret));
    } else {
    }
  } else if (RecursiveUnionState::R_UNION_READ_LEFT == state_) {
    if (OB_FAIL(try_get_left_rows())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get left rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else {
      state_ = R_UNION_READ_RIGHT;
    }
  } else if (RecursiveUnionState::R_UNION_READ_RIGHT == state_) {
    if (OB_FAIL(try_get_right_rows())) {
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

int ObRecursiveInnerDataOp::add_pseudo_column(bool cycle /*default false*/)
{
  int ret = OB_SUCCESS;
  if (nullptr != search_expr_) {
    int64_t cur_level = ordering_column_;
    common::number::ObNumber res_num;
    char buf_alloc[common::number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, common::number::ObNumber::MAX_BYTE_LEN);
    if (OB_FAIL(res_num.from(cur_level, allocator))) {
      LOG_WARN("fail to create obnumber", K(ret));
    } else {
      search_expr_->locate_datum_for_write(eval_ctx_).set_number(res_num);
      search_expr_->get_eval_info(eval_ctx_).evaluated_ = true;
      ++ordering_column_;
    }
  } else {
    // there is no search column or it's not in the output.
  }
  if (nullptr != cycle_expr_) {
    if (OB_FAIL(cycle_expr_->deep_copy_datum(eval_ctx_, cycle ? cycle_value_ : non_cycle_value_))) {
      LOG_WARN("expr datum deep copy failed", K(ret));
    } else {
      cycle_expr_->get_eval_info(eval_ctx_).evaluated_ = true;
    }
  }
  return ret;
}

int ObRecursiveInnerDataOp::rescan()
{
  int ret = OB_SUCCESS;
  state_ = R_UNION_READ_LEFT;
  result_output_.reset();
  ordering_column_ = 1;
  dfs_pump_.reuse();
  bfs_pump_.reuse();
  return ret;
}

int ObRecursiveInnerDataOp::set_fake_cte_table_empty()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The fake cte table is null", K(ret));
  } else {
    pump_operator_->set_empty();
  }
  return ret;
}

void ObRecursiveInnerDataOp::destroy()
{
  stored_row_buf_.reset();
}

int ObRecursiveInnerDataOp::assign_to_cur_row(ObChunkDatumStore::StoredRow* stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stored_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stored row is null", K(stored_row));
  } else {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stored_row->to_expr(output_union_exprs_, eval_ctx_))) {
        LOG_WARN("stored row to exprs failed", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
