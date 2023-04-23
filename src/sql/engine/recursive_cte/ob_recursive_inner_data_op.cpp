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

namespace oceanbase
{
namespace sql
{

int ObRecursiveInnerDataOp::init(const ObExpr *search_expr, const ObExpr *cycle_expr)
{
  int ret = OB_SUCCESS;
  search_expr_ = search_expr;
  cycle_expr_ = cycle_expr;

  if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session info is null", K(ret));
  } else if (OB_FAIL(dfs_pump_.init())) {
    LOG_WARN("Failed to init depth first search pump", K(ret));
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_CTE_MAX_RECURSION_DEPTH, max_recursion_depth_))) {
    LOG_WARN("Get sys variable error", K(ret));
  } else {
    int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    stored_row_buf_.set_tenant_id(tenant_id);
    stored_row_buf_.set_ctx_id(ObCtxIds::WORK_AREA);
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
    // 左边一行都没有，整个query直接就结束了
    ret = (left_rows_count == 0) ? OB_ITER_END : OB_SUCCESS;
  }
  return ret;
}

// Batch mode API of get_all_data_from_left_child
int ObRecursiveInnerDataOp::get_all_data_from_left_batch()
{
  int ret = OB_SUCCESS;
  bool all_skiped = true;
  const ObBatchRows * child_brs = nullptr;
  //ObSearchMethodOp *pump = nullptr;
  if (OB_ISNULL(left_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left_op_ is nullptr", K(ret), K(left_op_));
  } else {
    ObSearchMethodOp *pump = get_search_method_bump();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(left_op_->get_next_batch(batch_size_, child_brs))) {
        LOG_WARN("Failed to get next batch", K(ret));
      } else {
        // child_brs should NEVER be null. generate core if child return nullptr
        all_skiped &= child_brs->skip_->is_all_true(child_brs->size_);
        ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
        guard.set_batch_size(child_brs->size_);
        LOG_DEBUG("child batch_size is", KPC(child_brs));
        for (auto i = 0; OB_SUCC(ret) && i < child_brs->size_; i++) {
          if (child_brs->skip_->at(i)) {
            continue;
          }
          guard.set_batch_idx(i);
          if (OB_FAIL(pump->add_row(left_op_->get_spec().output_, eval_ctx_))) {
            LOG_WARN("Failed to add row", K(ret));
          }
        }
      }
      if (child_brs->end_) {
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    // end whole iteration if NO rows returned from left branch
    ret = all_skiped ? OB_ITER_END : OB_SUCCESS;
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
        if (OB_FAIL(bfs_pump_.add_row(right_op_->get_spec().output_, eval_ctx_))) { //add stored row
          LOG_WARN("Failed to add row", K(ret));
        }
      } else if (OB_SUCC(ret) && SearchStrategyType::DEPTH_FRIST == search_type_) {
        if (OB_FAIL(dfs_pump_.add_row(right_op_->get_spec().output_, eval_ctx_))) {
          LOG_WARN("Failed to add row", K(ret));
        }
      } else { }
    }
  }
  if (OB_ITER_END == ret) {
    // 右儿子行取完只是表明本轮执行完成
    ret = OB_SUCCESS;
  }
  return ret;
}

// Batch mode API of get_all_data_from_right_child
int ObRecursiveInnerDataOp::get_all_data_from_right_batch()
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  if (OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Right_op_ is nullptr", K(ret), K(right_op_));
  } else {
    ObSearchMethodOp *pump = get_search_method_bump();
    while (OB_SUCC(ret) &&
           OB_SUCC(right_op_->get_next_batch(batch_size_, child_brs))) {
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      guard.set_batch_size(child_brs->size_);
      LOG_DEBUG("child batch_size info", KPC(child_brs));
      for (auto i = 0; OB_SUCC(ret) && i < child_brs->size_; i++) {
        if (child_brs->skip_->at(i)) {
          continue;
        }
        guard.set_batch_idx(i);
        if (OB_FAIL(pump->add_row(right_op_->get_spec().output_, eval_ctx_))) {
          LOG_WARN("Failed to add row", K(ret));
        }
      }

      if (child_brs->end_ == true) {
        LOG_INFO("Reach iterating end for right batch");
        break;
      }
    }
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
  LOG_DEBUG("try_format_output_row: output info", K(result_node), K(ret));
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

int ObRecursiveInnerDataOp::fake_cte_table_add_row(ObTreeNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fake cte table op can not be nullptr", K(ret));
  } else if (OB_FAIL(pump_operator_->add_row(node.stored_row_))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  } else if (SearchStrategyType::BREADTH_FRIST == search_type_
              && OB_FAIL(bfs_pump_.update_parent_node(node))) {
    LOG_WARN("Failed to update last bst node stask", K(ret), K(node));
  } else if (SearchStrategyType::DEPTH_FRIST == search_type_
              && OB_FAIL(dfs_pump_.adjust_stack(node))) {
    LOG_WARN("Failed to adjust stask", K(ret), K(node));
  }
  return ret;
}

int ObRecursiveInnerDataOp::breadth_first_union(bool left_branch, bool &continue_search)
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
     *  sort_collations_不为空的时候，层次查询要求将整个一层的结果进行排序，
     *  continue_search为true.
     *  例如：
     *       AA        AB     <- - - -已经扫描了这层
     *    AAA  AAB  ABA  ABB
     *  如果不要求整层排序，也就是获得结果AAA AAB后即可设置
     *  continue search为false，停止扫描，开始向上吐行。
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

int ObRecursiveInnerDataOp::try_get_left_rows(bool batch_mode /* = false */)
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  const bool sort = false;
  if (batch_mode && OB_FAIL(get_all_data_from_left_batch())) {
    if (OB_ITER_END == ret) {
      // do nothing
    } else {
      LOG_WARN("Get batch from left child failed", K(ret));
    }
  } else if (!batch_mode && OB_FAIL(get_all_data_from_left_child())) {
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
    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_format_output_row())) {//why check does NOT check iter_end
        LOG_WARN("Failed to get next row", K(ret));
      }
    }
  }

  return ret;
}

int ObRecursiveInnerDataOp::try_get_right_rows(bool batch_mode /* = false */)
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  if (SearchStrategyType::DEPTH_FRIST == search_type_) {
    if (batch_mode && OB_FAIL(get_all_data_from_right_batch())) {
      LOG_WARN("Get row from right child in batch mode failed", K(ret));
    } else if (!batch_mode && OB_FAIL(get_all_data_from_right_child())) {
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
      if (batch_mode && OB_FAIL(get_all_data_from_right_batch())) {
        LOG_WARN("Get row from right child in batch mode failed", K(ret));
      } else if (!batch_mode && OB_FAIL(get_all_data_from_right_child())) {
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

/**
 * 如果行可以输出则优先输出行；
 * 没有输出行的时候，第一次先从左边拿，
 * 左边拿过来了则从右边拿。
 */
int ObRecursiveInnerDataOp::get_next_row()
{
  int ret = OB_SUCCESS;
  if (!result_output_.empty()) {
    if (OB_FAIL(try_format_output_row())) {
      LOG_WARN("Format output row failed", K(ret));
    } else { }
  } else if (RecursiveUnionState::R_UNION_READ_LEFT == state_) {
    if (is_mysql_mode() && OB_FAIL(check_recursive_depth())) {
      LOG_WARN("Recursive query abort", K(ret));
    } else if (OB_FAIL(try_get_left_rows())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get left rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else {
      state_ = R_UNION_READ_RIGHT;
    }
  } else if (RecursiveUnionState::R_UNION_READ_RIGHT == state_) {
    if (is_mysql_mode() && OB_FAIL(check_recursive_depth())) {
      LOG_WARN("Recursive query abort", K(ret));
    } else if (OB_FAIL(try_get_right_rows())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get right rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else { }
  } else if (RecursiveUnionState::R_UNION_END == state_) {
    ret = OB_ITER_END;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), K(state_));
  }
  return ret;
}

// Note: keep batch size to one when generating output
// The reason making output batch size to one is that the parent-child relation
// or search path is maintained in method search tree and is processed for row
// iteration. However, maintaining the parent-child relations or search paths
// during batch iterating make it MUCH MUCH more complicated. And its benefits
// outweight the overhead of maintaining the parent-child relation.
// Thus, make recursive union all return output with batch size to 1 so far.
int ObRecursiveInnerDataOp::get_next_batch(const int64_t max_row_cnt,
                                           ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  UNUSED(max_row_cnt);
  LOG_DEBUG("Entrance of get_next_batch", K(result_output_.empty()), K(state_));
  if (!result_output_.empty()) {
    if (OB_FAIL(try_format_output_row())) {
      LOG_WARN("Format output row failed", K(ret));
    }// else { } do nothing
  } else if (RecursiveUnionState::R_UNION_READ_LEFT == state_) {
    if (is_mysql_mode() && OB_FAIL(check_recursive_depth())) {
      LOG_WARN("Recursive query abort", K(ret));
    } else if (OB_FAIL(try_get_left_rows(true))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get left rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else {
      state_ = R_UNION_READ_RIGHT;
    }
  } else if (RecursiveUnionState::R_UNION_READ_RIGHT == state_) {
    if (is_mysql_mode() && OB_FAIL(check_recursive_depth())) {
      LOG_WARN("Recursive query abort", K(ret));
    } else if (OB_FAIL(try_get_right_rows(true))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get right rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else { }
  } else if (RecursiveUnionState::R_UNION_END == state_) {
    brs.size_ = 0;
    brs.end_ = true;
    LOG_DEBUG("reach iterator end", K(ret), K(brs));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), K(state_));
  }

  if (OB_SUCC(ret)) {
    if (brs.end_) {
      // do nothing already set in previouse block
    } else {
      brs.end_ = false;
      brs.size_ = 1;
    }
  } else if (ret == OB_ITER_END) {
    brs.end_ = true;
    brs.size_ = 0;
    LOG_DEBUG("ret is OB_ITER_END, should NEVER hit here",
             K(result_output_.empty()), K(ret), K(brs));
    ret = OB_SUCCESS;
  }
  LOG_DEBUG("end of get_next_batch", K(result_output_.empty()), K(ret), K(brs));
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
      search_expr_->set_evaluated_projected(eval_ctx_);
      ++ordering_column_;
    }
  } else {
      // 无search列或search 列不在output中
  }
  if (nullptr != cycle_expr_) {
    if (OB_FAIL(cycle_expr_->deep_copy_datum(eval_ctx_, cycle ? cycle_value_ : non_cycle_value_))) {
        LOG_WARN("expr datum deep copy failed", K(ret));
    } else {
      cycle_expr_->set_evaluated_projected(eval_ctx_);
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
  // 由于容器本身使用stored_row_buf_，故不能reset它。
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

int ObRecursiveInnerDataOp::assign_to_cur_row(ObChunkDatumStore::StoredRow *stored_row)
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

int ObRecursiveInnerDataOp::check_recursive_depth() {
  int ret = OB_SUCCESS;
  ObSearchMethodOp *pump = NULL;
  if (OB_ISNULL(pump = get_search_method_bump())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    uint64_t level = pump->get_last_node_level();
    if (level != UINT64_MAX && level > max_recursion_depth_) {
      ret = OB_ERR_CTE_MAX_RECURSION_DEPTH;
      LOG_USER_ERROR(OB_ERR_CTE_MAX_RECURSION_DEPTH, level);
      LOG_WARN("Recursive query aborted after too many iterations.", K(ret), K(level));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
