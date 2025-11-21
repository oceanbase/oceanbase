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
#include "ob_recursive_union_op.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_recursive_inner_data_op.h"

namespace oceanbase
{
namespace sql
{
int ObRCTEHashTable::init(ObIAllocator *allocator,
                          lib::ObMemAttr &mem_attr,
                          const common::ObIArray<ObSortFieldCollation> *sort_collations,
                          const common::ObIArray<ObCmpFunc> *cmp_funcs, int64_t initial_size)
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(ObExtendHashTable::init(allocator, mem_attr, initial_size))) {
    LOG_WARN("RCTE init hash table failed", K(ret));
  } else {
    sort_collations_ = sort_collations;
    cmp_funcs_ = cmp_funcs;
  }
  return ret;
}

int ObRCTEHashTable::likely_equal(const ObIArray<ObExpr *> &exprs, ObEvalCtx *eval_ctx,
                                  const ObRCTEStoredRowWrapper &right,
                                  ObRADatumStore::Reader *reader, bool &result)
{
  int ret = OB_SUCCESS;
  int cmp_result = 0;
  ObDatum *l_cell = nullptr;
  const ObRADatumStore::StoredRow *right_sr = nullptr;
  if (OB_FAIL(reader->get_row(right.row_id_, right_sr))) {
    LOG_WARN("Fail to read row from CTE Table when RCTE deduplicate data", K(ret));
  } else {
    const ObDatum *r_cells = right_sr->cells();
    // must evaled in calc_hash_values
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_collations_->count() && 0 == cmp_result; ++i) {
      int64_t idx = sort_collations_->at(i).field_idx_;
      l_cell = &exprs.at(idx)->locate_expr_datum(*eval_ctx);
      if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*l_cell, r_cells[idx], cmp_result))) {
        LOG_WARN("do cmp failed", K(ret));
      }
    }
    result = (0 == cmp_result);
  }

  return ret;
}

int ObRCTEHashTable::exist(uint64_t hash_val, const ObIArray<ObExpr *> &exprs, ObEvalCtx *eval_ctx,
                           ObRADatumStore::Reader *reader, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (buckets_ != NULL) {
    ObRCTEStoredRowWrapper *it = locate_bucket(*buckets_, hash_val).item_;
    while (NULL != it && OB_SUCC(ret)) {
      if (OB_FAIL(likely_equal(exprs, eval_ctx, *it, reader, exist))) {
        LOG_WARN("failed to cmp", K(ret));
      } else if (exist) {
        break;
      }
      it = it->next();
    }
  }
  return ret;
}

int ObRecursiveInnerDataOp::init() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session info is null", K(ret));
  } else {
    int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    stored_row_buf_.set_tenant_id(tenant_id);
    stored_row_buf_.set_ctx_id(ObCtxIds::WORK_AREA);
  }
  return ret;
}

int ObRecursiveInnerDataOracleOp::init(const ObExpr *search_expr, const ObExpr *cycle_expr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObRecursiveInnerDataOp::init())) {
    LOG_WARN("Failed to init ObRecursiveInnerDataOp", K(ret));
  } else if (OB_FAIL(dfs_pump_.init())) {
    LOG_WARN("Failed to init depth first search pump", K(ret));
  } else {
    search_expr_ = search_expr;
    cycle_expr_ = cycle_expr;
  }

  return ret;
}

int ObRecursiveInnerDataOracleOp::get_all_data_from_left_child()
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
        if (SearchStrategyType::BREADTH_FIRST_BULK == search_type_) {
          if (OB_FAIL(bfs_bulk_pump_.add_row(left_op_->get_spec().output_, eval_ctx_))) {
            LOG_WARN("Failed to add row", K(ret));
          }
        } else if (SearchStrategyType::BREADTH_FRIST == search_type_) {
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
int ObRecursiveInnerDataOracleOp::get_all_data_from_left_batch()
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
      if (OB_SUCC(ret) && child_brs->end_) {
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

int ObRecursiveInnerDataOracleOp::get_all_data_from_right_child()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_op_) || OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left_op_ or right_op_ is nullptr", K(ret), K(left_op_), K(right_op_));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(right_op_->get_next_row())) {
      if (OB_ITER_END == ret) {
      } else if (OB_SUCC(ret) && SearchStrategyType::BREADTH_FIRST_BULK == search_type_) {
        if (OB_FAIL(bfs_bulk_pump_.add_row(right_op_->get_spec().output_, eval_ctx_))) { //add stored row
          LOG_WARN("Failed to add row", K(ret));
        }
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
int ObRecursiveInnerDataOracleOp::get_all_data_from_right_batch()
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

int ObRecursiveInnerDataOracleOp::try_format_output_row(int64_t &read_rows)
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
    } else {
      read_rows = 1;
    }
  }
  LOG_DEBUG("try_format_output_row: output info", K(result_node), K(ret));
  return ret;
}

int ObRecursiveInnerDataOracleOp::try_format_output_batch(int64_t batch_size, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  ObTreeNode result_node;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(batch_size);
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
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
      guard.set_batch_idx(i);
      if (OB_FAIL(assign_to_cur_row(result_node.stored_row_))) {
        LOG_WARN("Failed to assign input row to cur row", K(ret));
      } else if (OB_FAIL(add_pseudo_column(result_node.is_cycle_))) {
        LOG_WARN("Add pseudo column failed", K(ret));
      } else {
        read_rows++;
      }
    }
    LOG_DEBUG("try_format_output_batch: output info", K(result_node), K(ret));
  }
  if (OB_ITER_END == ret && read_rows > 0) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(search_expr_)) {
      search_expr_->set_evaluated_projected(eval_ctx_);
      ObEvalInfo &info = search_expr_->get_eval_info(eval_ctx_);
      info.notnull_ = false;
      info.point_to_frame_ = false;
    }
    if (OB_NOT_NULL(cycle_expr_)) {
      cycle_expr_->set_evaluated_projected(eval_ctx_);
      ObEvalInfo &info = cycle_expr_->get_eval_info(eval_ctx_);
      info.notnull_ = false;
      info.point_to_frame_ = false;
    }
    for (int64_t i = 0; i < output_union_exprs_.count(); ++i) {
      ObExpr *expr = output_union_exprs_.at(i);
      expr->set_evaluated_projected(eval_ctx_);
      ObEvalInfo &info = expr->get_eval_info(eval_ctx_);
      info.notnull_ = false;
      info.point_to_frame_ = false;
    }
  }
  return ret;
}

int ObRecursiveInnerDataOracleOp::depth_first_union(const bool sort /*=true*/)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  if (OB_FAIL(dfs_pump_.finish_add_row(sort))) {
    LOG_WARN("Failed to add row", K(ret));
  } else if (OB_FAIL(set_fake_cte_table_empty())) {
    LOG_WARN("Set fake cte table to empty failed", K(ret));
  } else if (OB_FAIL(dfs_pump_.get_next_nocycle_node(result_output_, node))) {
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

int ObRecursiveInnerDataOracleOp::fake_cte_table_add_row(ObTreeNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fake cte table op can not be nullptr", K(ret));
  } else if (OB_FAIL(pump_operator_->add_single_row(node.stored_row_))) {
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

int ObRecursiveInnerDataOracleOp::breadth_first_union(bool left_branch, bool &continue_search)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  if (OB_FAIL(set_fake_cte_table_empty())) {
    LOG_WARN("Set fake cte table to empty failed", K(ret));
  } else if (OB_FAIL(bfs_pump_.add_result_rows())) {
    LOG_WARN("Failed to finish add row", K(ret));
  } else if (OB_FAIL(bfs_pump_.get_next_nocycle_node(result_output_, node))) {
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

int ObRecursiveInnerDataOracleOp::start_new_level(bool left_branch)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  if (OB_FAIL(bfs_pump_.finish_add_row(!left_branch))) {
    LOG_WARN("Failed to finish add row", K(ret));
  } else if (OB_FAIL(bfs_pump_.get_next_nocycle_node(result_output_, node))) {
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

// for breadth bulk search first
int ObRecursiveInnerDataOracleOp::fake_cte_table_add_bulk_rows(bool left_branch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fake cte table op can not be nullptr", K(ret));
  } else if (FALSE_IT(pump_operator_->update_status())) {
  } else if (OB_FAIL(bfs_bulk_pump_.update_search_depth())) {
    LOG_WARN("Failed to update last bst node stask", K(ret));
  }
  return ret;
}

int ObRecursiveInnerDataOracleOp::breadth_first_bulk_union(bool left_branch)
{
  int ret = OB_SUCCESS;
  bool need_sort = !sort_collations_.empty();
  if (OB_ISNULL(pump_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The fake cte table is null", K(ret));
  } else if (OB_FAIL(set_fake_cte_table_empty())) {
    LOG_WARN("Set fake cte table to empty failed", K(ret));
  } else if (OB_FAIL(bfs_bulk_pump_.add_result_rows(left_branch, identify_seq_offset_))) {
    LOG_WARN("Failed to finish add row", K(ret));
  } else if (OB_FAIL(bfs_bulk_pump_.get_next_nocycle_bulk(
        result_output_, pump_operator_->get_bulk_rows(), need_sort))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      state_ = RecursiveUnionState::R_UNION_END;
    } else {
      LOG_WARN("Failed to get next non cycle node", K(ret));
    }
  } else if (OB_FAIL(fake_cte_table_add_bulk_rows(left_branch))) {
    LOG_WARN("Fake cte table add row failed", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObRecursiveInnerDataOracleOp::try_get_left_rows(
    bool batch_mode, int64_t batch_size, int64_t &read_rows)
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
    if (SearchStrategyType::BREADTH_FIRST_BULK == search_type_) {
      if (OB_FAIL(breadth_first_bulk_union(true))) {
        LOG_WARN("Breadth first union failed", K(ret));
      }
    } else if (SearchStrategyType::BREADTH_FRIST == search_type_) {
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
      if (batch_mode && is_bulk_search()) {
        if (OB_FAIL(try_format_output_batch(batch_size, read_rows))) {
          LOG_WARN("failed to get next batch", K(ret));
        }
      } else {
        if (OB_FAIL(try_format_output_row(read_rows))) {
          LOG_WARN("Failed to get next row", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRecursiveInnerDataOracleOp::try_get_right_rows(
    bool batch_mode, int64_t batch_size, int64_t &read_rows)
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
  } else if (SearchStrategyType::BREADTH_FIRST_BULK == search_type_) {
    if (batch_mode && OB_FAIL(get_all_data_from_right_batch())) {
      LOG_WARN("Get row from right child in batch mode failed", K(ret));
    } else if (!batch_mode && OB_FAIL(get_all_data_from_right_child())) {
      LOG_WARN("Get row from right child failed", K(ret));
    } else if (OB_FAIL(right_op_->rescan())) {
      LOG_WARN("Recursive union right children rescan failed", K(ret));
    } else if (bfs_bulk_pump_.empty()) {
      // do nothing
    } else if (OB_FAIL(breadth_first_bulk_union(false))) {
      LOG_WARN("Breadth first union failed", K(ret));
    }
  }
  // try to get row again
  if (OB_SUCC(ret)) {
    if (batch_mode && is_bulk_search()) {
      if (OB_FAIL(try_format_output_batch(batch_size, read_rows))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next batch", K(ret));
        }
      }
    } else {
      if (OB_FAIL(try_format_output_row(read_rows))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("Failed to get next row", K(ret));
        }
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
int ObRecursiveInnerDataOracleOp::get_next_row()
{
  int ret = OB_SUCCESS;
  int64_t read_rows = 0;
  if (!result_output_.empty()) {
    if (OB_FAIL(try_format_output_row(read_rows))) {
      LOG_WARN("Format output row failed", K(ret));
    } else { }
  } else if (RecursiveUnionState::R_UNION_READ_LEFT == state_) {
    if (OB_FAIL(try_get_left_rows(false, 1, read_rows))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get left rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else {
      state_ = R_UNION_READ_RIGHT;
    }
  } else if (RecursiveUnionState::R_UNION_READ_RIGHT == state_) {
    if (OB_FAIL(try_get_right_rows(false, 1, read_rows))) {
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
int ObRecursiveInnerDataOracleOp::get_next_batch(const int64_t batch_size,
                                           ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = 0;
  LOG_DEBUG("Entrance of get_next_batch", K(result_output_.empty()), K(state_));
  if (!result_output_.empty()) {
    if (is_bulk_search()) {
      if (OB_FAIL(try_format_output_batch(batch_size, read_rows))) {
        LOG_WARN("Format output bacth failed", K(ret));
      }
    } else {
      if (OB_FAIL(try_format_output_row(read_rows))) {
        LOG_WARN("Format output row failed", K(ret));
      }
    }
  } else if (RecursiveUnionState::R_UNION_READ_LEFT == state_) {
    if (OB_FAIL(try_get_left_rows(true, batch_size, read_rows))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Get left rows failed", K(ret));
      } else {
        state_ = RecursiveUnionState::R_UNION_END;
      }
    } else {
      state_ = R_UNION_READ_RIGHT;
    }
  } else if (RecursiveUnionState::R_UNION_READ_RIGHT == state_) {
    if (OB_FAIL(try_get_right_rows(true, batch_size, read_rows))) {
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
      brs.size_ = read_rows;
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

int ObRecursiveInnerDataOracleOp::add_pseudo_column(bool cycle /*default false*/)
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
  if (OB_SUCC(ret) && nullptr != cycle_expr_) {
    if (OB_FAIL(cycle_expr_->deep_copy_datum(eval_ctx_, cycle ? cycle_value_ : non_cycle_value_))) {
      LOG_WARN("expr datum deep copy failed", K(ret));
    } else {
      cycle_expr_->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObRecursiveInnerDataOracleOp::rescan()
{
  int ret = OB_SUCCESS;
  state_ = R_UNION_READ_LEFT;
  result_output_.reset();
  ordering_column_ = 1;
  dfs_pump_.reuse();
  bfs_pump_.reuse();
  bfs_bulk_pump_.reuse();
  //rescan RCTE should clear cte intermediate data
  //while rescan cte don't need to clear itself because they have different meaning
  pump_operator_->reuse();
  // 由于容器本身使用stored_row_buf_，故不能reset它。
  return ret;
}

int ObRecursiveInnerDataOracleOp::set_fake_cte_table_empty()
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

void ObRecursiveInnerDataOracleOp::destroy()
{
  result_output_.reset();
  left_op_ = nullptr;
  right_op_ = nullptr;
  dfs_pump_.destroy();
  bfs_pump_.destroy();
  bfs_bulk_pump_.destroy();
  if (OB_NOT_NULL(pump_operator_)) {
    pump_operator_->destroy();
    pump_operator_ = nullptr;
  }
  stored_row_buf_.reset();
}

int ObRecursiveInnerDataOracleOp::assign_to_cur_row(ObChunkDatumStore::StoredRow *stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stored_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stored row is null", K(stored_row));
  } else if (OB_FAIL(stored_row->to_expr(
        output_union_exprs_, eval_ctx_, output_union_exprs_.count()))) {
    LOG_WARN("stored row to exprs failed", K(ret));
  }
  return ret;
}

void ObRecursiveInnerDataMysqlOp::destroy()
{
  left_op_ = nullptr;
  right_op_ = nullptr;
  if (OB_NOT_NULL(hash_value_reader_)) {
    hash_value_reader_->reset();
  }
  if (OB_NOT_NULL(pump_operator_)) {
    pump_operator_->destroy();
    pump_operator_ = nullptr;
  }

  hash_table_.destroy();

  stored_row_buf_.reset();
}

int ObRecursiveInnerDataMysqlOp::init(const ObExpr *search_expr, const ObExpr *cycle_expr)
{
  int ret = OB_SUCCESS;

  UNUSED(search_expr);
  UNUSED(cycle_expr);

  if (OB_FAIL(ObRecursiveInnerDataOp::init())) {
    LOG_WARN("Failed to init ObRecursiveInnerDataOp", K(ret));
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_CTE_MAX_RECURSION_DEPTH, max_recursion_depth_))) {
    LOG_WARN("Get sys variable error", K(ret));
  } else {
    ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(), "SqlCteHashTable",
                   ObCtxIds::WORK_AREA);
    void *skip_mem = nullptr;
    if (OB_FAIL(hash_table_.init(&stored_row_buf_, attr, &deduplicate_sort_collations_,
                                 &sort_cmp_funs_))) {
      LOG_WARN("RCTE fail to init hash_table", K(ret));
    } else if (OB_ISNULL(skip_mem = stored_row_buf_.alloc(ObBitVector::memory_size(batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(batch_size_));
    } else if (OB_ISNULL(hash_values_for_batch_ = static_cast<uint64_t *>(
                           stored_row_buf_.alloc(sizeof(uint64_t) * batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for hash value temp array", K(ret), K(batch_size_));
    } else {
      skips_ = to_bit_vector(skip_mem);
      skips_->reset(batch_size_);
    }
  }

  return ret;
}

int ObRecursiveInnerDataMysqlOp::rescan()
{
  int ret = OB_SUCCESS;
  state_ = R_UNION_READ_LEFT;

  hash_table_.reuse();
  pump_operator_->reuse();
  hash_value_reader_->reuse();
  curr_level_ = 0;

  return ret;
}

int ObRecursiveInnerDataMysqlOp::set_fake_cte_table_and_reader(ObFakeCTETableOp *cte_table)
{
  ObRecursiveInnerDataOp::set_fake_cte_table_and_reader(cte_table);
  int ret = OB_SUCCESS;
  void *reader_mem = nullptr;
  if (OB_ISNULL(reader_mem = ctx_.get_allocator().alloc(sizeof(ObRADatumStore::Reader)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObRADatumStore::Reader", K(ret));
  } else {
    hash_value_reader_ =
      new (reader_mem) ObRADatumStore::Reader(*pump_operator_->get_intermedia_table());
  }
  return ret;
}

int ObRecursiveInnerDataMysqlOp::get_next_row()
{
  int ret = OB_SUCCESS;
  do {
    if (curr_level_ > max_recursion_depth_) {
      ret = OB_ERR_CTE_MAX_RECURSION_DEPTH;
      LOG_USER_ERROR(OB_ERR_CTE_MAX_RECURSION_DEPTH, curr_level_);
      LOG_WARN("Recursive query aborted after too many iterations.", K(ret), K(curr_level_));
    } else {
      ret = get_next_row_no_materialization();
    }
  } while (ret == OB_ITER_END && state_ != RecursiveUnionState::R_UNION_END);
  return ret;
}

int ObRecursiveInnerDataMysqlOp::get_next_batch(const int64_t batch_size, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  if (curr_level_ > max_recursion_depth_) {
    ret = OB_ERR_CTE_MAX_RECURSION_DEPTH;
    LOG_USER_ERROR(OB_ERR_CTE_MAX_RECURSION_DEPTH, curr_level_);
    LOG_WARN("Recursive query aborted after too many iterations.", K(ret), K(curr_level_));
  } else {
    ret = get_next_batch_no_materialization(batch_size, brs);
  }
  return ret;
}


// The detailed procedure is described as follows:
// 1. The Recursive Union operator reads data from the left branch child node
// 2. If the Recursive Union is Distinct, deduplication is performed
// 3. Read all the data of the child operator in batches, which is the iteration data of this round, and:
//    a. pass data to the upper-layer operator
//    b. pass data to CTE Table in the CTE operator of right branch
// 4. When this round of recursive iteration ends, determine whether there is no more any new data is generated in this iteration round
//    a. If yes, the iteration ends
//    b. If no, data is still available, rescan() right branch, cotinue process logic, go to Step 5
// 5. Read the data from the right child node and go back to Step 2
int ObRecursiveInnerDataMysqlOp::get_next_batch_no_materialization(const int64_t batch_size,
                                                                   ObBatchRows &brs)
{
  int ret = OB_SUCCESS;

  if (state_ == RecursiveUnionState::R_UNION_END) {
    brs.size_ = 0;
    brs.end_ = true;
  } else {
    // 0.no need to get from result_output_ anymore

    ObOperator *child = ((state_ == RecursiveUnionState::R_UNION_READ_LEFT) ? left_op_ : right_op_);
    const ObBatchRows *child_brs = nullptr;

    // 1 or 5. read data from child(left or right)
    if (OB_FAIL(child->get_next_batch(batch_size, child_brs))) {
      LOG_WARN("get data from child branch failed", K(ret), K(curr_level_));
    } else if (OB_FAIL(handle_batch_data(child->get_spec().output_, child_brs->size_,
                                         child_brs->skip_, brs.skip_, hash_values_for_batch_))) {
      LOG_WARN("handle_batch_data failed", K(ret));
    } else if (child_brs->end_) {
      // 4. here we reach the end of a round of iteration
      if (state_ == RecursiveUnionState::R_UNION_READ_LEFT) {
        pump_operator_->update_round_limit();
        LOG_TRACE("RCTE finish round: ", K_(curr_level), K_(write_rows_in_this_iteration), K(pump_operator_->get_round_limit()));
        state_ = RecursiveUnionState::R_UNION_READ_RIGHT;
        curr_level_++;
        write_rows_in_this_iteration_ = 0;
        // todo: need rescan here?
      } else if (state_ == RecursiveUnionState::R_UNION_READ_RIGHT) {
        // if no more data are appended in this round iteration, we are already reach the final end

        // CTE table may still have data remain if there is a short-cut scenario
        // e.g. Right branch is a hash join, but build-side doesn't output even a single row, so
        // when cte in probe-side, it still has data but will be skipped
        // So whether is in the end of the operator can not be judged by whether the CTE table still
        // has unread data
        // That's why we need the RCTE operator to maintain the ‘write_rows_in_this_iteration_’ variable
        if (write_rows_in_this_iteration_ == 0) {
          // 4.a
          LOG_TRACE("RCTE finish *Finial* round: ", K_(curr_level), K_(write_rows_in_this_iteration), K(pump_operator_->get_round_limit()));
          brs.end_ = true;
          state_ = RecursiveUnionState::R_UNION_END;
        } else {
          // 4.b
          pump_operator_->update_round_limit();
          LOG_TRACE("RCTE finish round: ", K_(curr_level), K_(write_rows_in_this_iteration), K(pump_operator_->get_round_limit()));
          curr_level_++;
          write_rows_in_this_iteration_ = 0;
          right_op_->rescan();
        }
      } else {
        LOG_WARN("state_ should never be RecursiveUnionState::R_UNION_END after executing");
        OB_ASSERT(false);
      }
    }

    brs.size_ = child_brs->size_;

    // 3.a directly pass data to upper operator
    if (OB_SUCC(ret)
        && OB_FAIL(
             convert_batch(child->get_spec().output_, output_union_exprs_, brs.size_, brs.skip_))) {
      LOG_WARN("convert_batch failed", K(ret));
    }
  }

  return ret;
}

int ObRecursiveInnerDataMysqlOp::get_next_row_no_materialization()
{
  int ret = OB_SUCCESS;

  // 0.no need to get from result_output_ anymore
  ObOperator *child = ((state_ == RecursiveUnionState::R_UNION_READ_LEFT) ? left_op_ : right_op_);

  // 1 or 5. read data from child(left or right)
  bool is_duplicate = false;
  // keep loop to get data from lower operator untill
  // get a unique data
  // or
  // reach the end of this round iteration
  // or
  // other failure reason
  do {
    if (OB_FAIL(child->get_next_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("get data from child branch failed", K(ret), K(curr_level_));
      }
    } else {
      if (OB_SUCC(ret)
          && OB_FAIL(
               handle_row_data(child->get_spec().output_, hash_values_for_batch_, is_duplicate))) {
        LOG_WARN("handle_batch_data failed", K(ret));
      }
    }

  } while (OB_SUCC(ret) && is_duplicate);

  if (ret == OB_ITER_END) {
    // 4. here we reach the end of a round of iteration
    if (state_ == RecursiveUnionState::R_UNION_READ_LEFT) {
      state_ = RecursiveUnionState::R_UNION_READ_RIGHT;
      curr_level_++;
      write_rows_in_this_iteration_ = 0;
      pump_operator_->update_round_limit();
      // todo: need rescan here?
    } else if (state_ == RecursiveUnionState::R_UNION_READ_RIGHT) {
      // if no more data are appended in this round iteration, we are already reach the final end

      // CTE table may still have data remain if there is a short-cut scenario
      // e.g. Right branch is a hash join, but build-side doesn't output even a single row, so
      // when cte in probe-side, it still has data but will be skipped
      // So whether is in the end of the operator can not be judged by whether the CTE table still
      // has unread data
      // That's why we need the RCTE operator to maintain the ‘write_rows_in_this_iteration_’
      // variable
      if (write_rows_in_this_iteration_ == 0) {
        // 4.a
        state_ = RecursiveUnionState::R_UNION_END;
      } else {
        // 4.b
        curr_level_++;
        write_rows_in_this_iteration_ = 0;
        right_op_->rescan();
        pump_operator_->update_round_limit();
      }
    } else {
      LOG_WARN("state_ should never be RecursiveUnionState::R_UNION_END after executing");
      OB_ASSERT(false);
    }
  }

  // 3.a directly pass data to upper operator
  if (OB_SUCC(ret)
      && OB_FAIL(convert_batch(child->get_spec().output_, output_union_exprs_, 1, nullptr))) {
    LOG_WARN("convert_batch failed", K(ret));
  }

  return ret;
}

int ObRecursiveInnerDataMysqlOp::handle_batch_data(const common::ObIArray<ObExpr *> &exprs,
                                                   const int64_t batch_size,
                                                   ObBitVector *child_skip, ObBitVector *this_skip,
                                                   uint64_t *hash_values_for_batch)
{
  int ret = OB_SUCCESS;
  // 2. hash deduplication
  if (OB_NOT_NULL(child_skip) && OB_NOT_NULL(this_skip)) {
    this_skip->deep_copy(*child_skip, batch_size);
  }

  if (is_union_distinct_) {
    if (OB_ISNULL(hash_values_for_batch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hash values vector is not init", K(ret));
    } else if (OB_FAIL(
                 calc_hash_value_for_batch(exprs, batch_size, child_skip, hash_values_for_batch))) {
      LOG_WARN("failed to calc hash values", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    void *buf;
    ObRCTEStoredRowWrapper *key;
    bool data_exist = false;
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_size(batch_size);
    for (int64_t i = 0; i < batch_size && OB_SUCC(ret); i++) {
      if (OB_NOT_NULL(child_skip) && child_skip->at(i)) {
        continue;
      }
      guard.set_batch_idx(i);

      if (is_union_distinct_) {
        if (OB_FAIL(hash_table_.exist(hash_values_for_batch[i], exprs, &eval_ctx_,
                                      hash_value_reader_, data_exist))) {
          LOG_WARN("Fail to check whether data is exist in hash table in Recursive Union Distinct",
                   K(ret));
        }
      }

      // 3.b write data into intermedia table and update hashtable_
      // now CTE operator will maintenance ObRADatumStore
      // Append data into CTE table in
      // Recursive Union All
      // or
      // Recursive Union Distinct which is unique data
      if (OB_SUCC(ret) && (!is_union_distinct_ || !data_exist)) {
        write_rows_in_this_iteration_++;
        if (OB_FAIL(pump_operator_->add_single_row_to_intermedia_table(exprs, &eval_ctx_))) {
          LOG_WARN("Fail to append data into CTE intermedia table", K(ret));
        }
      }

      if (OB_SUCC(ret) && is_union_distinct_) {
        if (!data_exist) {
          if (OB_ISNULL(buf = stored_row_buf_.alloc(sizeof(ObRCTEStoredRowWrapper)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc buf for ObRCTEStoredRowWrapper failed", K(ret));
          } else {
            key = new (buf) ObRCTEStoredRowWrapper(
              hash_values_for_batch[i], pump_operator_->get_intermedia_table()->get_row_cnt() - 1);
            if (OB_FAIL(hash_table_.set(*key))) {
              LOG_WARN("failed to set key", K(ret));
            }
          }
        } else {
          // already exist, just skip this data
          this_skip->set(i);
        }
      }
    }
  }

  return ret;
}

int ObRecursiveInnerDataMysqlOp::handle_row_data(const common::ObIArray<ObExpr *> &exprs,
                                                 uint64_t *hash_values_for_batch,
                                                 bool &is_duplicate)
{
  int ret = OB_SUCCESS;
  // 2. hash deduplication

  if (is_union_distinct_) {
    if (OB_ISNULL(hash_values_for_batch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hash values vector is not init", K(ret));
    } else if (OB_FAIL(calc_hash_value_for_row(exprs, hash_values_for_batch))) {
      LOG_WARN("failed to calc hash values", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    void *buf;
    ObRCTEStoredRowWrapper *key;
    bool data_exist = false;

    if (is_union_distinct_) {
      if (OB_FAIL(hash_table_.exist(*hash_values_for_batch, exprs, &eval_ctx_, hash_value_reader_,
                                    data_exist))) {
        LOG_WARN("Fail to check whether data is exist in hash table in Recursive Union Distinct",
                 K(ret));
      }
    }

    // 3.b write data into intermedia table and update hashtable_
    // now CTE operator will maintenance ObRADatumStore
    // Append data into CTE table in
    // Recursive Union All
    // or
    // Recursive Union Distinct which is unique data
    if (OB_SUCC(ret) && (!is_union_distinct_ || !data_exist)) {
      write_rows_in_this_iteration_++;
      if (OB_FAIL(pump_operator_->add_single_row_to_intermedia_table(exprs, &eval_ctx_))) {
        LOG_WARN("Fail to append data into CTE intermedia table", K(ret));
      }
    }

    if (OB_SUCC(ret) && is_union_distinct_) {
      if (!data_exist) {
        if (OB_ISNULL(buf = stored_row_buf_.alloc(sizeof(ObRCTEStoredRowWrapper)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buf for ObRCTEStoredRowWrapper failed", K(ret));
        } else {
          key = new (buf) ObRCTEStoredRowWrapper(
            *hash_values_for_batch, pump_operator_->get_intermedia_table()->get_row_cnt() - 1);
          if (OB_FAIL(hash_table_.set(*key))) {
            LOG_WARN("failed to set key", K(ret));
          }
        }
      } else {
        // already exist, just skip this data
        is_duplicate = true;
      }
    }
  }

  return ret;
}

int ObRecursiveInnerDataMysqlOp::calc_hash_value_for_row(const common::ObIArray<ObExpr *> &exprs,
                                                         uint64_t *hash_values_for_batch)
{
  int ret = OB_SUCCESS;
  ObDatum *curr_datum = NULL;
  *hash_values_for_batch = ObRCTEStoredRowWrapper::DEFAULT_HASH_VALUE;
  for (int j = 0; j < deduplicate_sort_collations_.count(); j++) {
    const int64_t idx = deduplicate_sort_collations_.at(j).field_idx_;

    ObDatumHashFuncType hash_func = hash_funcs_.at(j).hash_func_;
    // There is no guarantee that the expression has been evaluated in non-vectorization
    // executing mode.
    // So we have to evalute them explicitly before use them
    if (OB_FAIL(exprs.at(idx)->eval(eval_ctx_, curr_datum))) {
      LOG_WARN("expr evaluate failed", K(ret), KPC(exprs.at(idx)), K_(eval_ctx));
    } else {
      hash_func(*curr_datum, *hash_values_for_batch, *hash_values_for_batch);
    }
  }

  *hash_values_for_batch &= ObRCTEStoredRowWrapper::HASH_VAL_MASK;

  return ret;
}

int ObRecursiveInnerDataMysqlOp::calc_hash_value_for_batch(const common::ObIArray<ObExpr *> &exprs,
                                                           const int64_t batch_size,
                                                           ObBitVector *child_skip,
                                                           uint64_t *hash_values_for_batch)
{
  int ret = OB_SUCCESS;
  uint64_t default_hash_value = ObRCTEStoredRowWrapper::DEFAULT_HASH_VALUE;
  for (int j = 0; j < deduplicate_sort_collations_.count(); j++) {
    bool is_batch_seed = (0 != j);
    const int64_t idx = deduplicate_sort_collations_.at(j).field_idx_;
    ObBatchDatumHashFunc hash_func = hash_funcs_.at(j).batch_hash_func_;
    ObDatum *curr_datum = exprs.at(idx)->locate_batch_datums(eval_ctx_);
    hash_func(hash_values_for_batch, curr_datum, exprs.at(idx)->is_batch_result(), *child_skip,
              batch_size, is_batch_seed ? hash_values_for_batch : &default_hash_value,
              is_batch_seed);
  }

  for (int64_t i = 0; i < batch_size; ++i) {
    hash_values_for_batch[i] &= ObRCTEStoredRowWrapper::HASH_VAL_MASK;
  }

  return ret;
}

int ObRecursiveInnerDataMysqlOp::convert_batch(const common::ObIArray<ObExpr *> &src_exprs,
                                               const common::ObIArray<ObExpr *> &dst_exprs,
                                               const int64_t batch_size, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  if (0 == batch_size) {
  } else if (OB_UNLIKELY(dst_exprs.count() != src_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: exprs is not match", K(ret), K(src_exprs.count()),
             K(dst_exprs.count()));
  } else {
    for (int64_t i = 0; i < dst_exprs.count(); ++i) {
      ObDatum *dst = dst_exprs.at(i)->locate_batch_datums(eval_ctx_);
      if (!src_exprs.at(i)->is_batch_result()) {
        ObDatum &src = src_exprs.at(i)->locate_expr_datum(eval_ctx_, 0);
        for (int64_t j = 0; j < batch_size; ++j) {
          if (OB_NOT_NULL(skip) && skip->at(j)) {
            continue;
          }
          dst_exprs.at(i)->locate_expr_datum(eval_ctx_, j) = src;
        }
      } else {
        ObDatum *src = src_exprs.at(i)->locate_batch_datums(eval_ctx_);
        MEMCPY(dst, src, sizeof(ObDatum) * batch_size);
      }
      dst_exprs.at(i)->set_evaluated_projected(eval_ctx_);
    } // end for
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
