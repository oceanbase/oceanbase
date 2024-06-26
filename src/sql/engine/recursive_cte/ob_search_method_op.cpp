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

#include "sql/engine/ob_operator.h"
#include "ob_search_method_op.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObSearchMethodOp::reuse()
{
  input_rows_.reuse();
  return OB_SUCCESS;
}

uint64_t ObSearchMethodOp::ObCycleHash::inner_hash() const
{
  uint64_t result = 99194853094755497L;
  if (OB_ISNULL(hash_col_idx_) || OB_ISNULL(exprs_) || OB_ISNULL(row_)) {
  } else {
    int64_t col_count = hash_col_idx_->count();
    ObExpr *expr = NULL;
    const ObDatum *datum = NULL;
    uint64_t idx = 0;
    for (int64_t i = 0; i < col_count; i++) {
      idx = hash_col_idx_->at(i);
      if (OB_UNLIKELY(idx >= exprs_->count())
          || OB_ISNULL(expr = exprs_->at(idx))
          || OB_ISNULL(expr->basic_funcs_)) {
      } else {
        datum = &row_->cells()[idx];
        expr->basic_funcs_->wy_hash_(*datum, result, result);
      }
    }
  }
  return result;
}

bool ObSearchMethodOp::ObCycleHash::operator ==(const ObCycleHash &other) const
{
  bool result = true;
	if (OB_ISNULL(hash_col_idx_) || OB_ISNULL(row_) || OB_ISNULL(exprs_) || OB_ISNULL(other.row_)) {
    result = false;
  } else {
    const ObDatum *lcell = row_->cells();
    const ObDatum *rcell = other.row_->cells();

    int64_t col_count = hash_col_idx_->count();
    ObExpr *expr = NULL;
    int cmp_ret = 0;
    for (int64_t i = 0; result && i < col_count; i++) {
      int64_t idx = hash_col_idx_->at(i);
      if (OB_UNLIKELY(idx >= exprs_->count())
          || OB_ISNULL(expr = exprs_->at(idx))
          || OB_ISNULL(expr->basic_funcs_)) {
      } else {
        (void)expr->basic_funcs_->null_first_cmp_(lcell[idx], rcell[idx], cmp_ret);
        result = (0 == cmp_ret);
      }
    }
  }
  return result;
}

int ObSearchMethodOp::add_row(const ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::LastStoredRow last_row(allocator_);
  if (input_rows_.empty() && 0 == input_rows_.get_capacity()
      && OB_FAIL(input_rows_.reserve(INIT_ROW_COUNT))) {
    LOG_WARN("Failed to pre allocate array", K(ret));
  } else if (OB_UNLIKELY(exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exprs empty", K(ret));
  } else if (OB_FAIL(last_row.save_store_row(exprs, eval_ctx, ROW_EXTRA_SIZE))) {
    LOG_WARN("save store row failed", K(ret));
  } else if (OB_ISNULL(last_row.store_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stored_row of last_stored_row is null", K(ret));
  } else if (OB_FAIL(input_rows_.push_back(last_row.store_row_))) {
    LOG_WARN("Push new row to result input error", K(ret));
  } else {
  }
  return ret;
}

int ObSearchMethodOp::sort_input_rows()
{
  int ret = OB_SUCCESS;
  // sort
  if (input_rows_.count() > 0) {
    ObChunkDatumStore::StoredRow **first_row = &input_rows_.at(0);
    ObNodeComparer comparer(sort_collations_, left_output_, &ret);
    lib::ob_sort(first_row, first_row + input_rows_.count(), comparer);
  }
  return ret;
}

int ObSearchMethodOp::sort_rownodes(ObArray<ObTreeNode>& sort_array)
{
  int ret = OB_SUCCESS;
  if (!sort_array.empty()) {
    LOG_DEBUG("Sort row nodes", K(sort_array.count()));
    ObTreeNode *first_row = &sort_array.at(0);
    ObNodeComparer comparer(sort_collations_, left_output_, &ret);
    lib::ob_sort(first_row, first_row + sort_array.count(), comparer);
    if (OB_SUCCESS != ret) {
      LOG_WARN("Failed to do sort", K(ret));
    }
  }
  return ret;
}

int ObSearchMethodOp::is_same_row(ObChunkDatumStore::StoredRow &row_1st,
                                  ObChunkDatumStore::StoredRow &row_2nd, bool &is_cycle)
{
  int ret = OB_SUCCESS;
  const ObDatum *cells_1st = row_1st.cells();
  const ObDatum *cells_2nd = row_2nd.cells();
  int cmp_ret = 0;
  if (OB_UNLIKELY(0 == row_1st.cnt_ || 0 == row_2nd.cnt_)
      || OB_ISNULL(cells_1st) || OB_ISNULL(cells_2nd)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Inconformity row schema", K(ret), K(row_1st), K(row_2nd));
  } else if (cycle_by_columns_.empty()) {
    // detect whole row
    is_cycle = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < left_output_.count(); i++) {
      if (OB_FAIL(left_output_.at(i)->basic_funcs_->null_first_cmp_(cells_1st[i], cells_2nd[i], cmp_ret))) {
        LOG_WARN("failed to compare", K(ret), K(i));
      } else if (0 != cmp_ret) {
        is_cycle = false;
        break;
      }
    }
    if (is_cycle) {
      ret = OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE;
      LOG_WARN("Cycle detected while executing recursive WITH query", K(ret));
    }
  } else {
    // detect some datum
    is_cycle = true;
    for (int64_t i = 0; OB_SUCC(ret) && i <  cycle_by_columns_.count(); ++i) {
      uint64_t index = cycle_by_columns_.at(i);
      if (index >= row_1st.cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column index out of range", K(ret));
      } else if (OB_FAIL(left_output_.at(index)->basic_funcs_->null_first_cmp_(cells_1st[index], cells_2nd[index], cmp_ret))) {
        LOG_WARN("failed to compare", K(ret), K(index), K(i));
      } else if (0 != cmp_ret) {
        is_cycle = false;
        break;
      }
    }// end for
  }
  return ret;
}

int ObDepthFirstSearchOp::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hash_filter_rows_.create(CTE_SET_NUM))) {
    LOG_WARN("row map init failed", K(ret));
  } else {
    if (cycle_by_columns_.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < left_output_.count(); ++i) {
        if (OB_FAIL(hash_col_idx_.push_back(i))) {
          LOG_WARN("push back failed");
        }
      }
    } else if (OB_FAIL(hash_col_idx_.assign(cycle_by_columns_))) {
      LOG_WARN("assign hash col idx failed", K(ret));
    }
  }
  return ret;
}

void ObDepthFirstSearchOp::destroy()
{
  search_stack_.reset();
}

int ObDepthFirstSearchOp::reuse()
{
  ObSearchMethodOp::reuse();
  last_node_level_ = UINT64_MAX;
  hash_filter_rows_.reuse();
  return OB_SUCCESS;
}

int ObDepthFirstSearchOp::finish_add_row(bool sort)
{
  int ret = OB_SUCCESS;
  if (sort && OB_FAIL(sort_input_rows())) {
    LOG_WARN("Sort input rows failed", K(ret));
  } else if (input_rows_.empty()) {
    if (current_search_path_.empty()) {
      //do nothing
    } else {
      int64_t count = current_search_path_.count();
      ObChunkDatumStore::StoredRow *tmp_row = current_search_path_.at(count - 1);
      ObCycleHash cycle_hash(tmp_row, &hash_col_idx_, &left_output_);
      if (OB_FAIL(hash_filter_rows_.erase_refactored(cycle_hash))) {
        LOG_WARN("Earse rows from the hash map failed", K(ret));
      } else if (OB_FAIL(recycle_rows_.push_back(tmp_row))) {
        LOG_WARN("Failed to push back rows", K(ret));
      } else {
        current_search_path_.pop_back();
      }
    }
  } else {
    for (int64_t i = input_rows_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      ObTreeNode node;
      node.stored_row_ = input_rows_.at(i);
      node.tree_level_ = (UINT64_MAX == last_node_level_) ? 0 : last_node_level_ + 1;
      if (OB_FAIL(is_depth_cycle_node(node))) {
        LOG_WARN("Check cycle node failed", K(ret));
      } else if (OB_FAIL(search_stack_.push_front(node))) {
        LOG_WARN("Push data to result hold stack failed", K(ret));
      } else { }
    }// end for
  }
  input_rows_.reuse();
  // 清空上一轮需要被回收的行
  if (OB_SUCC(ret) && !recycle_rows_.empty()) {
    ARRAY_FOREACH(recycle_rows_, i) {
      allocator_.free(recycle_rows_.at(i));
    }
    recycle_rows_.reuse();
  }
  return ret;
}

int ObDepthFirstSearchOp::is_depth_cycle_node(ObTreeNode &node)
{
  int ret = OB_SUCCESS;
  ObCycleHash cycle_hash(node.stored_row_, &hash_col_idx_, &left_output_);
  if (OB_FAIL(hash_filter_rows_.exist_refactored(cycle_hash))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      node.is_cycle_ = true;
      if (cycle_by_columns_.empty()) {
        ret = OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE;
        LOG_WARN("Cycle detected while executing recursive WITH query", K(ret));
      }
    } else{
      LOG_WARN("Failed to find in hashmap", K(ret));
    }
  } else {
    // succ
  }
  return ret;
}

int ObDepthFirstSearchOp::adjust_stack(ObTreeNode &node)
{
  int ret = OB_SUCCESS;
  last_node_level_ = node.tree_level_;

  if (current_search_path_.count() < node.tree_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The current node level is 2 or more large than last node level", K(ret));
  } else if (current_search_path_.count() > node.tree_level_) {
    /**
     * 假设树为
     *              A
     *      AA            AB
     *  AAA  AAB      ABA    ABB
     * current_search_path_为A AA AAB
     * node指向AB
     * 整个栈经过调整变为A AB
     */
    int64_t count = current_search_path_.count();
    int64_t pop_times = count - node.tree_level_;
    ObChunkDatumStore::StoredRow *tmp_row = nullptr;
    while (OB_SUCC(ret) && pop_times > 0) {
      --pop_times;
      if (OB_FAIL(current_search_path_.pop_back(tmp_row))) {
        LOG_WARN("Failed to pop stack", K(ret));
      } else {
        ObCycleHash cycle_hash(tmp_row, &hash_col_idx_, &left_output_);
        if (OB_FAIL(hash_filter_rows_.erase_refactored(cycle_hash))) {
          LOG_WARN("Earse rows from the hash map failed", K(ret));
        } else if (OB_FAIL(recycle_rows_.push_back(tmp_row))) {
          LOG_WARN("Failed to push back rows", K(ret));
        }
      }
    }
  }

  // add stack
  if (OB_SUCC(ret) && current_search_path_.count() == node.tree_level_) {
    ObCycleHash cycle_hash(node.stored_row_, &hash_col_idx_, &left_output_);
    if (OB_FAIL(current_search_path_.push_back(node.stored_row_))) {
      LOG_WARN("Push new row to result record failed", K(ret));
    } else if (OB_FAIL(hash_filter_rows_.set_refactored(cycle_hash))) {
      LOG_WARN("Failed to insert row to hashmap", K(ret));
    }
  }
  return ret;
}

int ObDepthFirstSearchOp::get_next_nocycle_node(ObList<ObTreeNode,
                                                common::ObIAllocator> &result_output,
                                                ObTreeNode &nocycle_node)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  bool got_row = false;
  while (OB_SUCC(ret) && !search_stack_.empty()) {
    if (OB_FAIL(search_stack_.pop_front(node))) {
      LOG_WARN("Search stack pop back failed", K(ret));
    } else if (OB_FAIL(result_output.push_back(node))) {
      LOG_WARN("Push back data to result output failed", K(ret));
    } else if (node.is_cycle_) {
      if (OB_FAIL(recycle_rows_.push_back(node.stored_row_))) {
        LOG_WARN("Failed to push back rows", K(ret));
      }
    } else {
      got_row = true;
      nocycle_node = node;
      break;
    }
  }// end while
  if (OB_SUCC(ret) && !got_row) {
    ret = OB_ITER_END;
  }
  return ret;
}

void ObBreadthFirstSearchOp::destroy()
{
  search_queue_.reset();
}

int ObBreadthFirstSearchOp::add_new_level()
{
  int ret = OB_SUCCESS;
  ObTreeNode new_level_node;
  new_level_node.stored_row_ = nullptr;
  if (OB_FAIL(search_queue_.push_back(new_level_node))) {
    LOG_WARN("Push back data to result hold queue failed", K(ret));
  }
  return ret;
}

int ObBreadthFirstSearchOp::reuse()
{
  ObSearchMethodOp::reuse();
  current_parent_node_ = &bst_root_;
  last_node_level_ = 0;
  bst_root_.child_num_ = 0;
  bst_root_.children_ = nullptr;
  bst_root_.parent_ = nullptr;
  bst_root_.stored_row_ = nullptr;
  search_queue_.reset();
  search_results_.reuse();
  return OB_SUCCESS;
}

int ObBreadthFirstSearchOp::init_new_nodes(ObBFSTreeNode *last_bstnode, int64_t child_num)
{
  int ret = OB_SUCCESS;
  void* childs_ptr = nullptr;
  // 初始化树节点的内存
  if (OB_UNLIKELY(0 == child_num)) {
    //do nothing
  } else if (OB_ISNULL(last_bstnode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Last bst node can not be null", K(ret));
  } else if (OB_UNLIKELY(last_bstnode->child_num_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Last bst node can not be ini twice", K(ret), KPC(last_bstnode));
  } else if (OB_ISNULL(childs_ptr = allocator_.alloc(sizeof(ObBFSTreeNode*) * child_num))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Alloc memory for row failed", "size", child_num * sizeof(ObBFSTreeNode*), K(ret));
  } else {
    last_bstnode->children_ = (ObBFSTreeNode**)childs_ptr;
    last_bstnode->child_num_ = child_num;
  }
  return ret;
}

int ObBreadthFirstSearchOp::is_breadth_cycle_node(ObTreeNode &node)
{
  int ret = OB_SUCCESS;
  ObBFSTreeNode* tmp = current_parent_node_;
  ObChunkDatumStore::StoredRow* row = node.stored_row_;
  if (OB_ISNULL(tmp) || OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The last_bstnode and row an not be null", K(ret), KPC(row));
  } else {
    // bst_root_ 的row_为空
    while(OB_SUCC(ret) && OB_NOT_NULL(tmp) && OB_NOT_NULL(tmp->stored_row_)) {
      ObChunkDatumStore::StoredRow* row_1st = row;
      ObChunkDatumStore::StoredRow* row_2nd = tmp->stored_row_;
      // 从扁鹊看，对cycle的检测占了层次查询绝大多数时间，特别慢。
      if (OB_FAIL(is_same_row(*row_1st, *row_2nd, node.is_cycle_))) {
        LOG_WARN("Failed to compare the two row", K(ret), KPC(row_1st), KPC(row_2nd));
      } else if (node.is_cycle_) {
        break;
      } else {
        tmp = tmp->parent_;
      }
    }
  }
  return ret;
}

int ObBreadthFirstSearchOp::get_next_nocycle_node(ObList<ObTreeNode,
                                                    common::ObIAllocator> &result_output,
                                                    ObTreeNode &nocycle_node)
{
  int ret = OB_SUCCESS;
  ObTreeNode node;
  bool got_row = false;
  while(OB_SUCC(ret) && !search_queue_.empty()) {
    if (OB_FAIL(search_queue_.pop_front(node))) {
      LOG_WARN("Get row from hold queue failed", K(ret));
    } else if (OB_FAIL(result_output.push_back(node))) {
      LOG_WARN("Failed to push row to output ", K(ret));
    } else if (node.is_cycle_) {
      if (OB_FAIL(recycle_rows_.push_back(node.stored_row_))) {
        LOG_WARN("Failed to push back rows", K(ret));
      }
    } else {
      got_row = true;
      nocycle_node = node;
      break;
    }
  }// end while
  if (OB_SUCC(ret) && !got_row) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObBreadthFirstSearchOp::update_parent_node(ObTreeNode &node)
{
  int ret = OB_SUCCESS;
  current_parent_node_ = node.in_bstree_node_;
  last_node_level_ = node.tree_level_;
  return ret;
}

int ObBreadthFirstSearchOp::add_result_rows()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_new_nodes(current_parent_node_, input_rows_.count()))) {
    LOG_WARN("Failed to init new bst node", K(ret));
  } else {
    ARRAY_FOREACH(input_rows_, i) {
      void* ptr = nullptr;
      ObBFSTreeNode* tmp = nullptr;
      ObTreeNode node;
      node.stored_row_ = input_rows_.at(i);
      // breadth search tree
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObBFSTreeNode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Alloc memory for row failed", "size", sizeof(ObBFSTreeNode), K(ret));
      } else {
        tmp = new(ptr) ObBFSTreeNode();
        tmp->stored_row_ = input_rows_.at(i);
        tmp->parent_ = current_parent_node_;
        current_parent_node_->children_[i] = tmp;
        node.in_bstree_node_ = tmp;
        node.tree_level_ = last_node_level_ + 1;
        if (OB_FAIL(is_breadth_cycle_node(node))) {
          LOG_WARN("Find cycle failed", K(ret));
        } else if (OB_FAIL(search_results_.push_back(node))) {
          LOG_WARN("Push back data to layer_results failed", K(ret));
        } else {
          LOG_DEBUG("Result node", K(node));
        }
      }
    }
  }
  input_rows_.reuse();
  return ret;
}

int ObBreadthFirstSearchOp::finish_add_row(bool sort)
{
  int ret = OB_SUCCESS;
  if (sort && OB_FAIL(sort_rownodes(search_results_))) {
    LOG_WARN("Failed to sort results", K(ret));
  } else if (!search_queue_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The last result still has residual", K(search_queue_), K(ret));
  } else {
    ARRAY_FOREACH(search_results_, i) {
      if (OB_FAIL(search_queue_.push_back(search_results_.at(i)))) {
        LOG_WARN("Push back failed", K(ret));
      }
    }
  }
  search_results_.reuse();
  return ret;
}

int ObBreadthFirstSearchBulkOp::reuse()
{
  ObSearchMethodOp::reuse();
  cur_recursion_depth_ = 0;
  bst_root_.parent_ = nullptr;
  bst_root_.stored_row_ = nullptr;
  search_results_.reuse();
  cur_iter_groups_.reuse();
  last_iter_groups_.reuse();
  free_last_iter_mem();
  return OB_SUCCESS;
}

void ObBreadthFirstSearchBulkOp::destroy()
{
  free_input_rows_mem();
  free_last_iter_mem();
  if (OB_NOT_NULL(mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
}

int ObBreadthFirstSearchBulkOp::is_breadth_cycle_node(
      ObBFSTreeNode* node, ObChunkDatumStore::StoredRow *row, bool &is_cycle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The last_bstnode and row an not be null", K(ret), KPC(row));
  } else {
    while(OB_SUCC(ret) && OB_NOT_NULL(node) && OB_NOT_NULL(node->stored_row_)) {
      ObChunkDatumStore::StoredRow* row_1st = row;
      ObChunkDatumStore::StoredRow* row_2nd = node->stored_row_;
      if (OB_FAIL(is_same_row(*row_1st, *row_2nd, is_cycle))) {
        LOG_WARN("Failed to compare the two row", K(ret), KPC(row_1st), KPC(row_2nd));
      } else if (is_cycle) {
        break;
      } else {
        node = node->parent_;
      }
    }
  }
  return ret;
}

int ObBreadthFirstSearchBulkOp::get_next_nocycle_bulk(
      ObList<ObTreeNode, common::ObIAllocator> &result_output,
      ObArray<ObChunkDatumStore::StoredRow *> &fake_table_bulk_rows,
      bool need_sort)
{
  int ret = OB_SUCCESS;
  int rows_cnt = search_results_.count();

  if (OB_SUCC(ret) && need_sort) {
    if (rows_cnt > max_buffer_cnt_) {
      if (OB_NOT_NULL(result_output_buffer_)) {
        allocator_.free(result_output_buffer_);
        result_output_buffer_ = NULL;
        max_buffer_cnt_ = 0;
      }
      result_output_buffer_ = (ObTreeNode **)allocator_.alloc(sizeof(ObTreeNode *) * rows_cnt);
      if (OB_ISNULL(result_output_buffer_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to alloc memory", K(ret));
      } else {
        max_buffer_cnt_ = rows_cnt;
        MEMSET(result_output_buffer_, 0, sizeof(ObTreeNode *) * rows_cnt);
      }
    } else {
      MEMSET(result_output_buffer_, 0, sizeof(ObTreeNode *) * rows_cnt);
    }
  }

  ObTreeNode node;
  ARRAY_FOREACH(search_results_, i) {
    if (FALSE_IT(node = search_results_.at(i))) {
      LOG_WARN("Get row from hold queue failed", K(ret));
    } else if (need_sort && FALSE_IT(result_output_buffer_[i] = &(search_results_.at(i)))) {
      LOG_WARN("Failed to push row to output ", K(ret));
    } else if (!need_sort && OB_FAIL(result_output.push_back(node))) {
      LOG_WARN("Failed to push row to output ", K(ret));
    } else if (node.is_cycle_) {
      if (OB_FAIL(recycle_rows_.push_back(node.stored_row_))) {
        LOG_WARN("Failed to push back rows", K(ret));
      }
    } else if (OB_FAIL(fake_table_bulk_rows.push_back(node.stored_row_))) {
      LOG_WARN("Failed to push back rows", K(ret));
    }
  }

  if (OB_SUCC(ret) && need_sort) {
    if (OB_FAIL(sort_result_output_nodes(rows_cnt))) {
      LOG_WARN("Failed to sort row nodes", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < rows_cnt; ++i) {
        if (OB_ISNULL(result_output_buffer_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected buffer node", K(ret));
        } else if (OB_FAIL(result_output.push_back(*result_output_buffer_[i]))) {
          LOG_WARN("Failed to push row to output ", K(ret));
        }
      }
    }
  }

  search_results_.reuse();
  if (OB_SUCC(ret) && fake_table_bulk_rows.empty()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObBreadthFirstSearchBulkOp::update_search_depth(uint64_t max_recursive_depth)
{
  int ret = OB_SUCCESS;
  cur_recursion_depth_++;
  if (!is_oracle_mode() && cur_recursion_depth_ > max_recursive_depth) {
    ret = OB_ERR_CTE_MAX_RECURSION_DEPTH;
    LOG_USER_ERROR(OB_ERR_CTE_MAX_RECURSION_DEPTH, cur_recursion_depth_);
    LOG_WARN("Recursive query aborted after too many iterations", K(ret), K(cur_recursion_depth_));
  }
  // warning for infinite recursion or large memory
  if (OB_SUCC(ret) && cur_recursion_depth_ > 1000) {
    if (0 == (cur_recursion_depth_ & (cur_recursion_depth_ - 1))) {
      LOG_WARN("Attention !! Recursion depth too large", K(cur_recursion_depth_));
    }
  }
  return ret;
}

int ObBreadthFirstSearchBulkOp::add_result_rows(bool left_branch, int64_t identify_seq_offset)
{
  int ret = OB_SUCCESS;
  if (!is_oracle_mode()) {
    free_last_iter_mem();
    ARRAY_FOREACH(input_rows_, i) {
      ObTreeNode tree_node;
      if (OB_ISNULL(input_rows_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected input row", K(ret));
      } else if (FALSE_IT(tree_node.stored_row_ = input_rows_.at(i))) {
      } else if (OB_FAIL(search_results_.push_back(tree_node))) {
        LOG_WARN("Failed to push back result rows", K(ret));
      } else if (OB_FAIL(last_iter_input_rows_.push_back(input_rows_.at(i)))) {
        LOG_WARN("Failed to push back last iter input rows");
      } else {
        LOG_DEBUG("Result node", K(tree_node));
      }
    }
  } else if (is_oracle_mode() && left_branch) {
    ARRAY_FOREACH(input_rows_, i) {
      void* ptr = nullptr;
      ObTreeNode tree_node;
      ObBFSTreeNode* cur_node = nullptr;
      ObChunkDatumStore::StoredRow *row = nullptr;
      if (OB_ISNULL(row = input_rows_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected input row", K(ret));
      } else if (FALSE_IT(tree_node.stored_row_ = row)) {
      } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObBFSTreeNode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Alloc memory for row failed", "size", sizeof(ObBFSTreeNode), K(ret));
      } else {
        cur_node = new(ptr) ObBFSTreeNode();
        cur_node->stored_row_ = row;
        cur_node->parent_ = &bst_root_;
        tree_node.is_cycle_ = false;
        tree_node.in_bstree_node_ = cur_node;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(search_results_.push_back(tree_node))) {
        LOG_WARN("Failed to push back result rows", K(ret));
      } else if (OB_FAIL(last_iter_groups_.push_back(cur_node))) {
      } else {
        LOG_DEBUG("Result node", K(tree_node));
      }
    }
  } else if (is_oracle_mode() && !left_branch) {
    ARRAY_FOREACH(input_rows_, i) {
      uint64_t identify_seq = 0;
      bool is_cycle = false;
      void* ptr = nullptr;
      ObTreeNode tree_node;
      ObBFSTreeNode* cur_node = nullptr;
      ObBFSTreeNode* parent_node = nullptr;
      ObChunkDatumStore::StoredRow *row = nullptr;
      if (OB_ISNULL(row = input_rows_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected input row", K(ret));
      } else if (identify_seq_offset < 0 || identify_seq_offset >= row->cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected identify seq offset", K(ret), K(identify_seq_offset));
      } else {
        const ObDatum *cells = row->cells();
        identify_seq = cells[identify_seq_offset].get_uint();
        tree_node.stored_row_ = input_rows_.at(i);
      }

      if (OB_FAIL(ret)) {
      } else if (identify_seq >= last_iter_groups_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected identify sequence of input row",
            K(identify_seq), K(last_iter_groups_.count()), K(ret));
      } else if (FALSE_IT(parent_node = last_iter_groups_[identify_seq])) {
      } else if (OB_FAIL(is_breadth_cycle_node(parent_node, row, is_cycle))) {
        LOG_WARN("Find cycle failed", K(ret));
      } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObBFSTreeNode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Alloc memory for row failed", "size", sizeof(ObBFSTreeNode), K(ret));
      } else {
        cur_node = new(ptr) ObBFSTreeNode();
        cur_node->stored_row_ = row;
        cur_node->parent_ = parent_node;
        tree_node.is_cycle_ = is_cycle;
        tree_node.in_bstree_node_ = cur_node;
      }

      if (OB_FAIL(ret)) {
      } else if (!is_cycle && OB_FAIL(cur_iter_groups_.push_back(cur_node))) {
        LOG_WARN("Failed to push back cur iter groups", K(ret));
      } else if (OB_FAIL(search_results_.push_back(tree_node))) {
        LOG_WARN("Failed to push back result rows", K(ret));
      } else {
        LOG_DEBUG("Result node", K(tree_node));
      }
    }
    if (OB_SUCC(ret)) {
      std::swap(cur_iter_groups_, last_iter_groups_);
      cur_iter_groups_.reuse();
    }
  }

  input_rows_.reuse();
  return ret;
}

int ObBreadthFirstSearchBulkOp::sort_result_output_nodes(int64_t rows_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(rows_cnt > 0)) {
    ObTreeNode *first_row = result_output_buffer_[0];
    ObNodeComparer comparer(sort_collations_, left_output_, &ret);
    lib::ob_sort(first_row, first_row + rows_cnt, comparer);
    if (OB_SUCCESS != ret) {
      LOG_WARN("Failed to do sort", K(ret));
    }
  }
  return ret;
}

// if mysql mode, free last iter memory
int ObBreadthFirstSearchBulkOp::add_row(const ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (!is_oracle_mode() && OB_ISNULL(malloc_allocator_)) {
    if (OB_FAIL(init_mem_context())) {
      LOG_WARN("Failed to init mem context", K(ret));
    } else if (OB_ISNULL(malloc_allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Malloc allocator not init in mysql mode", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObIAllocator *allocator = nullptr;
    allocator = is_oracle_mode() ? &allocator_ : malloc_allocator_;

    ObChunkDatumStore::StoredRow *store_row = NULL;
    if (input_rows_.empty() && 0 == input_rows_.get_capacity()
        && OB_FAIL(input_rows_.reserve(INIT_ROW_COUNT))) {
      LOG_WARN("Failed to pre allocate array", K(ret));
    } else if (OB_UNLIKELY(exprs.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exprs empty", K(ret));
    } else if (OB_FAIL(save_to_store_row(*allocator, exprs, eval_ctx, store_row))) {
      LOG_WARN("save store row failed", K(ret));
    } else if (OB_ISNULL(store_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stored_row  is null", K(ret));
    } else if (OB_FAIL(input_rows_.push_back(store_row))) {
      LOG_WARN("Push new row to result input error", K(ret));
    }
  }
  return ret;
}

int ObBreadthFirstSearchBulkOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), "CTESearchBulk", ObCtxIds::WORK_AREA);
  if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create entity failed", K(ret));
  } else if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory entity returned", K(ret));
  } else {
    malloc_allocator_ = &mem_context_->get_malloc_allocator();
  }
  return ret;
}

//RCTE operator may encounter -4013 memory problem when it want to allocate a very large memory for a new round of iteration data
//at that time, memory is stored in input_rows_, so we have to free them before reset allocator
void ObBreadthFirstSearchBulkOp::free_input_rows_mem()
{
  if (OB_ISNULL(malloc_allocator_)) {
    // do nothing
  } else {
    for (int64_t i = 0; i < input_rows_.size(); ++i) {
      if (OB_NOT_NULL(input_rows_.at(i))) {
        malloc_allocator_->free(input_rows_.at(i));
      }
    }
    input_rows_.reset();
  }
}

void ObBreadthFirstSearchBulkOp::free_last_iter_mem()
{
  if (OB_ISNULL(malloc_allocator_)) {
    // do nothing
  } else {
    for (int64_t i = 0; i < last_iter_input_rows_.size(); ++i) {
      if (OB_NOT_NULL(last_iter_input_rows_.at(i))) {
        malloc_allocator_->free(last_iter_input_rows_.at(i));
      }
    }
    last_iter_input_rows_.reset();
  }
}

int ObBreadthFirstSearchBulkOp::save_to_store_row(ObIAllocator &allocator,
                                                  const ObIArray<ObExpr *> &exprs,
                                                  ObEvalCtx &eval_ctx,
                                                  ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  int64_t row_size;
  if (OB_FAIL(ObChunkDatumStore::row_copy_size(exprs, eval_ctx, row_size))) {
    LOG_WARN("failed to calc copy size", K(ret));
  } else {
    int64_t head_size = sizeof(ObChunkDatumStore::StoredRow);
    int64_t buffer_len = row_size + head_size + ROW_EXTRA_SIZE;
    char *buf;
    if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc buf failed", K(ret));
    } else if (OB_FAIL(ObChunkDatumStore::StoredRow::build(store_row, exprs, eval_ctx, buf,
                                                           buffer_len,
                                                           static_cast<int32_t>(ROW_EXTRA_SIZE)))) {
      LOG_WARN("failed to build stored row", K(ret), K(buffer_len), K(row_size));
    }
  }
  return ret;
}
