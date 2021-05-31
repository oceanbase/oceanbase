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
#include "ob_search_method.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObSearchMethod::reuse()
{
  input_rows_.reuse();
  sort_.reuse();
  return OB_SUCCESS;
}

int ObSearchMethod::add_row(const common::ObNewRow* input_row)
{
  int ret = OB_SUCCESS;
  ObNewRow* new_row = nullptr;
  if (input_rows_.empty() && 0 == input_rows_.get_capacity() && OB_FAIL(input_rows_.reserve(INIT_ROW_COUNT))) {
    LOG_WARN("Failed to pre allocate array", K(ret));
  } else if (OB_ISNULL(input_row)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("Input row is null", K(ret));
  } else if (OB_FAIL(ObPhyOperator::deep_copy_row(*input_row, new_row, allocator_))) {
    LOG_WARN("Deep copy input row failed", K(ret));
  } else if (OB_FAIL(input_rows_.push_back(new_row))) {
    LOG_WARN("Push new row to result input error", K(ret));
  } else {
    LOG_DEBUG("Add a new row", KPC(new_row));
  }
  return ret;
}

int ObSearchMethod::sort_input_rows()
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  // set sort column
  if (OB_FAIL(sort_.set_sort_columns(sort_columns_, 0))) {
    LOG_WARN("Failed to set sort columns", K(ret));
  }
  // sort
  for (int64_t i = 0; OB_SUCC(ret) && i < input_rows_.count(); i++) {
    if (OB_FAIL(sort_.add_row(*input_rows_.at(i), need_sort, false))) {
      LOG_WARN("Failed to add row", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sort_.sort_rows())) {
    LOG_WARN("Sort result input row failed", K(ret));
  } else {
    input_rows_.reuse();
    const ObNewRow* row = nullptr;
    while (OB_SUCC(sort_.get_next_row(row))) {
      if (OB_FAIL(input_rows_.push_back(const_cast<common::ObNewRow*>(row)))) {
        LOG_WARN("Failed to push back row", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  // reuse
  sort_.reuse();
  return ret;
}

int ObSearchMethod::sort_rownodes(ObArray<ObTreeNode>& sort_array)
{
  int ret = OB_SUCCESS;
  if (!sort_array.empty()) {
    LOG_DEBUG("Sort row nodes", K(sort_array.count()));
    ObTreeNode* first_row = &sort_array.at(0);
    ObNodeComparer comparer(sort_columns_, &ret);
    std::sort(first_row, first_row + sort_array.count(), comparer);
    if (OB_SUCCESS != ret) {
      LOG_WARN("Failed to do sort", K(ret));
    }
  }
  return ret;
}

int ObSearchMethod::add_sort_column(ObSortColumn col)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_columns_.push_back(col))) {
    LOG_WARN("Add sort col info failed", K(ret));
  }
  return ret;
}

int ObSearchMethod::add_cycle_by_column(ObColumnInfo col)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cycle_by_columns_.push_back(col))) {
    LOG_WARN("Add cycle col info failed", K(ret));
  }
  return ret;
}

int ObSearchMethod::is_same_row(ObNewRow& row_1st, ObNewRow& row_2nd, bool& is_cycle)
{
  int ret = OB_SUCCESS;
  if (row_1st.get_count() != row_2nd.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Inconformity row schema", K(ret), K(row_1st), K(row_2nd));
  } else if (cycle_by_columns_.empty()) {
    // detect whole row
    is_cycle = false;
    if (row_1st == row_2nd) {
      is_cycle = true;
      ret = OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE;
      LOG_WARN("Cycle detected while executing recursive WITH query", K(ret));
    }
  } else {
    // detect some obj cell
    is_cycle = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < cycle_by_columns_.count(); ++i) {
      uint64_t index = cycle_by_columns_.at(i).index_;
      if (index >= row_1st.get_count() || index >= row_2nd.get_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column index out of range", K(ret));
      } else if (row_1st.get_cell(index) != row_2nd.get_cell(index)) {
        is_cycle = false;
        break;
      }
    }  // end for
  }
  return ret;
}

int ObDepthFisrtSearch::add_cycle_by_column(ObColumnInfo col)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSearchMethod::add_cycle_by_column(col))) {
    LOG_WARN("Add cycle col info failed", K(ret));
  } else if (OB_FAIL(hash_col_idx_.push_back(col))) {
    LOG_WARN("Add cycle col info failed", K(ret));
  }
  return ret;
}

int ObDepthFisrtSearch::reuse()
{
  ObSearchMethod::reuse();
  last_node_level_ = UINT64_MAX;
  hash_filter_rows_.reuse();
  return OB_SUCCESS;
}

int ObDepthFisrtSearch::finish_add_row(bool sort)
{
  int ret = OB_SUCCESS;
  if (sort && OB_FAIL(sort_input_rows())) {
    LOG_WARN("Sort input rows failed", K(ret));
  } else if (input_rows_.empty()) {
    if (current_search_path_.empty()) {
      // do nothing
    } else {
      int64_t count = current_search_path_.count();
      common::ObNewRow* tmp_row = current_search_path_.at(count - 1);
      ObHashCols hash_col;
      hash_col.init(tmp_row, &hash_col_idx_);
      if (OB_FAIL(hash_filter_rows_.erase_refactored(hash_col))) {
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
      node.row_ = input_rows_.at(i);
      node.tree_level_ = (UINT64_MAX == last_node_level_) ? 0 : last_node_level_ + 1;
      if (OB_FAIL(is_depth_cycle_node(node))) {
        LOG_WARN("Check cycle node failed", K(ret));
      } else if (OB_FAIL(search_stack_.push_front(node))) {
        LOG_WARN("Push data to result hold stack failed", K(ret));
      } else {
      }
    }  // end for
  }
  input_rows_.reuse();
  if (OB_SUCC(ret) && !recycle_rows_.empty()) {
    ARRAY_FOREACH(recycle_rows_, i)
    {
      allocator_.free(recycle_rows_.at(i));
    }
    recycle_rows_.reuse();
  }
  return ret;
}

int ObDepthFisrtSearch::is_depth_cycle_node(ObTreeNode& node)
{
  int ret = OB_SUCCESS;
  ObHashCols hash_col;
  hash_col.init(node.row_, &hash_col_idx_);
  if (OB_FAIL(hash_filter_rows_.exist_refactored(hash_col))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      node.is_cycle_ = true;
      if (cycle_by_columns_.empty()) {
        ret = OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE;
        LOG_WARN("Cycle detected while executing recursive WITH query", K(ret));
      }
    } else {
      LOG_WARN("Failed to find in hashmap", K(ret));
    }
  } else {
    // succ
  }
  return ret;
}

int ObDepthFisrtSearch::adjust_stack(ObTreeNode& node)
{
  int ret = OB_SUCCESS;
  last_node_level_ = node.tree_level_;
  if (0 == hash_col_idx_.count()) {
    // init hash column
    for (int64_t i = 0; i < node.row_->get_count(); ++i) {
      ObColumnInfo col_info;
      col_info.cs_type_ = node.row_->get_cell(i).get_collation_type();
      col_info.index_ = i;
      hash_col_idx_.push_back(col_info);
    }
  }

  if (current_search_path_.count() < node.tree_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The current node level is 2 or more large than last node level", K(ret));
  } else if (current_search_path_.count() > node.tree_level_) {
    /**
     * Tree of all rows is:
     *              A
     *      AA            AB
     *  AAA  AAB      ABA    ABB
     * current_search_path_ is A->AA->AAB
     * when current node goes to AB, path will be A->AB
     */
    int64_t count = current_search_path_.count();
    int64_t pop_times = count - node.tree_level_;
    common::ObNewRow* tmp_row = nullptr;
    while (OB_SUCC(ret) && pop_times > 0) {
      --pop_times;
      ObHashCols hash_col;
      if (OB_FAIL(current_search_path_.pop_back(tmp_row))) {
        LOG_WARN("Failed to pop stack", K(ret));
      } else if (FALSE_IT(hash_col.init(tmp_row, &hash_col_idx_))) {
      } else if (OB_FAIL(hash_filter_rows_.erase_refactored(hash_col))) {
        LOG_WARN("Earse rows from the hash map failed", K(ret));
      } else if (OB_FAIL(recycle_rows_.push_back(tmp_row))) {
        LOG_WARN("Failed to push back rows", K(ret));
      }
    }
  }

  // add stack
  if (OB_SUCC(ret) && current_search_path_.count() == node.tree_level_) {
    ObHashCols hash_col;
    hash_col.init(node.row_, &hash_col_idx_);
    if (OB_FAIL(current_search_path_.push_back(node.row_))) {
      LOG_WARN("Push new row to result record failed", K(ret));
    } else if (OB_FAIL(hash_filter_rows_.set_refactored(hash_col))) {
      LOG_WARN("Failed to insert row to hashmap", K(ret));
    }
  }
  return ret;
}

int ObDepthFisrtSearch::get_next_non_cycle_node(
    ObList<ObTreeNode, common::ObIAllocator>& result_output, ObTreeNode& node)
{
  int ret = OB_SUCCESS;
  ObTreeNode non_cycle_node;
  bool got_row = false;
  while (OB_SUCC(ret) && !search_stack_.empty()) {
    if (OB_FAIL(search_stack_.pop_front(non_cycle_node))) {
      LOG_WARN("Search stack pop back failed", K(ret));
    } else if (OB_FAIL(result_output.push_back(non_cycle_node))) {
      LOG_WARN("Push back data to result output failed", K(ret));
    } else if (non_cycle_node.is_cycle_) {
      if (OB_FAIL(recycle_rows_.push_back(non_cycle_node.row_))) {
        LOG_WARN("Failed to push back rows", K(ret));
      }
    } else {
      got_row = true;
      node = non_cycle_node;
      break;
    }
  }  // end while
  if (OB_SUCC(ret) && !got_row) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObBreadthFisrtSearch::add_new_level()
{
  int ret = OB_SUCCESS;
  ObTreeNode new_level_node;
  new_level_node.row_ = nullptr;
  if (OB_FAIL(search_queue_.push_back(new_level_node))) {
    LOG_WARN("Push back data to result hold queue failed", K(ret));
  }
  return ret;
}

int ObBreadthFisrtSearch::reuse()
{
  ObSearchMethod::reuse();
  current_parent_node_ = &bst_root_;
  bst_root_.child_num_ = 0;
  bst_root_.children_ = nullptr;
  bst_root_.parent_ = nullptr;
  bst_root_.row_ = nullptr;
  search_queue_.reset();
  search_results_.reuse();
  return OB_SUCCESS;
}

int ObBreadthFisrtSearch::init_new_nodes(ObBFSTreeNode* last_bstnode, int64_t child_num)
{
  int ret = OB_SUCCESS;
  void* childs_ptr = nullptr;
  // init memory of a node
  if (OB_UNLIKELY(0 == child_num)) {
    // do nothing
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

int ObBreadthFisrtSearch::is_breadth_cycle_node(ObTreeNode& node)
{
  int ret = OB_SUCCESS;
  ObBFSTreeNode* tmp = current_parent_node_;
  ObNewRow* row = node.row_;
  if (OB_ISNULL(tmp) || OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The last_bstnode and row an not be null", K(ret), KPC(row));
  } else {
    // row_ of bst_root_ is empty.
    while (OB_SUCC(ret) && OB_NOT_NULL(tmp) && OB_NOT_NULL(tmp->row_)) {
      ObNewRow* row_1st = row;
      ObNewRow* row_2nd = tmp->row_;
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

int ObBreadthFisrtSearch::get_next_non_cycle_node(
    ObList<ObTreeNode, common::ObIAllocator>& result_output, ObTreeNode& node)
{
  int ret = OB_SUCCESS;
  ObTreeNode non_cycle_node;
  bool got_row = false;
  while (OB_SUCC(ret) && !search_queue_.empty()) {
    if (OB_FAIL(search_queue_.pop_front(non_cycle_node))) {
      LOG_WARN("Get row from hold queue failed", K(ret));
    } else if (OB_FAIL(result_output.push_back(non_cycle_node))) {
      LOG_WARN("Failed to push row to output ", K(ret));
    } else if (non_cycle_node.is_cycle_) {
      if (OB_FAIL(recycle_rows_.push_back(non_cycle_node.row_))) {
        LOG_WARN("Failed to push back rows", K(ret));
      }
    } else {
      got_row = true;
      node = non_cycle_node;
      break;
    }
  }  // end while
  if (OB_SUCC(ret) && !got_row) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObBreadthFisrtSearch::update_parent_node(ObTreeNode& node)
{
  int ret = OB_SUCCESS;
  current_parent_node_ = node.in_bstree_node_;
  return ret;
}

int ObBreadthFisrtSearch::add_result_rows()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_new_nodes(current_parent_node_, input_rows_.count()))) {
    LOG_WARN("Failed to init new bst node", K(ret));
  } else {
    ARRAY_FOREACH(input_rows_, i)
    {
      void* ptr = nullptr;
      ObBFSTreeNode* tmp = nullptr;
      ObTreeNode node;
      node.row_ = input_rows_.at(i);
      // breadth search tree
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObBFSTreeNode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Alloc memory for row failed", "size", sizeof(ObBFSTreeNode), K(ret));
      } else {
        tmp = new (ptr) ObBFSTreeNode();
        tmp->row_ = input_rows_.at(i);
        tmp->parent_ = current_parent_node_;
        current_parent_node_->children_[i] = tmp;
        node.in_bstree_node_ = tmp;
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

int ObBreadthFisrtSearch::finish_add_row(bool sort)
{
  int ret = OB_SUCCESS;
  if (sort && OB_FAIL(sort_rownodes(search_results_))) {
    LOG_WARN("Failed to sort results", K(ret));
  } else if (!search_queue_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The last result still has residual", K(search_queue_), K(ret));
  } else {
    ARRAY_FOREACH(search_results_, i)
    {
      if (OB_FAIL(search_queue_.push_back(search_results_.at(i)))) {
        LOG_WARN("Push back failed", K(ret));
      }
    }
  }
  search_results_.reuse();
  return ret;
}
