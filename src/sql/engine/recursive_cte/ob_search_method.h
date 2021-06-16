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

#ifndef OB_DEPTH_FIRST_SEARCH_H_
#define OB_DEPTH_FIRST_SEARCH_H_

#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/list/ob_list.h"
#include "common/row/ob_row.h"

namespace oceanbase {
namespace sql {

class ObSearchMethod {
public:
  typedef struct _BreadthFirstSearchTreeNode {
    _BreadthFirstSearchTreeNode() : child_num_(0), row_(nullptr), children_(nullptr), parent_(nullptr)
    {}
    int64_t child_num_;
    common::ObNewRow* row_;
    struct _BreadthFirstSearchTreeNode** children_;
    struct _BreadthFirstSearchTreeNode* parent_;
    TO_STRING_KV("row ", row_, "child_num_", child_num_);
  } ObBFSTreeNode;

  typedef struct _TreeNode {
    _TreeNode() : is_cycle_(false), tree_level_(0), row_(nullptr), in_bstree_node_(nullptr)
    {}
    bool is_cycle_;
    uint64_t tree_level_;
    common::ObNewRow* row_;
    ObBFSTreeNode* in_bstree_node_;
    TO_STRING_KV("is tree level", tree_level_, "is cycle", is_cycle_, "row ", row_)
  } ObTreeNode;

  struct ObNodeComparer {
    explicit ObNodeComparer(const common::ObIArray<ObSortColumn>& sort_columns, int* err)
        : sort_columns_(sort_columns), err_(err)
    {}
    bool operator()(const ObTreeNode& r1, const ObTreeNode& r2)
    {
      bool bret = false;
      if (OB_UNLIKELY(common::OB_SUCCESS != *err_)) {
        // do nothing if we already have an error,
        // so we can finish the sort process ASAP.
      } else {
        int cmp = 0;
        for (int64_t i = 0; common::OB_SUCCESS == *err_ && 0 == cmp && i < sort_columns_.count(); ++i) {
          int64_t idx = sort_columns_.at(i).index_;
          cmp = r1.row_->get_cell(idx).compare(r2.row_->get_cell(idx), sort_columns_.at(i).cs_type_);
          if (cmp < 0) {
            bret = sort_columns_.at(i).is_ascending();
          } else if (cmp > 0) {
            bret = !sort_columns_.at(i).is_ascending();
          } else {
          }
        }  // end for
      }
      return bret;
    }

  private:
    const common::ObIArray<ObSortColumn>& sort_columns_;
    int* err_;
  };
  static const int64_t INIT_ROW_COUNT = 1 << 7l;
  // initial size of hash table for loop search
  static const int64_t CTE_SET_NUM = 1 << 5l;

public:
  explicit ObSearchMethod(common::ObIAllocator& allocator)
      : allocator_(allocator), input_rows_(), sort_columns_(), cycle_by_columns_(), sort_(), op_schema_objs_(nullptr){};
  virtual ~ObSearchMethod() = default;

  virtual int finish_add_row(bool sort) = 0;
  virtual int get_next_non_cycle_node(
      common::ObList<ObTreeNode, common::ObIAllocator>& result_output, ObTreeNode& node) = 0;
  virtual int empty() = 0;
  virtual int reuse();
  virtual int add_cycle_by_column(common::ObColumnInfo col);

  int add_row(const common::ObNewRow* row);
  int sort_input_rows();
  int sort_rownodes(common::ObArray<ObTreeNode>& sort_array);
  int add_sort_column(ObSortColumn col);
  void set_op_schema_objs(const common::ObIArray<ObOpSchemaObj>& op_schema_objs)
  {
    op_schema_objs_ = &op_schema_objs;
  }

  int is_same_row(common::ObNewRow& row_1st, common::ObNewRow& row_2nd, bool& is_cycle);
  int64_t count()
  {
    return input_rows_.count();
  }

protected:
  common::ObIAllocator& allocator_;
  common::ObArray<common::ObNewRow*> input_rows_;
  common::ObSEArray<ObSortColumn, 32> sort_columns_;
  common::ObSEArray<common::ObColumnInfo, 32> cycle_by_columns_;
  common::ObArray<common::ObNewRow*> recycle_rows_;
  ObBaseSort sort_;
  const common::ObIArray<ObOpSchemaObj>* op_schema_objs_;
};

class ObDepthFisrtSearch : public ObSearchMethod {
  typedef common::hash::ObHashSet<ObHashCols, common::hash::NoPthreadDefendMode> RowMap;

public:
  ObDepthFisrtSearch(common::ObIAllocator& allocator)
      : ObSearchMethod(allocator),
        hash_filter_rows_(),
        hash_col_idx_(),
        last_node_level_(UINT64_MAX),
        current_search_path_(),
        search_stack_(allocator_)
  {}
  virtual ~ObDepthFisrtSearch()
  {
    if (hash_filter_rows_.created()) {
      hash_filter_rows_.destroy();
    }
  }

  virtual int finish_add_row(bool sort) override;
  virtual int reuse() override;
  virtual int empty() override
  {
    return search_stack_.empty() && input_rows_.empty();
  }
  virtual int add_cycle_by_column(common::ObColumnInfo col) override;

  int init()
  {
    return hash_filter_rows_.create(CTE_SET_NUM);
  }
  int adjust_stack(ObTreeNode& node);
  int get_next_non_cycle_node(
      common::ObList<ObTreeNode, common::ObIAllocator>& result_output, ObTreeNode& node) override;

private:
  int is_depth_cycle_node(ObTreeNode& node);

private:
  RowMap hash_filter_rows_;
  common::ObSEArray<common::ObColumnInfo, 32> hash_col_idx_;
  // record level of current row in the tree.
  uint64_t last_node_level_;
  common::ObArray<common::ObNewRow*> current_search_path_;
  common::ObList<ObTreeNode, common::ObIAllocator> search_stack_;
};

class ObBreadthFisrtSearch : public ObSearchMethod {
public:
  ObBreadthFisrtSearch(common::ObIAllocator& allocator)
      : ObSearchMethod(allocator),
        bst_root_(),
        current_parent_node_(&bst_root_),
        search_queue_(allocator),
        search_results_()
  {}
  virtual ~ObBreadthFisrtSearch() = default;

  virtual int finish_add_row(bool sort) override;
  virtual int reuse() override;
  virtual int empty() override
  {
    return input_rows_.empty() && search_queue_.empty() && search_results_.empty();
  }

  int add_result_rows();
  int get_next_non_cycle_node(
      common::ObList<ObTreeNode, common::ObIAllocator>& result_output, ObTreeNode& node) override;
  int update_parent_node(ObTreeNode& node);

private:
  int init_new_nodes(ObBFSTreeNode* last_bstnode, int64_t child_num);
  int is_breadth_cycle_node(ObTreeNode& node);
  int add_new_level();

private:
  ObBFSTreeNode bst_root_;
  /**
   *            A
   *      AA         AB
   *  AAA  AAB    ABA   ABB
   *  when current_parent_node_ is AA:
   *  search_queue_ contains AA and AB
   *  search_results_ contains AAA and AAB
   */
  ObBFSTreeNode* current_parent_node_;
  common::ObList<ObTreeNode, common::ObIAllocator> search_queue_;
  common::ObArray<ObTreeNode> search_results_;
};

}  // namespace sql
}  // namespace oceanbase

#endif
