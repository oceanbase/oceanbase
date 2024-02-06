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

#ifndef OB_DEPTH_FIRST_SEARCH_OP_H_
#define OB_DEPTH_FIRST_SEARCH_OP_H_

#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/list/ob_list.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{

class ObSearchMethodOp
{
public:
  typedef struct _BreadthFirstSearchTreeNode {
    _BreadthFirstSearchTreeNode() :
      child_num_(0),
      stored_row_(nullptr),
      children_(nullptr),
      parent_(nullptr)
    {}
    int64_t child_num_;
    ObChunkDatumStore::StoredRow* stored_row_;
    struct _BreadthFirstSearchTreeNode** children_;
    struct _BreadthFirstSearchTreeNode* parent_;
    TO_STRING_KV("row ", stored_row_, "child_num_", child_num_);
  } ObBFSTreeNode;

  typedef struct _TreeNode
  {
    _TreeNode() :
      is_cycle_(false),
      tree_level_(0),
      stored_row_(nullptr),
      in_bstree_node_(nullptr)
    {
    }
    bool is_cycle_;
    uint64_t tree_level_;
    ObChunkDatumStore::StoredRow* stored_row_;
    ObBFSTreeNode* in_bstree_node_;
    TO_STRING_KV("is tree level", tree_level_, "is cycle", is_cycle_, "row", stored_row_);
  } ObTreeNode;

  struct ObNodeComparer
  {
    explicit ObNodeComparer(
      const common::ObIArray<ObSortFieldCollation> &sort_collations,
      const common::ObIArray<ObExpr *> &exprs,
      int *err)
      : sort_collations_(sort_collations),
        exprs_(exprs),
        err_(err)
    {
    }
    bool operator()(const ObTreeNode &r1, const ObTreeNode &r2)
    {
      const ObChunkDatumStore::StoredRow *l = r1.stored_row_;
      const ObChunkDatumStore::StoredRow *r = r2.stored_row_;
      return cmp_stored_row(l, r);
    }

    bool operator()(const ObChunkDatumStore::StoredRow *l, const ObChunkDatumStore::StoredRow *r)
    {
      return cmp_stored_row(l, r);
    }

    inline bool cmp_stored_row(const ObChunkDatumStore::StoredRow *l,
                               const ObChunkDatumStore::StoredRow *r)
    {
      bool bret = false;
      if (OB_UNLIKELY(common::OB_SUCCESS != *err_)) {
        // do nothing if we already have an error,
        // so we can finish the sort process ASAP.
      } else if (OB_ISNULL(l)
                  || OB_ISNULL(r)
                  || (l->cnt_ != r->cnt_)) {
        *err_ = OB_ERR_UNEXPECTED;
        SQL_LOG_RET(WARN, *err_, "invalid parameter", KPC(l), KPC(r), K(*err_));
      } else {
        const ObDatum *lcells = l->cells();
        const ObDatum *rcells = r->cells();
        int cmp = 0;
        for (int64_t i = 0; OB_SUCCESS == *err_ && 0 == cmp && i < sort_collations_.count(); i++) {
          const int64_t idx = sort_collations_.at(i).field_idx_;
          if (idx >= exprs_.count()) {
            *err_ = OB_ERR_UNEXPECTED;
            SQL_LOG_RET(WARN, *err_, "compare column id greater than exprs count", K(*err_),
                      K(idx), K(exprs_.count()));
          } else {
            bool null_first = (NULL_FIRST == sort_collations_.at(i).null_pos_);
            ObExprCmpFuncType cmp_func = null_first ? exprs_.at(idx)->basic_funcs_->null_first_cmp_
                                          : exprs_.at(idx)->basic_funcs_->null_last_cmp_;
            *err_ = cmp_func(lcells[idx], rcells[idx], cmp);
            if (OB_SUCCESS != *err_) {
              SQL_LOG_RET(WARN, *err_, "cmp failed", K(idx), KPC(l), KPC(r), K(*err_));
            } else if (cmp < 0) {
              bret = sort_collations_.at(i).is_ascending_;
            } else if (cmp > 0) {
              bret = !sort_collations_.at(i).is_ascending_;
            }
          }
        }
      }
      return bret;
    }

  private:
    const common::ObIArray<ObSortFieldCollation> &sort_collations_;
    //用于获取比较函数的表达式
    const common::ObIArray<ObExpr *> &exprs_;
    int *err_;
  };

  class ObCycleHash
  {
  public:
    ObCycleHash()
      : row_(NULL),
        hash_col_idx_(NULL),
        exprs_(NULL),
        hash_val_(0)
    {
    }
    ObCycleHash(const ObChunkDatumStore::StoredRow *row,
                const common::ObIArray<uint64_t> *hash_col_idx,
                const common::ObIArray<ObExpr *> *exprs)
      : row_(row),
        hash_col_idx_(hash_col_idx),
        exprs_(exprs),
        hash_val_(0)
    {
    }
    ~ObCycleHash()
    {}

    uint64_t hash() const
    {
      if (hash_val_ == 0) {
        hash_val_ = inner_hash();
      }
      return hash_val_;
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    
    uint64_t inner_hash() const;
    bool operator ==(const ObCycleHash &other) const;

  public:
    const ObChunkDatumStore::StoredRow *row_;
    const common::ObIArray<uint64_t> *hash_col_idx_;
    const common::ObIArray<ObExpr *> *exprs_;
    mutable uint64_t hash_val_;
  };

  // input row的初始化大小，以128开始。
  static const int64_t INIT_ROW_COUNT = 1<<7l;
  // 探测深度优先路径上是否成环的哈希表大小，32的树高足够了。
  static const int64_t CTE_SET_NUM = 1<<5l;
public:
  explicit ObSearchMethodOp(common::ObIAllocator &allocator, const ExprFixedArray &left_output,
      const common::ObIArray<ObSortFieldCollation> &sort_collations,
      const common::ObIArray<uint64_t> &cycle_by_columns)
    : allocator_(allocator), input_rows_(),
  sort_collations_(sort_collations), cycle_by_columns_(cycle_by_columns),
  left_output_(left_output), last_node_level_(UINT64_MAX) {};
  virtual ~ObSearchMethodOp() = default;

  virtual int finish_add_row(bool sort) = 0;
  virtual int get_next_non_cycle_node(common::ObList<ObTreeNode,
                common::ObIAllocator> &result_output, ObTreeNode &node) = 0;
  virtual int empty() = 0;
  virtual int reuse();

  int add_row(const ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx);
  int sort_input_rows();
  int sort_rownodes(common::ObArray<ObTreeNode> &sort_array);

  // 使用行内容进行比较，若有一样的数据则认为此节点为环
  int is_same_row(ObChunkDatumStore::StoredRow &row_1st, ObChunkDatumStore::StoredRow &row_2nd,
                  bool &is_cycle);
  int64_t count() { return input_rows_.count(); }
  virtual uint64_t get_last_node_level() { return last_node_level_; }
  const static int64_t ROW_EXTRA_SIZE = 0;
protected:
  // hard code seed, 24bit max prime number
  static const int64_t HASH_SEED = 16777213;
  common::ObIAllocator &allocator_;
  common::ObArray<ObChunkDatumStore::StoredRow *> input_rows_;
  const common::ObIArray<ObSortFieldCollation> &sort_collations_;
  const common::ObIArray<uint64_t> &cycle_by_columns_;
  common::ObArray<ObChunkDatumStore::StoredRow*> recycle_rows_;
  const ExprFixedArray &left_output_;
  // 记录当前查询行在树中的level
  uint64_t last_node_level_;
};

class ObDepthFisrtSearchOp : public ObSearchMethodOp
{
  typedef common::hash::ObHashSet<ObCycleHash, common::hash::NoPthreadDefendMode> RowMap;
public:
  ObDepthFisrtSearchOp(common::ObIAllocator &allocator, const ExprFixedArray &left_output,
      const common::ObIArray<ObSortFieldCollation> &sort_collations,
      const common::ObIArray<uint64_t> &cycle_by_columns) :
    ObSearchMethodOp(allocator, left_output, sort_collations, cycle_by_columns),
    hash_filter_rows_(), hash_col_idx_(),
    current_search_path_(), search_stack_(allocator_)
  { }
  virtual ~ObDepthFisrtSearchOp() {
    if (hash_filter_rows_.created()) {
      hash_filter_rows_.destroy();
    }
  }

  virtual int finish_add_row(bool sort) override;
  virtual int reuse() override;
  virtual int empty() override { return search_stack_.empty() && input_rows_.empty(); }

  int init();
  int adjust_stack(ObTreeNode &node);
  int get_next_non_cycle_node(common::ObList<ObTreeNode, common::ObIAllocator> &result_output,
                              ObTreeNode &node) override;

private:
  // 检测一个node是不是环节点
  int is_depth_cycle_node(ObTreeNode &node);

private:
  RowMap hash_filter_rows_;
  common::ObSEArray<uint64_t, 32> hash_col_idx_;
  /**
   *                              level
   *            A                   0
   *      AA         AB             1
   *  AAA  AAB    ABA   ABB         2
   *  例如一次查询中过程中，AA节点是上一次塞給右支进行查询的节点，
   *  current_search_stack_中包含A AA，其count＝level+1。
   *  新获得的结果AAA和AAB。search_stack_经历这次查询由A AB AA，
   *  变成A AB AAA AAB。
   */
  common::ObArray<ObChunkDatumStore::StoredRow *> current_search_path_;
  common::ObList<ObTreeNode, common::ObIAllocator> search_stack_;
};

/**
 * 由于需要判断环的存在，广度优先整个树都会被保存在内存中；
 * 能用深度优先的时候尽量不要使用广度优先。
 */
class ObBreadthFisrtSearchOp : public ObSearchMethodOp
{
public:
  ObBreadthFisrtSearchOp(common::ObIAllocator &allocator, const ExprFixedArray &left_output,
      const common::ObIArray<ObSortFieldCollation> &sort_collations,
      const common::ObIArray<uint64_t> &cycle_by_columns) :
    ObSearchMethodOp(allocator, left_output, sort_collations, cycle_by_columns), bst_root_(),
    current_parent_node_(&bst_root_), search_queue_(allocator), search_results_() {
      last_node_level_ = 0;
    }
  virtual ~ObBreadthFisrtSearchOp() = default;

  virtual int finish_add_row(bool sort) override;
  virtual int reuse() override;
  virtual int empty() override { return input_rows_.empty() && search_queue_.empty()
                                        && search_results_.empty(); }

  int add_result_rows();
  int get_next_non_cycle_node(common::ObList<ObTreeNode, common::ObIAllocator> &result_output,
                              ObTreeNode &node) override;
  int update_parent_node(ObTreeNode &node);

private:
  int init_new_nodes(ObBFSTreeNode *last_bstnode, int64_t child_num);
  int is_breadth_cycle_node(ObTreeNode &node);
  int add_new_level();

private:
  // breadth first search的root节点
  ObBFSTreeNode bst_root_;
  /**
   *            A
   *      AA         AB
   *  AAA  AAB    ABA   ABB
   *  例如一次查询中过程中，current_parent_node_指向AA
   *  search_queue_中包含AA AB是查询层
   *  search_results_中AAA AAB是查询结果层
   */
  ObBFSTreeNode* current_parent_node_;
  common::ObList<ObTreeNode, common::ObIAllocator> search_queue_;
  common::ObArray<ObTreeNode> search_results_;
};

}
}

#endif
