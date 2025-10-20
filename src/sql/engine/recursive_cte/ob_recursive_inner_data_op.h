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

#ifndef OB_RECURSIVE_INNER_DATA_OP_
#define OB_RECURSIVE_INNER_DATA_OP_

#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "ob_search_method_op.h"
#include "ob_fake_cte_table_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_ra_datum_store.h"

namespace oceanbase
{
namespace sql
{

//only used in Recursive Union *Distinct*
struct ObRCTEStoredRowWrapper
{
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  const static int64_t DEFAULT_HASH_VALUE = 99194853094755497L;
  ObRCTEStoredRowWrapper(uint64_t hash_value, int64_t row_id) :
    hash_value_(hash_value),
    row_id_(row_id)
  {}

  uint64_t hash() const
  {
    return hash_value_;
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  ObRCTEStoredRowWrapper *&next()
  {
    return *reinterpret_cast<ObRCTEStoredRowWrapper **>(&next_);
  }

  void *next_{nullptr};
  const uint64_t hash_value_{0};

  const int64_t row_id_{0}; // used to index data
  TO_STRING_KV(K_(hash_value), K_(row_id));
};

class ObRCTEHashTable : public ObExtendHashTable<ObRCTEStoredRowWrapper>
{
public:
  explicit ObRCTEHashTable() : ObExtendHashTable(), cmp_funcs_(nullptr)
  {}

  int exist(uint64_t hash_val, const ObIArray<ObExpr *> &exprs, ObEvalCtx *eval_ctx,
            ObRADatumStore::Reader *reader, bool &exist);

  int init(ObIAllocator *allocator, lib::ObMemAttr &mem_attr,
           const common::ObIArray<ObSortFieldCollation> *sort_collations,
           const common::ObIArray<ObCmpFunc> *cmp_funcs, int64_t initial_size = INITIAL_SIZE);

private:
  int likely_equal(const ObIArray<ObExpr *> &epxrs, ObEvalCtx *eval_ctx,
                   const ObRCTEStoredRowWrapper &right, ObRADatumStore::Reader *reader,
                   bool &result);

private:
  const common::ObIArray<ObSortFieldCollation> *sort_collations_;
  const common::ObIArray<ObCmpFunc> *cmp_funcs_;
  static const int64_t HASH_BUCKET_PREFETCH_MAGIC_NUM = 4 * 1024;
};

class ObRecursiveInnerDataOp {
public:
  struct RowComparer;
  enum RecursiveUnionState {
    R_UNION_BEGIN,
    R_UNION_READ_LEFT,
    R_UNION_READ_RIGHT,
    R_UNION_END,
    R_UNION_STATE_COUNT
  };
public:
  explicit ObRecursiveInnerDataOp(ObEvalCtx &eval_ctx,
                                  ObExecContext &exec_ctx,
                                  const common::ObIArray<ObExpr *> &output_union_exprs) :
      state_(RecursiveUnionState::R_UNION_READ_LEFT),
      stored_row_buf_(),
      pump_operator_(nullptr),
      left_op_(nullptr),
      right_op_(nullptr),
      eval_ctx_(eval_ctx),
      ctx_(exec_ctx),
      output_union_exprs_(output_union_exprs)
  {
  }
  virtual ~ObRecursiveInnerDataOp() = default;

  inline void set_left_child(ObOperator* op) { left_op_ = op; }
  inline void set_right_child(ObOperator* op) { right_op_ = op; }

  virtual int set_fake_cte_table_and_reader(ObFakeCTETableOp *cte_table)
  {
    pump_operator_ = cte_table;
    return OB_SUCCESS;
  }

  virtual int get_next_row() { return OB_SUCCESS; }
  virtual int get_next_batch(const int64_t batch_size, ObBatchRows &brs) {  return OB_SUCCESS; }
  virtual int rescan() { return OB_SUCCESS; }
  int init();
  virtual int init(const ObExpr *search_expr, const ObExpr *cycle_expr) { return OB_SUCCESS; }
  void set_batch_size(const int64_t batch_size) { batch_size_ = batch_size; }

  virtual void destroy() {}
private:
  
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRecursiveInnerDataOp);
protected:
  RecursiveUnionState state_;
  common::ObArenaAllocator stored_row_buf_;
  ObFakeCTETableOp* pump_operator_;
  ObOperator* left_op_;
  ObOperator* right_op_;
  
  ObEvalCtx &eval_ctx_;
  ObExecContext &ctx_;
  const common::ObIArray<ObExpr *> &output_union_exprs_;
  int64_t batch_size_ = 1;
};

class ObRecursiveInnerDataOracleOp : public ObRecursiveInnerDataOp{
  using ObTreeNode = ObSearchMethodOp::ObTreeNode;
  friend class ObRecursiveUnionOp;
  friend class ObRecursiveUnionSpec;
public:
  struct RowComparer;
  enum SearchStrategyType
  {
    DEPTH_FRIST, BREADTH_FRIST, BREADTH_FIRST_BULK
  };
public:
  explicit ObRecursiveInnerDataOracleOp(ObEvalCtx &eval_ctx,
                                  ObExecContext &exec_ctx,
                                  const common::ObIArray<ObExpr *> &output_union_exprs,
                                  const ExprFixedArray &left_output,
                                  const common::ObIArray<ObSortFieldCollation> &sort_collations,
                                  const common::ObIArray<uint64_t> &cycle_by_col_lists,
                                  const int64_t identify_seq_offset) :
      ObRecursiveInnerDataOp(eval_ctx, exec_ctx, output_union_exprs),
      sort_collations_(sort_collations),
      result_output_(stored_row_buf_),
      search_expr_(nullptr),
      cycle_expr_(nullptr),
      cycle_value_(),
      non_cycle_value_(),
      cte_columns_(nullptr),
      ordering_column_(1),
      dfs_pump_(stored_row_buf_, left_output, sort_collations, cycle_by_col_lists),
      bfs_pump_(stored_row_buf_, left_output, sort_collations, cycle_by_col_lists),
      bfs_bulk_pump_(stored_row_buf_, left_output, sort_collations, cycle_by_col_lists),
      identify_seq_offset_(identify_seq_offset)
  {
  }
  virtual ~ObRecursiveInnerDataOracleOp() = default;

  inline void set_search_strategy(ObRecursiveInnerDataOracleOp::SearchStrategyType strategy)
  {
    search_type_ = strategy;
  }
  int add_sort_collation(ObSortFieldCollation sort_collation);
  int add_cycle_column(uint64_t index);
  int add_cmp_func(ObCmpFunc cmp_func);
  int get_next_row();
  int get_next_batch(const int64_t batch_size, ObBatchRows &brs);
  int rescan();
  int set_fake_cte_table_empty();
  int init(const ObExpr *search_expr, const ObExpr *cycle_expr);
  void set_cte_column_exprs(common::ObIArray<ObExpr *> *exprs) { cte_columns_ = exprs; }
  void set_batch_size(const int64_t batch_size) { batch_size_ = batch_size; };

  virtual void destroy();
private:
  
  int add_pseudo_column(bool cycle = false);
  int try_get_left_rows(bool batch_mode, int64_t batch_size, int64_t &read_rows);
  int try_get_right_rows(bool batch_mode, int64_t batch_size, int64_t &read_rows);
  int try_format_output_row(int64_t &read_rows);
  int try_format_output_batch(int64_t batch_size, int64_t &read_rows);
  /**
   * recursive union的左儿子被称为plan a，右儿子被称为plan b
   * plan a会产出初始数据，recursive union本身控制递归的进度,
   * 右儿子是递归执行的plan
   */
  int get_all_data_from_left_child();
  int get_all_data_from_left_batch();
  int get_all_data_from_right_child();
  int get_all_data_from_right_batch();
  // 深度优先递归中，进行行的UNION ALL操作
  int depth_first_union(const bool sort = true);
  // 广度优先递归中，进行行的UNION ALL操作
  int breadth_first_union(bool left_branch, bool &continue_search);
  // 广度优先批量搜索递归中，进行行的UNION ALL操作
  int breadth_first_bulk_union(bool left_branch);
  int start_new_level(bool left_branch);
  // 将一行数据吐给fake cte table算子，它将作为下一次plan b的输入
  int fake_cte_table_add_row(ObTreeNode &node);
  // 将一批数据吐给fake cte table算子，它将作为下一次plan b的输入
  int fake_cte_table_add_bulk_rows(bool left_branch);
  // 设置cte table column expr的值
  int assign_to_cur_row(ObChunkDatumStore::StoredRow *stored_row);
  ObSearchMethodOp * get_search_method_bump() {
    if (SearchStrategyType::BREADTH_FIRST_BULK == search_type_) {
      return &bfs_bulk_pump_;
    } else if (SearchStrategyType::BREADTH_FRIST == search_type_) {
      return &bfs_pump_;
    } else {
      return &dfs_pump_;
    }
  };
  bool is_bulk_search() { return SearchStrategyType::BREADTH_FIRST_BULK == search_type_; }
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRecursiveInnerDataOracleOp);
private:
  // 标记深度优先或者广度优先
  SearchStrategyType search_type_;
  // 排序的依据列
  const common::ObIArray<ObSortFieldCollation> &sort_collations_;
  // 检测递归中循环的执行列，列的内容重复则终止执行
  //const common::ObIArray<uint64_t> &cycle_by_col_lists_;
  // 将要输出给下一个算子的数据，伪代码中的R
  common::ObList<ObTreeNode, common::ObIAllocator> result_output_;
  // 伪列
  const ObExpr *search_expr_;
  const ObExpr *cycle_expr_;
  // cycle value
  ObDatum cycle_value_;
  // non-cycle value
  ObDatum non_cycle_value_;
  common::ObIArray<ObExpr *> *cte_columns_;
  /**
   * 用来表示search breadth/depth first by xxx set ordering_column.
   * 这个变量用来计算ordering_column的值。
   * Oracle的解释是：
   * The ordering_column is automatically added to the column list for the query name.
   * The query that selects from query_name can include an ORDER BY on ordering_column to return
   * the rows in the order that was specified by the SEARCH clause.
   */
  int64_t ordering_column_;
  // 深度优先
  ObDepthFirstSearchOp dfs_pump_;
  // 广度优先
  ObBreadthFirstSearchOp bfs_pump_;
  // 广度优先批量搜索
  ObBreadthFirstSearchBulkOp bfs_bulk_pump_;
  int64_t identify_seq_offset_;
};

class ObRecursiveInnerDataMysqlOp : public ObRecursiveInnerDataOp {
  friend class ObRecursiveUnionOp;
  friend class ObRecursiveUnionSpec;
public:
  explicit ObRecursiveInnerDataMysqlOp(ObEvalCtx &eval_ctx,
                                    ObExecContext &exec_ctx,
                                    const common::ObIArray<ObExpr *> &output_union_exprs,

                                    bool is_union_distinct,
                                    const ObHashFuncs &hash_funcs,
                                    const ObSortFuncs &sort_cmp_funs,
                                    const ObSortCollations &deduplicate_sort_collations) :
      ObRecursiveInnerDataOp(eval_ctx, exec_ctx, output_union_exprs),                          
      max_recursion_depth_(0),
      is_union_distinct_(is_union_distinct),
      hash_funcs_(hash_funcs),
      sort_cmp_funs_(sort_cmp_funs),
      deduplicate_sort_collations_(deduplicate_sort_collations)
  {
  }
  virtual ~ObRecursiveInnerDataMysqlOp()
  {}

  int get_next_row();
  int get_next_batch(const int64_t batch_size, ObBatchRows &brs);
  int rescan();
  int init(const ObExpr *search_expr, const ObExpr *cycle_expr);

  int set_fake_cte_table_and_reader(ObFakeCTETableOp *cte_table);

  virtual void destroy();

private:
  /*
   * @Brief: Deduplicate and Update hash table for Recursive Union Distinct
   *         Appending data into intermedia table for Both Recursive Union Distinct/ALL
   *
   * @Input: child_skip[in] -- skips of batch data from lower operator
   *         this_skip[out] -- skips of batch data in this Recursive Union operator
   */
  int handle_batch_data(const common::ObIArray<ObExpr *> &exprs, const int64_t batch_size,
                        ObBitVector *child_skip, ObBitVector *this_skip,
                        uint64_t *hash_values_for_batch);

  int handle_row_data(const common::ObIArray<ObExpr *> &exprs, uint64_t *hash_values_for_batch,
                      bool &is_duplicate);

  /*
   * @Brief: Read and handle *batch* of data from lower operator, no materialization
   */
  int get_next_batch_no_materialization(const int64_t batch_size, ObBatchRows &brs);

  /*
   * @Brief: Read and handle *one row* of data from lower operator, no materialization
   */
  int get_next_row_no_materialization();

  int calc_hash_value_for_row(const common::ObIArray<ObExpr *> &exprs,
                              uint64_t *hash_values_for_batch);

  int calc_hash_value_for_batch(const common::ObIArray<ObExpr *> &exprs, const int64_t batch_size,
                                ObBitVector *child_skip, uint64_t *hash_values_for_batch);

  int convert_batch(const common::ObIArray<ObExpr *> &src_exprs,
                    const common::ObIArray<ObExpr *> &dst_exprs, const int64_t batch_size,
                    ObBitVector *skip);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRecursiveInnerDataMysqlOp);
private:
  uint64_t max_recursion_depth_{0};

  bool is_union_distinct_{false};
  const ObHashFuncs &hash_funcs_;
  const ObSortFuncs &sort_cmp_funs_;
  const ObSortCollations &deduplicate_sort_collations_;

  //Below 3 variable memory are unreleasable until close RCTE operator
  //So here we use ObArenaAllocator (stored_row_buf_) to manage these 3 variables
  ObBitVector *skips_{nullptr};                             // this skip use to skip:
                                                            // a. data that skiped by child operator
                                                            // and
                                                            // b. data already exist in hashtable_
  uint64_t *hash_values_for_batch_{nullptr};
  ObRCTEHashTable hash_table_;
  //While this one only have explict constructor, so we have to use exec_->allocator to construct it after init its corresponding ObRADatumStore
  ObRADatumStore::Reader *hash_value_reader_{nullptr};
  uint64_t write_rows_in_this_iteration_{0};
  uint64_t curr_level_{0};
};

} // end namespace sql
} // end namespace oceanbase

#endif
