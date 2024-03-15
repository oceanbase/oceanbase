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

#ifndef OCEANBASE_SQL_OB_RECURSIVE_UNION_OP_H_
#define OCEANBASE_SQL_OB_RECURSIVE_UNION_OP_H_

#include "sql/engine/set/ob_merge_set_op.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_fake_cte_table_op.h"
#include "ob_recursive_inner_data_op.h"

namespace oceanbase
{
namespace sql
{

class ObRecursiveUnionSpec: public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObRecursiveUnionSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObRecursiveUnionSpec();
  friend class ObRecursiveUnionOp;
  void set_search_pseudo_column(ObExpr *expr) { search_expr_ = expr; }
  void set_cycle_pseudo_column(ObExpr *expr) { cycle_expr_ = expr; }
  void set_identify_seq_offset(int64_t offset) { identify_seq_offset_ = offset; }
  inline void set_search_strategy(ObRecursiveInnerDataOracleOp::SearchStrategyType strategy)
  {
    strategy_ = strategy;
  }
  inline void set_fake_cte_table(uint64_t cte_table_id) { pump_operator_id_ = cte_table_id; };
  int set_cycle_pseudo_values(ObExpr *v, ObExpr *d_v);
  inline void set_is_rcte_distinct(bool is_rcte_distinct) { is_rcte_distinct_ = is_rcte_distinct; }
  inline ObHashFuncs& get_hash_funcs() { return hash_funcs_; }
  inline ObSortFuncs& get_sort_cmp_funcs() { return sort_cmp_funcs_; }
  inline ObSortCollations &get_deduplicate_sort_collations()
  {
    return deduplicate_sort_collations_;
  }
  static const int64_t UNUSED_POS;

protected:
  /**
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  //virtual int64_t to_string_kv(char *buf, const int64_t buf_len) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRecursiveUnionSpec);
public:
  // 排序的依据列
  common::ObFixedArray<ObSortFieldCollation, common::ObIAllocator> sort_collations_;
  // 检测递归中循环的执行列，列的内容重复则终止执行
  common::ObFixedArray<uint64_t, common::ObIAllocator> cycle_by_col_lists_;
  //recursive uion all 算子的的输出中的T_OP_UNION表达式，inner data获取输出行以后拷贝到这些表达式的datum中
  //inner data的输出行总不含search，cycle伪列，所以这里也不包含伪列表达式。
  common::ObFixedArray<ObExpr *, common::ObIAllocator> output_union_exprs_;
protected:
  static const int32_t CMP_DIRECTION_ASC = 1;
  static const int32_t CMP_DIRECTION_DESC = -1;
  uint64_t pump_operator_id_;
  ObExpr *search_expr_;
  ObExpr *cycle_expr_;
  ObRecursiveInnerDataOracleOp::SearchStrategyType strategy_;
  ObExpr *cycle_value_;
  ObExpr *cycle_default_value_;
  int64_t identify_seq_offset_;

  bool is_rcte_distinct_;
  //below variables only used in recursive union distinct
  ObHashFuncs hash_funcs_;
  ObSortFuncs sort_cmp_funcs_;

  // different from sort_collations_, the former is used in oracle mode to satisfy sort requirement
  // but here is used for deduplicate in Recursive Union Distinct
  ObSortCollations deduplicate_sort_collations_;
};

class ObRecursiveUnionOp : public ObOperator
{
public:
  explicit ObRecursiveUnionOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObOperator(exec_ctx, spec, input),
    inner_data_(nullptr)
  {}
  ~ObRecursiveUnionOp()
  {}
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_rescan() override;
  virtual void destroy()
  {
    if (OB_NOT_NULL(inner_data_)) {
      inner_data_->~ObRecursiveInnerDataOp();
    }
    ObOperator::destroy();
  }
  const ObRecursiveUnionSpec &get_spec() const
  { return static_cast<const ObRecursiveUnionSpec &>(spec_); }
  int cast_result(const ObExpr *src_expr, const ObExpr *dst_expr, ObDatum *expr_datum);
public:
  ObRecursiveInnerDataOp* inner_data_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_SET_OPERATOR_H_ */
