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

#ifndef OCEANBASE_BASIC_OB_SET_OB_MERGE_UNION_VEC_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_MERGE_UNION_VEC_OP_H_

#include "sql/engine/set/ob_merge_set_vec_op.h"

namespace oceanbase
{
namespace sql
{

class ObMergeUnionVecSpec : public ObMergeSetVecSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObMergeUnionVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObMergeUnionVecOp : public ObMergeSetVecOp
{
public:
  ObMergeUnionVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt);
  typedef ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> FixedLengthVectorBigInt;
  typedef ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> DiscreteVectorString;

private:
  int get_first_row_vectorize(const int64_t batch_size);
  int distinct_get_next_batch(const int64_t batch_size);
  int all_get_next_batch(const int64_t batch_size);
  void switch_curr_and_candidate();
  OB_INLINE void add_idx_into_selector(int src_idx, int dst_idx, bool is_left);
  int convert_batch_rows();
  void dispatch_convert_batch_rows(ObExpr *src_expr, ObExpr *dst_expr,
                                  uint16_t *src_selector, uint16_t *dst_selector,
                                  uint16_t selector_cnt);
  template<typename SrcVec, typename DstVec>
  void convert_batch_column(SrcVec *src_vec, DstVec *dst_vec,
                          uint16_t *src_selector, uint16_t *dst_selector,
                          uint16_t selector_cnt, bool hash_null);
private:
  struct UnionOpInfo {
    UnionOpInfo () : op_idx_(0), child_op_brs_(nullptr), op_added_(false) {}
    void reset() 
    { 
      op_idx_ = 0;
      child_op_brs_ = nullptr;
      op_added_ = false;
      result_op_brs_.size_ = 0;
    }
    int64_t op_idx_;
    const ObBatchRows *child_op_brs_;
    ObBatchRows result_op_brs_; // 表示batch内去重后的结果
    bool op_added_;
  };
  typedef int (ObMergeUnionVecOp::*GetNextRowFunc)();
  typedef int (ObMergeUnionVecOp::*GetNextBatchFunc)(const int64_t batch_size);
  int do_strict_distinct_vectorize(ObOperator &child_op,
                                  const ObCompactRow *compare_row,
                                  const RowMeta *meta,
                                  const int64_t batch_size,
                                  const int64_t compare_idx,
                                  UnionOpInfo &op_info,
                                  int &cmp,
                                  bool &found_valid_row);
  ObOperator *cur_child_op_;
  ObOperator *candidate_child_op_;
  const ObIArray<ObExpr*> *candidate_output_row_;
  int64_t next_child_op_idx_;
  bool first_got_row_;
  GetNextRowFunc get_next_row_func_;
  GetNextBatchFunc get_next_batch_func_;
  UnionOpInfo left_info_;
  UnionOpInfo right_info_;
  UnionOpInfo *curr_info_;
  UnionOpInfo *candidate_info_;
  //idx indicates a row need to store as last_row, if we store a row, reset last_valid_idx_ to -1
  //if we record a idx, reset last_row.store_row_ to nullptr
  int64_t last_valid_idx_;
  //we will not push candidate op to next row until curr equal to candidate
  //in batch mode, if candidate move to last in a batch, we may have to return batch firstly,
  //then at the start of next batch, 
  //we decide if need get a new batch from candidate by this parameter
  bool need_push_candidate_;
  uint16_t *left_src_idx_selectors_ = nullptr;
  uint16_t *left_dst_idx_selectors_ = nullptr;
  uint16_t *right_src_idx_selectors_ = nullptr;
  uint16_t *right_dst_idx_selectors_ = nullptr;
  uint16_t left_selectors_item_cnt_;
  uint16_t right_selectors_item_cnt_;
  uint16_t last_add_row_idx_; // last_valid_idx is ok?
  // default last_row_belong_to_left_op_ = true
  // otherwise, the first right batch distinct will use the first row of itself
  // as lastoutputrow, if this value is init as false
  bool last_row_belong_to_left_op_ = true;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_SET_OB_MERGE_UNION_VEC_OP_H_ */