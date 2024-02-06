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

#ifndef OCEANBASE_SQL_ENGINE_JOIN_OB_NESTED_LOOP_JOIN_OP_
#define OCEANBASE_SQL_ENGINE_JOIN_OB_NESTED_LOOP_JOIN_OP_

#include "sql/engine/join/ob_basic_nested_loop_join_op.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_group_join_buffer.h"
#include "sql/engine/basic/ob_material_op.h"

namespace oceanbase
{
namespace sql
{
class ObNestedLoopJoinSpec : public ObBasicNestedLoopJoinSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObNestedLoopJoinSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObBasicNestedLoopJoinSpec(alloc, type),
      group_rescan_(false),
      group_size_(OB_MAX_BULK_JOIN_ROWS),
      left_expr_ids_in_other_cond_(alloc),
      left_rescan_params_(alloc),
      right_rescan_params_(alloc)
  {}

public:
  // for group join buffer
  bool group_rescan_;
  int64_t group_size_;
  ObFixedArray<ObFixedArray<int, common::ObIAllocator>, common::ObIAllocator>
      left_expr_ids_in_other_cond_;
  // for multi level batch rescan
  //           NLJ 1
  //           / \
  //      TSC 1   NLJ 2
  //              / \
  //         TSC 2   TSC 3
  // As shown above, for NLJ 2, its left_rescan_params_ stores params used by TSC 2 and
  // set by NLJ 1.
  // Similarly, for NLJ 2, its right_rescan_params_ stores params used by TSC 3 and set
  // by NLJ 1.
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> left_rescan_params_;
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> right_rescan_params_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObNestedLoopJoinSpec);
};

// Nest loop join has no expression result overwrite problem:
//
// LEFT:
// Only overwrite by get row from left_store_ (left_store_iter_.get_next_row())
// in batch nest loop join. When get the last row of the batch from store, left child's output
// is recovered, it's safe to get row from child again (read next batch).
//
// RIGHT:
// Overwrite in blank_right_row(), right child is iterated end when blanked.
//
class ObNestedLoopJoinOp : public ObBasicNestedLoopJoinOp
{
public:
  enum ObJoinBatchState {
    JS_FILL_LEFT = 0,
    JS_RESCAN_RIGHT_OP,
    JS_PROCESS_RIGHT_BATCH,
    JS_OUTPUT
  };
  enum ObJoinState {
    JS_JOIN_END = 0,
    JS_READ_LEFT,
    JS_READ_RIGHT,
    JS_STATE_COUNT
  };
  enum ObFuncType {
    FT_ITER_GOING = 0,
    FT_ITER_END,
    FT_TYPE_COUNT
  };

  ObNestedLoopJoinOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int switch_iterator() override;
  virtual int rescan() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override
  {
    left_store_iter_.reset();
    left_store_.reset();
    last_store_row_.reset();
    batch_rescan_ctl_.reset();
    if (nullptr != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
    if (is_vectorized()) {
      right_store_iter_.reset();
      right_store_.reset();
      if (nullptr != batch_mem_ctx_) {
        DESTROY_CONTEXT(batch_mem_ctx_);
        batch_mem_ctx_ = nullptr;
      }
    }
    if (MY_SPEC.group_rescan_) {
      group_join_buffer_.destroy();
    }
    ObBasicNestedLoopJoinOp::destroy();
  }
  ObBatchRescanCtl &get_batch_rescan_ctl() { return batch_rescan_ctl_; }
  int fill_cur_row_rescan_param();
  int calc_other_conds(bool &is_match);

private:
  // state operation and transfer function type.
  typedef int (ObNestedLoopJoinOp::*state_operation_func_type)();
  typedef int (ObNestedLoopJoinOp::*state_function_func_type)();
  int join_row_with_semi_join();
  // JS_JOIN_END state operation and transfer functions.
  int join_end_operate();
  int join_end_func_end();
  // JS_READ_LEFT state operation and transfer functions.
  int read_left_operate();
  int read_left_operate_batch();
  int read_left_operate_group_batch();
  int group_read_left_operate();
  int read_left_func_going();
  int read_left_func_end();
  // JS_READ_RIGHT state operation and transfer functions.
  int read_right_operate();
  int read_right_func_going();
  int read_right_func_end();
  int rescan_right_operator();
  // state operations and transfer functions array.
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
  bool is_full() const;
  // used for rescan and switch iter
  virtual void reset_buf_state();

  // for vectorized
  int rescan_params_batch_one(int64_t batch_idx);
  int get_left_batch();
  int group_get_left_batch(const ObBatchRows *&left_brs);
  int inner_get_next_batch(const int64_t max_row_cnt);
  // for vectorized end

  // for refactor vectorized
  int rescan_right_op();
  int process_right_batch();
  int output();
  void reset_left_batch_state();
  void reset_right_batch_state();
  int left_expr_extend(int32_t size);
  void skip_l_idx();
  // for refactor vectorized end

  bool continue_fetching() { return !(left_brs_->end_ || is_full());}
public:
  ObJoinState state_;
  // for bnl join
  lib::MemoryContext mem_context_;
  ObChunkDatumStore left_store_;
  ObChunkDatumStore::Iterator left_store_iter_;
  bool is_left_end_;
  ObChunkDatumStore::ShadowStoredRow last_store_row_;
  bool save_last_row_;
  bool defered_right_rescan_;

  ObBatchRescanCtl batch_rescan_ctl_;

  // for vectorized
  ObJoinBatchState batch_state_;
  ObBatchRowDatums left_batch_;
  ObBatchRowDatums last_save_batch_;
  bool save_last_batch_;
  lib::MemoryContext batch_mem_ctx_;
  ObChunkDatumStore::StoredRow **stored_rows_;
  ObChunkDatumStore right_store_;
  ObChunkDatumStore::Iterator right_store_iter_;
  const ObBatchRows *left_brs_;
  ObBitVector *left_matched_;
  bool need_switch_iter_;
  bool iter_end_;
  int64_t op_max_batch_size_;
  int64_t max_group_size_;
  ObGroupJoinBufffer group_join_buffer_;
  // for vectorized end

  // for refactor vectorized
  bool match_left_batch_end_;
  bool match_right_batch_end_;
  int64_t l_idx_;
  bool no_match_row_found_;
  bool need_output_row_;
  int32_t left_expr_extend_size_;
  // for refactor vectorized end
private:
  DISALLOW_COPY_AND_ASSIGN(ObNestedLoopJoinOp);
};

} // end namespace sql
} // end namespace oceanbase
#endif
