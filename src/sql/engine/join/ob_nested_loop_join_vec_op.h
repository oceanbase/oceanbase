/** * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SQL_ENGINE_JOIN_OB_NEST_LOOP_JOIN_VEC_OP_H_
#define SRC_SQL_ENGINE_JOIN_OB_NEST_LOOP_JOIN_VEC_OP_H_
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_2d_array.h"
#include "lib/lock/ob_scond.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/join/ob_join_vec_op.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/basic/ob_group_join_buffer_v2.h"
#include "sql/engine/basic/ob_vector_result_holder.h"

namespace oceanbase
{
namespace sql
{

class ObNestedLoopJoinVecSpec : public ObJoinVecSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObNestedLoopJoinVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObJoinVecSpec(alloc, type),
      rescan_params_(alloc),
      gi_partition_id_expr_(nullptr),
      enable_gi_partition_pruning_(false),
      enable_px_batch_rescan_(false),
      group_rescan_(false),
      group_size_(OB_MAX_BULK_JOIN_ROWS),
      left_expr_ids_in_other_cond_(alloc),
      left_rescan_params_(alloc),
      right_rescan_params_(alloc)
  {}

  int init_param_count(int64_t count)
  { return rescan_params_.init(count); }

  int add_nlj_param(int64_t param_idx, ObExpr *org_expr, ObExpr *param_expr);
public:
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> rescan_params_;
  // 指示吐出的行中 partition id 列所在位置，通过 expr 读出 part id，用于右侧 pruning
  ObExpr *gi_partition_id_expr_;
  bool enable_gi_partition_pruning_;
  bool enable_px_batch_rescan_;
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
  DISALLOW_COPY_AND_ASSIGN(ObNestedLoopJoinVecSpec);
};

class ObNestedLoopJoinVecOp: public ObJoinVecOp
{
public:

  enum ObJoinBatchState {
    JS_GET_LEFT_ROW = 0,
    JS_RESCAN_RIGHT_OP,
    JS_PROCESS_RIGHT_BATCH,
    JS_OUTPUT,
    JS_CARTESIAN_OPTIMIZED_PROCESS
  };

  ObNestedLoopJoinVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt);
  virtual int inner_get_next_row() override { return common::OB_NOT_IMPLEMENT; }
  virtual int inner_close() final;
  
  virtual OperatorOpenOrder get_operator_open_order() const override final
  { return OPEN_SELF_FIRST; }

  int prepare_rescan_params(bool is_group = false);
  virtual void destroy() override { ObJoinVecOp::destroy(); }

  // ObBatchRescanCtl &get_batch_rescan_ctl() { return batch_rescan_ctl_; }
  int fill_cur_row_rescan_param();
  int do_drain_exch_multi_lvel_bnlj();

  const ObNestedLoopJoinVecSpec &get_spec() const
  { return static_cast<const ObNestedLoopJoinVecSpec &>(spec_); }

  void set_param_null()
  {
    set_pushdown_param_null(get_spec().rescan_params_);
  }

private:
  bool is_full() const;
  // used for rescan and switch iter
  virtual void reset_buf_state();
  int rescan_params_batch_one(int64_t batch_idx);
  int get_left_batch();
  int group_get_left_batch(const ObBatchRows *&left_brs);

  int get_next_left_row();
  int rescan_right_operator();
  int rescan_right_op();
  int perform_gi_partition_prunig();
  int process_right_batch();
  int cartesian_optimized_process();
  int output();
  void reset_left_batch_state();
  void reset_right_batch_state();
  void skip_l_idx();

  bool continue_fetching() { return !(left_brs_->end_ || is_full());}
  virtual int do_drain_exch() override;
  virtual int inner_drain_exch() { return OB_SUCCESS; }

  int get_next_batch_from_right(const ObBatchRows *right_brs);

public:
  ObJoinBatchState batch_state_;
  bool is_left_end_;
  const ObBatchRows *left_brs_;
  bool iter_end_;
  int64_t op_max_batch_size_;
  ObDriverRowIterator drive_iter_;
  bool match_right_batch_end_;
  bool no_match_row_found_;
  bool need_output_row_;
  bool defered_right_rescan_;
  bool is_cartesian_;
  bool cartesian_opt_;
  int64_t  right_total_row_cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObNestedLoopJoinVecOp);
};
} // end namespace sql
} // end namespace oceanbase

#endif /*SRC_SQL_ENGINE_JOIN_OB_NEST_LOOP_JOIN_VEC_OP_H_*/