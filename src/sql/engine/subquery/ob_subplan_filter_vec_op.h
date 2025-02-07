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

#ifndef OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_VEC_OP_H_
#define OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_VEC_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/ob_sql_define.h"
#include "sql/engine/basic/ob_group_join_buffer_v2.h"
#include "sql/engine/basic/ob_vector_result_holder.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"
namespace oceanbase
{
namespace sql
{

class ObSubQueryIterator;
class ObSubPlanFilterVecSpec : public ObSubPlanFilterSpec
{
  OB_UNIS_VERSION_V(1);
public:
  DECLARE_VIRTUAL_TO_STRING;
  ObSubPlanFilterVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObSubPlanFilterVecOp: public ObSubPlanFilterOp
{
public:
  ObSubPlanFilterVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObSubPlanFilterVecOp();

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int switch_iterator() override;

  virtual int inner_get_next_row() override  { return common::OB_NOT_IMPLEMENT; }
  virtual int inner_close() override;

  virtual void destroy() override;
  // const common::ObIArray<Iterator *> &get_subplan_iters() const { return subplan_iters_; }

  virtual int inner_get_next_batch(const int64_t max_row_cnt);

  virtual void get_current_group(uint64_t& current_group) const
  {
    current_group = drive_iter_.get_cur_group_id();
  }
  virtual void get_current_batch_cnt(int64_t& current_batch_cnt) const
  {
    current_batch_cnt = drive_iter_.get_group_rescan_cnt();
  }

  bool enable_left_das_batch() const {return MY_SPEC.enable_das_group_rescan_;}

  virtual const GroupParamArray *get_rescan_params_info() const { return MY_SPEC.enable_das_group_rescan_? drive_iter_.get_rescan_params_info() : nullptr; }

  const ObSubPlanFilterVecSpec &get_spec() const
  { return static_cast<const ObSubPlanFilterVecSpec &>(spec_); }

private:
  void set_param_null() { set_pushdown_param_null(MY_SPEC.rescan_params_); };
  void destroy_subplan_iters();
  int prepare_onetime_exprs();
  int prepare_onetime_exprs_inner();
  int init_subplan_iters();

private:
  // common::ObSEArray<Iterator *, 16> subplan_iters_;
  // lib::MemoryContext update_set_mem_;
  // bool iter_end_;
  // uint64_t max_group_size_; //Das batch rescan size;

  // // Combined in join buffer
  // // common::ObArrayWrap<ObSqlArrayObj> das_batch_params_;
  // common::ObSEArray<Iterator*, 8> subplan_iters_to_check_;
  ObDriverRowIterator drive_iter_;
public:
};

} // end anmespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_VEC_OP_H_