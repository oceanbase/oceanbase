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

#ifndef OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_OP_H_
#define OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase {
namespace sql {

// iterator subquery rows
class ObSubQueryIterator {
public:
  explicit ObSubQueryIterator(ObOperator& op);
  ~ObSubQueryIterator()
  {}
  void set_onetime_plan()
  {
    onetime_plan_ = true;
  }
  void set_init_plan()
  {
    init_plan_ = true;
  }

  const ExprFixedArray& get_output() const
  {
    return op_.get_spec().output_;
  }
  ObOperator& get_op() const
  {
    return op_;
  }

  // Call start() before get_next_row().
  // We need this because the subquery() may be common subexpression, need be rewinded to the
  // first row when iterate again.
  int start();
  int get_next_row();

  int prepare_init_plan();
  void reuse();
  void reset(bool reset_onetime_plan = false);

  TO_STRING_KV(K(onetime_plan_), K(init_plan_), K(inited_));

private:
  ObOperator& op_;
  bool onetime_plan_;
  bool init_plan_;
  bool inited_;
  bool iterated_;

  ObChunkDatumStore store_;
  ObChunkDatumStore::Iterator store_it_;
};

class ObSubPlanFilterSpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObSubPlanFilterSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);

  DECLARE_VIRTUAL_TO_STRING;

  // row from driver table is the rescan params
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> rescan_params_;
  // only compute once exprs
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> onetime_exprs_;
  // InitPlan idxs,InitPlan only compute once, need save result
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator> init_plan_idxs_;
  // One-Time idxs,One-Time only compute once, no need save result
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator> one_time_idxs_;

  // update set (, ,) = (subquery)
  ExprFixedArray update_set_;
};

class ObSubPlanFilterOp : public ObOperator {
public:
  typedef ObSubQueryIterator Iterator;

  ObSubPlanFilterOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObSubPlanFilterOp();

  virtual int open() override;
  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int switch_iterator() override;

  virtual int inner_get_next_row() override;
  virtual int inner_close() override;

  virtual void destroy() override;

  const common::ObIArray<Iterator*>& get_subplan_iters() const
  {
    return subplan_iters_;
  }

private:
  int set_param_null();
  void destroy_subplan_iters();
  void destroy_update_set_mem()
  {
    if (NULL != update_set_mem_) {
      DESTROY_CONTEXT(update_set_mem_);
      update_set_mem_ = NULL;
    }
  }

  int prepare_rescan_params();
  int prepare_onetime_exprs();
  int handle_update_set();

private:
  common::ObSEArray<Iterator*, 16> subplan_iters_;
  lib::MemoryContext* update_set_mem_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SUBQUERY_OB_SUBPLAN_FILTER_OP_H_
