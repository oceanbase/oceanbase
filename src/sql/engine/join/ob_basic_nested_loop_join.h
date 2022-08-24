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

#ifndef OCEANBASE_SQL_ENGINE_JOIN_OB_BASIC_NESTED_LOOP_JOIN_
#define OCEANBASE_SQL_ENGINE_JOIN_OB_BASIC_NESTED_LOOP_JOIN_

#include "ob_join.h"

namespace oceanbase {
namespace sql {
class ObBasicNestedLoopJoin : public ObJoin {
  OB_UNIS_VERSION_V(1);

public:
  static const int64_t DEFAULT_MEM_LIMIT = 10 * 1024 * 1024;
  static const int64_t DEFAULT_CACHE_LIMIT = 1000;

protected:
  class ObBasicNestedLoopJoinCtx : public ObJoinCtx {
    friend class ObBasicNestedLoopJoin;

  public:
    ObBasicNestedLoopJoinCtx(ObExecContext &ctx) : ObJoinCtx(ctx)
    {}
    virtual ~ObBasicNestedLoopJoinCtx()
    {}
    void reset();
    virtual void destroy()
    {
      ObJoinCtx::destroy();
    }

  protected:
    common::ObExprCtx expr_ctx_;
  };
  struct RescanParam {
    OB_UNIS_VERSION_V(1);

  public:
    RescanParam() : my_phy_plan_(NULL), expr_(NULL), param_idx_(-1)
    {}
    RescanParam(ObPhysicalPlan* my_plan, ObSqlExpression* expr, int64_t idx)
        : my_phy_plan_(my_plan), expr_(expr), param_idx_(idx)
    {}
    ~RescanParam()
    {}

  public:
    ObPhysicalPlan* my_phy_plan_;
    ObSqlExpression* expr_;  // freed by the physical plan
    int64_t param_idx_;
    TO_STRING_KV(N_EXPR, expr_, N_INDEX, param_idx_);
  };

public:
  explicit ObBasicNestedLoopJoin(common::ObIAllocator& alloc);
  virtual ~ObBasicNestedLoopJoin();
  virtual void reset() override;
  virtual void reuse() override;
  int init_param_count(int64_t count)
  {
    return init_array_size<>(rescan_params_, count);
  }
  int add_nlj_param(ObSqlExpression* expr, int64_t param_idx);
  void set_inner_get(bool inner_get)
  {
    is_inner_get_ = inner_get;
  }
  void set_self_join(bool self_join)
  {
    is_self_join_ = self_join;
  }

  TO_STRING_KV(N_ID, id_, N_COLUMN_COUNT, column_count_, N_PROJECTOR,
      common::ObArrayWrap<int32_t>(projector_, projector_size_), N_FILTER_EXPRS, filter_exprs_, N_CALC_EXPRS,
      calc_exprs_, N_JOIN_TYPE, ob_join_type_str(join_type_), N_JOIN_EQ_COND, rescan_params_, N_JOIN_OTHER_COND,
      other_join_conds_, N_INNER_GET, is_inner_get_, N_SELF_JOIN, is_self_join_);

protected:
  int get_next_left_row(ObJoinCtx& join_ctx) const override;
  int prepare_rescan_params(ObBasicNestedLoopJoinCtx& join_ctx) const;
  inline bool use_batch_index_join(ObPhyOperatorType right_op_type) const
  {
    return is_self_join_ && !IS_OUTER_JOIN(join_type_) && PHY_TABLE_SCAN == right_op_type;
  }
  virtual OperatorOpenOrder get_operator_open_order(ObExecContext& ctx) const override
  {
    UNUSED(ctx);
    return OPEN_SELF_FIRST;
  }
  virtual int inner_open(ObExecContext &ctx) const override;
  virtual int inner_close(ObExecContext &exec_ctx) const override;
  virtual int rescan(ObExecContext &exec_ctx) const override;

protected:
  common::ObFixedArray<RescanParam, common::ObIAllocator> rescan_params_;  // @todo a better name
  common::ObFixedArray<int64_t, common::ObIAllocator> left_scan_index_;
  bool is_inner_get_;
  bool is_self_join_;
};

inline int ObBasicNestedLoopJoin::add_nlj_param(ObSqlExpression* expr, int64_t param_idx)
{
  return rescan_params_.push_back(RescanParam(my_phy_plan_, expr, param_idx));
}
} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_ENGINE_JOIN_OB_BASIC_NESTED_LOOP_JOIN_ */
