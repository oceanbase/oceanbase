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

#ifndef OCEANBASE_SQL_ENGINE_JOIN_OB_BASIC_NESTED_LOOP_JOIN_OP_
#define OCEANBASE_SQL_ENGINE_JOIN_OB_BASIC_NESTED_LOOP_JOIN_OP_
#include "ob_join_op.h"
namespace oceanbase {
namespace sql {
class ObBasicNestedLoopJoinSpec : public ObJoinSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObBasicNestedLoopJoinSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObJoinSpec(alloc, type),
        rescan_params_(alloc),
        gi_partition_id_expr_(nullptr),
        enable_gi_partition_pruning_(false)
  {}
  virtual ~ObBasicNestedLoopJoinSpec(){};

  int init_param_count(int64_t count)
  {
    return rescan_params_.init(count);
  }

  int add_nlj_param(int64_t param_idx, ObExpr* org_expr, ObExpr* param_expr);

public:
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> rescan_params_;
  // Indicates the location of the partition id column in the output row,
  // read out partition id through expr for pruning on the right
  ObExpr* gi_partition_id_expr_;
  bool enable_gi_partition_pruning_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBasicNestedLoopJoinSpec);
};

class ObBasicNestedLoopJoinOp : public ObJoinOp {
public:
  static const int64_t DEFAULT_MEM_LIMIT = 10 * 1024 * 1024;
  static const int64_t DEFAULT_CACHE_LIMIT = 1000;
  ObBasicNestedLoopJoinOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObBasicNestedLoopJoinOp(){};

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_close() final;

  virtual OperatorOpenOrder get_operator_open_order() const override final
  {
    return OPEN_SELF_FIRST;
  }

  int prepare_rescan_params(bool is_group = false);
  virtual void destroy() override
  {
    ObJoinOp::destroy();
  }

  const ObBasicNestedLoopJoinSpec& get_spec() const
  {
    return static_cast<const ObBasicNestedLoopJoinSpec&>(spec_);
  }

  virtual int get_next_left_row() override;

  int save_left_row();
  int recover_left_row();

private:
  DISALLOW_COPY_AND_ASSIGN(ObBasicNestedLoopJoinOp);
};

inline int ObBasicNestedLoopJoinSpec::add_nlj_param(int64_t param_idx, ObExpr* org_expr, ObExpr* param_expr)
{
  return rescan_params_.push_back(ObDynamicParamSetter(param_idx, org_expr, param_expr));
}

}  // end namespace sql
}  // end namespace oceanbase
#endif
