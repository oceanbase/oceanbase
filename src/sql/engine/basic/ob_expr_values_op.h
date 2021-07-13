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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_EXPR_VALUES_OP_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_EXPR_VALUES_OP_

#include "sql/engine/ob_operator.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase {
namespace sql {
class ObPhyOpSeriCtx;

class ObExprValuesOpInput : public ObOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObExprValuesOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObOpInput(ctx, spec), partition_id_values_(0)
  {}
  virtual ~ObExprValuesOpInput()
  {}
  virtual void reset() override
  {}
  virtual int init(ObTaskInfo& task_info)
  {
    UNUSED(task_info);
    return common::OB_SUCCESS;
  }

public:
  int64_t partition_id_values_;
};

class ObExprValuesSpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObExprValuesSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObOpSpec(alloc, type), values_(alloc), str_values_array_(alloc)
  {}

  int64_t get_value_count() const
  {
    return values_.count();
  }

  virtual int serialize(char* buf, int64_t buf_len, int64_t& pos, ObPhyOpSeriCtx& seri_ctx) const override;
  virtual int64_t get_serialize_size(const ObPhyOpSeriCtx& seri_ctx) const override;
  int64_t get_serialize_size_(const ObPhyOpSeriCtx& seri_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprValuesSpec);

public:
  common::ObFixedArray<ObExpr*, common::ObIAllocator> values_;
  common::ObFixedArray<ObStrValues, common::ObIAllocator> str_values_array_;
};

class ObExprValuesOp : public ObOperator {
public:
  ObExprValuesOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual int inner_open() override;
  virtual int rescan() override;

  virtual int switch_iterator() override;

  virtual int inner_get_next_row() override;

  virtual int inner_close() override;

  virtual void destroy() override
  {
    ObOperator::destroy();
  }

private:
  int calc_next_row();
  int get_value_count();

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprValuesOp);

private:
  int64_t node_idx_;
  int64_t vector_index_;
  ObDatumCaster datum_caster_;
  common::ObCastMode cm_;
  int64_t value_count_;
  bool switch_value_;
};

}  // end namespace sql
}  // end namespace oceanbase
#endif
