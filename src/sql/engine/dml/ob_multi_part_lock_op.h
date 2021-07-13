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

#ifndef OB_MULTI_PART_LOCK_OP_H
#define OB_MULTI_PART_LOCK_OP_H
#include "sql/engine/dml/ob_table_lock_op.h"
namespace oceanbase {
namespace sql {
class ObMultiPartLockSpec : public ObTableModifySpec, public ObMultiDMLInfo {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiPartLockSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableModifySpec(alloc, type), ObMultiDMLInfo(alloc)
  {}

  virtual ~ObMultiPartLockSpec()
  {}
  virtual bool is_multi_dml() const
  {
    return true;
  }
};

class ObMultiPartLockOp : public ObTableModifyOp, public ObMultiDMLCtx {
public:
  static const int64_t LOCK_OP = 0;
  static const int64_t DML_OP_CNT = 1;

public:
  ObMultiPartLockOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableModifyOp(ctx, spec, input), ObMultiDMLCtx(ctx.get_allocator())
  {}
  ~ObMultiPartLockOp()
  {}

private:
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row();
  int shuffle_lock_row();
  int process_mini_task();
  virtual void destroy() override
  {
    ObTableModifyOp::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }

private:
  bool got_row_ = false;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OB_MULTI_PART_LOCK_OP_H
