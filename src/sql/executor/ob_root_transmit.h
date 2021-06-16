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

#ifndef OCEANBASE_SQL_EXECUTOR_ROOT_TRANSMIT_OPERATOR_
#define OCEANBASE_SQL_EXECUTOR_ROOT_TRANSMIT_OPERATOR_

#include "sql/executor/ob_transmit.h"
#include "sql/engine/ob_phy_operator_type.h"

namespace oceanbase {
namespace sql {
class ObRootTransmitInput : public ObTransmitInput {
  OB_UNIS_VERSION_V(1);

public:
  ObRootTransmitInput() : ObTransmitInput()
  {}
  virtual ~ObRootTransmitInput()
  {}
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_DIRECT_TRANSMIT;
  }
};

class ObRootTransmit : public ObTransmit {
private:
  class ObRootTransmitCtx : public ObTransmitCtx {
  public:
    explicit ObRootTransmitCtx(ObExecContext& ctx) : ObTransmitCtx(ctx)
    {}
    ~ObRootTransmitCtx()
    {}
    virtual void destroy()
    {
      ObTransmitCtx::destroy();
    }
  };

public:
  explicit ObRootTransmit(common::ObIAllocator& alloc);
  virtual ~ObRootTransmit();
  virtual int process_expect_error(ObExecContext& ctx, int errcode) const;

private:
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRootTransmit);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_ROOT_TRANSMIT_OPERATOR_ */
//// end of header file
