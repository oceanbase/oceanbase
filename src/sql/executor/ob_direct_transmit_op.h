/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DIRECT_TRANSMIT_OP_
#define OCEANBASE_SQL_EXECUTOR_OB_DIRECT_TRANSMIT_OP_

#include "sql/engine/px/exchange/ob_transmit_op.h"

namespace oceanbase
{
namespace sql
{

class ObDirectTransmitOpInput : public ObTransmitOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObDirectTransmitOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObTransmitOpInput(ctx, spec)
  {}
  virtual ~ObDirectTransmitOpInput() {};
  virtual int init(ObTaskInfo &task_info) override
  {
    UNUSED(task_info);
    return common::OB_SUCCESS;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectTransmitOpInput);
};

class ObDirectTransmitSpec : public ObTransmitSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObDirectTransmitSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObTransmitSpec(alloc, type)
  {}

  virtual ~ObDirectTransmitSpec() {};
};

class ObDirectTransmitOp : public ObTransmitOp
{
public:
  ObDirectTransmitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObTransmitOp(exec_ctx, spec, input) {}
  virtual ~ObDirectTransmitOp() {}
  virtual int inner_get_next_row();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectTransmitOp);
};

}
}
#endif /*  OCEANBASE_SQL_EXECUTOR_OB_DIRECT_TRANSMIT_OP_ */
//// end of header file
