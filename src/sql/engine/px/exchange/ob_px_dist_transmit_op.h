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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_DIST_TRANSMIT_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_DIST_TRANSMIT_OP_H_

#include "ob_px_transmit_op.h"

namespace oceanbase {
namespace sql {

class ObPxDistTransmitOpInput : public ObPxTransmitOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObPxDistTransmitOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObPxTransmitOpInput(ctx, spec)
  {}
  virtual ~ObPxDistTransmitOpInput()
  {}
};

class ObPxDistTransmitSpec : public ObPxTransmitSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxDistTransmitSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObPxTransmitSpec(alloc, type), dist_exprs_(alloc), dist_hash_funcs_(alloc)
  {}
  ~ObPxDistTransmitSpec()
  {}
  ExprFixedArray dist_exprs_;
  common::ObHashFuncs dist_hash_funcs_;
};

class ObPxDistTransmitOp : public ObPxTransmitOp {
public:
  ObPxDistTransmitOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObPxTransmitOp(exec_ctx, spec, input)
  {}
  virtual ~ObPxDistTransmitOp()
  {}

public:
  virtual int inner_open() override;
  virtual int rescan() override
  {
    return ObPxTransmitOp::rescan();
  }
  virtual void destroy() override
  {
    return ObPxTransmitOp::destroy();
  }
  virtual int inner_close() override;

  virtual int do_transmit() override;

private:
  int do_hash_dist();
  int do_bc2host_dist();
  int do_random_dist();
  int do_broadcast_dist();
  int do_sm_broadcast_dist();
  int do_sm_pkey_hash_dist();
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_DIST_TRANSMIT_OP_H_
