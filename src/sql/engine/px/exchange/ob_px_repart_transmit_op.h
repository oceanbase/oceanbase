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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_REPART_TRANSMIT_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_REPART_TRANSMIT_OP_H_

#include "ob_px_transmit_op.h"

namespace oceanbase {
namespace sql {

class ObRepartSliceIdxCalc;

class ObPxRepartTransmitOpInput : public ObPxTransmitOpInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxRepartTransmitOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObPxTransmitOpInput(ctx, spec)
  {}
  virtual ~ObPxRepartTransmitOpInput()
  {}
};

class ObPxRepartTransmitSpec : public ObPxTransmitSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxRepartTransmitSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  ~ObPxRepartTransmitSpec()
  {}

public:
  // for pkey,pkey-hash,pkey-range etc..
  ObExpr* calc_part_id_expr_;
  // for pkey-hash
  ExprFixedArray dist_exprs_;
  common::ObHashFuncs dist_hash_funcs_;
};

class ObPxRepartTransmitOp : public ObPxTransmitOp {
public:
  ObPxRepartTransmitOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObPxRepartTransmitOp()
  {}

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

  int do_transmit();

private:
  int do_repart_transmit(ObRepartSliceIdxCalc& repart_slice_calc);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_REPART_TRANSMIT_OP_H_
