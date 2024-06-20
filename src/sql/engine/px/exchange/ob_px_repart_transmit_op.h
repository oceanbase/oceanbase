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

namespace oceanbase
{
namespace sql
{

class ObRepartSliceIdxCalc;
class ObDynamicSamplePieceMsg;

class ObPxRepartTransmitOpInput : public ObPxTransmitOpInput
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObPxRepartTransmitOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxTransmitOpInput(ctx, spec)
  {}
  virtual ~ObPxRepartTransmitOpInput()
  {}
};

class ObPxRepartTransmitSpec : public ObPxTransmitSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxRepartTransmitSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObPxRepartTransmitSpec() {}
  virtual int register_to_datahub(ObExecContext &exec_ctx) const override;

public:
  ObExpr *calc_tablet_id_expr_; // for pkey, pkey-hash, pkey-range
  ExprFixedArray dist_exprs_; // for pkey-hash, pkey-range
  common::ObHashFuncs dist_hash_funcs_; // for pkey-hash
  ObSortFuncs sort_cmp_funs_; // for pkey-range
  ObSortCollations sort_collations_; // for pkey-range
  ObFixedArray<uint64_t, common::ObIAllocator> ds_tablet_ids_; // tablet_ids for dynamic sample
  ExprFixedArray repartition_exprs_; // for naaj pkey
};

class ObPxRepartTransmitOp : public ObPxTransmitOp
{
public:
  ObPxRepartTransmitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxRepartTransmitOp() { destroy(); }

  virtual int inner_open() override;
  virtual void destroy() override;
  virtual int inner_rescan() override { return ObPxTransmitOp::inner_rescan(); }
  virtual int inner_close() override;
  int do_transmit();
private:
  template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
  int do_repart_transmit(ObRepartSliceIdxCalc &repart_slice_calc);
private:
  virtual int build_ds_piece_msg(int64_t expected_range_count,
    ObDynamicSamplePieceMsg &piece_msg) override;
  int dynamic_sample();
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_REPART_TRANSMIT_OP_H_
