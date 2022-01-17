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

#ifndef OCEANBASE_EXCHANGE_OB_PX_DIST_TRANSMIT_H_
#define OCEANBASE_EXCHANGE_OB_PX_DIST_TRANSMIT_H_
#include "sql/engine/px/exchange/ob_px_transmit.h"

namespace oceanbase {
namespace sql {

class ObPxDistTransmitInput : public ObPxTransmitInput {
public:
  OB_UNIS_VERSION_V(1);

  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_DIST_TRANSMIT;
  }
};

class ObPxDistTransmit : public ObPxTransmit {
public:
  ObPxDistTransmit(common::ObIAllocator& alloc) : ObPxTransmit(alloc)
  {}
  virtual ~ObPxDistTransmit() = default;

  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int do_transmit(ObExecContext& ctx) const override;
  virtual int inner_close(ObExecContext& ctx) const override;
  virtual int init_op_ctx(ObExecContext& ctx) const override;
  virtual int create_operator_input(ObExecContext& ctx) const override;

private:
  int do_hash_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const;
  int do_bc2host_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const;
  int do_random_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const;
  int do_broadcast_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const;
  int do_sm_broadcast_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const;
  int do_sm_pkey_hash_dist(ObExecContext& ctx, ObPxTransmitCtx& op_ctx) const;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_EXCHANGE_OB_PX_DIST_TRANSMIT_H_
