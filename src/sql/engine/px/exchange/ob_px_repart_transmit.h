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

#ifndef _OB_SQ_OB_PX_REPART_TRANSMIT_H_
#define _OB_SQ_OB_PX_REPART_TRANSMIT_H_

#include "sql/engine/px/exchange/ob_px_transmit.h"

namespace oceanbase {
namespace sql {
class ObRepartSliceIdxCalc;
class ObPxRepartTransmitInput : public ObPxTransmitInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxRepartTransmitInput() : ObPxTransmitInput()
  {}
  virtual ~ObPxRepartTransmitInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_REPART_TRANSMIT;
  }
};

class ObPxRepartTransmit : public ObPxTransmit {
public:
  class ObPxRepartTransmitCtx : public ObPxTransmitCtx {
  public:
    friend class ObPxRepartTransmit;

  public:
    explicit ObPxRepartTransmitCtx(ObExecContext& ctx);
    virtual ~ObPxRepartTransmitCtx();
  };

public:
  explicit ObPxRepartTransmit(common::ObIAllocator& alloc);
  virtual ~ObPxRepartTransmit();
  virtual int add_compute(ObColumnExpression* expr) override;

protected:
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int do_transmit(ObExecContext& ctx) const override;
  virtual int inner_close(ObExecContext& exec_ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int create_operator_input(ObExecContext& ctx) const;

private:
  int do_repart_transmit(ObExecContext& exec_ctx, ObRepartSliceIdxCalc& repart_slice_calc) const;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQ_OB_PX_REPART_TRANSMIT_H_ */
//// end of header file
