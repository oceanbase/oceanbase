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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DIRECT_TRANSMIT_
#define OCEANBASE_SQL_EXECUTOR_OB_DIRECT_TRANSMIT_

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/executor/ob_transmit.h"

namespace oceanbase {
namespace sql {

class ObDirectTransmitInput : public ObTransmitInput {
  OB_UNIS_VERSION_V(1);

public:
  ObDirectTransmitInput();
  virtual ~ObDirectTransmitInput();
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_DIRECT_TRANSMIT;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectTransmitInput);
};

class ObDirectTransmit : public ObTransmit {
private:
  class ObDirectTransmitCtx : public ObTransmitCtx {
    friend class ObDirectTransmit;

  public:
    explicit ObDirectTransmitCtx(ObExecContext& ctx) : ObTransmitCtx(ctx)
    {}
    virtual ~ObDirectTransmitCtx()
    {}
    virtual void destroy()
    {
      ObTransmitCtx::destroy();
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(ObDirectTransmitCtx);
  };

public:
  explicit ObDirectTransmit(common::ObIAllocator& alloc);
  virtual ~ObDirectTransmit();

  virtual int create_operator_input(ObExecContext& ctx) const;

protected:
  int get_next_row(ObExecContext& ctx, const ObNewRow*& row) const override;

private:
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;

private:
  // disallow copy assign
  DISALLOW_COPY_AND_ASSIGN(ObDirectTransmit);
};

}  // namespace sql
}  // namespace oceanbase
#endif /*  OCEANBASE_SQL_EXECUTOR_OB_DIRECT_TRANSMIT_ */
//// end of header file
