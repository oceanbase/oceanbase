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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_FIFO_COORD_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_FIFO_COORD_OP_H_

#include "sql/engine/px/ob_px_coord_op.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/ob_px_coord.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"

namespace oceanbase {
namespace sql {

class ObPxFifoCoordOpInput : public ObPxReceiveOpInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxFifoCoordOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObPxReceiveOpInput(ctx, spec)
  {}
  virtual ~ObPxFifoCoordOpInput()
  {}
};

class ObPxFifoCoordSpec : public ObPxCoordSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxFifoCoordSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObPxCoordSpec(alloc, type)
  {}
  ~ObPxFifoCoordSpec()
  {}
};

class ObPxFifoCoordOp : public ObPxCoordOp {
public:
  ObPxFifoCoordOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObPxFifoCoordOp()
  {}

public:
  class ObPxFifoCoordOpEventListener : public ObIPxCoordEventListener {
  public:
    virtual int on_root_data_channel_setup()
    {
      return common::OB_SUCCESS;
    }
  };

public:
  virtual int inner_open() override;
  virtual void destroy() override
  {
    ObPxCoordOp::destroy();
    // no need to reset interrupt_proc_
    // no need to reset sqc_init_msg_proc_
    // no need to reset sqc_finish_msg_proc_
    // no need to reset msg_proc_
    // no need to reset listener_
  }
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;

  virtual ObIPxCoordEventListener& get_listenner() override
  {
    return listener_;
  }

private:
  int next_row(bool& wait_next_msg);
  int setup_loop_proc() override;

private:
  ObPxFifoCoordOpEventListener listener_;
  ObSerialDfoScheduler serial_scheduler_;
  ObParallelDfoScheduler parallel_scheduler_;
  ObPxMsgProc msg_proc_;  // msg_loop call back function for dealing with msg
  ObPxFinishSqcResultP sqc_finish_msg_proc_;
  ObPxInitSqcResultP sqc_init_msg_proc_;
  ObBarrierPieceMsgP barrier_piece_msg_proc_;
  ObWinbufPieceMsgP winbuf_piece_msg_proc_;
  ObPxQcInterruptedP interrupt_proc_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_FIFO_COORD_OP_H_
