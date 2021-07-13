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

#ifndef _OB_SQL_PX_FIFO_COORD_H_
#define _OB_SQL_PX_FIFO_COORD_H_

#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/executor/ob_receive.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_px_coord.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"

namespace oceanbase {
namespace sql {
class ObPxFifoCoord;
class ObPxMergeSortCoord;

class ObPxFifoCoordInput : public ObPxReceiveInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxFifoCoordInput() : ObPxReceiveInput()
  {}
  virtual ~ObPxFifoCoordInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_FIFO_COORD;
  }
};

class ObPxFifoCoord : public ObPxCoord {
public:
  class ObPxFifoCoordCtx;

public:
  class ObPxFifoCoordEventListener : public ObIPxCoordEventListener {
  public:
    virtual int on_root_data_channel_setup()
    {
      return common::OB_SUCCESS;
    }
  };
  class ObPxFifoCoordCtx : public ObPxCoordCtx {
  public:
    explicit ObPxFifoCoordCtx(ObExecContext& ctx)
        : ObPxCoordCtx(ctx),
          serial_scheduler_(coord_info_, *this, listener_),
          msg_proc_(coord_info_, listener_, *this),
          parallel_scheduler_(coord_info_, *this, listener_, msg_proc_),
          sqc_finish_msg_proc_(ctx, msg_proc_),
          sqc_init_msg_proc_(ctx, msg_proc_),
          barrier_piece_msg_proc_(ctx, msg_proc_),
          winbuf_piece_msg_proc_(ctx, msg_proc_),
          interrupt_proc_(ctx, msg_proc_)
    {}
    virtual ~ObPxFifoCoordCtx()
    {
      ObPxCoordCtx::destroy();
    }
    virtual void destory()
    {
      ObPxCoordCtx::destroy();
      // no need to reset interrupt_proc_
      // no need to reset sqc_init_msg_proc_
      // no need to reset sqc_finish_msg_proc_
      // no need to reset msg_proc_
      // no need to reset listener_
    }
    virtual ObIPxCoordEventListener& get_listenner() override
    {
      return listener_;
    }
    ObSerialDfoScheduler& get_serial_scheduler()
    {
      return serial_scheduler_;
    }
    ObParallelDfoScheduler& get_parallel_scheduler()
    {
      return parallel_scheduler_;
    }
    ObPxMsgProc& get_msg_proc()
    {
      return msg_proc_;
    }

  protected:
    ObPxFifoCoordEventListener listener_;
    ObSerialDfoScheduler serial_scheduler_;
    ObPxMsgProc msg_proc_;  // dtl message callback for msg_loop
    ObParallelDfoScheduler parallel_scheduler_;
    ObPxFinishSqcResultP sqc_finish_msg_proc_;
    ObPxInitSqcResultP sqc_init_msg_proc_;
    ObBarrierPieceMsgP barrier_piece_msg_proc_;
    ObWinbufPieceMsgP winbuf_piece_msg_proc_;
    ObPxQcInterruptedP interrupt_proc_;
    friend class ObPxFifoCoord;
  };

public:
  explicit ObPxFifoCoord(common::ObIAllocator& alloc);
  virtual ~ObPxFifoCoord();
  // int open();
  // for debug purpose, should remove later
  // inline void set_dfo_tree(ObDfo &root) { root_ = &root;}
private:
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObNewRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;

  int next_row(ObExecContext& ctx, ObPxFifoCoordCtx& px_ctx, const common::ObNewRow*& row, bool& wait_next_msg) const;

  virtual int setup_loop_proc(ObExecContext& ctx, ObPxCoordCtx& px_ctx) const override;

  int init_op_ctx(ObExecContext& ctx) const;
  int setup_scheduler(ObPxFifoCoordCtx& px_ctx) const;
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxFifoCoord);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_PX_FIFO_COORD_H_ */
//// end of header file
