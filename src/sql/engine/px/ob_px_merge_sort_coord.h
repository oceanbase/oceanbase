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

#ifndef _OB_SQL_PX_MERGE_SORT_COORD_H_
#define _OB_SQL_PX_MERGE_SORT_COORD_H_

#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/executor/ob_receive.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_coord.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"

namespace oceanbase {
namespace sql {
class ObPxReceive;

class ObPxMergeSortCoord;
class ObPxMergeSortCoord;

class ObPxMergeSortCoordInput : public ObPxReceiveInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxMergeSortCoordInput() : ObPxReceiveInput()
  {}
  virtual ~ObPxMergeSortCoordInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_MERGE_SORT_COORD;
  }
};

class ObMsgReceiveSortOrder : public dtl::ObIDltChannelLoopPred {
public:
  ObMsgReceiveSortOrder(ObRowHeap<>& heap) : data_ch_idx_start_(-1), data_ch_idx_end_(-1), heap_(heap)
  {}
  ~ObMsgReceiveSortOrder() = default;
  // idx range: [start_idx, end_idx)
  void set_data_channel_idx_range(int64_t start_idx, int64_t end_idx)
  {
    data_ch_idx_start_ = start_idx;
    data_ch_idx_end_ = end_idx;
  }
  bool pred_process(int64_t ch_idx, dtl::ObDtlChannel* ch) override
  {
    UNUSED(ch);
    return (-1 == data_ch_idx_start_) ||    /* no ROOT DFO data received, only accept ctrl msg */
           (ch_idx < data_ch_idx_start_) || /* ctrl msg */
           (ch_idx >= data_ch_idx_end_) ||  /* ctrl msg */
           (heap_.writable_channel_idx() + data_ch_idx_start_ == ch_idx);
  }
  OB_INLINE int64_t get_data_channel_start_idx()
  {
    return data_ch_idx_start_;
  }

private:
  int64_t data_ch_idx_start_;
  int64_t data_ch_idx_end_;
  ObRowHeap<>& heap_;
};

class ObPxMergeSortCoord : public ObPxCoord, public ObSortableTrait {
public:
  OB_UNIS_VERSION_V(2);

public:
  class ObPxMergeSortCoordCtx;

public:
  class ObPxMergeSortCoordEventListener : public ObIPxCoordEventListener {
  public:
    ObPxMergeSortCoordEventListener(ObPxMergeSortCoordCtx& px_ctx) : px_ctx_(px_ctx)
    {}
    int on_root_data_channel_setup();

  private:
    ObPxMergeSortCoordCtx& px_ctx_;
  };
  class ObPxMergeSortCoordCtx : public ObPxCoordCtx {
  public:
    explicit ObPxMergeSortCoordCtx(ObExecContext& ctx)
        : ObPxCoordCtx(ctx),
          listener_(*this),
          serial_scheduler_(coord_info_, *this, listener_),
          msg_proc_(coord_info_, listener_, *this),
          parallel_scheduler_(coord_info_, *this, listener_, msg_proc_),
          sqc_finish_msg_proc_(ctx, msg_proc_),
          sqc_init_msg_proc_(ctx, msg_proc_),
          barrier_piece_msg_proc_(ctx, msg_proc_),
          winbuf_piece_msg_proc_(ctx, msg_proc_),
          interrupt_proc_(ctx, msg_proc_),
          row_heap_(),
          receive_order_(row_heap_),
          fifo_alloc_(),
          last_row_(NULL)
    {}
    virtual ~ObPxMergeSortCoordCtx()
    {
      fifo_alloc_.reset();
      // no need to reset receive_order_
      row_heap_.reset();
      // no need to reset interrupt_proc_
      // no need to reset sqc_init_msg_proc_
      // no need to reset sqc_finish_msg_proc_
      // no need to reset msg_proc_
      // no need to reset listener_
      ObPxCoordCtx::destroy();
    }
    virtual void destroy()
    {
      fifo_alloc_.reset();
      // no need to reset receive_order_
      row_heap_.reset();
      // no need to reset interrupt_proc_
      // no need to reset sqc_init_msg_proc_
      // no need to reset sqc_finish_msg_proc_
      // no need to reset msg_proc_
      // no need to reset listener_
      ObPxCoordCtx::destroy();
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
    ObPxMergeSortCoordEventListener listener_;
    ObSerialDfoScheduler serial_scheduler_;
    ObPxMsgProc msg_proc_;
    ObParallelDfoScheduler parallel_scheduler_;
    ObPxFinishSqcResultP sqc_finish_msg_proc_;
    ObPxInitSqcResultP sqc_init_msg_proc_;
    ObBarrierPieceMsgP barrier_piece_msg_proc_;
    ObWinbufPieceMsgP winbuf_piece_msg_proc_;
    ObPxQcInterruptedP interrupt_proc_;
    // row_heap & receive_order is for receiving ordered data
    ObRowHeap<> row_heap_;
    ObMsgReceiveSortOrder receive_order_;
    common::ObFIFOAllocator fifo_alloc_;
    const common::ObNewRow* last_row_;
    friend class ObPxMergeSortCoord;
  };

public:
  explicit ObPxMergeSortCoord(common::ObIAllocator& alloc) : ObPxCoord(alloc), ObSortableTrait(alloc)
  {}
  virtual ~ObPxMergeSortCoord() = default;
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
  virtual int free_allocator(ObExecContext& ctx) const;

  int next_row(
      ObExecContext& ctx, ObPxMergeSortCoordCtx& px_ctx, const common::ObNewRow*& row, bool& wait_next_msg) const;

  virtual int setup_loop_proc(ObExecContext& ctx, ObPxCoordCtx& px_ctx) const override;

  int init_op_ctx(ObExecContext& ctx) const;

  int setup_scheduler(ObPxMergeSortCoordCtx& px_ctx) const;
  /* variables */
  // DISALLOW_COPY_AND_ASSIGN(ObPxMergeSortCoord);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_PX_MERGE_SORT_COORD_H_ */
//// end of header file
