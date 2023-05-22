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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_MS_COORD_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_MS_COORD_OP_H_

#include "sql/engine/px/ob_px_coord_op.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/px/datahub/components/ob_dh_rollup_key.h"
#include "sql/engine/px/datahub/components/ob_dh_sample.h"
#include "lib/container/ob_iarray.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"
#include "sql/engine/px/datahub/components/ob_dh_second_stage_reporting_wf.h"

namespace oceanbase
{
namespace sql
{

class ObPxMSCoordOpInput : public ObPxReceiveOpInput
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObPxMSCoordOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxReceiveOpInput(ctx, spec)
  {}
  virtual ~ObPxMSCoordOpInput()
  {}
};

class ObPxMSCoordSpec : public ObPxCoordSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMSCoordSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObPxCoordSpec(alloc, type),
    all_exprs_(alloc),
    sort_collations_(alloc),
    sort_cmp_funs_(alloc),
    is_old_unblock_mode_(true)
  {}
  ~ObPxMSCoordSpec() {}
  virtual const common::ObIArray<ObExpr *> *get_all_exprs() const override { return &all_exprs_; }

  ExprFixedArray all_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funs_;
  bool is_old_unblock_mode_;
};

class ObPxMSCoordOp : public ObPxCoordOp
{
public:
  ObPxMSCoordOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxMSCoordOp() {}
public:
  class ObPxMSCoordOpEventListener : public ObIPxCoordEventListener
  {
  public:
    ObPxMSCoordOpEventListener(ObPxMSCoordOp &px_coord_op)
    : px_coord_op_(px_coord_op)
    {}
    int on_root_data_channel_setup();
  private:
      ObPxMSCoordOp &px_coord_op_;
  };
  class ObMsgReceiveFilter : public dtl::ObIDltChannelLoopPred
  {
  public:
    ObMsgReceiveFilter(ObRowHeap<ObMaxDatumRowCompare, ObChunkDatumStore::LastStoredRow> &heap)
        : data_ch_idx_start_(-1), data_ch_idx_end_(-1), heap_(heap) {}
    ~ObMsgReceiveFilter() = default;
    // idx range 范围左闭右开: [start_idx, end_idx)
    void set_data_channel_idx_range(int64_t start_idx, int64_t end_idx)
    {
      data_ch_idx_start_ = start_idx;
      data_ch_idx_end_ = end_idx;
    }
    bool pred_process(int64_t ch_idx, dtl::ObDtlChannel *ch) override
    {
      UNUSED(ch);
      // NOTE: 多个 DFO 的控制信息 channel 创建时间不同，某些可能晚于 ROOT DFO 被调度起来
      //       所以 heap 的范围可能是中间的一段
      return (-1 == data_ch_idx_start_) || /* 还没到接收 ROOT DFO 数据，只接受控制消息阶段 */
          (ch_idx < data_ch_idx_start_) || /* 控制消息 */
          (ch_idx >= data_ch_idx_end_)  || /* 控制消息 */
          (heap_.writable_channel_idx() + data_ch_idx_start_ == ch_idx); /* 预期数据消息 */
    }
    OB_INLINE int64_t get_data_channel_start_idx() { return data_ch_idx_start_; }
  private:
    int64_t data_ch_idx_start_;
    int64_t data_ch_idx_end_;
    ObRowHeap<ObMaxDatumRowCompare, ObChunkDatumStore::LastStoredRow> &heap_;
  };
public:
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;

  virtual ObIPxCoordEventListener &get_listenner() override { return listener_; }
  int init_row_heap(int64_t n_ways);

  // initialize readers after receive channel root DFO.
  virtual int receive_channel_root_dfo(ObExecContext &ctx,
                                       ObDfo &parent,
                                       ObPxTaskChSets &parent_ch_sets) override;
  // initialize readers after receive channel root DFO.
  virtual int receive_channel_root_dfo(ObExecContext &ctx,
                                       ObDfo &parent,
                                       dtl::ObDtlChTotalInfo &ch_info) override;
  void reset_finish_ch_cnt() { finish_ch_cnt_ = 0; }
  void reset_readers();
  void reuse_heap() {
    row_heap_.reuse_heap(task_channels_.count(), alloc_);
    last_pop_row_ = NULL;
  }
private:
  virtual int free_allocator();
  // fetch row from reader
  int next_row(ObReceiveRowReader &reader, bool &wait_next_msg);
  virtual int setup_loop_proc() override;
  int init_store_rows(int64_t n_ways);
  int setup_readers();
  void destroy_readers();
  virtual void clean_dfos_dtl_interm_result() override
  {
    msg_proc_.clean_dtl_interm_result(ctx_);
  }
private:
  ObPxMSCoordOpEventListener listener_;
  ObSerialDfoScheduler serial_scheduler_;
  ObParallelDfoScheduler parallel_scheduler_;
  ObPxMsgProc msg_proc_; // msg_loop 处理消息的回调函数
  ObPxFinishSqcResultP sqc_finish_msg_proc_;
  ObPxInitSqcResultP sqc_init_msg_proc_;
  ObBarrierPieceMsgP barrier_piece_msg_proc_;
  ObWinbufPieceMsgP winbuf_piece_msg_proc_;
  ObPxQcInterruptedP interrupt_proc_;
  ObDynamicSamplePieceMsgP sample_piece_msg_proc_;
  ObRollupKeyPieceMsgP rollup_key_piece_msg_proc_;
  ObRDWFPieceMsgP rd_wf_piece_msg_proc_;
  ObInitChannelPieceMsgP init_channel_piece_msg_proc_;
  ObReportingWFPieceMsgP reporting_wf_piece_msg_proc_;
  ObOptStatsGatherPieceMsgP opt_stats_gather_piece_msg_proc_;
  // 存储merge sort的每一路的当前行
  ObArray<ObChunkDatumStore::LastStoredRow*> store_rows_;
  ObChunkDatumStore::LastStoredRow* last_pop_row_;
  // row_heap 和 receive_order 仅仅用于有序收取数据的场景
  // 这里采用最大内存方式避免每一行都申请内存
  ObRowHeap<ObMaxDatumRowCompare, ObChunkDatumStore::LastStoredRow> row_heap_;
  ObMsgReceiveFilter receive_order_;

  int64_t finish_ch_cnt_;
  bool all_rows_finish_;
  ObReceiveRowReader *readers_;
  int64_t reader_cnt_;
  common::ObArenaAllocator alloc_;
};


} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_FIFO_COORD_OP_H_
