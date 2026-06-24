/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_ORDERED_COORD_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_ORDERED_COORD_OP_H_

#include "sql/engine/px/ob_px_coord_op.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/px/exchange/ob_px_ordered_receive_filter.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/px/datahub/components/ob_dh_rollup_key.h"
#include "sql/engine/px/datahub/components/ob_dh_sample.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"
#include "sql/engine/px/datahub/components/ob_dh_second_stage_reporting_wf.h"
#include "sql/engine/px/datahub/components/ob_dh_join_filter_count_row.h"

namespace oceanbase
{
namespace sql
{

class ObPxOrderedCoordOpInput : public ObPxReceiveOpInput
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObPxOrderedCoordOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxReceiveOpInput(ctx, spec)
  {}
  virtual ~ObPxOrderedCoordOpInput()
  {}
};

class ObPxOrderedCoordSpec : public ObPxCoordSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxOrderedCoordSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObPxCoordSpec(alloc, type)
  {}
  ~ObPxOrderedCoordSpec() {}
  virtual const common::ObIArray<ObExpr *> *get_all_exprs() const override { return &output_; }

};

class ObPxOrderedCoordOp : public ObPxCoordOp
{
public:
  ObPxOrderedCoordOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxOrderedCoordOp() {}
public:
  class ObPxOrderedCoordOpEventListener : public ObIPxCoordEventListener
  {
  public:
    ObPxOrderedCoordOpEventListener(ObPxOrderedCoordOp &op) :
      px_coord_op_(op) {}
    virtual int on_root_data_channel_setup();
  private:
    ObPxOrderedCoordOp &px_coord_op_;
  };
public:
  virtual int inner_open() override;
  virtual void destroy() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;

  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;

  virtual ObIPxCoordEventListener &get_listenner() override { return listener_; }

    // initialize readers after receive channel root DFO.
  virtual int receive_channel_root_dfo(ObExecContext &ctx,
                                       ObDfo &parent,
                                       ObPxTaskChSets &parent_ch_sets) override;
  // initialize readers after receive channel root DFO.
  virtual int receive_channel_root_dfo(ObExecContext &ctx,
                                       ObDfo &parent,
                                       dtl::ObDtlChTotalInfo &ch_info) override;
private:

  int setup_loop_proc() override;
  int setup_readers();
  void destroy_readers();
  int next_row(ObReceiveRowReader &reader, bool &wait_next_msg);
  int next_rows(ObReceiveRowReader &reader, int64_t max_row_cnt, int64_t &read_rows);
  virtual void clean_dfos_dtl_interm_result() override
  {
    msg_proc_.clean_dtl_interm_result(ctx_);
  }
private:
  ObPxOrderedCoordOpEventListener listener_;
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
  ObSPWinFuncPXPieceMsgP sp_winfunc_px_piece_msg_proc_;
  ObRDWinFuncPXPieceMsgP rd_winfunc_px_piece_msg_proc_;
  ObJoinFilterCountRowPieceMsgP join_filter_count_row_piece_msg_proc_;
  ObReceiveRowReader *readers_;
  ObOrderedReceiveFilter receive_order_;
  int64_t reader_cnt_;
  int64_t channel_idx_;
  int64_t finish_ch_cnt_;
  bool all_rows_finish_;
  // stored rows used for get batch rows from DTL reader.
  const ObChunkDatumStore::StoredRow **stored_rows_;
  const ObCompactRow **vector_rows_;

};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_ORDERED_COORD_OP_H_
