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

#ifndef OCEANBASE_ENGINE_PX_OB_PX_SCHUDULER_H_
#define OCEANBASE_ENGINE_PX_OB_PX_SCHUDULER_H_

#include "sql/engine/px/exchange/ob_receive_op.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_local_first_buffer_manager.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"

namespace oceanbase {
namespace sql {

class ObPxRootDfoAction {
public:
  virtual int receive_channel_root_dfo(ObExecContext& ctx, ObDfo& parent, ObPxTaskChSets& parent_ch_sets) = 0;
  virtual int receive_channel_root_dfo(ObExecContext& ctx, ObDfo& parent, dtl::ObDtlChTotalInfo& ch_info) = 0;
};

// following infos are varibles on which scheduling depends.
class ObPxCoordInfo {
public:
  ObPxCoordInfo(ObIAllocator& allocator, dtl::ObDtlChannelLoop& msg_loop, ObInterruptibleTaskID& interrupt_id)
      : dfo_mgr_(allocator),
        rpc_proxy_(),
        all_threads_finish_(false),
        first_error_code_(common::OB_SUCCESS),
        msg_loop_(msg_loop),
        interrupt_id_(interrupt_id)
  {}
  virtual ~ObPxCoordInfo()
  {}
  virtual void destroy()
  {
    dfo_mgr_.destroy();
    piece_msg_ctx_mgr_.reset();
  }
  void reset_for_rescan()
  {
    all_threads_finish_ = false;
    dfo_mgr_.destroy();
    piece_msg_ctx_mgr_.reset();
  }

public:
  ObDfoMgr dfo_mgr_;
  ObPieceMsgCtxMgr piece_msg_ctx_mgr_;
  obrpc::ObPxRpcProxy rpc_proxy_;
  // QC knows all tasks has been finished and released all resources
  bool all_threads_finish_;
  int first_error_code_;
  dtl::ObDtlChannelLoop& msg_loop_;
  ObInterruptibleTaskID& interrupt_id_;
};

class ObDfoSchedulerBasic;

class ObPxTerminateMsgProc : public ObIPxCoordMsgProc {
public:
  ObPxTerminateMsgProc(ObPxCoordInfo& coord_info, ObIPxCoordEventListener& listener)
      : coord_info_(coord_info), listener_(listener)
  {}
  // msg processor callback
  int on_sqc_init_msg(ObExecContext& ctx, const ObPxInitSqcResultMsg& pkt);
  int on_sqc_finish_msg(ObExecContext& ctx, const ObPxFinishSqcResultMsg& pkt);
  int on_eof_row(ObExecContext& ctx);
  int on_sqc_init_fail(ObDfo& dfo, ObPxSqcMeta& sqc);
  int on_interrupted(ObExecContext& ctx, const common::ObInterruptCode& pkt);
  int startup_msg_loop(ObExecContext& ctx);
  // begin DATAHUB msg processing
  int on_piece_msg(ObExecContext& ctx, const ObBarrierPieceMsg& pkt);
  int on_piece_msg(ObExecContext& ctx, const ObWinbufPieceMsg& pkt);
  // end DATAHUB msg processing

  ObPxCoordInfo& coord_info_;
  ObIPxCoordEventListener& listener_;
};
class ObPxMsgProc : public ObIPxCoordMsgProc {
public:
  ObPxMsgProc(ObPxCoordInfo& coord_info, ObIPxCoordEventListener& listener, ObPxRootDfoAction& root_dfo_action)
      : coord_info_(coord_info), listener_(listener), root_dfo_action_(root_dfo_action), scheduler_(NULL)
  {}
  // msg processor callback
  int on_sqc_init_msg(ObExecContext& ctx, const ObPxInitSqcResultMsg& pkt);
  int on_sqc_finish_msg(ObExecContext& ctx, const ObPxFinishSqcResultMsg& pkt);
  int on_eof_row(ObExecContext& ctx);
  int on_sqc_init_fail(ObDfo& dfo, ObPxSqcMeta& sqc);
  int on_interrupted(ObExecContext& ctx, const common::ObInterruptCode& pkt);
  int startup_msg_loop(ObExecContext& ctx);
  int on_process_end(ObExecContext& ctx);

  void set_scheduler(ObDfoSchedulerBasic* scheduler)
  {
    scheduler_ = scheduler;
  }

  // root dfo's special scheduling route
  int on_dfo_pair_thread_inited(ObExecContext& ctx, ObDfo& child, ObDfo& parent);
  static int send_rpc_filter(ObExecContext& ctx);
  // begin DATAHUB msg processing
  int on_piece_msg(ObExecContext& ctx, const ObBarrierPieceMsg& pkt);
  int on_piece_msg(ObExecContext& ctx, const ObWinbufPieceMsg& pkt);
  // end DATAHUB msg processing
private:
  int do_cleanup_dfo(ObDfo& dfo);
  int fast_dispatch_sqc(ObExecContext& exec_ctx, ObDfo& dfo, ObArray<ObPxSqcMeta*>& sqcs);
  int wait_for_dfo_finish(ObDfoMgr& dfo_mgr);

private:
  ObPxCoordInfo& coord_info_;
  ObIPxCoordEventListener& listener_;
  ObPxRootDfoAction& root_dfo_action_;
  ObDfoSchedulerBasic* scheduler_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_COORD_OP_H_
