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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_px_fifo_coord_op.h"
#include "share/ob_rpc_share.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql
{

OB_SERIALIZE_MEMBER((ObPxFifoCoordOpInput, ObPxReceiveOpInput));

OB_SERIALIZE_MEMBER((ObPxFifoCoordSpec, ObPxCoordSpec));

ObPxFifoCoordOp::ObPxFifoCoordOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObPxCoordOp(exec_ctx, spec, input),
    serial_scheduler_(coord_info_, *this, listener_),
    parallel_scheduler_(coord_info_, *this, listener_, msg_proc_),
    msg_proc_(coord_info_, listener_, *this),
    sqc_finish_msg_proc_(exec_ctx, msg_proc_),
    sqc_init_msg_proc_(exec_ctx, msg_proc_),
    barrier_piece_msg_proc_(exec_ctx, msg_proc_),
    winbuf_piece_msg_proc_(exec_ctx, msg_proc_),
    interrupt_proc_(exec_ctx, msg_proc_),
    sample_piece_msg_proc_(exec_ctx, msg_proc_),
    rollup_key_piece_msg_proc_(exec_ctx, msg_proc_),
    rd_wf_piece_msg_proc_(exec_ctx, msg_proc_),
    init_channel_piece_msg_proc_(exec_ctx, msg_proc_),
    reporting_wf_piece_msg_proc_(exec_ctx, msg_proc_),
    opt_stats_gather_piece_msg_proc_(exec_ctx, msg_proc_)
  {}

int ObPxFifoCoordOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
  if (OB_FAIL(ObPxCoordOp::inner_open())) {
    LOG_WARN("fail open op", K(MY_SPEC.id_), K(ret));
  } else if (OB_FAIL(setup_loop_proc())) {
    LOG_WARN("fail setup loop proc", K(ret));
  } else {
    if (OB_UNLIKELY(session->get_ddl_info().is_ddl())) {
      // use parallel scheduler for ddl to avoid large memory usage because
      // serial scheduler will hold the memory of intermediate result rows
      msg_proc_.set_scheduler(&parallel_scheduler_);
    } else if (1 == px_dop_) {
      msg_proc_.set_scheduler(&serial_scheduler_);
    } else {
      msg_proc_.set_scheduler(&parallel_scheduler_);
    }
    metric_.set_id(MY_SPEC.id_);
  }
  return ret;
}

int ObPxFifoCoordOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxCoordOp::inner_close())) {
    LOG_WARN("fail close op", K(MY_SPEC.id_), K(ret));
  }
  return ret;
}

int ObPxFifoCoordOp::setup_loop_proc()
{
  int ret = OB_SUCCESS;
  msg_loop_.clear_all_proc();
  (void)msg_loop_
      .register_processor(px_row_msg_proc_)
      .register_processor(sqc_init_msg_proc_)
      .register_processor(sqc_finish_msg_proc_)
      .register_processor(barrier_piece_msg_proc_)
      .register_processor(winbuf_piece_msg_proc_)
      .register_processor(sample_piece_msg_proc_)
      .register_processor(rollup_key_piece_msg_proc_)
      .register_processor(rd_wf_piece_msg_proc_)
      .register_processor(init_channel_piece_msg_proc_)
      .register_processor(reporting_wf_piece_msg_proc_)
      .register_processor(opt_stats_gather_piece_msg_proc_)
      .register_interrupt_processor(interrupt_proc_);
  return ret;
}

int ObPxFifoCoordOp::inner_get_next_row()
{
  const int64_t row_cnt = 1;
  return fetch_rows(row_cnt);
}

int ObPxFifoCoordOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = fetch_rows(std::min(max_row_cnt, MY_SPEC.max_batch_size_));
  if (OB_ITER_END == ret) {
    brs_.size_ = 0;
    brs_.end_ = true;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPxFifoCoordOp::fetch_rows(const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!first_row_fetched_)) {
    // 驱动初始 DFO 的分发
    if (OB_FAIL(msg_proc_.startup_msg_loop(ctx_))) {
      LOG_WARN("initial dfos NOT dispatched successfully", K(ret));
    }
    msg_loop_.set_tenant_id(ctx_.get_my_session()->get_effective_tenant_id());
    first_row_fetched_ = true; // 控制不再主动调用 startup_msg_loop，后继 loop 都消息触发
  }

#ifdef ERRSIM
  ObSQLSessionInfo *session = ctx_.get_my_session();
  int64_t query_timeout = 0;
  session->get_query_timeout(query_timeout);
  if (OB_FAIL(OB_E(EventTable::EN_PX_QC_EARLY_TERMINATE, query_timeout) OB_SUCCESS)) {
    LOG_WARN("fifo qc not interrupt qc by design", K(ret), K(query_timeout));
    sleep(14);
    return ret;
  }
#endif

  while (OB_SUCC(ret)) {
    // rows must received by coord op instead of receive op, otherwise we will
    // trap in receive op and lose control.
    int64_t left = row_reader_.left_rows();
    if (left >= row_cnt || (left > 0 && msg_loop_.all_eof(task_channels_.count()))) {
      clear_evaluated_flag();
      clear_dynamic_const_parent_flag();
      metric_.mark_interval_start();
      if (!is_vectorized()) {
        ret = row_reader_.get_next_row(MY_SPEC.child_exprs_,
                                       MY_SPEC.dynamic_const_exprs_,
                                       eval_ctx_);
      } else {
        int64_t read_rows = 0;
        ret = row_reader_.get_next_batch(MY_SPEC.child_exprs_, MY_SPEC.dynamic_const_exprs_,
                                         eval_ctx_,  row_cnt, read_rows, stored_rows_);
        brs_.size_ = read_rows;
      }
      metric_.mark_interval_end(&time_recorder_);
      if (OB_FAIL(ret)) {
        LOG_WARN("get row failed", K(ret));
      } else {
        if (!first_row_sent_) {
          // used to make sure logging the following message once.
          first_row_sent_ = true;
          LOG_TRACE("TIMERECORD ",
                    "reserve:=1 name:=RQC dfoid:=-1 sqcid:=-1 taskid:=-1 start:",
                    ObTimeUtility::current_time());
        }
      }
      break;
    }

    if (msg_loop_.all_eof(task_channels_.count())) {
      // process end condition:
      // 1. all rows returned
      // 2. all SQC report worker execution finish
      if (GCONF.enable_sql_audit) {
        op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::EXCHANGE_EOF_TIMESTAMP;
        op_monitor_info_.otherstat_2_value_ = oceanbase::common::ObClockGenerator::getClock();
      }
      if (coord_info_.all_threads_finish_) {
        (void) msg_proc_.on_process_end(ctx_);
        ret = OB_ITER_END;
        LOG_TRACE("all rows received, all sqcs reported, qc says: byebye!", K(ret));
        LOG_TRACE("TIMERECORD ",
                  "reserve:=1 name:=RQC dfoid:=-1 sqcid:=-1 taskid:=-1 end:",
                  ObTimeUtility::current_time());
        break;
      }
    }

    if (OB_FAIL(ctx_.fast_check_status())) {
      LOG_WARN("fail check status, maybe px query timeout", K(ret));
    } else if (OB_FAIL(msg_loop_.process_any())) {
      LOG_DEBUG("process one failed error", K(ret));
      if (OB_EAGAIN == ret) {
        LOG_TRACE("no message, try again", K(ret));
        ret = OB_SUCCESS;
      } else if (OB_ITER_END != ret) {
        LOG_WARN("fail process message", K(ret));
      }
    } else {
      ObDtlMsgType msg_type = msg_loop_.get_last_msg_type();
      switch (msg_type) {
        case ObDtlMsgType::PX_NEW_ROW:
          // got rows
          break;
        case ObDtlMsgType::INIT_SQC_RESULT:
        case ObDtlMsgType::FINISH_SQC_RESULT:
        case ObDtlMsgType::DH_BARRIER_PIECE_MSG:
        case ObDtlMsgType::DH_WINBUF_PIECE_MSG:
        case ObDtlMsgType::DH_DYNAMIC_SAMPLE_PIECE_MSG:
        case ObDtlMsgType::DH_ROLLUP_KEY_PIECE_MSG:
        case ObDtlMsgType::DH_RANGE_DIST_WF_PIECE_MSG:
        case ObDtlMsgType::DH_INIT_CHANNEL_PIECE_MSG:
        case ObDtlMsgType::DH_SECOND_STAGE_REPORTING_WF_PIECE_MSG:
        case ObDtlMsgType::DH_OPT_STATS_GATHER_PIECE_MSG:
          // all message processed in callback
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected msg type", K(msg_type));
          break;
      }
    }
  }
  if (ret == OB_ITER_END && !iter_end_) {
    iter_end_ = true;
    LOG_TRACE("RECORDTIME", K(time_recorder_));
  } else if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    int ret_terminate = terminate_running_dfos(coord_info_.dfo_mgr_);
    LOG_WARN("QC get error code", K(ret), K(ret_terminate));
    if (OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == ret
        && OB_SUCCESS != ret_terminate) {
      ret = ret_terminate;
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
