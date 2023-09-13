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
#include "sql/engine/px/ob_dfo.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_basic_channel.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_sqc_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/ob_px_util.h"
#include "storage/tx/ob_trans_service.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

#define BREAK_TASK_CNT(a) ((a) + 1)

int ObBloomFilterSendCtx::generate_filter_indexes(
    int64_t each_group_size,
    int64_t channel_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_data_) || channel_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter data is null", K(ret));
  } else {
    int64_t send_size = GCONF._send_bloom_filter_size * 125;
    int64_t filter_len = filter_data_->filter_.get_bits_array_length();
    int64_t count = ceil(filter_len / (double)send_size);
    int64_t start_idx = 0, end_idx = 0;
    int64_t group_channel_count = each_group_size > channel_count ?
        channel_count : each_group_size;
    BloomFilterIndex filter_index;
    for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
      start_idx = i * send_size;
      end_idx = (i + 1) * send_size;
      if (start_idx >= filter_len) {
        start_idx = filter_len - 1;
      }
      if (end_idx >= filter_len) {
        end_idx = filter_len - 1;
      }
      filter_index.begin_idx_ = start_idx;
      filter_index.end_idx_ = end_idx;
      int64_t group_count = ceil((double)channel_count / group_channel_count);
      int64_t start_channel = ObRandom::rand(0, group_count - 1);
      start_channel *= group_channel_count;
      int pos = 0;
      for (int j = start_channel; OB_SUCC(ret) &&
          j < start_channel + channel_count;
          j += group_channel_count) {
        pos = (j >= channel_count ? j - channel_count : j);
        pos = (pos / group_channel_count) * group_channel_count;
        filter_index.channel_ids_.reset();
        if (pos + group_channel_count > channel_count) {
          filter_index.channel_id_ = (i % (channel_count - pos)) + pos;
        } else {
          filter_index.channel_id_ = (i % group_channel_count) + pos;
        }
        for (int k = pos; OB_SUCC(ret) && k < channel_count && k < pos + group_channel_count; ++k) {
          OZ(filter_index.channel_ids_.push_back(k));
        }
        OZ(filter_indexes_.push_back(filter_index));
      }
    }
  }
  return ret;
}

ObPxSQCProxy::ObPxSQCProxy(ObSqcCtx &sqc_ctx,
                           ObPxRpcInitSqcArgs &arg)
  : sqc_ctx_(sqc_ctx),
    sqc_arg_(arg),
    leader_token_lock_(common::ObLatchIds::PX_WORKER_LEADER_LOCK),
    first_buffer_cache_(nullptr),
    bf_send_ctx_array_(),
    sample_msg_(),
    init_channel_msg_(),
    p2p_dh_map_()
{
}

ObPxSQCProxy::~ObPxSQCProxy()
{
  sample_msg_.reset();
}

int ObPxSQCProxy::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(link_sqc_qc_channel(sqc_arg_))) {
    LOG_WARN("fail to link sqc qc channel", K(ret));
  } else if (OB_FAIL(setup_loop_proc(sqc_ctx_))) {
    LOG_WARN("fail to setup loop proc", K(ret));
  }
  return ret;
}

int ObPxSQCProxy::append_bf_send_ctx(int64_t &bf_send_ctx_idx)
{
  int ret = OB_SUCCESS;
  bf_send_ctx_idx = -1;
  if (OB_FAIL(bf_send_ctx_array_.push_back(ObBloomFilterSendCtx()))) {
    LOG_WARN("failed to pre alloc ObBloomFilterSendCtx", K(ret));
  } else {
    bf_send_ctx_idx = bf_send_ctx_array_.count() - 1;
  }
  return ret;
}

int ObPxSQCProxy::link_sqc_qc_channel(ObPxRpcInitSqcArgs &sqc_arg)
{
  int ret = OB_SUCCESS;
  ObPxSqcMeta &sqc = sqc_arg.sqc_;
  ObDtlChannel *ch = sqc.get_sqc_channel();
  // 注意：ch 已经提前在 ObInitSqcP::process() 中 link 过了
  // 这是一个优化，为了能够尽早收到 qc 下发的 data channel 信息
  if (OB_ISNULL(ch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail link sqc qc channel", K(sqc), K(ret));
  } else {
    (void) sqc_ctx_.msg_loop_.register_channel(*ch);
    const ObDtlBasicChannel *basic_channel = static_cast<ObDtlBasicChannel*>(sqc.get_sqc_channel());
    sqc_ctx_.msg_loop_.set_tenant_id(basic_channel->get_tenant_id());
    sqc_ctx_.msg_loop_.set_process_query_time(get_process_query_time());
    sqc_ctx_.msg_loop_.set_query_timeout_ts(get_query_timeout_ts());
    LOG_TRACE("register sqc-qc channel", K(sqc));
  }
  return ret;
}

int ObPxSQCProxy::setup_loop_proc(ObSqcCtx &sqc_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg_ready_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    LOG_WARN("fail init cond", K(ret));
  } else if (OB_FAIL(sqc_ctx.receive_data_ch_provider_.init())) {
    LOG_WARN("fail init receive ch provider", K(ret));
  } else if (OB_FAIL(sqc_ctx.transmit_data_ch_provider_.init())) {
    LOG_WARN("fail init transmit ch provider", K(ret));
  } else if (OB_FAIL(sqc_ctx.bf_ch_provider_.init())) {
    LOG_WARN("fail init bool filter provider", K(ret));
  } else {
    (void)sqc_ctx.msg_loop_
        .register_processor(sqc_ctx.receive_data_ch_msg_proc_)
        .register_processor(sqc_ctx.transmit_data_ch_msg_proc_)
        .register_processor(sqc_ctx.px_bloom_filter_msg_proc_)
        .register_processor(sqc_ctx.barrier_whole_msg_proc_)
        .register_processor(sqc_ctx.winbuf_whole_msg_proc_)
        .register_processor(sqc_ctx.sample_whole_msg_proc_)
        .register_processor(sqc_ctx.rollup_key_whole_msg_proc_)
        .register_processor(sqc_ctx.rd_wf_whole_msg_proc_)
        .register_processor(sqc_ctx.init_channel_whole_msg_proc_)
        .register_processor(sqc_ctx.reporting_wf_piece_msg_proc_)
        .register_processor(sqc_ctx.opt_stats_gather_whole_msg_proc_)
        .register_interrupt_processor(sqc_ctx.interrupt_proc_);
  }
  return ret;
}

void ObPxSQCProxy::destroy()
{
  int ret_unreg = OB_SUCCESS;
  if (OB_SUCCESS != (ret_unreg = sqc_ctx_.msg_loop_.unregister_all_channel())) {
    // the following unlink actions is not safe is any unregister failure happened
    LOG_ERROR_RET(ret_unreg, "fail unregister all channel from msg_loop", KR(ret_unreg));
  }
  sample_msg_.reset();
}

int ObPxSQCProxy::unlink_sqc_qc_channel(ObPxRpcInitSqcArgs &sqc_arg)
{
  int ret = OB_SUCCESS;
  ObDtlChannel *ch = NULL;
  ObPxSqcMeta &sqc = sqc_arg.sqc_;
  ch = sqc.get_sqc_channel();
  ObDtlChannelInfo &ci = sqc.get_sqc_channel_info();

  if (OB_NOT_NULL(ch)) {
    if (OB_FAIL(ch->flush())) {
      LOG_WARN("failed flush", K(ret));
    }
    /* even if unregister fail we still unlink it */
    if (OB_FAIL(dtl::ObDtlChannelGroup::unlink_channel(ci))) {
      LOG_WARN("fail unlink channel", K(ci), K(ret));
    }
    sqc.set_sqc_channel(nullptr);
  }
  return ret;
}

int ObPxSQCProxy::process_dtl_msg(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(do_process_dtl_msg(timeout_ts))) {
    // next loop
  }

  // 如果 do_process_dtl_msg 没有获取到任何消息，
  // 则返回 EAGAIN，否则返回 SUCC
  if (OB_EAGAIN == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("leader fail process dtl msg", K(ret));
  }
  return ret;
}

int ObPxSQCProxy::do_process_dtl_msg(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout_ts);
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sqc_ctx_.msg_loop_.process_any(10))) {
      if (OB_EAGAIN == ret) {
        LOG_TRACE("no message for sqc, exit", K(ret), K(timeout_ts));
      } else {
        LOG_WARN("fail proccess dtl msg", K(timeout_ts), K(ret));
      }
    }
  }
  return ret;
}

int ObPxSQCProxy::get_transmit_data_ch(
  const int64_t sqc_id,
  const int64_t task_id,
  int64_t timeout_ts,
  ObPxTaskChSet &task_ch_set,
  ObDtlChTotalInfo **ch_info)
{
  int ret = OB_SUCCESS;
  bool need_process_dtl = need_transmit_channel_map_via_dtl();
  do {
    ObSqcLeaderTokenGuard guard(leader_token_lock_, msg_ready_cond_);
    if (guard.hold_token()) {
      do {
        if (need_process_dtl) {
          ret = process_dtl_msg(timeout_ts);
        }

        // 当收完所有消息后，再关注做自己的任务
        // 看看自己期望的 transmit channel map 是否已经收到
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sqc_ctx_.transmit_data_ch_provider_.get_data_ch_nonblock(
                      sqc_id, task_id, timeout_ts, task_ch_set, ch_info,
                      sqc_arg_.sqc_.get_qc_addr(), get_process_query_time()))) {
            if (OB_EAGAIN == ret) {
              // 如果 provider 里没有任何消息，同时又判定不需要通过 dtl 取数据，说明存在逻辑错误
              if (!need_process_dtl) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expect peek data channel succ", K(ret));
              }
            } else {
              LOG_WARN("fail peek data channel from ch_provider", K(ret));
            }
          }
        }
      } while (OB_EAGAIN == ret);
    } else {
      // follower
      ret = sqc_ctx_.transmit_data_ch_provider_.get_data_ch(sqc_id, task_id, timeout_ts, task_ch_set, ch_info);
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObPxSQCProxy::get_receive_data_ch(int64_t child_dfo_id,
                                      const int64_t sqc_id,
                                      const int64_t task_id,
                                      int64_t timeout_ts,
                                      ObPxTaskChSet &task_ch_set,
                                      ObDtlChTotalInfo *ch_info)
{
  int ret = OB_SUCCESS;
  bool need_process_dtl = need_receive_channel_map_via_dtl(child_dfo_id);

  LOG_TRACE("get_receive_data_ch", K(need_process_dtl), K(child_dfo_id));
  do {
    ObSqcLeaderTokenGuard guard(leader_token_lock_, msg_ready_cond_);
    if (guard.hold_token()) {
      do {
        if (need_process_dtl) {
          ret = process_dtl_msg(timeout_ts);
        }

        LOG_TRACE("process dtl msg done", K(ret));
        // 当收完所有消息后，再关注做自己的任务
        // 看看自己期望的 receive channel map 是否已经收到
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sqc_ctx_.receive_data_ch_provider_.get_data_ch_nonblock(
                      child_dfo_id, sqc_id, task_id, timeout_ts, task_ch_set, ch_info,
                      sqc_arg_.sqc_.get_qc_addr(), get_process_query_time()))) {
            if (OB_EAGAIN == ret) {
              // 如果 provider 里没有任何消息，同时又判定不需要通过 dtl 取数据，说明存在逻辑错误
              if (!need_process_dtl) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expect peek data channel succ", K(ret));
              }
            } else {
              LOG_WARN("fail peek data channel from ch_provider", K(ret));
            }
          } else {
            LOG_TRACE("SUCC got nonblock receive channel", K(task_ch_set), K(child_dfo_id));
          }
        }
      } while (OB_EAGAIN == ret);
    } else {
      // follower
      ret = sqc_ctx_.receive_data_ch_provider_.get_data_ch(
          child_dfo_id, sqc_id, task_id, timeout_ts, task_ch_set, ch_info);
    }
  } while(OB_EAGAIN == ret);
  return ret;
}


int ObPxSQCProxy::get_part_ch_map(ObPxPartChInfo &map, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  bool need_process_dtl = need_transmit_channel_map_via_dtl();
  ObSqcLeaderTokenGuard guard(leader_token_lock_, msg_ready_cond_);
  do {
    if (guard.hold_token()) {
      do {
        if (need_process_dtl) {
          ret = process_dtl_msg(timeout_ts);
        }
        // 当收完所有消息后，再关注做自己的任务
        // 看看自己期望的 transmit channel map 是否已经收到
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sqc_ctx_.transmit_data_ch_provider_.get_part_ch_map_nonblock(
                      map, timeout_ts, sqc_arg_.sqc_.get_qc_addr(), get_process_query_time()))) {
            if (OB_EAGAIN == ret) {
              // 如果 provider 里没有任何消息，同时又判定不需要通过 dtl 取数据，说明存在逻辑错误
              if (!need_process_dtl) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expect peek data channel succ", K(ret));
              }
            } else {
              LOG_WARN("fail peek data channel from ch_provider", K(ret));
            }
          }
        }
      } while (OB_EAGAIN == ret);
    } else {
      // follower
      ret = sqc_ctx_.transmit_data_ch_provider_.get_part_ch_map(map, timeout_ts);
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObPxSQCProxy::report_task_finish_status(int64_t task_idx, int rc)
{
  int ret = OB_SUCCESS;
  auto &tasks = sqc_ctx_.get_tasks();
  if (task_idx < 0 || task_idx >= tasks.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task idx", K(task_idx), K(tasks.count()), K(ret));
  } else {
    ObPxTask &task = tasks.at(task_idx);
    if (task.has_result()) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("task finish status already set", K(task), K(task_idx), K(rc), K(ret));
    } else {
      task.set_result(rc);
    }
  }
  return ret;
}

// only can be called by root thread
int ObPxSQCProxy::check_task_finish_status(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  auto &tasks = sqc_ctx_.get_tasks();
  bool all_tasks_finish = true;
  do {
    if (timeout_ts < ObTimeUtility::current_time()) {
      ret = OB_TIMEOUT;
      break;
    }
    all_tasks_finish = true;
    ARRAY_FOREACH(tasks, idx) {
      ObPxTask &task = tasks.at(idx);
      if (!task.has_result()) {
        all_tasks_finish = false;
        break;
      }
    }
    ObSqcLeaderTokenGuard guard(leader_token_lock_, msg_ready_cond_);
    if (guard.hold_token()) {
      // 如果还有 task 没有完成，则尝试收取 dtl 消息
      // 要特别注意，此时可能并没有什么 DTL 消息要
      // 收了，task 的 channel 信息已经收取完毕。
      // 这里之所以要尝试一次 process_dtl_msg 是因为
      // 在 root 线程调用 check_task_finish_status
      // 时，还可能存在 slave 线程等在 get_data_ch 中，
      // 需要人帮助推进。
      //
      // FIXME: 这段代码可能引入额外代价，白等若干毫秒
      // 更精细的控制方式是，先判断是否所有预期的消息都已经
      // 收到并处理，如果是，则跳过 process_dtl_msg 步骤，
      // 如果不是，则的确需要执行 process_dtl_msg
      // 暂时总假设 all_ctrl_msg_received = false
      bool all_ctrl_msg_received = false;

      if (!all_tasks_finish && !all_ctrl_msg_received) {
        if (OB_FAIL(process_dtl_msg(timeout_ts))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("fail process dtl msg", K(ret));
          }
        }
      }
    } else {
      // TODO: wait 100us
      ob_usleep(1000);
    }
  } while (!all_tasks_finish);

  if (all_tasks_finish) {
    sqc_ctx_.all_tasks_finish_ = true;
  }
  return ret;
}

int ObPxSQCProxy::report(int end_ret) const
{
  int ret = OB_SUCCESS;
  ObPxRpcInitSqcArgs &sqc_arg = sqc_arg_;
  ObSqcCtx &sqc_ctx = sqc_ctx_;
  ObPxSqcMeta &sqc = sqc_arg.sqc_;
  ObPxFinishSqcResultMsg finish_msg;
  int64_t affected_rows = 0;
  // 任意一个 task 失败，则意味着全部 task 失败
  // 第一版暂不支持重试
  int sqc_ret = OB_SUCCESS;
  auto &tasks = sqc_ctx.get_tasks();
  ObSQLSessionInfo *session = NULL;
  CK(OB_NOT_NULL(sqc_arg.exec_ctx_) &&
     OB_NOT_NULL(session = GET_MY_SESSION(*sqc_arg.exec_ctx_)));
  for (int64_t i = 0; i < tasks.count(); ++i) {
    // overwrite ret
    ObPxTask &task = tasks.at(i);
    ObPxErrorUtil::update_sqc_error_code(sqc_ret, task.get_result(), task.err_msg_, finish_msg.err_msg_);
    if (OB_SUCCESS == finish_msg.das_retry_rc_) {
      //Even if the PX task is successfully retried by DAS,
      //it is necessary to collect the retry error code of each task and provide feedback to the QC node of PX,
      //so that the QC node can refresh the location cache in a timely manner,
      //avoiding the next execution from being sent to the same erroneous node.
      finish_msg.das_retry_rc_ = task.get_das_retry_rc();
    }
    affected_rows += task.get_affected_rows();
    finish_msg.dml_row_info_.add_px_dml_row_info(task.dml_row_info_);
    finish_msg.temp_table_id_ = task.temp_table_id_;
    if (OB_NOT_NULL(session)) {
      transaction::ObTxDesc *&sqc_tx_desc = session->get_tx_desc();
      transaction::ObTxDesc *&task_tx_desc = tasks.at(i).get_tx_desc();
      if (OB_NOT_NULL(task_tx_desc)) {
        if (OB_NOT_NULL(sqc_tx_desc)) {
          (void)MTL(transaction::ObTransService*)->merge_tx_state(*sqc_tx_desc, *task_tx_desc);
          (void)MTL(transaction::ObTransService*)->release_tx(*task_tx_desc);
        } else {
          sql::ObSQLSessionInfo::LockGuard guard(session->get_thread_data_lock());
          sqc_tx_desc = task_tx_desc;
        }
        task_tx_desc = NULL;
      }
    }
    if (0 == i) {
      OZ(finish_msg.fb_info_.assign(tasks.at(i).fb_info_));
    } else {
      OZ(finish_msg.fb_info_.merge_feedback_info(tasks.at(i).fb_info_));
    }

    OZ(append(finish_msg.interm_result_ids_, task.interm_result_ids_));
  }
  ObPxErrorUtil::update_error_code(sqc_ret, end_ret);
  if (OB_SUCCESS != ret && OB_SUCCESS == sqc_ret) {
    sqc_ret = ret;
  }
  finish_msg.sqc_affected_rows_ = affected_rows;
  finish_msg.sqc_id_ = sqc.get_sqc_id();
  finish_msg.dfo_id_ = sqc.get_dfo_id();
  finish_msg.rc_ = sqc_ret;
  // 重写错误码，使得scheduler端能等待远端schema刷新并重试
  if (OB_SUCCESS != sqc_ret && is_schema_error(sqc_ret)) {
    ObInterruptUtil::update_schema_error_code(sqc_arg.exec_ctx_, finish_msg.rc_);
  }

  // 如果 session 为 null，rc 不会为 SUCCESS，没有设置 trans_result 也无妨
  if (OB_NOT_NULL(session) && OB_NOT_NULL(session->get_tx_desc())) {
    // overwrite ret
    if (OB_FAIL(MTL(transaction::ObTransService*)
                ->get_tx_exec_result(*session->get_tx_desc(),
                                     finish_msg.get_trans_result()))) {
      LOG_WARN("fail get tx result", K(ret),
               "msg_trans_result", finish_msg.get_trans_result(),
               "tx_desc", *session->get_tx_desc());
      finish_msg.rc_ = (OB_SUCCESS != sqc_ret) ? sqc_ret : ret;
    } else {
      LOG_TRACE("report trans_result",
                "msg_trans_result", finish_msg.get_trans_result(),
                "tx_desc", *session->get_tx_desc());
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    int64_t query_timeout = 0;
    session->get_query_timeout(query_timeout);
    if (OB_FAIL(OB_E(EventTable::EN_PX_SQC_NOT_REPORT_TO_QC, query_timeout) OB_SUCCESS)) {
      static bool errsim = false;
      errsim = !errsim;
      if (errsim) {
        LOG_WARN("sqc report to qc by design", K(ret), K(query_timeout));
        return OB_SUCCESS;
      }
    }
  }
#endif

  ObDtlChannel *ch = sqc.get_sqc_channel();
  // overwrite ret
  if (OB_ISNULL(ch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty channel", K(sqc), K(ret));
  } else if (OB_FAIL(ch->send(finish_msg,
      sqc_arg.exec_ctx_->get_physical_plan_ctx()->get_timeout_timestamp()))) {
      // 尽力而为，如果 push 失败就由其它机制处理
    LOG_WARN("fail push data to channel", K(ret));
  } else if (OB_FAIL(ch->flush())) {
    LOG_WARN("fail flush dtl data", K(ret));
  }

  return ret;
}

void ObPxSQCProxy::get_self_sqc_info(ObDtlSqcInfo &sqc_info)
{
  ObPxSqcMeta &sqc = sqc_arg_.sqc_;
  sqc_info.set(sqc.get_qc_id(), sqc.get_dfo_id(), sqc.get_sqc_id());
}

void ObPxSQCProxy::get_self_dfo_key(ObDtlDfoKey &key)
{
  ObPxSqcMeta &sqc = sqc_arg_.sqc_;
  key.set(sqc.get_qc_server_id(), sqc.get_px_sequence_id(), sqc.get_qc_id(), sqc.get_dfo_id());
}

void ObPxSQCProxy::get_parent_dfo_key(ObDtlDfoKey &key)
{
  ObPxSqcMeta &sqc = sqc_arg_.sqc_;
  key.set(sqc.get_qc_server_id(), sqc.get_px_sequence_id(),
      sqc.get_qc_id(), sqc.get_parent_dfo_id());
}

bool ObPxSQCProxy::need_transmit_channel_map_via_dtl()
{
  ObPxSqcMeta &sqc = sqc_arg_.sqc_;
  return !sqc.is_prealloc_transmit_channel();
}

bool ObPxSQCProxy::need_receive_channel_map_via_dtl(int64_t child_dfo_id)
{

  ObPxSqcMeta &sqc = sqc_arg_.sqc_;
  bool via_sqc = false;
  if (sqc.is_prealloc_transmit_channel()) {
     via_sqc = (sqc.get_receive_channel_msg().get_child_dfo_id() == child_dfo_id);
  }
  return !via_sqc;
}

int ObPxSQCProxy::get_bloom_filter_ch(
  ObPxBloomFilterChSet &ch_set,
  int64_t &sqc_count,
  int64_t timeout_ts,
  bool is_transmit)
{
  int ret = OB_SUCCESS;
  int64_t wait_count = 0;
  do {
    ObSqcLeaderTokenGuard guard(leader_token_lock_, msg_ready_cond_);
    if (guard.hold_token()) {
      ret = process_dtl_msg(timeout_ts);
      LOG_DEBUG("process dtl bf msg done", K(ret));
    }
    if (OB_SUCCESS == ret || OB_EAGAIN == ret) {
      ret = sqc_ctx_.bf_ch_provider_.get_data_ch_nonblock(
              ch_set, sqc_count, timeout_ts, is_transmit, sqc_arg_.sqc_.get_qc_addr(),
              get_process_query_time());
    }
    if (OB_EAGAIN == ret) {
      if(0 == (++wait_count) % 100) {
        LOG_TRACE("try to get bf data channel repeatly", K(wait_count), K(ret));
      }
      // wait 1000us
      ob_usleep(1000);
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObPxSQCProxy::get_whole_msg_provider(uint64_t op_id, ObDtlMsgType msg_type, ObPxDatahubDataProvider *&provider)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqc_ctx_.get_whole_msg_provider(op_id, msg_type, provider))) {
    SQL_LOG(WARN, "fail get provider", K(ret));
  }
  return ret;
}

int ObPxSQCProxy::make_sqc_sample_piece_msg(ObDynamicSamplePieceMsg &msg, bool &finish)
{
  int ret = OB_SUCCESS;
  if (msg.sample_type_ == HEADER_INPUT_SAMPLE && sample_msg_.row_stores_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row stores", K(ret));
  } else if (OB_FAIL(sample_msg_.merge_piece_msg(
      sqc_ctx_.get_task_count(),
      msg,
      finish))) {
    LOG_WARN("fail to merge piece msg", K(ret));
  } else if (finish) {
    sample_msg_.expect_range_count_ = msg.expect_range_count_;
    sample_msg_.source_dfo_id_ = msg.source_dfo_id_;
    sample_msg_.target_dfo_id_ = msg.target_dfo_id_;
    sample_msg_.op_id_ = msg.op_id_;
    sample_msg_.sample_type_ = msg.sample_type_;
    OZ(sample_msg_.tablet_ids_.assign(msg.tablet_ids_));
  }
  return ret;
}

int64_t ObPxSQCProxy::get_process_query_time()
{
  int64_t res = 0;
  if (OB_NOT_NULL(sqc_arg_.exec_ctx_) && OB_NOT_NULL(sqc_arg_.exec_ctx_->get_my_session())) {
    res = sqc_arg_.exec_ctx_->get_my_session()->get_process_query_time();
  }
  return res;
}

int64_t ObPxSQCProxy::get_query_timeout_ts()
{
  int64_t res = 0;
  if (OB_NOT_NULL(sqc_arg_.exec_ctx_) && OB_NOT_NULL(sqc_arg_.exec_ctx_->get_physical_plan_ctx())) {
    res = sqc_arg_.exec_ctx_->get_physical_plan_ctx()->get_timeout_timestamp();
  }
  return res;
}

int64_t ObPxSQCProxy::get_task_count() const { return sqc_ctx_.get_task_count(); }

int ObPxSQCProxy::sync_wait_all(ObPxDatahubDataProvider &provider)
{
  int ret = OB_SUCCESS;
  const int64_t task_cnt = get_task_count();
  const int64_t curr_rescan_cnt = ATOMIC_LOAD(&provider.rescan_cnt_) + 1;
  MEM_BARRIER();
  const int64_t idx = ATOMIC_AAF(&provider.dh_msg_cnt_, 1);
  int64_t loop_cnt = 0;
  // The whole message should be reset in next rescan, we reset it after last piece msg
  // firstly do sync wait until all piece threads are in loop
  do {
    ++loop_cnt;
    if (task_cnt == idx % (BREAK_TASK_CNT(task_cnt))) { // last thread
      provider.msg_set_ = false;
      provider.reset(); // reset whole message
      ATOMIC_AAF(&provider.rescan_cnt_, 1);
      MEM_BARRIER();
      ATOMIC_AAF(&provider.dh_msg_cnt_, 1); // to break the loop
    } else {
      ob_usleep(1000);
      if (0 == loop_cnt % 64) {
        if (OB_UNLIKELY(IS_INTERRUPTED())) {
          ObInterruptCode &code = GET_INTERRUPT_CODE();
          ret = code.code_;
          LOG_WARN("message loop is interrupted", K(code), K(ret));
        } else if (OB_FAIL(THIS_WORKER.check_status())) {
          LOG_WARN("failed to sync wait", K(ret), K(task_cnt), K(provider.dh_msg_cnt_));
        }
      }
    }
  } while (OB_SUCC(ret) && provider.dh_msg_cnt_ < BREAK_TASK_CNT(task_cnt) * curr_rescan_cnt);

  return ret;
}

int ObPxSQCProxy::construct_p2p_dh_map(ObP2PDhMapInfo &map_info)
{
  int ret = OB_SUCCESS;
  int bucket_size = 2 * map_info.p2p_sequence_ids_.count();
  if (0 == bucket_size) {
  } else if (OB_FAIL(p2p_dh_map_.create(bucket_size,
      "SQCDHMapKey",
      "SQCDHMAPNode"))) {
    LOG_WARN("create hash table failed", K(ret));
  } else if (map_info.p2p_sequence_ids_.count() !=
      map_info.target_addrs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected map info count", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) &&
         i < map_info.p2p_sequence_ids_.count(); ++i) {
      if (OB_FAIL(p2p_dh_map_.set_refactored(map_info.p2p_sequence_ids_.at(i),
          &map_info.target_addrs_.at(i)))) {
        LOG_WARN("fail to set p2p dh map", K(ret));
      }
    }
  }
  return ret;
}

int ObPxSQCProxy::check_is_local_dh(int64_t p2p_dh_id, bool &is_local, int64_t msg_cnt)
{
  int ret = OB_SUCCESS;
  ObSArray<ObAddr> *target_addrs = nullptr;
  if (OB_FAIL(p2p_dh_map_.get_refactored(p2p_dh_id, target_addrs))) {
    LOG_WARN("fail to get dh map", K(ret));
  } else if (OB_ISNULL(target_addrs) || target_addrs->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected target addrs", K(ret));
  } else if (target_addrs->count() == 1 &&
             GCTX.self_addr() == target_addrs->at(0) &&
             1 == msg_cnt) {
    is_local = true;
  } else {
    is_local = false;
  }
  return ret;
}
