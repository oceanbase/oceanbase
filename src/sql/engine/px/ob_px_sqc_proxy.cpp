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

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

ObPxSQCProxy::ObPxSQCProxy(ObSqcCtx& sqc_ctx, ObPxRpcInitSqcArgs& arg)
    : sqc_ctx_(sqc_ctx),
      sqc_arg_(arg),
      leader_token_lock_(common::ObLatchIds::PX_WORKER_LEADER_LOCK),
      first_buffer_cache_(nullptr)
{}

ObPxSQCProxy::~ObPxSQCProxy()
{}

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

int ObPxSQCProxy::link_sqc_qc_channel(ObPxRpcInitSqcArgs& sqc_arg)
{
  int ret = OB_SUCCESS;
  ObPxSqcMeta& sqc = sqc_arg.sqc_;
  ObDtlChannel* ch = sqc.get_sqc_channel();
  // Note: ch has been linked in ObInitSqcP::process() in advance
  // This is an optimization,
  // in order to receive the data channel information issued by qc as soon as possible
  if (OB_ISNULL(ch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail link sqc qc channel", K(sqc), K(ret));
  } else {
    (void)sqc_ctx_.msg_loop_.register_channel(*ch);
    const ObDtlBasicChannel* basic_channel = static_cast<ObDtlBasicChannel*>(sqc.get_sqc_channel());
    sqc_ctx_.msg_loop_.set_tenant_id(basic_channel->get_tenant_id());
    LOG_TRACE("register sqc-qc channel", K(sqc));
  }
  return ret;
}

int ObPxSQCProxy::setup_loop_proc(ObSqcCtx& sqc_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqc_ctx.receive_data_ch_provider_.init())) {
    LOG_WARN("fail init receive ch provider", K(ret));
  } else if (OB_FAIL(sqc_ctx.transmit_data_ch_provider_.init())) {
    LOG_WARN("fail init transmit ch provider", K(ret));
  } else {
    (void)sqc_ctx.msg_loop_.register_processor(sqc_ctx.receive_data_ch_msg_proc_)
        .register_processor(sqc_ctx.transmit_data_ch_msg_proc_)
        .register_processor(sqc_ctx.barrier_whole_msg_proc_)
        .register_processor(sqc_ctx.winbuf_whole_msg_proc_)
        .register_interrupt_processor(sqc_ctx.interrupt_proc_);
  }
  return ret;
}

void ObPxSQCProxy::destroy()
{
  int ret_unreg = OB_SUCCESS;
  if (OB_SUCCESS != (ret_unreg = sqc_ctx_.msg_loop_.unregister_all_channel())) {
    // the following unlink actions is not safe is any unregister failure happened
    LOG_ERROR("fail unregister all channel from msg_loop", KR(ret_unreg));
  }
}

int ObPxSQCProxy::unlink_sqc_qc_channel(ObPxRpcInitSqcArgs& sqc_arg)
{
  int ret = OB_SUCCESS;
  ObDtlChannel* ch = NULL;
  ObPxSqcMeta& sqc = sqc_arg.sqc_;
  ch = sqc.get_sqc_channel();
  ObDtlChannelInfo& ci = sqc.get_sqc_channel_info();

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

  // If do_process_dtl_msg does not get any messages,
  // EAGAIN is returned, otherwise SUCC is returned
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
    if (OB_FAIL(sqc_ctx_.msg_loop_.process_one(1000))) {
      if (OB_EAGAIN == ret) {
        LOG_TRACE("no message for sqc, exit", K(ret), K(timeout_ts));
      } else {
        LOG_WARN("fail proccess dtl msg", K(timeout_ts), K(ret));
      }
    }
  }
  return ret;
}

int ObPxSQCProxy::get_transmit_data_ch(const int64_t sqc_id, const int64_t task_id, int64_t timeout_ts,
    ObPxTaskChSet& task_ch_set, ObDtlChTotalInfo** ch_info)
{
  int ret = OB_SUCCESS;
  bool need_process_dtl = need_transmit_channel_map_via_dtl();
  ObSqcLeaderTokenGuard guard(leader_token_lock_);
  if (guard.hold_token()) {
    do {
      if (need_process_dtl) {
        ret = process_dtl_msg(timeout_ts);
      }

      // After receiving all the news, focus on doing your own task
      // See if the transmit channel map you expect has been received
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sqc_ctx_.transmit_data_ch_provider_.get_data_ch_nonblock(
                sqc_id, task_id, timeout_ts, task_ch_set, ch_info))) {
          if (OB_EAGAIN == ret) {
            // If there is no message in the provider,
            // and there is no need to fetch data through dtl,
            // it means that there is a logic error
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
  return ret;
}

int ObPxSQCProxy::get_receive_data_ch(int64_t child_dfo_id, const int64_t sqc_id, const int64_t task_id,
    int64_t timeout_ts, ObPxTaskChSet& task_ch_set, ObDtlChTotalInfo* ch_info)
{
  int ret = OB_SUCCESS;
  bool need_process_dtl = need_receive_channel_map_via_dtl(child_dfo_id);

  LOG_TRACE("get_receive_data_ch", K(need_process_dtl), K(child_dfo_id));

  ObSqcLeaderTokenGuard guard(leader_token_lock_);
  if (guard.hold_token()) {
    do {
      if (need_process_dtl) {
        ret = process_dtl_msg(timeout_ts);
      }

      LOG_TRACE("process dtl msg done", K(ret));
      // After receiving all the news, focus on doing your own task
      // see if the receive channel map you expect has been received
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sqc_ctx_.receive_data_ch_provider_.get_data_ch_nonblock(
                child_dfo_id, sqc_id, task_id, timeout_ts, task_ch_set, ch_info))) {
          if (OB_EAGAIN == ret) {
            // If there is no msg in the provider,
            // and there is no need to fetch data through dtl,
            // it means that there is a logic error
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
    LOG_TRACE("ready to block wait get_data_ch", K(child_dfo_id));
    ret =
        sqc_ctx_.receive_data_ch_provider_.get_data_ch(child_dfo_id, sqc_id, task_id, timeout_ts, task_ch_set, ch_info);
    LOG_TRACE("block wait get_data_ch done", K(child_dfo_id), K(ret));
  }
  return ret;
}

int ObPxSQCProxy::get_part_ch_map(ObPxPartChInfo& map, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  bool need_process_dtl = need_transmit_channel_map_via_dtl();
  ObSqcLeaderTokenGuard guard(leader_token_lock_);
  if (guard.hold_token()) {
    do {
      if (need_process_dtl) {
        ret = process_dtl_msg(timeout_ts);
      }
      // After receiving all the news, focus on doing your own task
      // see if the receive channel map you expect has been received
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sqc_ctx_.transmit_data_ch_provider_.get_part_ch_map_nonblock(map, timeout_ts))) {
          if (OB_EAGAIN == ret) {
            // If there is no msg in the provider,
            // and there is no need to fetch data through dtl,
            // it means that there is a logic error
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
  return ret;
}

int ObPxSQCProxy::report_task_finish_status(int64_t task_idx, int rc)
{
  int ret = OB_SUCCESS;
  auto& tasks = sqc_ctx_.get_tasks();
  if (task_idx < 0 || task_idx >= tasks.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task idx", K(task_idx), K(tasks.count()), K(ret));
  } else {
    ObPxTask& task = tasks.at(task_idx);
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
  auto& tasks = sqc_ctx_.get_tasks();
  bool all_tasks_finish = true;
  do {
    if (timeout_ts < ObTimeUtility::current_time()) {
      ret = OB_TIMEOUT;
      break;
    }
    all_tasks_finish = true;
    ARRAY_FOREACH(tasks, idx)
    {
      ObPxTask& task = tasks.at(idx);
      if (!task.has_result()) {
        all_tasks_finish = false;
        break;
      }
    }
    ObSqcLeaderTokenGuard guard(leader_token_lock_);
    if (guard.hold_token()) {
      // If there are still tasks that have not been completed, try to receive dtl messages
      // Pay special attention, there may not be any DTL messages to receive at this time,
      // and the channel information of the task has been collected.
      // The reason for trying process_dtl_msg here is because
      // when the root thread calls check_task_finish_status,
      // there may also be slave threads waiting in get_data_ch, which need help from someone.
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
      usleep(1000);
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
  ObPxRpcInitSqcArgs& sqc_arg = sqc_arg_;
  ObSqcCtx& sqc_ctx = sqc_ctx_;
  ObPxSqcMeta& sqc = sqc_arg.sqc_;
  ObPxFinishSqcResultMsg finish_msg;
  int64_t affected_rows = 0;
  // If any task fails, it means all tasks have failed
  // The first version does not support retry
  int sqc_ret = OB_SUCCESS;
  auto& tasks = sqc_ctx.get_tasks();
  update_error_code(sqc_ret, end_ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    ObPxTask& task = tasks.at(i);
    update_error_code(sqc_ret, task.get_result());
    (void)finish_msg.task_monitor_info_array_.push_back(task.get_task_monitor_info());
    affected_rows += task.get_affected_rows();
    finish_msg.dml_row_info_.add_px_dml_row_info(task.dml_row_info_);
  }
  finish_msg.sqc_affected_rows_ = affected_rows;
  finish_msg.sqc_id_ = sqc.get_sqc_id();
  finish_msg.dfo_id_ = sqc.get_dfo_id();
  finish_msg.rc_ = sqc_ret;
  // Rewrite the error code so that the scheduler can wait for the remote schema to refresh and try again
  if (OB_SUCCESS != sqc_ret && is_schema_error(sqc_ret)) {
    finish_msg.rc_ = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
  } else if (common::OB_INVALID_ID != sqc_ctx.get_temp_table_id()) {
    finish_msg.temp_table_id_ = sqc_ctx.get_temp_table_id();
    if (OB_FAIL(finish_msg.interm_result_ids_.assign(sqc_ctx.interm_result_ids_))) {
      LOG_WARN("failed to assgin to interm result ids.", K(ret));
    } else { /*do nothing.*/
    }
  } else { /*do nothing.*/
  }

  // If session is null, rc will not be SUCCESS,
  // and it does not matter if trans_result is not set
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(sqc_arg.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null exec ctx", K(ret));
  } else if (OB_ISNULL(session = GET_MY_SESSION(*sqc_arg.exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized exec ctx without phy plan session set. Unexpected", K(ret));
  } else {
    // overwrite ret
    if (OB_FAIL(finish_msg.get_trans_result().merge_result(session->get_trans_result()))) {
      LOG_WARN("fail merge trans result",
          K(ret),
          "msg_trans_result",
          finish_msg.get_trans_result(),
          "session_trans_result",
          session->get_trans_result());
      finish_msg.rc_ = (OB_SUCCESS != sqc_ret) ? sqc_ret : ret;
    } else {
      LOG_DEBUG("report trans_result",
          "msg_trans_result",
          finish_msg.get_trans_result(),
          "session_trans_result",
          session->get_trans_result());
    }
  }

  ObDtlChannel* ch = sqc.get_sqc_channel();
  if (OB_ISNULL(ch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty channel", K(sqc), K(ret));
  } else if (OB_FAIL(ch->send(finish_msg, sqc_arg.exec_ctx_->get_physical_plan_ctx()->get_timeout_timestamp()))) {
    LOG_WARN("fail push data to channel", K(ret));
  } else if (OB_FAIL(ch->flush())) {
    LOG_WARN("fail flush dtl data", K(ret));
  }

  return ret;
}

void ObPxSQCProxy::get_self_dfo_key(ObDtlDfoKey& key)
{
  ObPxSqcMeta& sqc = sqc_arg_.sqc_;
  key.set(sqc.get_qc_server_id(), sqc.get_px_sequence_id(), sqc.get_qc_id(), sqc.get_dfo_id());
}

void ObPxSQCProxy::get_parent_dfo_key(ObDtlDfoKey& key)
{
  ObPxSqcMeta& sqc = sqc_arg_.sqc_;
  key.set(sqc.get_qc_server_id(), sqc.get_px_sequence_id(), sqc.get_qc_id(), sqc.get_parent_dfo_id());
}

bool ObPxSQCProxy::need_transmit_channel_map_via_dtl()
{
  ObPxSqcMeta& sqc = sqc_arg_.sqc_;
  return !sqc.is_prealloc_transmit_channel();
}

bool ObPxSQCProxy::need_receive_channel_map_via_dtl(int64_t child_dfo_id)
{

  ObPxSqcMeta& sqc = sqc_arg_.sqc_;
  bool via_sqc = false;
  if (sqc.is_prealloc_transmit_channel()) {
    via_sqc = (sqc.get_receive_channel_msg().get_child_dfo_id() == child_dfo_id);
  }
  return !via_sqc;
}

int ObPxSQCProxy::get_whole_msg_provider(uint64_t op_id, ObPxDatahubDataProvider*& provider)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqc_ctx_.get_whole_msg_provider(op_id, provider))) {
    SQL_LOG(WARN, "fail get provider", K(ret));
  }
  return ret;
}
