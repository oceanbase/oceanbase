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

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl_channel_loop.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/diagnosis/ob_sql_monitor_statname.h"
#include "observer/omt/ob_th_worker.h"
#include "share/ob_occam_time_guard.h"
#include "sql/engine/px/ob_px_util.h"

using namespace oceanbase::common;

namespace oceanbase {
using namespace omt;
namespace sql {
namespace dtl {

ObDtlChannelLoop::ObDtlChannelLoop()
    : proc_map_(),
      interrupt_proc_(NULL),
      chans_(),
      next_idx_(0),
      last_msg_type_(static_cast<uint16_t>(ObDtlMsgType::MAX)),
      cond_(ObWaitEventIds::PX_LOOP_COND_WAIT),
      ignore_interrupt_(false),
      tenant_id_(UINT64_MAX),
      timeout_(INT64_MAX),
      spin_lock_(common::ObLatchIds::DTL_CHANNEL_LIST_LOCK),
      mock_addr_(),
      sentinel_node_(1, 0, mock_addr_, ObDtlChannel::DtlChannelType::LOCAL_CHANNEL),
      n_first_no_data_(0),
      op_monitor_info_(default_op_monitor_info_),
      first_data_get_(false),
      process_func_(last_msg_type_, proc_map_),
      use_interm_result_(false),
      eof_channel_cnt_(0),
      loop_times_(0),
      begin_wait_time_(0),
      process_query_time_(0),
      last_dump_channel_time_(0),
      query_timeout_ts_(0)
{
  op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::DTL_LOOP_TOTAL_MISS_AFTER_DATA;
  op_monitor_info_.otherstat_6_id_ = ObSqlMonitorStatIds::DTL_LOOP_TOTAL_MISS;
  sentinel_node_.prev_link_ = &sentinel_node_;
  sentinel_node_.next_link_ = &sentinel_node_;
}


ObDtlChannelLoop::ObDtlChannelLoop(ObMonitorNode &op_monitor_info)
    : proc_map_(),
      interrupt_proc_(NULL),
      chans_(),
      next_idx_(0),
      last_msg_type_(static_cast<uint16_t>(ObDtlMsgType::MAX)),
      cond_(ObWaitEventIds::PX_LOOP_COND_WAIT),
      ignore_interrupt_(false),
      tenant_id_(UINT64_MAX),
      timeout_(INT64_MAX),
      spin_lock_(common::ObLatchIds::DTL_CHANNEL_LIST_LOCK),
      mock_addr_(),
      sentinel_node_(1, 0, mock_addr_, ObDtlChannel::DtlChannelType::LOCAL_CHANNEL),
      n_first_no_data_(0),
      op_monitor_info_(op_monitor_info),
      first_data_get_(false),
      process_func_(last_msg_type_, proc_map_),
      use_interm_result_(false),
      eof_channel_cnt_(0),
      loop_times_(0),
      begin_wait_time_(0),
      process_query_time_(0),
      last_dump_channel_time_(0),
      query_timeout_ts_(0)
{
  op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::DTL_LOOP_TOTAL_MISS_AFTER_DATA;
  op_monitor_info_.otherstat_6_id_ = ObSqlMonitorStatIds::DTL_LOOP_TOTAL_MISS;
  sentinel_node_.prev_link_ = &sentinel_node_;
  sentinel_node_.next_link_ = &sentinel_node_;
}

void ObDtlChannelLoop::notify(ObDtlChannel &chan)
{
  UNUSED(chan);
  add_last_data_list(&chan);
  cond_.signal();
}

int ObDtlChannelLoop::unregister_channel(ObDtlChannel &chan)
{
  int ret = OB_SUCCESS;
  int find_idx = -1;
  for (int i = 0; i != chans_.count(); i++) {
    if (&chan == chans_[i]) {
      find_idx = i;
      break;
    }
  }
  if (find_idx >= 0) {
    ret = chans_.remove(find_idx);
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("channel not exist, removed already?", K(chan), K(ret));
  }
  return ret;
}


int ObDtlChannelLoop::unregister_all_channel()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = chans_.count() - 1; i >= 0; --i) {
    if (OB_SUCCESS != (tmp_ret = chans_.remove(i))) {
      if (OB_SUCCESS == ret) {
        ret = tmp_ret; // record fisrt failure reason, but continue to remove
      }
      continue;
    }
  }
  return ret;
}

int ObDtlChannelLoop::find(ObDtlChannel* ch, int64_t &out_idx)
{
  int ret = OB_SUCCESS;
  out_idx = OB_INVALID_ID;
  ARRAY_FOREACH_X(chans_, idx, cnt, OB_INVALID_ID == out_idx) {
    if (ch == chans_.at(idx)) {
      out_idx = idx;
    }
  }
  if (OB_INVALID_ID == out_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("channel not found", K(ch), KP(ch->get_id()), K(ch->get_peer()), K(out_idx));
  }
  return ret;
}

int ObDtlChannelLoop::ObDtlChannelLoopProc::process(
  const ObDtlLinkedBuffer &buffer, bool &transferred)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObDtlMsgHeader header;
  if (buffer.is_data_msg()) {
    last_msg_type_ = static_cast<int16_t>(ObDtlMsgType::PX_NEW_ROW);
    if (last_msg_type_ >= static_cast<int16_t>(ObDtlMsgType::MAX)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_DTL_LOG(WARN, "channel has received message with unknown type", K(last_msg_type_));
    } else if (proc_map_[last_msg_type_] == nullptr){
      ret = OB_INVALID_ARGUMENT;
      SQL_DTL_LOG(WARN, "channel has received message without processor", K(last_msg_type_));
    } else if (OB_FAIL(proc_map_[last_msg_type_]->process(buffer, transferred))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("process message in channel fail", K(ret), K(last_msg_type_));
      } else {
        LOG_TRACE("channel loop receive ob iter end");
      }
    }
  } else if (OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(buffer, header))) {
    // 这里可能是OB_ITER_END，不能打WARN日志.
    LOG_TRACE("failed to deserialize msg", K(ret), K(&buffer), K(lbt()));
  } else {
    last_msg_type_ = header.type_;
    if (proc_map_[header.type_] == nullptr) {
      ret = OB_INVALID_ARGUMENT;
      SQL_DTL_LOG(WARN, "channel has received message without processor",
                  K(header), K(ret));
    } else if (OB_FAIL(proc_map_[header.type_]->process(buffer, transferred))) {
      LOG_WARN("process message in channel fail",
              K(header), K(ret));
    }
  }
  return ret;
}

int ObDtlChannelLoop::process_base(ObIDltChannelLoopPred *pred, int64_t &hinted_channel, int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (chans_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("channel hasn't set", K(ret));
  } else {
    // 处理思路：
    // 1. 轮询所有 channel，如果能收到一个数据，则成功
    // 2. 如果没有任何消息，则等待消息唤醒
    // 3. 被唤醒后，再轮询所有 channel 是否有数据可处理，如果没有则返回 EAGAIN
    //
    uint32_t wait_key = cond_.get_key();
    if (OB_UNLIKELY(nullptr != pred)) {
      if (OB_UNLIKELY(OB_INVALID_INDEX_INT64 != hinted_channel)) {
        next_idx_ = hinted_channel % chans_.count();
      }
      ret = process_channels(pred, hinted_channel);
    } else {
      if (OB_UNLIKELY(OB_INVALID_INDEX_INT64 != hinted_channel)) {
        int64_t chan_cnt = chans_.count();
        bool last_row_in_buffer = false;
        // try best to process hinted channel
        next_idx_ = hinted_channel % chan_cnt;
        hinted_channel = OB_INVALID_INDEX_INT64;
        if (OB_SUCC(chans_[next_idx_]->process1(&process_func_, 0, last_row_in_buffer))) {
          hinted_channel = next_idx_;
        } else if (OB_EAGAIN == ret) {
          ret = process_channel(hinted_channel);
        }
      } else {
        ret = process_channel(hinted_channel);
      }
    }
    if (OB_SUCC(ret)) {
      // succ process one channel
    } else if (OB_EAGAIN == ret) {
      begin_wait_time_counting();
      // 通过TPCH 100G Q1测试发现，轮询时间间隔会导致性能有差异，轮询时间间隔较短
      // 会占用大量CPU，导致CPU利用率不高，从而影响性能
      if (timeout > 0) {
        cond_.wait(wait_key, timeout);
      } else if (OB_UNLIKELY(INT64_MAX == timeout_)) {
        ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
        if (tenant_config.is_valid()) {
          timeout_ = tenant_config->_parallel_server_sleep_time * 1000;
          cond_.wait(wait_key, timeout_);
          LOG_DEBUG("channel loop polling time", K(timeout_), K(timeout), K(ret));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to init tenant config", K(tenant_id_), K(ret));
        }
      } else {
        cond_.wait(wait_key, timeout_);
      }
      end_wait_time_counting();
    } else {
      LOG_WARN("fail process channel", K(ret));
    }

    ++loop_times_;
    if ((loop_times_ & (INTERRUPT_CHECK_TIMES - 1)) == 0) {
      last_dump_channel_time_ = last_dump_channel_time_ < process_query_time_ ? process_query_time_ : last_dump_channel_time_;
      int64_t curr_time = ::oceanbase::common::ObTimeUtility::current_time();
      if (OB_UNLIKELY(curr_time - last_dump_channel_time_ >= static_cast<int64_t> (100_s))) {
        if (0 != process_query_time_) {
          LOG_INFO("dump channel loop info for query which active for more than 100 seconds",
                  K(process_query_time_), K(curr_time),
                  K(timeout), K(timeout_), K(query_timeout_ts_));
        }
        last_dump_channel_time_ = curr_time;
        int64_t idx = -1;
        int64_t last_in_msg_time = INT64_MAX;
        // Find a channel that has not received data for the longest time
        for (int64_t i = 0; i < chans_.count(); ++i) {
          if (nullptr != chans_.at(i)) {
            ObDtlBasicChannel *channel = static_cast<ObDtlBasicChannel *> (chans_.at(i));
            if (channel->get_op_metric().get_last_in_ts() < last_in_msg_time) {
              last_in_msg_time = channel->get_op_metric().get_last_in_ts();
              idx = i;
            }
          }
        }
        if (-1 == idx) {
          LOG_WARN("no channel exists");
        } else if (0 != process_query_time_) {
          ObDtlBasicChannel *channel = static_cast<ObDtlBasicChannel *> (chans_.at(idx));
          LOG_INFO("dump channel info for query which active for more than 100 seconds",
                   K(idx), K(channel->get_id()),
                   K(channel->get_peer_id()), K(channel->get_peer()),
                   K(channel->get_op_metric()));
        }
      }
    }
    if ((loop_times_ & (SERVER_ALIVE_CHECK_TIMES - 1)) == 0) {
      for (int64_t i = 0; i < chans_.count(); ++i) {
        if (nullptr != chans_.at(i)) {
          ObDtlBasicChannel *ch = static_cast<ObDtlBasicChannel *> (chans_.at(i));
          if (OB_UNLIKELY(ObPxCheckAlive::is_in_blacklist(ch->get_peer(),
                              get_process_query_time()))) {
            ret = OB_RPC_CONNECT_ERROR;
            LOG_WARN("peer no in communication, maybe crashed", K(ret), K(ch->get_peer()),
                      K(static_cast<int64_t>(GCONF.cluster_id)));
          }
        }
      }
    }
    if (OB_UNLIKELY(OB_RPC_CONNECT_ERROR == ret)) {
    } else if (ignore_interrupt_) {
      // do nothing.
    } else if ((loop_times_ & (INTERRUPT_CHECK_TIMES - 1)) == 0 && OB_UNLIKELY(IS_INTERRUPTED())) {
      // 中断错误处理
      // overwrite ret
      ObInterruptCode &code = GET_INTERRUPT_CODE();
      ret = code.code_;
      LOG_WARN("message loop is interrupted", K(code), K(ret));
    }
  }
  return ret;
}

// 目前使用process处理数据主要可以分为3类
// 1）fifo，纯拿数据，不计较数据顺序
// 2）merge receive，优先拿排序列，没有，则任意数据，类似1）
//   1)+2) -> 使用process_one
// 3）merge sort coord，pred限制拿的数据，如要求是控制消息或者排序对应的channel数据
//      3）-> 使用process_one_if
int ObDtlChannelLoop::process_one(int64_t &hinted_channel, int64_t timeout)
{
  int ret = OB_SUCCESS;
  ret = process_base(nullptr, hinted_channel, timeout);
  if (OB_SUCC(ret) && OB_INVALID_INDEX_INT64 == hinted_channel) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: get data from invalid channel", K(hinted_channel));
  }
  return ret;
}

int ObDtlChannelLoop::process_any(int64_t timeout)
{
  int64_t hinted_channel = OB_INVALID_INDEX_INT64;
  return process_base(nullptr, hinted_channel, timeout);
}

int ObDtlChannelLoop::process_one_if(ObIDltChannelLoopPred *pred, int64_t &ret_channel, int64_t timeout)
{
  int ret = OB_SUCCESS;
  ret_channel = OB_INVALID_INDEX_INT64;
  ret = process_base(pred, ret_channel, timeout);
  if (OB_SUCC(ret) && OB_INVALID_INDEX_INT64 == ret_channel) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: get data from invalid channel", K(ret_channel));
  }
  return ret;
}

int ObDtlChannelLoop::process_channels(ObIDltChannelLoopPred *pred, int64_t &nth_channel)
{
  int ret = OB_EAGAIN;
  ObDtlChannel *chan = nullptr;
  bool last_row_in_buffer = false;
  int64_t chan_cnt = chans_.count();
  for (int64_t i = 0; i != chan_cnt && ret == OB_EAGAIN; ++i) {
    if (next_idx_ >= chan_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect next idx", K(next_idx_), K(chan_cnt), K(ret));
    } else {
      chan = chans_[next_idx_];
      if (nullptr == pred || pred->pred_process(next_idx_, chan)) {
        if (OB_SUCC(chan->process1(&process_func_, 0, last_row_in_buffer))) {
          nth_channel = next_idx_;
          first_data_get_ = true;
          if (last_row_in_buffer) {
            // 每次接收一个channel的所有buffer
            ++next_idx_;
            if (next_idx_ >= chan_cnt) {
              next_idx_ %= chan_cnt;
            }
          }
          break;
        } else if (OB_ITER_END == ret) {
          // return which channel is end
          ret = OB_ERR_UNEXPECTED;
          nth_channel = next_idx_;
          LOG_WARN("finish get data from channel", K(nth_channel), K(ret), KP(chan->get_id()), K(chan->get_peer()));
        }
      }
      ++next_idx_;
      if (next_idx_ >= chan_cnt) {
        next_idx_ %= chan_cnt;
      }
    }
    if (first_data_get_) {
      // 第一次命中后的miss计数
      // ObSqlMonitorStatIds::DTL_LOOP_TOTAL_MISS_AFTER_DATA;
      ++op_monitor_info_.otherstat_5_value_;
    }
    // ObSqlMonitorStatIds::DTL_LOOP_TOTAL_MISS;
    ++op_monitor_info_.otherstat_6_value_;
  }
  return ret;
}

int ObDtlChannelLoop::process_channel(int64_t &nth_channel)
{
  int ret = OB_EAGAIN;
  int64_t n_times = 0;
  bool last_row_in_buffer = false;
  if (ret == OB_EAGAIN && use_interm_result_) {
    // less then chan_cnt, then probe first buffer
    ret = process_channels(nullptr, nth_channel);
  }
  ObDtlChannel *ch = sentinel_node_.next_link_;
  while (OB_EAGAIN == ret && ch != &sentinel_node_) {
    if (OB_SUCC(ch->process1(&process_func_, 0, last_row_in_buffer))) {
      nth_channel = ch->get_loop_index();
      break;
    } else if (OB_EAGAIN == ret) {
      remove_data_list(ch);
    }
    if (n_times > 100) {
      // it's maybe unexpected !
      LOG_WARN("loop times", K(n_times));
      int tmp_ret = THIS_WORKER.check_status();
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("worker interrupt", K(tmp_ret), K(ret));
        break;
      }
      ob_usleep<ObWaitEventIds::DTL_PROCESS_CHANNEL_SLEEP>(1);
    }
    ++loop_times_;
    if (ignore_interrupt_) {
    } else if ((loop_times_ & (INTERRUPT_CHECK_TIMES - 1)) == 0 && OB_UNLIKELY(IS_INTERRUPTED())) {
      ObInterruptCode &code = GET_INTERRUPT_CODE();
      ret = code.code_;
      LOG_WARN("message loop is interrupted", K(code), K(ret));
    } else if ((loop_times_ & (SERVER_ALIVE_CHECK_TIMES - 1)) == 0
               && OB_UNLIKELY(ObPxCheckAlive::is_in_blacklist(ch->get_peer(),
                              get_process_query_time()))) {
      ret = OB_RPC_CONNECT_ERROR;
      LOG_WARN("peer no in communication, maybe crashed", K(ret), K(ch->get_peer()),
                K(static_cast<int64_t>(GCONF.cluster_id)));
    }
    ch = sentinel_node_.next_link_;
    ++n_times;
  }
  return ret;
}

// for merge sort coord
// when merge sort coord can't get row for cur_channel
// then unblock blocked channel
// if not, then maybe hang
int ObDtlChannelLoop::unblock_channels(int64_t data_channel_idx)
{
  int ret = OB_SUCCESS;
  if (data_channel_idx >= chans_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: data channel idx is invalid", K(ret),
      K(data_channel_idx), K(chans_.count()));
  } else {
    ObDfcServer &dfc_server = DTL.get_dfc_server();
    ObDtlChannel *ch = chans_.at(data_channel_idx);
    if (nullptr == ch->get_dfc()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data channel, dfc is null", K(ret), K(data_channel_idx), K(chans_.count()));
    } else {
      if (OB_FAIL(dfc_server.unblock_channels(ch->get_dfc()))) {
        LOG_WARN("failed to unblock channels", K(ret));
      }
    }
  }
  return ret;
}

// It's not used now !!!
// for merge sort coord
// when merge sort coord can't get row for cur_channel
// then unblock blocked channel
// if not, then maybe hang
// only for one channel
int ObDtlChannelLoop::unblock_channel(int64_t start_data_channel_idx, int64_t dfc_channel_idx)
{
  int ret = OB_SUCCESS;
  if (start_data_channel_idx >= chans_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: data channel idx is invalid", K(ret),
      K(start_data_channel_idx), K(chans_.count()));
  } else {
    ObDfcServer &dfc_server = DTL.get_dfc_server();
    ObDtlChannel *ch = chans_.at(start_data_channel_idx);
    if (nullptr == ch->get_dfc()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data channel, dfc is null", K(ret), K(start_data_channel_idx), K(chans_.count()));
    } else {
      if (OB_FAIL(dfc_server.unblock_channel(ch->get_dfc(), dfc_channel_idx))) {
        LOG_WARN("failed to unblock channels", K(ret));
      }
    }
  }
  return ret;
}

}  // dtl
}  // sql
}  // oceanbase
