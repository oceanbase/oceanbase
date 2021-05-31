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

#include "ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_px_util.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

int ObPxTransmitChProvider::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg_ready_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    LOG_WARN("fail init cond", K(ret));
  }
  return ret;
}

int ObPxTransmitChProvider::get_data_ch_nonblock(
    const int64_t sqc_id, const int64_t task_id, int64_t timeout_ts, ObPxTaskChSet& ch_set, ObDtlChTotalInfo** ch_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_status(timeout_ts))) {
    // nop
  } else if (!msg_set_) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(ObPxChProviderUtil::inner_get_data_ch(
            msg_.get_ch_map_opt(), msg_.get_ch_sets(), msg_.get_ch_total_info(), sqc_id, task_id, ch_set, true))) {
      LOG_WARN("failed to get data channel", K(ret));
    } else if (OB_NOT_NULL(ch_info)) {
      *ch_info = &msg_.get_ch_total_info();
    }
  }
  return ret;
}

int ObPxTransmitChProvider::get_data_ch(
    const int64_t sqc_id, const int64_t task_id, int64_t timeout_ts, ObPxTaskChSet& ch_set, ObDtlChTotalInfo** ch_info)
{
  int ret = OB_SUCCESS;
  ret = wait_msg(timeout_ts);
  if (OB_SUCC(ret)) {
    if (!msg_set_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel set is empty. expect at lease one data channel for transmit op", K(ret));
    } else if (OB_FAIL(ObPxChProviderUtil::inner_get_data_ch(msg_.get_ch_map_opt(),
                   msg_.get_ch_sets(),
                   msg_.get_ch_total_info(),
                   sqc_id,
                   task_id,
                   ch_set,
                   true))) {
      LOG_WARN("failed to get data channel", K(ret));
    } else if (OB_NOT_NULL(ch_info)) {
      *ch_info = &msg_.get_ch_total_info();
    }
  }
  return ret;
}

int ObPxChProviderUtil::inner_get_data_ch(bool ch_map_opt, ObPxTaskChSets& ch_sets,
    dtl::ObDtlChTotalInfo& ch_total_info, const int64_t sqc_id, const int64_t task_id, ObPxTaskChSet& ch_set,
    bool is_transmit)
{
  int ret = OB_SUCCESS;
  if (ch_map_opt) {
    // transmit get channel
    // M: transmit task count
    // N: receive task count
    uint64_t chid = 0;
    if (is_transmit) {
      // transmit
      if (OB_FAIL(ObDtlChannelUtil::get_transmit_dtl_channel_set(sqc_id, task_id, ch_total_info, ch_set))) {
        LOG_WARN("failed to get transmit dtl channel set", K(ret));
      }
    } else {
      // receive
      if (OB_FAIL(ObDtlChannelUtil::get_receive_dtl_channel_set(sqc_id, task_id, ch_total_info, ch_set))) {
        LOG_WARN("failed to get transmit dtl channel set", K(ret));
      }
    }
  } else {
    if (ch_sets.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel set is empty. expect at lease one data channel for transmit op", K(ret));
    } else if (OB_INVALID_ID == task_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task id invalid", K(ret));
    } else if (task_id < 0 || task_id >= ch_sets.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task id is invalid", K(task_id), "cnt", ch_sets.count(), K(ret));
    } else {
      ObPxTaskChSet& tmp_ch_set = ch_sets.at(task_id);
      if (0 == tmp_ch_set.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no channel set for task",
            "tasks_ch_sets",
            ch_sets.count(),
            "task_ch_set",
            tmp_ch_set.count(),
            K(task_id),
            K(ret));
      } else if (OB_FAIL(ch_set.assign(tmp_ch_set))) {
        LOG_WARN("assign ch set failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPxTransmitChProvider::get_part_ch_map(ObPxPartChInfo& map, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ret = wait_msg(timeout_ts);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(inner_get_part_ch_map(map))) {
      LOG_WARN("failed to get part ch map", K(ret));
    }
  }
  return ret;
}

int ObPxTransmitChProvider::get_part_ch_map_nonblock(ObPxPartChInfo& map, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_status(timeout_ts))) {
    // nop
  } else if (!msg_set_) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(inner_get_part_ch_map(map))) {
      LOG_WARN("failed to get part ch map", K(ret));
    }
  }
  return ret;
}

int ObPxTransmitChProvider::inner_get_part_ch_map(ObPxPartChInfo& map)
{
  int ret = OB_SUCCESS;
  if (msg_.get_ch_map_opt()) {
    // [partition_id(idx), prefix_task_count, task_id]
    for (int64_t i = 0; i < msg_.get_part_affinity_map().count() && OB_SUCC(ret); ++i) {
      ObPxPartChMapItem part_ch_item = msg_.get_part_affinity_map().at(i);
      ObPxPartChMapItem tmp_part_ch_item;
      tmp_part_ch_item.assign(part_ch_item);
      if (INT64_MAX == tmp_part_ch_item.third_) {
        // scenarios such as slave mapping and PDML random

        // the sqc to be processed in this loop. get_part_affinity_map() contains all sqc
        int64_t sqc_id = part_ch_item.second_;
        // prefix_task_counts_[sqc_id] + task_id is the global task_id corresponding
        // to a certain task of sqc_id
        ObIArray<int64_t>& receive_prefix_task_counts =
            msg_.get_ch_total_info().receive_exec_server_.prefix_task_counts_;
        int64_t sqc_task_count = 0;
        int64_t pre_sqc_task_count = receive_prefix_task_counts.at(sqc_id);
        if (sqc_id == receive_prefix_task_counts.count() - 1) {
          sqc_task_count = msg_.get_ch_total_info().receive_exec_server_.total_task_cnt_;
        } else {
          sqc_task_count = receive_prefix_task_counts.at(sqc_id + 1);
        }
        for (int64_t task_idx = pre_sqc_task_count; task_idx < sqc_task_count && OB_SUCC(ret); ++task_idx) {
          tmp_part_ch_item.second_ = task_idx;  // global task idx
          if (OB_FAIL(map.part_ch_array_.push_back(tmp_part_ch_item))) {
            LOG_WARN("failed to push back part ch item", K(ret));
          } else {
            LOG_DEBUG("debug partition map", K(tmp_part_ch_item));
          }
        }
        LOG_DEBUG("debug get partition map", K(map.part_ch_array_));
      } else {
        // PK map and PDML map:
        // [partition_id(idx), prefiex_task_count + sqc_task_id(sqc_worker_id)]
        tmp_part_ch_item.second_ = tmp_part_ch_item.second_ + tmp_part_ch_item.third_;
        if (OB_FAIL(map.part_ch_array_.push_back(tmp_part_ch_item))) {
          LOG_WARN("failed to push back part ch item", K(ret));
        } else {
          LOG_DEBUG("debug get partition map", K(map.part_ch_array_));
        }
      }
    }
  } else {
    if (OB_FAIL(map.part_ch_array_.assign(msg_.get_part_affinity_map()))) {
      LOG_WARN("fail to assign part ch map", K(ret));
    } else {
      LOG_DEBUG("debug get partition map", K(map.part_ch_array_));
    }
  }
  return ret;
}

int ObPxTransmitChProvider::wait_msg(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  int64_t wait_count = 0;
  ObThreadCondGuard guard(msg_ready_cond_);
  while (!msg_set_) {
    if (!msg_set_) {
      // wait 1 ms or notified.
      msg_ready_cond_.wait_us(1000);

      wait_count++;
      // trace log each 100ms
      if (0 == wait_count % 100) {
        LOG_TRACE("wait for data channel ready", K(wait_count), K(lbt()));
      }
      if (OB_UNLIKELY(IS_INTERRUPTED())) {
        // interrupt error handling
        // overwrite ret
        ObInterruptCode code = GET_INTERRUPT_CODE();
        ret = code.code_;
        LOG_WARN("transmit channel provider wait msg loop is interrupted", K(code), K(ret));
        break;
      }
    } else {
      LOG_TRACE("[CMD] wait msg ok", K(wait_count), K(ret));
    }
    if (timeout_ts <= ObTimeUtility::current_time()) {
      ret = OB_TIMEOUT;
      LOG_TRACE("wait for data channel fail", K(wait_count), K(lbt()), K(ret));
      break;
    }
  }
  return ret;
}

int ObPxTransmitChProvider::add_msg(const ObPxTransmitDataChannelMsg& msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg_.assign(msg))) {
    LOG_WARN("fail assign msg", K(msg), K(ret));
  } else {
    ObThreadCondGuard guard(msg_ready_cond_);
    msg_set_ = true;
    msg_ready_cond_.broadcast();
    LOG_TRACE("set transmit data ch msg to provider done", K(msg));
  }
  return ret;
}

int ObPxTransmitChProvider::check_status(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_INTERRUPTED())) {
    // interrupt error handling
    // overwrite ret
    ObInterruptCode code = GET_INTERRUPT_CODE();
    ret = code.code_;
    LOG_WARN("received a interrupt", K(code), K(ret));
  } else if (timeout_ts <= ObTimeUtility::current_time()) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout and abort", K(timeout_ts), K(ret));
  }
  return ret;
}

int ObPxReceiveChProvider::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg_ready_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    LOG_WARN("fail init cond", K(ret));
  } else if (OB_FAIL(reserve_msg_set_array_size(MSG_SET_DEFAULT_SIZE))) {
    LOG_WARN("fail to reserve msg set array", K(ret));
  }
  return ret;
}
int ObPxReceiveChProvider::reserve_msg_set_array_size(int64_t size)
{
  int ret = OB_SUCCESS;
  // because the receive provider needs child_dfo_id as an index to determine
  // whether to write control messages, the number and id of child_dfo are unknown
  // indexing by array is more efficient
  // dynamic reserve array size according to child_dfo_id value
  int64_t pre_count = msg_set_.count();
  if (OB_FAIL(msg_set_.prepare_allocate(size))) {
    LOG_WARN("fail to reserve array size", K(ret), K(size));
  } else {
    for (int i = pre_count; i < msg_set_.count(); ++i) {
      msg_set_.at(i) = false;
    }
  }
  return ret;
}

int ObPxReceiveChProvider::get_data_ch_nonblock(const int64_t child_dfo_id, const int64_t sqc_id, const int64_t task_id,
    int64_t timeout_ts, ObPxTaskChSet& ch_set, ObDtlChTotalInfo* ch_info)
{
  int ret = OB_SUCCESS;
  bool found = false;

  if (child_dfo_id < 0 || child_dfo_id >= ObDfo::MAX_DFO_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid msg", K(child_dfo_id), K(ret));
  } else if (OB_FAIL(check_status(timeout_ts))) {
    // nop
  } else {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    ARRAY_FOREACH_X(msgs_, idx, cnt, OB_SUCC(ret) && !found)
    {
      if (msgs_.at(idx).get_child_dfo_id() == child_dfo_id) {
        found = true;
        ObPxReceiveDataChannelMsg& msg = msgs_.at(idx);
        if (OB_FAIL(ObPxChProviderUtil::inner_get_data_ch(
                msg.get_ch_map_opt(), msg.get_ch_sets(), msg.get_ch_total_info(), sqc_id, task_id, ch_set, false))) {
          LOG_WARN("fail to copy channel info", K(child_dfo_id), K(idx), K(cnt), K(ret));
        } else if (OB_NOT_NULL(ch_info)) {
          // deep copy here, because push_back will change the address
          *ch_info = msg.get_ch_total_info();
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObPxReceiveChProvider::get_data_ch(const int64_t child_dfo_id, const int64_t sqc_id, const int64_t task_id,
    int64_t timeout_ts, ObPxTaskChSet& ch_set, ObDtlChTotalInfo* ch_info)
{
  int ret = OB_SUCCESS;
  bool found = false;

  if (child_dfo_id < 0 || child_dfo_id >= ObDfo::MAX_DFO_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid msg", K(child_dfo_id), K(ret));
  } else {
    ret = wait_msg(child_dfo_id, timeout_ts);
    if (OB_SUCC(ret)) {
      ObLockGuard<ObSpinLock> lock_guard(lock_);
      ARRAY_FOREACH_X(msgs_, idx, cnt, OB_SUCC(ret) && !found)
      {
        if (msgs_.at(idx).get_child_dfo_id() == child_dfo_id) {
          found = true;
          ObPxReceiveDataChannelMsg& msg = msgs_.at(idx);
          if (OB_FAIL(ObPxChProviderUtil::inner_get_data_ch(
                  msg.get_ch_map_opt(), msg.get_ch_sets(), msg.get_ch_total_info(), sqc_id, task_id, ch_set, false))) {
            LOG_WARN("fail to copy channel info", K(child_dfo_id), K(idx), K(cnt), K(ret));
          } else if (OB_NOT_NULL(ch_info)) {
            // deep copy here, because push_back will change the address
            *ch_info = msg.get_ch_total_info();
          }
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no receive ch found for dfo", K(child_dfo_id), K(ret));
    }
  }
  return ret;
}

int ObPxReceiveChProvider::add_msg(const ObPxReceiveDataChannelMsg& msg)
{
  int ret = OB_SUCCESS;
  int64_t child_dfo_id = msg.get_child_dfo_id();
  if (child_dfo_id < 0 || child_dfo_id >= ObDfo::MAX_DFO_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid msg", K(msg), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    if (OB_FAIL(msgs_.push_back(msg))) {
      LOG_WARN("fail assign msg", K(ret));
    } else if (child_dfo_id >= msg_set_.count() && OB_FAIL(reserve_msg_set_array_size(child_dfo_id * 2 + 1))) {
      LOG_WARN("fail to reserve msg set array size", K(ret));
    } else {
      msg_set_[child_dfo_id] = true;
    }
  }
  if (OB_SUCC(ret)) {
    msg_ready_cond_.broadcast();
    LOG_TRACE("set receive data ch msg to provider done", K(msg));
  }
  return ret;
}

int ObPxReceiveChProvider::wait_msg(int64_t child_dfo_id, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  int64_t wait_count = 0;
  if (child_dfo_id >= msg_set_.count()) {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    if (child_dfo_id < msg_set_.count()) {
      /*do nothing*/
    } else if (OB_FAIL(reserve_msg_set_array_size(child_dfo_id * 2 + 1))) {
      LOG_WARN("fail to reserve msg set array", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    while (!msg_set_[child_dfo_id]) {
      msg_ready_cond_.lock();
      if (!msg_set_[child_dfo_id]) {
        msg_ready_cond_.wait_us(1 * 1000); /* 1 ms */
      }
      msg_ready_cond_.unlock();
      if (!msg_set_[child_dfo_id]) {
        wait_count++;
        if (0 == wait_count % 1000) {
          LOG_TRACE("wait for data channel ready", K(child_dfo_id), K(wait_count), K(lbt()));
        }
        if (OB_UNLIKELY(IS_INTERRUPTED())) {
          // interrupt error handling
          // overwrite ret
          ObInterruptCode code = GET_INTERRUPT_CODE();
          ret = code.code_;
          LOG_WARN(
              "receive channel provider wait msg loop is interrupted", K(child_dfo_id), K(wait_count), K(code), K(ret));
          break;
        }
      }
      if (timeout_ts <= ObTimeUtility::current_time()) {
        ret = OB_TIMEOUT;
        LOG_WARN("child dfo channel not ready, timeout and abort", K(child_dfo_id), K(wait_count), K(lbt()), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObPxReceiveChProvider::check_status(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_INTERRUPTED())) {
    // interrupt error handling
    // overwrite ret
    ObInterruptCode code = GET_INTERRUPT_CODE();
    ret = code.code_;
    LOG_WARN("received a interrupt", K(code), K(ret));
  } else if (timeout_ts <= ObTimeUtility::current_time()) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout and abort", K(timeout_ts), K(ret));
  }
  return ret;
}
