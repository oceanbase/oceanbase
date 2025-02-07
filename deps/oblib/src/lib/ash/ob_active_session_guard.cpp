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

#define USING_LOG_PREFIX SHARE

#include "lib/ash/ob_active_session_guard.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/stat/ob_diagnostic_info_container.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "lib/stat/ob_diagnostic_info_util.h"

using namespace oceanbase::common;

// a sample would be taken place up to 20ms after ash iteration begins.
// if sample time is above this threshold, mean ash execution too slow
constexpr int64_t ash_iteration_time = 40000;   // 40ms

extern uint64_t lib_get_cpu_khz();

void ObActiveSessionStat::fixup_last_stat(ObWaitEventDesc &desc)
{
  if (fixup_index_ != -1) {
    if (OB_LIKELY(fixup_ash_buffer_.is_valid())) {
      fixup_ash_buffer_->fixup_stat(fixup_index_, desc);
      fixup_ash_buffer_.reset();
      fixup_index_ = -1;
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fixup index with no fixup buffer.", K_(fixup_index), KPC(this));
    }
  }
}

// con only be called from ash sample thread(mutex protected).
void ObActiveSessionStat::set_fixup_buffer(common::ObSharedGuard<ObAshBuffer> &ash_buffer)
{
  // fixup buffer can only hold one ash buffer. Cannot switch it.
  // Otherwise core would happened when set_fixup_buffer coincide with session's reset fixup buffer.
  if (OB_LIKELY(!fixup_ash_buffer_.is_valid())) {
    fixup_ash_buffer_ = ash_buffer;
  }
}

void ObActiveSessionStat::set_sess_active()
{
  if (!is_active_session_) {
    is_active_session_ = true;
    accumulate_tm_idle_time();
    if (trace_id_.is_invalid()) {
      trace_id_ = *common::ObCurTraceId::get_trace_id();
    }
  }
}

void ObActiveSessionStat::set_sess_inactive()
{
  last_inactive_ts_ = rdtsc();
  is_active_session_ = false;
}

void ObActiveSessionStat::accumulate_tm_idle_time()
{
  if (last_inactive_ts_ > 0) {
    // When set_sess_inactive() is called, there is some time left after last_ts_. So we mark it as
    // extra time to calculat in next round of calc_db_time when session is active again.
    const int64_t cur = rdtsc();
    tm_idle_time_ += (cur - last_inactive_ts_) * 1000 / lib_get_cpu_khz();
  }
}

void ObActiveSessionStat::calc_db_time(
    ObDiagnosticInfo *di, const int64_t sample_time, const int64_t tsc_sample_time)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObActiveSessionStat &stat = di->get_ash_stat();
    const int64_t delta_time = (tsc_sample_time - stat.last_touch_ts_) * 1000 / lib_get_cpu_khz();
    if (OB_UNLIKELY(delta_time <= 0)) {
      // ash sample happened before set_session_active
      if (delta_time < -ash_iteration_time) {
        LOG_INFO("ash sample happened before set_session_active.", K(delta_time), K(sample_time), K_(stat.last_touch_ts));
      }
      stat.delta_time_ = 0;
      stat.delta_cpu_time_ = 0;
      stat.delta_db_time_ = 0;
    } else {
      const uint64_t cur_wait_begin_ts = stat.wait_event_begin_ts_;
      const int64_t cur_event_no = stat.retry_wait_event_no_ > 0 ? stat.retry_wait_event_no_ : stat.event_no_;
      if (OB_UNLIKELY(cur_wait_begin_ts != 0 && cur_event_no != 0)) {
        // has unfinished wait event
        stat.wait_event_begin_ts_ = tsc_sample_time;
        const uint64_t cur_wait_time =
            (tsc_sample_time - cur_wait_begin_ts) * 1000 / lib_get_cpu_khz();
        if (OB_WAIT_EVENTS[cur_event_no].wait_class_ != ObWaitClassIds::IDLE) {
          stat.total_non_idle_wait_time_ += cur_wait_time;
        } else {
          stat.total_idle_wait_time_ += cur_wait_time;
        }
      }
      const int64_t delta_non_idle_wait_time = stat.total_non_idle_wait_time_ - stat.prev_non_idle_wait_time_;
      const int64_t delta_idle_wait_time = stat.total_idle_wait_time_ - stat.prev_idle_wait_time_;
      if (OB_UNLIKELY(delta_time < delta_non_idle_wait_time + delta_idle_wait_time)) {
        /**
         * Because we take sample_time_ in the begining of ash sample iteration.
         * So each session's sample actually taken place after sample_time_. Resulting in wait_time >
         * delta time. Therefore a negative delta db time would happened.
         * Which is fine. the negative db time got fixed up in the next round of calc_db_time
         */
        stat.delta_time_ = 0;
        stat.delta_db_time_ = 0;
        stat.delta_cpu_time_ = 0;
      } else {
        stat.prev_non_idle_wait_time_ = stat.total_non_idle_wait_time_;
        stat.prev_idle_wait_time_ = stat.total_idle_wait_time_;
        stat.delta_time_ = delta_time;
        stat.delta_db_time_ = delta_time - stat.tm_idle_time_ - delta_idle_wait_time;
        stat.delta_cpu_time_ = stat.delta_db_time_ - delta_non_idle_wait_time;
        if (stat.delta_db_time_ < 0 || stat.delta_cpu_time_ < 0) {
          //When delta_db_time < 0 or delta_cpu_time < 0,
          //it indicates that the sampled db_time is invalid.
          //A possible reason for this is that the ASH sampling encountered some concurrency issues.
          //Therefore, the sampled delta time will be discarded and not recorded.
          stat.delta_time_ = 0;
          stat.delta_db_time_ = 0;
          stat.delta_cpu_time_ = 0;
        }
        // no need to lock, only this function modifies time model related stats.
        if (stat.session_type_ == ObActiveSessionStatItem::SessionType::BACKGROUND) {
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_BKGD_TIME, delta_time);
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_BKGD_CPU, stat.delta_cpu_time_);
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_BKGD_DB_TIME, stat.delta_db_time_);
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_BKGD_NON_IDLE_WAIT_TIME, delta_non_idle_wait_time);
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_BKGD_IDLE_WAIT_TIME, delta_idle_wait_time);
        } else {
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_DB_TIME, stat.delta_db_time_);
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_DB_CPU, stat.delta_cpu_time_);
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_NON_IDLE_WAIT_TIME, delta_non_idle_wait_time);
          di->add_stat(ObStatEventIds::SYS_TIME_MODEL_IDLE_WAIT_TIME, delta_idle_wait_time);
        }
      }
    }
    stat.tm_idle_time_ = 0;
    stat.last_touch_ts_ = tsc_sample_time;
  }
}
void ObActiveSessionStat::calc_retry_wait_event(ObActiveSessionStat &stat, const int64_t sample_time)
{
  int ret = OB_SUCCESS;
  int64_t retry_wait_event_no = stat.retry_wait_event_no_;
  if (retry_wait_event_no > 0 && stat.need_calc_wait_event_end_) {
    LOG_DEBUG("[retry wait event] end query retry wait event", K(ret), K(stat.session_id_),
                  K(sample_time), K(stat.curr_query_start_time_), K(stat.last_query_exec_use_time_us_));
    if (stat.curr_query_start_time_ > 0 &&
          sample_time - stat.curr_query_start_time_ > stat.last_query_exec_use_time_us_)  {
      // end query retry wait event
      stat.end_retry_wait_event();
      if (retry_wait_event_no == ObWaitEventIds::ROW_LOCK_WAIT && stat.need_calc_wait_event_end_) {
        stat.block_sessid_ = 0;
      }
    } else {
      LOG_DEBUG("[retry wait event] end query retry wait event", K(ret), K(stat.session_id_),
                    K(sample_time), K(stat.curr_das_task_start_time_), K(stat.last_das_task_exec_use_time_us_));
      if (stat.curr_das_task_start_time_ > 0 &&
            sample_time - stat.curr_das_task_start_time_ > stat.last_das_task_exec_use_time_us_)  {
        // end das retry wait event
        stat.end_retry_wait_event();
      }
    }
  }
}

void ObActiveSessionStat::begin_retry_wait_event(const int64_t retry_wait_event_no,
                                                 const int64_t retry_wait_event_p1,
                                                 const int64_t retry_wait_event_p2,
                                                 const int64_t retry_wait_event_p3)
{
#ifdef ENABLE_DEBUG_LOG
  if (retry_wait_event_no_ == ObWaitEventIds::ROW_LOCK_WAIT) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "ASH's retry_wait_event concurrency may occur ", K(retry_wait_event_no_), K(retry_wait_event_no), K(retry_wait_event_p1));
  }
#endif
  retry_wait_event_no_ = retry_wait_event_no;
  retry_wait_event_p1_ = retry_wait_event_p1;
  retry_wait_event_p2_ = retry_wait_event_p2;
  retry_wait_event_p3_ = retry_wait_event_p3;
}

void ObActiveSessionStat::end_retry_wait_event()
{
  retry_wait_event_no_ = 0;
  retry_wait_event_p1_ = 0;
  retry_wait_event_p2_ = 0;
  retry_wait_event_p3_ = 0;
  retry_plan_line_id_  = -1;
  need_calc_wait_event_end_ = false;
}

void ObActiveSessionStat::begin_row_lock_wait_event()
{
  if (retry_wait_event_no_ == ObWaitEventIds::ROW_LOCK_WAIT) {
    need_calc_wait_event_end_ = false;
    // After entering the wait lock queue, query may wait for a long time.
    // ASH will no longer calculate the end of the ROW_LOCK waiting event in this case.
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid retry wait event no", K(ret), K(retry_wait_event_no_));
  }
}

void ObActiveSessionStat::end_row_lock_wait_event()
{
  if (retry_wait_event_no_ == ObWaitEventIds::ROW_LOCK_WAIT) {
    end_retry_wait_event();
    block_sessid_ = 0;
  } else {
    block_sessid_ = 0;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "error wait event no", K(ret), K(retry_wait_event_no_));
  }
}

void ObActiveSessionGuard::set_sess_active()
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (OB_NOT_NULL(di)) {
    di->get_ash_stat().set_sess_active();
  }
}

void ObActiveSessionGuard::set_sess_inactive()
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (OB_NOT_NULL(di)) {
    di->get_ash_stat().set_sess_inactive();
  }
}

const ObActiveSessionStatItem &ObAshBuffer::get(int64_t pos) const
{
  return buffer_[pos];
}
int64_t ObAshBuffer::copy_from_ash_buffer(const ObActiveSessionStatItem &stat)
{
  int64_t idx = (write_pos_++ + buffer_.size()) % buffer_.size();
  MEMCPY(&buffer_[idx], &stat, sizeof(ObActiveSessionStatItem));
  buffer_[idx].id_ = write_pos_;
  if (0 == write_pos_ % WR_ASH_SAMPLE_INTERVAL) {
    buffer_[idx].is_wr_sample_ = true;
  }
  return idx;
}

int64_t ObAshBuffer::append(const ObActiveSessionStat &stat)
{
  // TODO: optimize performance, eliminate '%'
  OB_ASSERT(stat.wait_time_ == 0);
  int64_t idx = (write_pos_++ + buffer_.size()) % buffer_.size();
  MEMCPY(&buffer_[idx], &stat, sizeof(ObActiveSessionStatItem));
  buffer_[idx].id_ = write_pos_;
  if (0 == write_pos_ % WR_ASH_SAMPLE_INTERVAL) {
    buffer_[idx].is_wr_sample_ = true;
  }
  if (stat.retry_wait_event_no_ > 0) {
    // We think retry wait event is more import than normal wait event
    buffer_[idx].event_no_ = stat.retry_wait_event_no_;
    buffer_[idx].p1_ = stat.retry_wait_event_p1_;
    buffer_[idx].p2_ = stat.retry_wait_event_p2_;
    buffer_[idx].p3_ = stat.retry_wait_event_p3_;
    buffer_[idx].plan_line_id_ = stat.retry_plan_line_id_;
  }
  return idx;
}

void ObAshBuffer::fixup_stat(int64_t index, const ObWaitEventDesc &desc)
{
  if (OB_UNLIKELY(index < 0 || index >= write_pos_)) {
    // index invalid for fixup, do noting.
  } else {
    ObActiveSessionStatItem &stat = buffer_[(index + buffer_.size()) % buffer_.size()];
    if (OB_UNLIKELY(stat.wait_time_ != 0 || stat.event_no_ != desc.event_no_)) {
      // wait event invalid for fix up, do noting.
    } else {
      stat.wait_time_ = desc.wait_time_;
      stat.p1_ = desc.p1_;
      stat.p2_ = desc.p2_;
      stat.p3_ = desc.p3_;
#if !defined(NDEBUG)
      const char *bt = lbt();
      int64_t size = std::min(sizeof(stat.bt_) - 1, STRLEN(bt));
      MEMCPY(stat.bt_, bt, size);
      stat.bt_[size] = '\0';
#endif
    }
  }
}

void ObAshBuffer::set_read_pos(int64_t pos)
{
  if (OB_UNLIKELY(pos >= write_pos_ || pos < 0)) {
    // index invalid for read_pos, do nothing.
  } else {
    read_pos_ = pos;
  }
}

ObBackgroundSessionIdGenerator &ObBackgroundSessionIdGenerator::get_instance() {
  static ObBackgroundSessionIdGenerator the_one;
  return the_one;
}
// |<---------------------------------64bit---------------------------->|
// 0b                        32b                                      64b
// +---------------------------+----------------------------------------+
// |         Local Seq         |                Zero                    |
// +---------------------------+----------------------------------------+
//
//Local Seq: 一个server可用连接数，目前单台server最多有INT32_MAX个连接;
//Zero     : 置零，保留字段，用于和sql_session做区分
uint64_t ObBackgroundSessionIdGenerator::get_next_sess_id() {
  uint64_t sessid = 0;
  const uint64_t local_seq = static_cast<uint32_t>(ATOMIC_AAF(&local_seq_, 1));
  sessid |= (local_seq << 32);
  LOG_DEBUG("succ to generate background session id", K(local_seq), K(sessid));

  return sessid;
}

bool ObBackgroundSessionIdGenerator::is_background_session_id(uint64_t session_id)
{
  return session_id & 0xFFFFF;
}
