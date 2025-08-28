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
#include "lib/time/ob_tsc_timestamp.h"
#include "lib/stat/ob_diagnostic_info_util.h"
#include "sql/ob_sql_context.h"
#include "share/ash/ob_active_sess_hist_list.h"

using namespace oceanbase::common;

// a sample would be taken place up to 20ms after ash iteration begins.
// if sample time is above this threshold, mean ash execution too slow
constexpr int64_t ash_iteration_time = 40000;   // 40ms

extern uint64_t lib_get_cpu_khz();

void ObActiveSessionStat::fixup_last_stat(const ObWaitEventDesc &desc)
{
  if (fixup_index_ != -1) {
    if (OB_LIKELY(fixup_ash_buffer_.is_valid())) {
      common::ObSharedGuard<ObAshBuffer> tmp_buffer = fixup_ash_buffer_; // copy on write
      if (tmp_buffer.is_valid()) {
        tmp_buffer->fixup_stat(fixup_index_, desc);
      }
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fixup index with no fixup buffer.", K_(fixup_index), KPC(this));
    }
  }
}
void ObActiveSessionStat::fixup_last_stat(const ObCurTraceId::TraceId &trace_id,
                                          const int64_t session_id,
                                          const char* sql_id,
                                          const int64_t plan_id, 
                                          const int64_t plan_hash,
                                          const int64_t stmt_type)
{
  if (sql_id[0] == '\0' && plan_id == 0 && plan_hash == 0) {
    // do nothing, skip fixup
  } else {
    if (fixup_index_ != -1) {
      if (OB_LIKELY(fixup_ash_buffer_.is_valid())) {
        common::ObSharedGuard<ObAshBuffer> tmp_buffer = fixup_ash_buffer_; // copy on write
        if (tmp_buffer.is_valid()) {
          tmp_buffer->fixup_stat(fixup_index_, trace_id, session_id, sql_id, plan_id, plan_hash, stmt_type);
        }
      } else {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "fixup index with no fixup buffer.", K_(fixup_index), KPC(this));
      }
    }
  }
}

// con only be called from ash sample thread(mutex protected).
void ObActiveSessionStat::set_fixup_buffer(common::ObSharedGuard<ObAshBuffer> &ash_buffer)
{
  // fixup buffer can only hold one ash buffer. Cannot switch it.
  // Otherwise core would happened when set_fixup_buffer coincide with session's reset fixup buffer.
  if (OB_LIKELY(!fixup_ash_buffer_.is_valid()) && ash_buffer.is_valid()) {
    fixup_ash_buffer_ = ash_buffer;
  }
}

void ObActiveSessionStat::set_fixup_buffer()
{
  if (OB_NOT_NULL(lib_get_ash_list_instance())) {
    if (!fixup_ash_buffer_.is_valid()) {
      // fixup_ash_buffer is not init
      common::ObSharedGuard<ObAshBuffer> buffer = lib_get_ash_list_instance()->get_ash_buffer();
      if (buffer.is_valid()) {
        set_fixup_buffer(buffer);
        LOG_DEBUG("succ to fixup buffer");
      }
    } else if (fixup_ash_buffer_.get_ptr() != lib_get_ash_list_instance()->get_ash_buffer().get_ptr()) {
      // process resize ash buffer
      common::ObSharedGuard<ObAshBuffer> buffer = lib_get_ash_list_instance()->get_ash_buffer();
      if (buffer.is_valid()) {
        fixup_index_ = -1;
        set_fixup_buffer(buffer);
        LOG_DEBUG("succ to fixup buffer");
      }
    }
  }
}

void ObActiveSessionStat::set_sess_active()
{
  if (!is_active_session_) {
    accumulate_tm_idle_time();
    is_active_session_ = true;
    if (trace_id_.is_invalid()) {
      trace_id_ = *common::ObCurTraceId::get_trace_id();
    }
    set_fixup_buffer();
  }

  if (id_ > 10) {
    // After init ash_buffer, fixup_ash_buffer_ should be valid.
    OB_ASSERT(fixup_ash_buffer_.is_valid());
  }
}

void ObActiveSessionStat::set_sess_inactive()
{
  if (is_active_session_) {
    is_active_session_ = false;
    last_inactive_ts_ = rdtsc();
  }
  fixup_index_ = -1;
}

void ObActiveSessionStat::accumulate_tm_idle_time()
{
  if (last_inactive_ts_ > 0) {
    // When set_sess_inactive() is called, there is some time left after last_ts_. So we mark it as
    // extra time to calculat in next round of calc_db_time when session is active again.
    const int64_t cur = rdtsc();
    tm_idle_cpu_cycles_ += cur - last_inactive_ts_;
  }
}

void ObActiveSessionStat::calc_db_time(
    ObDiagnosticInfo *di, const int64_t sample_time, const int64_t tsc_sample_time)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObActiveSessionStat &stat = di->get_ash_stat();
    int64_t last_touch_ts = stat.last_touch_ts_;
    int64_t cur_touch_ts = rdtsc();
    stat.last_touch_ts_ = cur_touch_ts;
    int64_t delta_time = (cur_touch_ts - last_touch_ts) * 1000 / static_cast<int64_t>(lib_get_cpu_khz());
    if (OB_UNLIKELY(delta_time <= 0)) {
      // ash sample happened same with session created
      stat.delta_time_ = 0;
      stat.delta_cpu_time_ = 0;
      stat.delta_db_time_ = 0;
    } else {
      const int64_t cur_wait_begin_ts = stat.wait_event_begin_ts_;
      const int64_t cur_event_no = stat.retry_wait_event_no_ > 0 ? stat.retry_wait_event_no_ : stat.event_no_;
      if (OB_UNLIKELY(cur_wait_begin_ts > 0 && cur_wait_begin_ts < cur_touch_ts && cur_event_no != 0)) {
        // has unfinished wait event
        stat.wait_event_begin_ts_ = cur_touch_ts;
        const uint64_t cur_wait_time =
            (cur_touch_ts - cur_wait_begin_ts) * 1000 / static_cast<int64_t>(lib_get_cpu_khz());
        if (OB_WAIT_EVENTS[cur_event_no].wait_class_ != ObWaitClassIds::IDLE) {
          stat.total_non_idle_wait_time_ += cur_wait_time;
        } else {
          stat.total_idle_wait_time_ += cur_wait_time;
        }
      }
      const int64_t delta_non_idle_wait_time = stat.total_non_idle_wait_time_ - stat.prev_non_idle_wait_time_;
      const int64_t delta_idle_wait_time = stat.total_idle_wait_time_ - stat.prev_idle_wait_time_;
      const int64_t tm_idle_time = stat.tm_idle_cpu_cycles_ * 1000 / static_cast<int64_t>(lib_get_cpu_khz());
      int64_t total_delta_time = tm_idle_time + delta_non_idle_wait_time + delta_idle_wait_time;
      if (delta_time < total_delta_time) {
        //During calc_db_time(),
        //the accumulation of wait events occurring concurrently in the session
        //may cause the delta_time to be underestimated.
        //In this situation, update cur_touch_ts to ensure that delta_time can cover all the events being tracked.
        cur_touch_ts = rdtsc();
        stat.last_touch_ts_ = cur_touch_ts;
        delta_time = (cur_touch_ts - last_touch_ts) * 1000 / static_cast<int64_t>(lib_get_cpu_khz());
        if (total_delta_time - delta_time <= 1000L) {
          //delta_time is slightly smaller than the total of the other times,
          //which may be due to the normal precision in time calculation.
          //Consider adjusting the delta_time to a larger value
          delta_time = tm_idle_time + delta_non_idle_wait_time + delta_idle_wait_time;
        } else if (total_delta_time - delta_time > 1000L) {
          LOG_WARN_RET(OB_ERR_UNEXPECTED, "calc_db_time catch an unexpected exception and needs attention, "
                                          "possibly due to an irregularity in the precision of rdtsc",
                                          K(stat.session_id_), K(cur_touch_ts), K(last_touch_ts), K(delta_time),
                                          K(cur_wait_begin_ts), K(cur_event_no), K(stat.retry_wait_event_no_), K(stat.event_no_),
                                          K(stat.total_non_idle_wait_time_), K(stat.prev_non_idle_wait_time_),
                                          K(delta_non_idle_wait_time), K(stat.total_idle_wait_time_),
                                          K(stat.prev_idle_wait_time_), K(delta_idle_wait_time), K(tm_idle_time));
        }
      }
      stat.delta_time_ = delta_time;
      stat.delta_db_time_ = delta_time - tm_idle_time - delta_idle_wait_time;
      stat.delta_cpu_time_ = stat.delta_db_time_ - delta_non_idle_wait_time;
      if (stat.delta_db_time_ < 0 || stat.delta_cpu_time_ < 0) {
        stat.delta_db_time_ = 0;
        stat.delta_cpu_time_ = 0;
      }
      stat.prev_non_idle_wait_time_ = stat.total_non_idle_wait_time_;
      stat.prev_idle_wait_time_ = stat.total_idle_wait_time_;
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
    stat.tm_idle_cpu_cycles_ = 0;
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

void ObActiveSessionStat::cal_delta_io_data(ObDiagnosticInfo *di) {
  if (OB_NOT_NULL(di)) {
    ObActiveSessionStat &stat = di->get_ash_stat();
    ObStatEventAddStatArray &event_stat = di->get_add_stat_stats();
    const int64_t total_io_read_count = event_stat.get(ObStatEventIds::IO_READ_COUNT)->stat_value_;
    const int64_t total_io_read_size = event_stat.get(ObStatEventIds::IO_READ_BYTES)->stat_value_;
    const int64_t total_io_write_count = event_stat.get(ObStatEventIds::IO_WRITE_COUNT)->stat_value_;
    const int64_t total_io_write_size = event_stat.get(ObStatEventIds::IO_WRITE_BYTES)->stat_value_;
    stat.delta_read_.count_ = total_io_read_count - stat.prev_read_.count_;
    stat.delta_read_.size_ = total_io_read_size - stat.prev_read_.size_;
    stat.delta_write_.count_ = total_io_write_count - stat.prev_write_.count_;
    stat.delta_write_.size_ = total_io_write_size - stat.prev_write_.size_;
    stat.prev_read_.count_ = total_io_read_count;
    stat.prev_read_.size_ = total_io_read_size;
    stat.prev_write_.count_ = total_io_write_count;
    stat.prev_write_.size_ = total_io_write_size;
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
  wait_event_begin_ts_ = rdtsc();
}

void ObActiveSessionStat::end_retry_wait_event()
{
  if (retry_wait_event_no_ > 0) {
    retry_wait_event_no_ = 0;
    retry_wait_event_p1_ = 0;
    retry_wait_event_p2_ = 0;
    retry_wait_event_p3_ = 0;
    retry_plan_line_id_  = -1;
    need_calc_wait_event_end_ = false;
    const int64_t cur_wait_time = (rdtsc() - wait_event_begin_ts_) * 1000 / lib_get_cpu_khz();

    if (event_no_ > 0) {
      // If there is a normal wait event that is still executing,
      // it needs to be updated.
      wait_event_begin_ts_ = rdtsc();
    } else {
      wait_event_begin_ts_ = 0;
    }
    total_non_idle_wait_time_ += cur_wait_time;
  }
}

void ObActiveSessionStat::begin_row_lock_wait_event()
{
  if (retry_wait_event_no_ == ObWaitEventIds::ROW_LOCK_WAIT) {
    need_calc_wait_event_end_ = false;
    wait_event_begin_ts_ = rdtsc();
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
  if (stat.action_[0] == '\0') {
    if (stat.mysql_cmd_ != -1) {
      MEMCCPY(buffer_[idx].action_,
          oceanbase::obmysql::ObMySQLPacket::get_mysql_cmd_name(static_cast<oceanbase::obmysql::ObMySQLCmd>(stat.mysql_cmd_)),
          '\0',
          sizeof(stat.action_) - 1);
      buffer_[idx].action_[sizeof(stat.action_) - 1] = '\0';
    } else if (stat.pcode_ != 0) {
      MEMCCPY(buffer_[idx].action_,
          obrpc::ObRpcPacketSet::instance().name_of_pcode(
              static_cast<oceanbase::obrpc::ObRpcPacketCode>(stat.pcode_)),
          '\0',
          sizeof(stat.action_) - 1);
      buffer_[idx].action_[sizeof(stat.action_) - 1] = '\0';
    }
  }
  return idx;
}

void ObAshBuffer::fixup_stat(int64_t index, const ObWaitEventDesc &desc)
{
  if (OB_UNLIKELY(index < 0 || index >= write_pos_)) {
    // index invalid for fixup, do noting.
  } else {
    ObActiveSessionStatItem &last_stat = buffer_[(index + buffer_.size()) % buffer_.size()];
    if (OB_UNLIKELY(last_stat.wait_time_ != 0 || last_stat.event_no_ != desc.event_no_)) {
      // wait event invalid for fix up, do noting.
    } else {
      last_stat.wait_time_ = desc.wait_time_;
      last_stat.p1_ = desc.p1_;
      last_stat.p2_ = desc.p2_;
      last_stat.p3_ = desc.p3_;
#if !defined(NDEBUG)
      const char *bt = lbt();
      int64_t size = std::min(sizeof(last_stat.bt_) - 1, STRLEN(bt));
      MEMCPY(last_stat.bt_, bt, size);
      last_stat.bt_[size] = '\0';
#endif
    }
  }
}

void ObAshBuffer::fixup_stat(int64_t index,
                             const ObCurTraceId::TraceId &trace_id,
                             const int64_t session_id,
                             const char* sql_id,
                             const int64_t plan_id,
                             const int64_t plan_hash,
                             const int64_t stmt_type)
{
  if (OB_UNLIKELY(index < 0 || index >= write_pos_)) {
    // index invalid for fixup, do noting.
  } else {
    ObActiveSessionStatItem &last_stat = buffer_[(index + buffer_.size()) % buffer_.size()];
    if (last_stat.session_id_ == session_id && trace_id.is_valid() && last_stat.trace_id_ == trace_id) {
      // make sure it is the same request
      if (last_stat.sql_id_[0] == '\0') {
        MEMCPY(last_stat.sql_id_, sql_id, sizeof(last_stat.sql_id_));
      }

      if (last_stat.plan_id_ == 0) {
        last_stat.plan_id_ = plan_id;
      }
      
      if (last_stat.plan_hash_ == 0) {
        last_stat.plan_hash_ = plan_hash;
      }

      if (last_stat.stmt_type_ == 0) {
        last_stat.stmt_type_ = stmt_type;
      }
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
// 0b                                                        59b       64b
// +---------------------------+----------------------------------------+
// |                         Local Seq                          |1|2|3|4|
// +---------------------------+----------------------------------------+
//
// Local Seq: 从0递增的int64原子变量
// 1: rpc请求置为1
// 2: 后台会话置为1
// 3: inner sql置为1
// 4: reserved
// Roughly speaking, over 500k ids would be consumed over 1 minutes.
uint64_t ObBackgroundSessionIdGenerator::get_next_rpc_session_id() {
  uint64_t sessid = static_cast<uint64_t>(ATOMIC_AAF(&local_seq_, 1));
  sessid &= 0xFFFFFFFFFFFFFFF;
  sessid |= ((uint64_t)1 << 60);
  LOG_DEBUG("succ to generate rpc session id", K_(local_seq), K(sessid));

  return sessid;
}

uint64_t ObBackgroundSessionIdGenerator::get_next_background_session_id() {
  uint64_t sessid = static_cast<uint64_t>(ATOMIC_AAF(&local_seq_, 1));
  sessid &= 0xFFFFFFFFFFFFFFF;
  sessid |= ((uint64_t)1 << 61);
  LOG_DEBUG("succ to generate background session id", K_(local_seq), K(sessid));

  return sessid;
}

uint64_t ObBackgroundSessionIdGenerator::get_next_inner_sql_session_id() {
  uint64_t sessid = static_cast<uint64_t>(ATOMIC_AAF(&local_seq_, 1));
  sessid &= 0xFFFFFFFFFFFFFFF;
  sessid |= ((uint64_t)1 << 62);
  LOG_DEBUG("succ to generate inner sql session id", K_(local_seq), K(sessid));

  return sessid;
}

ObASHTabletIdSetterGuard::ObASHTabletIdSetterGuard(const int64_t tablet_id) {
  GET_DIAGNOSTIC_INFO->get_ash_stat().tablet_id_ = tablet_id;
}

ObASHTabletIdSetterGuard::~ObASHTabletIdSetterGuard() {
  GET_DIAGNOSTIC_INFO->get_ash_stat().tablet_id_ = 0;
}

thread_local ObQueryRetryAshInfo* ObQueryRetryAshGuard::info_ = nullptr;
ObQueryRetryAshInfo *&ObQueryRetryAshGuard::get_info_ptr()
{
  return info_;
}

void ObQueryRetryAshGuard::setup_info(ObQueryRetryAshInfo &info)
{
  info_ = &info;
}

void ObQueryRetryAshGuard::reset_info()
{
  info_ = nullptr;
}