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

using namespace oceanbase::common;

thread_local ObActiveSessionStat ObActiveSessionGuard::thread_local_stat_;

// a sample would be taken place up to 20ms after ash iteration begins.
// if sample time is above this threshold, mean ash execution too slow
constexpr int64_t ash_iteration_time = 40000;   // 40ms

ObActiveSessionStat *&ObActiveSessionGuard::get_stat_ptr()
{
  // before ObActiveSessionGuard constructed,
  // ensure it can get the dummy value if anyone call get_stat
  RLOCAL_INIT(ObActiveSessionStat *, stat, &thread_local_stat_);
  return stat;
}

ObActiveSessionStat &ObActiveSessionGuard::get_stat()
{
  return *get_stat_ptr();
}

void ObActiveSessionGuard::setup_default_ash()
{
  setup_thread_local_ash();
}

void ObActiveSessionGuard::setup_ash(ObActiveSessionStat &stat)
{
  OB_ASSERT(&stat != &thread_local_stat_);
  if (thread_local_stat_.is_bkgd_active_) {
    // some case(e.g. rpc) thread_local_stat_ is first activated and then session in session mgr is created.
    thread_local_stat_.accumulate_elapse_time();
    thread_local_stat_.is_bkgd_active_ = false;
  }
  get_stat_ptr() = &stat;
  stat.last_ts_ = common::ObTimeUtility::current_time();
}

void ObActiveSessionGuard::resetup_ash(ObActiveSessionStat &stat)
{
  get_stat_ptr() = &stat;
}

void ObActiveSessionGuard::resetup_thread_local_ash()
{
  get_stat_ptr() = &thread_local_stat_;
}

void ObActiveSessionGuard::setup_thread_local_ash()
{
  get_stat_ptr() = &thread_local_stat_;
  thread_local_stat_.last_ts_ = common::ObTimeUtility::current_time();
  thread_local_stat_.tid_ = GETTID();
}

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

// con only be called from ash sample thread(mutex protected).
void ObActiveSessionStat::set_fixup_buffer(common::ObSharedGuard<ObAshBuffer> &ash_buffer)
{
  // fixup buffer can only hold one ash buffer. Cannot switch it.
  // Otherwise core would happened when set_fixup_buffer coincide with session's reset fixup buffer.
  if (OB_LIKELY(!fixup_ash_buffer_.is_valid())) {
    fixup_ash_buffer_ = ash_buffer;
  }
}

void ObActiveSessionStat::set_async_committing()
{
  in_committing_ = true;
  int ret = OB_SUCCESS;
  wait_event_begin_ts_ = ObTimeUtility::current_time();
  event_no_ = ObWaitEventIds::ASYNC_COMMITTING_WAIT;
}

void ObActiveSessionStat::finish_async_commiting() {
  in_committing_ = false;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(event_no_ == 0 || wait_event_begin_ts_ == 0)) {
    // if happened. caller of set_async_committing should check and revert the wait event.
  } else {
    event_no_ = 0;
    const int64_t cur_wait_time = ObTimeUtility::current_time() - wait_event_begin_ts_;
    wait_event_begin_ts_ = 0;
    if (OB_LIKELY(cur_wait_time > 0)) {
      total_non_idle_wait_time_ += cur_wait_time;
    }
  }
}

int64_t ObAshBuffer::append(const ObActiveSessionStatItem &stat)
{
  // TODO: optimize performance, eliminate '%'
  OB_ASSERT(stat.wait_time_ == 0);
  int64_t idx = (write_pos_++ + buffer_.size()) % buffer_.size();
  MEMCPY(&buffer_[idx], &stat, sizeof(ObActiveSessionStatItem));
  buffer_[idx].id_ = write_pos_;
  if (0 == write_pos_ % WR_ASH_SAMPLE_INTERVAL) {
    buffer_[idx].is_wr_sample_ = true;
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
#ifndef NDEBUG
      const char *bt = lbt();
      int64_t size = std::min(sizeof(stat.bt_) - 1, STRLEN(bt));
      MEMCPY(stat.bt_, bt, size);
      stat.bt_[size] = '\0';
#endif
    }
  }
}

void ObActiveSessionStat::set_bkgd_sess_active()
{
  last_ts_ = common::ObTimeUtility::current_time();
  is_bkgd_active_ = true;
  trace_id_ = *common::ObCurTraceId::get_trace_id();
}

void ObActiveSessionStat::set_bkgd_sess_inactive()
{
  accumulate_elapse_time();
  is_bkgd_active_ = false;
}

void ObActiveSessionStat::accumulate_elapse_time()
{
  bkgd_elapse_time_ += common::ObTimeUtility::current_time() - last_ts_;
}

void ObActiveSessionStat::calc_db_time(ObActiveSessionStat &stat, const int64_t sample_time)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    const int64_t delta_time = sample_time - stat.last_ts_ + stat.bkgd_elapse_time_;
    stat.bkgd_elapse_time_ = 0;
    if (OB_UNLIKELY(delta_time <= 0)) {
      // ash sample happened before set_session_active
      if (delta_time < -ash_iteration_time) {
        LOG_WARN_RET(OB_SUCCESS, "ash sample happened before set_session_active.", K(delta_time), K(sample_time), K_(stat.last_ts));
      }
    } else if (OB_UNLIKELY(stat.last_ts_ == 0)) {
      // session is active, but last_ts_ is no set yet. see ObBasicSessionInfo::set_session_active()
      // LOG_INFO("stat's last_ts is 0, no need to record", K(stat));
    } else {
      const uint64_t cur_wait_begin_ts = stat.wait_event_begin_ts_;
      const int64_t cur_event_no = stat.event_no_;
      if (OB_UNLIKELY(cur_wait_begin_ts != 0 && cur_event_no != 0)) {
        // has unfinished wait event
        stat.wait_event_begin_ts_ = sample_time;
        const uint64_t cur_wait_time = sample_time - cur_wait_begin_ts;
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
        // LOG_WARN_RET(OB_SUCCESS, "negative db time happened, could be a race condition", K(stat),
        //     K(delta_time), K(delta_non_idle_wait_time), K(delta_idle_wait_time));
      } else {
        stat.last_ts_ = sample_time;
        stat.prev_non_idle_wait_time_ = stat.total_non_idle_wait_time_;
        stat.prev_idle_wait_time_ = stat.total_idle_wait_time_;
        // TODO: verify cpu time
        stat.delta_time_ = delta_time;
        stat.delta_cpu_time_ = delta_time - delta_non_idle_wait_time - delta_idle_wait_time;
        stat.delta_db_time_ = delta_time - delta_idle_wait_time;

        if (stat.session_type_ == ObActiveSessionStatItem::SessionType::BACKGROUND) {
          ObTenantStatEstGuard guard(stat.tenant_id_);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, delta_time);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_CPU, stat.delta_cpu_time_);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_DB_TIME, stat.delta_db_time_);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_NON_IDLE_WAIT_TIME, delta_non_idle_wait_time);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_IDLE_WAIT_TIME, delta_idle_wait_time);
        } else {
          ObSessionStatEstGuard guard(stat.tenant_id_, stat.session_id_);
          EVENT_ADD(SYS_TIME_MODEL_DB_TIME, stat.delta_db_time_);
          EVENT_ADD(SYS_TIME_MODEL_DB_CPU, stat.delta_cpu_time_);
          EVENT_ADD(SYS_TIME_MODEL_NON_IDLE_WAIT_TIME, delta_non_idle_wait_time);
          EVENT_ADD(SYS_TIME_MODEL_IDLE_WAIT_TIME, delta_idle_wait_time);
        }
      }
    }
  }
}

void ObActiveSessionStat::calc_db_time_for_background_session(ObActiveSessionStat &stat, const int64_t sample_time)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    const int64_t delta_time = stat.bkgd_elapse_time_;
    stat.bkgd_elapse_time_ = 0;
    if (OB_UNLIKELY(delta_time <= 0)) {
      // ash sample happened before set_session_active
      if (delta_time < -ash_iteration_time) {
        LOG_WARN_RET(OB_SUCCESS, "ash sample happened before set_session_active.", K(delta_time), K(sample_time), K_(stat.last_ts));
      }
    } else if (OB_UNLIKELY(stat.last_ts_ == 0)) {
      // session is active, but last_ts_ is no set yet. see ObBasicSessionInfo::set_session_active()
      // LOG_INFO("stat's last_ts is 0, no need to record", K(stat));
    } else {
      const uint64_t cur_wait_begin_ts = stat.wait_event_begin_ts_;
      const int64_t cur_event_no = stat.event_no_;
      if (OB_UNLIKELY(cur_wait_begin_ts != 0 && cur_event_no != 0)) {
        // has unfinished wait event
        stat.wait_event_begin_ts_ = sample_time;
        const uint64_t cur_wait_time = sample_time - cur_wait_begin_ts;
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
        // LOG_WARN_RET(OB_SUCCESS, "negative db time happened, could be a race condition", K(stat),
        //     K(delta_time), K(delta_non_idle_wait_time), K(delta_idle_wait_time));
      } else {
        stat.last_ts_ = sample_time;
        stat.prev_non_idle_wait_time_ = stat.total_non_idle_wait_time_;
        stat.prev_idle_wait_time_ = stat.total_idle_wait_time_;
        // TODO: verify cpu time
        stat.delta_time_ = delta_time;
        stat.delta_cpu_time_ = delta_time - delta_non_idle_wait_time - delta_idle_wait_time;
        stat.delta_db_time_ = delta_time - delta_idle_wait_time;

        if (stat.session_type_ == ObActiveSessionStatItem::SessionType::BACKGROUND) {
          ObTenantStatEstGuard guard(stat.tenant_id_);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, delta_time);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_CPU, stat.delta_cpu_time_);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_DB_TIME, stat.delta_db_time_);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_NON_IDLE_WAIT_TIME, delta_non_idle_wait_time);
          EVENT_ADD(SYS_TIME_MODEL_BKGD_IDLE_WAIT_TIME, delta_idle_wait_time);
        } else {
          LOG_WARN_RET(OB_ERR_UNEXPECTED, "calc background db time wrongly", K(stat));
        }
      }
    }
  }
}

void ObActiveSessionGuard::set_bkgd_sess_active()
{
  get_stat().set_bkgd_sess_active();
}

void ObActiveSessionGuard::set_bkgd_sess_inactive()
{
  get_stat().set_bkgd_sess_inactive();
}

ObRPCActiveGuard::ObRPCActiveGuard(int pcode)
{
  ObActiveSessionGuard::set_bkgd_sess_active();
  ObActiveSessionGuard::get_stat().pcode_ = pcode;
}

ObRPCActiveGuard::~ObRPCActiveGuard()
{
  const ObActiveSessionStat *stat = &ObActiveSessionGuard::get_stat();
  if (OB_UNLIKELY(stat != &ObActiveSessionGuard::thread_local_stat_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ash stat didn't reset to thread local ash",
        KPC(stat), K_(ObActiveSessionGuard::thread_local_stat), K(stat), K(&ObActiveSessionGuard::thread_local_stat_));
    ObActiveSessionGuard::setup_thread_local_ash();
  }
  ObActiveSessionGuard::get_stat().is_bkgd_active_ = false;
  ObActiveSessionGuard::get_stat().pcode_ = 0;
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
  LOG_INFO("succ to generate background session id", K(sessid));

  return sessid;
}