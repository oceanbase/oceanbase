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

#define USING_LOG_PREFIX COMMON

#include "lib/stat/ob_diagnostic_info.h"
#include "lib/stat/ob_diagnostic_info_container.h"
#include "lib/stat/ob_diagnostic_info_util.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

namespace oceanbase
{
namespace common
{

int64_t ObListWaitEventStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(event_no), K_(total_waits), K_(total_timeouts), K_(time_waited), K_(max_wait));
  return pos;
}

ObWaitEventContainer::ObWaitEventContainer()
    : rule_(ObWaitEventRule::LIST), list_(), array_(nullptr), pool_(nullptr)
{}

int ObWaitEventContainer::init(ObWaitEventPool *pool)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(pool != nullptr);
  pool_ = pool;
  list_.reset();
  return ret;
}

int ObWaitEventContainer::get_and_set(
    oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum event_no, ObWaitEventStat *&event)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(pool_ != nullptr);
  if (OB_FAIL(get(event_no, event))) {
    if (ret == OB_ITEM_NOT_SETTED) {
      ret = OB_SUCCESS;
      if (OB_LIKELY(rule_ == ObWaitEventRule::LIST)) {
        int list_size = 0;
        for (int i = 0; i < WAIT_EVENT_LIST_THRESHOLD; i++) {
          ObListWaitEventStat *cur_event = list_.get(i);
          if (cur_event->event_no_ != 0) {
            list_size++;
          }
        }
        if (OB_UNLIKELY(list_size >= WAIT_EVENT_LIST_THRESHOLD)) {
          // need to switch list to array.
          if (OB_ISNULL(pool_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            ObWaitEventStatArray *array = pool_->borrow_object();
            if (OB_ISNULL(array)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              array_ = array;
              copy_list_stat_to_array();
              rule_ = ObWaitEventRule::ARRAY;
              event = array_->get(event_no);
              OB_ASSERT(event != nullptr);
              // For simplicity, we don't free list_'s memory till this ObWaitEventContainer end of
              // life.
            }
          }
        } else {
          for (int i = 0; i < WAIT_EVENT_LIST_THRESHOLD; i++) {
            ObListWaitEventStat *cur_event = list_.get(i);
            if (cur_event->event_no_ == 0) {
              cur_event->event_no_ = event_no;
              event = static_cast<ObWaitEventStat *>(cur_event);
              break;
            }
          }
          OB_ASSERT(event != nullptr);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rule should be LIST", K(rule_));
      }
    } else {
      // Error happened, return err code.
    }
  } else {
    // Event stat already exist. Do noting
  }
  return ret;
}

int ObWaitEventContainer::get(
    oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum event_no, ObWaitEventStat *&event)
{
  int ret = OB_ITEM_NOT_SETTED;
  OB_ASSERT(event_no > 0 && event_no < WAIT_EVENTS_TOTAL);
  if (OB_LIKELY(rule_ == ObWaitEventRule::LIST)) {
    for (int i = 0; i < WAIT_EVENT_LIST_THRESHOLD; i++) {
      ObListWaitEventStat *cur_event = list_.get(i);
      if (cur_event->event_no_ == event_no) {
        event = static_cast<ObWaitEventStat *>(cur_event);
        ret = OB_SUCCESS;
        break;
      }
    }
  } else {
    event = array_->get(event_no);
    ret = OB_SUCCESS;
  }
  return ret;
}

void ObWaitEventContainer::for_each(const std::function<void(
        oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum, const ObWaitEventStat &)> &fn)
{
  if (OB_LIKELY(rule_ == ObWaitEventRule::LIST)) {
    for (int i = 0; i < WAIT_EVENT_LIST_THRESHOLD; i++) {
      ObListWaitEventStat *cur_event = list_.get(i);
      if (cur_event->event_no_ != 0) {
        fn(cur_event->event_no_, *static_cast<ObWaitEventStat *>(cur_event));
      }
    }
  } else {
    for (int i = ObWaitEventIds::NULL_EVENT + 1; i < ObWaitEventIds::WAIT_EVENT_DEF_END; i++) {
      const ObWaitEventStat *cur = array_->get(static_cast<ObWaitEventIds::ObWaitEventIdEnum>(i));
      if (cur->is_valid()) {
        fn(static_cast<ObWaitEventIds::ObWaitEventIdEnum>(i), *cur);
      }
    }
  }
}

void ObWaitEventContainer::accumulate_to(ObWaitEventStatArray &target)
{
  if (OB_LIKELY(rule_ == ObWaitEventRule::LIST)) {
    for (int i = 0; i < WAIT_EVENT_LIST_THRESHOLD; i++) {
      ObListWaitEventStat *cur_event = list_.get(i);
      if (cur_event->event_no_ != 0) {
        target.get(cur_event->event_no_)->add(*static_cast<ObWaitEventStat *>(cur_event));
      }
    }
  } else {
    target.add(*array_);
  }
}

void ObWaitEventContainer::copy_list_stat_to_array()
{
  for (int i = 0; i < WAIT_EVENT_LIST_THRESHOLD; i++) {
    ObListWaitEventStat *cur_event = list_.get(i);
    if (cur_event->event_no_ != 0) {
      ObWaitEventStat *cur = array_->get(cur_event->event_no_);
      cur->add(*static_cast<const ObWaitEventStat *>(cur_event));
    }
  }
}

void ObWaitEventContainer::reset()
{
  list_.reset();
  if (nullptr != array_) {
    OB_ASSERT(pool_ != nullptr);
    pool_->return_object(array_);
  }
  array_ = nullptr;
  rule_ = ObWaitEventRule::LIST;
}

ObDiagnosticInfo::~ObDiagnosticInfo()
{
  if (need_aggregate_ && is_inited_) {
    ObLocalDiagnosticInfo::aggregate_diagnostic_info_summary(this);
  }
}

int ObDiagnosticInfo::init(
    int64_t tenant_id, int64_t group_id, int64_t session_id, ObWaitEventPool &pool)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("di info init twice", K(ret), K(tenant_id), K(group_id), K(session_id), KPC(this),
        K(lbt()));
  } else {
    tenant_id_ = tenant_id;
    group_id_ = group_id;
    session_id_ = session_id;
    ash_stat_.tenant_id_ = tenant_id;
    ash_stat_.group_id_ = group_id_;
    ash_stat_.session_id_ = session_id;
    // By default, the proxy_sid is consistent with the server session id.
    // If this is a genuine proxy connection, the proxy_sid will be corrected by the
    // deliver_mysql_request interface
    ash_stat_.proxy_sid_ = session_id;
    ash_stat_.last_touch_ts_ = rdtsc();
    ash_stat_.last_inactive_ts_ = ash_stat_.last_touch_ts_;
    pool_ = &pool;
    events_.init(pool_);
    is_inited_ = true;
  }
  return ret;
}

// for wait event begins when is_active_session_ = false.
void ObDiagnosticInfo::inner_begin_wait_event(const int64_t event_no, const uint64_t timeout_ms,
    const uint64_t p1, const uint64_t p2, const uint64_t p3)
{
  const int64_t cur_event_no = ash_stat_.event_no_;
  if (cur_event_no == 0) {
    // TODO(roland.qk): unify wait event record source.
    ash_stat_.set_event(event_no, p1, p2, p3);
    curr_wait_.reset();
    curr_wait_.event_no_ = event_no;
    curr_wait_.p1_ = p1;
    curr_wait_.p2_ = p2;
    curr_wait_.p3_ = p3;
    curr_wait_.timeout_ms_ = timeout_ms;
    curr_wait_.wait_begin_time_ = rdtsc();
    ash_stat_.wait_event_begin_ts_ = curr_wait_.wait_begin_time_;
  } else {
    // do noting
  }
}

void ObDiagnosticInfo::begin_wait_event(const int64_t event_no, const uint64_t timeout_ms,
    const uint64_t p1, const uint64_t p2, const uint64_t p3)
{
  if (ash_stat_.is_active_session_) {
    inner_begin_wait_event(event_no, timeout_ms, p1, p2, p3);
  }
}

void ObDiagnosticInfo::end_wait_event(const int64_t event_no, const bool is_idle)
{
  int ret = OB_SUCCESS;
  const int cur_event_no = ash_stat_.event_no_;
  if (cur_event_no == event_no) {
    curr_wait_.wait_end_time_ = rdtsc();
    curr_wait_.wait_time_ = (curr_wait_.wait_end_time_ - curr_wait_.wait_begin_time_) * 1000 /
                            lib_get_cpu_khz();
    // TODO(roland.qk): unify wait event record source.
    const int64_t cur_wait_time = (curr_wait_.wait_end_time_ - ash_stat_.wait_event_begin_ts_) * 1000 /
                            lib_get_cpu_khz();
    if (!is_idle) {
      ash_stat_.total_non_idle_wait_time_ += cur_wait_time;
    } else {
      ash_stat_.total_idle_wait_time_ += cur_wait_time;
    }

    ObWaitEventDesc desc;
    desc.event_no_ = event_no;
    desc.p1_ = ash_stat_.p1_;
    desc.p2_ = ash_stat_.p2_;
    desc.p3_ = ash_stat_.p3_;
    ash_stat_.fixup_last_stat(desc);

    ash_stat_.reset_event();
    total_wait_.time_waited_ += curr_wait_.wait_time_;
    ++total_wait_.total_waits_;
    ObWaitEventStat *event_record = nullptr;
    if (OB_FAIL(events_.get_and_set(
            static_cast<ObWaitEventIds::ObWaitEventIdEnum>(event_no), event_record))) {
      LOG_WARN("failed to retrive wait event record", K(ret), K(event_no));
    } else {
      event_record->time_waited_ += curr_wait_.wait_time_;
      ++event_record->total_waits_;
      event_record->max_wait_ =
          std::max(static_cast<int64_t>(event_record->max_wait_), curr_wait_.wait_time_);
      if (curr_wait_.timeout_ms_ > 0 &&
          (curr_wait_.wait_time_ > (static_cast<int64_t>(curr_wait_.timeout_ms_) * 1000))) {
        ++event_record->total_timeouts_;
      }
    }
  } else {
    // do noting
  }
}

} /* namespace common */
} /* namespace oceanbase */
