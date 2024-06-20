//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "share/compaction/ob_compaction_time_guard.h"

namespace oceanbase
{
namespace compaction
{

/**
 * -------------------------------------------------------------------ObCompactionTimeGuard-------------------------------------------------------------------
 */
ObCompactionTimeGuard::~ObCompactionTimeGuard()
{
  int64_t total_cost = 0;
  for (int64_t idx = 0; idx < size_; ++idx) {
    total_cost += event_times_[idx];
  }
  total_cost += common::ObTimeUtility::current_time() - last_click_ts_;
  if (OB_UNLIKELY(total_cost >= warn_threshold_)) {
    ::oceanbase::common::OB_PRINT(log_mod_, OB_LOG_LEVEL_DIRECT_NO_ERRCODE(WARN), OB_SUCCESS, "cost too much time", LOG_KVS(K(*this)));
  }
}

int64_t ObCompactionTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  fmt_ts_to_meaningful_str(buf, buf_len, pos, "|threshold", warn_threshold_);
  common::databuff_printf(buf, buf_len, pos, "start at %s|", common::ObTime2Str::ob_timestamp_str(add_time_));
  int64_t total_cost = 0;
  for (int64_t idx = 0; idx < size_; ++idx) {
    const uint64_t ts = event_times_[idx];
    if (ts < 1_ms) {
      common::databuff_printf(buf, buf_len, pos, "%ldus|", ts);
    } else if (ts < 1_s) {
      common::databuff_printf(buf, buf_len, pos, "%.2lfms|", double(ts) / 1_ms);
    } else {
      common::databuff_printf(buf, buf_len, pos, "%.2lfs|", double(ts) / 1_s);
    }
    total_cost += event_times_[idx];
  }
  total_cost += common::ObTimeUtility::current_time() - last_click_ts_;
  fmt_ts_to_meaningful_str(buf, buf_len, pos, "total", total_cost);
  if (pos != 0 && pos < buf_len) {
    pos -= 1;
  }
  return pos;
}

void ObCompactionTimeGuard::reuse()
{
  size_ = 0;
  last_click_ts_ = common::ObTimeUtility::current_time();
  add_time_ = common::ObTimeUtility::current_time();
  for (uint16_t i = 0; i < capacity_; ++i) {
    event_times_[i] = 0;
  }
}

bool ObCompactionTimeGuard::click(const uint16_t event)
{
  if (OB_LIKELY(event < CAPACITY)) {
    if (OB_LIKELY(size_ <= event)) {
      size_ = event + 1;
    }
    const int64_t now = common::ObTimeUtility::current_time();
    event_times_[event] += now - last_click_ts_;
    last_click_ts_ = now;
  }
  return true;
}

void ObCompactionTimeGuard::fmt_ts_to_meaningful_str(
     char *buf,
     const int64_t buf_len,
     int64_t &pos,
     const char *lvalue,
     const int64_t ts) const
{
  common::databuff_printf(buf, buf_len, pos, "%s", lvalue);
  if (ts < 1_ms) {
    common::databuff_printf(buf, buf_len, pos, "=%ldus|", ts);
  } else if (ts < 1_s) {
    common::databuff_printf(buf, buf_len, pos, "=%.2lfms|", double(ts) / 1_ms);
  } else {
    common::databuff_printf(buf, buf_len, pos, "=%.2lfs|", double(ts) / 1_s);
  }
}
void ObCompactionTimeGuard::add_time_guard(const ObCompactionTimeGuard &other)
{
  if (OB_LIKELY(guard_type_ == other.guard_type_ && CAPACITY == other.capacity_)) {
    size_ = std::max(size_, other.size_);
    for (uint16_t i = 0; i < size_; i++) {
      event_times_[i] += other.event_times_[i];
    }
  }
}

ObCompactionTimeGuard & ObCompactionTimeGuard::operator=(const ObCompactionTimeGuard &other)
{
  guard_type_ =  other.guard_type_;
  capacity_ = other.capacity_;
  size_ = other.size_;
  last_click_ts_ = other.last_click_ts_;
  add_time_ = other.add_time_;
  for (uint16_t i = 0; i < other.size_; ++i) {
    event_times_[i] = other.event_times_[i];
  }
  return *this;
}

uint16_t ObCompactionTimeGuard::get_max_event_count(const ObCompactionTimeGuardType guard_type)
{
  uint16_t max_event_count = CAPACITY;
  if (RS_COMPACT_TIME_GUARD == guard_type) {
    max_event_count =  ObRSCompactionTimeGuard::COMPACTION_EVENT_MAX;
  } else if (SCHEDULE_COMPACT_TIME_GUARD == guard_type) {
    max_event_count =  ObCompactionScheduleTimeGuard::COMPACTION_EVENT_MAX;
  } else if (STORAGE_COMPACT_TIME_GUARD == guard_type) {
    max_event_count = ObStorageCompactionTimeGuard::COMPACTION_EVENT_MAX;
  }
  return max_event_count;
}

/**
 * -------------------------------------------------------------------ObRSCompactionTimeGuard-------------------------------------------------------------------
 */
const char *ObRSCompactionTimeGuard::CompactionEventStr[] = {
    "PREPARE_UNFINISH_TABLE_IDS",
    "GET_TABLET_LS_PAIRS",
    "GET_TABLET_META_TABLE",
    "CKM_VERIFICATION"
};

const char *ObRSCompactionTimeGuard::get_comp_event_str(enum CompactionEvent event)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) == ARRAYSIZEOF(CompactionEventStr), "events str len is mismatch");
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) <= static_cast<int64_t>(CAPACITY), "too many events, need update CAPACITY");
  const char *str = "";
  if (event >= COMPACTION_EVENT_MAX || event < PREPARE_UNFINISH_TABLE_IDS) {
    str = "invalid_type";
  } else {
    str = CompactionEventStr[event];
  }
  return str;
}

int64_t ObRSCompactionTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  for (uint16_t idx = 0; idx < size_; ++idx) {
    if (event_times_[idx] > 0) {
      fmt_ts_to_meaningful_str(buf, buf_len, pos, get_comp_event_str(static_cast<CompactionEvent>(idx)), event_times_[idx]);
    }
  }
  return pos;
}

/**
 * ObCompactionScheduleTimeGuard Impl
 */
const char *ObCompactionScheduleTimeGuard::CompactionEventStr[] = {
    "GET_TABLET",
    "UPDATE_TABLET_REPORT_STATUS",
    "READ_MEDIUM_INFO",
    "SCHEDULE_NEXT_MEDIUM",
    "SCHEDULE_TABLET_MEDIUM",
    "FAST_FREEZE",
    "SEARCH_META_TABLE",
    "CHECK_META_TABLE",
    "SEARCH_CHECKSUM",
    "CHECK_CHECKSUM",
    "SCHEDULER_NEXT_ROUND"
};

const char *ObCompactionScheduleTimeGuard::get_comp_event_str(enum CompactionEvent event)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) == ARRAYSIZEOF(CompactionEventStr), "events str len is mismatch");
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) <= static_cast<int64_t>(CAPACITY), "too many events, need update CAPACITY");
  const char *str = "";
  if (event >= COMPACTION_EVENT_MAX || event < GET_TABLET) {
    str = "invalid_type";
  } else {
    str = CompactionEventStr[event];
  }
  return str;
}

int64_t ObCompactionScheduleTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  for (int16_t idx = 0; idx < size_; ++idx) {
    if (event_times_[idx] > 0) {
      fmt_ts_to_meaningful_str(buf, buf_len, pos, get_comp_event_str(static_cast<CompactionEvent>(idx)), event_times_[idx]);
    }
  }
  return pos;
}

/*
 *  ----------------------------------------------ObCompactionTimeGuard--------------------------------------------------
 */
constexpr float ObStorageCompactionTimeGuard::COMPACTION_SHOW_PERCENT_THRESHOLD;
const char *ObStorageCompactionTimeGuard::CompactionEventStr[] = {
    "WAIT_TO_SCHEDULE",
    "COMPACTION_POLICY",
    "PRE_PROCESS_TX_TABLE",
    "GET_PARALLEL_RANGE",
    "EXECUTE",
    "CREATE_SSTABLE",
    "UPDATE_UPPER_TRANS",
    "UPDATE_TABLET",
    "RELEASE_MEMTABLE",
    "SCHEDULE_OTHER_COMPACTION",
    "DAG_FINISH"
};

const char *ObStorageCompactionTimeGuard::get_comp_event_str(const enum CompactionEvent event)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) == ARRAYSIZEOF(CompactionEventStr), "events str len is mismatch");
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) <= static_cast<int64_t>(CAPACITY), "too many events, need update CAPACITY");
  const char *str = "";
  if (event >= COMPACTION_EVENT_MAX || event < DAG_WAIT_TO_SCHEDULE) {
    str = "invalid_type";
  } else {
    str = CompactionEventStr[event];
  }
  return str;
}

int64_t ObStorageCompactionTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t total_cost = 0;
  J_KV(K_(add_time));
  common::databuff_printf(buf, buf_len, pos, "|");
  if (size_ > DAG_WAIT_TO_SCHEDULE && event_times_[DAG_WAIT_TO_SCHEDULE] > COMPACTION_SHOW_TIME_THRESHOLD) {
    fmt_ts_to_meaningful_str(buf, buf_len, pos, "wait_schedule_time", event_times_[DAG_WAIT_TO_SCHEDULE]);
  }
  for (int64_t idx = COMPACTION_POLICY; idx < size_; ++idx) {
    total_cost += event_times_[idx];
  }
  if (total_cost > COMPACTION_SHOW_TIME_THRESHOLD) {
    float ratio = 0;
    for (int64_t idx = COMPACTION_POLICY; idx < size_; ++idx) {
      const uint32_t time_interval = event_times_[idx]; // include the retry time since previous event
      ratio = (float)(time_interval)/ total_cost;
      if (ratio >= COMPACTION_SHOW_PERCENT_THRESHOLD || time_interval >= COMPACTION_SHOW_TIME_THRESHOLD) {
        fmt_ts_to_meaningful_str(buf, buf_len, pos, get_comp_event_str(static_cast<CompactionEvent>(idx)), event_times_[idx]);
        if (ratio > 0.01) {
          common::databuff_printf(buf, buf_len, pos, "(%.2f)", ratio);
        }
        common::databuff_printf(buf, buf_len, pos, "|");
      }
    }
  }
  fmt_ts_to_meaningful_str(buf, buf_len, pos, "total", total_cost);
  if (pos != 0 && pos < buf_len) {
    buf[pos - 1] = ';';
  }

  if (pos != 0 && pos < buf_len) {
    pos -= 1;
  }
  return pos;
}


} // namespace compaction
} // namespace oceanbase
