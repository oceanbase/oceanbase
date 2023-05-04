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

#ifndef _OB_TRACE_EVENT_H
#define _OB_TRACE_EVENT_H 1
#include "lib/json/ob_yson.h"  // for yson::databuff_print_elements
#include "lib/ob_name_id_def.h"
#include "lib/time/ob_time_utility.h"
#include "lib/trace/ob_seq_event_recorder.h"
#include "lib/lock/ob_mutex.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/ob_lib_config.h"
namespace oceanbase
{
namespace common
{
// <event_id, timestamp, (yson elment KV)*>
// @note sizeof(ObTraceEvent)=16
struct ObTraceEvent
{
  ObTraceEvent()
  {
    timestamp_ = 0;
    id_ = 0;
    yson_beg_pos_ = 0;
    yson_end_pos_ = 0;
  }
  void set_id_time(ObEventID id) { id_ = id; timestamp_ = ObTimeUtility::fast_current_time(); yson_beg_pos_ = yson_end_pos_ = 0; }
  int64_t timestamp_;
  ObEventID id_;
  int16_t yson_beg_pos_;
  int16_t yson_end_pos_;
};

template<int64_t EVENT_COUNT, int64_t INFO_BUFFER_SIZE>
class ObTraceEventRecorderBase : public ObSeqEventRecorder<ObTraceEvent, EVENT_COUNT, INFO_BUFFER_SIZE>
{
public:
  ObTraceEventRecorderBase(bool need_lock=false, uint32_t latch_id=ObLatchIds::TRACE_RECORDER_LOCK)
      :lock_(latch_id),
       need_lock_(need_lock)
  {
    STATIC_ASSERT(sizeof(ObTraceEvent)==16, "sizeof(ObTraceEvent)=16");
    STATIC_ASSERT(sizeof(*this)<=8192, "sizeof(*this)<=8192");
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    int64_t prev_ts = 0;
    int64_t total_time = 0;
    const char* event_name = NULL;
    const ObString nls_format;
    for (int64_t i = 0; i < this->next_idx_; ++i) {
      const ObTraceEvent &ev = this->events_[i];
      event_name = NAME(ev.id_);
      if (prev_ts == 0) {
        (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "begin_ts=%ld ", ev.timestamp_);
        common::ObTimeConverter::datetime_to_str(ev.timestamp_, nullptr, nls_format, 6, buf, buf_len, pos);
        prev_ts = ev.timestamp_;
      }
      if (NULL == event_name) {
        (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|[%hu] u=%ld ",
                                                   ev.id_, ev.timestamp_ - prev_ts);
      } else {
        (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|[%s] u=%ld ",
                                                   event_name, ev.timestamp_ - prev_ts);
      }
      if (ev.yson_end_pos_ > ev.yson_beg_pos_ && ev.yson_beg_pos_ >= 0) {
        (void)::oceanbase::yson::databuff_print_elements(buf, buf_len, pos,
                                                         this->buffer_ + ev.yson_beg_pos_,
                                                         ev.yson_end_pos_-ev.yson_beg_pos_);
      }
      total_time += ev.timestamp_ - prev_ts;
      prev_ts = ev.timestamp_;
    }
    (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|total_timeu=%ld", total_time);
    if (this->dropped_events_ > 0) {
      (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, " DROPPED_EVENTS=%ld", this->dropped_events_);
    }
    return pos;
  }
  void check_lock()
  {
    if (need_lock_) {
      (void)lock_.lock();
    }
  }
  void check_unlock()
  {
    if (need_lock_) {
      (void)lock_.unlock();
    }
  }
private:
  lib::ObMutex lock_;
  bool need_lock_;
};

// template <EVENT_COUNT, BUF_SIZE>
// EVENT_COUNT is generally sufficient
// After BUF_SIZE exceeds the defined value, the kv pair in REC_TRACE_EXT will not be printed
typedef ObTraceEventRecorderBase<189, 1200> ObTraceEventRecorder;

inline ObTraceEventRecorder *get_trace_recorder()
{
  auto *ptr = GET_TSI_MULT(ObTraceEventRecorder, 1);
  return ptr;
}

} // end namespace common
} // end namespace oceanbase

// record into the specified recorder, e.g.

#define REC_TRACE(recorder, trace_event) \
  do {                                                       \
    if (oceanbase::lib::is_trace_log_enabled()) {                          \
      (recorder).add_event().set_id_time(OB_ID(trace_event)); \
    }                                                        \
  } while (0)

//   REC_TRACE_EXT(m, start_trans, OB_ID(arg1), a, OB_ID(arg2), b, N(c), N_(d));
#define REC_TRACE_EXT(recorder, trace_event, pairs...) do {             \
    if (oceanbase::lib::is_trace_log_enabled()) {                          \
      bool rec__is_overflow_ = false;                                     \
      ObTraceEvent &trace_ev = (recorder).add_event(rec__is_overflow_);   \
      if (OB_UNLIKELY(rec__is_overflow_)) {                               \
        trace_ev.set_id_time(OB_ID(trace_event));                            \
      } else {                                                            \
        trace_ev.set_id_time(OB_ID(trace_event));                            \
        (recorder).check_lock();                                          \
        trace_ev.yson_beg_pos_ = static_cast<int16_t>((recorder).get_buffer_pos()); \
        int rec__err_ = oceanbase::yson::databuff_encode_elements((recorder).get_buffer(), ObTraceEventRecorder::MAX_INFO_BUFFER_SIZE, (recorder).get_buffer_pos(), ##pairs); \
        if (OB_LIKELY(OB_SUCCESS == rec__err_)) {                  \
          trace_ev.yson_end_pos_ = static_cast<int16_t>((recorder).get_buffer_pos()); \
        } else {                                                          \
          (recorder).get_buffer_pos() = trace_ev.yson_beg_pos_;           \
          trace_ev.yson_beg_pos_ = trace_ev.yson_end_pos_ = 0;            \
        }                                                                 \
        (recorder).check_unlock();                                        \
      }                                                                   \
    }                                                                   \
  } while (0)


// record trace events into THE one recorder
#define THE_TRACE ::oceanbase::common::get_trace_recorder()

#define NG_TRACE(...)                           \
  do {                                          \
    if (oceanbase::lib::is_trace_log_enabled() && OB_LIKELY(THE_TRACE != nullptr)) {     \
      REC_TRACE(*THE_TRACE, __VA_ARGS__);        \
    }                                           \
  } while (0)                                   \

#define NG_TRACE_EXT(...)                       \
  do {                                          \
    if (oceanbase::lib::is_trace_log_enabled() && OB_LIKELY(THE_TRACE != nullptr)) {     \
      REC_TRACE_EXT(*THE_TRACE, __VA_ARGS__);    \
    }                                           \
  } while (0)

#endif /* _OB_TRACE_EVENT_H */
