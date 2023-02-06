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

#ifndef _OB_SEQ_EVENT_RECORDER_H
#define _OB_SEQ_EVENT_RECORDER_H 1
#include <stdint.h>
#include "lib/utility/ob_template_utils.h"  // for STATIC_ASSERT
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
// event id name
typedef uint16_t ObEventID;
// event recorder
template <typename EventType, int64_t EVENT_COUNT, int64_t INFO_BUFFER_SIZE>
struct ObSeqEventRecorder
{
  ObSeqEventRecorder()
      :next_idx_(0),
       buffer_pos_(0),
       dropped_events_(0)
  {
    STATIC_ASSERT(INFO_BUFFER_SIZE<INT16_MAX, "INFO_BUFFER_SIZE too large");
  }
  int assign(const ObSeqEventRecorder &other)
  {
    next_idx_ = (EVENT_COUNT < other.next_idx_) ? EVENT_COUNT : other.next_idx_;
    memcpy(this->events_, other.events_, sizeof(EventType)*next_idx_);
    memcpy(this->buffer_, other.buffer_, other.buffer_pos_);
    buffer_pos_ = other.buffer_pos_;
    dropped_events_ = other.dropped_events_;
    return OB_SUCCESS;
  }
  EventType &add_event(bool &overflow)
  {
    if (0 > next_idx_) {
      COMMON_LOG_RET(ERROR, OB_ERROR, "fatal: next_idx illegal", K_(next_idx));
      dropped_events_++;
      overflow = true;
      return events_[EVENT_COUNT-1];
    } else if (next_idx_ >= EVENT_COUNT) {  // Avoid the following atomic operations
      dropped_events_++;
      overflow = true;
      return events_[EVENT_COUNT-1];
    } else {
      int64_t idx = __sync_fetch_and_add(&next_idx_, 1);
      return (idx < EVENT_COUNT) ?
          events_[idx] :
          (dropped_events_++, (overflow = true), events_[EVENT_COUNT-1]);
    }
  }
  EventType &add_event()
  {
    if (0 > next_idx_) {
      COMMON_LOG_RET(ERROR, OB_ERROR, "fatal: next_idx illegal", K_(next_idx));
      dropped_events_++;
      return events_[EVENT_COUNT-1];
    } else if (next_idx_ >= EVENT_COUNT) {  // Avoid the following atomic operations
      dropped_events_++;
      return events_[EVENT_COUNT-1];
    } else {
      int64_t idx = __sync_fetch_and_add(&next_idx_, 1);
      return (idx < EVENT_COUNT) ?
          events_[idx] :
          (dropped_events_++, events_[EVENT_COUNT-1]);
    }
  }
  const EventType &get_event(int64_t idx) const { return events_[idx]; }
  int64_t count() const { return (EVENT_COUNT < next_idx_) ? EVENT_COUNT : next_idx_; }
  void reset() { next_idx_ = 0; buffer_pos_ = 0; dropped_events_ = 0; }
  char *get_buffer() { return buffer_;}
  int64_t &get_buffer_pos() { return buffer_pos_; }
  // internal use
  EventType* get_events_array() { return events_; }
  const EventType* get_events_array() const { return events_; }
  void set_count(int64_t count) { next_idx_ = count; }
public:
  static const int64_t MAX_EVENT_COUNT = EVENT_COUNT;
  static const int64_t MAX_INFO_BUFFER_SIZE = INFO_BUFFER_SIZE;
protected:
  EventType events_[EVENT_COUNT];
  int64_t next_idx_;
  char buffer_[INFO_BUFFER_SIZE];
  int64_t buffer_pos_;
  int64_t dropped_events_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSeqEventRecorder);
};
} // end namespace common
} // end namespace oceanbase

#endif /* _OB_SEQ_EVENT_RECORDER_H */
