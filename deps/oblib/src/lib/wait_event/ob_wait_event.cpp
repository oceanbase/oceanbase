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

#include "lib/wait_event/ob_wait_event.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
ObWaitEventDef OB_WAIT_EVENTS[ObWaitEventIds::WAIT_EVENT_DEF_END + ObLatchIds::LATCH_END] = {
#define WAIT_EVENT_DEF(def, id, name, param1, param2, param3, wait_class, is_phy) \
  {id, name, param1, param2, param3, ObWaitClassIds::wait_class, is_phy},
#include "lib/wait_event/ob_wait_event.h"
#undef WAIT_EVENT_DEF
};

// would called after OB_LATCHES have been initialized
static __attribute__ ((constructor(103))) void init_latch_wait_events()
{
  // For every latch, there is a wait event to measure the lock wait time.
  // There is no need to explicitly define a wait event for every latch.
  // We automatically allocate one latch wait event after all the explicit
  // defined wait events.
  for (int32_t i = 0; i < ObLatchIds::LATCH_END; ++i) {
    ObWaitEventDef &event_def = OB_WAIT_EVENTS[ObWaitEventIds::WAIT_EVENT_DEF_END+i];
    event_def.event_id_ = ObLatchDesc::wait_event_id(OB_LATCHES[i].latch_id_);
    snprintf(event_def.event_name_, MAX_WAIT_EVENT_NAME_LENGTH, "latch: %s wait", OB_LATCHES[i].latch_name_);
    strcpy(event_def.param1_, "address");
    strcpy(event_def.param2_, "number");
    strcpy(event_def.param3_, "tries");
    event_def.wait_class_ = ObWaitClassIds::CONCURRENCY;
    event_def.is_phy_ = true;  // latch wait is atomic
  }
}

int64_t ObWaitEventDesc::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (event_no_ >= 0 && event_no_ < WAIT_EVENTS_TOTAL) {
    J_KV(K_(event_no), K_(p1), K_(p2), K_(p3), K_(wait_begin_time), K_(wait_end_time), K_(wait_time), K_(timeout_ms), K_(level), K_(parent),
        "event_id", OB_WAIT_EVENTS[event_no_].event_id_,
        "event_name", OB_WAIT_EVENTS[event_no_].event_name_,
        "param1", OB_WAIT_EVENTS[event_no_].param1_,
        "param2", OB_WAIT_EVENTS[event_no_].param2_,
        "param3", OB_WAIT_EVENTS[event_no_].param3_);
  } else {
    J_KV(K_(event_no), K_(p1), K_(p2), K_(p3), K_(wait_begin_time), K_(wait_end_time), K_(wait_time), K_(timeout_ms), K_(level), K_(parent));
  }
  return pos;
}

int ObWaitEventStat::add(const ObWaitEventStat &other)
{
  int ret = OB_SUCCESS;
  if (other.is_valid()) {
    if (is_valid()) {
      total_waits_ += other.total_waits_;
      total_timeouts_ += other.total_timeouts_;
      time_waited_ += other.time_waited_;
      max_wait_ = std::max(max_wait_, other.max_wait_);
    } else {
      *this = other;
    }
  }
  return ret;
}

int64_t ObWaitEventStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(total_waits), K_(total_timeouts), K_(time_waited), K_(max_wait));
  return pos;
}

}
}
