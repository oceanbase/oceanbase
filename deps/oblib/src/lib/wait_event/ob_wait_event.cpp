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

#include "lib/stat/ob_latch_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/stat/ob_latch_define.h"

namespace oceanbase
{
namespace common
{

const ObWaitEventDef OB_WAIT_EVENTS[WAIT_EVENTS_TOTAL] = {
#define WAIT_EVENT_DEF_true(def, id, name, param1, param2, param3, wait_class, is_phy) \
  {id, name, param1, param2, param3, ObWaitClassIds::wait_class, is_phy},
#define WAIT_EVENT_DEF_false(def, id, name, param1, param2, param3, wait_class, is_phy)
#define WAIT_EVENT_DEF(def, id, name, param1, param2, param3, wait_class, is_phy, enable) \
WAIT_EVENT_DEF_##enable(def, id, name, param1, param2, param3, wait_class, is_phy)
#include "lib/wait_event/ob_wait_event.h"
#undef WAIT_EVENT_DEF
#undef WAIT_EVENT_DEF_true
#undef WAIT_EVENT_DEF_false

#define LATCH_DEF_true(def, id, name, policy, max_spin_cnt, max_yield_cnt)  \
{id, "latch: " name " wait", "address", "number", "tries", ObWaitClassIds::CONCURRENCY, true},
#define LATCH_DEF_false(def, id, name, policy, max_spin_cnt, max_yield_cnt)
#define LATCH_DEF(def, id, name, policy, max_spin_cnt, max_yield_cnt, enable)  \
LATCH_DEF_##enable(def, id, name, policy, max_spin_cnt, max_yield_cnt)
#include "lib/stat/ob_latch_define.h"
#undef LATCH_DEF
#undef LATCH_DEF_true
#undef LATCH_DEF_false
};

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

bool ObWaitEventDesc::is_valid()
{
  return event_no_>=0 && event_no_ < WAIT_EVENTS_TOTAL;
}
int ObWaitEventStat::add(const ObWaitEventStat &other)
{
  int ret = OB_SUCCESS;
  if (other.is_valid()) {
    // TODO(roland.qk): remove this if() and make it SIMD compatible.
    if (is_valid()) {
      total_waits_ += other.total_waits_;
      total_timeouts_ += other.total_timeouts_;
      time_waited_ += other.time_waited_;
      max_wait_ = std::max(max_wait_, other.max_wait_);
    } else {
      total_waits_ = other.total_waits_;
      total_timeouts_ = other.total_timeouts_;
      time_waited_ = other.time_waited_;
      max_wait_ =  other.max_wait_;
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
