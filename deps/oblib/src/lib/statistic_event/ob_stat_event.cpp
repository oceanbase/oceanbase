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

#include "lib/statistic_event/ob_stat_event.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

#define STAT_DEF_true(def, name, stat_class, stat_id, summary_in_session, can_visible)\
{name, stat_class, stat_id, summary_in_session, can_visible},

#define STAT_DEF_false(def, name, stat_class, stat_id, summary_in_session, can_visible)

const ObStatEvent OB_STAT_EVENTS[] = {
#define STAT_EVENT_ADD_DEF(def, name, stat_class, stat_id, summary_in_session, can_visible, enable) \
  STAT_DEF_##enable(def, name, stat_class, stat_id, summary_in_session, can_visible)
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_ADD_DEF
#define STAT_EVENT_SET_DEF(def, name, stat_class, stat_id, summary_in_session, can_visible, enable) \
  STAT_DEF_##enable(def, name, stat_class, stat_id, summary_in_session, can_visible)
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_SET_DEF
};

#undef STAT_DEF_true
#undef STAT_DEF_false

ObStatEventAddStat::ObStatEventAddStat()
  : stat_value_(0)
{
}

int ObStatEventAddStat::add(const ObStatEventAddStat &other)
{
  int ret = OB_SUCCESS;
  if (other.is_valid()) {
    if (is_valid()) {
      stat_value_ += other.stat_value_;
    } else {
      *this = other;
    }
  }
  return ret;
}

int ObStatEventAddStat::add(int64_t value)
{
  int ret = OB_SUCCESS;
  stat_value_ += value;
  return ret;
}

void ObStatEventAddStat::reset()
{
  stat_value_ = 0;
}

ObStatEventSetStat::ObStatEventSetStat()
  : stat_value_(0),
    set_time_(0)
{
}

int ObStatEventSetStat::add(const ObStatEventSetStat &other)
{
  int ret = OB_SUCCESS;
  if (other.is_valid()) {
    if (is_valid()) {
      if (set_time_ < other.set_time_) {
        *this = other;
      }
    } else {
      *this = other;
    }
  }
  return ret;
}

void ObStatEventSetStat::reset()
{
  stat_value_ = 0;
  set_time_ = 0;
}


}
}
