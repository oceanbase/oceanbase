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

#ifndef _OB_SHARE_ASH_ACTIVE_SESSION_GUARD_H_
#define _OB_SHARE_ASH_ACTIVE_SESSION_GUARD_H_

#include "lib/lock/ob_spin_lock.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/profile/ob_trace_id.h"
namespace oceanbase
{
namespace common
{

struct ActiveSessionStat
{
public:
  ActiveSessionStat()
      : id_(0),
        tenant_id_(0),
        user_id_(0),
        session_id_(0),
        plan_id_(0),
        sample_time_(0),
        event_no_(0),
        wait_time_(0),
        p1_(0),
        p2_(0),
        p3_(0),
        exec_phase_(0),
        trace_id_(),
        plan_line_id_(-1),
        session_type_(false),
        last_stat_(nullptr)
  {
    sql_id_[0] = '\0';
#ifndef NDEBUG
    bt_[0] = '\0';
#endif
  }
  ~ActiveSessionStat() = default;
  void fixup_last_stat(ObWaitEventDesc &desc)
  {
    ActiveSessionStat *stat = last_stat_;
    if (stat) {
      stat->wait_time_ = desc.wait_time_;
      stat->p1_ = desc.p1_;
      stat->p2_ = desc.p2_;
      stat->p3_ = desc.p3_;
#ifndef NDEBUG
      const char *bt = lbt();
      int64_t size = std::min(sizeof(stat->bt_) - 1, STRLEN(bt));
      MEMCPY(stat->bt_, bt, size);
      stat->bt_[size] = '\0';
#endif
      last_stat_ = nullptr;
    }
  }
  void set_last_stat(ActiveSessionStat *stat)
  {
    last_stat_ = stat;
  }
public:
  uint64_t id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t session_id_;
  uint64_t plan_id_;
  int64_t sample_time_; // sample time
  int64_t event_no_; // wait event no
  uint64_t wait_time_; // how long the event waited in total
  uint64_t p1_; // event parameter 1
  uint64_t p2_; // event parameter 2
  uint64_t p3_; // event parameter 3

  union {
    uint64_t exec_phase_; // phase of execution bitmap
    struct {
      uint64_t in_parse_          : 1;
      uint64_t in_pl_parse_       : 1;
      uint64_t in_get_plan_cache_ : 1;
      uint64_t in_sql_optimize_   : 1;
      uint64_t in_sql_execution_  : 1;
      uint64_t in_px_execution_   : 1;
      uint64_t in_sequence_load_  : 1;
    };
  };

  common::ObCurTraceId::TraceId trace_id_;
  int32_t plan_line_id_; // which SQL operater the session is processing when sampling
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  bool session_type_; // false=0, FOREGROUND, true=1, BACKGROUND
#ifndef NDEBUG
  char bt_[256];
#endif
  TO_STRING_KV("id", OB_WAIT_EVENTS[event_no_].event_id_, "event", OB_WAIT_EVENTS[event_no_].event_name_, K_(wait_time));
private:
  // `last_stat_` is for wait time fix-up.
  // Fixes-up values unknown at sampling time
  // So we collect the wait time after the event finish
  ActiveSessionStat *last_stat_;
};

class ObActiveSessionGuard
{
public:
  ObActiveSessionGuard() = default;
  ~ObActiveSessionGuard() = default;
  // When Worker execution done, reset the thread local ash_stat_ to dummy_stat_
  // so that we can ensure the session stat will not be accessed after this thread switch
  // to any other task
  static void setup_default_ash();
  // set ash_stat in session to the thread local ash_stat_
  static void setup_ash(ActiveSessionStat &stat);
  static ActiveSessionStat &get_stat();
private:
  static ActiveSessionStat dummy_stat_;
  static ActiveSessionStat *&get_stat_ptr();
  DISALLOW_COPY_AND_ASSIGN(ObActiveSessionGuard);
};




}
}
#endif /* _OB_SHARE_ASH_ACTIVE_SESSION_GUARD_H_ */
//// end of header file
