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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_MONITOR_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_MONITOR_H_

#include "palf/palf_callback.h"

namespace oceanbase
{
namespace logservice
{

class ObLogMonitor : public palf::PalfMonitorCb
{
public:
  ObLogMonitor() { }
  virtual ~ObLogMonitor() { }
public:
  // =========== PALF Event Reporting ===========
  int record_role_change_event(const int64_t palf_id,
                               const common::ObRole &prev_role,
                               const palf::ObReplicaState &prev_state,
                               const common::ObRole &curr_role,
                               const palf::ObReplicaState &curr_state,
                               const char *extra_info = NULL) override final;
  // =========== PALF Event Reporting ===========
public:
  // =========== PALF Performance Statistic ===========
  int add_log_write_stat(const int64_t palf_id, const int64_t log_write_size) override final;
  // =========== PALF Performance Statistic ===========
private:
  enum EventType
  {
    UNKNOWN = 0,
    DEGRADE,
    UPGRADE,
    ROLE_TRANSITION,
  };

  const char *type_to_string_(const EventType &event) const
  {
    #define CHECK_LOG_EVENT_TYPE_STR(x) case(EventType::x): return #x
    switch (event)
    {
      CHECK_LOG_EVENT_TYPE_STR(DEGRADE);
      CHECK_LOG_EVENT_TYPE_STR(UPGRADE);
      case (EventType::ROLE_TRANSITION):
        return "ROLE TRANSITION";
      default:
        return "UNKNOWN";
    }
    #undef CHECK_LOG_EVENT_TYPE_STR
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMonitor);
};

} // logservice
} // oceanbase

#endif
