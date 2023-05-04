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

#include "ob_log_monitor.h"
#include "observer/ob_server_event_history_table_operator.h"   // SERVER_EVENT_ADD_WITH_RETRY

namespace oceanbase
{
namespace logservice
{
#define LOG_MONITOR_EVENT_FMT_PREFIX "LOG", type_to_string_(event), "TENANT_ID", mtl_id, "LS_ID", palf_id

// =========== PALF Event Reporting ===========
int ObLogMonitor::record_role_change_event(const int64_t palf_id,
                                           const common::ObRole &prev_role,
                                           const palf::ObReplicaState &prev_state,
                                           const common::ObRole &curr_role,
                                           const palf::ObReplicaState &curr_state,
                                           const char *extra_info)
{
  int ret = OB_SUCCESS;
  int pret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::ROLE_TRANSITION;
  const int64_t MAX_BUF_LEN = 50;
  char prev_str[MAX_BUF_LEN] = {'\0'};
  char curr_str[MAX_BUF_LEN] = {'\0'};
  TIMEGUARD_INIT(LOG_MONITOR, 100_ms, 5_s);                                                                         \
  if (0 >= (pret = snprintf(prev_str, MAX_BUF_LEN, "%s %s", role_to_string(prev_role),
      replica_state_to_string(prev_state)))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", KR(ret), K(prev_role), K(prev_state));
  } else if (0 >= (pret = snprintf(curr_str, MAX_BUF_LEN, "%s %s", role_to_string(curr_role),
      replica_state_to_string(curr_state)))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", KR(ret), K(curr_role), K(curr_state));
  } else {
    CLICK();
    if (OB_NOT_NULL(extra_info)) {
      SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
          "PREVIOUS", prev_str,
          "CURRENT", curr_str,
          "", NULL,
          "", NULL,
          extra_info);
    } else {
      SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
          "PREVIOUS", prev_str,
          "CURRENT", curr_str);
    }
    CLICK();
  }
  return ret;
}
// =========== PALF Event Reporting ===========

// =========== PALF Performance Statistic ===========
int ObLogMonitor::add_log_write_stat(const int64_t palf_id, const int64_t log_write_size)
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}
// =========== PALF Performance Statistic ===========


#undef LOG_MONITOR_EVENT_FMT_PREFIX
} // end namespace logservice
} // end namespace oceanbase