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
#define LOG_MONITOR_EVENT_STR_FMT_PREFIX "LOG", event_str, "TENANT_ID", mtl_id, "LS_ID", palf_id

// =========== PALF Event Reporting ===========
int ObLogMonitor::record_set_initial_member_list_event(const int64_t palf_id,
                                                       const int64_t replica_num,
                                                       const char *member_list,
                                                       const char *extra_info)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::SET_INITIAL_MEMBER_LIST;
  if (OB_NOT_NULL(extra_info)) {
    SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
        "MEMBER_LIST", member_list,
        "REPLICA_NUM", replica_num,
        "", NULL,
        "", NULL,
        extra_info);
  } else {
    SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
        "MEMBER_LIST", member_list,
        "REPLICA_NUM", replica_num);
  }
  return ret;
}

int ObLogMonitor::record_election_leader_change_event(const int64_t palf_id, const common::ObAddr &dest_addr)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::ELECTION_LEADER_CHANGE;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX, "LEADER", dest_addr);
  return ret;
}

int ObLogMonitor::record_reconfiguration_event(const char *sub_event,
                                               const int64_t palf_id,
                                               const palf::LogConfigVersion& config_version,
                                               const int64_t prev_replica_num,
                                               const int64_t curr_replica_num,
                                              const char* extra_info)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::RECONFIGURATION;
  const int64_t MAX_BUF_LEN = 50;
  char event_str[MAX_BUF_LEN] = {'\0'};
  if (0 >= snprintf(event_str, MAX_BUF_LEN, "%s:%s", type_to_string_(event), sub_event)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", KR(ret), K(event), K(sub_event));
  } else if (OB_NOT_NULL(extra_info)) {
    SERVER_EVENT_ADD_WITH_RETRY("LOG", event_str,
        "TENANT_ID", mtl_id,
        "LS_ID", palf_id,
        "CONFIG_VERSION", config_version,
        "PREV_REPLICA_NUM", prev_replica_num,
        "CURR_REPLICA_NUM", curr_replica_num,
        "", NULL,
        extra_info);
  } else {
    SERVER_EVENT_ADD_WITH_RETRY("LOG", event_str,
        "TENANT_ID", mtl_id,
        "LS_ID", palf_id,
        "CONFIG_VERSION", config_version,
        "PREV_REPLICA_NUM", prev_replica_num,
        "CURR_REPLICA_NUM", curr_replica_num);
  }
  return ret;
}
int ObLogMonitor::record_replica_type_change_event(const int64_t palf_id,
                                                   const palf::LogConfigVersion& config_version,
                                                   const char *prev_replica_type,
                                                   const char *curr_replica_type,
                                                   const char *extra_info)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::REPLICA_TYPE_TRANSITION;
  if (OB_NOT_NULL(extra_info)) {
    SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
        "CONFIG_VERSION", config_version,
        "PREV_REPLICA_TYPE", prev_replica_type,
        "CURR_REPLICA_TYPE", curr_replica_type,
        "", NULL,
        extra_info);
  } else {
    SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
        "CONFIG_VERSION", config_version,
        "PREV_REPLICA_TYPE", prev_replica_type,
        "CURR_REPLICA_TYPE", curr_replica_type);
  }

  return ret;
}

int ObLogMonitor::record_access_mode_change_event(const int64_t palf_id,
                                                  const int64_t prev_mode_version,
                                                  const int64_t curr_mode_verion,
                                                  const palf::AccessMode& prev_access_mode,
                                                  const palf::AccessMode& curr_access_mode,
                                                  const char *extra_info)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::ACCESS_MODE_TRANSITION;
  const int64_t MAX_BUF_LEN = 32;
  char prev_access_mode_str[MAX_BUF_LEN] = {'\0'};
  char curr_access_mode_str[MAX_BUF_LEN] = {'\0'};
  if (OB_FAIL(palf::access_mode_to_string(prev_access_mode, prev_access_mode_str, sizeof(prev_access_mode_str)))) {
    PALF_LOG(WARN, "access_mode_to_string failed", K(prev_access_mode));
  } else if (OB_FAIL(palf::access_mode_to_string(curr_access_mode, curr_access_mode_str, sizeof(curr_access_mode_str)))) {
    PALF_LOG(WARN, "access_mode_to_string failed", K(prev_access_mode));
  } else if (OB_NOT_NULL(extra_info)) {
    SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
        "PREV_MODE_VERSION", prev_mode_version,
        "CURR_MODE_VERSION", curr_mode_verion,
        "PREV_ACCESS_MODE", prev_access_mode_str,
        "CURR_ACCESS_MODE", curr_access_mode_str,
        extra_info);
  } else {
    SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
        "PREV_MODE_VERSION", prev_mode_version,
        "CURR_MODE_VERSION", curr_mode_verion,
        "PREV_ACCESS_MODE", prev_access_mode_str,
        "CURR_ACCESS_MODE", curr_access_mode_str);
  }
  return ret;
}

int ObLogMonitor::record_set_base_lsn_event(const int64_t palf_id, const palf::LSN &new_base_lsn)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::SET_BASE_LSN;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
      "NEW_BASE_LSN", new_base_lsn);
  return ret;
}

int ObLogMonitor::record_enable_sync_event(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::ENABLE_SYNC;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX);
  return ret;
}

int ObLogMonitor::record_disable_sync_event(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::DISABLE_SYNC;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX);
  return ret;
}

int ObLogMonitor::record_enable_vote_event(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::ENABLE_VOTE;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX);
  return ret;
}

int ObLogMonitor::record_disable_vote_event(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::DISABLE_VOTE;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX);
  return ret;
}

int ObLogMonitor::record_advance_base_info_event(const int64_t palf_id, const palf::PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::ADVANCE_BASE_INFO;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
      "PALF_BASE_INFO", palf_base_info);
  return ret;
}

int ObLogMonitor::record_rebuild_event(const int64_t palf_id,
                                       const common::ObAddr &server,
                                       const palf::LSN &base_lsn)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::REBUILD;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
      "SOURCE_SERVER", server,
      "BASE_LSN", base_lsn);
  return ret;
}

int ObLogMonitor::record_flashback_event(const int64_t palf_id,
                                         const int64_t mode_version,
                                         const share::SCN &flashback_scn,
                                         const share::SCN &curr_end_scn,
                                         const share::SCN &curr_max_scn)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::FLASHBACK;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
      "MODE_VERSION", mode_version,
      "FLASHBACK_SCN", flashback_scn,
      "CURR_END_SCN", curr_end_scn,
      "CURR_MAX_SCN", curr_max_scn);
  return ret;
}

int ObLogMonitor::record_truncate_event(const int64_t palf_id,
                                        const palf::LSN &lsn,
                                        const int64_t min_block_id,
                                        const int64_t max_block_id,
                                        const int64_t truncate_end_block_id)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::TRUNCATE;
  SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_FMT_PREFIX,
      "LSN", lsn,
      "MIN_BLOCK_ID", min_block_id,
      "MAX_BLOCK_ID", max_block_id,
      "TRUNCATE_END_BLOCK_ID", truncate_end_block_id);
  return ret;
}

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

int ObLogMonitor::record_parent_child_change_event(const int64_t palf_id,
                                                   const bool is_register, /* true: register; false; retire; */
                                                   const bool is_parent,   /* true: parent; false: child; */
                                                   const common::ObAddr &server,
                                                   const common::ObRegion &region,
                                                   const int64_t register_time_us,
                                                   const char *extra_info)
{
  int ret = OB_SUCCESS;
  int pret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const char *action_str = (is_register)? "REGISTER": "RETIRE";
  const char *object_str = (is_parent)? "PARENT": "CHILD";
  const int64_t MAX_BUF_LEN = 50;
  char event_str[MAX_BUF_LEN] = {'\0'};
  TIMEGUARD_INIT(LOG_MONITOR, 100_ms, 5_s);                                                                         \
  if (0 >= (pret = snprintf(event_str, MAX_BUF_LEN, "%s %s", action_str, object_str))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", KR(ret), K(action_str), K(object_str));
  } else {
    CLICK();
    if (OB_NOT_NULL(extra_info)) {
      SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_STR_FMT_PREFIX,
          object_str, server,
          "REGION", region,
          "REGISTER_TIME_US", register_time_us,
          "", NULL,
          extra_info);
    } else {
      SERVER_EVENT_ADD_WITH_RETRY(LOG_MONITOR_EVENT_STR_FMT_PREFIX,
          object_str, server,
          "REGION", region,
          "REGISTER_TIME_US", register_time_us,
          "", NULL);
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

#ifdef OB_BUILD_ARBITRATION
// =========== Arbitration Event Reporting ===========
#define ARBSRV_MONITOR_EVENT_FMT_PREFIX "ARB", type_to_string_(event), "TENANT_ID", mtl_id, "LS_ID", palf_id
int ObLogMonitor::record_degrade_event(const int64_t palf_id, const char *degraded_list, const char *reasons)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::DEGRADE;
  SERVER_EVENT_ADD_WITH_RETRY(ARBSRV_MONITOR_EVENT_FMT_PREFIX,
      "DEGRADED LIST", degraded_list,
      "REASONS", reasons);
  return ret;
}

int ObLogMonitor::record_upgrade_event(const int64_t palf_id, const char *upgraded_list, const char *reasons)
{
  int ret = OB_SUCCESS;
  const int64_t mtl_id = MTL_ID();
  const EventType event = EventType::UPGRADE;
  SERVER_EVENT_ADD_WITH_RETRY(ARBSRV_MONITOR_EVENT_FMT_PREFIX,
      "UPGRADED LIST", upgraded_list,
      "REASONS", reasons);
  return ret;
}
#undef ARBSRV_MONITOR_EVENT_FMT_PREFIX
// =========== Arbitration Event Reporting ===========
#endif

#undef LOG_MONITOR_EVENT_FMT_PREFIX
} // end namespace logservice
} // end namespace oceanbase