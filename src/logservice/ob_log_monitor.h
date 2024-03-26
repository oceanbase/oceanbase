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

#ifdef OB_BUILD_ARBITRATION
#include "logservice/ob_arbitration_service.h"
#endif
#include "palf/palf_callback.h"

namespace oceanbase
{
namespace logservice
{

class ObLogMonitor : public palf::PalfMonitorCb
#ifdef OB_BUILD_ARBITRATION
, public IObArbitrationMonitor
#endif
{
public:
  ObLogMonitor() { }
  virtual ~ObLogMonitor() { }
public:
  // =========== PALF Event Reporting ===========
  int record_set_initial_member_list_event(const int64_t palf_id,
                                           const int64_t replica_num,
                                           const char *member_list = NULL,
                                           const char *extra_info = NULL) override final;
  int record_election_leader_change_event(const int64_t palf_id, const common::ObAddr &dest_addr) override final;
  int record_reconfiguration_event(const char *sub_event,
                                   const int64_t palf_id,
                                   const palf::LogConfigVersion& config_version,
                                   const int64_t prev_replica_num,
                                   const int64_t curr_replica_num,
                                   const char *extra_info = NULL) override final;
  int record_replica_type_change_event(const int64_t palf_id,
                                       const palf::LogConfigVersion& config_version,
                                       const char *prev_replica_type,
                                       const char *curr_replica_type,
                                       const char *extra_info = NULL) override final;
  int record_access_mode_change_event(const int64_t palf_id,
                                      const int64_t prev_mode_version,
                                      const int64_t curr_mode_verion,
                                      const palf::AccessMode& prev_access_mode,
                                      const palf::AccessMode& curr_access_mode,
                                      const char *extra_info = NULL) override final;
  int record_set_base_lsn_event(const int64_t palf_id, const palf::LSN &new_base_lsn) override final;
  int record_enable_sync_event(const int64_t palf_id) override final;
  int record_disable_sync_event(const int64_t palf_id) override final;
  int record_enable_vote_event(const int64_t palf_id) override final;
  int record_disable_vote_event(const int64_t palf_id) override final;
  int record_advance_base_info_event(const int64_t palf_id, const palf::PalfBaseInfo &palf_base_info) override final;
  int record_rebuild_event(const int64_t palf_id,
                           const common::ObAddr &server,
                           const palf::LSN &base_lsn) override final;
  int record_flashback_event(const int64_t palf_id,
                             const int64_t mode_version,
                             const share::SCN &flashback_scn,
                             const share::SCN &curr_end_scn,
                             const share::SCN &curr_max_scn) override final;
  int record_truncate_event(const int64_t palf_id,
                            const palf::LSN &lsn,
                            const int64_t min_block_id,
                            const int64_t max_block_id,
                            const int64_t truncate_end_block_id) override final;
  int record_role_change_event(const int64_t palf_id,
                               const common::ObRole &prev_role,
                               const palf::ObReplicaState &prev_state,
                               const common::ObRole &curr_role,
                               const palf::ObReplicaState &curr_state,
                               const char *extra_info = NULL) override final;
  int record_parent_child_change_event(const int64_t palf_id,
                                       const bool is_register, /* true: register; false; retire; */
                                       const bool is_parent,   /* true: parent; false: child; */
                                       const common::ObAddr &server,
                                       const common::ObRegion &region,
                                       const int64_t register_time_us,
                                       const char *extra_info = NULL) override final;
  // =========== PALF Event Reporting ===========
public:
  // =========== PALF Performance Statistic ===========
  int add_log_write_stat(const int64_t palf_id, const int64_t log_write_size) override final;
  // =========== PALF Performance Statistic ===========
#ifdef OB_BUILD_ARBITRATION
public:
  // =========== Arbitration Event Reporting ===========
  int record_degrade_event(const int64_t palf_id, const char *degraded_list, const char *reasons) override final;
  int record_upgrade_event(const int64_t palf_id, const char *upgraded_list, const char *reasons) override final;
  // =========== Arbitration Event Reporting ===========
#endif
private:
  enum EventType
  {
    UNKNOWN = 0,
    DEGRADE,
    UPGRADE,
    SET_INITIAL_MEMBER_LIST,
    ELECTION_LEADER_CHANGE,
    ROLE_TRANSITION,
    RECONFIGURATION,
    REPLICA_TYPE_TRANSITION,
    ACCESS_MODE_TRANSITION,
    SET_BASE_LSN,
    ENABLE_SYNC,
    DISABLE_SYNC,
    ENABLE_VOTE,
    DISABLE_VOTE,
    ADVANCE_BASE_INFO,
    REBUILD,
    FLASHBACK,
    TRUNCATE,
  };

  const char *type_to_string_(const EventType &event) const
  {
    #define CHECK_LOG_EVENT_TYPE_STR(x) case(EventType::x): return #x
    switch (event)
    {
      CHECK_LOG_EVENT_TYPE_STR(DEGRADE);
      CHECK_LOG_EVENT_TYPE_STR(UPGRADE);
      case (EventType::SET_INITIAL_MEMBER_LIST):
        return "SET INITIAL MEMBER LIST";
      case (EventType::ELECTION_LEADER_CHANGE):
        return "ELECTION LEADER CHANGE";
      case (EventType::ROLE_TRANSITION):
        return "ROLE TRANSITION";
      case (EventType::RECONFIGURATION):
        return "RECONFIGURATION";
      case (EventType::REPLICA_TYPE_TRANSITION):
        return "REPLICA TYPE TRANSITION";
      case (EventType::ACCESS_MODE_TRANSITION):
        return "ACCESS MODE TRANSITION";
      case (EventType::SET_BASE_LSN):
        return "SET BASE LSN";
      case (EventType::ENABLE_SYNC):
        return "ENABLE SYNC";
      case (EventType::DISABLE_SYNC):
        return "DISABLE SYNC";
      case (EventType::ENABLE_VOTE):
        return "ENABLE VOTE";
      case (EventType::DISABLE_VOTE):
        return "DISABLE VOTE";
      case (EventType::ADVANCE_BASE_INFO):
        return "ADVANCE BASE INFO";
      CHECK_LOG_EVENT_TYPE_STR(REBUILD);
      CHECK_LOG_EVENT_TYPE_STR(FLASHBACK);
      CHECK_LOG_EVENT_TYPE_STR(TRUNCATE);
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
