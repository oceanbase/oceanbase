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

#ifndef OCEANBASE_CLOG_OB_LOG_RESTORE_MGR_H_
#define OCEANBASE_CLOG_OB_LOG_RESTORE_MGR_H_

#include "ob_log_define.h"
#include "common/ob_partition_key.h"
#include "common/ob_role.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/utility/utility.h"
#include "share/partition_table/ob_partition_info.h"

namespace oceanbase {
namespace archive {
class ObArchiveRestoreEngine;
}
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObILogSWForStateMgr;
class ObILogEngine;
class ObILogMembershipMgr;

class ObLogRestoreMgr {
public:
  ObLogRestoreMgr()
      : lock_(),
        partition_service_(NULL),
        sw_(NULL),
        log_engine_(NULL),
        mm_(NULL),
        archive_restore_engine_(NULL),
        last_takeover_time_(common::OB_INVALID_TIMESTAMP),
        last_alive_req_send_ts_(common::OB_INVALID_TIMESTAMP),
        last_leader_resp_time_(common::OB_INVALID_TIMESTAMP),
        last_renew_location_time_(common::OB_INVALID_TIMESTAMP),
        last_check_state_time_(common::OB_INVALID_TIMESTAMP),
        restore_log_finish_ts_(common::OB_INVALID_TIMESTAMP),
        role_(common::INVALID_ROLE),
        partition_key_(),
        self_(),
        restore_leader_(),
        archive_restore_state_(share::REPLICA_NOT_RESTORE),
        fetch_log_result_(OB_ARCHIVE_FETCH_LOG_INIT),
        is_inited_(false)
  {}
  ~ObLogRestoreMgr()
  {
    destroy();
  }
  void destroy();
  int init(storage::ObPartitionService* partition_service, archive::ObArchiveRestoreEngine* restore_engine,
      ObILogSWForStateMgr* sw, ObILogEngine* log_engine, ObILogMembershipMgr* mm, const common::ObPartitionKey& pkey,
      const common::ObAddr& self, const int16_t archive_restore_state);
  common::ObRole get_role() const
  {
    return role_;
  }
  common::ObAddr get_restore_leader() const
  {
    return restore_leader_;
  }
  int check_state();
  int handle_reject_msg(const common::ObAddr& sender);
  int record_leader_resp_time(const common::ObAddr& server);
  int64_t get_last_takeover_time() const
  {
    return last_takeover_time_;
  }
  int leader_takeover();
  bool is_archive_restoring() const;
  bool is_archive_restoring_log() const;
  bool is_archive_restoring_mlist() const;
  bool can_update_next_replay_log_ts_in_restore() const;
  bool is_restore_log_finished() const;
  int set_archive_restore_state(const int16_t archive_restore_state);
  void reset_archive_restore_state();
  int change_restore_leader(const common::ObAddr& new_leader);
  int set_fetch_log_result(ObArchiveFetchLogResult result);
  bool has_finished_fetching_log() const
  {
    return (OB_ARCHIVE_FETCH_LOG_FINISH_AND_NOT_ENOUGH == fetch_log_result_ ||
            OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH == fetch_log_result_);
  }
  bool has_fetched_enough_log() const
  {
    return OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH == fetch_log_result_;
  }
  int64_t get_restore_log_finish_ts() const
  {
    return restore_log_finish_ts_;
  }
  bool is_standby_restore_state() const;

private:
  int leader_takeover_();
  int leader_revoke_();
  int leader_check_role_();
  int follower_check_state_();
  int try_renew_location_();
  int try_submit_restore_task_();
  int try_send_alive_req_();

private:
  // time threshold that follower judges leader down
  static const int64_t RESTORE_LEADER_TIMEOUT_THRESHOLD = 1 * 60 * 1000 * 1000l;
  // renew location interval
  static const int64_t RENEW_LOCATION_INTERVAL = 10 * 1000 * 1000l;
  // check state interval
  static const int64_t RESTORE_CHECK_STATE_INTERVAL = 10 * 1000 * 1000l;
  // interval for follower sending alive req
  static const int64_t RESTORE_ALIVE_REQ_INTERVAL = 5 * 1000 * 1000l;

private:
  common::ObSpinLock lock_;
  storage::ObPartitionService* partition_service_;
  ObILogSWForStateMgr* sw_;
  ObILogEngine* log_engine_;
  ObILogMembershipMgr* mm_;
  archive::ObArchiveRestoreEngine* archive_restore_engine_;
  int64_t last_takeover_time_;
  int64_t last_alive_req_send_ts_;
  int64_t last_leader_resp_time_;
  int64_t last_renew_location_time_;
  int64_t last_check_state_time_;
  int64_t restore_log_finish_ts_;
  common::ObRole role_;
  common::ObPartitionKey partition_key_;
  common::ObAddr self_;
  common::ObAddr restore_leader_;
  int16_t archive_restore_state_;
  ObArchiveFetchLogResult fetch_log_result_;
  bool is_inited_;
};
}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_RESOTRE_MGR_H_
