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

#ifndef OCEANBASE_SHARE_OB_SERVER_STATUS_H_
#define OCEANBASE_SHARE_OB_SERVER_STATUS_H_

#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include "common/ob_zone.h"
#include "share/ob_lease_struct.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace share
{
namespace status
{
enum ObRootServiceStatus
{
  INVALID = -1,
  INIT = 0,
  STARTING = 1,
  IN_SERVICE,
  FULL_SERVICE,
  STARTED,
  NEED_STOP,
  STOPPING,
  MAX,
};
int get_rs_status_str(const ObRootServiceStatus status,
                      const char *&str);
}
/////
struct ObServerStatus
{
  // server admin status
  enum ServerAdminStatus
  {
    OB_SERVER_ADMIN_NORMAL,
    OB_SERVER_ADMIN_DELETING,
    OB_SERVER_ADMIN_TAKENOVER_BY_RS,
    OB_SERVER_ADMIN_MAX,
  };

  // server heart beat status
  enum HeartBeatStatus
  {
    OB_HEARTBEAT_ALIVE,
    OB_HEARTBEAT_LEASE_EXPIRED,
    OB_HEARTBEAT_PERMANENT_OFFLINE,
    OB_HEARTBEAT_MAX,
  };

  // display status (display in __all_server table)
  enum DisplayStatus
  {
    OB_SERVER_INACTIVE,
    OB_SERVER_ACTIVE,
    OB_SERVER_DELETING,
    OB_SERVER_TAKENOVER_BY_RS,
    OB_DISPLAY_MAX,
  };

  enum ClockSyncStatus
  {
    OB_CLOCK_SYNC,
    OB_CLOCK_NOT_SYNC,
    OB_CLOCK_MAX,
  };

  ObServerStatus();
  virtual ~ObServerStatus();

  bool is_status_valid() const;

  static int server_admin_status_str(const ServerAdminStatus status, const char *&str);
  static int heartbeat_status_str(const HeartBeatStatus status, const char *&str);
  static int display_status_str(const DisplayStatus status, const char *&str);
  static int str2display_status(const char *str, DisplayStatus &status);
  static int clock_sync_status_str(bool is_sync, const char *&str);

  bool is_valid() const;
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  bool is_active() const { return is_alive() && OB_SERVER_ADMIN_NORMAL == admin_status_; }
  bool is_deleting() const { return OB_SERVER_ADMIN_DELETING == admin_status_; }
  bool is_taken_over_by_rs() const { return OB_SERVER_ADMIN_TAKENOVER_BY_RS == admin_status_; }

  bool is_alive() const { return OB_HEARTBEAT_ALIVE == hb_status_; }
  bool is_temporary_offline() const { return OB_HEARTBEAT_LEASE_EXPIRED == hb_status_; }
  bool is_permanent_offline() const { return OB_HEARTBEAT_PERMANENT_OFFLINE == hb_status_; }
  bool is_permanent_offline_and_without_partition() const { return OB_HEARTBEAT_PERMANENT_OFFLINE == hb_status_ && !with_partition_; }
  bool is_with_partition() const { return with_partition_; }
  bool need_check_empty(const int64_t now) const {
    return !is_alive()
           && with_partition_
           && last_hb_time_ + GCONF.lease_time + common::OB_MAX_ADD_MEMBER_TIMEOUT < now;
  }
  DisplayStatus get_display_status() const;

  void block_migrate_in() { block_migrate_in_time_ = common::ObTimeUtility::current_time(); }
  void unblock_migrate_in() { block_migrate_in_time_ = 0; }
  bool is_migrate_in_blocked() const { return 0 != block_migrate_in_time_; }
  bool is_stopped() const { return 0 != stop_time_; }

  bool in_service() const { return 0 != start_service_time_; }
  bool can_migrate_in() const { return is_active() && !is_migrate_in_blocked(); }
  uint64_t get_server_id() const { return id_; }

  uint64_t id_;
  common::ObZone zone_;
  char build_version_[common::OB_SERVER_VERSION_LENGTH];
  common::ObAddr server_;
  int64_t sql_port_;    // sql listen port
  int64_t register_time_;
  int64_t last_hb_time_;
  int64_t block_migrate_in_time_;
  int64_t stop_time_; //stop server time
  int64_t start_service_time_;
  int64_t last_offline_time_; //last offline timestamp
  int64_t last_server_behind_time_;
  int64_t last_round_trip_time_;

  // Merged version report by server merge finish.
  // Can only be used to wakeup daily merge thread. (invalid between RS start and server merge finish)
  int64_t merged_version_;
  ServerAdminStatus admin_status_;
  HeartBeatStatus hb_status_;
  bool with_rootserver_;
  bool with_partition_;
  bool force_stop_hb_;

  ObServerResourceInfo resource_info_;
  int64_t leader_cnt_;
  int64_t server_report_status_;
  // lease_expire_time_ is only valid under single-zone mode
  int64_t lease_expire_time_;
  int64_t ssl_key_expired_time_;
  bool in_recovery_for_takenover_by_rs_;

private:
  static int get_status_str(const char *strs[],
      const int64_t strs_len, const int64_t status, const char *&str);
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_SERVER_STATUS_H_
