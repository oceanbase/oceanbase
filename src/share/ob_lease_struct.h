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

#ifndef OCEANBASE_SHARE_HEARTBEAT_OB_LEASE_STRUCT_H_
#define OCEANBASE_SHARE_HEARTBEAT_OB_LEASE_STRUCT_H_

#include "share/ob_define.h"
#include "share/ob_cluster_info_proxy.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/net/ob_addr.h"
#include "common/ob_zone.h"
#include "common/ob_role.h"
#include "common/storage/ob_freeze_define.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace share {
// Observer status recognized by RootService
// RSS_IS_STOPPED after stop server/stop zone,
// RSS_IS_WORKING in other cases
enum RSServerStatus {
  RSS_INVALID,
  RSS_IS_WORKING,
  RSS_IS_STOPPED,
  RSS_MAX,
};

// OBS judges whether it can provide log services based on this value;
enum ServerServiceStatus {
  OBSERVER_INVALID_STATUS = 0,
  OBSERVER_ACTIVE,
  OBSERVER_SWITCHING,
  OBSERVER_FLASHBACK,
  OBSERVER_DISABLED,
  // Mainly to distinguish lossless failover and lossy failover.
  // Lossless failover will take over in the cleanup phase, lossy failover must take over in the flashback phase
  // Taking office during the flashback phase is lossy.
  OBSERVER_CLEANUP,
};

enum ServerPreProceStatus {
  SPPS_INVALID_STATUS = 0,
  SPPS_SERVER_NOT_EXIST,
  SPPS_PRE_PROCE_NOT_START,
  SPPS_IN_PRE_PROCE,
  SPPS_PRE_PROCE_FINISHED,
};

const char* server_service_status_to_str(const ServerServiceStatus status);

// Is the state value represented by a bit field
enum LeaseRequestServerStatus {
  LEASE_REQUEST_NORMAL = 0,
  LEASE_REQUEST_DATA_DISK_ERROR = 0x1,
};

struct ObServerResourceInfo {
  OB_UNIS_VERSION(1);

public:
  double cpu_;
  int64_t mem_in_use_;  // in KB
  int64_t mem_total_;
  int64_t disk_in_use_;
  int64_t disk_total_;
  int64_t partition_cnt_;

  double report_cpu_assigned_;
  double report_cpu_max_assigned_;
  int64_t report_mem_assigned_;
  int64_t report_mem_max_assigned_;

  ObServerResourceInfo();
  void reset();
  bool is_valid() const;
  bool operator==(const ObServerResourceInfo& other) const;
  bool operator!=(const ObServerResourceInfo& other) const;

  TO_STRING_KV(K_(cpu), K_(mem_in_use), K_(mem_total), K_(disk_in_use), K_(disk_total), K_(partition_cnt),
      K_(report_cpu_assigned), K_(report_cpu_max_assigned), K_(report_mem_assigned), K_(report_mem_max_assigned));
};

struct ObLeaseRequest {
  OB_UNIS_VERSION(1);

public:
  static const int64_t LEASE_VERSION = 1;
  // Server lease length, 50s
  static const int64_t SERVICE_LEASE = 40 * 1000000;
  // rs waits for 30s after the lease timeout, and then tries to take over the observer,
  // This 30s is to wait for the end of the writing of the observer who lost his heartbeat
  static const int64_t RS_TAKENOVER_OBS_INTERVAL = 30 * 1000000;
  int64_t version_;
  common::ObZone zone_;
  common::ObAddr server_;
  int64_t inner_port_;  // mysql listen port
  char build_version_[common::OB_SERVER_VERSION_LENGTH];
  ObServerResourceInfo resource_info_;
  int64_t start_service_time_;
  int64_t current_server_time_;
  int64_t round_trip_time_;
  int64_t server_status_;
  int64_t ssl_key_expired_time_;
  common::ObSEArray<std::pair<uint64_t, int64_t>, 10> tenant_config_version_;
  // The order cannot be changed, this is a new field added by 22x
  int64_t timeout_partition_;
  /*
   * 1. General deployment cluster summary, request_lease_time_ is not used, it is always 0
   * 2. In the scenario of single zone ofs deployment, request_lease_time_ is not 0, which means a lease is requested.
   * request_lease_time_ is 0, which means to report other observer information.
   */
  int64_t request_lease_time_;

  ObLeaseRequest();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(version), K_(zone), K_(server), K_(inner_port), K_(build_version), K_(resource_info),
      K_(start_service_time), K_(current_server_time), K_(round_trip_time), K_(tenant_config_version),
      K_(request_lease_time), K_(ssl_key_expired_time), K_(timeout_partition));
};

struct ObLeaseResponse {
  OB_UNIS_VERSION(1);

public:
  static const int64_t LEASE_VERSION = 1;
  int64_t version_;
  int64_t lease_expire_time_;
  int64_t lease_info_version_;  // check whether need to update info from __all_zone_stat
  int64_t frozen_version_;
  int64_t schema_version_;
  uint64_t server_id_;
  storage::ObFrozenStatus frozen_status_;
  bool force_frozen_status_;
  RSServerStatus rs_server_status_;
  int64_t global_max_decided_trans_version_;
  share::schema::ObRefreshSchemaInfo refresh_schema_info_;
  ObClusterInfo cluster_info_;
  ServerServiceStatus server_service_status_;
  common::ObSEArray<std::pair<uint64_t, int64_t>, 10> tenant_config_version_;
  int64_t baseline_schema_version_;
  common::ObSEArray<int64_t, 1> sync_cluster_ids_;
  share::ObRedoTransportOption redo_options_;
  int64_t heartbeat_expire_time_;

  ObLeaseResponse();
  int set(const ObLeaseResponse& that);
  TO_STRING_KV(K_(version), K_(lease_expire_time), K_(lease_info_version), K_(frozen_version), K_(schema_version),
      K_(server_id), K_(frozen_status), K_(force_frozen_status), K_(rs_server_status),
      K_(global_max_decided_trans_version), K_(refresh_schema_info), K_(cluster_info), K_(server_service_status),
      K_(tenant_config_version), K_(baseline_schema_version), K_(heartbeat_expire_time), K_(sync_cluster_ids),
      K_(redo_options));

  void reset();
  bool is_valid() const;
  int assign(const ObLeaseResponse& other);
};

struct ObInZoneHbRequest {
  OB_UNIS_VERSION(1);

public:
  ObInZoneHbRequest() : server_()
  {}

public:
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(server));

public:
  common::ObAddr server_;
};

struct ObInZoneHbResponse {
  OB_UNIS_VERSION(1);

public:
  ObInZoneHbResponse() : in_zone_hb_expire_time_(-1)
  {}

public:
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(in_zone_hb_expire_time));

public:
  int64_t in_zone_hb_expire_time_;
};

struct ObZoneLeaseInfo {
  common::ObZone zone_;
  int64_t privilege_version_;
  int64_t config_version_;
  int64_t lease_info_version_;
  int64_t broadcast_version_;
  int64_t last_merged_version_;         // zone merged version
  int64_t global_last_merged_version_;  // cluster merged version
  int64_t time_zone_info_version_;
  bool suspend_merging_;
  int64_t warm_up_start_time_;
  int64_t sys_var_version_;  // system variable version
  int64_t proposal_frozen_version_;

  ObZoneLeaseInfo()
      : zone_(),
        privilege_version_(0),
        config_version_(0),
        lease_info_version_(0),
        broadcast_version_(0),
        last_merged_version_(0),
        global_last_merged_version_(0),
        time_zone_info_version_(0),
        suspend_merging_(false),
        warm_up_start_time_(0),
        sys_var_version_(0),
        proposal_frozen_version_(0)
  {}
  TO_STRING_KV(K_(zone), K_(privilege_version), K_(config_version), K_(lease_info_version), K_(broadcast_version),
      K_(last_merged_version), K_(global_last_merged_version), K_(time_zone_info_version), K_(suspend_merging),
      K_(warm_up_start_time), K_(sys_var_version), K_(proposal_frozen_version));

  bool is_valid() const;
};

}  // end namespace share
}  // end namespace oceanbase
#endif
