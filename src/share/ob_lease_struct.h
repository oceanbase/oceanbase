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
#include "lib/string/ob_fixed_length_string.h"
#include "lib/net/ob_addr.h"
#include "common/ob_zone.h"
#include "common/ob_role.h"
#include "common/storage/ob_freeze_define.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
// Observer status recognized by RootService
// RSS_IS_STOPPED after stop server/stop zone,
// RSS_IS_WORKING in other cases
enum RSServerStatus
{
  RSS_INVALID,
  RSS_IS_WORKING,
  RSS_IS_STOPPED,
  RSS_MAX,
};

// OBS judges whether it can provide log services based on this value;
enum ServerServiceStatus
{
  OBSERVER_INVALID_STATUS = 0,
  OBSERVER_ACTIVE = 1,
  OBSERVER_SWITCHING = 2,
  OBSERVER_FLASHBACK_USER = 3,
  OBSERVER_DISABLED = 4,
  //Mainly to distinguish lossless failover and lossy failover.
  //Lossless failover will take over in the cleanup phase, lossy failover must take over in the flashback phase
  //Taking office during the flashback phase is lossy.
  OBSERVER_CLEANUP = 5,
  OBSERVER_FLASHBACK_INNER = 6,
};

enum ServerPreProceStatus
{
  SPPS_INVALID_STATUS = 0,
  SPPS_SERVER_NOT_EXIST,
  SPPS_PRE_PROCE_NOT_START,
  SPPS_IN_PRE_PROCE,
  SPPS_PRE_PROCE_FINISHED,
};

const char *server_service_status_to_str(const ServerServiceStatus status);

// Is the state value represented by a bit field
enum LeaseRequestServerStatus
{
  LEASE_REQUEST_NORMAL = 0,
  LEASE_REQUEST_DATA_DISK_ERROR = 0x1,
};

struct ObServerResourceInfo
{
  OB_UNIS_VERSION(1);
public:
  double cpu_;                          // CPU总容量
  double report_cpu_assigned_;          // CPU已分配大小: server所有unit min_cpu总和
  double report_cpu_max_assigned_;      // CPU最大已分配大小: server所有unit max_cpu总和

  int64_t mem_total_;                   // 内存总容量
  int64_t report_mem_assigned_;         // 内存已分配大小：server所有unit memory_size总和
  int64_t mem_in_use_;                  // 已使用内存大小

  int64_t log_disk_total_;              // 日志盘总容量
  int64_t report_log_disk_assigned_;    // 日志盘已分配大小：server所有unit log_disk_size总和

  int64_t disk_total_;                  // 数据盘总容量大小
  int64_t disk_in_use_;                 // 数据盘已使用大小


  ObServerResourceInfo();
  void reset();
  bool is_valid() const;
  bool operator==(const ObServerResourceInfo &other) const;
  bool operator!=(const ObServerResourceInfo &other) const;
  int assign(const ObServerResourceInfo& other);

  DECLARE_TO_STRING;
};

struct ObLeaseRequest
{
  OB_UNIS_VERSION(1);
public:
  struct TLRqKeyVersion
  {
    OB_UNIS_VERSION(1);
  public:
    TLRqKeyVersion() : max_flushed_key_version_(0) {}
    uint64_t max_flushed_key_version_;
    TO_STRING_KV(K(max_flushed_key_version_));
  };
  static const int64_t LEASE_VERSION = 1;
  // Server lease length, 50s
  static const int64_t SERVICE_LEASE = 40 * 1000000;
  // rs waits for 30s after the lease timeout, and then tries to take over the observer,
  // This 30s is to wait for the end of the writing of the observer who lost his heartbeat
  static const int64_t RS_TAKENOVER_OBS_INTERVAL = 30 * 1000000;
  int64_t version_;
  common::ObZone zone_;
  common::ObAddr server_;
  int64_t sql_port_;  // mysql listen port
  char build_version_[common::OB_SERVER_VERSION_LENGTH];
  ObServerResourceInfo resource_info_;
  int64_t start_service_time_;
  int64_t current_server_time_;
  int64_t round_trip_time_;
  int64_t server_status_;
  int64_t ssl_key_expired_time_;
  common::ObSEArray<std::pair<uint64_t, int64_t>, 10> tenant_config_version_;
  //The order cannot be changed, this is a new field added by 22x
  int64_t timeout_partition_;
  int64_t request_lease_time_;
  common::ObSEArray<std::pair<uint64_t, TLRqKeyVersion>, 10> tenant_max_flushed_key_version_;
  /* 1 常规部署集群汇总，request_lease_time_不被使用，始终为0
   * 2 单zone ofs部署的场景下，request_lease_time_不为0，表示请求lease，
   *   request_lease_time_为0，表示汇报其他observer信息。
   */

  ObLeaseRequest();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(version), K_(zone), K_(server), K_(sql_port),
      K_(build_version), K_(resource_info), K_(start_service_time),
      K_(current_server_time), K_(round_trip_time), K_(tenant_config_version),
      K_(ssl_key_expired_time), K_(timeout_partition),
      K_(tenant_max_flushed_key_version), K_(request_lease_time));
};

struct ObLeaseResponse
{
  OB_UNIS_VERSION(1);
public:
  struct TLRpKeyVersion
  {
    OB_UNIS_VERSION(1);
  public:
    TLRpKeyVersion() : max_key_version_(0), max_available_key_version_(0) {}
    uint64_t max_key_version_;
    uint64_t max_available_key_version_;
    TO_STRING_KV(K(max_key_version_), K(max_available_key_version_));
  };
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
  share::schema::ObRefreshSchemaInfo refresh_schema_info_;
  ServerServiceStatus server_service_status_;
  common::ObSEArray<std::pair<uint64_t, int64_t>, 10> tenant_config_version_;
  int64_t baseline_schema_version_;
  int64_t heartbeat_expire_time_;
  common::ObSEArray<std::pair<uint64_t, TLRpKeyVersion>, 10> tenant_max_key_version_;

  ObLeaseResponse();
  int set(const ObLeaseResponse &that);
  TO_STRING_KV(K_(version), K_(lease_expire_time), K_(lease_info_version),
      K_(frozen_version), K_(schema_version), K_(server_id), K_(frozen_status),
      K_(force_frozen_status), K_(rs_server_status),
      K_(refresh_schema_info),
      K_(server_service_status), K_(tenant_config_version),
      K_(baseline_schema_version),
      K_(tenant_max_key_version), K_(heartbeat_expire_time));

  void reset();
  bool is_valid() const;
  int assign(const ObLeaseResponse &other);

};

struct ObInZoneHbRequest
{
  OB_UNIS_VERSION(1);
public:
  ObInZoneHbRequest() : server_() {}
public:
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(server));
public:
  common::ObAddr server_;
};

struct ObInZoneHbResponse
{
  OB_UNIS_VERSION(1);
public:
  ObInZoneHbResponse() : in_zone_hb_expire_time_(-1) {}
public:
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(in_zone_hb_expire_time));
public:
  int64_t in_zone_hb_expire_time_;
};


struct ObZoneLeaseInfo
{
  common::ObZone zone_;
  int64_t privilege_version_;
  int64_t config_version_;
  int64_t lease_info_version_;
  int64_t time_zone_info_version_;
  int64_t sys_var_version_; // system variable version

  ObZoneLeaseInfo(): zone_(), privilege_version_(0), config_version_(0),
    lease_info_version_(0), time_zone_info_version_(0), sys_var_version_(0)
  {
  }
  TO_STRING_KV(K_(zone), K_(privilege_version), K_(config_version), K_(lease_info_version),
               K_(time_zone_info_version), K_(sys_var_version));

  bool is_valid() const;
};

} // end namespace share
} // end namespace oceanbase
#endif
