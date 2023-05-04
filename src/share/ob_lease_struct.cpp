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

#define USING_LOG_PREFIX SHARE
#include "share/ob_lease_struct.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_serialization_helper.h"
namespace oceanbase
{
using namespace common;
namespace share
{

const char *server_service_status_to_str(const ServerServiceStatus status)
{
  const char *str = "UNKNOWN";
  switch (status) {
    case OBSERVER_INVALID_STATUS:
      str = "INVALID_STATUS";
      break;
    case OBSERVER_ACTIVE:
      str = "ACTIVE";
      break;
    case OBSERVER_SWITCHING:
      str = "SWITCHING";
      break;
    case OBSERVER_FLASHBACK_USER:
      str = "FLASHBACK_USER";
      break;
    case OBSERVER_FLASHBACK_INNER:
      str = "FLASHBACK_INNER";
      break;
    case OBSERVER_DISABLED:
      str = "DISABLED";
      break;
    case OBSERVER_CLEANUP:
      str = "CLEANUP";
      break;
    default:
      str = "UNKNOWN";
      break;
  }
  return str;
}

ObServerResourceInfo::ObServerResourceInfo()
{
  reset();
}

void ObServerResourceInfo::reset()
{
  cpu_ = 0;
  report_cpu_assigned_ = 0;
  report_cpu_max_assigned_ = 0;

  mem_total_ = 0;
  report_mem_assigned_ = 0;
  mem_in_use_ = 0;

  log_disk_total_ = 0;
  report_log_disk_assigned_ = 0;

  disk_total_ = 0;
  disk_in_use_ = 0;
}

bool ObServerResourceInfo::is_valid() const
{
  return cpu_ > 0
         && report_cpu_assigned_ >= 0
         && report_cpu_max_assigned_ >= 0
         && mem_total_ > 0
         && report_mem_assigned_ >= 0
         && mem_in_use_ >= 0
         && log_disk_total_ > 0
         && report_log_disk_assigned_ >= 0
         && disk_total_ > 0
         && disk_in_use_ >= 0;
}

bool ObServerResourceInfo::operator==(const ObServerResourceInfo &other) const
{
  return std::fabs(cpu_ - other.cpu_) < OB_DOUBLE_EPSINON
      && std::fabs(report_cpu_assigned_ - other.report_cpu_assigned_) < OB_DOUBLE_EPSINON
      && std::fabs(report_cpu_max_assigned_ - other.report_cpu_max_assigned_) < OB_DOUBLE_EPSINON
      && mem_total_ == other.mem_total_
      && report_mem_assigned_ ==  other.report_mem_assigned_
      && mem_in_use_ == other.mem_in_use_
      && log_disk_total_ == other.log_disk_total_
      && report_log_disk_assigned_ == other.report_log_disk_assigned_
      && disk_total_ == other.disk_total_
      && disk_in_use_ == other.disk_in_use_;
}

bool ObServerResourceInfo::operator!=(const ObServerResourceInfo &other) const
{
  return std::fabs(cpu_ - other.cpu_) > OB_DOUBLE_EPSINON
      || std::fabs(report_cpu_assigned_ - other.report_cpu_assigned_) > OB_DOUBLE_EPSINON
      || std::fabs(report_cpu_max_assigned_ - other.report_cpu_max_assigned_) > OB_DOUBLE_EPSINON
      || mem_total_ != other.mem_total_
      || report_mem_assigned_ !=  other.report_mem_assigned_
      || mem_in_use_ != other.mem_in_use_
      || log_disk_total_ != other.log_disk_total_
      || report_log_disk_assigned_ != other.report_log_disk_assigned_
      || disk_total_ != other.disk_total_
      || disk_in_use_ != other.disk_in_use_;
}

int ObServerResourceInfo::assign(const ObServerResourceInfo& other)
{
  int ret = OB_SUCCESS;

  cpu_ = other.cpu_;
  mem_in_use_ = other.mem_in_use_;
  mem_total_ = other.mem_total_;
  disk_in_use_ = other.disk_in_use_;
  disk_total_ = other.disk_total_;
  log_disk_total_ = other.log_disk_total_;
  report_log_disk_assigned_ = other.report_log_disk_assigned_;
  report_cpu_assigned_ = other.report_cpu_assigned_;
  report_cpu_max_assigned_ = other.report_cpu_max_assigned_;
  report_mem_assigned_ = other.report_mem_assigned_;

  return ret;
}

OB_SERIALIZE_MEMBER(ObServerResourceInfo,
                    cpu_,
                    report_cpu_assigned_,
                    report_cpu_max_assigned_,
                    mem_total_,
                    report_mem_assigned_,
                    mem_in_use_,
                    log_disk_total_,
                    report_log_disk_assigned_,
                    disk_total_,
                    disk_in_use_);

DEF_TO_STRING(ObServerResourceInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  (void)databuff_printf(buf, buf_len, pos,
      "cpu_capacity:%.6g, "
      "cpu_assigned:%.6g, "
      "cpu_assigned_max:%.6g, "
      "mem_capacity:\"%.9gGB\", "
      "mem_assigned:\"%.9gGB\", "
      "mem_in_use:%.9gGB, "
      "log_disk_capacity:%.9gGB, "
      "log_disk_assigned:%.9gGB, "
      "data_disk_capacity:%.9gGB, "
      "data_disk_in_use:%.9gGB",
      cpu_,
      report_cpu_assigned_,
      report_cpu_max_assigned_,
      (double)mem_total_/1024/1024/1024,
      (double)report_mem_assigned_/1024/1024/1024,
      (double)mem_in_use_/1024/1024/1024,
      (double)log_disk_total_/1024/1024/1024,
      (double)report_log_disk_assigned_/1024/1024/1024,
      (double)disk_total_/1024/1024/1024,
      (double)disk_in_use_/1024/1024/1024);
  J_OBJ_END();
  return pos;
}


OB_SERIALIZE_MEMBER(ObLeaseRequest::TLRqKeyVersion,
                    max_flushed_key_version_);

ObLeaseRequest::ObLeaseRequest()
{
  reset();
}

void ObLeaseRequest::reset()
{
  version_ = LEASE_VERSION;
  zone_.reset();
  server_.reset();
  memset(build_version_, 0, OB_SERVER_VERSION_LENGTH);
  sql_port_ = 0;
  resource_info_.reset();
  start_service_time_ = 0;
  current_server_time_ = 0;
  round_trip_time_ = 0;
  server_status_ = 0;
  ssl_key_expired_time_ = 0;
  tenant_config_version_.reset();
  timeout_partition_ = 0;
  tenant_max_flushed_key_version_.reset();
  request_lease_time_ = 0;
}

bool ObLeaseRequest::is_valid() const
{
  // No need to determine the value of server_status_
  return version_ > 0 && !zone_.is_empty() && server_.is_valid()
      && sql_port_ > 0 && resource_info_.is_valid()
      && start_service_time_ >= 0;
}

OB_SERIALIZE_MEMBER(ObLeaseRequest,
                    version_,
                    zone_,
                    server_,
                    sql_port_,
                    build_version_,
                    resource_info_,
                    start_service_time_,
                    current_server_time_,
                    round_trip_time_,
                    server_status_,
                    tenant_config_version_,
                    ssl_key_expired_time_,
                    timeout_partition_,
                    request_lease_time_,
                    tenant_max_flushed_key_version_);

OB_SERIALIZE_MEMBER(ObLeaseResponse::TLRpKeyVersion,
                    max_key_version_,
                    max_available_key_version_);

ObLeaseResponse::ObLeaseResponse()
{
  reset();
}

void ObLeaseResponse::reset()
{
  version_ = LEASE_VERSION;
  lease_expire_time_ = -1;
  lease_info_version_ = 0;
  frozen_version_ = 0;
  schema_version_ = 0;
  server_id_ = OB_INVALID_ID;
  frozen_status_.reset();
  force_frozen_status_ = false;
  rs_server_status_ = RSS_INVALID;
  refresh_schema_info_.reset();
  server_service_status_ = OBSERVER_INVALID_STATUS;
  tenant_config_version_.reset();
  baseline_schema_version_ = 0;
  heartbeat_expire_time_ = -1;
  tenant_max_key_version_.reset();
}

int ObLeaseResponse::assign(const ObLeaseResponse &other)
{
  int ret = OB_SUCCESS;
  version_ = other.version_;
  lease_expire_time_ = other.lease_expire_time_;
  lease_info_version_ = other.lease_info_version_;
  frozen_version_ = other.frozen_version_;
  schema_version_ = other.schema_version_;
  server_id_ = other.server_id_;
  frozen_status_ = other.frozen_status_;
  force_frozen_status_ = other.force_frozen_status_;
  rs_server_status_ = other.rs_server_status_;
  server_service_status_ = other.server_service_status_;
  baseline_schema_version_ = other.baseline_schema_version_;
  heartbeat_expire_time_ = other.heartbeat_expire_time_;
  if (OB_FAIL(refresh_schema_info_.assign(other.refresh_schema_info_))) {
    LOG_WARN("failed to assign ohter schema info", KR(ret), "this", *this, K(other));
  } else if (OB_FAIL(tenant_config_version_.assign(other.tenant_config_version_))) {
    LOG_WARN("failed to assign tenant config version", KR(ret), K(other), "this", *this);
  } else if (OB_FAIL(tenant_max_key_version_.assign(
          other.tenant_max_key_version_))) {
    LOG_WARN("failed to assign tenant max available key version", KR(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLeaseResponse,
                    version_,
                    lease_expire_time_,
                    lease_info_version_,
                    frozen_version_,
                    schema_version_,
                    server_id_,
                    frozen_status_,
                    force_frozen_status_,
                    rs_server_status_,
                    refresh_schema_info_,
                    server_service_status_,
                    tenant_config_version_,
                    baseline_schema_version_,
                    heartbeat_expire_time_,
                    tenant_max_key_version_);


bool ObLeaseResponse::is_valid() const
{
  // other member may be invalid value while RS restart.
  return version_ > 0
         && schema_version_ > 0
         && heartbeat_expire_time_ > 0;
}

int ObLeaseResponse::set(const ObLeaseResponse &that)
{
  int ret = OB_SUCCESS;
  version_ = that.version_;
  if (lease_expire_time_ < that.lease_expire_time_) {
    lease_expire_time_ = that.lease_expire_time_;
  }
  lease_info_version_ = that.lease_info_version_;
  frozen_version_ = that.frozen_version_;
  schema_version_ = that.schema_version_;
  server_id_ = that.server_id_;
  frozen_status_ = that.frozen_status_;
  force_frozen_status_ = that.force_frozen_status_;
  rs_server_status_ = that.rs_server_status_;
  refresh_schema_info_ = that.refresh_schema_info_;
  server_service_status_ = that.server_service_status_;
  baseline_schema_version_ = that.baseline_schema_version_;
  heartbeat_expire_time_ = that.heartbeat_expire_time_;
  if (OB_FAIL(tenant_config_version_.assign(that.tenant_config_version_))) {
    SHARE_LOG(WARN, "fail to assign array", K(ret));
  }
  return ret;
}

bool ObZoneLeaseInfo::is_valid() const
{
  return !zone_.is_empty() && privilege_version_ >= 0 && config_version_ >= 0
      && lease_info_version_ >= 0 && time_zone_info_version_ >= 0;
}

// ====================== ObInZoneHbRequest =======================
bool ObInZoneHbRequest::is_valid() const
{
  return server_.is_valid();
}

void ObInZoneHbRequest::reset()
{
  server_.reset();
}

OB_SERIALIZE_MEMBER(ObInZoneHbRequest,
                    server_);

// =================== ObInZoneHbResponse =====================
bool ObInZoneHbResponse::is_valid() const
{
  /* in_zone_hb_expire_time_ > 0 时表示in_zone_mgr还在继续授予heartbeat,
   * in_zone_hb_expire_time_ == 0 时表示in_zone_mgr不再授予heartbeat了
   * in_zone_hb_expire_time_ < 0 是表示无效
   */
  return in_zone_hb_expire_time_ >= 0;
}

void ObInZoneHbResponse::reset()
{
  in_zone_hb_expire_time_ = -1;
}

OB_SERIALIZE_MEMBER(ObInZoneHbResponse,
                    in_zone_hb_expire_time_);

} // end namespace share
} // end namespace oceanbase
