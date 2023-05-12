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
#include "share/ob_server_status.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace status
{
int get_rs_status_str(const ObRootServiceStatus status,
                      const char *&str)
{
  int ret = OB_SUCCESS;
  static const char *strs[] = {"invalid", "init", "starting", "in_service", "full_service", "started",
    "need_stop", "stopping", "max"};
  if (status < 0 || status >= MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(status));
  } else {
    str = strs[status];
  }
  return ret;
}
}

ObServerStatus::ObServerStatus()
{
  reset();
}

ObServerStatus::~ObServerStatus()
{
}

void ObServerStatus::reset()
{
  id_ = OB_INVALID_ID;
  zone_.reset();
  memset(build_version_, 0, sizeof(build_version_));
  server_.reset();
  sql_port_ = 0;
  register_time_ = 0;
  last_hb_time_ = 0;
  block_migrate_in_time_ = 0;
  stop_time_ = 0;
  start_service_time_ = 0;
  last_offline_time_ = 0;
  last_server_behind_time_ = 0;
  last_round_trip_time_ = 0;
  admin_status_ = OB_SERVER_ADMIN_MAX;
  hb_status_ = OB_HEARTBEAT_MAX;
  merged_version_ = 0;
  with_rootserver_ = false;
  with_partition_ = false;
  force_stop_hb_ = false;
  resource_info_.reset();
  leader_cnt_ = -1;
  server_report_status_ = 0;
  lease_expire_time_ = 0;
  ssl_key_expired_time_ = 0;
  in_recovery_for_takenover_by_rs_ = false;
}

bool ObServerStatus::is_status_valid() const
{
  return (admin_status_ >= 0 && admin_status_ < OB_SERVER_ADMIN_MAX)
      && (hb_status_ >= 0 && hb_status_ < OB_HEARTBEAT_MAX);
}

bool ObServerStatus::is_valid() const
{
  return is_valid_server_id(id_) && server_.is_valid() && is_status_valid()
      && register_time_ >= 0 && last_hb_time_ >= 0 && block_migrate_in_time_ >= 0
      && stop_time_ >= 0 && start_service_time_ >= 0;
}

static const char *g_server_display_status_str[] = {
  "INACTIVE",
  "ACTIVE",
  "DELETING",
  "TAKEOVER_BY_RS"
};

int ObServerStatus::display_status_str(const DisplayStatus status, const char *&str)
{
  STATIC_ASSERT(OB_DISPLAY_MAX == ARRAYSIZEOF(g_server_display_status_str),
      "status string array size mismatch");
  int ret = OB_SUCCESS;
  if (status < 0 || status >= OB_DISPLAY_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else if (OB_FAIL(get_status_str(g_server_display_status_str,
      ARRAYSIZEOF(g_server_display_status_str), status, str))) {
    LOG_WARN("get status str failed", K(ret), K(status));
  }
  return ret;
}

int ObServerStatus::str2display_status(const char *str, ObServerStatus::DisplayStatus &status)
{
  int ret = OB_SUCCESS;
  if (NULL == str) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(str));
  } else {
    status = OB_DISPLAY_MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(g_server_display_status_str); ++i) {
      if (STRCASECMP(g_server_display_status_str[i], str) == 0) {
        status = static_cast<DisplayStatus>(i);
        break;
      }
    }
    if (OB_DISPLAY_MAX == status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("display status str not found", K(ret), K(str));
    }
  }
  return ret;
}

int ObServerStatus::server_admin_status_str(const ServerAdminStatus status, const char *&str)
{
  static const char *strs[] = { "NORMAL", "DELETING", "TAKENOVER_BY_RS" };
  STATIC_ASSERT(OB_SERVER_ADMIN_MAX == ARRAYSIZEOF(strs), "status string array size mismatch");
  int ret = OB_SUCCESS;
  if (status < 0 || status >= OB_SERVER_ADMIN_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else if (OB_FAIL(get_status_str(strs, ARRAYSIZEOF(strs), status, str))) {
    LOG_WARN("get status str failed", K(ret), K(status));
  }
  return ret;
}

int ObServerStatus::clock_sync_status_str(bool is_sync, const char *&str)
{
  static const char *strs[] = { "SYNC", "NOT SYNC" };
  STATIC_ASSERT(OB_CLOCK_MAX == ARRAYSIZEOF(strs), "status string array size mismatch");
  int ret = OB_SUCCESS;
  const ClockSyncStatus status = is_sync ? ClockSyncStatus::OB_CLOCK_SYNC : ClockSyncStatus::OB_CLOCK_NOT_SYNC;
  if (status < 0 || status >= OB_CLOCK_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else if (OB_FAIL(get_status_str(strs, ARRAYSIZEOF(strs), status, str))) {
    LOG_WARN("get status str failed", K(ret), K(status));
  }
  return ret;
}

int ObServerStatus::heartbeat_status_str(const HeartBeatStatus status, const char *&str)
{
  int ret = OB_SUCCESS;
  static const char *strs[] = {"alive", "lease_expired", "permanent_offline" };
  STATIC_ASSERT(OB_HEARTBEAT_MAX == ARRAYSIZEOF(strs), "status string array size mismatch");
  if (status < 0 || status >= OB_HEARTBEAT_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else if (OB_FAIL(get_status_str(strs, ARRAYSIZEOF(strs), status, str))) {
    LOG_WARN("get status str failed", K(ret), K(status));
  }
  return ret;
}

int ObServerStatus::get_status_str(const char *strs[], const int64_t strs_len,
    const int64_t status, const char *&str)
{
  int ret = OB_SUCCESS;
  str = NULL;
  if (NULL == strs || strs_len <= 0 || status < 0 || status >= strs_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(strs), K(strs_len), K(status), K(strs_len));
  } else {
    str = strs[status];
  }
  return ret;
}

ObServerStatus::DisplayStatus ObServerStatus::get_display_status() const
{
  DisplayStatus status = OB_SERVER_ACTIVE;
  if (OB_SERVER_ADMIN_NORMAL == admin_status_) {
    if (is_alive()) {
      status = OB_SERVER_ACTIVE;
    } else {
      status = OB_SERVER_INACTIVE;
    }
  } else if (OB_SERVER_ADMIN_DELETING == admin_status_) {
    status = OB_SERVER_DELETING;
  } else {
    status = OB_SERVER_TAKENOVER_BY_RS;
  }
  return status;
}

int64_t ObServerStatus::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    const char *admin_str = NULL;
    const char *heartbeat_str = NULL;
    // ignore get_xxx_str error, and do not check NULL str
    int tmp_ret = server_admin_status_str(admin_status_, admin_str);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("get server admin status str failed", K(tmp_ret), K_(admin_status));
    }
    if (OB_SUCCESS != (tmp_ret = heartbeat_status_str(hb_status_, heartbeat_str))) {
      LOG_WARN("get heartbeat status str failed", K(tmp_ret), K_(hb_status));
    }
    J_OBJ_START();
    J_KV("server", server_,
        "id", id_,
        "zone", zone_,
        "build_version", build_version_,
        "sql_port", sql_port_,
        "register_time", register_time_,
        "last_hb_time", last_hb_time_,
        "block_migrate_in_time", block_migrate_in_time_,
        "stop_time", stop_time_,
        "start_service_time", start_service_time_,
        "last_offline_time", last_offline_time_,
        "last_server_behind_time", last_server_behind_time_,
        "last_round_trip_time", last_round_trip_time_,
        "admin_status", admin_str,
        "hb_status", heartbeat_str,
        "with_rootserver", with_rootserver_,
        "with_partition", with_partition_,
        "resource_info", resource_info_,
        "leader_cnt", leader_cnt_,
        "server_report_status", server_report_status_,
        "lease_expire_time", lease_expire_time_,
        "ssl_key_expired_time", ssl_key_expired_time_,
        "in_recovery_for_takenover_by_rs", in_recovery_for_takenover_by_rs_);
    J_OBJ_END();
  }
  return pos;
}
}//end namespace share
}//end namespace oceanbase
