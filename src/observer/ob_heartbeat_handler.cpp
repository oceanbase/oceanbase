/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER
#include "observer/ob_heartbeat_handler.h"

#include "observer/ob_server.h"
#include "share/ob_version.h"
#include "observer/ob_service.h"

namespace oceanbase
{
namespace observer
{
static const char *OB_DATA_DISK_STATUS_STR[] = {"INVALID", "NORMAL", "ERROR"};
OB_SERIALIZE_MEMBER(
    ObServerHealthStatus,
    data_disk_status_
)
ObServerHealthStatus::ObServerHealthStatus()
    : data_disk_status_(ObDataDiskStatus::DATA_DISK_STATUS_INVALID)
{
}
ObServerHealthStatus::~ObServerHealthStatus()
{
}
int ObServerHealthStatus::init(ObDataDiskStatus data_disk_status)
{
  int ret = OB_SUCCESS;
  if (data_disk_status <= DATA_DISK_STATUS_INVALID || data_disk_status >= DATA_DISK_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_disk_status));
  } else {
    data_disk_status_ = data_disk_status;
  }
  return ret;
}
int ObServerHealthStatus::assign(const ObServerHealthStatus server_health_status)
{
  int ret = OB_SUCCESS;
  data_disk_status_ = server_health_status.data_disk_status_;
  return ret;
}
void ObServerHealthStatus::reset()
{
  data_disk_status_ = ObDataDiskStatus::DATA_DISK_STATUS_INVALID;
}
bool ObServerHealthStatus::is_valid() const
{
  return data_disk_status_ > ObDataDiskStatus::DATA_DISK_STATUS_INVALID
      && data_disk_status_ < ObDataDiskStatus::DATA_DISK_STATUS_MAX;
}
bool ObServerHealthStatus::is_healthy() const
{
  return ObDataDiskStatus::DATA_DISK_STATUS_NORMAL == data_disk_status_;
}
const char *ObServerHealthStatus::data_disk_status_to_str(const ObDataDiskStatus data_disk_status)
{
  STATIC_ASSERT(ARRAYSIZEOF(OB_DATA_DISK_STATUS_STR) == DATA_DISK_STATUS_MAX, "array size mismatch");
  const char *str = "UNKNOWN";
  if (OB_UNLIKELY(data_disk_status >= ARRAYSIZEOF(OB_DATA_DISK_STATUS_STR)
                  || data_disk_status < DATA_DISK_STATUS_INVALID)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown data disk status", K(data_disk_status));
  } else {
    str = OB_DATA_DISK_STATUS_STR[data_disk_status];
  }
  return str;
}
ObHeartbeatHandler::ObHeartbeatHandler()
{
}
ObHeartbeatHandler::~ObHeartbeatHandler()
{
}
int64_t ObHeartbeatHandler::rs_epoch_id_ = palf::INVALID_PROPOSAL_ID;
bool ObHeartbeatHandler::is_rs_epoch_id_valid()
{
  return palf::INVALID_PROPOSAL_ID != ATOMIC_LOAD(&rs_epoch_id_);
}
int ObHeartbeatHandler::handle_heartbeat(
    const share::ObHBRequest &hb_request,
    share::ObHBResponse &hb_response)
{
  int ret = OB_SUCCESS;
  hb_response.reset();
  int64_t rs_epoch_id = ATOMIC_LOAD(&rs_epoch_id_);
  if (OB_UNLIKELY(!hb_request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("receive an invalid heartbeat request", KR(ret), K(hb_request));
  } else if (OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs manager is null", KR(ret), KP(GCTX.rs_mgr_));
  } else {
    const int64_t epoch_id = hb_request.get_epoch_id();
    if (rs_epoch_id < epoch_id || palf::INVALID_PROPOSAL_ID == rs_epoch_id) {
      LOG_INFO("receive new rs epoch", "old rs_epoch_id", rs_epoch_id, "new rs_epoch_id", epoch_id);
      int64_t current_epoch_id = ATOMIC_CAS(&rs_epoch_id_, rs_epoch_id, epoch_id);
      if (rs_epoch_id != current_epoch_id) {
        ret = OB_NEED_RETRY;
        LOG_WARN("set rs_epoch_id_failed", KR(ret), K(rs_epoch_id), K(epoch_id), K(current_epoch_id));
      }
    } else if (rs_epoch_id > epoch_id) {
      ret = OB_RS_NOT_MASTER;
      LOG_WARN("this rs is not the newest leader", KR(ret), K(rs_epoch_id), K(epoch_id));
    }
  }
  if (FAILEDx(GCTX.rs_mgr_->force_set_master_rs(hb_request.get_rs_addr()))) {
    LOG_WARN("fail to set master rs", KR(ret), K(hb_request.get_rs_addr()));
  } else if (OB_FAIL(init_hb_response_(hb_response))) {
    LOG_WARN("fail to init hb response", KR(ret));
  } else {
    // const uint64_t server_id = hb_request.get_server_id();
    const share::RSServerStatus rs_server_status = hb_request.get_rs_server_status();
    // if (GCTX.server_id_ != server_id) {
    //   LOG_INFO("receive new server id", "old server_id_", GCTX.server_id_, "new server_id_", server_id);
    //   GCTX.server_id_ = server_id;
    // }
    if (GCTX.rs_server_status_ != rs_server_status) {
      LOG_INFO("receive new server status recorded in rs",
          "old_status", GCTX.rs_server_status_,
          "new_status", rs_server_status);
      GCTX.rs_server_status_ = rs_server_status;
    }
  }
  return ret;
}
int ObHeartbeatHandler::check_disk_status_(ObServerHealthStatus &server_health_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
  int64_t abnormal_time = 0;
  server_health_status.reset();
  if (OB_TMP_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, abnormal_time))) {
    LOG_WARN("fail to get device health status", KR(ret), KR(tmp_ret));
  } else if (OB_UNLIKELY(DEVICE_HEALTH_ERROR == dhs)) {
    const int64_t PRINT_LOG_INTERVAL_IN_US = 60 * 1000 * 1000; // 1min
    if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_IN_US)) {
      LOG_WARN("error occurs on data disk, ",
          "data_disk_health_status", device_health_status_to_str(dhs), K(abnormal_time));
    }
  }
  const bool is_data_disk_error = (DEVICE_HEALTH_ERROR == dhs);
  if (is_data_disk_error) {
    server_health_status.init(ObServerHealthStatus::DATA_DISK_STATUS_ERROR);
  } else {
    server_health_status.init(ObServerHealthStatus::DATA_DISK_STATUS_NORMAL);
  }
  return ret;
}
ERRSIM_POINT_DEF(ERRSIM_DISK_ERROR);
int ObHeartbeatHandler::init_hb_response_(share::ObHBResponse &hb_response)
{
  int ret = OB_SUCCESS;
  ObServerHealthStatus server_health_status;
  if (OB_FAIL(check_disk_status_(server_health_status))) {
    LOG_WARN("fail to check disk status", KR(ret));
  } else {
    int64_t sql_port = GCONF.mysql_port;
    share::ObServerInfoInTable::ObBuildVersion build_version;
    common::ObZone zone;
    int64_t test_id = ERRSIM_DISK_ERROR ? 2 : OB_INVALID_ID;
    if (test_id == GCTX.server_id_) {
      server_health_status.reset();
      server_health_status.init(ObServerHealthStatus::DATA_DISK_STATUS_ERROR);
    }
    if (OB_FAIL(zone.assign(GCONF.zone.str()))) {
      LOG_WARN("fail to assign zone", KR(ret), K(GCONF.zone.str()));
    } else if (OB_FAIL(ObService::get_build_version(build_version))) {
      LOG_WARN("fail to get build_version", KR(ret), K(build_version));
    } else if (OB_FAIL(hb_response.init(
        zone,
        GCTX.self_addr(),
        sql_port,
        build_version,
        GCTX.start_service_time_,
        server_health_status))) {
      LOG_WARN("fail to init the heartbeat response", KR(ret), K(zone), K(GCTX.self_addr()),
          K(sql_port), K(build_version), K(GCTX.start_service_time_), K(server_health_status));
    } else {}
  }
  return ret;
}
} // observer
} // oceanbase