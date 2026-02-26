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
#define USING_LOG_PREFIX SHARE
#include "share/ob_heartbeat_struct.h"
namespace oceanbase
{
using namespace common;
namespace share
{
OB_SERIALIZE_MEMBER(
    ObHBRequest,
    server_,
    server_id_,
    rs_addr_,
    rs_server_status_,
    epoch_id_);
OB_SERIALIZE_MEMBER(
    ObHBResponse,
    zone_,
    server_,
    sql_port_,
    build_version_,
    start_service_time_,
    server_health_status_);
ObServerHBInfo::ObServerHBInfo ()
    : server_(),
      last_hb_time_(0),
      server_health_status_(),
      hb_status_(ObServerStatus::OB_HEARTBEAT_MAX)
{
}
ObServerHBInfo::~ObServerHBInfo()
{
}
int ObServerHBInfo::init(
    const common::ObAddr &server,
    const int64_t last_hb_time,
    const ObServerStatus::HeartBeatStatus hb_status)
{
  int ret = OB_SUCCESS;
  server_health_status_.reset();
  if (OB_UNLIKELY(!server.is_valid()
      || last_hb_time <= 0
      || hb_status >= ObServerStatus::OB_HEARTBEAT_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(last_hb_time), K(hb_status));
  } else if (OB_FAIL(server_health_status_.init(ObServerHealthStatus::DATA_DISK_STATUS_NORMAL))) {
    LOG_WARN("fail to init server_health_status_", KR(ret));
  } else {
    server_ = server;
    last_hb_time_ = last_hb_time;
    hb_status_ = hb_status;
  }
  return ret;
}
int ObServerHBInfo::assign(const ObServerHBInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_health_status_.assign(other.server_health_status_))) {
    LOG_WARN("fail to assign server_health_status_", KR(ret), K(other.server_health_status_));
  } else {
    server_ = other.server_;
    last_hb_time_ = other.last_hb_time_;
    hb_status_ = other.hb_status_;
  }
  return ret;
}
bool ObServerHBInfo::is_valid() const
{
  return server_.is_valid()
      && last_hb_time_ > 0
      && hb_status_ < ObServerStatus::OB_HEARTBEAT_MAX
      && server_health_status_.is_valid();
}
void ObServerHBInfo::reset()
{
  server_.reset();
  last_hb_time_ = 0;
  server_health_status_.reset();
  hb_status_ = ObServerStatus::OB_HEARTBEAT_MAX;
}
ObHBRequest::ObHBRequest()
    : server_(),
    server_id_(OB_INVALID_ID),
    rs_addr_(),
    rs_server_status_(RSS_INVALID),
    epoch_id_(palf::INVALID_PROPOSAL_ID)
{
}
ObHBRequest::~ObHBRequest()
{
}
int ObHBRequest::init(
    const common::ObAddr &server,
    const uint64_t server_id,
    const common::ObAddr &rs_addr,
    const share::RSServerStatus rs_server_status,
    const int64_t epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid()
      || !is_valid_server_id(server_id)
      || !rs_addr.is_valid()
      || palf::INVALID_PROPOSAL_ID == epoch_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(server_id), K(rs_addr), K(epoch_id));
  } else {
    server_ = server;
    server_id_ = server_id;
    rs_addr_ = rs_addr;
    rs_server_status_ = rs_server_status;
    epoch_id_ = epoch_id;
  }
  return ret;
}
int ObHBRequest::assign(const ObHBRequest &other)
{
  int ret = OB_SUCCESS;
  server_ = other.server_;
  server_id_  = other.server_id_;
  rs_addr_ = other.rs_addr_;
  rs_server_status_ = other.rs_server_status_;
  epoch_id_ = other.epoch_id_;
  return ret;
}
bool ObHBRequest::is_valid() const
{
  return server_.is_valid()
      && is_valid_server_id(server_id_)
      && rs_addr_.is_valid()
      && rs_server_status_ > RSS_INVALID
      && rs_server_status_ < RSS_MAX
      && palf::INVALID_PROPOSAL_ID != epoch_id_;
}
void ObHBRequest::reset()
{
  server_.reset();
  server_id_ = OB_INVALID_ID;
  rs_addr_.reset();
  rs_server_status_ = RSS_INVALID;
  epoch_id_ = palf::INVALID_PROPOSAL_ID;
}
ObHBResponse::ObHBResponse()
    : zone_(),
      server_(),
      sql_port_(0),
      build_version_(),
      start_service_time_(0),
      server_health_status_()
{
}
ObHBResponse::~ObHBResponse()
{
}
int ObHBResponse::init(
    const common::ObZone &zone,
    const common::ObAddr &server,
    const int64_t sql_port,
    const ObServerInfoInTable::ObBuildVersion &build_version,
    const int64_t start_service_time,
    const ObServerHealthStatus server_health_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty()
      || !server.is_valid()
      || sql_port <= 0
      || build_version.is_empty()
      || start_service_time < 0
      || !server_health_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone), K(server), K(sql_port), K(build_version),
        K(start_service_time), K(server_health_status));
  } else {
    if (OB_FAIL(zone_.assign(zone))) {
      LOG_WARN("fail to init zone", KR(ret),  K(zone));
    } else if (OB_FAIL(build_version_.assign(build_version))) {
      LOG_WARN("fail to init build_version_", KR(ret), K(build_version));
    } else if (OB_FAIL(server_health_status_.assign(server_health_status))) {
      LOG_WARN("fail to init server_health_status_", KR(ret), K(server_health_status));
    } else {
      server_ = server;
      sql_port_ = sql_port;
      start_service_time_ = start_service_time;
    }
  }
  return ret;
}
int ObHBResponse::assign(const ObHBResponse &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_.assign(other.zone_))) {
    LOG_WARN("fail to assign zone", KR(ret), K(other.zone_));
  } else if (OB_FAIL(build_version_.assign(other.build_version_))) {
    LOG_WARN("fail to assign build version", KR(ret), K(other.build_version_));
  } else if (OB_FAIL(server_health_status_.assign(other.server_health_status_))) {
    LOG_WARN("fail to assign server_health_status_", KR(ret), K(other.server_health_status_));
  } else {
    server_ = other.server_;
    sql_port_ = other.sql_port_;
    start_service_time_ = other.start_service_time_;
  }
  return ret;
}
bool ObHBResponse::is_valid() const
{
  return !zone_.is_empty()
      && server_.is_valid()
      && sql_port_ > 0
      && !build_version_.is_empty()
      && start_service_time_ >= 0
      && server_health_status_.is_valid();
}
void ObHBResponse::reset()
{
  zone_.reset();
  server_.reset();
  sql_port_ = 0;
  build_version_.reset();
  start_service_time_ = 0;
  server_health_status_.reset();
}
} // share
} // oceanbase