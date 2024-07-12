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
#ifndef OCEANBASE_OBSERVER_OB_HEARTBEAT_STRUCT_H_
#define OCEANBASE_OBSERVER_OB_HEARTBEAT_STRUCT_H_
#include "lib/net/ob_addr.h"
#include "share/ob_heartbeat_handler.h" // ObServerHealthStatus
#include "ob_server_status.h"
#include "ob_server_table_operator.h"
namespace oceanbase
{
namespace share
{
// volatile memory in heartbeat service
// last_hb_time_: distinguish which server is online/offline
// server_health_status_: decide whether we should stop/start server due to the change of server_health_status_
// hb_status_: to mark whether the server is alive/lease_expire/permanent_offline
struct ObServerHBInfo
{
public:
  ObServerHBInfo ();
  virtual ~ObServerHBInfo();
  int init(
      const common::ObAddr &server,
      const int64_t last_hb_time,
      const ObServerStatus::HeartBeatStatus hb_status);
  int assign(const ObServerHBInfo &other);
  bool is_valid() const;
  void reset();
  const common::ObAddr &get_server() const
  {
    return server_;
  }
  int64_t get_last_hb_time() const
  {
    return last_hb_time_;
  }
  const ObServerHealthStatus &get_server_health_status() const
  {
    return server_health_status_;
  }

  ObServerStatus::HeartBeatStatus get_hb_status() const
  {
    return hb_status_;
  }
  int set_server_health_status(const ObServerHealthStatus &server_health_status) {
    return server_health_status_.assign(server_health_status);
  }
  void set_last_hb_time(const int64_t last_hb_time) {
    last_hb_time_ = last_hb_time;
  }
  void set_hb_status(ObServerStatus::HeartBeatStatus hb_status)
  {
    hb_status_ = hb_status;
  }
  TO_STRING_KV(
      K_(server),
      K_(last_hb_time),
      K_(server_health_status),
      K_(hb_status));
private:
  common::ObAddr server_;
  int64_t last_hb_time_;
  ObServerHealthStatus server_health_status_;
  ObServerStatus::HeartBeatStatus hb_status_;
};
// heartbeat service send heartbeat requests to observers on the whitelist
// server_: the request is sent to which server
// server_id_: server_'s unique id in the cluster
// rs_addr_: the request is sent from which server. And this server is current rs leader.
// rs_server_status_: server_ is stopped or not in rs's view.
// epoch_id_: // It indicates the request is based on which whitelist_epoch_id (or which whitelist)
struct ObHBRequest
{
  OB_UNIS_VERSION(1);
public:
  ObHBRequest();
  virtual ~ObHBRequest();
  int init(
      const common::ObAddr &server,
      const uint64_t server_id,
      const common::ObAddr &rs_addr,
      const share::RSServerStatus rs_server_status,
      const int64_t epoch_id);
  int assign(const ObHBRequest &other);
  bool is_valid() const;
  void reset();
  const common::ObAddr &get_server() const
  {
    return server_;
  }
  uint64_t get_server_id() const
  {
    return server_id_;
  }
  const common::ObAddr &get_rs_addr() const
  {
    return rs_addr_;
  }
  share::RSServerStatus get_rs_server_status() const
  {
    return rs_server_status_;
  }
  int64_t get_epoch_id() const
  {
    return epoch_id_;
  }
  TO_STRING_KV(K_(server), K_(server_id), K_(rs_addr), K_(rs_server_status), K_(epoch_id));
private:
  common::ObAddr server_;
  uint64_t server_id_;
  common::ObAddr rs_addr_;
  share::RSServerStatus rs_server_status_;
  int64_t epoch_id_;
};
// servers send heartbeat responses back to heartbeat service
// to report there own zone, address, sql port, build version, start service time and health status.
struct ObHBResponse
{
  OB_UNIS_VERSION(1);
public:
  ObHBResponse();
  virtual ~ObHBResponse();
  int init(
      const common::ObZone &zone,
      const common::ObAddr &server,
      const int64_t sql_port,
      const ObServerInfoInTable::ObBuildVersion &build_version,
      const int64_t start_service_time,
      const ObServerHealthStatus server_health_status);
  int assign(const ObHBResponse &other);
  bool is_valid() const;
  void reset();
  const common::ObZone &get_zone() const
  {
    return zone_;
  }
  const common::ObAddr &get_server() const
  {
    return server_;
  }
  int64_t get_sql_port() const
  {
    return sql_port_;
  }
  const share::ObServerInfoInTable::ObBuildVersion &get_build_version() const
  {
    return build_version_;
  }
  int64_t get_start_service_time() const
  {
    return start_service_time_;
  }
  const ObServerHealthStatus &get_server_health_status() const
  {
    return server_health_status_;
  }
  TO_STRING_KV(
      K_(zone),
      K_(server),
      K_(sql_port),
      K_(build_version),
      K_(start_service_time),
      K_(server_health_status))

private:
  common::ObZone zone_;
  common::ObAddr server_;
  int64_t sql_port_;  // mysql listen port
  share::ObServerInfoInTable::ObBuildVersion build_version_;
  int64_t start_service_time_;
  ObServerHealthStatus server_health_status_;
};
} // share
} // oceanbase
#endif