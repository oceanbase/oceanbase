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

#ifndef OCEANBASE_SHARE_OB_ALL_SERVER_TRACER_H_
#define OCEANBASE_SHARE_OB_ALL_SERVER_TRACER_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_iserver_trace.h"
#include "share/ob_server_table_operator.h"
#include "lib/function/ob_function.h"

namespace oceanbase
{
namespace share
{

class ObServerTraceMap : public share::ObIServerTrace
{
public:
  ObServerTraceMap();
  virtual ~ObServerTraceMap();
  int init();
  virtual int is_server_exist(const common::ObAddr &server, bool &exist) const;
  virtual int get_server_rpc_port(const common::ObAddr &server, const int64_t sql_port,
                                          int64_t &rpc_port, bool &exist) const;
  virtual int check_server_alive(const common::ObAddr &server, bool &is_alive) const;
  virtual int check_in_service(const common::ObAddr &addr, bool &service_started) const;
  virtual int check_migrate_in_blocked(const common::ObAddr &addr, bool &is_block) const;
  virtual int check_server_permanent_offline(const common::ObAddr &server, bool &is_offline) const;
  virtual int check_server_active(const common::ObAddr &server, bool &is_active) const;
  virtual int check_server_can_migrate_in(const common::ObAddr &server, bool &can_migrate_in) const;
  virtual int is_server_stopped(const common::ObAddr &server, bool &is_stopped) const;
  virtual int get_server_zone(const common::ObAddr &server, common::ObZone &zone) const;
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers) const;
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers,
      common::ObIArray<uint64_t> &server_id_list) const;
  virtual int get_server_info(const common::ObAddr &server, ObServerInfoInTable &server_info) const;
  virtual int get_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &servers_info,
      bool include_permanent_offline) const;
  virtual int get_active_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &active_servers_info) const;
  virtual int get_alive_servers(const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list) const;
  virtual int get_alive_servers_count(const common::ObZone &zone, int64_t &count) const;
  virtual int get_alive_and_not_stopped_servers(const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list) const;
  virtual int get_servers_by_status(
      const ObZone &zone,
      common::ObIArray<common::ObAddr> &alive_server_list,
      common::ObIArray<common::ObAddr> &not_alive_server_list) const;
  virtual int get_min_server_version(
              char min_server_version_str[OB_SERVER_VERSION_LENGTH],
              uint64_t &min_observer_version);
  bool has_build() const {return has_build_; };
  int refresh();
  int for_each_server_info(const ObFunction<int(const ObServerInfoInTable &server_info)> &functor);

private:
  int find_server_status(const ObAddr &addr, ObServerInfoInTable &status) const;
  int get_rpc_port_status(const ObAddr &addr, const int64_t sql_port,
                          int64_t &rpc_port, ObServerInfoInTable &status) const;
  int find_server_info(const ObAddr &addr, ObServerInfoInTable &server_info) const;

private:
  static const int64_t DEFAULT_SERVER_COUNT = 2048;
  bool is_inited_;
  bool has_build_;
  mutable common::SpinRWLock lock_;
  common::ObArray<ObServerInfoInTable> server_info_arr_;
};

class ObServerTraceTask : public common::ObTimerTask
{
public:
  ObServerTraceTask();
  virtual ~ObServerTraceTask();
  int init(ObServerTraceMap *trace_map, int tg_id);
  virtual void runTimerTask();
  TO_STRING_KV(KP_(trace_map));
private:
  const static int64_t REFRESH_INTERVAL_US = 5L * 1000 * 1000;
  ObServerTraceMap *trace_map_;
  bool is_inited_;
};

class ObAllServerTracer : public share::ObIServerTrace
{
public:
  static ObAllServerTracer &get_instance();
  int init(int tg_id, ObServerTraceTask &trace_task);
  int for_each_server_info(const ObFunction<int(const ObServerInfoInTable &server_info)> &functor);
  virtual int is_server_exist(const common::ObAddr &server, bool &exist) const;
  virtual int get_server_rpc_port(const common::ObAddr &server, const int64_t sql_port,
                                          int64_t &rpc_port, bool &exist) const;
  virtual int check_server_alive(const common::ObAddr &server, bool &is_alive) const;
  virtual int check_in_service(const common::ObAddr &addr, bool &service_started) const;
  virtual int check_server_permanent_offline(const common::ObAddr &server, bool &is_offline) const;
  virtual int is_server_stopped(const common::ObAddr &server, bool &is_stopped) const;
  virtual int check_migrate_in_blocked(const common::ObAddr &addr, bool &is_block) const;
  virtual int get_server_zone(const common::ObAddr &server, common::ObZone &zone) const;
  // empty zone means that get all servers
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers) const;
  // empty zone means that get all servers
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers,
      common::ObIArray<uint64_t> &server_id_list) const;
  virtual int get_server_info(
      const common::ObAddr &server,
      ObServerInfoInTable &server_info) const;
  virtual int get_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &servers_info,
      bool include_permanent_offline = true) const;
  virtual int get_active_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &active_servers_info) const;
  virtual int get_alive_servers(const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list) const;
  virtual int check_server_active(const common::ObAddr &server, bool &is_active) const;
  virtual int refresh();
  virtual int check_server_can_migrate_in(const common::ObAddr &server, bool &can_migrate_in) const;
  virtual int get_alive_servers_count(const common::ObZone &zone, int64_t &count) const;
  virtual int get_alive_and_not_stopped_servers(const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list) const;
  virtual int get_servers_by_status(
      const ObZone &zone,
      common::ObIArray<common::ObAddr> &alive_server_list,
      common::ObIArray<common::ObAddr> &not_alive_server_list) const;
  virtual int get_min_server_version(
              char min_server_version_str[OB_SERVER_VERSION_LENGTH],
              uint64_t &min_observer_version);
  bool has_build() const;
private:
  ObAllServerTracer();
  virtual ~ObAllServerTracer();
private:
  bool is_inited_;
  ObServerTraceMap trace_map_;
};

}  // end namespace share
}  // end namespace oceanbase

#define SVR_TRACER (::oceanbase::share::ObAllServerTracer::get_instance())

#endif  // OCEANBASE_SHARE_OB_ALL_SERVER_TRACER_H_
