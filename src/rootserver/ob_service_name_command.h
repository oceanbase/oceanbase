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

#ifndef OCEANBASE_ROOTSERVER_OB_SERVICE_NAME_COMMAND_H
#define OCEANBASE_ROOTSERVER_OB_SERVICE_NAME_COMMAND_H

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include "share/ob_service_name_proxy.h"
#include "sql/session/ob_sql_session_mgr.h"
namespace oceanbase
{
namespace rootserver
{
class ObServiceNameKillSessionFunctor
{
public:
    ObServiceNameKillSessionFunctor()
        : tenant_id_(OB_INVALID_TENANT_ID), service_name_(), killed_connection_list_(NULL) {};
    ~ObServiceNameKillSessionFunctor() {};
    int init(const uint64_t tenant_id,
    const share::ObServiceNameString &service_name,
    ObArray<uint64_t> *killed_connection_list);
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
private:
    uint64_t tenant_id_;
    ObServiceNameString service_name_;
    ObArray<uint64_t> *killed_connection_list_;
};
class ObServiceNameCommand
{
public:
  ObServiceNameCommand();
  ~ObServiceNameCommand();
  static int create_service(
      const uint64_t tenant_id,
      const share::ObServiceNameString &service_name_str);
  static int delete_service(
      const uint64_t tenant_id,
      const share::ObServiceNameString &service_name_str);
  static int start_service(
      const uint64_t tenant_id,
      const share::ObServiceNameString &service_name_str);
  static int stop_service(
      const uint64_t tenant_id,
      const share::ObServiceNameString &service_name_str);
  static int kill_local_connections(
    const uint64_t tenant_id,
    const share::ObServiceName &service_name);
private:
  static int check_and_get_tenants_servers_(
      const uint64_t tenant_id,
      const bool include_temp_offline,
      common::ObIArray<common::ObAddr> &target_servers);
  static int server_check_and_push_back_(
      const common::ObAddr &server,
      const bool include_temp_offline,
      common::ObIArray<common::ObAddr> &target_servers);
  static int broadcast_refresh_(
      const uint64_t tenant_id,
      const share::ObServiceNameID &target_service_name_id,
      const share::ObServiceNameArg::ObServiceOp &service_op,
      const common::ObIArray<common::ObAddr> &target_servers,
      const int64_t epoch,
      const ObArray<share::ObServiceName> &all_service_names);
  static int extract_service_name_(
      const ObArray<share::ObServiceName> &all_service_names,
      const share::ObServiceNameString &service_name_str,
      share::ObServiceName &service_name);
};
} // end namespace rootserver
} // end namespace oceanbase
#endif