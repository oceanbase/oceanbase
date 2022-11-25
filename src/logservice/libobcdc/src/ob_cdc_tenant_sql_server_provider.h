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
 *
 * ObCDCTenantSQLServerProvider define
 */

#ifndef OCEANBASE_LIBOBCDC_TENANT_SQL_SERVER_PROVIDER_H__
#define OCEANBASE_LIBOBCDC_TENANT_SQL_SERVER_PROVIDER_H__

#include "lib/lock/ob_spin_rwlock.h"                    // SpinRWLock
#include "lib/hash/ob_link_hashmap.h"                   // ObLinkHashMap
#include "lib/mysqlclient/ob_mysql_server_provider.h"   // ObMySQLServerProvider

#include "ob_log_systable_helper.h"                     // IObLogSysTableHelper
#include "ob_log_svr_blacklist.h"                       // ObLogSvrBlacklist

namespace oceanbase
{
namespace libobcdc
{
class ObLogSysTableHelper;
class ObLogSvrBlacklist;
class ObLogConfig;

typedef common::ObSEArray<uint64_t, 16> TenantList;
typedef common::ObSEArray<common::ObAddr, 16> ServerList;
struct TenantServerList : public LinkHashValue<common::sqlclient::TenantMapKey>
{
  TenantServerList() : server_list_(ObModIds::OB_LOG_SERVER_PROVIDER, OB_MALLOC_NORMAL_BLOCK_SIZE) {}
  ~TenantServerList() { server_list_.reset(); }

  ServerList &get_server_list() { return server_list_; } // used to build tenant_server_list
  const ServerList &get_server_list() const { return server_list_; } // used to assign to connection_pool refresh
  int64_t get_server_count() const { return server_list_.count(); }

  ServerList server_list_;
  TO_STRING_KV(K_(server_list));
};

typedef common::ObLinkHashMap<common::sqlclient::TenantMapKey, TenantServerList> TenantServerMap;
class ObCDCTenantSQLServerProvider : public common::sqlclient::ObMySQLServerProvider
{
public:
  ObCDCTenantSQLServerProvider();
  virtual ~ObCDCTenantSQLServerProvider() { destroy(); }
public:
  // provide all_server in cluster(ignore standby cluster if exists).
  virtual int get_server(const int64_t svr_idx, common::ObAddr &server) override;
  // get all_server count
  virtual int64_t get_server_count() const override;
  // get all_tenant_id list of current cluster
  virtual int get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids) override;
  // get servers that serves specified tenant
  virtual int get_tenant_servers(
      const uint64_t tenant_id,
      common::ObIArray<common::ObAddr> &tenant_servers) override;
  // refresh server_list in cluster
  // 1. get all server addr(ip, rpc_port, sql_port)
  // 2. filter by server_blacklist_(ip:rpc_port)
  // 3. store svr(ip:sql_port) in server_list_
  virtual int refresh_server_list(void) override;
  // Thread safe guarateed by:
  //   (1) lock while prepare_refresh and unlock while end_refresh
  //   (2) MySQLConnectionPoll refresh server_list with lock
  // process flow:
  // 1. lock refresh_server_lock_ to refresh server_list
  // 2. clear server_list_ and tenant_server_map_
  // 3. query all_server_info and add into server_list
  // 4. query tenant_server_list and add into tenant_server_map_
  virtual int prepare_refresh() override;
  // release refresh_server_lock_
  virtual int end_refresh() override;
public:
  int init(IObLogSysTableHelper &systable_helper);
  void destroy();
  // black_list
  void configure(const ObLogConfig &config);
  int del_tenant(const uint64_t tenant_id);
private:
  // query info from OceanBase Cluster by systable_helper
  int query_all_tenant_();
  int query_all_server_();
  int query_tenant_server_();
private:
  bool                  is_inited_;
  IObLogSysTableHelper  *systable_helper_;
  TenantList            tenant_list_;
  ServerList            server_list_;
  TenantServerMap       tenant_server_map_;
  ObLogSvrBlacklist     server_blacklist_; // sql server blacklist(ip:rpc_port)

  mutable common::SpinRWLock  refresh_server_lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCTenantSQLServerProvider);
};

}// end namespace libobcdc
} // end namespace oceanbase

#endif
