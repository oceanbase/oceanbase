/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 *
 */

#ifndef _OCEANBASE_TESTBENCH_SERVER_PROVIDER_H_
#define _OCEANBASE_TESTBENCH_SERVER_PROVIDER_H_

#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_server_provider.h"
#include "ob_testbench_systable_helper.h"

namespace oceanbase {
namespace testbench {
typedef common::ObSEArray<uint64_t, 16> TenantList;
typedef common::ObSEArray<ObTenantName, 16> TenantNameList;
typedef common::ObSEArray<common::ObAddr, 16> ServerList;
struct TenantServerList
    : public LinkHashValue<common::sqlclient::TenantMapKey> {
  TenantServerList()
      : server_list_(ObModIds::OB_LOG_SERVER_PROVIDER,
                     OB_MALLOC_NORMAL_BLOCK_SIZE) {}
  ~TenantServerList() { server_list_.reset(); }
  ServerList &get_server_list() {
    return server_list_;
  } // used to build tenant_server_list
  const ServerList &get_server_list() const {
    return server_list_;
  } // used to assign to connection_pool refresh
  int64_t get_server_count() const { return server_list_.count(); }
  ServerList server_list_;

  TO_STRING_KV(K_(server_list));
};

typedef common::ObLinkHashMap<common::sqlclient::TenantMapKey, TenantServerList>
    TenantServerMap;
class ObTestbenchServerProvider
    : public common::sqlclient::ObMySQLServerProvider {
public:
  ObTestbenchServerProvider();
  virtual ~ObTestbenchServerProvider() {}

public:
  virtual int get_server(const int64_t svr_idx,
                         common::ObAddr &server) override;
  virtual int64_t get_server_count() const override;
  virtual int get_tenants(common::ObIArray<ObTenantName> &tenants) override;
  virtual int get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids) override;
  virtual int
  get_tenant_servers(const uint64_t tenant_id,
                     common::ObIArray<common::ObAddr> &tenant_servers) override;
  virtual int refresh_server_list(void) override;
  virtual int prepare_refresh() override;
  virtual int end_refresh() override;

public:
  int init(ObTestbenchSystableHelper &systable_helper);
  void destroy();
  int del_tenant(const uint64_t tenant_id);

private:
  int query_all_tenants();
  int query_all_servers();
  int query_tenant_servers();

private:
  bool is_inited_;
  TenantList tenant_list_;
  TenantNameList tenant_name_list_;
  ServerList server_list_;
  TenantServerMap tenant_server_map_;
  ObTestbenchSystableHelper *systable_helper_;
  mutable common::SpinRWLock refresh_server_lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTestbenchServerProvider);
};
} // namespace testbench
} // namespace oceanbase
#endif