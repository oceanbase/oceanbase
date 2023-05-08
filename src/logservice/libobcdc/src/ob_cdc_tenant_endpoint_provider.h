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
 * ObCDCEndpointProvider define
 */

#ifndef OCEANBASE_LIBOBCDC_TENANT_ENDPOINT_PROVIDER_H__
#define OCEANBASE_LIBOBCDC_TENANT_ENDPOINT_PROVIDER_H__

#include "lib/lock/ob_spin_rwlock.h"                    // SpinRWLock
#include "lib/mysqlclient/ob_mysql_server_provider.h"   // ObMySQLServerProvider
#include "ob_cdc_server_endpoint_access_info.h"
#include "ob_log_svr_blacklist.h"                       // ObLogSvrBlacklist

namespace oceanbase
{
namespace libobcdc
{
class ObLogConfig;

typedef common::ObArray<common::ObAddr> SQLServerList;
class ObCDCEndpointProvider : public common::sqlclient::ObMySQLServerProvider
{
public:
  ObCDCEndpointProvider();
  virtual ~ObCDCEndpointProvider();
public:
  int init(const char *tenant_endpoint_str);
  void destroy();
  int refresh_server_list(ObIArray<ObAddr> &svr_list);
  const ObLogSvrBlacklist &get_svr_black_list() const { return svr_blacklist_; }
  void configure(const ObLogConfig &cfg);
public:
  // override ObMySQLServerProvider
  virtual int64_t get_server_count() const override;
  virtual int get_server(const int64_t svr_idx, common::ObAddr &server) override;
  virtual int get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids) override;
  virtual int get_tenant_servers(const uint64_t tenant_id, common::ObIArray<ObAddr> &tenant_servers) override;
  virtual int refresh_server_list(void) { return OB_SUCCESS; }
  virtual int prepare_refresh() override { return OB_SUCCESS; }
  virtual int end_refresh() override { return OB_SUCCESS; }
private:
  int parse_tenant_endpoint_list_(const char *tenant_endpoint_str);
  int init_sql_svr_list_();
private:
  bool                        is_inited_;
  mutable common::SpinRWLock  refresh_lock_; // lock to protect refresh ServerList server_list_
  SQLServerList               sql_svr_list_;
  ObCDCEndpointList           endpoint_list_; // user provided tenant_endpoint, only used while init cause may use hostname.
  ObLogSvrBlacklist           svr_blacklist_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
