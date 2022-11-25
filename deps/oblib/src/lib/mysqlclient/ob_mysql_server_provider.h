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

#ifndef __OB_COMMON_SQLCLIENT_MYSQL_SERVER_PROVIDER__
#define __OB_COMMON_SQLCLIENT_MYSQL_SERVER_PROVIDER__

#include "lib/net/ob_addr.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLServerProvider
{
public:
  ObMySQLServerProvider() {};
  virtual ~ObMySQLServerProvider() {};
  virtual int get_server(const int64_t svr_idx, common::ObAddr &server) = 0;
  virtual int64_t get_server_count() const = 0;
  // should imply get_tenant_ids/get_tenant_servers
  // if using MySQLConnectionPool and MySQLConnectionPoolType is TENANT_POOL
  // MUST contains SYS_TENANT
  virtual int get_tenant_ids(ObIArray<uint64_t> &tenant_ids) = 0;
  virtual int get_tenant_servers(const uint64_t tenant_id, ObIArray<ObAddr> &tenant_servers) = 0;
  virtual int refresh_server_list(void) = 0;
  virtual int prepare_refresh() = 0;
  virtual int end_refresh() = 0;
  virtual bool need_refresh() { return true; }
private:
  ObMySQLServerProvider(const ObMySQLServerProvider &);
  ObMySQLServerProvider &operator=(const ObMySQLServerProvider &);
};
}
}
}

#endif
