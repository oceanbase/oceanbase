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

#define USING_LOG_PREFIX LIB_MYSQLC
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
ObSingleMySQLServerProvider::ObSingleMySQLServerProvider()
{
}

void ObSingleMySQLServerProvider::init(const ObAddr &server)
{
  server_ = server;
}

int ObSingleMySQLServerProvider::get_server(
    const int64_t svr_idx,
    common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (svr_idx != 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("server index out of range", K(ret));
  } else if (server_.is_valid()) {
    server = server_;
  } else {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int64_t ObSingleMySQLServerProvider::get_server_count() const
{
  return (server_.is_valid()) ? 1 : 0;
}

int ObSingleMySQLServerProvider::get_tenant_ids(ObIArray<uint64_t> &tenant_ids)
{
  tenant_ids.reset();
  return OB_SUCCESS;
}

int ObSingleMySQLServerProvider::get_tenant_servers(const uint64_t tenant_id, ObIArray<ObAddr> &tenant_servers)
{
  tenant_servers.reset();
  return OB_SUCCESS;
}

int ObSingleMySQLServerProvider::refresh_server_list(void)
{
  return OB_SUCCESS;
}

int ObSingleMySQLServerProvider::prepare_refresh()
{
  return OB_SUCCESS;
}

int ObSingleMySQLServerProvider::end_refresh()
{
  return OB_SUCCESS;
}

ObSingleMySQLConnectionPool::ObSingleMySQLConnectionPool()
{
}

ObSingleMySQLConnectionPool::~ObSingleMySQLConnectionPool()
{
}

int ObSingleMySQLConnectionPool::init(const ObAddr &server, const ObConnPoolConfigParam &config)
{
  provider_.init(server);
  set_server_provider(&provider_);//just fake
  update_config(config);
  //init the single server connection
  int64_t cluster_id = OB_INVALID_ID;
  int ret = create_server_connection_pool(server);
  return ret;
}

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase
