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

#define USING_LOG_PREFIX SHARE
#include "share/ob_remote_sql_proxy.h"
#include "share/ob_thread_mgr.h"
#include "lib/mysqlclient/ob_mysql_server_provider.h"
using namespace oceanbase::common::sqlclient;

namespace oceanbase {
namespace share {
ObRemoteSqlProxy::ObRemoteSqlProxy() : inited_(false), mysql_proxy_(), connection_pool_(), server_provider_(NULL)
{}
ObRemoteSqlProxy::~ObRemoteSqlProxy()
{
  destroy();
}

int ObRemoteSqlProxy::init(ObRemoteServerProvider* server_provider)
{
  UNUSEDx(server_provider);
  return OB_NOT_SUPPORTED;
}

int ObRemoteSqlProxy::set_login_info(const char* usr_info, const char* passwd)
{
  UNUSEDx(usr_info, passwd);
  return OB_NOT_SUPPORTED;
}

void ObRemoteSqlProxy::destroy()
{
  inited_ = false;
  connection_pool_.stop();
  TG_DESTROY(lib::TGDefIDs::RSqlPool);
}

int ObRemoteSqlProxy::escape(
    const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
{
  UNUSEDx(from, from_size, to, to_size, out_size);
  return OB_NOT_SUPPORTED;
}

int ObRemoteSqlProxy::read(ReadResult& res, const uint64_t tenant_id, const char* sql)
{
  UNUSEDx(res, tenant_id, sql);
  return OB_NOT_SUPPORTED;
}

int ObRemoteSqlProxy::read(ReadResult& res, const int64_t cluster_id, const uint64_t tenant_id, const char* sql)
{
  UNUSEDx(res, tenant_id, sql, cluster_id);
  return OB_NOT_SUPPORTED;
}

int ObRemoteSqlProxy::write(const uint64_t tenant_id, const char* sql, int64_t& affected_rows)
{
  UNUSEDx(tenant_id, sql, affected_rows);
  return OB_NOT_SUPPORTED;
}

sqlclient::ObISQLConnectionPool* ObRemoteSqlProxy::get_pool()
{
  return &connection_pool_;
}

}  // namespace share
}  // namespace oceanbase
