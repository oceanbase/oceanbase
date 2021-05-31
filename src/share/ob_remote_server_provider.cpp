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

#define USING_LOG_PREFIX SERVER

#include "share/ob_remote_server_provider.h"
#include "share/ob_multi_cluster_util.h"
#include "observer/ob_server_struct.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_common_rpc_proxy.h"
#include "rootserver/ob_server_table_operator.h"
#include "share/ob_remote_sql_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
namespace oceanbase {
namespace share {
void ObRemoteServerProvider::ServerAddr::reset()
{
  server_.reset();
  sql_port_ = OB_INVALID_ID;
}

ObRemoteServerProvider::~ObRemoteServerProvider()
{}

ObRemoteServerProvider::ObRemoteServerProvider()
{}

int ObRemoteServerProvider::init(obrpc::ObCommonRpcProxy& rpc_proxy, common::ObMySQLProxy& sql_proxy)
{
  UNUSEDx(rpc_proxy, sql_proxy);
  return OB_NOT_SUPPORTED;
}

int ObRemoteServerProvider::get_server(const int64_t cluster_id, const int64_t svr_idx, common::ObAddr& server)
{
  UNUSEDx(cluster_id, svr_idx, server);
  return OB_NOT_SUPPORTED;
}

int64_t ObRemoteServerProvider::get_primary_cluster_id() const
{
  return OB_INVALID_ID;
}

int ObRemoteServerProvider::get_cluster_list(common::ObIArray<int64_t>& cluster_list)
{
  UNUSEDx(cluster_list);
  return OB_NOT_SUPPORTED;
}

int64_t ObRemoteServerProvider::get_cluster_count() const
{
  return 0;
}

int64_t ObRemoteServerProvider::get_server_count() const
{
  return 0;
}

int64_t ObRemoteServerProvider::get_server_count(const int64_t cluster_id) const
{
  UNUSEDx(cluster_id);
  return 0;
}

int ObRemoteServerProvider::refresh_server_list()
{
  return OB_NOT_SUPPORTED;
}

int ObRemoteServerProvider::prepare_refresh()
{
  return OB_NOT_SUPPORTED;
}

bool ObRemoteServerProvider::need_refresh()
{
  return false;
}
}  // namespace share
}  // namespace oceanbase
