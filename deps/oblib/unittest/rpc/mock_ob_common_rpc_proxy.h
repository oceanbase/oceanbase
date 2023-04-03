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

#ifndef OCEANBASE_OBRPC_MOCK_OB_COMMON_RPC_PROXY_H_
#define OCEANBASE_OBRPC_MOCK_OB_COMMON_RPC_PROXY_H_

#include "share/ob_common_rpc_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class MockObCommonRpcProxy : public ObCommonRpcProxy
{
public:
  MockObCommonRpcProxy()
    : ObCommonRpcProxy(this)
  {
  }
  MOCK_METHOD3(renew_lease,
               int(const share::ObLeaseRequest &request, share::ObLeaseResponse &response,
                   const ObRpcOpts &opts));
  MOCK_METHOD2(remove_root_partition, int(const common::ObAddr &server, const ObRpcOpts &opts));
  MOCK_METHOD2(rebuild_root_partition, int(const common::ObAddr &server, const ObRpcOpts &opts));
  MOCK_METHOD2(clear_rebuild_root_partition, int(const common::ObAddr &server, const ObRpcOpts &opts));
  MOCK_METHOD3(fetch_location,
               int(const UInt64 &table_id, common::ObSArray<share::ObPartitionLocation> &locations,
                   const ObRpcOpts &opts));
  MOCK_METHOD3(create_tenant,
               int(const obrpc::ObCreateTenantArg, UInt64 &tenant_id, const ObRpcOpts &opts));
  MOCK_METHOD3(create_database,
               int(const obrpc::ObCreateDatabaseArg, UInt64 &db_id, const ObRpcOpts &opts));
  MOCK_METHOD3(create_table,
               int(const ObCreateTableArg &arg, UInt64 &table_id, const ObRpcOpts &opts));
  MOCK_METHOD2(drop_tenant, int(const UInt64 &tenant_id, const ObRpcOpts &opts));
  MOCK_METHOD2(drop_database, int(const UInt64 &db_id, const ObRpcOpts &opts));
  MOCK_METHOD2(drop_table, int(const UInt64 &table_id, const ObRpcOpts &opts));
  MOCK_METHOD2(execute_bootstrap, int(const ObServerInfoList &server_infos, const ObRpcOpts &opts));
  MOCK_METHOD2(root_minor_freeze, int(const ObRootMinorFreezeArg &arg, const ObRpcOpts &opts));
  MOCK_METHOD2(root_major_freeze, int(const ObRootMajorFreezeArg &arg, const ObRpcOpts &opts));
  MOCK_METHOD2(get_frozen_version, int(Int64 &frozen_version, const ObRpcOpts &opts));
  MOCK_METHOD2(update_index_status, int(const ObUpdateIndexStatusArg &, const ObRpcOpts &opts));
  MOCK_METHOD2(broadcast_ds_action, int(const ObDebugSyncActionArg &, const ObRpcOpts &));
  MOCK_METHOD3(fetch_alive_server, int(const ObFetchAliveServerArg &, ObFetchAliveServerResult &, const ObRpcOpts &));
};

}//end namespace obrpc
}//end namespace oceanbase

#endif
