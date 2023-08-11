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

#ifndef _OB_TABLE_RPC_PROXY_H
#define _OB_TABLE_RPC_PROXY_H 1
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/table/ob_table_rpc_struct.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObTableRpcProxy: public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObTableRpcProxy);
  RPC_S(PR5 login, obrpc::OB_TABLE_API_LOGIN, (table::ObTableLoginRequest), table::ObTableLoginResult);
  RPC_S(PR5 execute, obrpc::OB_TABLE_API_EXECUTE, (table::ObTableOperationRequest), table::ObTableOperationResult);
  RPC_S(PR5 batch_execute, obrpc::OB_TABLE_API_BATCH_EXECUTE, (table::ObTableBatchOperationRequest), table::ObTableBatchOperationResult);
  RPC_SS(PR5 execute_query, obrpc::OB_TABLE_API_EXECUTE_QUERY, (table::ObTableQueryRequest), table::ObTableQueryResult);
  RPC_S(PR5 query_and_mutate, obrpc::OB_TABLE_API_QUERY_AND_MUTATE, (table::ObTableQueryAndMutateRequest), table::ObTableQueryAndMutateResult);
  RPC_S(PR5 execute_query_sync, obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC, (table::ObTableQuerySyncRequest), table::ObTableQuerySyncResult);
  RPC_S(PR5 direct_load, obrpc::OB_TABLE_API_DIRECT_LOAD, (table::ObTableDirectLoadRequest), table::ObTableDirectLoadResult);
};

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* _OB_TABLE_RPC_PROXY_H */
