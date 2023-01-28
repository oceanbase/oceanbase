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
#include "share/table/ob_table_load_rpc_struct.h"
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

  /**
   * table load
   */
  // client -> server
  RPC_S(PR5 load_begin, obrpc::OB_TABLE_API_LOAD_BEGIN, (table::ObTableLoadBeginRequest), table::ObTableLoadBeginResult);
  RPC_S(PR5 load_finish, obrpc::OB_TABLE_API_LOAD_FINISH, (table::ObTableLoadFinishRequest), table::ObTableLoadFinishResult);
  RPC_S(PR5 load_commit, obrpc::OB_TABLE_API_LOAD_COMMIT, (table::ObTableLoadCommitRequest), table::ObTableLoadCommitResult);
  RPC_S(PR5 load_abort, obrpc::OB_TABLE_API_LOAD_ABORT, (table::ObTableLoadAbortRequest), table::ObTableLoadAbortResult);
  RPC_S(PR5 load_get_status, obrpc::OB_TABLE_API_LOAD_GET_STATUS, (table::ObTableLoadGetStatusRequest), table::ObTableLoadGetStatusResult);
  RPC_S(PR5 load, obrpc::OB_TABLE_API_LOAD, (table::ObTableLoadRequest), table::ObTableLoadResult);
  RPC_S(PR5 load_start_trans, obrpc::OB_TABLE_API_LOAD_START_TRANS, (table::ObTableLoadStartTransRequest), table::ObTableLoadStartTransResult);
  RPC_S(PR5 load_finish_trans, obrpc::OB_TABLE_API_LOAD_FINISH_TRANS, (table::ObTableLoadFinishTransRequest), table::ObTableLoadFinishTransResult);
  RPC_S(PR5 load_abandon_trans, obrpc::OB_TABLE_API_LOAD_ABANDON_TRANS, (table::ObTableLoadAbandonTransRequest), table::ObTableLoadAbandonTransResult);
  RPC_S(PR5 load_get_trans_status, obrpc::OB_TABLE_API_LOAD_GET_TRANS_STATUS, (table::ObTableLoadGetTransStatusRequest), table::ObTableLoadGetTransStatusResult);
  // coordinator -> peer
  RPC_S(PR5 load_pre_begin_peer, obrpc::OB_TABLE_API_LOAD_PRE_BEGIN_PEER, (table::ObTableLoadPreBeginPeerRequest), table::ObTableLoadPreBeginPeerResult);
  RPC_S(PR5 load_confirm_begin_peer, obrpc::OB_TABLE_API_LOAD_CONFIRM_BEGIN_PEER, (table::ObTableLoadConfirmBeginPeerRequest), table::ObTableLoadConfirmBeginPeerResult);
  RPC_S(PR5 load_pre_merge_peer, obrpc::OB_TABLE_API_LOAD_PRE_MERGE_PEER, (table::ObTableLoadPreMergePeerRequest), table::ObTableLoadPreMergePeerResult);
  RPC_S(PR5 load_start_merge_peer, obrpc::OB_TABLE_API_LOAD_START_MERGE_PEER, (table::ObTableLoadStartMergePeerRequest), table::ObTableLoadStartMergePeerResult);
  RPC_S(PR5 load_commit_peer, obrpc::OB_TABLE_API_LOAD_COMMIT_PEER, (table::ObTableLoadCommitPeerRequest), table::ObTableLoadCommitPeerResult);
  RPC_S(PR5 load_abort_peer, obrpc::OB_TABLE_API_LOAD_ABORT_PEER, (table::ObTableLoadAbortPeerRequest), table::ObTableLoadAbortPeerResult);
  RPC_S(PR5 load_get_status_peer, obrpc::OB_TABLE_API_LOAD_GET_STATUS_PEER, (table::ObTableLoadGetStatusPeerRequest), table::ObTableLoadGetStatusPeerResult);
  RPC_S(PR5 load_peer, obrpc::OB_TABLE_API_LOAD_PEER, (table::ObTableLoadPeerRequest), table::ObTableLoadPeerResult);
  RPC_S(PR5 load_pre_start_trans_peer, obrpc::OB_TABLE_API_LOAD_PRE_START_TRANS_PEER, (table::ObTableLoadPreStartTransPeerRequest), table::ObTableLoadPreStartTransPeerResult);
  RPC_S(PR5 load_confirm_start_trans_peer, obrpc::OB_TABLE_API_LOAD_CONFIRM_START_TRANS_PEER, (table::ObTableLoadConfirmStartTransPeerRequest), table::ObTableLoadConfirmStartTransPeerResult);
  RPC_S(PR5 load_pre_finish_trans_peer, obrpc::OB_TABLE_API_LOAD_PRE_FINISH_TRANS_PEER, (table::ObTableLoadPreFinishTransPeerRequest), table::ObTableLoadPreFinishTransPeerResult);
  RPC_S(PR5 load_confirm_finish_trans_peer, obrpc::OB_TABLE_API_LOAD_CONFIRM_FINISH_TRANS_PEER, (table::ObTableLoadConfirmFinishTransPeerRequest), table::ObTableLoadConfirmFinishTransPeerResult);
  RPC_S(PR5 load_abandon_trans_peer, obrpc::OB_TABLE_API_LOAD_ABANDON_TRANS_PEER, (table::ObTableLoadAbandonTransPeerRequest), table::ObTableLoadAbandonTransPeerResult);
  RPC_S(PR5 load_get_trans_status_peer, obrpc::OB_TABLE_API_LOAD_GET_TRANS_STATUS_PEER, (table::ObTableLoadGetTransStatusPeerRequest), table::ObTableLoadGetTransStatusPeerResult);
};

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* _OB_TABLE_RPC_PROXY_H */
