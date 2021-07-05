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

#ifndef OCEANBASE_OBRPC_MOCK_OB_SRV_RPC_PROXY_H_
#define OCEANBASE_OBRPC_MOCK_OB_SRV_RPC_PROXY_H_

#include <gmock/gmock.h>
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "common/storage/ob_freeze_define.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
namespace obrpc {
class MockObSrvRpcProxy : public ObSrvRpcProxy {
public:
  MockObSrvRpcProxy() : ObSrvRpcProxy(this)
  {}

  MOCK_METHOD3(
      create_partition, int(const ObCreatePartitionArg& arg, AsyncCB<OB_CREATE_PARTITION>* cb, const ObRpcOpts& opts));
  MOCK_METHOD3(create_partition_batch,
      int(const ObCreatePartitionBatchArg& arg, AsyncCB<OB_CREATE_PARTITION_BATCH>* cb, const ObRpcOpts& opts));
  MOCK_METHOD2(fetch_root_partition, int(share::ObPartitionReplica& replica, const ObRpcOpts& opts));
  MOCK_METHOD3(get_wrs_info, int(const ObGetWRSArg& arg, AsyncCB<OB_GET_WRS_INFO>* cb, const ObRpcOpts& opts));
  MOCK_METHOD2(migrate, int(const ObMigrateArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(report_single_replica, int(const ObReportSingleReplicaArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(migrate_replica, int(const ObMigrateReplicaArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(migrate_replica_batch, int(const ObMigrateReplicaBatchArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(standby_cutdata_batch_task, int(const ObStandbyCutDataBatchTaskArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(remove_replica, int(const ObRemoveNonPaxosReplicaArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(remove_replica_batch, int(const ObRemoveNonPaxosReplicaBatchArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(remove_member, int(const ObMemberChangeArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(remove_member_batch, int(const ObMemberChangeBatchArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD3(remove_member_batch,
      int(const ObMemberChangeBatchArg& arg, ObMemberChangeBatchResult& result, const ObRpcOpts& opts));
  MOCK_METHOD2(rebuild_replica, int(const ObRebuildReplicaArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(rebuild_replica_batch, int(const ObRebuildReplicaBatchArg& arg, const ObRpcOpts& opts));

  MOCK_METHOD2(add_replica, int(const ObAddReplicaArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(add_replica_batch, int(const ObAddReplicaBatchArg& arg, const ObRpcOpts& opts));

  MOCK_METHOD3(prepare_major_freeze,
      int(const ObMajorFreezeArg& arg, AsyncCB<OB_PREPARE_MAJOR_FREEZE>* cb, const ObRpcOpts& opts));
  MOCK_METHOD3(commit_major_freeze,
      int(const ObMajorFreezeArg& arg, AsyncCB<OB_COMMIT_MAJOR_FREEZE>* cb, const ObRpcOpts& opts));
  MOCK_METHOD3(
      abort_major_freeze, int(const ObMajorFreezeArg& arg, AsyncCB<OB_ABORT_MAJOR_FREEZE>* cb, const ObRpcOpts& opts));
  MOCK_METHOD2(sync_frozen_status, int(const storage::ObFrozenStatus& frozen_status, const ObRpcOpts& opts));
  MOCK_METHOD3(
      get_member_list, int(const common::ObPartitionKey& partition_key, ObServerList& members, const ObRpcOpts& opts));
  MOCK_METHOD2(switch_leader, int(const ObSwitchLeaderArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD2(switch_leader_list, int(const ObSwitchLeaderListArg& arg, const ObRpcOpts& opts));
  MOCK_METHOD3(switch_leader_list_async,
      int(const ObSwitchLeaderListArg& arg, AsyncCB<OB_SWITCH_LEADER_LIST_ASYNC>* cb, const ObRpcOpts& opts));
  MOCK_METHOD3(get_leader_candidates,
      int(const ObGetLeaderCandidatesArg& arg, ObGetLeaderCandidatesResult& result, const ObRpcOpts& opts));
  MOCK_METHOD3(get_leader_candidates_async,
      int(const ObGetLeaderCandidatesArg& arg, AsyncCB<OB_GET_LEADER_CANDIDATES_ASYNC>* cb, const ObRpcOpts& opts));
  MOCK_METHOD2(switch_schema, int(const Int64& schema_version, const ObRpcOpts& opts));
  MOCK_METHOD2(bootstrap, int(const ObServerInfoList& server_infos, const ObRpcOpts& opts));
  MOCK_METHOD2(is_empty_server, int(Bool& is_empty, const ObRpcOpts& opts));
  MOCK_METHOD2(get_partition_stat, int(ObPartitionStatList& stat_list, const ObRpcOpts& opts));
  MOCK_METHOD1(report_replica, int(const ObRpcOpts&));
  MOCK_METHOD1(recycle_replica, int(const ObRpcOpts&));
  MOCK_METHOD2(drop_replica, int(const ObDropReplicaArg&, const ObRpcOpts&));
  MOCK_METHOD1(clear_location_cache, int(const ObRpcOpts&));
  MOCK_METHOD2(
      broadcast_sys_schema, int(const common::ObSArray<share::schema::ObTableSchema>& args, const ObRpcOpts& opts));

  MOCK_METHOD2(add_tenant_tmp, int(const UInt64& tenant_id, const ObRpcOpts&));
  MOCK_METHOD2(sync_partition_table, int(const Int64&, const ObRpcOpts&));
  MOCK_METHOD3(get_member_list_and_leader,
      int(const common::ObPartitionKey& partition_key, obrpc::ObMemberListAndLeaderArg& args, const ObRpcOpts& opts));
  MOCK_METHOD2(get_partition_count, int(obrpc::ObGetPartitionCountResult& result, const ObRpcOpts& opts));
  MOCK_METHOD2(get_rootserver_role, int(obrpc::ObGetRootserverRoleResult& result, const ObRpcOpts& opts));
  int create_partition_wrapper(const ObCreatePartitionArg& arg, AsyncCB<OB_CREATE_PARTITION>* cb, const ObRpcOpts& opts)
  {
    UNUSED(arg);
    UNUSED(opts);
    return cb->process();
  }

  int create_partition_batch_wrapper(
      const ObCreatePartitionBatchArg& arg, AsyncCB<OB_CREATE_PARTITION_BATCH>* cb, const ObRpcOpts& opts)
  {
    UNUSED(arg);
    UNUSED(opts);
    return cb->process();
  }
  int get_wrs_info_wrapper(
      const ObGetWRSArg& arg, ObSrvRpcProxy::AsyncCB<OB_GET_WRS_INFO>* cb, const obrpc::ObRpcOpts& opts)
  {
    UNUSED(arg);
    UNUSED(opts);
    static int64_t index = 0;
    if (index <= 3) {
      cb->rcode_.rcode_ = OB_SUCCESS;
    } else {
      cb->rcode_.rcode_ = OB_TIMEOUT;
    }
    return cb->process();
  }
};

}  // end namespace obrpc
}  // end namespace oceanbase

#endif
