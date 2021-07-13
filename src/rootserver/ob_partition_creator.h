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

#ifndef OCEANBASE_ROOTSERVER_OB_PARTITION_CREATOR_H_
#define OCEANBASE_ROOTSERVER_OB_PARTITION_CREATOR_H_

#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/partition_table/ob_partition_info.h"
#include "ob_rs_async_rpc_proxy.h"
#include "lib/hash/ob_hashmap.h"
#include "rootserver/ob_server_manager.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
class ObSplitInfo;
class ObDMLSqlSplicer;
}  // namespace share
namespace rootserver {
class ObServerManager;
// async create partition
class ObPartitionCreator {
public:
  typedef common::hash::ObHashMap<common::ObAddr, obrpc::ObCreatePartitionBatchArg*> DestArgMap;
  typedef common::hash::ObHashMap<common::ObAddr, obrpc::ObSetMemberListBatchArg*> DestMemberArgMap;
  typedef common::hash::ObHashMap<common::ObAddr, int64_t> DestCountMap;
  typedef common::hash::ObHashMap<common::ObAddr, int> DestRetMap;

  static const int64_t MAX_PARTITION_LIMIT_RPC_CNT = 128;
  static const int64_t CREATE_PARTITION_BATCH_CNT = 128;
  static const int64_t SERVER_CONCURRENCY = 4;
  static const int64_t HASHMAP_SERVER_CNT = 512;

  // only be updated in unittest to disable batch update.
  static int64_t flag_replica_batch_update_cnt;

  // allow server_mgr is NULL only when pre bootstrap
  // if server_mgr is NULL, regard dest server as a active server
  ObPartitionCreator(obrpc::ObSrvRpcProxy& rpc_proxy, share::ObPartitionTableOperator& pt_operator,
      ObServerManager* server_mgr, const bool binding = false, const bool ignore_member_list = false,
      common::ObMySQLProxy* sql_proxy = NULL);
  virtual ~ObPartitionCreator()
  {}

  virtual int add_flag_replica(const share::ObPartitionReplica& replica);
  virtual int add_create_partition_arg(const common::ObAddr& dest, const obrpc::ObCreatePartitionArg& arg);
  virtual void set_create_mode(obrpc::ObCreateTableMode create_mode)
  {
    create_mode_ = create_mode;
  }
  virtual int execute();
  virtual int execute(const share::ObSplitInfo& split_info);
  // only create partition, do not insert flag replica
  static int create_partition_sync(obrpc::ObSrvRpcProxy& rpc_proxy, share::ObPartitionTableOperator& pt_operator,
      const common::ObAddr& dest, const obrpc::ObCreatePartitionArg& arg);
  void reuse();

  int check_partition_already_exist(const int64_t table_id, const int64_t partition_id, const int64_t paxos_replica_num,
      bool& partition_already_exist);
  bool is_tenant_partition_already_exist()
  {
    return 0 < member_lists_.size();
  }
  bool is_ignore_member_list()
  {
    return ignore_member_list_;
  }
  int build_member_list_map(const obrpc::ObSetMemberListArg& arg);
  static int check_majority(const common::ObPartitionKey& partition_key, const int failed_ret,
      common::hash::ObHashMap<common::ObPartitionKey, int64_t>& failed_partition_map,
      common::hash::ObHashMap<common::ObPartitionKey, int>& check_full_partition_map, const int64_t replica_num,
      const int64_t member_cnt, common::ObReplicaType replica_type);
  int set_tenant_exist_partition_member_list(const uint64_t tenant_id, const int64_t quorum);

private:
  int update_flag_replica();
  int batch_update_flag_replica();
  int batch_update_flag_replica(const int64_t begin, const int64_t end);
  int try_check_reach_server_partition_limit();
  int do_check_loose_mode_reach_create_partition_limit(
      const DestCountMap& dest_count_map, const common::ObIArray<int>& ret_array);
  int create_partitions();
  int batch_create_partitions(DestArgMap& dest_arg_map, const int64_t replica_cnt);
  int init_prev_not_processed_idx_array(common::ObIArray<int64_t>& prev_not_processed_idx_array);
  int get_whole_partition_idx_scope(
      const common::ObIArray<int64_t>& not_processed_idx_array, const int64_t p_start, int64_t& p_end);
  int check_whole_partition_dest_rpc_capacity(const common::ObIArray<int64_t>& not_processed_idx_array,
      const int64_t p_start, const int64_t p_end, DestArgMap& dest_arg_map, bool& in_tolerance);
  int append_not_processed_idx_array(const common::ObIArray<int64_t>& prev_not_processed_idx_array,
      common::ObIArray<int64_t>& not_processed_idx_array, const int64_t p_start, const int64_t p_end);
  int init_batch_args(DestArgMap& dest_arg_map, common::PageArena<>& allocator);
  int check_partition_in_strict_create_mode(
      const int64_t replica_cnt, ObCreatePartitionBatchProxy& proxy_batch, common::ObArray<int>& return_code_array);
  int check_partition_in_non_strict_create_mode(const int64_t replica_cnt, ObCreatePartitionBatchProxy& proxy_batch,
      common::ObArray<int>& return_code_array,
      const common::ObIArray<obrpc::ObCreatePartitionBatchArg*>& inactive_args);
  int check_binding_partition_execution_result(ObCreatePartitionBatchProxy& proxy_batch,
      common::ObIArray<int>& return_code_array, common::ObIArray<common::ObAddr>& inactive_servers,
      common::ObIArray<obrpc::ObCreatePartitionBatchArg>& inactive_args);
  int build_binding_partition_new_arg(const obrpc::ObCreatePartitionBatchArg& arg,
      const obrpc::ObCreatePartitionBatchRes& result, common::ObIArray<common::ObAddr>& new_dest_array,
      common::ObIArray<obrpc::ObCreatePartitionBatchArg>& new_arg_array);
  int do_execute_binding_partition_request(ObCreatePartitionBatchProxy& proxy_batch,
      const common::ObIArray<common::ObAddr>& new_dest_array,
      const common::ObIArray<obrpc::ObCreatePartitionBatchArg>& new_arg_array, common::ObIArray<int>& return_code_array,
      common::ObIArray<common::ObAddr>& inactive_servers,
      common::ObIArray<obrpc::ObCreatePartitionBatchArg>& inactive_args);
  // select from __all_partition_member_list
  // the partition has created and persisted member_list
  int set_exist_partition_member_list(const uint64_t tenant_id, const int64_t table_id, const int64_t quorum);
  int construct_member_list_map(const ObSqlString& sql, const int64_t quorum);
  // partitions have created, persist member_list to sys_table, no need to create partition again
  int batch_persist_member_list(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_args);
  int fill_dml_splicer(
      const ObPartitionKey& partition_key, const int64_t schema_version, share::ObDMLSqlSplicer& dml_splicer);
  // construct batch_arg by server
  int distrubte_for_set_member_list(common::PageArena<>& allocate, DestMemberArgMap& dest_arg_map);
  // batch setting member_list
  int batch_send_set_member_list_rpc(const int64_t partition_count, DestMemberArgMap& arg_map);
  // check whether set member_list success
  int check_set_member_list_succ(const int64_t partition_count, const common::ObIArray<int>& return_code_array,
      const common::ObIArray<obrpc::ObSetMemberListBatchArg*>& inactive_args);
  int get_member_list_count(const obrpc::ObCreatePartitionArg& arg, int64_t& member_list_count);
  // create partition success, set member_list
  int send_set_member_list_rpc();

private:
  ObCreatePartitionProxy proxy_;
  share::ObPartitionTableOperator& pt_operator_;
  ObCreatePartitionBatchProxy proxy_batch_;
  ObServerManager* server_mgr_;

  common::ObSEArray<share::ObPartitionReplica, share::ObPartitionReplica::DEFAULT_REPLICA_COUNT> replicas_;
  common::ObSEArray<common::ObAddr, share::ObPartitionReplica::DEFAULT_REPLICA_COUNT> dests_;
  common::ObSEArray<obrpc::ObCreatePartitionArg, share::ObPartitionReplica::DEFAULT_REPLICA_COUNT> args_;
  share::ObSplitInfo split_info_;
  obrpc::ObCreateTableMode create_mode_;
  bool binding_;  // true when binding partitions
  common::ObMySQLProxy* sql_proxy_;
  ObSetMemberListBatchProxy set_member_list_proxy_;
  // true  : set_member_list after create partitions
  // false : set_member_list while create partitions
  bool ignore_member_list_;
  int64_t paxos_replica_num_;
  common::hash::ObHashMap<ObPartitionKey, obrpc::ObSetMemberListArg> member_lists_;
  ObReachPartitionLimitProxy partition_limit_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObPartitionCreator);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_PARTITION_CREATOR_H_
