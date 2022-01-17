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

#ifndef OB_TEST_OB_TEST_ERROR_TRANSACTION_H_
#define OB_TEST_OB_TEST_ERROR_TRANSACTION_H_

#include "storage/transaction/ob_trans_service.h"
#include "../mockcontainer/mock_ob_location_cache.h"
#include "common/ob_partition_key.h"
#include "test_ob_trans_service_thread.h"
#include "test_ob_trans_service_define.h"

namespace oceanbase {
using namespace transaction;
using namespace share;
namespace unittest {
// class ObTransTestCaseStat
//{
// public:
// class ObTransTestCaseItem
//{
// public:
// bool result_;
// int64_t start_time_;
// int64_t end_time_;
//};

// public:
// typedef common::ObSEArray<ObTransTestCaseItem, 64> ObTransTestCaseItemArray;
// ObTransTestCaseItemArray testcase_array_;
//};

class ObTransServiceCluster {
public:
  ObTransServiceCluster();
  ~ObTransServiceCluster();

public:
  bool is_inited()
  {
    return inited_;
  }
  int init();
  int start();
  void reset();
  int stop();
  int wait();
  int destroy();

public:
  ObTransRpcProxy* get_trans_proxy()
  {
    return &trans_proxy_;
  }
  ObElectionRpcProxy* get_election_proxy()
  {
    return &election_proxy_;
  }
  const char* get_ip_str()
  {
    return ip_str_;
  }
  int32_t get_rpc_port() const
  {
    return rpc_port_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int set_scheduler(const int32_t scheduler_idx);
  int add_partition(const uint64_t tenant_id, const int64_t table_id, const int32_t idx);
  share::schema::ObMultiVersionSchemaService& get_multi_version_schema_service()
  {
    return schema_service_;
  }
  int set_trans_major_freeze(
      ObTransServiceCtx* trans_service_ctx, const int64_t msg_type, const bool is_do_major_freeze);
  int set_trans_switch_leader(ObTransServiceCtx* trans_service_ctx, const int64_t type, const bool need_change,
      const bool change_back, const int64_t change_num, const bool is_clear = true);
  int set_trans_msg_exception(const int64_t msg_type, ObTransServiceCtx* trans_service_ctx, const bool request_abort,
      const int64_t start_request_abort_time, const int64_t end_request_abort_time, const bool response_abort,
      const int64_t start_response_abort_time, const int64_t end_response_abort_time, const bool is_clear = true);

private:
  typedef common::hash::ObHashMap<common::ObAddr, common::ObPartitionLeaderArray> ObPartitionLeaderMap;
  int32_t get_scheduler_idx_()
  {
    return scheduler_idx_;
  }
  int add_partition_(const ObPartitionKey& partition, const ObAddr& addr, ObPartitionLeaderMap& partition_leader_map);

private:
  int run_all_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_local_single_partition_transaction_all_major_freeze_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_local_single_partition_transaction_all_rpc_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_local_single_partition_transaction_all_switch_leader_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_local_single_partition_transaction_all_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_local_multi_partition_transaction_all_rpc_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_local_multi_partition_transaction_all_switch_leader_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_local_multi_partition_transaction_all_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_remote_single_partition_transaction_all_rpc_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_remote_single_partition_transaction_all_switch_leader_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_remote_single_partition_transaction_all_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_remote_multi_partition_transaction_all_rpc_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_remote_multi_partition_transaction_all_switch_leader_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_remote_multi_parititon_transaction_all_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_distributed_multi_partition_transaction_all_rpc_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_distributed_multi_partition_transaction_all_switch_leader_exp_tests_(ObTransServiceCtx* trans_service_ctx);
  int run_distributed_multi_partititon_transaction_all_tests_(ObTransServiceCtx* trans_service_ctx);

  int do_local_single_partition_transaction_major_freeze_(const int64_t msg_type, ObTransServiceCtx* trans_service_ctx);
  int do_local_single_partition_transaction_switch_leader_(const int64_t type, ObTransServiceCtx* trans_service_ctx);
  int do_local_single_partition_transaction_rpc_exp_(const int64_t msg_type, ObTransServiceCtx* trans_service_ctx);
  int do_local_multi_partition_transaction_switch_leader_(const int64_t type, ObTransServiceCtx* trans_service_ctx);
  int do_local_multi_partition_transaction_rpc_exp_(const int64_t msg_type, ObTransServiceCtx* trans_service_ctx);
  int do_remote_single_partition_transaction_switch_leader_(const int64_t type, ObTransServiceCtx* trans_service_ctx);
  int do_remote_single_partition_transaction_rpc_exp_(const int64_t msg_type, ObTransServiceCtx* trans_service_ctx);
  int do_remote_multi_partition_transaction_switch_leader_(const int64_t type, ObTransServiceCtx* trans_service_ctx);
  int do_remote_multi_partition_transaction_rpc_exp_(const int64_t msg_type, ObTransServiceCtx* trans_service_ctx);
  int do_distributed_multi_partition_transaction_switch_leader_(
      const int64_t type, ObTransServiceCtx* trans_service_ctx);
  int do_distributed_multi_partition_transaction_rpc_exp_(const int64_t msg_type, ObTransServiceCtx* trans_service_ctx);
  int do_distributed_multi_partition_transaction_switch_leader_and_rpc_exp_(
      const int64_t type, const int64_t msg_type, ObTransServiceCtx* trans_service_ctx);
  int do_multi_partition_transaction_(ObTransServiceCtx* trans_service_ctx, const int32_t partition_num,
      const bool is_local, const bool is_remote, const bool is_distributed, const bool is_rollback = false);
  int set_test_retry_end_trans(ObTransServiceCtx* trans_service_ctx, const bool test_retry_end_trans);

private:
  bool inited_;
  bool is_running_;
  bool is_add_partition_;
  int thread_count_;
  int32_t rpc_port_;
  uint64_t tenant_id_;
  const char* ip_str_;
  int32_t scheduler_idx_;
  obrpc::ObTransRpcProxy trans_proxy_;
  obrpc::ObElectionRpcProxy election_proxy_;
  ObTransServiceThread trans_service_thread_;
  ObTransServiceHandler trans_service_handler_;
  share::schema::ObMultiVersionSchemaService schema_service_;
};

}  // namespace unittest
}  // namespace oceanbase

#endif
