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

#ifndef OB_TEST_OB_TEST_ERROR_TRANSACTION_CTX_H_
#define OB_TEST_OB_TEST_ERROR_TRANSACTION_CTX_H_

#include "storage/transaction/ob_trans_service.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_trans_rpc.h"
#include "election/ob_election_mgr.h"
#include "election/ob_election_cb.h"
#include "mock_ob_trans_service.h"
#include "mock_ob_trans_rpc.h"
#include "mock_ob_election_mgr.h"
#include "mock_ob_election_rpc.h"
#include "mock_ob_clog_adapter.h"
#include "mock_ob_election_callback.h"
#include "mock_ob_freeze_cb.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "mock_ob_location_cache.h"
#include "test_ob_trans_service_rpc_exp.h"
#include "test_ob_trans_service_switch_leader_exp.h"
#include "test_ob_trans_service_major_freeze_exp.h"
#include "test_ob_trans_service_define.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase {
using namespace transaction;
using namespace share;
using namespace storage;
using namespace obrpc;

namespace unittest {
class ObTransServiceCtxThread : public share::ObThreadPool {
public:
  int setThreadParameter(const int threadCount, void* arg);

protected:
  bool inited_;
  void* arg_;
};

class ObTransServiceCtx : public ObITransRpc, public ObIElectionRpc {
public:
  ObTransServiceCtx() : inited_(false), base_rpc_port_(0), scheduler_idx_(0), tenant_id_(0)
  {
    reset();
  }
  ~ObTransServiceCtx()
  {}
  int stop()
  {
    return OB_SUCCESS;
  }
  int wait()
  {
    return OB_SUCCESS;
  }

  int on_get_election_priority(election::ObElectionPriority& priority)
  {
    UNUSED(priority);
    return OB_SUCCESS;
  }

  int init_trans_service(const int64_t idex, const char* ip_str, const int32_t base_rpc_port,
      obrpc::ObTransRpcProxy* trans_proxy, obrpc::ObElectionRpcProxy* election_proxy,
      share::schema::ObMultiVersionSchemaService* schema_service);

  int init(const uint64_t tenant_id, const char* ip_str, const int32_t base_rpc_port,
      obrpc::ObTransRpcProxy* trans_proxy, obrpc::ObElectionRpcProxy* election_proxy,
      share::schema::ObMultiVersionSchemaService* schema_service);
  int init(obrpc::ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const ObAddr& self)
  {
    int ret = OB_SUCCESS;
    UNUSED(rpc_proxy);
    UNUSED(trans_service);
    UNUSED(self);
    return ret;
  }

  int init(obrpc::ObElectionRpcProxy* rpc_proxy, ObIElectionMgr* election_mgr, const common::ObAddr& self)
  {
    int ret = OB_SUCCESS;
    UNUSED(rpc_proxy);
    UNUSED(election_mgr);
    UNUSED(self);
    return ret;
  }

  int start()
  {
    return OB_SUCCESS;
  }
  void reset();
  void destroy();
  bool is_inited() const
  {
    return inited_;
  }

  int post_trans_resp_msg(const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg);
  int post_trans_msg(
      const uint64_t tenant_id, const ObAddr& server, const transaction::ObTransMsg& msg, const int64_t msg_type);
  virtual int ObTransRpc::post_batch_msg(const uint64_t tenant_id, const ObAddr& server, const obrpc::ObIFill& msg,
      const int64_t msg_type, const ObPartitionKey& pkey)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    UNUSED(msg_type);
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  int post_election_msg(const ObAddr& server, const ObPartitionKey& partition, const election::ObElectionMsg& msg);
  int add_partition(const ObPartitionKey& partition, const ObAddr& addr);
  int set_scheduler_idx(const int32_t scheduler_idx);

  int get_leader();
  int get_leader_(const common::ObPartitionKey& partition, ObAddr& leader, const int64_t idx);
  int change_leader(const ObPartitionKey& partition);
  int change_leader(const ObPartitionKey& partition, const ObAddr& addr);
  int set_major_freeze(const int64_t msg_type, const bool is_do_major_freeze);
  int set_switch_leader_partitions(const int64_t type, const ObPartitionArray& partitions);
  int set_switch_leader(const int64_t type, const bool need_change, const bool change_back, const int64_t change_num,
      const bool is_clear);
  int set_msg_exception(const int64_t msg_type, const bool request_abort, const int64_t start_request_abort_time,
      const int64_t end_request_abort_time, const bool response_abort, const int64_t start_response_abort_time,
      const int64_t end_response_abort_time, const bool is_clear);

  int init_local_trans_params();
  int init_remote_trans_params();
  int init_distributed_trans_params();
  int do_major_freeze(const int64_t freeze_version);
  int start_participant(ObPartitionKey& partition, ObPartitionArray& partitions);
  int start_participant(ObAddr& addr, ObPartitionArray& partitions);
  int end_participant(const bool is_rollback, ObPartitionKey& partition, ObPartitionArray& partitions);
  int end_participant(const bool is_rollback, ObAddr& addr, ObPartitionArray& partitions);
  int start_trans();
  int end_trans(const bool is_rollback, const int error_code);
  int start_stmt(ObPartitionArray& participants, const ObPartitionLeaderArray& leader_arr);
  int end_stmt(const bool is_rollback);
  void set_test_retry_end_trans(const bool test_retry_end_trans)
  {
    test_retry_end_trans_ = test_retry_end_trans;
  }

private:
  ObTransService* get_scheduler_()
  {
    return &txs_[scheduler_idx_];
  }
  void check_rpc_exp_(const int64_t msg_type, bool& abort);
  void check_switch_leader_(const int64_t type, const ObPartitionKey& parition);
  void check_switch_leader_partition(const int64_t msg_type, const ObPartitionKey& parition);
  int get_txs_(const ObAddr& addr, ObTransService*& txs);
  int get_participant_(const ObPartitionKey& partition, ObTransService*& participant);
  int get_participant_(const ObAddr& addr, ObTransService*& participant);
  int check_major_freeze_(const int64_t msg_type, const ObPartitionKey& partition);

public:
  class ObGetLeaderThread : public ObTransServiceCtxThread {
  public:
    void run1();
  };
  class ObMajorfreezeThread : public ObTransServiceCtxThread {
  public:
    void run1();
  };

private:
  bool inited_;
  int32_t base_rpc_port_;
  char ip_[MAX_IP_ADDR_LENGTH];
  int32_t scheduler_idx_;
  uint64_t tenant_id_;
  bool test_retry_end_trans_;
  MockObIPartitionService partition_service_;
  transaction::ObStartTransParam start_trans_parma_;
  transaction::ObTransDesc trans_desc_;
  transaction::ObStmtDesc stmt_desc_;
  int64_t trans_expired_time_;
  int64_t stmt_expired_time_;
  MockObElectionCallback mock_election_cb_;
  MockObClogAdapter clog_adapter_[MAX_OB_TRANS_SERVICE_NUM];
  share::MockObLocationCache location_cache_[MAX_OB_TRANS_SERVICE_NUM];
  MockObTransService txs_[MAX_OB_TRANS_SERVICE_NUM];
  MockObElectionMgr election_mgr_[MAX_OB_TRANS_SERVICE_NUM];
  MockObTransRpc trans_rpc_[MAX_OB_TRANS_SERVICE_NUM];
  MockObElectionRpc election_rpc_[MAX_OB_TRANS_SERVICE_NUM];
  ObPartitionArray partitions_;
  ObTransRpcExecption rpc_exp_;
  ObMySQLProxy sql_proxy_;
  ObTransSwitchLeaderException switch_leader_exp_;
  ObTransMajorFreezeException major_freeze_exp_;
  ObGetLeaderThread get_leader_thread_;
  ObMajorfreezeThread major_freeze_thread_;
  MockObFreezeTransCb freeze_cb_;
};

}  // namespace unittest
}  // namespace oceanbase

#endif
