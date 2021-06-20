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

#ifndef OCEANBASE_RS_OB_GLOBAL_MAX_DECIDED_TRANS_VERSION_MGR_H_
#define OCEANBASE_RS_OB_GLOBAL_MAX_DECIDED_TRANS_VERSION_MGR_H_

#include "lib/hash/ob_linear_hash_map.h"
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "common/ob_partition_key.h"
#include "rootserver/ob_rs_reentrant_thread.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace share {
class ObPartitionTableOperator;
class ObPartitionInfo;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share

namespace rootserver {
class ObGlobalMaxDecidedTransVersionMgr : public ObRsReentrantThread {
public:
  ObGlobalMaxDecidedTransVersionMgr();
  virtual ~ObGlobalMaxDecidedTransVersionMgr();

public:
  int init(obrpc::ObSrvRpcProxy* rpc_proxy, share::ObPartitionTableOperator* pt_operator,
      share::schema::ObMultiVersionSchemaService* schema_service, const common::ObAddr& self_addr);
  virtual void run3() override;
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int get_global_max_decided_trans_version(int64_t& trans_version) const;

private:
  class InsertPartitionFunctor;
  class QueryPartitionFunctor;
  typedef common::ObLinearHashMap<common::ObAddr, common::ObPartitionArray> ServerPartitionMap;
  static const int64_t SUCCESS_INTERVAL_MS = 120 * 1000;
  static const int64_t FAIL_INTERVAL_MS = 10 * 1000;
  // If global_max_decided_trans_version_ is more than one hour behind the current time, alarm
  static const int64_t DELAY_ERROR_THRESHOLD = 60 * 60 * 1000 * 1000ull;
  // The standby database can only be advanced through replay log, and the backward threshold must be greater than the
  // primary database
  static const int64_t STANDBY_DELAY_ERROR_THRESHOLD = 2 * 60 * 60 * 1000 * 1000ull;

  int construct_server_partition_map_(ServerPartitionMap& server_partition_map) const;
  int get_leader_addr_and_restore_state_(const share::ObPartitionInfo& partition_info, common::ObAddr& leader,
      bool& is_restore, common::ObPartitionKey& partition_key) const;
  int construct_server_partition_map_(ServerPartitionMap& server_partition_map, const common::ObAddr& leader,
      const common::ObPartitionKey& partition_key) const;
  int handle_each_partition_(ServerPartitionMap& server_partition_map);
  bool cluster_version_before_2000_() const;

private:
  bool is_inited_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  share::ObPartitionTableOperator* pt_operator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObAddr self_addr_;
  int64_t global_max_decided_trans_version_;
  int64_t start_ts_;
  common::ObPartitionKey contributor_pkey_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGlobalMaxDecidedTransVersionMgr);
};

}  // namespace rootserver
}  // namespace oceanbase

#endif  // OCEANBASE_RS_OB_GLOBAL_MAX_DECIDED_TRANS_VERSION_MGR_H_
