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

#ifndef OCEANBASE_ROOTSERVER_OB_EMPTY_SERVER_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_EMPTY_SERVER_CHECKER_H_

#include "lib/lock/ob_thread_cond.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "rootserver/ob_rs_reentrant_thread.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace obrpc {
class ObSrvRpcProxy;
}
namespace rootserver {
class ObServerManager;

/// Check whether have partitions on non-alive servers.
class ObEmptyServerCheckRound {
public:
  const static int64_t PT_SYNC_TIMEOUT = 10L * 60 * 1000 * 1000;  // 10 minutes
  explicit ObEmptyServerCheckRound(volatile bool& stop);
  virtual ~ObEmptyServerCheckRound();

  int init(ObServerManager& server_mgr, obrpc::ObSrvRpcProxy& rpc_proxy, share::ObPartitionTableOperator& pt_operator,
      share::schema::ObMultiVersionSchemaService& schema_service);

  // check server is empty with %cond locked
  int check(common::ObThreadCond& cond);
  int pt_sync_finish(const common::ObAddr& server, const int64_t version);

private:
  int wait_pt_sync_finish(common::ObThreadCond& cond);
  int update_with_partition_flag();

private:
  bool inited_;
  volatile bool& stop_;
  int64_t version_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObServerManager* server_mgr_;
  share::ObPartitionTableOperator* pt_operator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  // alive servers for sync partition table, value is sync finish times.
  common::hash::ObHashMap<common::ObAddr, int64_t, common::hash::NoPthreadDefendMode> alive_servers_;
  // empty server to be checked, value is last heartbeat time.
  common::hash::ObHashMap<common::ObAddr, int64_t, common::hash::NoPthreadDefendMode> empty_servers_;

  DISALLOW_COPY_AND_ASSIGN(ObEmptyServerCheckRound);
};

/// Empty server checker thread.
class ObEmptyServerChecker : public ObRsReentrantThread {
public:
  ObEmptyServerChecker();
  virtual ~ObEmptyServerChecker();

  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }

  int init(ObServerManager& server_mgr, obrpc::ObSrvRpcProxy& rpc_proxy, share::ObPartitionTableOperator& pt_operator,
      share::schema::ObMultiVersionSchemaService& schema_service);

  virtual void wakeup();
  virtual void stop();
  virtual int notify_check();
  virtual int pt_sync_finish(const common::ObAddr& server, const int64_t version);

private:
  bool inited_;
  bool need_check_;
  ObEmptyServerCheckRound check_round_;
  common::ObThreadCond cond_;

  DISALLOW_COPY_AND_ASSIGN(ObEmptyServerChecker);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_EMPTY_SERVER_CHECKER_H_
