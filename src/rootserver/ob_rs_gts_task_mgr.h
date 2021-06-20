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

#ifndef OCEANBASE_ROOTSERVER_OB_RS_GTS_TASK_MGR_H_
#define OCEANBASE_ROOTSERVER_OB_RS_GTS_TASK_MGR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/lock/ob_thread_cond.h"
#include "share/ob_gts_table_operator.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_rs_gts_monitor.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}

namespace rootserver {
class ObUnitManager;
class ObServerManager;
class ObZoneManager;

class ObRsGtsTaskQueue {
public:
  ObRsGtsTaskQueue() : inited_(false), task_alloc_(), wait_list_(), task_map_()
  {}

public:
  int init(const int64_t bucket_num);
  int push_task(const ObGtsReplicaTask* task);
  int pop_task(ObGtsReplicaTask*& task);
  int get_task_cnt(int64_t& task_cnt);
  common::ObIAllocator& get_task_allocator()
  {
    return task_alloc_;
  }

private:
  typedef common::ObDList<ObGtsReplicaTask> TaskList;
  typedef common::hash::ObHashMap<ObGtsReplicaTaskKey, ObGtsReplicaTask*, common::hash::NoPthreadDefendMode>
      TaskInfoMap;

private:
  bool inited_;
  common::ObFIFOAllocator task_alloc_;
  TaskList wait_list_;
  TaskInfoMap task_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRsGtsTaskQueue);
};

class ObRsGtsTaskMgr : public ObRsReentrantThread {
public:
  ObRsGtsTaskMgr(
      rootserver::ObUnitManager& unit_mgr, rootserver::ObServerManager& server_mgr, rootserver::ObZoneManager& zone_mgr)
      : inited_(false),
        cond_(),
        rs_gts_task_queue_(),
        unit_mgr_(unit_mgr),
        server_mgr_(server_mgr),
        zone_mgr_(zone_mgr),
        rpc_proxy_(nullptr),
        sql_proxy_(nullptr)
  {}

public:
  int init(obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* sql_proxy);
  int push_task(const ObGtsReplicaTask* task);

  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  void stop();

private:
  const static int64_t TASK_QUEUE_LIMIT = 1 << 16;
  int pop_task(ObGtsReplicaTask*& task);
  int get_task_cnt(int64_t& task_cnt);

private:
  bool inited_;
  common::ObThreadCond cond_;
  ObRsGtsTaskQueue rs_gts_task_queue_;
  share::ObGtsTableOperator gts_table_operator_;
  ObUnitManager& unit_mgr_;
  ObServerManager& server_mgr_;
  ObZoneManager& zone_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  common::ObMySQLProxy* sql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRsGtsTaskMgr);
};

}  // end namespace rootserver
}  // end namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_MGR_H_
