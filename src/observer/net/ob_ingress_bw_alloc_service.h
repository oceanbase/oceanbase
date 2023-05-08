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

#ifndef OCEANBASE_ROOTSERVER_OB_INGRESS_BW_ALLOC_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_INGRESS_BW_ALLOC_SERVICE_H

#include "share/ob_define.h"
#include "logservice/ob_log_base_type.h"  //ObIRoleChangeSubHandler ObICheckpointSubHandler ObIReplaySubHandler
#include "rootserver/ob_primary_ls_service.h"        //ObTenantThreadHelper
#include "observer/net/ob_net_endpoint_ingress_rpc_proxy.h"

namespace oceanbase
{
using namespace obrpc;
namespace rootserver
{
class ObNetEndpointIngressManager
{
public:
  ObNetEndpointIngressManager() : ingress_plan_map_(), total_bw_limit_(0), lock_()
  {}
  ~ObNetEndpointIngressManager() = default;
  int init();
  void reset();
  void destroy();
  int register_endpoint(const ObNetEndpointKey &endpoint_key, const int64_t expire_time);
  int collect_predict_bw(ObNetEndpointKVArray &update_kvs);
  int update_ingress_plan(ObNetEndpointKVArray &update_kvs);
  int commit_bw_limit_plan(ObNetEndpointKVArray &update_kvs);
  int set_total_bw_limit(int64_t total_bw_limit);
  int64_t get_map_size();

private:
  typedef common::hash::ObHashMap<ObNetEndpointKey, ObNetEndpointValue *> ObIngressPlanMap;
  ObIngressPlanMap ingress_plan_map_;
  int64_t total_bw_limit_;
  ObSpinLock lock_;
  DISALLOW_COPY_AND_ASSIGN(ObNetEndpointIngressManager);
};
class ObIngressBWAllocService : public common::ObTimerTask,
                                public logservice::ObIRoleChangeSubHandler,
                                public logservice::ObICheckpointSubHandler,
                                public logservice::ObIReplaySubHandler
{
public:
  ObIngressBWAllocService()
      : is_inited_(false),
        is_leader_(false),
        is_stop_(true),
        tg_id_(-1),
        cluster_id_(OB_INVALID_CLUSTER_ID),
        expire_time_(0),
        ingress_manager_(),
        endpoint_ingress_rpc_proxy_()
  {}
  virtual ~ObIngressBWAllocService()
  {
    destroy();
  }
  int init(const uint64_t cluster_id);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask() override;
  int register_endpoint(const obrpc::ObNetEndpointKey &endpoint_key, const int64_t expire_time);
  void follower_task();
  void leader_task();
  bool is_leader()
  {
    return ATOMIC_LOAD(&is_leader_);
  }
  bool is_stop()
  {
    return ATOMIC_LOAD(&is_stop_);
  }

public:
  // for replay, do nothing
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  // for checkpoint, do nothing
  virtual share::SCN get_rec_scn() override
  {
    return share::SCN::max_scn();
  }
  virtual int flush(share::SCN &scn) override
  {
    return OB_SUCCESS;
  }

  // for role change
  void switch_to_follower_forcedly() override final;
  int switch_to_leader() override final;
  int switch_to_follower_gracefully() override final;
  int resume_leader() override final;

private:
  bool is_inited_;
  bool is_leader_;
  bool is_stop_;
  int tg_id_;
  uint64_t cluster_id_;
  int64_t expire_time_;
  ObNetEndpointIngressManager ingress_manager_;
  obrpc::ObNetEndpointIngressRpcProxy endpoint_ingress_rpc_proxy_;
  const int64_t INGRESS_SERVICE_INTERVAL_US = 3L * 1000L * 1000L;                  // 3s
  const int64_t ENDPOINT_REGISTER_INTERVAL_US = 3L * 1000L * 1000L;                // 3s
  const int64_t ENDPOINT_EXPIRE_INTERVAL_US = 10 * ENDPOINT_REGISTER_INTERVAL_US;  // 30s
};
}  // namespace rootserver
}  // namespace oceanbase

#endif /* !OCEANBASE_ROOTSERVER_OB_INGRESS_BW_ALLOC_SERVICE_H */
