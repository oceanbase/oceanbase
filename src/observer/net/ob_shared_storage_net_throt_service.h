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

#ifndef OCEANBASE_ROOTSERVER_SHARED_STORAGE_NET_THROT_SERVICE_H
#define OCEANBASE_ROOTSERVER_SHARED_STORAGE_NET_THROT_SERVICE_H

#include "observer/net/ob_shared_storage_net_throt_rpc_proxy.h"
#include "rootserver/ob_primary_ls_service.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace rootserver
{
// ObSharedStorageNetThrotManager is for RS
class ObSharedStorageNetThrotManager
{
public:
  ObSharedStorageNetThrotManager();
  ~ObSharedStorageNetThrotManager();
  int init();
  void reset();
  void destroy();
  // RS registers the endpoint
  int register_endpoint(const obrpc::ObSSNTEndpointArg &endpoint_storage_infos);
  int clear_expired_infos();
  // RS asks the observer for the quotas
  int collect_predict_resource(const int64_t &expire_time);
  int register_or_update_predict_resource(
      const ObAddr &addr, const obrpc::ObSharedDeviceResourceArray *predict_resources, const int64_t &expire_time);
  int assign_type_value(
      obrpc::ObSSNTValue *value, const obrpc::ResourceType &type, const int64_t &val, const int64_t &expire_time);
  // RS performs fair distribution algorithm and updates the quota
  int update_quota_plan();
  int update_one_storage_quota_plan(
      const ObTrafficControl::ObStorageKey &storage_key, obrpc::ObQuotaPlanMap *quota_plan_map);
  int cal_iops_quota(const int64_t limit, obrpc::ObQuotaPlanMap *quota_plan_map);
  int cal_bw_quota(
      const int64_t limit, int64_t obrpc::ObSSNTResource::*member_ptr, obrpc::ObQuotaPlanMap *quota_plan_map);
  int cal_iobw_quota(obrpc::ObQuotaPlanMap *quota_plan_map);
  // RS submits the quota to the observer
  int commit_quota_plan();
  int get_predict(const ObTrafficControl::ObStorageKey &storage_key, const obrpc::ObSSNTValue &value,
      obrpc::ObSharedDeviceResourceArray &arg);
  int register_or_update_storage_key_limit(const ObTrafficControl::ObStorageKey &storage_key);
  int get_storage_iops_and_bandwidth_limit(
      const ObTrafficControl::ObStorageKey &storage_key, int64_t &max_iops, int64_t &max_bandwidth);
private:
  int is_unlimited_category(const ObStorageInfoType category) const;
private:
  obrpc::ObEndpointRegMap endpoint_infos_map_;  // for register
  obrpc::ObBucketThrotMap bucket_throt_map_;
  obrpc::ObStorageKeyLimitMap storage_key_limit_map_;
  bool is_inited_;
  ObSpinLock lock_;
  int clear_storage_key(const ObSEArray<ObTrafficControl::ObStorageKey, 10> &delete_storage_keys);
  DISALLOW_COPY_AND_ASSIGN(ObSharedStorageNetThrotManager);
};
// ObSSNTAllocService is for RS and observer
class ObSSNTAllocService : public common::ObTimerTask,
                           public logservice::ObIRoleChangeSubHandler,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler
{
public:
  ObSSNTAllocService()
      : is_inited_(false),
        is_leader_(false),
        is_stop_(true),
        tg_id_(-1),
        cluster_id_(OB_INVALID_CLUSTER_ID),
        expire_time_(0),
        quota_manager_()
  {}
  virtual ~ObSSNTAllocService()
  {
    destroy();
  }
  int init(const uint64_t cluster_id);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask() override;
  int register_endpoint(const obrpc::ObSSNTEndpointArg &endpoint_storage_infos);
  int follower_task();
  int leader_task();
  bool is_leader()
  {
    return ATOMIC_LOAD(&is_leader_);
  }
  bool is_stop()
  {
    return ATOMIC_LOAD(&is_stop_);
  }

  // this part is for RS thread
  // for replay, do nothing
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  // for checkpoint
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
  bool is_leader_;  // atomic
  bool is_stop_;
  int tg_id_;
  uint64_t cluster_id_;
  int64_t expire_time_;
  ObSharedStorageNetThrotManager quota_manager_;
  obrpc::ObSSNTRpcProxy endpoint_rpc_proxy_;
  int get_follower_register_arg(obrpc::ObSSNTEndpointArg &arg);
  const int64_t QUOTA_SERVICE_INTERVAL_US = 3L * 1000L * 1000L;                    // 3s
  const int64_t ENDPOINT_REGISTER_INTERVAL_US = 3L * 1000L * 1000L;                // 3s
  const int64_t ENDPOINT_EXPIRE_INTERVAL_US = 10 * ENDPOINT_REGISTER_INTERVAL_US;  // 30s
};
}  // namespace rootserver
}  // namespace oceanbase

#endif /* !OCEANBASE_ROOTSERVER_SHARED_STORAGE_NET_THROT_SERVICE_H */
