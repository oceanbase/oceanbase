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

#ifndef OCEANBASE_OBSERVER_OB_LEASE_STATE_MGR_H_
#define OCEANBASE_OBSERVER_OB_LEASE_STATE_MGR_H_

#include <stdint.h>

#include "share/ob_define.h"
#include "lib/task/ob_timer.h"
#include "lib/net/ob_addr.h"
#include "share/ob_lease_struct.h"
#include "share/ob_rs_mgr.h"

namespace oceanbase
{

namespace obrpc
{
class ObCommonRpcProxy;
}

namespace observer
{
class ObLeaseStateMgr;
class ObService;

class IHeartBeatProcess
{
public:
  virtual ~IHeartBeatProcess() {}
  virtual int init_lease_request(share::ObLeaseRequest &lease_request) = 0;
  virtual int do_heartbeat_event(const share::ObLeaseResponse &lease_response) = 0;
};

class ObRefreshSchemaStatusTimerTask: public common::ObTimerTask
{
public:
  ObRefreshSchemaStatusTimerTask();  //no repeat, no retry
  virtual ~ObRefreshSchemaStatusTimerTask() {}
  void destroy();
  virtual void runTimerTask() override;
};

//heartbeat or lease management
//root servers list maintenance
class ObLeaseStateMgr
{
public:
  friend class HeartBeat;
  ObLeaseStateMgr();
  ~ObLeaseStateMgr();

  void destroy();
  int init(obrpc::ObCommonRpcProxy *rpc_proxy, share::ObRsMgr *rs_mgr,
           IHeartBeatProcess *heartbeat_process,
           ObService &service,
           const int64_t renew_timeout = RENEW_TIMEOUT);
  int register_self();
  int register_self_busy_wait();
  int renew_lease();

  inline void set_stop() { stopped_  = true; }
  inline bool is_inited() const { return inited_; }
  inline bool is_valid_heartbeat() const;
  inline int64_t get_heartbeat_expire_time();
  inline int set_lease_response(const share::ObLeaseResponse &lease_response);
private:
  int try_report_sys_ls();
  class HeartBeat : public common::ObTimerTask
  {
  public:
    HeartBeat();
    ~HeartBeat();

    int init(ObLeaseStateMgr *lease_state_mgr);
    virtual void runTimerTask();
  private:
    bool inited_;
    ObLeaseStateMgr *lease_state_mgr_;
  private:
    DISALLOW_COPY_AND_ASSIGN(HeartBeat);
  };
  static const int64_t DELAY_TIME = 2 * 1000 * 1000;//2s
  static const int64_t RENEW_TIMEOUT = 2 * 1000 * 1000; //2s
  static const int64_t REGISTER_TIME_SLEEP = 2 * 1000 * 1000; //5s

private:
  int start_heartbeat();
  int do_renew_lease();
#ifdef OB_BUILD_TDE_SECURITY
  int update_master_key_info(const share::ObLeaseResponse &lease_response);
#endif
  int update_major_merge_info(const share::ObLeaseResponse &lease_response);
private:
  bool inited_;
  bool stopped_;
  share::ObLeaseResponse lease_response_;
  volatile int64_t lease_expire_time_ CACHE_ALIGNED;
  ObRefreshSchemaStatusTimerTask schema_status_task_;
  common::ObTimer hb_timer_;
  common::ObTimer cluster_info_timer_;
  common::ObTimer merge_timer_;
  share::ObRsMgr *rs_mgr_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  IHeartBeatProcess *heartbeat_process_;
  HeartBeat hb_;
  int64_t renew_timeout_;
  ObService *ob_service_;
  int64_t baseline_schema_version_;
  volatile int64_t heartbeat_expire_time_ CACHE_ALIGNED;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLeaseStateMgr);
};

bool ObLeaseStateMgr::is_valid_heartbeat() const
{
  return heartbeat_expire_time_ > common::ObTimeUtility::current_time();
}

int ObLeaseStateMgr::set_lease_response(const share::ObLeaseResponse &lease_response)
{
  if (lease_expire_time_ < lease_response.lease_expire_time_) {
    lease_expire_time_ = lease_response.lease_expire_time_;
  }
  heartbeat_expire_time_ = lease_response.heartbeat_expire_time_;
  return lease_response_.set(lease_response);
}

int64_t ObLeaseStateMgr::get_heartbeat_expire_time()
{
  return heartbeat_expire_time_;
}
}//end namespace observer
}//end namespace oceanbase
#endif //OCEANBASE_OBSERVER_OB_LEASE_STATE_MGR_H_
