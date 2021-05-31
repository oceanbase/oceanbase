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

#if 0
#define USING_LOG_PREFIX RS

#include "ob_in_zone_hb_state_mgr.h"
#include "ob_in_zone_master.h"
#include "storage/scs/ob_server_lease_mgr.h"
#include "share/ob_common_rpc_proxy.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;

namespace rootserver
{
// =================== ObInZoneMasterAddrMgr ==================
ObInZoneMasterAddrMgr::ObInZoneMasterAddrMgr()
  : inited_(false),
    in_zone_master_addr_(),
    in_zone_master_lease_(),
    lock_()
{
}

int ObInZoneMasterAddrMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObInZoneMasterAddrMgr::do_renew(
    common::ObAddr &new_in_zone_master_addr,
    TimeScope &in_zone_master_lease)
{
  int ret = OB_SUCCESS;
  common::ObAddr zone_addr;
  int64_t start_ts = -1;
  int64_t end_ts = -1;
  if (OB_FAIL(share::ObServerLeaseMgr::get_instance().get_zone_lease(
          zone_addr, start_ts, end_ts))) {
    LOG_WARN("fail to get zone lease", K(ret));
  } else {
    new_in_zone_master_addr = zone_addr;
    in_zone_master_lease.left_bound_ = start_ts;
    in_zone_master_lease.right_bound_ = end_ts;
  }
  return ret;
}

int ObInZoneMasterAddrMgr::renew_in_zone_master()
{
  int ret = OB_SUCCESS;
  common::ObAddr server;
  TimeScope lease;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(do_renew(server, lease))) {
    LOG_WARN("fail to do renew", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    in_zone_master_addr_ = server;
    in_zone_master_lease_ = lease;
  }
  return ret;
}

int ObInZoneMasterAddrMgr::get_in_zone_master_addr(
    common::ObAddr &in_zone_master_addr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    in_zone_master_addr = in_zone_master_addr_;
  }
  return ret;
}

int ObInZoneMasterAddrMgr::get_in_zone_master_info(
    common::ObAddr &in_zone_master_addr,
    TimeScope &in_zone_master_lease) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    in_zone_master_addr = in_zone_master_addr_;
    in_zone_master_lease = in_zone_master_lease_;
  }
  return ret;
}

bool ObInZoneMasterAddrMgr::need_renew_in_zone_master() const
{
  const int64_t now = common::ObTimeUtility::current_time();
  return now >= in_zone_master_lease_.right_bound_;
}

int ObInZoneMasterAddrMgr::register_self()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInZoneMasterAddrMgr not init", K(ret));
  } else if (OB_FAIL(renew_in_zone_master())) {
    LOG_WARN("fail to renew in zone master", K(ret));
  }
  return ret;
}

// ====================== ObInZoneMasterLocker ==================
ObInZoneMasterLocker::InZoneMasterLockerTask::InZoneMasterLockerTask()
  : inited_(false),
    in_zone_master_locker_(nullptr)
{
}

ObInZoneMasterLocker::InZoneMasterLockerTask::~InZoneMasterLockerTask()
{
}

int ObInZoneMasterLocker::InZoneMasterLockerTask::init(
    ObInZoneMasterLocker *in_zone_master_locker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == in_zone_master_locker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(in_zone_master_locker));
  } else {
    inited_ = true;
    in_zone_master_locker_ = in_zone_master_locker;
  }
  return ret;
}

void ObInZoneMasterLocker::InZoneMasterLockerTask::runTimerTask()
{
  if (OB_UNLIKELY(!inited_)) {
    int tmp_ret = OB_NOT_INIT;
    LOG_WARN("InZoneMasterLockerTask not init", "ret", tmp_ret);
  } else if (OB_UNLIKELY(nullptr == in_zone_master_locker_)) {
    int tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in zone master locker ptr is null", "ret", tmp_ret);
  } else {
    int tmp_ret = in_zone_master_locker_->try_renew_in_zone_master_lock();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to try renew in zone master lock","ret", tmp_ret);
    }
  }
}

ObInZoneMasterLocker::ObInZoneMasterLocker()
  : inited_(false),
    timer_(),
    in_zone_master_lock_got_(false),
    rpc_proxy_(nullptr),
    in_zone_master_addr_mgr_(nullptr),
    in_zone_master_(nullptr),
    self_addr_(),
    locker_task_(),
    in_zone_master_addr_(),
    in_zone_master_lease_(),
    need_force_renew_master_locker_(false)
{
}

ObInZoneMasterLocker::~ObInZoneMasterLocker()
{
}

int ObInZoneMasterLocker::init(
    const common::ObAddr &self_addr,
    ObSrvRpcProxy &rpc_proxy,
    ObInZoneMasterAddrMgr &in_zone_master_addr_mgr,
    ObInZoneMaster &in_zone_master)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!self_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(self_addr));
  } else if (OB_FAIL(timer_.init("InZoneMasterLocker"))) {
    LOG_WARN("fail to init timer", K(ret));
  } else if (OB_FAIL(locker_task_.init(this))) {
    LOG_WARN("fail to init locker task", K(ret));
  } else {
    inited_ = true;
    in_zone_master_lock_got_ = false;
    rpc_proxy_ = &rpc_proxy;
    in_zone_master_addr_mgr_ = &in_zone_master_addr_mgr;
    in_zone_master_ = &in_zone_master;
    self_addr_ = self_addr;
    in_zone_master_lease_.reset();
    need_force_renew_master_locker_ = false;
  }
  return ret;
}

void ObInZoneMasterLocker::destroy()
{
  timer_.destroy();
  inited_ = false;
  rpc_proxy_ = nullptr;
  in_zone_master_addr_mgr_ = nullptr;
  in_zone_master_ = nullptr;
  self_addr_.reset(); 
}

int ObInZoneMasterLocker::fetch_new_master_lock_lease(
    TimeScope &zone_master_lease)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t new_lease_left_bound = now;
  const int64_t new_lease_right_bound = now + IN_ZONE_MASTER_LEASE;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInZoneMasterLocker not init", K(ret));
  } else if (OB_FAIL(share::ObServerLeaseMgr::get_instance().update_zone_lease(
          in_zone_master_addr_,
          in_zone_master_lease_.left_bound_,
          in_zone_master_lease_.right_bound_,
          new_lease_left_bound,
          new_lease_right_bound))) {
    LOG_WARN("fail to update zone lease", K(ret));
  } else {
    zone_master_lease.reset();
    zone_master_lease.left_bound_ = new_lease_left_bound;
    zone_master_lease.right_bound_ = new_lease_right_bound;
  }
  return ret;
}

int ObInZoneMasterLocker::renew_in_zone_master()
{
  int ret = OB_SUCCESS;
  common::ObAddr my_in_zone_master_addr;
  TimeScope my_in_zone_master_lease;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == in_zone_master_addr_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in zone master addr mgr", K(ret));
  } else if (OB_FAIL(in_zone_master_addr_mgr_->renew_in_zone_master())) {
    LOG_WARN("fail to get in zone master addr", K(ret));
  } else if (OB_FAIL(in_zone_master_addr_mgr_->get_in_zone_master_info(
          my_in_zone_master_addr, my_in_zone_master_lease))) {
    LOG_WARN("fail to get in zone master info", K(ret));
  } else {
    in_zone_master_lease_ = my_in_zone_master_lease;
    in_zone_master_addr_ = my_in_zone_master_addr;
  }
  return ret;
}

int ObInZoneMasterLocker::do_renew_in_zone_master_lock()
{
  int ret = OB_SUCCESS;
  TimeScope my_zone_master_lease;
  int tmp_ret = fetch_new_master_lock_lease(my_zone_master_lease);
  if (OB_SUCCESS != tmp_ret) {
    // rewrite ret
    if (OB_FAIL(renew_in_zone_master())) {
      LOG_WARN("fail to renew in zone master", K(ret));
    } else if (OB_FAIL(fetch_new_master_lock_lease(my_zone_master_lease))) {
      LOG_WARN("fail to fetch new master lock lease", K(ret));
    } else {
      in_zone_master_lease_ = my_zone_master_lease;
      in_zone_master_addr_ = self_addr_;
    }
  } else {
    // succeed to get mater lock lease, set local observer to in_zone_master
    in_zone_master_lease_ = my_zone_master_lease;
    in_zone_master_addr_ = self_addr_;
  }
  return ret;
}

bool ObInZoneMasterLocker::is_valid_master_lock() const
{
  return in_zone_master_lease_.right_bound_ > ObTimeUtility::current_time();
}

int ObInZoneMasterLocker::try_renew_in_zone_master_lock()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("InZoneMasterLocker not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == in_zone_master_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in zone master ptr is null", K(ret));
  } else {
    if (ATOMIC_LOAD(&need_force_renew_master_locker_) || in_zone_master_lock_got_) {
      ATOMIC_STORE(&need_force_renew_master_locker_, false);
      ret = do_renew_in_zone_master_lock();
      if (OB_FAIL(ret)) {
        if (!is_valid_master_lock()) {
          in_zone_master_lock_got_ = false;
          int tmp_ret = in_zone_master_->try_trigger_stop_in_zone_master();
        } else {
          // do not stop service until lock lease
        }
      } else {
        in_zone_master_lock_got_ = true;
        int tmp_ret = in_zone_master_->try_trigger_restart_in_zone_master();
      }
    }
    const bool repeat = false;
    if (OB_FAIL(timer_.schedule(locker_task_, DELAY_TIME, repeat))) {
      LOG_ERROR("fail to schedule timer task", K(ret));
    }
  }
  return ret;
}

int ObInZoneMasterLocker::register_self()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("InZoneMasterLocker not init", K(ret));
  } else {
    const bool repeat = false;
    if (OB_FAIL(timer_.schedule(locker_task_, 0/* no delay */, repeat))) {
      LOG_WARN("fail to schedule locker task", K(ret));
    }
  }
  return ret;
}

void ObInZoneMasterLocker::notify_force_renew_in_zone_master_locker()
{
  ATOMIC_STORE(&need_force_renew_master_locker_, true);
}

// ====================== ObInZoneHbStateMgr =====================
ObInZoneHbStateMgr::InZoneHeartBeat::InZoneHeartBeat()
  : inited_(false),
    in_zone_hb_state_mgr_(nullptr)
{
}
ObInZoneHbStateMgr::InZoneHeartBeat::~InZoneHeartBeat()
{
}

int ObInZoneHbStateMgr::InZoneHeartBeat::init(
    ObInZoneHbStateMgr *in_zone_hb_state_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == in_zone_hb_state_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(in_zone_hb_state_mgr));
  } else {
    in_zone_hb_state_mgr_ = in_zone_hb_state_mgr;
    inited_ = true;
  }
  return ret;
}

void ObInZoneHbStateMgr::InZoneHeartBeat::runTimerTask()
{
  if (OB_UNLIKELY(!inited_)) {
    int tmp_ret = OB_SUCCESS;
    LOG_WARN("InZoneHeartBeat not init", "ret", tmp_ret);
  } else if (OB_UNLIKELY(nullptr == in_zone_hb_state_mgr_)) {
    int tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in zone hb state mgr ptr is null", "ret", tmp_ret);
  } else {
    int tmp_ret = in_zone_hb_state_mgr_->renew_in_zone_hb();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to renew in zone heartbeat", "ret", tmp_ret);
    }
  }
}

ObInZoneHbStateMgr::ObInZoneHbStateMgr()
  : inited_(false),
    stopped_(false),
    in_zone_hb_response_(),
    timer_(),
    rpc_proxy_(nullptr),
    in_zone_master_locker_(nullptr),
    in_zone_master_addr_mgr_(nullptr),
    self_addr_(),
    in_zone_master_addr_(),
    hb_(),
    heartbeat_expire_time_(-1)
{
}

ObInZoneHbStateMgr::~ObInZoneHbStateMgr()
{
}

int ObInZoneHbStateMgr::init(
    const common::ObAddr &self_addr,
    ObSrvRpcProxy &rpc_proxy,
    ObInZoneMasterLocker &in_zone_master_locker,
    ObInZoneMasterAddrMgr &in_zone_master_addr_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!self_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(self_addr));
  } else if (OB_FAIL(timer_.init("InZoneHb"))) {
    LOG_WARN("fail to init timer", K(ret));
  } else if (OB_FAIL(hb_.init(this))) {
    LOG_WARN("fail to init", K(ret));
  } else {
    rpc_proxy_ = &rpc_proxy;
    in_zone_master_locker_ = &in_zone_master_locker;
    in_zone_master_addr_mgr_ = &in_zone_master_addr_mgr;
    self_addr_ = self_addr;
    heartbeat_expire_time_ = -1;
    inited_ = true;
  }
  return ret;
}

void ObInZoneHbStateMgr::destroy()
{
  timer_.destroy();
  inited_ = false;
  stopped_ = true;
  in_zone_hb_response_;
  rpc_proxy_ = nullptr;
  in_zone_master_locker_ = nullptr;
  in_zone_master_addr_mgr_ = nullptr;
  self_addr_.reset();
}

int ObInZoneHbStateMgr::register_self_busy_wait()
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin register self busy wait");
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (!stopped_) {
      if (OB_FAIL(do_renew_in_zone_hb())) {
        LOG_WARN("fail to do register failed", K(ret),
                 "retry latency(ms)", REGISTER_TIME_USLEEP / 1000);
        usleep(REGISTER_TIME_USLEEP);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = renew_in_zone_master())) {
          LOG_WARN("fail to renew in zone master", K(ret));
        }
      } else {
        if (OB_FAIL(start_in_zone_hb())) {
          LOG_ERROR("fail to do start in zone hb", K(ret));
        }
        break;
      }
    }
  }
  if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("fail to register self busy wait", K(ret));
  }
  LOG_INFO("end register self busy wait");
  return ret;
}

bool ObInZoneHbStateMgr::is_valid_in_zone_hb() const
{
  return heartbeat_expire_time_ > ObTimeUtility::current_time();
}

void ObInZoneHbStateMgr::try_notify_in_zone_master_locker()
{
  if (!is_valid_in_zone_hb() && nullptr != in_zone_master_locker_) {
    in_zone_master_locker_->notify_force_renew_in_zone_master_locker();
  }
}

int ObInZoneHbStateMgr::init_in_zone_hb_request(
    ObInZoneHbRequest &hb_request)
{
  int ret = OB_SUCCESS;
  hb_request.server_ = self_addr_;
  return ret;
}

int ObInZoneHbStateMgr::do_renew_in_zone_hb()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == in_zone_master_addr_mgr_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("in zone master addr state not match", K(ret), K(in_zone_master_addr_mgr_));
  } else {
    ObInZoneHbRequest hb_request;
    ObInZoneHbResponse hb_response;
    if (OB_FAIL(init_in_zone_hb_request(hb_request))) {
      LOG_WARN("fail to init in zone hb request", K(ret));
    } else if (OB_FAIL(rpc_proxy_->to(in_zone_master_addr_)
                                  .timeout(RENEW_TIMEOUT)
                                  .renew_in_zone_hb(hb_request, hb_response))) {
      LOG_WARN("fail to renew in zone hb", K(ret));
    } else {
      in_zone_hb_response_ = hb_response;
      heartbeat_expire_time_ = hb_response.in_zone_hb_expire_time_;
    }
  }
  return ret;
}

int ObInZoneHbStateMgr::renew_in_zone_hb()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("in zone heartbeat state mgr stopped", K(ret));
  } else {
    ret = do_renew_in_zone_hb();
    if (OB_FAIL(ret)) {
      try_notify_in_zone_master_locker();
      if (need_renew_in_zone_master()) {
        if (OB_FAIL(renew_in_zone_master())) {
          LOG_WARN("fail to execute renew in zone master", K(ret));
        }
      } else {
        // no need to renew in zone master
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(do_renew_in_zone_hb())) {
          LOG_WARN("fail to do renew in zone hb", K(ret));
        }
      }
    } else {
      // do renew renew lease succeed
    }
    const bool repeat = false;
    if (OB_FAIL(timer_.schedule(hb_, DELAY_TIME, repeat))) {
      LOG_WARN("fail to schedule timer task", K(ret));
    }
  }
  return ret;
}

int ObInZoneHbStateMgr::start_in_zone_hb()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const bool repeat = false;
    if (OB_FAIL(timer_.schedule(hb_, DELAY_TIME, repeat))) {
      LOG_WARN("fail to schedule", K(ret));
    }
  }
  return ret;
}

int ObInZoneHbStateMgr::renew_in_zone_master()
{
  int ret = OB_SUCCESS;
  common::ObAddr my_in_zone_master;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == in_zone_master_addr_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in zone master addr mgr", K(ret));
  } else if (OB_FAIL(in_zone_master_addr_mgr_->renew_in_zone_master())) {
    LOG_WARN("fail to do renew in zone master add mgr", K(ret));
  } else if (OB_FAIL(in_zone_master_addr_mgr_->get_in_zone_master_addr(my_in_zone_master))) {
    LOG_WARN("fail to get in zone master addr", K(ret));
  } else {
    in_zone_master_addr_ = my_in_zone_master;
  }
  return ret;
}

bool ObInZoneHbStateMgr::need_renew_in_zone_master() const
{
  bool need_renew = false;
  if (OB_UNLIKELY(nullptr == in_zone_master_addr_mgr_)) {
    need_renew = false; // bypass, since addr mgr is null
  } else if (in_zone_master_addr_mgr_->need_renew_in_zone_master()) {
    need_renew = true;
  } else {
    need_renew = false;
  }
  return need_renew;
}

}//end namespace rootserver
}//end namespace oceanbase
#endif
