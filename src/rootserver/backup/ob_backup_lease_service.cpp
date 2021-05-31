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

#define USING_LOG_PREFIX RS
#include "ob_backup_lease_service.h"
#include "lib/thread/ob_thread_name.h"
#include "share/backup/ob_backup_manager.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_rs_event_history_table_operator.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace obrpc;
using namespace rootserver;

ObBackupLeaseService::ObBackupLeaseService()
    : is_inited_(false),
      can_be_leader_ts_(0),
      expect_round_(0),
      lock_(),
      backup_lease_info_mgr_(),
      lease_info_(),
      sql_proxy_(nullptr),
      idle_(has_set_stop()),
      local_addr_()
{}

ObBackupLeaseService::~ObBackupLeaseService()
{}

int ObBackupLeaseService::init(const ObAddr& addr, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(backup_lease_info_mgr_.init(addr, sql_proxy))) {
    LOG_WARN("failed to init backup_lease_info_mgr", K(ret));
  } else {
    is_inited_ = true;
    can_be_leader_ts_ = 0;
    expect_round_ = 0;
    lease_info_.reset();
    sql_proxy_ = &sql_proxy;
    local_addr_ = addr;
  }

  return ret;
}

int ObBackupLeaseService::register_scheduler(ObRsReentrantThread& scheduler)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(backup_schedulers_.push_back(&scheduler))) {
    LOG_WARN("failed to add scheduler", K(ret));
  }
  return ret;
}

int ObBackupLeaseService::start_lease()
{
  int ret = OB_SUCCESS;
  {
    SpinRLockGuard guard(lock_);
    int64_t expect_round = expect_round_ + 1;
    ROOTSERVICE_EVENT_ADD("backup_lease", "start backup lease", K_(local_addr), K(expect_round));
  }
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    ++expect_round_;
    can_be_leader_ts_ = ObTimeUtil::current_time();
    FLOG_INFO("start backup lease service", K(*this));
  }
  wakeup();
  return ret;
}

int ObBackupLeaseService::stop_lease()
{
  int ret = OB_SUCCESS;

  {
    SpinRLockGuard guard(lock_);
    int64_t expect_round = expect_round_ + 1;

    ROOTSERVICE_EVENT_ADD("backup_lease", "stop backup lease", K_(local_addr), K(expect_round));
  }
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    stop_backup_scheduler_();
    ++expect_round_;
    can_be_leader_ts_ = 0;
    FLOG_INFO("stop backup lease service", K(*this));
  }
  wakeup();
  return ret;
}

void ObBackupLeaseService::wait_lease()
{
  // do nothing
}

int ObBackupLeaseService::start()
{
  return ThreadPool::start();
}

void ObBackupLeaseService::stop()
{
  ThreadPool::stop();
  idle_.wakeup();
}

void ObBackupLeaseService::wakeup()
{
  idle_.wakeup();
}

void ObBackupLeaseService::destroy()
{
  const int64_t start_ts = ObTimeUtil::current_time();

  LOG_INFO("start destroy ObBackupLeaseService");
  stop_lease();
  wait_backup_scheduler_stop_();
  stop();
  backup_schedulers_.destroy();
  wait();
  ThreadPool::destroy();
  const int64_t cost_ts = ObTimeUtil::current_time() - start_ts;
  LOG_INFO("finish destroy ObBackupLeaseService", K(cost_ts));
}

int ObBackupLeaseService::check_lease()
{
  int ret = OB_SUCCESS;
  bool is_lease_valid = false;
  if (OB_FAIL(get_lease_status(is_lease_valid))) {
    LOG_WARN("failed to get lease status", K(ret));
  } else if (!is_lease_valid) {
    ret = OB_LEASE_NOT_ENOUGH;
    LOG_WARN("not own lease, cannot do work", K(ret));
  }
  return ret;
}

int ObBackupLeaseService::get_lease_status(bool& is_lease_valid)
{
  int ret = OB_SUCCESS;
  is_lease_valid = false;
  SpinRLockGuard guard(lock_);
  const int64_t cur_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (expect_round_ != lease_info_.round_ || !can_be_leader_()) {
    is_lease_valid = false;
    LOG_WARN("not own lease, because round not match", K(*this));
  } else if (cur_ts - lease_info_.lease_start_ts_ >= ObBackupLeaseInfo::MAX_LEASE_TIME) {
    is_lease_valid = false;
    LOG_WARN("not own lease, because lease time not match", K(lease_info_));
  } else if (!lease_info_.is_leader_) {
    is_lease_valid = false;
    LOG_WARN("not own lease, because not leader now", K(lease_info_));
  } else {
    is_lease_valid = true;
  }

  return ret;
}

int ObBackupLeaseService::start_backup_scheduler_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  FLOG_INFO("[BACKUP_LEASE] start_backup_scheduler");

  for (int64_t i = 0; i < backup_schedulers_.count(); ++i) {
    ObRsReentrantThread* scheduler = backup_schedulers_.at(i);
    FLOG_INFO("[BACKUP_LEASE] start scheduler", "name", scheduler->get_thread_name());
    if (OB_SUCCESS != (tmp_ret = scheduler->start())) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_ERROR("failed to start scheduler", K(tmp_ret), K(ret), "name", scheduler->get_thread_name());
    }
  }
  return ret;
}

void ObBackupLeaseService::stop_backup_scheduler_()
{
  FLOG_INFO("[BACKUP_LEASE] stop_backup_scheduler");
  for (int64_t i = 0; i < backup_schedulers_.count(); ++i) {
    ObRsReentrantThread* scheduler = backup_schedulers_.at(i);
    FLOG_INFO("[BACKUP_LEASE] stop scheduler", "name", scheduler->get_thread_name());
    scheduler->stop();
  }
}

void ObBackupLeaseService::wait_backup_scheduler_stop_()
{
  int64_t start_ts = ObTimeUtil::current_time();
  FLOG_INFO("[BACKUP_LEASE] start wait_backup_scheduler_stop_");

  for (int64_t i = 0; i < backup_schedulers_.count(); ++i) {
    ObRsReentrantThread* scheduler = backup_schedulers_.at(i);
    FLOG_INFO("[BACKUP_LEASE] waiting scheduler stop", "name", scheduler->get_thread_name(), KP(scheduler));
    int64_t start_ts2 = ObTimeUtil::current_time();
    scheduler->wait();
    int64_t cost_ts2 = ObTimeUtil::current_time() - start_ts2;
    FLOG_INFO("[BACKUP_LEASE] waiting scheduler stop", K(cost_ts2), "name", scheduler->get_thread_name());
  }

  int64_t cost_ts = ObTimeUtil::current_time() - start_ts;
  FLOG_INFO("[BACKUP_LEASE] finish wait_backup_scheduler_stop_", K(cost_ts));
}

int64_t ObBackupLeaseService::ObBackupLeaseIdle::get_idle_interval_us()
{
  return DEFAULT_IDLE_US;
}

void ObBackupLeaseService::run1()
{
  int tmp_ret = OB_SUCCESS;
  lib::set_thread_name("BackupLease");
  FLOG_INFO("ObBackupLeaseService start");

  while (!has_set_stop()) {
    if (OB_SUCCESS != (tmp_ret = renew_lease_())) {
      LOG_WARN("failed to renew lease", K(tmp_ret));
    }
    do_idle(tmp_ret);
  }

  FLOG_INFO("ObBackupLeaseService exit");
}

void ObBackupLeaseService::do_idle(const int32_t result)
{
  int64_t idle_us = ObBackupLeaseIdle::DEFAULT_IDLE_US;

  {
    SpinRLockGuard guard(lock_);
    if (can_be_leader_ts_ > 0 || lease_info_.is_leader_ || expect_round_ != lease_info_.round_ ||
        OB_SUCCESS != result) {
      idle_us = ObBackupLeaseIdle::FAST_IDLE_US;
    } else {
      LOG_INFO("do idle", K(idle_us), K(can_be_leader_ts_), K(expect_round_), K(lease_info_));
    }
  }
  idle_.idle(idle_us);  // ignore ret
}

// Only single thread will change lease_info_
int ObBackupLeaseService::renew_lease_()
{
  int ret = OB_SUCCESS;
  bool need_wait_stop = false;
  int64_t can_be_leader_ts = 0;
  int64_t next_round = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    next_round = expect_round_;
    if (expect_round_ != lease_info_.round_) {
      need_wait_stop = true;
    }

    can_be_leader_ts = can_be_leader_ts_;
  }

  if (OB_SUCC(ret) && need_wait_stop) {
    stop_backup_scheduler_();
    wait_backup_scheduler_stop_();
    if (OB_FAIL(clean_backup_lease_info_(next_round))) {
      LOG_WARN("failed to clean backup leader info", K(ret), K(next_round));
    }
  }

  if (OB_SUCC(ret) && can_be_leader_ts > 0 && next_round == lease_info_.round_) {
    share::ObBackupLeaseInfo new_lease_info;
    bool need_start_scheduler = !lease_info_.is_leader_;
    const char* msg = "";
    if (OB_FAIL(check_sys_backup_info_())) {
      LOG_WARN("failed to check sys backup info", K(ret));
    } else if (OB_FAIL(backup_lease_info_mgr_.renew_lease(
                   can_be_leader_ts, next_round, lease_info_, new_lease_info, msg))) {
      LOG_WARN("failed to do renew lease", K(ret), K(lease_info_), K(new_lease_info));
    } else if (OB_FAIL(set_backup_lease_info_(new_lease_info, msg))) {
      LOG_ERROR("failed to set lease info", K(ret), K(lease_info_), K(new_lease_info));
    } else if (!new_lease_info.is_leader_) {
      LOG_WARN("cannot own new lease", K(new_lease_info));
    } else if (need_start_scheduler) {
      if (OB_FAIL(start_backup_scheduler_())) {
        LOG_ERROR("failed to start backup scheduler", K(ret));
      }
    }
  }

  LOG_TRACE("after renew lease", K(next_round), K(expect_round_), K(lease_info_));

  return ret;
}

// Only single thread will change lease_info_
int ObBackupLeaseService::clean_backup_lease_info_(const int64_t next_round)
{
  int ret = OB_SUCCESS;
  ObBackupLeaseInfo new_lease_info;
  bool is_leader_changed = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(backup_lease_info_mgr_.clean_backup_lease_info(next_round, lease_info_, new_lease_info))) {
    LOG_WARN("failed to release lease", K(ret), K(next_round));
  } else {
    SpinWLockGuard guard(lock_);
    is_leader_changed = lease_info_.is_leader_;
    lease_info_ = new_lease_info;
  }
  FLOG_INFO("clean_backup_leader_info_", K(ret), K(lease_info_), K(is_leader_changed));
  if (is_leader_changed) {
    ROOTSERVICE_EVENT_ADD("backup_lease", "release backup lease", K_(local_addr), "round", lease_info_.round_);
  }
  return ret;
}

int ObBackupLeaseService::set_backup_lease_info_(const share::ObBackupLeaseInfo& lease_info, const char* msg)
{
  int ret = OB_SUCCESS;
  bool is_leader_changed = false;

  if (!lease_info.is_valid() || OB_ISNULL(msg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(msg), K(lease_info));
  } else {
    SpinWLockGuard guard(lock_);
    const int64_t cur_ts = ObTimeUtil::current_time();

    if (lease_info.round_ != lease_info_.round_ || OB_ISNULL(msg)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid lease info", K(ret), K(lease_info), K(lease_info_), KP(msg));
    } else if (lease_info.is_leader_ && lease_info.lease_start_ts_ + ObBackupLeaseInfo::MAX_LEASE_TIME < cur_ts) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lease info start ts is too old", K(ret), K(lease_info), K(cur_ts));
    } else {
      if (lease_info.is_leader_ != lease_info_.is_leader_) {
        is_leader_changed = true;
      }
      lease_info_ = lease_info;
    }
  }

  FLOG_INFO("set_backup_leader_info_", K(ret), K(lease_info_));
  if (OB_SUCC(ret) && is_leader_changed) {
    if (lease_info_.is_leader_) {
      ROOTSERVICE_EVENT_ADD(
          "backup_lease", "acquire backup lease", K_(local_addr), "round", lease_info_.round_, K(msg), K_(lease_info));
    } else {
      ROOTSERVICE_EVENT_ADD("backup_lease", "release backup lease", K_(local_addr), "round", lease_info_.round_);
    }
  }
  return ret;
}

int ObBackupLeaseService::check_sys_backup_info_()
{
  int ret = OB_SUCCESS;
  ObBackupInfoChecker checker;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(checker.init(sql_proxy_))) {
    LOG_WARN("failed to init checker", K(ret));
  } else if (OB_FAIL(checker.check(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to check sys backup info", K(ret));
  }

  LOG_TRACE("finish check sys backup info", K(ret));
  return ret;
}
