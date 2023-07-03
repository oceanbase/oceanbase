// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//         http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX RS
#include "ob_backup_base_service.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "logservice/ob_log_service.h"

using namespace oceanbase;
using namespace rootserver;

ObBackupBaseService::ObBackupBaseService()
  :  is_created_(false),
     tg_id_(-1),
     proposal_id_(0),
     wakeup_cnt_(0),
     interval_idle_time_us_(INT64_MAX),
     thread_cond_(),
     thread_name_("")
{
}

ObBackupBaseService::~ObBackupBaseService()
{
}

void ObBackupBaseService::run1()
{
  int tmp_ret = OB_SUCCESS;
  lib::set_thread_name(thread_name_);
  LOG_INFO("ObBackupBaseService thread run", K(thread_name_));
  if (OB_UNLIKELY(!is_created_)) {
    tmp_ret = OB_NOT_INIT;
    LOG_WARN_RET(OB_NOT_INIT, "not init", K(tmp_ret));
  } else {
    ObRSThreadFlag rs_work;
    run2();
  }
}

int ObBackupBaseService::create(const char* thread_name, ObBackupBaseService &tenant_thread, int32_t event_no)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_created_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_ISNULL(thread_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("thread name is null", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SimpleLSService, tg_id_))) {
    LOG_ERROR("create tg failed", KR(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    LOG_ERROR("set thread runable fail", KR(ret));
  } else if (OB_FAIL(thread_cond_.init(event_no))) {
    LOG_WARN("fail to init thread cond", K(ret), K(event_no));
  } else {
    stop();
    thread_name_ = thread_name;
    wakeup_cnt_ = 0;
    is_created_ = true;
  }
  return ret;
}

void ObBackupBaseService::destroy()
{
  LOG_INFO("[BACKUP_SERVICE] thread destory start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    {
      ObThreadCondGuard guard(thread_cond_);
      thread_cond_.broadcast();
    }
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  is_created_ = false;
  LOG_INFO("[BACKUP_SERVICE] thread destory finish", K(tg_id_), K(thread_name_));
}

int ObBackupBaseService::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_REENTRANT_LOGICAL_START(tg_id_))) {
    LOG_WARN("failed to start", KR(ret));
  }
  LOG_INFO("[BACKUP_SERVICE] thread start", K(ret), K(tg_id_), K(thread_name_));
  return ret;
}

void ObBackupBaseService::stop()
{
  LOG_INFO("[BACKUP_SERVICE] thread ready to stop", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_REENTRANT_LOGICAL_STOP(tg_id_);
    wakeup();
  }
  LOG_INFO("[BACKUP_SERVICE] thread stopped", K(tg_id_), K(thread_name_));
}

void ObBackupBaseService::wait()
{
  LOG_INFO("[BACKUP_SERVICE] thread ready to wait", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_REENTRANT_LOGICAL_WAIT(tg_id_);
  }
  LOG_INFO("[BACKUP_SERVICE] thread in wait", K(tg_id_), K(thread_name_));
}

void ObBackupBaseService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

void ObBackupBaseService::idle()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  if (has_set_stop() || wakeup_cnt_ > 0) {
    wakeup_cnt_ = 0;
  } else {
    thread_cond_.wait_us(interval_idle_time_us_);
  }
}

void ObBackupBaseService::switch_to_follower_forcedly()
{
  stop();
  LOG_INFO("[BACKUP_SERVICE]switch to follower finish", K(tg_id_), K(thread_name_));
}

int ObBackupBaseService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t proposal_id = 0;
  if (OB_FAIL(start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else if (OB_FAIL(MTL(logservice::ObLogService *)->get_palf_role(share::SYS_LS, role, proposal_id))) {
    LOG_WARN("get ObBackupBaseService role fail", K(ret));
  } else {
    proposal_id_ = proposal_id;
    wakeup();
    LOG_INFO("[BACKUP_SERVICE]switch to leader finish", K(tg_id_), K(thread_name_));
  }
  return ret;
}

int ObBackupBaseService::switch_to_follower_gracefully()
{
  stop();
  LOG_INFO("[BACKUP_SERVICE]switch to follower gracefully", K(tg_id_), K(thread_name_));
  return OB_SUCCESS;
}

int ObBackupBaseService::resume_leader()
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t proposal_id = 0;
  if (OB_FAIL(start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else if (OB_FAIL(MTL(logservice::ObLogService *)->get_palf_role(share::SYS_LS, role, proposal_id))) {
    LOG_WARN("get ObBackupBaseService role fail", K(ret));
  } else {
    proposal_id_ = proposal_id;
    wakeup();
    LOG_INFO("[BACKUP_SERVICE]resume leader finish", K(tg_id_), K(thread_name_));
  }
  return ret;
}

int ObBackupBaseService::replay(
    const void *buffer,
    const int64_t nbytes,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  UNUSED(buffer);
  UNUSED(nbytes);
  UNUSED(lsn);
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObBackupBaseService::flush(share::SCN &scn)
{
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObBackupBaseService::check_leader()
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t proposal_id = 0;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not created backup thread", K(ret));
  } else if (OB_FAIL(MTL(logservice::ObLogService *)->get_palf_role(share::SYS_LS, role, proposal_id))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_NOT_MASTER;
      LOG_WARN("sys ls is not exist, service can't be leader", K(ret));
    } else {
      LOG_WARN("get ObBackupBaseService role fail", K(ret));
    }
  } else if (common::ObRole::LEADER == role && proposal_id == proposal_id_) {
  } else {
    ret = OB_NOT_MASTER;
  }
  return ret;
}

void ObBackupBaseService::mtl_thread_stop()
{
  LOG_INFO("[BACKUP_SERVICE] thread stop start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
  }
  LOG_INFO("[BACKUP_SERVICE] thread stop finish", K(tg_id_), K(thread_name_));
}

void ObBackupBaseService::mtl_thread_wait()
{
  LOG_INFO("[BACKUP_SERVICE] thread wait start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    {
      ObThreadCondGuard guard(thread_cond_);
      thread_cond_.broadcast();
    }
    TG_WAIT(tg_id_);
  }
  LOG_INFO("[BACKUP_SERVICE] thread wait finish", K(tg_id_), K(thread_name_));
}