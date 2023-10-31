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

#define USING_LOG_PREFIX RS_COMPACTION

#include "rootserver/freeze/ob_freeze_reentrant_thread.h"
#include "share/ob_service_epoch_proxy.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "logservice/ob_log_handler.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace rootserver
{
ObFreezeReentrantThread::ObFreezeReentrantThread(const uint64_t tenant_id)
  : ObRsReentrantThread(true), tenant_id_(tenant_id),
    sql_proxy_(nullptr), is_paused_(false), epoch_(-1)
{}

int ObFreezeReentrantThread::set_epoch(const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (epoch < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(epoch));
  } else {
    epoch_ = epoch;
  }
  return ret;
}

void ObFreezeReentrantThread::pause()
{
  is_paused_ = true;
}

void ObFreezeReentrantThread::resume()
{
  is_paused_ = false;
}

int ObFreezeReentrantThread::try_idle(
    const int64_t idle_time_us,
    const int exe_ret)
{
  int ret = exe_ret;
  if (idle_time_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(idle_time_us));
  } else if (!stop_) {
    if (is_paused()) {
      LOG_INFO("this thread is paused", KR(ret), K(idle_time_us), "epoch", get_epoch());
      while (!stop_ && is_paused()) {
        get_cond().wait(idle_time_us / 1000);
        // if in paused state, we also need to update run_ts. otherwise, the checker may
        // mark this thread as hang.
        update_last_run_timestamp();
      }
      LOG_INFO("this thread is not paused", KR(ret), K(idle_time_us), "epoch", get_epoch());
    } else {
      get_cond().wait(idle_time_us / 1000);
    }
  }
  return ret;
}

int ObFreezeReentrantThread::obtain_proposal_id_from_ls(
    const bool is_primary_service,
    int64_t &proposal_id,
    ObRole &role)
{
  int ret = OB_SUCCESS;

  storage::ObLSHandle ls_handle;
  logservice::ObLogHandler *handler = nullptr;
  logservice::ObLogRestoreHandler *restore_handler = nullptr;
  if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(SYS_LS, ls_handle, ObLSGetMod::RS_MOD))) {
    LOG_WARN("fail to get ls", KR(ret));
  } else if (is_primary_service) {
    if (OB_ISNULL(ls_handle.get_ls())
        || OB_ISNULL(handler = ls_handle.get_ls()->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not null", KR(ret), K(is_primary_service));
    } else if (OB_FAIL(handler->get_role(role, proposal_id))) {
      LOG_WARN("fail to get role", KR(ret), K(is_primary_service));
    }
  } else {
    if (OB_ISNULL(ls_handle.get_ls())
        || OB_ISNULL(restore_handler = ls_handle.get_ls()->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not null", KR(ret), K(is_primary_service));
    } else if (OB_FAIL(restore_handler->get_role(role, proposal_id))) {
      LOG_WARN("fail to get role", KR(ret), K(is_primary_service));
    }
  }
  return ret;
}

} // rootserver
} // oceanbase
