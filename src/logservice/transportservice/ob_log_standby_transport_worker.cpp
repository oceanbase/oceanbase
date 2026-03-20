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

#define USING_LOG_PREFIX CLOG

#include "ob_log_standby_transport_worker.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_define.h"
#include "share/ob_thread_mgr.h"
#include "logservice/ob_log_service.h"
#include "logservice/palf/log_define.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"

namespace oceanbase
{
namespace logservice
{

ObLogStandbyTransportWorker::ObLogStandbyTransportWorker() :
  is_inited_(false),
  stop_flag_(false),
  tg_id_(-1),
  tenant_id_(OB_INVALID_TENANT_ID),
  ls_svr_(nullptr),
  log_service_(nullptr),
  cond_(ObCond::SPIN_WAIT_NUM, common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
  not_init_warn_time_us_(common::OB_INVALID_TIMESTAMP),
  running_info_print_time_us_(common::OB_INVALID_TIMESTAMP)
{}

ObLogStandbyTransportWorker::~ObLogStandbyTransportWorker()
{
  destroy();
}

int ObLogStandbyTransportWorker::init(const uint64_t tenant_id,
                                       storage::ObLSService *ls_svr,
                                       ObLogService *log_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "ObLogStandbyTransportWorker has been initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_svr)
      || OB_ISNULL(log_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(tenant_id), KP(ls_svr), KP(log_service));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StandbyTransportWorker, tg_id_))) {
    CLOG_LOG(WARN, "ObLogStandbyTransportWorker TG_CREATE failed", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    ls_svr_ = ls_svr;
    log_service_ = log_service;
    ATOMIC_STORE(&stop_flag_, false);
    not_init_warn_time_us_ = 0;
    running_info_print_time_us_ = 0;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogStandbyTransportWorker init success", K(tenant_id), K(tg_id_));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObLogStandbyTransportWorker::destroy()
{
  stop();
  wait();
  if (is_inited_) {
    if (-1 != tg_id_) {
      TG_DESTROY(tg_id_);
      tg_id_ = -1;
    }
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_svr_ = nullptr;
    log_service_ = nullptr;
    not_init_warn_time_us_ = common::OB_INVALID_TIMESTAMP;
    running_info_print_time_us_ = common::OB_INVALID_TIMESTAMP;
    is_inited_ = false;
  }
  CLOG_LOG(INFO, "ObLogStandbyTransportWorker destroy success");
}

int ObLogStandbyTransportWorker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogStandbyTransportWorker not init", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    CLOG_LOG(WARN, "ObLogStandbyTransportWorker TG_SET_RUNNABLE_AND_START failed", K(ret), K(tg_id_));
  } else {
    ATOMIC_STORE(&stop_flag_, false);
    CLOG_LOG(INFO, "ObLogStandbyTransportWorker start succ", K_(tenant_id), K_(tg_id));
  }
  return ret;
}

void ObLogStandbyTransportWorker::stop()
{
  CLOG_LOG(INFO, "ObLogStandbyTransportWorker thread stop", K_(tenant_id), K_(tg_id));
  ATOMIC_STORE(&stop_flag_, true);
  cond_.signal();
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
  }
}

void ObLogStandbyTransportWorker::wait()
{
  CLOG_LOG(INFO, "ObLogStandbyTransportWorker thread wait", K_(tenant_id), K_(tg_id));
  if (-1 != tg_id_) {
    TG_WAIT(tg_id_);
  }
}

void ObLogStandbyTransportWorker::signal()
{
  cond_.signal();
}

int ObLogStandbyTransportWorker::submit_transport_task(const ObLogTransportReq &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogStandbyTransportWorker not init", K(ret));
  } else if (ATOMIC_LOAD(&stop_flag_)) {
    ret = OB_IN_STOP_STATE;
    CLOG_LOG(WARN, "ObLogStandbyTransportWorker is stopping, reject new task", K(ret), K(req.ls_id_));
  } else if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid request", K(ret), K(req));
  } else {
    // 获取对应日志流的 restore_handler，将任务提交到其队列
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    ObLogRestoreHandler *restore_handler = nullptr;

    if (OB_FAIL(ls_svr_->get_ls(req.ls_id_, ls_handle, ObLSGetMod::LOG_MOD))) {
      CLOG_LOG(WARN, "get ls failed", K(ret), K(req.ls_id_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ls is null", K(ret), K(req.ls_id_));
    } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "log restore handler is null", K(ret), K(req.ls_id_));
    } else if (OB_FAIL(restore_handler->submit_transport_task(req))) {
      if (OB_SIZE_OVERFLOW == ret) {
        CLOG_LOG(WARN, "transport task queue is full", K(ret), K(req.ls_id_));
      } else {
        CLOG_LOG(WARN, "submit transport task to restore handler failed", K(ret), K(req.ls_id_));
      }
    } else {
      signal();  // 通知 worker 线程处理
      CLOG_LOG(TRACE, "submit_transport_task succ", K(req.ls_id_), K(req.start_lsn_), K(req.end_lsn_));
    }
  }
  return ret;
}

void ObLogStandbyTransportWorker::run1()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "ObLogStandbyTransportWorker thread start", K_(tg_id));
  lib::set_thread_name("StandbyTpWorker");

  if (OB_UNLIKELY(!is_inited_)) {
    CLOG_LOG(ERROR, "ObLogStandbyTransportWorker not init");
  } else {
    while (!has_set_stop() && !ATOMIC_LOAD(&stop_flag_)) {
      int64_t begin_tstamp = ObTimeUtility::current_time();
      do_thread_task_();
      int64_t end_tstamp = ObTimeUtility::current_time();
      int64_t wait_interval = THREAD_RUN_INTERVAL - (end_tstamp - begin_tstamp);
      if (wait_interval > 0) {
        common::ObBKGDSessInActiveGuard inactive_guard;
        cond_.timedwait(wait_interval);
      }
    }
  }
  CLOG_LOG(INFO, "ObLogStandbyTransportWorker thread end", K_(tg_id));
}

void ObLogStandbyTransportWorker::do_thread_task_()
{
  int ret = OB_SUCCESS;
  // 迭代所有日志流，处理每个日志流自己的 transport task 队列
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> iter_guard;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogStandbyTransportWorker not init", K(ret));
  } else if (OB_ISNULL(ls_svr_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "ls_svr_ is NULL", K(ret));
  } else if (OB_FAIL(ls_svr_->get_ls_iter(iter_guard, ObLSGetMod::LOG_MOD))) {
    CLOG_LOG(WARN, "get ls iter failed", K(ret));
  } else if (OB_ISNULL(iter = iter_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "iter is NULL", K(ret));
  } else {
    ObLS *ls = nullptr;
    while (OB_SUCC(ret) && !has_set_stop() && !ATOMIC_LOAD(&stop_flag_)) {
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          CLOG_LOG(WARN, "iter get next ls failed", K(ret));
          break;
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "ls is NULL", K(ret));
        break;
      } else {
        ObLogRestoreHandler *restore_handler = ls->get_log_restore_handler();
        if (OB_ISNULL(restore_handler)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "restore handler is null", K(ret), K(ls->get_ls_id()));
        } else {
          // 处理该日志流的 transport tasks
          int tmp_ret = restore_handler->process_transport_tasks(BATCH_SIZE);
          if (OB_NOT_INIT == tmp_ret) {
            if (palf::palf_reach_time_interval(1 * 1000 * 1000L, not_init_warn_time_us_)) {
              CLOG_LOG(WARN, "process transport tasks not init", K(tmp_ret), "ls_id", ls->get_ls_id());
            }
          } else if (OB_SUCCESS != tmp_ret && OB_NOT_MASTER != tmp_ret) {
            CLOG_LOG(WARN, "process transport tasks failed", K(tmp_ret), "ls_id", ls->get_ls_id());
          }
        }
      }
    }

    if (palf::palf_reach_time_interval(10 * 1000 * 1000L, running_info_print_time_us_)) {
      CLOG_LOG(INFO, "ObLogStandbyTransportWorker is running", "stop_flag", ATOMIC_LOAD(&stop_flag_), K_(tg_id));
    }
  }
}

} // namespace logservice
} // namespace oceanbase
