/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_transfer_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "observer/ob_server_struct.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"

namespace oceanbase
{
namespace storage
{

ObTransferService::ObTransferService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    ls_service_(nullptr)
{
}

ObTransferService::~ObTransferService()
{
}

int ObTransferService::mtl_init(ObTransferService *&transfer_service)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(transfer_service->init(ls_service))) {
    LOG_WARN("failed to init transfer service", K(ret), KP(ls_service));
  }
  return ret;
}

int ObTransferService::init(
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer service is aleady init", K(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init high avaiable handler mgr get invalid argument", K(ret), KP(ls_service));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init ha service thread cond", K(ret));
  } else {
    lib::ThreadPool::set_run_wrapper(MTL_CTX());
    ls_service_ = ls_service;
    is_inited_ = true;
  }
  return ret;
}

void ObTransferService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

void ObTransferService::destroy()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTransferService starts to destroy");
    thread_cond_.destroy();
    wakeup_cnt_ = 0;
    is_inited_ = false;
    COMMON_LOG(INFO, "ObTransferService destroyed");
  }
}

void ObTransferService::stop()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTransferService starts to stop");
    ThreadPool::stop();
    wakeup();
    COMMON_LOG(INFO, "ObTransferService stopped");
  }
}

void ObTransferService::wait()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTransferService starts to wait");
    ThreadPool::wait();
    COMMON_LOG(INFO, "ObTransferService finish to wait");
  }
}

int ObTransferService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer service do not init", K(ret));
  } else {
    if (OB_FAIL(lib::ThreadPool::start())) {
      COMMON_LOG(WARN, "ObTransferService start thread failed", K(ret));
    } else {
      COMMON_LOG(INFO, "ObTransferService start");
    }
  }
  return ret;
}

void ObTransferService::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("TransferService");
#ifdef ERRSIM
  ObErrsimModuleType module_type(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
  THIS_WORKER.set_module_type(module_type);
#endif

  while (!has_set_stop()) {
    ls_id_array_.reset();
    if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
      ret = OB_SERVER_IS_INIT;
      LOG_WARN("server is not serving", K(ret), K(GCTX.status_));
    } else if (OB_FAIL(get_ls_id_array_())) {
      LOG_WARN("failed to get ls id array", K(ret));
    } else if (OB_FAIL(scheduler_transfer_handler_())) {
      LOG_WARN("failed to do scheduler transfer handler", K(ret));
    }

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      int64_t wait_time_ms = 10_s;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        wait_time_ms = tenant_config->_transfer_service_wakeup_interval / 1000;
      }
      thread_cond_.wait(wait_time_ms);
    }
  }
}

int ObTransferService::get_ls_id_array_()
{
  int ret = OB_SUCCESS;
  ls_id_array_.reset();
  common::ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *ls_iter = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer service do not init", K(ret));
  } else if (OB_FAIL(ls_service_->get_ls_iter(ls_iter_guard, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else if (OB_ISNULL(ls_iter = ls_iter_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls iter should not be NULL", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObLS *ls = nullptr;
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", K(ret), KP(ls));
      } else if (!(ls->get_log_handler()->is_replay_enabled())) {
        LOG_INFO("log handler not enable replay, should not schduler transfer hander", "ls_id", ls->get_ls_id());
      } else if (OB_FAIL(ls_id_array_.push_back(ls->get_ls_id()))) {
        LOG_WARN("failed to push ls id into array", K(ret), KPC(ls));
      }
    }
  }
  return ret;
}

int ObTransferService::scheduler_transfer_handler_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer service do not init", K(ret));
  } else {
    LOG_INFO("start do transfer handler", K(ls_id_array_));

    for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array_.count(); ++i) {
      const share::ObLSID &ls_id = ls_id_array_.at(i);
      if (OB_SUCCESS != (tmp_ret = do_transfer_handler_(ls_id))) {
        //The purpose of using tmp_ret here is to not block the scheduling of other ls afterward
        LOG_WARN("failed to do ha handler", K(tmp_ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObTransferService::do_transfer_handler_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer service do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do ha handler get invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else if (ls->is_offline()) {
    LOG_INFO("ls is during offline, cannot schedule transfer handler", K(ls_id));
  } else if (OB_FAIL(ls->get_transfer_handler()->process())) {
    LOG_WARN("failed to process transfer", K(ret), KP(ls));
  } else {
    LOG_INFO("[TRANSFER] transfer handler process", K(ret), K(ls_id));
  }
  return ret;
}

}
}
