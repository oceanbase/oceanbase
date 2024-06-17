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

#define USING_LOG_PREFIX STORAGE
#include "ob_storage_ha_service.h"
#include "storage/tx_storage/ob_ls_handle.h"


namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(EN_STORAGE_HA_SERVICE_SET_LS_MIGRATION_STATUS_HOLD);
ObStorageHAService::ObStorageHAService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    ls_service_(nullptr)
{
}

ObStorageHAService::~ObStorageHAService()
{
}

int ObStorageHAService::mtl_init(ObStorageHAService *&ha_service)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  ha_service->ls_id_array_.set_attr(ObMemAttr(MTL_ID(), "ls_id"));
  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ha_service->init(ls_service))) {
    LOG_WARN("failed to init ha service", K(ret), KP(ls_service));
  }
  return ret;
}

int ObStorageHAService::init(
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha service is aleady init", K(ret));
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

void ObStorageHAService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

void ObStorageHAService::destroy()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObStorageHAService starts to destroy");
    thread_cond_.destroy();
    wakeup_cnt_ = 0;
    is_inited_ = false;
    COMMON_LOG(INFO, "ObStorageHAService destroyed");
  }
}

void ObStorageHAService::stop()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObStorageHAService starts to stop");
    ThreadPool::stop();
    wakeup();
    COMMON_LOG(INFO, "ObStorageHAService stopped");
  }
}

void ObStorageHAService::wait()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObStorageHAService starts to wait");
    ThreadPool::wait();
    COMMON_LOG(INFO, "ObStorageHAService finish to wait");
  }
}

int ObStorageHAService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else {
    if (OB_FAIL(lib::ThreadPool::start())) {
      COMMON_LOG(WARN, "ObStorageHAService start thread failed", K(ret));
    } else {
      COMMON_LOG(INFO, "ObStorageHAService start");
    }
  }
  return ret;
}

void ObStorageHAService::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("HAService");

  while (!has_set_stop()) {
    ls_id_array_.reset();

    if (OB_FAIL(get_ls_id_array_())) {
      LOG_WARN("failed to get ls id array", K(ret));
    } else if (OB_FAIL(scheduler_ls_ha_handler_())) {
      LOG_WARN("failed to do scheduler ls ha handler", K(ret));
    }

#ifdef ERRSIM
    if (FAILEDx(errsim_set_ls_migration_status_hold_())) {
      LOG_WARN("failed to errsim set ls migration status hold", K(ret));
    }
#endif

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      thread_cond_.wait(SCHEDULER_WAIT_TIME_MS);
    }
  }
}

int ObStorageHAService::get_ls_id_array_()
{
  int ret = OB_SUCCESS;
  ls_id_array_.reset();
  common::ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *ls_iter = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
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
        LOG_WARN("log stream should not be NULL", K(ret), KP(ls));
      } else if (OB_FAIL(ls_id_array_.push_back(ls->get_ls_id()))) {
        LOG_WARN("failed to push ls id into array", K(ret), KPC(ls));
      }
    }
  }
  return ret;
}

int ObStorageHAService::scheduler_ls_ha_handler_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else {
    std::random_shuffle(ls_id_array_.begin(), ls_id_array_.end());
    LOG_INFO("start do ls ha handler", K(ls_id_array_));

    for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array_.count(); ++i) {
      const share::ObLSID &ls_id = ls_id_array_.at(i);
      if (OB_SUCCESS != (tmp_ret = do_ha_handler_(ls_id))) {
        //The purpose of using tmp_ret here is to not block the scheduling of other ls afterward
        LOG_WARN("failed to do ha handler", K(tmp_ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObStorageHAService::do_ha_handler_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do ha handler get invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else {
    if (OB_SUCCESS != (tmp_ret = ls->get_ls_migration_handler()->process())) {
      LOG_WARN("failed to do ls migration handler process", K(tmp_ret), K(ls_id));
    }

    if (OB_SUCCESS != (tmp_ret = ls->get_ls_restore_handler()->process())) {
      LOG_WARN("failed to do ls restore handler process", K(tmp_ret), K(ls_id));
    }

    //ls->tablets transfer
  }
  return ret;
}

#ifdef ERRSIM
int ObStorageHAService::errsim_set_ls_migration_status_hold_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_HOLD;
  const bool write_slog = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha service do not init", K(ret));
  } else {
    ret = EN_STORAGE_HA_SERVICE_SET_LS_MIGRATION_STATUS_HOLD ? : OB_SUCCESS;
    const ObAddr &self = GCONF.self_addr_;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_STORAGE_HA_SERVICE_SET_LS_MIGRATION_STATUS_HOLD", K(ret));
      //overwrite ret
      ret = OB_SUCCESS;
      const ObString &errsim_server = GCONF.errsim_migration_src_server_addr.str();
      if (!errsim_server.empty()) {
        common::ObAddr tmp_errsim_addr;
        if (OB_FAIL(tmp_errsim_addr.parse_from_string(errsim_server))) {
          LOG_WARN("failed to parse from string", K(ret), K(errsim_server));
        } else if (self != tmp_errsim_addr) {
          //do nothing
        } else {
          const int64_t errsim_migration_ls_id = GCONF.errsim_migration_ls_id;
          const ObLSID ls_id(errsim_migration_ls_id);
          if (errsim_migration_ls_id <= 0 || !ls_id.is_valid()) {
            //do nothing
          } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
            LOG_WARN("failed to get ls", K(ret), K(ls_id));
          } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
          } else if (OB_FAIL(ls->set_migration_status(migration_status, ls->get_rebuild_seq(), write_slog))) {
            LOG_WARN("failed to set migration status", K(ret), KPC(ls), K(ls_id));
          }
        }
      }

    }
  }
  return ret;
}
#endif

}
}
