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
#include "ob_rebuild_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ls/ob_ls_table_operator.h"
#include "lib/utility/ob_tracepoint.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "observer/ob_server.h"
#include "logservice/ob_log_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "ob_storage_ha_utils.h"

using namespace oceanbase;
using namespace share;
using namespace storage;

ERRSIM_POINT_DEF(CHECK_CAN_REBUILD);

ObLSRebuildCtx::ObLSRebuildCtx()
  : ls_id_(),
    type_(),
    task_id_()
{
}

void ObLSRebuildCtx::reset()
{
  ls_id_.reset();
  type_ = ObLSRebuildType::MAX;
  task_id_.reset();
}

bool ObLSRebuildCtx::is_valid() const
{
  return ls_id_.is_valid()
      && type_.is_valid()
      && !task_id_.is_invalid();
}

int ObLSRebuildInfoHelper::get_next_rebuild_info(
    const ObLSRebuildInfo &curr_info,
    const ObLSRebuildType &rebuild_type,
    const int32_t result,
    const ObMigrationStatus &status,
    ObLSRebuildInfo &next_info)
{
  int ret = OB_SUCCESS;
  next_info.reset();

  if (!curr_info.is_valid() || !rebuild_type.is_valid() || !ObMigrationStatusHelper::is_valid(status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get next change status get invalid argument", K(ret), K(curr_info), K(rebuild_type), K(status));
  } else {
    switch (curr_info.status_) {
    case ObLSRebuildStatus::NONE: {
      if (OB_SUCCESS == result) {
        next_info.status_ = ObLSRebuildStatus::INIT;
        next_info.type_ = rebuild_type;
      } else {
        next_info = curr_info;
      }
      break;
    }
    case ObLSRebuildStatus::INIT: {
      if (OB_SUCCESS == result) {
        next_info.status_ = ObLSRebuildStatus::DOING;
        next_info.type_ = rebuild_type;
      } else {
        next_info.status_ = ObLSRebuildStatus::CLEANUP;
        next_info.type_ = rebuild_type;
      }
      break;
    }
    case ObLSRebuildStatus::DOING: {
      if (OB_SUCCESS == result
          || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL == status) {
        next_info.status_ = ObLSRebuildStatus::CLEANUP;
        next_info.type_ = rebuild_type;
      } else {
        next_info.status_ = ObLSRebuildStatus::DOING;
        next_info.type_ = rebuild_type;
      }
     break;
    }
    case ObLSRebuildStatus::CLEANUP: {
      if (OB_SUCCESS == result) {
        next_info.status_ = ObLSRebuildStatus::NONE;
        next_info.type_ = ObLSRebuildType::NONE;
      } else {
        next_info.status_ = ObLSRebuildStatus::CLEANUP;
        next_info.type_ = rebuild_type;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid curr info for fail", K(ret), K(curr_info));
    }
    }
  }
  return ret;
}

int ObLSRebuildInfoHelper::check_can_change_info(
    const ObLSRebuildInfo &old_info,
    const ObLSRebuildInfo &new_info,
    bool &can_change)
{
  int ret = OB_SUCCESS;
  can_change = false;
  if (!old_info.is_valid() || !new_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can change status get invalid argument", K(ret), K(old_info), K(new_info));
  }else {
    switch (old_info.status_) {
    case ObLSRebuildStatus::NONE: {
      if (ObLSRebuildStatus::NONE == new_info.status_
          || ObLSRebuildStatus::INIT == new_info.status_) {
        can_change = true;
      }
      break;
    }
    case ObLSRebuildStatus::INIT: {
      if (ObLSRebuildStatus::INIT == new_info.status_
          || ObLSRebuildStatus::DOING == new_info.status_
          || ObLSRebuildStatus::CLEANUP == new_info.status_) {
        can_change = true;
      }
      break;
    }
    case ObLSRebuildStatus::DOING: {
      if (ObLSRebuildStatus::DOING == new_info.status_
          || ObLSRebuildStatus::CLEANUP == new_info.status_) {
        can_change = true;
      }
      break;
    }
    case ObLSRebuildStatus::CLEANUP : {
      if (ObLSRebuildStatus::CLEANUP == new_info.status_
          || ObLSRebuildStatus::NONE == new_info.status_) {
        can_change = true;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid old info for fail", K(ret), K(old_info), K(new_info));
    }
    }
  }
  return ret;
}


ObRebuildService::ObRebuildService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    ls_service_(nullptr),
    map_lock_(),
    rebuild_ctx_map_()
{
}

ObRebuildService::~ObRebuildService()
{
}

int ObRebuildService::mtl_init(ObRebuildService *&rebuild_service)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(rebuild_service->init(ls_service))) {
    LOG_WARN("failed to init rebuild service", K(ret), KP(ls_service));
  }
  return ret;
}

int ObRebuildService::init(
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(map_lock_);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rebuild service is aleady init", K(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init rebuild service get invalid argument", K(ret), KP(ls_service));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init ha service thread cond", K(ret));
  } else if (OB_FAIL(rebuild_ctx_map_.create(MAX_BUCKET_NUM, "RebuildCtx"))) {
    LOG_WARN("failed to create rebuild ctx map", K(ret));
  } else {
    lib::ThreadPool::set_run_wrapper(MTL_CTX());
    ls_service_ = ls_service;
    is_inited_ = true;
  }
  return ret;
}

int ObRebuildService::add_rebuild_ls(
    const ObLSID &ls_id,
    const ObLSRebuildType &rebuild_type)
{
  int ret = OB_SUCCESS;
  ObLSRebuildCtx rebuild_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (!ls_id.is_valid() || !rebuild_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add rebuild ls get invalid argument", K(ret), K(ls_id), K(rebuild_type));
  } else {
    common::SpinWLockGuard guard(map_lock_);
    int hash_ret = rebuild_ctx_map_.get_refactored(ls_id, rebuild_ctx);
    if (OB_SUCCESS == hash_ret) {
      wakeup();
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      rebuild_ctx.ls_id_ = ls_id;
      rebuild_ctx.type_ = rebuild_type;
      rebuild_ctx.task_id_.init(GCONF.self_addr_);
      if (OB_FAIL(rebuild_ctx_map_.set_refactored(ls_id, rebuild_ctx))) {
        LOG_WARN("failed to set rebuild ctx", K(ret), K(rebuild_ctx));
      } else {
        wakeup();
        FLOG_INFO("succeed add rebuild ls", K(ls_id), K(rebuild_ctx));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("failed to add rebuild ls", K(ret), K(ls_id), K(rebuild_type));
    }
  }
  return ret;
}

int ObRebuildService::inner_remove_rebuild_ls_(
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(map_lock_);
  if (OB_FAIL(rebuild_ctx_map_.erase_refactored(ls_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase rebuild ls", K(ret), K(ls_id));
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("succeed remove rebuild ls", K(ls_id));
  }
  return ret;
}

int ObRebuildService::finish_rebuild_ls(
    const share::ObLSID &ls_id,
    const int32_t result)
{
  int ret = OB_SUCCESS;
  ObLSRebuildMgr ls_rebuild_mgr;
  ObLSRebuildCtx rebuild_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove rebuild ls get invalid argument", K(ret), K(ls_id));
  } else {
    {
      common::SpinRLockGuard guard(map_lock_);
      if (OB_FAIL(rebuild_ctx_map_.get_refactored(ls_id, rebuild_ctx))) {
        LOG_WARN("failed to get rebuild ctx", K(ret), K(ls_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls_rebuild_mgr.init(rebuild_ctx, ls_service_))) {
      LOG_WARN("failed to init ls rebuild mgr", K(ret), K(ls_id), K(rebuild_ctx));
    } else if (OB_FAIL(ls_rebuild_mgr.finish_ls_rebuild(result))) {
      LOG_WARN("failed to finish ls rebuild", K(ret), K(ls_id), K(result), K(rebuild_ctx));
    }
  }
  return ret;
}

int ObRebuildService::check_ls_need_rebuild(
    const share::ObLSID &ls_id,
    bool &need_rebuild)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObLSRebuildCtx rebuild_ctx;
  need_rebuild = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove rebuild ls get invalid argument", K(ret), K(ls_id));
  } else {
    common::SpinRLockGuard guard(map_lock_);
    if (OB_FAIL(rebuild_ctx_map_.get_refactored(ls_id, rebuild_ctx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        need_rebuild = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get rebuild ctx", K(ret), K(ls_id));
      }
    } else {
      need_rebuild = true;
    }
  }
  return ret;
}

int ObRebuildService::remove_rebuild_ls(
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove rebuild ls get invalid argument", K(ret), K(ls_id));
  } else {
    common::SpinWLockGuard guard(map_lock_);
    if (OB_FAIL(rebuild_ctx_map_.erase_refactored(ls_id))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to erase rebuild ls", K(ret), K(ls_id));
      }
    }
  }
  return ret;
}

void ObRebuildService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

void ObRebuildService::destroy()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObRebuildService starts to destroy");
    thread_cond_.destroy();
    wakeup_cnt_ = 0;
    rebuild_ctx_map_.destroy();
    is_inited_ = false;
    COMMON_LOG(INFO, "ObRebuildService destroyed");
  }
}

void ObRebuildService::stop()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObRebuildService starts to stop");
    ThreadPool::stop();
    wakeup();
    COMMON_LOG(INFO, "ObRebuildService stopped");
  }
}

void ObRebuildService::wait()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObRebuildService starts to wait");
    ThreadPool::wait();
    COMMON_LOG(INFO, "ObRebuildService finish to wait");
  }
}

int ObRebuildService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else {
    if (OB_FAIL(lib::ThreadPool::start())) {
      COMMON_LOG(WARN, "ObRebuildService start thread failed", K(ret));
    } else {
      COMMON_LOG(INFO, "ObRebuildService start");
    }
  }
  return ret;
}

void ObRebuildService::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("RebuildService");

  while (!has_set_stop()) {
    if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
      ret = OB_SERVER_IS_INIT;
      LOG_WARN("server is not serving", K(ret), K(GCTX.status_));
    } else if (OB_FAIL(build_rebuild_ctx_map_())) {
      LOG_WARN("failed to build rebuild ctx map", K(ret));
    } else if (OB_FAIL(build_ls_rebuild_info_())) {
      LOG_WARN("failed to build ls rebuild info", K(ret));
    } else if (OB_FAIL(scheduler_rebuild_mgr_())) {
      LOG_WARN("failed to do scheduler rebuild mgr", K(ret));
    } else if (OB_FAIL(check_rebuild_ctx_map_())) {
      LOG_WARN("failed to check rebuild ctx map", K(ret));
    }

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      int64_t wait_time_ms = SCHEDULER_WAIT_TIME_MS;
      if (OB_SERVER_IS_INIT == ret) {
        wait_time_ms = WAIT_SERVER_IN_SERVICE_TIME_MS;
      }
      thread_cond_.wait(wait_time_ms);
    }
  }
}

int ObRebuildService::build_rebuild_ctx_map_()
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *ls_iter = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (OB_FAIL(ls_service_->get_ls_iter(ls_iter_guard, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else if (OB_ISNULL(ls_iter = ls_iter_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls iter should not be NULL", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObLS *ls = nullptr;
      ObLSRebuildInfo rebuild_info;
      ObLSRebuildCtx rebuild_ctx;
      int hash_ret = OB_SUCCESS;
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", K(ret), KP(ls));
      } else if (OB_FAIL(ls->get_rebuild_info(rebuild_info))) {
        LOG_WARN("failed to get rebuild info", K(ret), KPC(ls));
      } else if (!rebuild_info.is_in_rebuild()) {
        //do nothing
      } else {
        common::SpinWLockGuard guard(map_lock_);
        if (FALSE_IT(hash_ret = rebuild_ctx_map_.get_refactored(ls->get_ls_id(), rebuild_ctx))) {
        } else if (OB_SUCCESS == hash_ret) {
          //do nothing
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          rebuild_ctx.ls_id_ = ls->get_ls_id();
          rebuild_ctx.type_ = rebuild_info.type_;
          rebuild_ctx.task_id_.init(GCONF.self_addr_);
          if (OB_FAIL(rebuild_ctx_map_.set_refactored(ls->get_ls_id(), rebuild_ctx))) {
            LOG_WARN("failed to set rebuild ctx", K(ret), KPC(ls), K(rebuild_info));
          }
        } else {
          ret = hash_ret;
          LOG_WARN("failed to get ls rebuild ctx", K(ret), KPC(ls));
        }
      }
    }
  }
  return ret;
}

int ObRebuildService::scheduler_rebuild_mgr_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObLSRebuildCtx> rebuild_ctx_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (OB_FAIL(get_ls_rebuild_ctx_array_(rebuild_ctx_array))) {
    LOG_WARN("failed to get ls rebuild ctx array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rebuild_ctx_array.count(); ++i) {
      const ObLSRebuildCtx &rebuild_ctx = rebuild_ctx_array.at(i);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObLSRebuildInfo rebuild_info;
      if (OB_FAIL(ls_service_->get_ls(rebuild_ctx.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
        if (OB_LS_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get ls", K(ret), K(rebuild_ctx));
        }
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", K(ret), K(rebuild_ctx));
      } else if (OB_FAIL(ls->get_rebuild_info(rebuild_info))) {
        LOG_WARN("failed to get rebuild info", K(ret), KPC(ls));
      } else if (!rebuild_info.is_in_rebuild()) {
        //do nothing
      } else if (OB_SUCCESS != (tmp_ret = do_rebuild_mgr_(rebuild_ctx))) {
        LOG_WARN("failed to do rebuild mgr", K(tmp_ret), K(rebuild_ctx));
      }
    }
  }
  return ret;
}

int ObRebuildService::do_rebuild_mgr_(const ObLSRebuildCtx &rebuild_ctx)
{
  int ret = OB_SUCCESS;
  ObLSRebuildMgr ls_rebuild_mgr;
  LOG_INFO("start do rebuild mgr", K(rebuild_ctx));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (!rebuild_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do rebuild mgr get invalid argument", K(ret), K(rebuild_ctx));
  } else if (OB_FAIL(ls_rebuild_mgr.init(rebuild_ctx, ls_service_))) {
    LOG_WARN("failed to ini ls rebuild mgr", K(ret), K(rebuild_ctx));
  } else if (OB_FAIL(ls_rebuild_mgr.process())) {
    LOG_WARN("failed to do ls rebuld mgr", K(ret), K(rebuild_ctx));
  }
  return ret;
}

int ObRebuildService::check_rebuild_ctx_map_()
{
  int ret = OB_SUCCESS;
  ObArray<ObLSID> ls_id_array;
  ObArray<ObLSRebuildCtx> rebuild_ctx_array;
  ObMigrationStatus status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (OB_FAIL(get_ls_rebuild_ctx_array_(rebuild_ctx_array))) {
    LOG_WARN("failed to get ls rebuild ctx array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rebuild_ctx_array.count(); ++i) {
      const ObLSRebuildCtx &rebuild_ctx = rebuild_ctx_array.at(i);
      const ObLSID &ls_id = rebuild_ctx.ls_id_;
      bool is_exist = false;
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObLSRebuildInfo rebuild_info;
      bool in_final_state = false;
      if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
        if (OB_LS_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(ls_id_array.push_back(ls_id))) {
            LOG_WARN("failed to push ls id into array", K(ret), K(ls_id));
          }
        } else {
          LOG_WARN("failed to get ls", K(ret), K(ls_id));
        }
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", K(ret), KP(ls));
      } else if (OB_FAIL(ls->get_rebuild_info(rebuild_info))) {
        LOG_WARN("failed to get ls rebuild info", K(ret), KPC(ls), K(ls_id));
      } else if (OB_FAIL(ls->get_migration_status(status))) {
        LOG_WARN("failed to get migration status", K(ret), KPC(ls), K(ls_id));
      } else if (OB_FAIL(ObMigrationStatusHelper::check_migration_in_final_state(status, in_final_state))) {
        LOG_WARN("failed to check migration status in final state", K(ret), K(status), KPC(ls));
      } else if (rebuild_info.is_in_rebuild() || !in_final_state) {
        //do nohting
      } else if (OB_FAIL(ls_id_array.push_back(ls_id))) {
        LOG_WARN("failed to push ls id into array", K(ret), K(ls_id));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array.count(); ++i) {
        const ObLSID &ls_id = ls_id_array.at(i);
        if (OB_FAIL(inner_remove_rebuild_ls_(ls_id))) {
          LOG_WARN("failed to inner remove rebuild ls", K(ret), K(ls_id));
        }
      }
    }
  }
  return ret;
}

int ObRebuildService::get_ls_rebuild_ctx_array_(
    common::ObIArray<ObLSRebuildCtx> &rebuild_ctx_array)
{
  int ret = OB_SUCCESS;
  rebuild_ctx_array.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(map_lock_);
    for (LSRebuildCtxMap::iterator iter = rebuild_ctx_map_.begin(); OB_SUCC(ret) && iter != rebuild_ctx_map_.end(); ++iter) {
      const ObLSRebuildCtx &rebuild_ctx = iter->second;
      if (OB_FAIL(rebuild_ctx_array.push_back(rebuild_ctx))) {
        LOG_WARN("failed to push rebuild ctx into array", K(ret), K(rebuild_ctx));
      }
    }
  }
  return ret;
}

int ObRebuildService::build_ls_rebuild_info_()
{
  int ret = OB_SUCCESS;
  ObArray<ObLSRebuildCtx> rebuild_ctx_array;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (OB_FAIL(get_ls_rebuild_ctx_array_(rebuild_ctx_array))) {
    LOG_WARN("failed to get ls rebuild ctx array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rebuild_ctx_array.count(); ++i) {
      const ObLSRebuildCtx &rebuild_ctx = rebuild_ctx_array.at(i);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObLSRebuildInfo rebuild_info;
      ObMigrationStatus status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
      bool can_rebuild = false;
      if (OB_FAIL(ls_service_->get_ls(rebuild_ctx.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
        if (OB_LS_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get ls", K(ret), K(rebuild_ctx));
        }
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(rebuild_ctx));
      } else if (OB_FAIL(ls->get_migration_status(status))) {
        LOG_WARN("failed to get ls migration status", K(ret), KPC(ls));
      } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != status
          && ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD != status) {
        FLOG_INFO("ls migration status is not none or rebuild, skip it", K(ret), KPC(ls));
      } else if (OB_FAIL(ls->get_rebuild_info(rebuild_info))) {
        LOG_WARN("failed to get ls rebuild info", K(ret), KPC(ls), K(rebuild_ctx));
      } else if (rebuild_info.is_in_rebuild()) {
        //do nothing
      } else if (OB_FAIL(check_can_rebuild_(rebuild_ctx, ls, can_rebuild))) {
        LOG_WARN("failed to check can rebuild", K(ret), KPC(ls));
      } else if (!can_rebuild) {
        LOG_INFO("ls cannot rebuild", K(ret), KPC(ls));
      } else {
        rebuild_info.status_ = ObLSRebuildStatus::INIT;
        rebuild_info.type_ = rebuild_ctx.type_;
        if (OB_FAIL(ls->set_rebuild_info(rebuild_info))) {
          LOG_WARN("failed to set rebuild info", K(ret), K(rebuild_info), K(rebuild_ctx), KPC(ls));
        }
      }
    }
  }
  return ret;
}

int ObRebuildService::check_can_rebuild_(
    const ObLSRebuildCtx &rebuild_ctx,
    ObLS *ls,
    bool &can_rebuild)
{
  int ret = OB_SUCCESS;
  can_rebuild = false;
  ObRole role;
  int64_t proposal_id = 0;
  const uint64_t tenant_id = MTL_ID();
  common::ObMemberList member_list;
  int64_t paxos_replica_num = 0;
  const ObAddr &self_addr = GCONF.self_addr_;
  bool is_primary_tenant = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild service do not init", K(ret));
  } else if (!rebuild_ctx.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls should not be NULL", K(ret), K(rebuild_ctx), KP(ls));
  } else if (OB_FAIL(ObStorageHAUtils::check_is_primary_tenant(tenant_id, is_primary_tenant))) {
    LOG_WARN("failed to check is primary tenant", K(ret), K(tenant_id));
  } else if (OB_FAIL(ls->get_log_handler()->get_paxos_member_list(member_list, paxos_replica_num))) {
    LOG_WARN("failed to get paxos member list and learner list", K(ret), KPC(ls));
  } else if (ObLSRebuildType::TRANSFER == rebuild_ctx.type_
      && is_primary_tenant
      && member_list.contains(self_addr)) {
    //primary will has this condition
    can_rebuild = false;
    LOG_INFO("ls cannot do rebuild", K(rebuild_ctx), K(is_primary_tenant), K(member_list));
  } else if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
    LOG_WARN("failed to get role", K(ret), KPC(ls));
  } else if (is_strong_leader(role)) {
    can_rebuild = false;
    FLOG_INFO("leader cannot rebuild", KPC(ls));
  } else {
    can_rebuild = true;
    if (ObLSRebuildType::CLOG == rebuild_ctx.type_
        && is_primary_tenant
        && member_list.contains(self_addr)) {
      LOG_ERROR("paxos member lost clog, need rebuild", "ls_id", ls->get_ls_id(), K(role));
    }
  }
  return ret;
}


ObLSRebuildMgr::ObLSRebuildMgr()
  : is_inited_(false),
    rebuild_ctx_()
{
}

ObLSRebuildMgr::~ObLSRebuildMgr()
{
}

int ObLSRebuildMgr::init(
    const ObLSRebuildCtx &rebuild_ctx,
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls rebuild mgr init twice", K(ret));
  } else if (!rebuild_ctx.is_valid() || OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ls rebuild mgr get invalid argument", K(ret), K(rebuild_ctx), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(rebuild_ctx.ls_id_, ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(rebuild_ctx));
  } else {
    rebuild_ctx_ = rebuild_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLSRebuildMgr::process()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSRebuildInfo rebuild_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild mgr do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(rebuild_ctx_));
  } else if (OB_FAIL(ls->get_rebuild_info(rebuild_info))) {
    LOG_WARN("failed to get rebuild info", K(ret), K(rebuild_info));
  } else {
    switch (rebuild_info.status_) {
    case ObLSRebuildStatus::NONE: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in ls rebuld mgr rebuild info should not be NULL", K(ret), K(rebuild_info));
      break;
    }
    case ObLSRebuildStatus::INIT : {
      if (OB_FAIL(do_with_init_status_(rebuild_info))) {
        LOG_WARN("failed to do with init status", K(ret), K(rebuild_info));
      }
      break;
    }
    case ObLSRebuildStatus::DOING : {
      if (OB_FAIL(do_with_doing_status_(rebuild_info))) {
        LOG_WARN("failed to do with aborted status", K(ret), K(rebuild_info));
      }
      break;
    }
    case ObLSRebuildStatus::CLEANUP : {
      if (OB_FAIL(do_with_cleanup_status_(rebuild_info))) {
        LOG_WARN("failed to do with cleanup status", K(ret), K(rebuild_info));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(rebuild_info));
    }
    }
  }
  return ret;
}

int ObLSRebuildMgr::finish_ls_rebuild(
    const int32_t result)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSRebuildInfo rebuild_info;
  int32_t tmp_result = result;
  const uint64_t tenant_id = MTL_ID();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild mgr do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_rebuild_info(rebuild_info))) {
    LOG_WARN("failed to get ls rebuild info", K(ret), KPC(ls));
  } else if (ObLSRebuildStatus::DOING != rebuild_info.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls rebuild info is unexpected", K(ret), KPC(ls), K(rebuild_info));
  } else if (OB_FAIL(switch_next_status_(rebuild_info, tmp_result))) {
    LOG_WARN("failed to switch next status", K(ret), K(rebuild_info), KPC(ls));
  }
  return ret;
}

int ObLSRebuildMgr::do_with_init_status_(const ObLSRebuildInfo &rebuild_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  const uint64_t tenant_id = MTL_ID();
  const bool need_check_log_missing = ObLSRebuildType::CLOG == rebuild_info.type_ ? true : false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild mgr do not init", K(ret));
  } else {
    if (!rebuild_info.is_valid() || ObLSRebuildStatus::INIT != rebuild_info.status_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("do with none status get invalid argument", K(ret), K(rebuild_info));
    } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(rebuild_info));
    } else {
      ROOTSERVICE_EVENT_ADD("disaster_recovery", "start_rebuild_ls_replica",
                            "tenant_id", tenant_id,
                            "ls_id", rebuild_ctx_.ls_id_,
                            "task_id", rebuild_ctx_.task_id_,
                            "source", MYADDR,
                            "destination", MYADDR,
                            "comment", "");
      DEBUG_SYNC(BEFORE_MIGRATION_DISABLE_VOTE);

      if (OB_FAIL(ls->disable_vote(need_check_log_missing))) {
        LOG_WARN("failed to disable vote", K(ret), KPC(ls), K(rebuild_info));
      }

      if (OB_FAIL(ret)) {
        SERVER_EVENT_ADD("storage_ha", "rebuild_disable_vote_failed",
                         "tenant_id", tenant_id,
                         "ls_id",  rebuild_ctx_.ls_id_.id(),
                         "task_id", rebuild_ctx_.task_id_,
                         "destination", MYADDR,
                         "type", rebuild_info.type_,
                         "result", ret,
                         "REBUILD_LS_OP");
      }
    }

    if (OB_SUCCESS != (tmp_ret = switch_next_status_(rebuild_info, ret))) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      LOG_WARN("failed to switch next status", K(ret), K(tmp_ret), K(rebuild_info));
      if (OB_SUCCESS != (tmp_ret = ls->enable_vote())) {
        LOG_ERROR("failed to enable vote", K(tmp_ret), K(ret), K(rebuild_info), K(rebuild_ctx_));
      }
    }
  }
  return ret;
}

int ObLSRebuildMgr::do_with_doing_status_(const ObLSRebuildInfo &rebuild_info)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  bool is_exist = false;
  LOG_INFO("start do with doing status", K(rebuild_info), K(rebuild_ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild mgr do not init", K(ret), K(rebuild_info));
  } else if (!rebuild_info.is_valid() || ObLSRebuildStatus::DOING != rebuild_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do with none status get invalid argument", K(ret), K(rebuild_info));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(rebuild_ctx_), K(rebuild_info));
  } else if (OB_FAIL(ls->get_ls_migration_handler()->check_task_exist(rebuild_ctx_.task_id_, is_exist))) {
    LOG_WARN("failed to check task exist", K(ret), K(rebuild_ctx_));
  } else if (is_exist) {
    LOG_INFO("rebuild task is already exist", K(ret), K(rebuild_ctx_));
  } else if (OB_FAIL(generate_rebuild_task_())) {
    LOG_WARN("failed to generate rebuild task", K(ret), K(rebuild_ctx_));
  }
  return ret;
}

int ObLSRebuildMgr::do_with_cleanup_status_(
    const ObLSRebuildInfo &rebuild_info)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  int32_t tmp_result = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  LOG_INFO("start do with cleanup status", K(rebuild_info), K(rebuild_ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild mgr do not init", K(ret), K(rebuild_info));
  } else if (!rebuild_info.is_valid() || ObLSRebuildStatus::CLEANUP != rebuild_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do with none status get invalid argument", K(ret), K(rebuild_info));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(rebuild_ctx_), K(rebuild_info));
  } else {

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_MIGRATION_ENABLE_VOTE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      tmp_result = ret;
      ret = OB_SUCCESS;
      STORAGE_LOG(ERROR, "fake EN_MIGRATION_ENABLE_VOTE_FAILED", K(ret));
    }
  }
#endif

    if (OB_SUCCESS == tmp_result && OB_FAIL(ls->enable_vote())) {
      LOG_WARN("failed to enable vote", K(ret), KPC(ls), K(rebuild_info));
    } else {
      LOG_INFO("succeed enable vote", KPC(ls), K(rebuild_info));
    #ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_MIGRATION_ENABLE_VOTE_RETRY) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          tmp_result = ret;
          ret = OB_SUCCESS;
          STORAGE_LOG(ERROR, "fake EN_MIGRATION_ENABLE_VOTE_RETRY", K(ret));
        }
      }
    #endif
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(switch_next_status_(rebuild_info, tmp_result))) {
      LOG_WARN("failed to switch next status", K(ret), K(rebuild_info), KPC(ls));
    } else {
      ROOTSERVICE_EVENT_ADD("disaster_recovery", "finish_rebuild_ls_replica",
                            "tenant_id", tenant_id,
                            "ls_id", rebuild_ctx_.ls_id_,
                            "task_id", rebuild_ctx_.task_id_,
                            "source", MYADDR,
                            "destination", MYADDR,
                            "comment", tmp_result);
    }
  }
  return ret;
}

int ObLSRebuildMgr::switch_next_status_(
    const ObLSRebuildInfo &curr_rebuild_info,
    const int32_t result)
{
  int ret = OB_SUCCESS;
  ObLSRebuildInfo next_rebuild_info;
  bool can_change = false;
  ObRebuildService *rebuild_service = MTL(ObRebuildService*);
  ObLS *ls = nullptr;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild mgr do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(rebuild_ctx_));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(ls));
  } else if (OB_ISNULL(rebuild_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage ha handler service should not be NULL", K(ret), KP(rebuild_service));
  } else if (!curr_rebuild_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("switch next status get invalid argument", K(ret), K(curr_rebuild_info));
  } else {
    if (OB_FAIL(ObLSRebuildInfoHelper::get_next_rebuild_info(curr_rebuild_info, rebuild_ctx_.type_, result, migration_status, next_rebuild_info))) {
      LOG_WARN("failed to get next change status", K(ret), K(curr_rebuild_info), K(result), K(rebuild_ctx_), K(migration_status));
    } else if (OB_FAIL(ObLSRebuildInfoHelper::check_can_change_info(curr_rebuild_info, next_rebuild_info, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(curr_rebuild_info), K(next_rebuild_info), K(rebuild_ctx_));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change ls migration handler status", K(ret), K(curr_rebuild_info), K(next_rebuild_info), K(rebuild_ctx_));
    } else if (OB_FAIL(ls->set_rebuild_info(next_rebuild_info))) {
      LOG_WARN("failed to set rebuild info", K(ret), K(next_rebuild_info));
    } else {
      FLOG_INFO("update rebuild info", K(curr_rebuild_info), K(next_rebuild_info));
    }
    wakeup_();
  }
  return ret;
}

void ObLSRebuildMgr::wakeup_()
{
  int ret = OB_SUCCESS;
  ObRebuildService *rebuild_service = MTL(ObRebuildService*);
  if (OB_ISNULL(rebuild_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage ha handler service should not be NULL", K(ret), KP(rebuild_service));
  } else {
    rebuild_service->wakeup();
  }
}

int ObLSRebuildMgr::generate_rebuild_task_()
{
  int ret = OB_SUCCESS;
  const int64_t timestamp = 0;
  common::ObMemberList member_list;
  int64_t paxos_replica_num = 0;
  ObLSInfo ls_info;
  int64_t cluster_id = GCONF.cluster_id;
  uint64_t tenant_id = MTL_ID();
  ObAddr leader_addr;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild mgr do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(rebuild_ctx_));
  } else {
    if (OB_FAIL(ls->get_paxos_member_list(member_list, paxos_replica_num))) {
      LOG_WARN("failed to get paxos member list", K(ret), KPC(ls));
    } else if (OB_FAIL(get_ls_info_(cluster_id, tenant_id, ls->get_ls_id(), ls_info))) {
      LOG_WARN("failed to get ls info", K(ret), K(cluster_id), K(tenant_id), KPC(ls));
      //overwrite ret
      if (OB_FAIL(ls->get_log_handler()->get_election_leader(leader_addr))) {
        LOG_WARN("failed to get election leader", K(ret), KPC(ls), K(tenant_id));
      } else {
        paxos_replica_num = 1;
      }
    } else {
      //TODO(muwei.ym) do not use leader as src in 4.3
      const ObLSInfo::ReplicaArray &replica_array = ls_info.get_replicas();
      for (int64_t i = 0; OB_SUCC(ret) && i < replica_array.count(); ++i) {
        const ObLSReplica &replica = replica_array.at(i);
        if (replica.is_strong_leader()) {
          leader_addr = replica.get_server();
          break;
        }
      }
    }

  #ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_GENERATE_REBUILD_TASK_FAILED) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          STORAGE_LOG(ERROR, "fake EN_GENERATE_REBUILD_TASK_FAILED", K(ret));
        }
      }
  #endif

    if (OB_FAIL(ret)) {
    } else {
      ObTaskId task_id;
      task_id.init(GCONF.self_addr_);
      ObReplicaMember dst_replica_member(GCONF.self_addr_, timestamp);
      ObReplicaMember src_replica_member(leader_addr, timestamp);
      ObMigrationOpArg arg;
      arg.cluster_id_ = GCONF.cluster_id;
      arg.data_src_ = src_replica_member;
      arg.dst_ = dst_replica_member;
      arg.ls_id_ = ls->get_ls_id();
      arg.priority_ = ObMigrationOpPriority::PRIO_MID;
      arg.paxos_replica_number_ = paxos_replica_num;
      arg.src_ = src_replica_member;
      arg.type_ = ObMigrationOpType::REBUILD_LS_OP;

      if (OB_FAIL(ls->get_ls_migration_handler()->add_ls_migration_task(rebuild_ctx_.task_id_, arg))) {
        LOG_WARN("failed to add ls migration task", K(ret), K(arg), KPC(ls));
      }
    }
  }
  return ret;
}

int ObLSRebuildMgr::get_ls_info_(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  share::ObLSTableOperator *lst_operator = GCTX.lst_operator_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild mgr do not init", K(ret));
  } else if (cluster_id < 0 || OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ls info get invalid argument", K(ret), K(cluster_id), K(tenant_id), K(ls_id));
  } else if (nullptr == lst_operator) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst_operator ptr is null", K(ret));
  } else if (OB_FAIL(lst_operator->get(cluster_id, tenant_id,
      ls_id, share::ObLSTable::DEFAULT_MODE, ls_info))) {
    LOG_WARN("failed to get log stream info", K(ret), K(cluster_id), K(tenant_id), K(ls_id));
  }
  return ret;
}
