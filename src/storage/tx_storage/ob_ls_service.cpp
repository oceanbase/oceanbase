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

#include "lib/guard/ob_shared_guard.h"
#include "logservice/ob_garbage_collector.h"
#include "observer/ob_service.h"
#include "observer/ob_srv_network_frame.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "share/rc/ob_tenant_base.h"  // MTL_IS_RESTORE_TENANT
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_lock.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/tx_storage/ob_ls_safe_destroy_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
using namespace lib;
using namespace logservice;
namespace storage
{

static inline void prepare_palf_base_info(const obrpc::ObCreateLSArg &arg,
                                          palf::PalfBaseInfo &palf_base_info)
{
  palf_base_info.generate_by_default();
  palf_base_info.prev_log_info_.scn_ = arg.get_create_scn();
  if (arg.is_create_ls_with_palf()) {
    palf_base_info = arg.get_palf_base_info();
  }
}

ObLSService::ObLSService()
  : is_inited_(false),
    is_running_(false),
    tenant_id_(OB_INVALID_ID),
    ls_map_(),
    ls_allocator_(),
    iter_allocator_(),
    change_lock_(common::ObLatchIds::LS_CHANGE_LOCK),
    rs_reporter_(nullptr),
    storage_svr_rpc_proxy_(),
    storage_rpc_(),
    safe_ls_destroy_task_cnt_(0),
    iter_cnt_(0)
{}

ObLSService::~ObLSService()
{
  destroy();
}

void ObLSService::destroy()
{
  int ret = OB_SUCCESS;
  LOG_INFO("destroy ls service", K_(iter_cnt));
  if (is_running_) {
    if (OB_FAIL(stop())) {
      LOG_WARN("stop ls service failed", K(ret));
    }
  }
  if (IS_INIT) {
    tenant_id_ = OB_INVALID_ID;
    ls_map_.reset();
    ls_allocator_.destroy();
    iter_allocator_.destroy();
    rs_reporter_ = nullptr;
    storage_svr_rpc_proxy_.destroy();
    storage_rpc_.destroy();
  }
  is_inited_ = false;
}

bool ObLSService::safe_to_destroy()
{
  bool is_safe = (ls_map_.is_empty() &&
                  ATOMIC_LOAD(&safe_ls_destroy_task_cnt_) == 0 &&
                  ATOMIC_LOAD(&iter_cnt_) == 0);
  if (!is_safe && REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    bool is_t3m_meta_released = false;
    MTL(ObTenantMetaMemMgr*)->check_all_meta_mem_released(is_t3m_meta_released, "ObLSService"); //just for debug
    LOG_INFO("ls service is not safe to destroy", K(ls_map_.is_empty()),
             K_(safe_ls_destroy_task_cnt), K_(iter_cnt), K(is_t3m_meta_released));
  }
  return is_safe;
}

void ObLSService::inc_ls_safe_destroy_task_cnt()
{
  ATOMIC_INC(&safe_ls_destroy_task_cnt_);
}

void ObLSService::dec_ls_safe_destroy_task_cnt()
{
  ATOMIC_DEC(&safe_ls_destroy_task_cnt_);
}

void ObLSService::inc_iter_cnt()
{
  ATOMIC_INC(&iter_cnt_);
}

void ObLSService::dec_iter_cnt()
{
  ATOMIC_DEC(&iter_cnt_);
}

int ObLSService::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ls service not inited, cannot stop.", K(ret));
  } else {
    // remove all the ls from ls map and push it into the
    // safe to destroy thread.
    common::ObSharedGuard<ObLSIterator> ls_iter;
    ObLSSafeDestroyTask *task = nullptr;
    ObLSHandle handle;
    ObLS *ls = nullptr;
    const bool remove_from_disk = false;
    static const int64_t SLEEP_TS = 100_ms;

    lib::ObMutexGuard change_guard(change_lock_);
    if (OB_FAIL(get_ls_iter(ls_iter, ObLSGetMod::TXSTORAGE_MOD))) {
      LOG_WARN("failed to get ls iter", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        task = nullptr;
        if (OB_FAIL(ls_iter->get_next(ls))) {
          if (OB_ITER_END != ret) {
            LOG_ERROR("fail to get next ls", K(ret));
          }
        } else if (nullptr == ls) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ls is null", K(ret));
        } else if (OB_FAIL(ls->offline())) {
          LOG_WARN("ls offline failed", K(ret), K(ls->get_ls_id()), KP(ls));
        } else if (OB_FAIL(ls->stop())) {
          LOG_WARN("stop ls failed", K(ret), KP(ls), K(ls->get_ls_id()));
        } else if (FALSE_IT(ls->wait())) {
        } else if (OB_FAIL(handle.set_ls(ls_map_, *ls, ObLSGetMod::TXSTORAGE_MOD))) {
          LOG_WARN("get ls handle failed", K(ret), KPC(ls));
        } else {
          ObGarbageCollector *gc_service = MTL(logservice::ObGarbageCollector *);
          ObLSLockGuard lock_ls(ls);
          if (OB_ISNULL(gc_service)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("gc service is null", K(ret));
          } else if (OB_ISNULL(task = (ObLSSafeDestroyTask*)ob_malloc(sizeof(ObLSSafeDestroyTask),
                                                               "LSSafeDestroy"))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret));
          } else if (FALSE_IT(task = new(task) ObLSSafeDestroyTask())) {
          } else if (OB_FAIL(task->init(MTL_ID(),
                                        handle,
                                        this))) {
            LOG_WARN("init safe destroy task failed", K(ret));
          } else {
            remove_ls_(ls, remove_from_disk);
            // try until success.
            while (OB_FAIL(gc_service->add_safe_destroy_task(*task))) {
              if (REACH_TIME_INTERVAL(1_min)) { // every minute
                LOG_WARN("add safe destroy task failed, retry", K(ret), KPC(task));
              }
              ob_usleep(SLEEP_TS);
            }
          }
          if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
            task->~ObLSSafeDestroyTask();
            ob_free(task);
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    LOG_INFO("stop ls service");
    is_running_ = false;
  }
  return ret;
}

int ObLSService::wait()
{
  int ret = OB_SUCCESS;
  while(!safe_to_destroy()) {
    usleep(100 * 1000); // 100 ms
  }
  return ret;
}

int ObLSService::mtl_init(ObLSService* &ls_service)
{
  observer::ObIMetaReport *reporter = GCTX.ob_service_;
  uint64_t tenant_id = MTL_ID();

  return ls_service->init(tenant_id, reporter);
}

int ObLSService::init(const uint64_t tenant_id,
                      observer::ObIMetaReport *reporter)
{
  int ret = OB_SUCCESS;
  const char *OB_LS_SERVICE = "LSSvr";
  const char *OB_LS_ITER = "LSIter";
  const int64_t LS_ALLOC_TOTAL_LIMIT = 1024 * 1024 * 1024;
  const int64_t ITER_ALLOC_TOTAL_LIMIT = 1024 * 1024 * 1024;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls service is inited.", K_(is_inited), K(ret));
  } else if (!is_valid_tenant_id(tenant_id) ||
             OB_ISNULL(reporter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(reporter));
  } else if (OB_FAIL(ls_allocator_.init(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                        OB_LS_SERVICE,
                                        tenant_id,
                                        LS_ALLOC_TOTAL_LIMIT))) {
    LOG_WARN("fail to init ls allocator, ", K(ret));
  } else if (OB_FAIL(iter_allocator_.init(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                          OB_LS_ITER,
                                          tenant_id,
                                          ITER_ALLOC_TOTAL_LIMIT))) {
    LOG_WARN("fail to init iter allocator, ", K(ret));
  } else if (OB_FAIL(ls_map_.init(tenant_id, &ls_allocator_))) {
    LOG_WARN("fail to init ls map", K(ret));
  } else if (OB_FAIL(storage_svr_rpc_proxy_.init(GCTX.net_frame_->get_req_transport(), GCTX.self_addr()))) {
    LOG_WARN("failed to init storage svr rpc proxy", K(ret));
  } else if (OB_FAIL(storage_rpc_.init(&storage_svr_rpc_proxy_, GCTX.self_addr(), GCTX.rs_rpc_proxy_))) {
    STORAGE_LOG(WARN, "fail to init partition service rpc", K(ret));
  } else {
    tenant_id_ = tenant_id;
    rs_reporter_ = reporter;
    is_inited_ = true;
  }
  return ret;
}

int ObLSService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ls service is not inited, cannot start.", K(ret));
  } else if (OB_UNLIKELY(is_running_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is already running", K(ret));
  } else {
    LOG_INFO("ls service start successfully");
    is_running_ = true;
  }
  return ret;
}

int ObLSService::check_tenant_ls_num_()
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const int64_t normal_ls_count = ls_map_.get_ls_count();
  const int64_t removeing_ls_count = ATOMIC_LOAD(&safe_ls_destroy_task_cnt_);
  const int64_t tenant_memory = lib::get_tenant_memory_limit(MTL_ID());
  int64_t tenant_max_ls_limit = OB_MAX(tenant_memory - SMALL_TENANT_MEMORY_LIMIT, 0) / TENANT_MEMORY_PER_LS_NEED +
                                 OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT;
  // the max ls limit should not greater than OB_MAX_LS_NUM_PER_TENANT_PER_SERVER
  tenant_max_ls_limit = OB_MIN(tenant_max_ls_limit, OB_MAX_LS_NUM_PER_TENANT_PER_SERVER);
  // the config priority is higher than the calculate result.
  if (OB_LIKELY(tenant_config.is_valid())) {
    tenant_max_ls_limit = (tenant_config->_max_ls_cnt_per_server != 0 ?
                           tenant_config->_max_ls_cnt_per_server : tenant_max_ls_limit);
  }
  if (normal_ls_count + removeing_ls_count + 1 > tenant_max_ls_limit) {
    ret = OB_TOO_MANY_TENANT_LS;
    LOG_WARN("too many ls of a tenant", K(ret), K(normal_ls_count), K(removeing_ls_count),
             K(tenant_max_ls_limit), K(tenant_memory));
  }
  return ret;
}

int ObLSService::inner_create_ls_(const share::ObLSID &lsid,
                                  const ObMigrationStatus &migration_status,
                                  const ObLSRestoreStatus &restore_status,
                                  const SCN &create_scn,
                                  ObLS *&ls)
{
  int ret = OB_SUCCESS;

  const char *OB_LS_MODE = "ObLS";
  ObMemAttr memattr(tenant_id_, OB_LS_MODE);
  void *buf = NULL;
  if (OB_ISNULL(buf = ls_allocator_.alloc(sizeof(ObLS), memattr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ls", K(ret));
  } else if (FALSE_IT(ls = new (buf) ObLS())) {

  } else if (OB_FAIL(ls->init(lsid,
                              tenant_id_,
                              migration_status,
                              restore_status,
                              create_scn,
                              rs_reporter_))) {
    LOG_WARN("fail to init ls", K(ret), K(lsid));
  }
  if (OB_FAIL(ret) && NULL != ls) {
    ls->~ObLS();
    ls_allocator_.free(ls);
    ls = NULL;
  }
  return ret;
}

int ObLSService::inner_del_ls_(ObLS *&ls)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls)) {
    // do nothing
  } else {
    ls->~ObLS();
    ls_allocator_.free(ls);
    ls = NULL;
  }

  return ret;
}

int ObLSService::add_ls_to_map_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_map_.add_ls(*ls))) {
    LOG_WARN("add ls failed.", K(ret), K(ls->get_ls_id()));
  }
  return ret;
}

int ObLSService::remove_ls_from_map_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_map_.del_ls(ls_id)) &&
      OB_LS_NOT_EXIST != ret) {
    LOG_ERROR("delete ls from map failed", K(ret), K(ls_id));
  }
  if (OB_LS_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLSService::write_prepare_create_ls_slog_(const ObLSMeta &ls_meta) const
{
  int ret = OB_SUCCESS;
  ObCreateLSPrepareSlog slog_entry(ls_meta);
  ObStorageLogParam log_param;
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                          ObRedoLogSubType::OB_REDO_LOG_CREATE_LS);
  if (OB_FAIL(slogger->write_log(log_param))) {
    LOG_WARN("fail to write remove ls slog", K(log_param));
  }
  return ret;
}

int ObLSService::write_commit_create_ls_slog_(const share::ObLSID &ls_id) const
{
  int ret = OB_SUCCESS;
  share::ObLSID tmp_ls_id = ls_id;
  ObCreateLSCommitSLog slog_entry(tmp_ls_id);
  ObStorageLogParam log_param;
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_COMMIT);
  if (OB_FAIL(slogger->write_log(log_param))) {
    LOG_WARN("fail to write create ls commit slog", K(log_param));
  }
  return ret;
}

int ObLSService::write_abort_create_ls_slog_(const share::ObLSID &ls_id) const
{
  int ret = OB_SUCCESS;
  share::ObLSID tmp_ls_id = ls_id;
  ObCreateLSAbortSLog slog_entry(tmp_ls_id);
  ObStorageLogParam log_param;
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_ABORT);
  if (OB_FAIL(slogger->write_log(log_param))) {
    LOG_WARN("fail to write create ls abort slog", K(log_param));
  }
  return ret;
}

int ObLSService::write_remove_ls_slog_(const share::ObLSID &ls_id) const
{
  int ret = OB_SUCCESS;
  share::ObLSID tmp_ls_id = ls_id;
  ObDeleteLSLog slog_entry(tmp_ls_id);
  ObStorageLogParam log_param;
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_DELETE_LS);
  if (OB_FAIL(slogger->write_log(log_param))) {
    LOG_WARN("fail to write remove ls slog", K(log_param));
  }
  return ret;
}

int ObLSService::create_ls(const obrpc::ObCreateLSArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t abs_timeout_ts = INT64_MAX;
  ObLS *ls = NULL;
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  ObLSCreateState state = ObLSCreateState::CREATE_STATE_INIT;
  bool is_commit = true;
  bool need_retry = true;
  bool ls_exist = false;
  bool waiting_destroy = false;
  const SCN create_scn = arg.get_create_scn();
  palf::PalfBaseInfo palf_base_info;
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
  LOG_INFO("create_ls begin", K(arg));
  DEBUG_SYNC(BEFORE_CREATE_USER_LS);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ObServerCheckpointSlogHandler::get_instance().is_started())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls service does not service before slog replay finished", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ObShareUtil::get_abs_timeout(DEFAULT_LOCK_TIMEOUT /* default timeout */,
                                                  abs_timeout_ts))) {
    LOG_WARN("get timeout ts failed", KR(ret));
  } else {
    ObMutexGuardWithTimeout change_guard(change_lock_, abs_timeout_ts);
    if (OB_UNLIKELY(!is_running_)) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("ls service is not running.", K(ret));
    } else if (OB_FAIL(change_guard.get_ret())) {
      LOG_WARN("lock failed, try again later", K(ret));
      ret = OB_EAGAIN;
    } else if (OB_FAIL(check_ls_exist(arg.get_ls_id(), ls_exist))) {
      LOG_WARN("check ls exist failed", K(ret), K(arg));
    } else if (ls_exist) {
      LOG_WARN("ls exist, does not need create again, just return success", K(arg));
    } else if (OB_FAIL(check_ls_waiting_safe_destroy(arg.get_ls_id(),
                                                     waiting_destroy))) {
      LOG_WARN("check ls waiting safe destroy failed", K(ret), K(arg));
    } else if (waiting_destroy) {
      ret = OB_LS_WAITING_SAFE_DESTROY;
      LOG_WARN("ls waiting for destroy, need retry later", K(ret), K(arg));
    } else if (OB_FAIL(check_tenant_ls_num_())) {
      LOG_WARN("too many ls", K(ret));
    } else if (OB_FAIL(inner_create_ls_(arg.get_ls_id(),
                                        migration_status,
                                        (is_ls_to_restore_(arg) ?
                                         ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_START) :
                                         ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE)),
                                        create_scn,
                                        ls))) {
      LOG_WARN("inner create log stream failed.", K(ret), K(arg), K(migration_status));
    } else {
      state = ObLSCreateState::CREATE_STATE_INNER_CREATED;
      // TODO by yunlong: to remove allow_log_sync flag
      const bool unused_allow_log_sync = true;
      prepare_palf_base_info(arg, palf_base_info);
      ObLSLockGuard lock_ls(ls);
      const ObLSMeta &ls_meta = ls->get_ls_meta();
      if (OB_FAIL(add_ls_to_map_(ls))) {
        LOG_WARN("add log stream to map failed.", K(ret));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_ADDED_TO_MAP)) {
        // do nothing
      } else if (OB_FAIL(write_prepare_create_ls_slog_(ls_meta))) {
        LOG_WARN("fail to write create log stream slog", K(ls_meta));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_WRITE_PREPARE_SLOG)) {
      } else if (OB_FAIL(ls->create_ls(arg.get_tenant_info().get_tenant_role(),
                                       palf_base_info,
                                       arg.get_replica_type(),
                                       unused_allow_log_sync))) {
        LOG_WARN("enable ls palf failed", K(ret), K(arg), K(palf_base_info));
        // only restore ls does not need enable replay
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_PALF_ENABLED)) {
        // inner tablet reverted by inner_del_ls_ if fail to create
        // only restore ls with base will not need create inner tablet
      } else if (need_create_inner_tablets_(arg) &&
                 OB_FAIL(ls->create_ls_inner_tablet(arg.get_compat_mode(),
                                                    arg.get_create_scn()))) {
        LOG_WARN("create ls inner tablet failed", K(ret), K(arg));
      } else if (OB_FAIL(write_commit_create_ls_slog_(ls->get_ls_id()))) {
        LOG_WARN("fail to write create log stream commit slog", K(ret), K(ls_meta));
      } else {
        state = ObLSCreateState::CREATE_STATE_FINISH;
        ls->finish_create(is_commit);
        if (OB_FAIL(ls->start())) {
          LOG_ERROR("ls start failed", K(ret), K(arg));
        } else if (!is_ls_to_restore_(arg) &&
                   OB_FAIL(ls->enable_replay_without_lock())) {
          LOG_WARN("enable ls replay failed", K(ret), K(arg));
        } else if (is_ls_to_restore_(arg)) {
          if (OB_FAIL(ls->offline_without_lock())) {
            LOG_WARN("failed to offline", K(ret), K(arg));
          } else if (OB_FAIL(ls->get_log_handler()->enable_sync())) {
            LOG_WARN("failed to enable sync", K(ret), K(arg));
          } else if (OB_FAIL(ls->get_ls_restore_handler()->online())) {
            LOG_WARN("failed to online restore handler", K(ret), K(arg));
          }
        }

        FLOG_INFO("add ls to ls service succ", K(ls->get_ls_id()), K(arg));
        if (OB_SUCCESS != (tmp_ret = ls->report_replica_info())) {
          LOG_WARN("fail to report ls", KR(tmp_ret), K(arg));
        }
      }
      if (OB_FAIL(ret)) {
        do {
          need_retry = false;
          if (state >= ObLSCreateState::CREATE_STATE_WRITE_PREPARE_SLOG) {
            is_commit = false;
            ls->finish_create(is_commit);
            if (OB_SUCCESS != (tmp_ret = write_abort_create_ls_slog_(ls->get_ls_id()))) {
              need_retry = true;
              LOG_ERROR("fail to write create log stream abort slog", K(tmp_ret), K(ls_meta));
            }
          }
        } while (need_retry);
        do {
          // TODO: yanyuan.cxf every remove disable or stop function need be re-entrant
          need_retry = false;
          if (state >= ObLSCreateState::CREATE_STATE_PALF_ENABLED) {
            if (OB_SUCCESS != (tmp_ret = ls->remove_ls())) {
              need_retry = true;
              LOG_WARN("ls disable palf failed", K(tmp_ret));
            }
          }
        } while (need_retry);
      }
    }
    if (OB_FAIL(ret)) {
      del_ls_after_create_ls_failed_(state, ls);
    }
  }
  FLOG_INFO("create_ls finish", K(ret), K(arg), K(abs_timeout_ts));
  return ret;
}

int ObLSService::replay_create_ls(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  bool ls_is_existed = false;
  lib::ObMutexGuard change_guard(change_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_meta));
  } else if (OB_FAIL(check_ls_exist(ls_meta.ls_id_, ls_is_existed))) {
    LOG_WARN("fail to check log stream existence", K(ret), K(ls_meta));
  } else if (!ls_is_existed) {
    if (OB_FAIL(replay_create_ls_(ls_meta))) {
      LOG_WARN("fail to create ls for replay", K(ret), K(ls_meta));
    }
  } else if (OB_FAIL(replay_update_ls_(ls_meta))) {
    LOG_WARN("fail to update ls for replay", K(ret), K(ls_meta));
  } else {
    // do nothing
  }

  return ret;
}

int ObLSService::replay_update_ls(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;

  bool ls_is_existed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_meta));
  } else if (OB_FAIL(check_ls_exist(ls_meta.ls_id_, ls_is_existed))) {
    LOG_WARN("fail to check log stream existence", K(ret), K(ls_meta));
  } else if (!ls_is_existed) {
    LOG_WARN("ls not exit, update will create a new one", K(ls_meta));
    if (OB_FAIL(replay_create_ls_(ls_meta))) {
      LOG_WARN("fail to create ls for replay", K(ret), K(ls_meta));
    }
  } else if (OB_FAIL(replay_update_ls_(ls_meta))) {
    LOG_WARN("fail to update ls for replay", K(ret), K(ls_meta));
  }

  return ret;
}

int ObLSService::restore_update_ls(const ObLSMetaPackage &meta_package)
{
  int ret = OB_SUCCESS;

  bool ls_is_existed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!meta_package.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(meta_package));
  } else if (OB_FAIL(restore_update_ls_(meta_package))) {
    LOG_WARN("fail to update ls for replay", K(ret), K(meta_package));
  }

  return ret;
}

int ObLSService::replay_remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(replay_remove_ls_(ls_id))) {
    LOG_WARN("fail to remove ls for replay", K(ret), K(ls_id));
  }

  return ret;
}

int ObLSService::replay_create_ls_commit(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  bool is_commit = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ls_id));
  } else {
    ls->finish_create(is_commit);
    FLOG_INFO("replay create ls succ", K(ls_id));
  }
  return ret;
}

int ObLSService::gc_ls_after_replay_slog()
{
  // NOTE: we only gc the ls that not create finished or removed.
  // the migrate failed ls will be gc at ObGarbageCollector.
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  static const int64_t SLEEP_TS = 100_ms;
  ObInnerLSStatus ls_status;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  lib::ObMutexGuard change_guard(change_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_FAIL(get_ls_iter(ls_iter, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("fail to get next ls", K(ret));
        }
      } else if (nullptr == ls) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is null", K(ret));
      // this must be succeed
      } else {
        ls_status = ls->get_create_state();
        if (ObInnerLSStatus::CREATING == ls_status || ObInnerLSStatus::REMOVED == ls_status) {
          do {
            if (OB_TMP_FAIL(ls->stop())) {
              LOG_WARN("ls stop failed", K(tmp_ret), K(ls->get_ls_id()), KP(ls));
            } else {
              ls->wait();
            }
            if (OB_SUCCESS != tmp_ret) {
              usleep(SLEEP_TS);
            }
          } while(tmp_ret != OB_SUCCESS);
        }
        ObLSLockGuard lock_ls(ls);
        if (ObInnerLSStatus::CREATING == ls_status) {
          do {
            if (OB_TMP_FAIL(write_abort_create_ls_slog_(ls->get_ls_id()))) {
              LOG_ERROR("fail to write create ls abort slog", K(tmp_ret), KPC(ls));
            }
            if (OB_TMP_FAIL(tmp_ret)) {
              usleep(SLEEP_TS);
            }
          } while (tmp_ret != OB_SUCCESS);
          remove_ls_(ls);
        } else if (ObInnerLSStatus::REMOVED == ls_status) {
          remove_ls_(ls);
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObLSService::enable_replay()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInnerLSStatus ls_status;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  bool can_replay = true;
  if (OB_FAIL(get_ls_iter(ls_iter, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("fail to get next ls", K(ret));
        }
      } else if (nullptr == ls) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is null", K(ret));
      } else if (OB_FAIL(ls->check_can_replay_clog(can_replay))) {
        LOG_WARN("failed to check ls can replay clog", K(ret), KPC(ls));
      } else if (!can_replay) {
        // ls can not enable replay
      } else if (OB_FAIL(ls->enable_replay())) {
        if (OB_LS_IS_DELETED == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("ls status is WAIT_GC, skip it", K(ls->get_ls_id()));
        } else {
          LOG_ERROR("fail to enable replay", K(ret));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObLSService::replay_update_ls_(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  if (OB_FAIL(get_ls(ls_meta.ls_id_, ls_handle, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ls_meta));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ls_meta));
  } else if (OB_FAIL(ls->set_ls_meta(ls_meta))) {
    LOG_WARN("fail to set ls's meta for replay", K(ls_meta));
  }
  return ret;
}

int ObLSService::restore_update_ls_(const ObLSMetaPackage &meta_package)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObLSMeta ls_meta = meta_package.ls_meta_;
  const bool is_rebuild = false;
  if (OB_FAIL(get_ls(ls_meta.ls_id_, ls_handle, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(meta_package));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(meta_package));
  } else if (OB_FAIL(ls->set_clog_checkpoint(ls_meta.get_clog_base_lsn(), ls_meta.get_clog_checkpoint_scn()))) {
    LOG_WARN("failed to set clog checkpoint", K(meta_package));
  } else if (OB_FAIL(ls->advance_base_info(meta_package.palf_meta_, is_rebuild))) {
    LOG_WARN("failed to advance base lsn", K(meta_package));
  }
  return ret;
}

int ObLSService::replay_remove_ls_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  if (OB_FAIL(get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ls_id));
  } else {
    ls->set_create_state(ObInnerLSStatus::REMOVED);
  }
  if (OB_LS_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLSService::replay_create_ls_(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  bool need_retry = true;
  ObLSCreateState state = ObLSCreateState::CREATE_STATE_INIT;
  ObMigrationStatus migration_status;
  ObLSRestoreStatus restore_status;

  if (OB_FAIL(ls_meta.get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(ls_meta));
  } else if (OB_FAIL(ls_meta.get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(ls_meta));
  } else if (OB_FAIL(inner_create_ls_(ls_meta.ls_id_,
                                      migration_status,
                                      restore_status,
                                      ls_meta.get_clog_checkpoint_scn(),
                                      ls))) {
    LOG_WARN("fail to inner create ls", K(ret), K(ls_meta.ls_id_));
  } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_INNER_CREATED)) {
    // do nothing
  } else {
    // Note: if the ls is created successfully, the two argument is not used at palf.
    // if the ls is not created successfully, the ls will be GC later, and the argument
    // just ensure the palf will not return an error code.
    palf::PalfBaseInfo palf_base_info;
    palf_base_info.generate_by_default();
    ObTenantRole unused_tenant_role = share::PRIMARY_TENANT_ROLE;
    // TODO by yunlong: to remove allow_log_sync flag
    const bool unused_allow_log_sync = true;
    ObLSLockGuard lock_ls(ls);
    if (OB_FAIL(ls->set_ls_meta(ls_meta))) {
      LOG_WARN("set ls meta failed", K(ret), K(ls_meta));
    } else if (OB_FAIL(add_ls_to_map_(ls))) {
      LOG_WARN("fail to add ls to the map", K(ret), K(ls_meta.ls_id_));
    } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_ADDED_TO_MAP)) {
      // do nothing
    } else if (OB_FAIL(ls->load_ls(unused_tenant_role,
                                   palf_base_info,
                                   unused_allow_log_sync))) {
      LOG_WARN("enable ls palf failed", K(ret), K(ls_meta));
    } else {
      ls->disable_to_read();
      LOG_INFO("success replay create ls", K(ret), K(ls_meta));
    }
  }
  if (OB_FAIL(ret)) {
    del_ls_after_create_ls_failed_(state, ls);
  }
  return ret;
}

int ObLSService::get_ls(
    const share::ObLSID &ls_id,
    ObLSHandle &handle,
    ObLSGetMod mod)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ls_id));
  } else if (OB_FAIL(ls_map_.get_ls(ls_id, handle, mod))) {
    if (OB_LS_NOT_EXIST != ret || REACH_TIME_INTERVAL(1000 * 1000)) {
      LOG_WARN("get log stream fail", K(ret), K(ls_id));
    }
  }

  return ret;
}

int ObLSService::remove_ls(
    const share::ObLSID &ls_id,
    const bool is_replay)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t abs_timeout_ts = INT64_MAX;
  ObLSHandle handle;
  ObLS *ls = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ObServerCheckpointSlogHandler::get_instance().is_started())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls service does not service before slog replay finished", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(ObShareUtil::get_abs_timeout(DEFAULT_LOCK_TIMEOUT /* default timeout */,
                                                  abs_timeout_ts))) {
    LOG_WARN("get timeout ts failed", KR(ret));
  } else {
    ObGarbageCollector *gc_service = MTL(logservice::ObGarbageCollector *);
    ObMutexGuardWithTimeout change_guard(change_lock_, abs_timeout_ts);
    if (OB_FAIL(change_guard.get_ret())) {
      LOG_WARN("lock failed, try again later", K(ret));
      ret = OB_EAGAIN;
    } else if (OB_ISNULL(gc_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gc service is null", K(ret));
    } else if (OB_FAIL(get_ls(ls_id, handle, ObLSGetMod::TXSTORAGE_MOD))) {
      if (ret == OB_LS_NOT_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get log stream failed", K(ret), K(ls_id));
      }
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("log stream is null, unexpected error", K(ls_id));
    } else if (FALSE_IT(ls->set_is_remove())) {
    // ls leader gc must has block tx start, gracefully kill tx and write offline log before here.
    } else if (OB_FAIL(ls->offline())) {
      LOG_WARN("ls offline failed", K(ret), K(ls_id), KP(ls));
    } else if (OB_FAIL(ls->stop())) {
      LOG_WARN("stop ls failed", K(ret), KP(ls), K(ls_id));
    } else if (FALSE_IT(ls->wait())) {
    } else {
      ObLSSafeDestroyTask *task = nullptr;
      static const int64_t SLEEP_TS = 100_ms;
      ObLSLockGuard lock_ls(ls);
      if (OB_ISNULL(task = (ObLSSafeDestroyTask*)ob_malloc(sizeof(ObLSSafeDestroyTask),
                                                           "LSSafeDestroy"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (FALSE_IT(task = new(task) ObLSSafeDestroyTask())) {
      } else if (FALSE_IT(ls->set_create_state(ObInnerLSStatus::REMOVED))) {
        // set ls to remove state and prevent slog write
      } else if(!is_replay &&
                OB_FAIL(write_remove_ls_slog_(ls_id))) {
        LOG_WARN("fail to write remove ls slog", K(ret));
      } else if (OB_FAIL(task->init(MTL_ID(),
                                    handle,
                                    this))) {
        LOG_WARN("init safe destroy task failed", K(ret));
      } else {
        remove_ls_(ls);
        // try until success.
        while (OB_FAIL(gc_service->add_safe_destroy_task(*task))) {
          if (REACH_TIME_INTERVAL(1_min)) { // every minute
            LOG_WARN("add safe destroy task failed, retry", K(ret), KPC(task));
          }
          ob_usleep(SLEEP_TS);
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
        task->~ObLSSafeDestroyTask();
        ob_free(task);
      }
    }
    // report after remove
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(rs_reporter_)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rs_reporter_ is null", KR(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = rs_reporter_->submit_ls_update_task(tenant_id_, ls_id))) {
        LOG_WARN("fail to submit_ls_update_task", KR(tmp_ret), K_(tenant_id), K(ls_id));
      } else {
        LOG_INFO("submit ls update task after remove_ls success", K(ls_id), K_(tenant_id));
      }
    }
  }

  FLOG_INFO("remove_ls finish", K(ret), K(ls_id), KPC(ls), K(is_replay), K(abs_timeout_ts));
  return ret;
}

void ObLSService::remove_ls_(ObLS *ls, const bool remove_from_disk)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls->get_ls_id();
  static const int64_t SLEEP_TS = 100_ms;
  int64_t retry_cnt = 0;
  do {
    if (OB_FAIL(ls->prepare_for_safe_destroy())) {
      LOG_WARN("prepare safe destroy failed", K(ret), KPC(ls));
    } else if (remove_from_disk && OB_FAIL(ls->remove_ls())) {
      LOG_WARN("remove ls from disk failed", K(ret), K(remove_from_disk), K(ls_id));
    } else if (OB_FAIL(remove_ls_from_map_(ls_id))) {
      LOG_WARN("remove log stream from map fail", K(ret), K(ls_id));
    }
    if (OB_FAIL(ret)) {
      retry_cnt++;
      ob_usleep(SLEEP_TS);
      if (retry_cnt % 100 == 0) {
        LOG_ERROR("remove_ls_ cost too much time", K(ret), KP(ls), K(ls_id));
      }
    }
  } while (OB_FAIL(ret));
}

int ObLSService::create_ls_for_ha(
    const share::ObTaskId task_id,
    const ObMigrationOpArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t abs_timeout_ts = INT64_MAX;
  ObLSCreateState state = ObLSCreateState::CREATE_STATE_INIT;
  ObLS *ls = NULL;
  ObLSMeta ls_meta;
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  bool is_commit = true;
  bool need_retry = true;
  ObMigrationStatus migration_status;
  ObLSRestoreStatus restore_status = ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE);
  bool ls_exist = false;
  bool waiting_destroy = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ObServerCheckpointSlogHandler::get_instance().is_started())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls service does not service before slog replay finished", K(ret));
  } else if (task_id.is_invalid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create ls for ha get invalid argument", K(ret), K(task_id), K(arg));
  } else if (ObMigrationOpType::MIGRATE_LS_OP != arg.type_ && ObMigrationOpType::ADD_LS_OP != arg.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create ls for migration get unexpected op type", K(ret), K(task_id), K(arg));
  } else if (OB_FAIL(ObShareUtil::get_abs_timeout(DEFAULT_LOCK_TIMEOUT /* default timeout */,
                                                  abs_timeout_ts))) {
    LOG_WARN("get timeout ts failed", KR(ret));
  } else {
    ObMutexGuardWithTimeout change_guard(change_lock_, abs_timeout_ts);
    if (OB_UNLIKELY(!is_running_)) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("ls service is not running.", K(ret));
    } else if (OB_FAIL(change_guard.get_ret())) {
      LOG_WARN("lock failed, try again later", K(ret));
      ret = OB_EAGAIN;
    } else if (OB_FAIL(check_ls_exist(arg.ls_id_, ls_exist))) {
      LOG_WARN("check ls exist failed", K(ret), K(arg));
    } else if (ls_exist) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("ls exist, cannot create ls now", K(ret), K(arg));
    } else if (OB_FAIL(check_ls_waiting_safe_destroy(arg.ls_id_,
                                                     waiting_destroy))) {
      LOG_WARN("check ls waiting safe destroy failed", K(ret), K(arg));
    } else if (waiting_destroy) {
      ret = OB_LS_WAITING_SAFE_DESTROY;
      LOG_WARN("ls waiting for destroy, need retry later", K(ret), K(arg));
    } else if (OB_FAIL(check_tenant_ls_num_())) {
      LOG_WARN("too many ls", K(ret));
    } else if (OB_FAIL(ObMigrationStatusHelper::trans_migration_op(arg.type_, migration_status))) {
      LOG_WARN("failed to trans migration op", K(ret), K(arg), K(task_id));
    } else if (OB_FAIL(get_restore_status_(restore_status))) {
      LOG_WARN("failed to get restore status", K(ret), K(arg), K(task_id));
    } else if (OB_FAIL(inner_create_ls_(arg.ls_id_,
                                        migration_status,
                                        restore_status,
                                        ObScnRange::MIN_SCN, /* create scn */
                                        ls))) {
      LOG_WARN("create ls failed", K(ret), K(arg), K(task_id));
    } else {
      state = ObLSCreateState::CREATE_STATE_INNER_CREATED;
      palf::PalfBaseInfo palf_base_info;
      palf_base_info.generate_by_default();
      ObLSLockGuard lock_ls(ls);
      // TODO by yunlong: to remove allow_log_sync flag
      const bool allow_log_sync = false;
      ls_meta = ls->get_ls_meta();
      if (OB_FAIL(add_ls_to_map_(ls))) {
        LOG_WARN("add log stream to map failed.", K(ret));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_ADDED_TO_MAP)) {
        // do nothing
      } else if (OB_FAIL(write_prepare_create_ls_slog_(ls_meta))) {
        LOG_WARN("fail to write create log stream slog", K(ls_meta));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_WRITE_PREPARE_SLOG)) {
      } else if (OB_FAIL(ls->create_ls(share::RESTORE_TENANT_ROLE,
                                       palf_base_info,
                                       arg.dst_.get_replica_type(),
                                       allow_log_sync))) {
        LOG_WARN("enable ls palf failed", K(ret), K(ls_meta));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_PALF_ENABLED)) {
      } else if (OB_FAIL(write_commit_create_ls_slog_(ls->get_ls_id()))) {
        LOG_WARN("fail to write create log stream commit slog", K(ret), K(ls_meta));
      } else {
        state = ObLSCreateState::CREATE_STATE_FINISH;
        ls->finish_create(is_commit);
        if (OB_FAIL(ls->start())) {
          LOG_ERROR("ls start failed", K(tmp_ret), K(ls_meta));
        } else if (OB_FAIL(ls->get_ls_migration_handler()->add_ls_migration_task(task_id, arg))) {
          LOG_WARN("failed to add ls migration task", K(ret), K(arg));
        } else {
          FLOG_INFO("add ls to ls service succ", K(ls->get_ls_id()), K(ls_meta));
        }
      }
      if (OB_FAIL(ret)) {
        do {
          need_retry = false;
          if (state >= ObLSCreateState::CREATE_STATE_WRITE_PREPARE_SLOG) {
            is_commit = false;
            ls->finish_create(is_commit);
            if (OB_SUCCESS != (tmp_ret = write_abort_create_ls_slog_(ls->get_ls_id()))) {
              need_retry = true;
              LOG_ERROR("fail to write create log stream abort slog", K(tmp_ret), K(ls_meta));
            }
          }
        } while (need_retry);
        do {
          need_retry = false;
          // TODO: yanyuan.cxf every remove disable or stop function need be re-entrant
          if (state >= ObLSCreateState::CREATE_STATE_PALF_ENABLED) {
            if (OB_SUCCESS != (tmp_ret = ls->remove_ls())) {
              need_retry = true;
              LOG_WARN("ls disable palf failed", K(tmp_ret));
            }
          }
        } while (need_retry);
      }
    }
    if (OB_FAIL(ret)) {
      del_ls_after_create_ls_failed_(state, ls);
    }
  }
  FLOG_INFO("create_ls for ha finish", K(ret), K(ls_meta), K(abs_timeout_ts));
  return ret;
}

void ObLSService::del_ls_after_create_ls_failed_(ObLSCreateState& ls_create_state, ObLS *ls)
{
  if (OB_NOT_NULL(ls)) {
    int tmp_ret = OB_SUCCESS;
    bool need_retry = false;
    do {
      need_retry = false;
      if (ls_create_state >= ObLSCreateState::CREATE_STATE_ADDED_TO_MAP) {
        if (OB_SUCCESS != (tmp_ret = remove_ls_from_map_(ls->get_ls_id()))) {
          need_retry = true;
          LOG_ERROR_RET(tmp_ret, "remove ls from map failed", K(tmp_ret));
        }
      } else if (ls_create_state >= ObLSCreateState::CREATE_STATE_INNER_CREATED) {
        if (OB_SUCCESS != (tmp_ret = inner_del_ls_(ls))) {
          need_retry = true;
          LOG_ERROR_RET(tmp_ret, "inner del ls failed.", K(tmp_ret));
        }
      }
    } while (need_retry);
  }
}

int ObLSService::check_ls_exist(const share::ObLSID &ls_id, bool &exist)
{
  int ret = OB_SUCCESS;
  ObLSHandle handle;
  exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls(ls_id, handle, ObLSGetMod::TXSTORAGE_MOD))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_DEBUG("get log stream failed", K(ls_id), K(ret));
    }
  } else if (OB_ISNULL(handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get log stream failed", K(ls_id), K(ret));
  } else {
    exist = true;
  }
  if (OB_LS_NOT_EXIST == ret) {
    exist = false;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLSService::check_ls_waiting_safe_destroy(const share::ObLSID &ls_id, bool &waiting)
{
  int ret = OB_SUCCESS;
  waiting = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (ATOMIC_LOAD(&safe_ls_destroy_task_cnt_) == 0) {
    // there is no ls waiting safe destroy
  } else {
    ObGarbageCollector *gc_service = MTL(logservice::ObGarbageCollector *);
    ObSafeDestroyCheckLSExist fn(ls_id);
    if (OB_ISNULL(gc_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gc service is null", K(ret));
    } else if (OB_FAIL(gc_service->safe_destroy_task_for_each(fn))) {
      LOG_WARN("check ls waiting safe destroy failed", K(ret), K(ls_id));
    } else if (OB_FAIL(fn.get_ret_code())) {
      LOG_WARN("the check process failed", K(ret), K(ls_id));
    } else {
      waiting = fn.is_exist();
    }
  }
  return ret;
}

int ObLSService::get_ls_iter(common::ObSharedGuard<ObLSIterator> &guard, ObLSGetMod mod)
{
  int ret = OB_SUCCESS;
  ObLSIterator *ls_iter = NULL;
  void *buf = NULL;
  const char* LS = "ObLSIter";
  ObMemAttr attr(tenant_id_, LS);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == (buf = iter_allocator_.alloc(sizeof(ObLSIterator), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory for log stream iterator.", K(ret));
  } else {
    ls_iter = new (buf) ObLSIterator();
    ls_iter->set_ls_map(ls_map_, mod);
    inc_iter_cnt();
    if (OB_FAIL(guard.assign(ls_iter, [&](ObLSIterator *iter) mutable {
                                        iter->~ObLSIterator();
                                        iter_allocator_.free(iter);
                                        dec_iter_cnt();
                                      }))) {
      LOG_WARN("create guard failed.", K(ret));
    }
  }
  return ret;
}

int ObLSService::get_ls_ids(common::ObIArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  ls_id_array.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ls_map_.get_all_ls_id(ls_id_array))) {
    LOG_WARN("failed to get all ls id", K(ret));
  }
  return ret;
}

bool ObLSService::is_ls_to_restore_(const obrpc::ObCreateLSArg &arg) const
{
  return arg.get_tenant_info().is_restore();
}

bool ObLSService::need_create_inner_tablets_(const obrpc::ObCreateLSArg &arg) const
{
  return arg.need_create_inner_tablets();
}

int ObLSService::iterate_diagnose(const ObFunction<int(const storage::ObLS &ls)> &func)
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;

  if (OB_FAIL(get_ls_iter(ls_iter, ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("fail to get next ls", K(ret));
        }
      } else if (nullptr == ls) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is null", K(ret));
      } else if (OB_FAIL(func(*ls))) {
        LOG_WARN("iter ls diagnose failed", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLSService::get_restore_status_(
    share::ObLSRestoreStatus &restore_status)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  restore_status = ObLSRestoreStatus::RESTORE_NONE;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    restore_status = ObLSRestoreStatus::RESTORE_NONE;
  } else if (share::ObTenantRole::INVALID_TENANT == MTL_GET_TENANT_ROLE_CACHE()) {
    //tenant role not ready, need wait
    ret = OB_NEED_WAIT;
    LOG_WARN("tenant role is invalid now, need wait", KR(ret), K(tenant_id));
  } else if (FALSE_IT(restore_status = MTL_TENANT_ROLE_CACHE_IS_RESTORE() ?
      ObLSRestoreStatus::RESTORE_START : ObLSRestoreStatus::RESTORE_NONE)) {
  }
  return ret;
}

int ObLSService::dump_ls_info()
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  ObLSMeta ls_meta;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_ls_iter(ls_iter, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_iter->get_next(ls))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next ls", K(ret));
      }
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret));
    } else if (OB_FAIL(ls->get_ls_meta(ls_meta))) {
      LOG_WARN("fail to get ls meta", K(ret));
    } else {
      FLOG_INFO("dump ls info", K(ls_meta));
    }
  }
  return ret;
}



} // storage
} // oceanbase

