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
#include "storage/ls/ob_ls_state.h"
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
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
using namespace lib;
using namespace logservice;
namespace storage
{
#define OB_BREAK_FAIL(statement) (OB_UNLIKELY(((++process_point) && break_point == process_point && OB_FAIL(OB_BREAK_BY_TEST)) || OB_FAIL(statement)))

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
    is_stopped_(false),
    tenant_id_(OB_INVALID_ID),
    ls_map_(),
    ls_allocator_(),
    iter_allocator_(),
    change_lock_(common::ObLatchIds::LS_CHANGE_LOCK),
    rs_reporter_(nullptr),
    storage_svr_rpc_proxy_(),
    storage_rpc_(),
    safe_ls_destroy_task_cnt_(0),
    iter_cnt_(0),
    max_ls_cnt_(0)
{}

ObLSService::~ObLSService()
{
  destroy();
}

void ObLSService::destroy()
{
  int ret = OB_SUCCESS;
  LOG_INFO("destroy ls service", KP(this));
  if (is_running_ || !is_stopped_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("should has been stopped before destroy", K(ret), K_(is_running), K_(is_stopped), KP(this));
  }
  if (ATOMIC_LOAD(&iter_cnt_) != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls iter cnt is not 0", K(ret), K_(iter_cnt), KP(this));
  }
  tenant_id_ = OB_INVALID_ID;
  ls_map_.reset();
  ls_allocator_.destroy();
  iter_allocator_.destroy();
  rs_reporter_ = nullptr;
  storage_svr_rpc_proxy_.destroy();
  storage_rpc_.destroy();
  max_ls_cnt_ = 0;
  is_inited_ = false;
}

bool ObLSService::is_empty()
{
  bool is_safe = (ls_map_.is_empty() &&
                  ATOMIC_LOAD(&safe_ls_destroy_task_cnt_) == 0);
  if (!is_safe && REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    bool is_t3m_meta_released = false;
    MTL(ObTenantMetaMemMgr*)->check_all_meta_mem_released(is_t3m_meta_released, "ObLSService"); //just for debug
    LOG_INFO("ls service is not empty and not safe to destroy", K(ls_map_.is_empty()),
             K_(safe_ls_destroy_task_cnt), K(is_t3m_meta_released));
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

int ObLSService::get_resource_constraint_value(ObResoureConstraintValue &constraint_value)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls service not inited, the resource info may not right.", K(ret));
  } else if (!is_running_ || is_stopped_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("the ls service is not running ,the resource info may not right.", K(ret));
  } else {
    ret = get_resource_constraint_value_(constraint_value);
  }

  return ret;
}

int ObLSService::get_resource_constraint_value_(ObResoureConstraintValue &constraint_value)
{
  int ret = OB_SUCCESS;
  int64_t config_value = OB_MAX_LS_NUM_PER_TENANT_PER_SERVER;
  int64_t memory_value = INT64_MAX;
  int64_t clog_disk_value = INT64_MAX;
  // 1. configuration
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_LIKELY(tenant_config.is_valid())) {
    config_value = (tenant_config->_max_ls_cnt_per_server != 0
                    ? tenant_config->_max_ls_cnt_per_server : config_value);
  }

  // 2. memory
  const int64_t tenant_memory = lib::get_tenant_memory_limit(MTL_ID());
  memory_value = OB_MAX(tenant_memory - SMALL_TENANT_MEMORY_LIMIT, 0) / TENANT_MEMORY_PER_LS_NEED +
    OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT;

  // 3. clog disk
  palf::PalfOptions palf_opts;
  if (OB_FAIL(MTL(ObLogService*)->get_palf_options(palf_opts))) {
    LOG_WARN("get palf options failed", K(ret));
  } else {
    const palf::PalfDiskOptions &disk_opts = palf_opts.disk_options_;
    clog_disk_value = disk_opts.log_disk_usage_limit_size_ / MIN_DISK_SIZE_PER_PALF_INSTANCE;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(constraint_value.set_type_value(CONFIGURATION_CONSTRAINT, config_value))) {
    LOG_WARN("set_type_value failed", K(ret), K(CONFIGURATION_CONSTRAINT), K(config_value));
  } else if (OB_FAIL(constraint_value.set_type_value(MEMORY_CONSTRAINT, memory_value))) {
    LOG_WARN("set_type_value failed", K(ret), K(MEMORY_CONSTRAINT), K(memory_value));
  } else if (OB_FAIL(constraint_value.set_type_value(CLOG_DISK_CONSTRAINT, clog_disk_value))) {
    LOG_WARN("set_type_value failed", K(ret), K(CLOG_DISK_CONSTRAINT), K(clog_disk_value));
  }

  return ret;
}

int ObLSService::get_current_info(share::ObResourceInfo &info)
{
  int ret = OB_SUCCESS;
  ObResoureConstraintValue constraint_value;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls service not inited, the resource info may not right.", K(ret));
  } else if (!is_running_ || is_stopped_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("the ls service is not running ,the resource info may not right.", K(ret));
  } else if (OB_FAIL(get_resource_constraint_value_(constraint_value))) {
    LOG_WARN("get resource constraint value failed", K(ret));
  } else {
    info.curr_utilization_ = ls_map_.get_ls_count() + ATOMIC_LOAD(&safe_ls_destroy_task_cnt_);
    info.max_utilization_ = ATOMIC_LOAD(&max_ls_cnt_);
    info.reserved_value_ = 0; // reserve value will be used later
    constraint_value.get_min_constraint(info.min_constraint_type_, info.min_constraint_value_);
  }
  return ret;
}

int ObLSService::cal_min_phy_resource_needed(share::ObMinPhyResourceResult &min_phy_res)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls service not inited, the resource info may not right.", K(ret));
  } else if (!is_running_ || is_stopped_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("the ls service is not running ,the resource info may not right.", K(ret));
  } else {
    int64_t ls_cnt = ls_map_.get_ls_count() + ATOMIC_LOAD(&safe_ls_destroy_task_cnt_);
    ret = cal_min_phy_resource_needed_(ls_cnt, min_phy_res);
  }
  return ret;
}

int ObLSService::cal_min_phy_resource_needed(const int64_t num,
                                             ObMinPhyResourceResult &min_phy_res)
{
  int ret = OB_SUCCESS;
  ret = cal_min_phy_resource_needed_(num, min_phy_res);
  return ret;
}

int ObLSService::cal_min_phy_resource_needed_(const int64_t num,
                                              ObMinPhyResourceResult &min_phy_res)
{
  int ret = OB_SUCCESS;
  int64_t ls_cnt = num;
  int64_t clog_disk_bytes = 0;
  int64_t memory_bytes = 0;
  // 1. memory
  // if the ls num is smaller than OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT,
  // just return SMALL_TENANT_MEMORY_LIMIT.
  memory_bytes = (SMALL_TENANT_MEMORY_LIMIT
                  + OB_MAX(ls_cnt - OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT, 0)
                  * TENANT_MEMORY_PER_LS_NEED);
  memory_bytes = ls_cnt > 0 ? memory_bytes : 0;
  // 2. clog disk
  clog_disk_bytes = MIN_DISK_SIZE_PER_PALF_INSTANCE * OB_MAX(0, ls_cnt);

  if (OB_FAIL(min_phy_res.set_type_value(PHY_RESOURCE_MEMORY, memory_bytes))) {
    LOG_WARN("set type value failed", K(PHY_RESOURCE_MEMORY), K(memory_bytes));
  } else if (OB_FAIL(min_phy_res.set_type_value(PHY_RESOURCE_CLOG_DISK, clog_disk_bytes))) {
    LOG_WARN("set type value failed", K(PHY_RESOURCE_CLOG_DISK), K(clog_disk_bytes));
  }
  return ret;
}

int ObLSService::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ls service not inited, cannot stop.", K(ret));
  } else if (!is_running_ || is_stopped_) {
    // do nothing
  } else {
    // remove all the ls from ls map and push it into the
    // safe to destroy thread.
    common::ObSharedGuard<ObLSIterator> ls_iter;
    ObLSHandle handle;
    ObLS *ls = nullptr;
    const bool remove_from_disk = false;

    lib::ObMutexGuard change_guard(change_lock_);
    if (OB_FAIL(get_ls_iter(ls_iter, ObLSGetMod::TXSTORAGE_MOD))) {
      LOG_WARN("failed to get ls iter", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(ls_iter->get_next(ls))) {
          if (OB_ITER_END != ret) {
            LOG_ERROR("fail to get next ls", K(ret));
          }
        } else if (OB_FAIL(handle.set_ls(ls_map_, *ls, ObLSGetMod::TXSTORAGE_MOD))) {
          LOG_WARN("get ls handle failed", K(ret), KPC(ls));
        } else if (OB_FAIL(safe_remove_ls_(handle, remove_from_disk))) {
          LOG_WARN("safe remove ls failed", K(ret), KPC(ls));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    is_running_ = false;
    is_stopped_ = true;
  }
  LOG_INFO("stop ls service");
  return ret;
}

int ObLSService::wait()
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  int64_t begin_time = ObTimeUtility::current_time();
  while(!is_empty()) {
    ++retry_times;
    if (retry_times % 100 == 0) {
      LOG_WARN("ls service wait empty for too much time", K(retry_times), K(begin_time));
    }
    usleep(100 * 1000); // 100 ms
  }
  retry_times = 0;
  while(ATOMIC_LOAD(&iter_cnt_) != 0) {
    ++retry_times;
    if (retry_times % 100 == 0) {
      LOG_WARN("ls service wait ls iter for too much time", K(retry_times), K_(iter_cnt), K(begin_time));
    }
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
  ObResoureConstraintValue constraint_value;
  if (OB_FAIL(get_resource_constraint_value_(constraint_value))) {
    LOG_WARN("get resource constraint value failed", K(ret));
  } else {
    int64_t min_constraint_value = 0;
    int64_t min_constraint_type = 0;
    const int64_t normal_ls_count = ls_map_.get_ls_count();
    const int64_t removeing_ls_count = ATOMIC_LOAD(&safe_ls_destroy_task_cnt_);
    constraint_value.get_min_constraint(min_constraint_type, min_constraint_value);
    if (normal_ls_count + removeing_ls_count + 1 > min_constraint_value) {
      ret = OB_TOO_MANY_TENANT_LS;
      LOG_WARN("too many ls of a tenant", K(ret), K(normal_ls_count), K(removeing_ls_count),
               K(min_constraint_type), K(get_constraint_type_name(min_constraint_type)), K(min_constraint_value));
      LOG_DBA_WARN_(OB_STORAGE_LS_COUNT_REACH_UPPER_LIMIT, ret,
                    "The current tenant has too many log streams. ",
                    "normal_ls_count(", normal_ls_count, ") + removeing_ls_count(", removeing_ls_count,
                    ") >= min_constraint_value(", min_constraint_value, "). ",
                    "The resource of ", get_constraint_type_name(min_constraint_type),
                    " limits the number of log streams. ",
                    "[suggestion] Expand the tenant's unit resources. ",
                    "You can also query the GV$OB_TENANT_RESOURCE_LIMIT_DETAIL view ",
                    "to get which resource limits the number of log streams. ");
    }
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
  } else {
    // update the max ls cnt
    const int64_t normal_ls_count = ls_map_.get_ls_count();
    const int64_t removeing_ls_count = ATOMIC_LOAD(&safe_ls_destroy_task_cnt_);
    inc_update(&max_ls_cnt_, normal_ls_count + removeing_ls_count);
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
  LOG_INFO("create_ls begin", K(arg));
  DEBUG_SYNC(BEFORE_CREATE_USER_LS);

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    palf::PalfBaseInfo palf_base_info;
    prepare_palf_base_info(arg, palf_base_info);

    ObMigrationOpArg mig_arg;
    ObCreateLSCommonArg common_arg;
    common_arg.ls_id_ = arg.get_ls_id();
    common_arg.create_scn_ = arg.get_create_scn();
    common_arg.palf_base_info_ = palf_base_info;
    common_arg.tenant_role_ = arg.get_tenant_info().get_tenant_role();
    common_arg.replica_type_ = arg.get_replica_type();
    common_arg.compat_mode_ = arg.get_compat_mode();
    common_arg.migration_status_ = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
    common_arg.restore_status_ = get_restore_status_by_tenant_role_(arg.get_tenant_info().get_tenant_role());
    common_arg.create_type_ = get_create_type_by_tenant_role_(arg.get_tenant_info().get_tenant_role());
    common_arg.need_create_inner_tablet_ = need_create_inner_tablets_(arg);

    if (OB_FAIL(create_ls_(common_arg, mig_arg))) {
      LOG_WARN("create ls failed", K(ret), K(arg));
    }
    if (OB_LS_EXIST == ret) {
      // new create ls can not be gc because it does not has memberlist.
      // we need reuse the ls if it exist.
      ret = OB_SUCCESS;
    }
  }
  FLOG_INFO("create_ls finish", K(ret), K(arg));
  return ret;
}

int ObLSService::post_create_ls_(const int64_t create_type,
                                 ObLS *&ls)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_online = false;
  if (OB_FAIL(ls->check_ls_need_online(need_online))) {
    LOG_WARN("check ls need online failed", K(ret));
  } else if (need_online &&
             OB_FAIL(ls->online_without_lock())) {
    LOG_ERROR("ls start failed", K(ret));
  } else {
    switch(create_type) {
    case ObLSCreateType::NORMAL: {
      if (OB_FAIL(ls->set_start_work_state())) {
        LOG_ERROR("ls set start work state failed", KR(ret), KPC(ls));
      } else {
        ls->enable_to_read();
      }
      break;
    }
    case ObLSCreateType::RESTORE: {
      if (!need_online && ls->is_restore_first_step()) {
        if (OB_FAIL(ls->get_log_handler()->enable_sync())) {
          LOG_WARN("failed to enable sync", K(ret));
        } else if (OB_FAIL(ls->get_ls_restore_handler()->online())) {
          LOG_WARN("failed to online restore handler", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ls->set_start_ha_state())) {
        LOG_ERROR("ls set start ha state failed", KR(ret), KPC(ls));
      }
      break;
    }
    case ObLSCreateType::MIGRATE: {
      if (OB_FAIL(ls->set_start_ha_state())) {
        LOG_ERROR("ls set start ha state failed", KR(ret), KPC(ls));
      }
      break;
    }
    case ObLSCreateType::CLONE: {
      if (!need_online && ls->is_clone_first_step()) {
        if (OB_FAIL(ls->get_log_handler()->enable_sync())) {
          LOG_WARN("failed to enable sync", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ls->set_start_ha_state())) {
        LOG_ERROR("ls set start ha state failed", KR(ret), KPC(ls));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("should not be here.", KR(ret));
    } // default
    } // switch
  }

  if (OB_SUCCESS != (tmp_ret = ls->report_replica_info())) {
    LOG_WARN("fail to report ls", KR(tmp_ret), KPC(ls));
  }
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
  int64_t create_type;
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
    ObLSLockGuard lock_ls(ls);
    if (OB_FAIL(ls->get_create_type(create_type))) {
      LOG_WARN("get ls create type failed", K(ret));
    } else {
      switch(create_type) {
      case ObLSCreateType::NORMAL: {
        if (OB_FAIL(ls->set_start_work_state())) {
          LOG_ERROR("ls set start work state failed", KR(ret), K(ls_id));
        }
        break;
      }
      case ObLSCreateType::RESTORE: {
        if (OB_FAIL(ls->set_start_ha_state())) {
          LOG_ERROR("ls set start ha state failed", KR(ret), K(ls_id));
        }
        break;
      }
      case ObLSCreateType::MIGRATE: {
        if (OB_FAIL(ls->set_start_ha_state())) {
          LOG_ERROR("ls set start ha state failed", KR(ret), K(ls_id));
        }
        break;
      }
      case ObLSCreateType::CLONE: {
        if (OB_FAIL(ls->set_start_ha_state())) {
          LOG_ERROR("ls set start ha state failed", KR(ret), K(ls_id));
        }
        break;
      }
      default: {
        if (OB_FAIL(ls->set_start_work_state())) {
          LOG_ERROR("ls set start work state failed", KR(ret), K(ls_id));
        }
      } // default
      } // switch
    }
    FLOG_INFO("replay create ls", KR(ret), K(ls_id));
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
  ObLSPersistentState ls_status;
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
        ls_status = ls->get_persistent_state();
        if (ls_status.is_need_gc()) {
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
        if (ls_status.is_init_state()) {
          do {
            if (OB_TMP_FAIL(write_abort_create_ls_slog_(ls->get_ls_id()))) {
              LOG_ERROR("fail to write create ls abort slog", K(tmp_ret), KPC(ls));
            }
            if (OB_TMP_FAIL(tmp_ret)) {
              usleep(SLEEP_TS);
            }
          } while (tmp_ret != OB_SUCCESS);
          remove_ls_(ls, true/*remove_from_disk*/, false/*write_slog*/);
        } else if (ls_status.is_zombie_state()) {
          remove_ls_(ls, true/*remove_from_disk*/, false/*write_slog*/);
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

// online all the ls
int ObLSService::online_ls()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  int64_t create_type = ObLSCreateType::NORMAL;
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
      } else {
        ObLSLockGuard lock_ls(ls);
        if (OB_FAIL(ls->get_create_type(create_type))) {
          LOG_WARN("get ls create type failed", K(ret));
        } else if (OB_FAIL(post_create_ls_(create_type, ls))) {
          LOG_WARN("post create ls failed", K(ret));
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
  } else if (OB_FAIL(ls->set_remove_state())) {
    LOG_ERROR("ls set remove state failed", KR(ret), K(ls_id));
  } else {
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
    const bool allow_log_sync = false;
    ObLSLockGuard lock_ls(ls);
    if (OB_FAIL(ls->set_ls_meta(ls_meta))) {
      LOG_WARN("set ls meta failed", K(ret), K(ls_meta));
    } else if (OB_FAIL(add_ls_to_map_(ls))) {
      LOG_WARN("fail to add ls to the map", K(ret), K(ls_meta.ls_id_));
    } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_ADDED_TO_MAP)) {
      // do nothing
    } else if (OB_FAIL(ls->load_ls(unused_tenant_role,
                                   palf_base_info,
                                   allow_log_sync))) {
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

int ObLSService::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t abs_timeout_ts = INT64_MAX;
  ObLSHandle handle;
  const bool remove_from_disk = true;
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
    // ls leader gc must has block tx start, gracefully kill tx and write offline log before here.
    } else if (OB_FAIL(safe_remove_ls_(handle, remove_from_disk))) {
      LOG_WARN("safe remove ls failed", K(ret), K(ls_id), K(handle));
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

  FLOG_INFO("remove_ls finish", K(ret), K(ls_id), K(handle), K(abs_timeout_ts));
  return ret;
}

int ObLSService::safe_remove_ls_(ObLSHandle handle, const bool remove_from_disk)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  int64_t process_point = 0; // for test
  if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log stream is null, unexpected error");
  } else if (OB_BREAK_FAIL(ls->offline())) {
    LOG_WARN("ls offline failed", K(ret), KP(ls));
  } else if (OB_BREAK_FAIL(ls->stop())) {
    LOG_WARN("stop ls failed", K(ret), KP(ls));
  } else if (FALSE_IT(ls->wait())) {
  } else {
    ObGarbageCollector *gc_service = MTL(logservice::ObGarbageCollector *);
    ObLSSafeDestroyTask *task = nullptr;
    const ObLSID &ls_id = ls->get_ls_id();
    static const int64_t SLEEP_TS = 100_ms;
    ObLSLockGuard lock_ls(ls);
    const bool write_slog = remove_from_disk;
    if (OB_ISNULL(task = (ObLSSafeDestroyTask*)ob_malloc(sizeof(ObLSSafeDestroyTask),
                                                         "LSSafeDestroy"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (FALSE_IT(task = new(task) ObLSSafeDestroyTask())) {
    } else if (OB_BREAK_FAIL(ret)) {
      LOG_WARN("break fail for malloc", K(ret));
    } else if (remove_from_disk && OB_BREAK_FAIL(ls->set_remove_state())) {
      LOG_WARN("ls set remove state failed", KR(ret), K(ls_id));
    } else if (OB_BREAK_FAIL(task->init(MTL_ID(),
                                        handle,
                                        this))) {
      LOG_WARN("init safe destroy task failed", K(ret));
    } else {
      remove_ls_(ls, remove_from_disk, write_slog);
      // try until success.
      while (OB_BREAK_FAIL(gc_service->add_safe_destroy_task(*task))) {
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
  return ret;
}

void ObLSService::remove_ls_(ObLS *ls, const bool remove_from_disk, const bool write_slog)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls->get_ls_id();
  static const int64_t SLEEP_TS = 100_ms;
  int64_t retry_cnt = 0;
  int64_t success_step = 0;
  transaction::ObTransService *tx_svr = MTL(transaction::ObTransService*);

  do {
    // We must do prepare_for_safe_destroy to remove tablets from ObLSTabletService before writing the remove_ls_slog,
    // After removing tablets, no update_tablet_slog will be written. Otherwise, writing the update_tablet_slog will be
    // concurrent with remove_ls_slog, causing the update_tablet_slog to fall behind remove_ls_slog, and causing replay
    // creating an invalid tablet during restart.
    ret = OB_SUCCESS;
    if (success_step < 1) {
      if (OB_FAIL(ls->prepare_for_safe_destroy())) {
        LOG_WARN("prepare safe destroy failed", K(ret), KPC(ls));
      } else {
        success_step = 1;
      }
    }
    if (success_step < 2 && OB_SUCC(ret)) {
      if(write_slog && OB_FAIL(write_remove_ls_slog_(ls_id))) {
        LOG_WARN("fail to write remove ls slog", K(ret));
      } else {
        success_step = 2;
      }
    }
    if (success_step < 3 && OB_SUCC(ret)) {
      if (remove_from_disk && OB_FAIL(ls->remove_ls())) {
        LOG_WARN("remove ls from disk failed", K(ret), K(remove_from_disk), K(ls_id));
      } else {
        success_step = 3;
      }
    }
    if (success_step < 4 && OB_SUCC(ret)) {
      if (OB_FAIL(remove_ls_from_map_(ls_id))) {
        LOG_WARN("remove log stream from map fail", K(ret), K(ls_id));
      } else {
        success_step = 4;
      }
    }
    if (success_step < 5 && OB_SUCC(ret)) {
      if (OB_FAIL(tx_svr->remove_tablet(ls_id))) {
        LOG_WARN("remove tablet cache fail", K(ret), K(ls_id));
      } else {
        success_step = 5;
      }
    }
    if (OB_FAIL(ret)) {
      retry_cnt++;
      ob_usleep(SLEEP_TS);
      if (retry_cnt % 100 == 0) {
        LOG_ERROR("remove_ls_ cost too much time", K(ret), KP(ls), K(ls_id), K(success_step));
      }
    }
  } while (OB_FAIL(ret));
}

int ObLSService::create_ls_(const ObCreateLSCommonArg &arg,
                            const ObMigrationOpArg &mig_arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t abs_timeout_ts = INT64_MAX;
  ObLSCreateState state = ObLSCreateState::CREATE_STATE_INIT;
  ObLS *ls = NULL;
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  bool need_retry = true;
  bool ls_exist = false;
  bool waiting_destroy = false;
  int64_t process_point = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ls service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ObServerCheckpointSlogHandler::get_instance().is_started())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls service does not service before slog replay finished", K(ret));
  } else if (OB_BREAK_FAIL(ObShareUtil::get_abs_timeout(DEFAULT_LOCK_TIMEOUT /* default timeout */,
                                                  abs_timeout_ts))) {
    LOG_WARN("get timeout ts failed", KR(ret));
  } else {
    ObMutexGuardWithTimeout change_guard(change_lock_, abs_timeout_ts);
    if (OB_UNLIKELY(!is_running_)) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("ls service is not running.", K(ret));
    } else if (OB_BREAK_FAIL(change_guard.get_ret())) {
      LOG_WARN("lock failed, try again later", K(ret));
      ret = OB_EAGAIN;
    } else if (OB_BREAK_FAIL(check_ls_exist(arg.ls_id_, ls_exist))) {
      LOG_WARN("check ls exist failed", K(ret), K(arg.ls_id_));
    } else if (ls_exist) {
      ret = OB_LS_EXIST;
      LOG_WARN("ls exist, cannot create ls now", K(ret), K(arg.ls_id_));
    } else if (OB_BREAK_FAIL(check_ls_waiting_safe_destroy(arg.ls_id_,
                                                           waiting_destroy))) {
      LOG_WARN("check ls waiting safe destroy failed", K(ret), K(arg.ls_id_));
    } else if (waiting_destroy) {
      ret = OB_LS_WAITING_SAFE_DESTROY;
      LOG_WARN("ls waiting for destroy, need retry later", K(ret), K(arg.ls_id_));
    } else if (OB_BREAK_FAIL(check_tenant_ls_num_())) {
      LOG_WARN("too many ls", K(ret));
    } else if (OB_BREAK_FAIL(inner_create_ls_(arg.ls_id_,
                                              arg.migration_status_,
                                              arg.restore_status_,
                                              arg.create_scn_,
                                              ls))) {
      LOG_WARN("create ls failed", K(ret), K(arg.ls_id_));
    } else {
      state = ObLSCreateState::CREATE_STATE_INNER_CREATED;
      ObLSLockGuard lock_ls(ls);
      const ObLSMeta &ls_meta = ls->get_ls_meta();
      if (OB_BREAK_FAIL(add_ls_to_map_(ls))) {
        LOG_WARN("add log stream to map failed.", K(ret));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_ADDED_TO_MAP)) {
        // do nothing
      } else if (OB_BREAK_FAIL(write_prepare_create_ls_slog_(ls_meta))) {
        LOG_WARN("fail to write create log stream slog", K(ls_meta));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_WRITE_PREPARE_SLOG)) {
      } else if (OB_BREAK_FAIL(ls->create_ls(arg.tenant_role_,
                                             arg.palf_base_info_,
                                             arg.replica_type_))) {
        LOG_WARN("enable ls palf failed", K(ret), K(ls_meta));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_PALF_ENABLED)) {
      } else if (arg.need_create_inner_tablet_ &&
                 OB_BREAK_FAIL(ls->create_ls_inner_tablet(arg.compat_mode_,
                                                          arg.create_scn_))) {
        LOG_WARN("create ls inner tablet failed", K(ret), K(ls_meta));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_INNER_TABLET_CREATED)) {
      } else if (OB_BREAK_FAIL(write_commit_create_ls_slog_(ls->get_ls_id()))) {
        LOG_WARN("fail to write create log stream commit slog", K(ret), K(ls_meta));
      } else if (OB_BREAK_FAIL(ls->finish_create_ls())) {
        LOG_WARN("finish create ls failed", KR(ret));
      } else if (FALSE_IT(state = ObLSCreateState::CREATE_STATE_FINISH)) {
      } else if (OB_BREAK_FAIL(post_create_ls_(arg.create_type_, ls))) {
        LOG_WARN("post create ls failed", K(ret), K(ls_meta));
      } else if (ObLSCreateType::MIGRATE == arg.create_type_ &&
                 OB_BREAK_FAIL(ls->get_ls_migration_handler()->add_ls_migration_task(arg.task_id_,
                                                                                     mig_arg))) {
        LOG_WARN("failed to add ls migration task", K(ret), K(mig_arg));
      }
    }
    if (OB_BREAK_FAIL(ret)) {
      del_ls_after_create_ls_failed_(state, ls);
    }
  }
  return ret;
}

int ObLSService::create_ls_for_ha(
    const share::ObTaskId task_id,
    const ObMigrationOpArg &arg)
{
  int ret = OB_SUCCESS;
  ObMigrationStatus migration_status;
  ObLSRestoreStatus restore_status = ObLSRestoreStatus(ObLSRestoreStatus::NONE);

  if (task_id.is_invalid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create ls for ha get invalid argument", K(ret), K(task_id), K(arg));
  } else if (ObMigrationOpType::MIGRATE_LS_OP != arg.type_ && ObMigrationOpType::ADD_LS_OP != arg.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create ls for migration get unexpected op type", K(ret), K(task_id), K(arg));
  } else if (OB_FAIL(ObMigrationStatusHelper::trans_migration_op(arg.type_, migration_status))) {
    LOG_WARN("failed to trans migration op", K(ret), K(arg), K(task_id));
  } else if (OB_FAIL(get_restore_status_(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(arg), K(task_id));
  } else {
    palf::PalfBaseInfo palf_base_info;
    palf_base_info.generate_by_default();

    ObCreateLSCommonArg common_arg;
    common_arg.ls_id_ = arg.ls_id_;
    common_arg.create_scn_ = ObScnRange::MIN_SCN;
    common_arg.palf_base_info_ = palf_base_info;
    common_arg.tenant_role_ = share::RESTORE_TENANT_ROLE;
    common_arg.replica_type_ = arg.dst_.get_replica_type();
    common_arg.compat_mode_ = Worker::CompatMode::INVALID;
    common_arg.create_type_ = ObLSCreateType::MIGRATE;
    common_arg.migration_status_ = migration_status;
    common_arg.restore_status_ = restore_status;
    common_arg.task_id_ = task_id;
    common_arg.need_create_inner_tablet_ = false;

    if (OB_FAIL(create_ls_(common_arg, arg))) {
      LOG_WARN("failed to create ls", K(ret), K(arg));
    }
  }
  FLOG_INFO("create_ls for ha finish", K(ret), K(task_id), K(arg));
  return ret;
}

void ObLSService::del_ls_after_create_ls_failed_(ObLSCreateState& in_ls_create_state, ObLS *ls)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_retry = false;
  const bool remove_from_disk = true;
  ObLSHandle handle;
  ObLSCreateState ls_create_state = in_ls_create_state;
  if (OB_NOT_NULL(ls)) {
    do {
      need_retry = false;
      tmp_ret = OB_SUCCESS;
      if (ls_create_state >= ObLSCreateState::CREATE_STATE_FINISH) {
        if (OB_TMP_FAIL(handle.set_ls(ls_map_, *ls, ObLSGetMod::TXSTORAGE_MOD))) {
          need_retry = true;
          LOG_WARN("get ls handle failed", K(tmp_ret), KPC(ls));
        } else if (OB_TMP_FAIL(safe_remove_ls_(handle, remove_from_disk))) {
          need_retry = true;
          LOG_WARN("safe remove ls failed", K(tmp_ret));
        }
      } else {
        if (ls_create_state >= ObLSCreateState::CREATE_STATE_INNER_TABLET_CREATED) {
          if (OB_TMP_FAIL(ls->remove_ls_inner_tablet())) {
            need_retry = true;
            LOG_WARN("remove ls inner tablet failed", K(tmp_ret));
          } else {
            ls_create_state = ObLSCreateState::CREATE_STATE_PALF_ENABLED;
          }
        }
        if (OB_TMP_FAIL(tmp_ret)) {
        } else if (ls_create_state >= ObLSCreateState::CREATE_STATE_PALF_ENABLED) {
          if (OB_TMP_FAIL(ls->remove_ls())) {
            need_retry = true;
            LOG_WARN("ls inner remove failed", K(tmp_ret));
          } else {
            ls_create_state = ObLSCreateState::CREATE_STATE_WRITE_PREPARE_SLOG;
          }
        }
        if (OB_TMP_FAIL(tmp_ret)) {
        } else if (ls_create_state >= ObLSCreateState::CREATE_STATE_WRITE_PREPARE_SLOG) {
          if (OB_TMP_FAIL(ls->set_remove_state())) {
            need_retry = true;
            LOG_ERROR("fail to set ls remove state", K(tmp_ret), KPC(ls));
          } else if (OB_TMP_FAIL(write_abort_create_ls_slog_(ls->get_ls_id()))) {
            need_retry = true;
            LOG_ERROR("fail to write create log stream abort slog", K(tmp_ret), KPC(ls));
          } else {
            ls_create_state = ObLSCreateState::CREATE_STATE_ADDED_TO_MAP;
          }
        }
        if (OB_TMP_FAIL(tmp_ret)) {
        } else if (ls_create_state >= ObLSCreateState::CREATE_STATE_ADDED_TO_MAP) {
          if (OB_TMP_FAIL(remove_ls_from_map_(ls->get_ls_id()))) {
            need_retry = true;
            LOG_ERROR_RET(tmp_ret, "remove ls from map failed", K(tmp_ret));
          } else {
            ls_create_state = ObLSCreateState::CREATE_STATE_INIT;
          }
        }
        if (OB_TMP_FAIL(tmp_ret)) {
        } else if (ls_create_state >= ObLSCreateState::CREATE_STATE_INNER_CREATED) {
          if (OB_TMP_FAIL(inner_del_ls_(ls))) {
            need_retry = true;
            LOG_ERROR_RET(tmp_ret, "inner del ls failed.", K(tmp_ret));
          } else {
            ls_create_state = ObLSCreateState::CREATE_STATE_INIT;
          }
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

ERRSIM_POINT_DEF(ALLOC_LS_ITER_GUARD_FAIL)
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
  } else if (is_stopped_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls service is stopped.", K(ret), KP(this));
  } else if (NULL == (buf = iter_allocator_.alloc(sizeof(ObLSIterator), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for log stream iterator.", K(ret));
  } else {
    ls_iter = new (buf) ObLSIterator();
    ls_iter->set_ls_map(ls_map_, mod);
    inc_iter_cnt();
    if (OB_FAIL(ALLOC_LS_ITER_GUARD_FAIL)) {
      LOG_WARN("ALLOC_LS_ITER_GUARD_FAIL");
    } else if (OB_FAIL(guard.assign(ls_iter, [&](ObLSIterator *iter) mutable {
                                               iter->~ObLSIterator();
                                               iter_allocator_.free(iter);
                                               dec_iter_cnt();
                                             }))) {
      LOG_WARN("create guard failed.", K(ret));
    }
    // if assign failed, we need free the memory we have allocated.
    if (OB_FAIL(ret)) {
      ls_iter->~ObLSIterator();
      iter_allocator_.free(ls_iter);
      dec_iter_cnt();
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

bool ObLSService::is_ls_to_clone_(const obrpc::ObCreateLSArg &arg) const
{
  return arg.get_tenant_info().is_clone();
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
  restore_status = ObLSRestoreStatus::NONE;

  if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    restore_status = ObLSRestoreStatus::NONE;
  } else {
    ObAllTenantInfo tenant_info;
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id,
                                           GCTX.sql_proxy_,
                                           false,
                                           tenant_info))) {
      LOG_WARN("fail to load tenant info", KR(ret));
    } else {
      restore_status = get_restore_status_by_tenant_role_(tenant_info.get_tenant_role());
    }
  }
  return ret;
}

ObLSRestoreStatus ObLSService::get_restore_status_by_tenant_role_(const ObTenantRole& tenant_role)
{
  ObLSRestoreStatus restore_status = ObLSRestoreStatus(ObLSRestoreStatus::NONE);

  if (tenant_role.is_restore()) {
    restore_status = ObLSRestoreStatus::RESTORE_START;
  } else if (tenant_role.is_clone()) {
    restore_status = ObLSRestoreStatus::CLONE_START;
  } else {
    restore_status = ObLSRestoreStatus::NONE;
  }

  return restore_status;
}

int64_t ObLSService::get_create_type_by_tenant_role_(const ObTenantRole& tenant_role)
{
  int64_t create_type = ObLSCreateType::NORMAL;

  if (tenant_role.is_restore()) {
    create_type = ObLSCreateType::RESTORE;
  } else if (tenant_role.is_clone()) {
    create_type = ObLSCreateType::CLONE;
  } else {
    create_type = ObLSCreateType::NORMAL;
  }

  return create_type;
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

