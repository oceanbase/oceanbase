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
#include "ob_log_service.h"
#include "lib/file/file_directory_utils.h"
#include "lib/ob_errno.h"
#include "ob_server_log_block_mgr.h"
#include "palf/log_block_pool_interface.h"
#include "rpc/frame/ob_req_transport.h"
#include "share/ob_ls_id.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_tenant_info_proxy.h"
#include "share/ob_unit_getter.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/ob_srv_network_frame.h"
#include "logservice/palf_handle_guard.h"
#include "storage/ob_file_system_router.h"
#include "palf/palf_env.h"
#include "palf/palf_callback.h"
#include "palf/palf_options.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace palf;

namespace logservice
{
using namespace oceanbase::share;
using namespace oceanbase::common;

ObLogService::ObLogService() :
    is_inited_(false),
    is_running_(false),
    self_(),
    palf_env_(NULL),
    apply_service_(),
    replay_service_(),
    role_change_service_(),
    location_adapter_(),
    ls_adapter_(),
    rpc_proxy_(),
    reporter_(),
    restore_service_()
  {}

ObLogService::~ObLogService()
{
  destroy();
}

int ObLogService::mtl_init(ObLogService* &logservice)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  observer::ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  const palf::PalfDiskOptions &disk_options = MTL_INIT_CTX()->disk_options_;
  const char *tenant_clog_dir = MTL_INIT_CTX()->tenant_clog_dir_;
  const char *clog_dir = OB_FILE_SYSTEM_ROUTER.get_clog_dir();
  ObLocationService *location_service = GCTX.location_service_;
  observer::ObIMetaReport *reporter = GCTX.ob_service_;
  ObServerLogBlockMgr *log_block_mgr = GCTX.log_block_mgr_;
  if (OB_FAIL(logservice->init(disk_options,
                          tenant_clog_dir,
                          self,
                          net_frame->get_req_transport(),
                          MTL(ObLSService*),
                          location_service,
                          reporter,
                          log_block_mgr))) {
    CLOG_LOG(ERROR, "init ObLogService failed", K(ret), K(tenant_clog_dir));
  } else if (OB_FAIL(FileDirectoryUtils::fsync_dir(clog_dir))) {
    CLOG_LOG(ERROR, "fsync_dir failed", K(ret), K(clog_dir));
  }
  return ret;
}

int ObLogService::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(apply_service_.start())) {
    CLOG_LOG(WARN, "failed to start apply_service_", K(ret));
  } else if (OB_FAIL(replay_service_.start())) {
    CLOG_LOG(WARN, "failed to start replay_service_", K(ret));
  } else if (OB_FAIL(role_change_service_.start())) {
    CLOG_LOG(WARN, "failed to start role_change_service_", K(ret));
  } else if (OB_FAIL(cdc_service_.start())) {
    CLOG_LOG(WARN, "failed to start cdc_service_", K(ret));
  } else if (OB_FAIL(restore_service_.start())) {
    CLOG_LOG(WARN, "failed to start restore_service_", K(ret));
  } else {
    is_running_ = true;
    FLOG_INFO("ObLogService is started");
  }
  return ret;
}

void ObLogService::stop()
{
  is_running_ = false;
  CLOG_LOG(INFO, "begin to stop ObLogService");
  (void)apply_service_.stop();
  (void)replay_service_.stop();
  (void)role_change_service_.stop();
  (void)cdc_service_.stop();
  (void)restore_service_.stop();
  FLOG_INFO("ObLogService is stopped");
}

void ObLogService::wait()
{
  apply_service_.wait();
  replay_service_.wait();
  role_change_service_.wait();
  cdc_service_.wait();
  restore_service_.wait();
}

void ObLogService::destroy()
{
  is_inited_ = false;
  self_.reset();
  apply_service_.destroy();
  replay_service_.destroy();
  role_change_service_.destroy();
  location_adapter_.destroy();
  ls_adapter_.destroy();
  rpc_proxy_.destroy();
  reporter_.destroy();
  restore_service_.destroy();
  if (NULL != palf_env_) {
    PalfEnv::destroy_palf_env(palf_env_);
    palf_env_ = NULL;
  }
  cdc_service_.destroy();
  FLOG_INFO("ObLogService is destroyed");
}

int check_and_prepare_dir(const char *dir)
{
  bool is_exist = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(common::FileDirectoryUtils::is_exists(dir, is_exist))) {
    CLOG_LOG(WARN, "chcck dir exist failed", K(ret), K(dir));
    // means it's restart
  } else if (is_exist == true) {
    CLOG_LOG(INFO, "director exist", K(ret), K(dir));
    // means it's create tenant
  } else if (OB_FAIL(common::FileDirectoryUtils::create_directory(dir))) {
    CLOG_LOG(WARN, "create_directory failed", K(ret), K(dir));
  } else {
    CLOG_LOG(INFO, "check_and_prepare_dir success", K(ret), K(dir));
  }
  return ret;
}

int ObLogService::init(const PalfDiskOptions &disk_options,
                       const char *base_dir,
                       const common::ObAddr &self,
                       rpc::frame::ObReqTransport *transport,
                       ObLSService *ls_service,
                       ObLocationService *location_service,
                       observer::ObIMetaReport *reporter,
                       palf::ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  common::ObILogAllocator *alloc_mgr = NULL;

  int64_t tenant_id = MTL_ID();
  if (OB_FAIL(check_and_prepare_dir(base_dir))) {
    CLOG_LOG(WARN, "check_and_prepare_dir failed", K(ret), K(base_dir));
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogService init twice", K(ret));
  } else if (OB_ISNULL(base_dir) || OB_UNLIKELY(!self.is_valid()) || OB_ISNULL(transport)
      || OB_ISNULL(transport) || OB_ISNULL(ls_service) || OB_ISNULL(location_service) || OB_ISNULL(reporter)
      || false == disk_options.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KP(base_dir), K(self), KP(transport),
             KP(ls_service), KP(location_service), KP(reporter), K(disk_options), K(ret));
  } else if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr))) {
    CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(ret));
  } else if (OB_FAIL(PalfEnv::create_palf_env(disk_options, base_dir, self, transport,
                                              alloc_mgr, log_block_pool, palf_env_))) {
    CLOG_LOG(WARN, "failed to create_palf_env", K(base_dir), K(ret));
  } else if (OB_ISNULL(palf_env_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "palf_env_ is NULL", K(ret));
  } else if (OB_FAIL(ls_adapter_.init(ls_service))) {
    CLOG_LOG(ERROR, "failed to init ls_adapter", K(ret));
  } else if (OB_FAIL(apply_service_.init(palf_env_, &ls_adapter_))) {
    CLOG_LOG(WARN, "failed to init apply_service", K(ret));
  } else if (OB_FAIL(replay_service_.init(palf_env_, &ls_adapter_, alloc_mgr))) {
    CLOG_LOG(WARN, "failed to init replay_service", K(ret));
  } else if (OB_FAIL(role_change_service_.init(ls_service, &apply_service_, &replay_service_))) {
    CLOG_LOG(WARN, "failed to init role_change_service_", K(ret));
  } else if (OB_FAIL(location_adapter_.init(location_service))) {
    CLOG_LOG(WARN, "failed to init location_adapter_", K(ret));
  } else if (OB_FAIL(rpc_proxy_.init(transport))) {
    CLOG_LOG(WARN, "LogServiceRpcProxy init failed", K(ret));
  } else if (OB_FAIL(reporter_.init(reporter))) {
    CLOG_LOG(WARN, "ReporterAdapter init failed", K(ret));
  } else if (OB_FAIL(cdc_service_.init(tenant_id, ls_service))) {
    CLOG_LOG(WARN, "failed to init cdc_service_", K(ret));
  } else if (OB_FAIL(restore_service_.init(transport, ls_service, this))) {
    CLOG_LOG(WARN, "failed to init restore_service_", K(ret));
  } else {
    self_ = self;
    is_inited_ = true;
    FLOG_INFO("ObLogService init success", K(ret), K(base_dir), K(self), KP(transport),
        KP(ls_service), K(tenant_id));
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

palf::AccessMode ObLogService::get_palf_access_mode(const share::ObTenantRole &tenant_role)
{
  palf::AccessMode mode = palf::AccessMode::INVALID_ACCESS_MODE;
  switch (tenant_role.value()) {
    case share::ObTenantRole::INVALID_TENANT:
      mode = palf::AccessMode::INVALID_ACCESS_MODE;
      break;
    case share::ObTenantRole::PRIMARY_TENANT:
      mode = palf::AccessMode::APPEND;
      break;
    case share::ObTenantRole::STANDBY_TENANT:
    case share::ObTenantRole::RESTORE_TENANT:
      mode = palf::AccessMode::RAW_WRITE;
      break;
    default:
      mode = palf::AccessMode::INVALID_ACCESS_MODE;
      break;
  }
  return mode;
}

int ObLogService::create_ls(const ObLSID &id,
                            const ObReplicaType &replica_type,
                            const share::ObTenantRole &tenant_role,
                            const int64_t create_ts,
                            const bool allow_log_sync,
                            ObLogHandler &log_handler,
                            ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  palf::PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  palf_base_info.prev_log_info_.log_ts_ = create_ts;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == create_ts)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(id), K(replica_type),
        K(tenant_role), K(create_ts));
  } else if (OB_FAIL(create_ls_(id, replica_type, tenant_role, palf_base_info, allow_log_sync,
                                log_handler, restore_handler))) {
    CLOG_LOG(WARN, "create ls failed", K(ret), K(id), K(replica_type),
        K(tenant_role), K(palf_base_info));
  } else {
    FLOG_INFO("ObLogService create_ls success", K(ret), K(id), K(replica_type), K(tenant_role), K(create_ts),
        K(log_handler), K(restore_handler));
  }

  return ret;
}

int ObLogService::create_ls(const share::ObLSID &id,
                            const common::ObReplicaType &replica_type,
                            const share::ObTenantRole &tenant_role,
                            const palf::PalfBaseInfo &palf_base_info,
                            const bool allow_log_sync,
                            ObLogHandler &log_handler,
                            ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (!palf_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(id), K(replica_type),
        K(tenant_role), K(palf_base_info));
  } else if (OB_FAIL(create_ls_(id, replica_type, tenant_role, palf_base_info, allow_log_sync,
                                log_handler, restore_handler))) {
    CLOG_LOG(WARN, "create ls failed", K(ret), K(id), K(replica_type),
        K(tenant_role), K(palf_base_info));
  } else {
    FLOG_INFO("ObLogService create_ls success", K(ret), K(id), K(replica_type), K(tenant_role), K(palf_base_info),
        K(log_handler), K(restore_handler));
  }
  return ret;
}

int ObLogService::remove_ls(const ObLSID &id,
                            ObLogHandler &log_handler,
                            ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_FAIL(apply_service_.remove_ls(id))) {
    CLOG_LOG(WARN, "failed to remove from apply_service", K(ret), K(id));
  } else if (OB_FAIL(replay_service_.remove_ls(id))) {
    CLOG_LOG(WARN, "failed to remove from replay_service", K(ret), K(id));
    // NB: remove palf_handle lastly.
  } else {
    // NB: can not execute destroy, otherwise, each interface in log_handler or restore_handler
    // may return OB_NOT_INIT.
    // TODO by runlin: create_ls don't init ObLogHandler and ObLogRestoreHandler.
    //
    // In normal case(for gc), stop has been executed, this stop has no effect.
    // In abnormal case(create ls failed, need remove ls directlly), there is no possibility for dead lock.
    log_handler.stop();
    restore_handler.stop();
    if (OB_FAIL(palf_env_->remove(id.id()))) {
      CLOG_LOG(WARN, "failed to remove from palf_env_", K(ret), K(id));
    } else {
      FLOG_INFO("ObLogService remove_ls success", K(ret), K(id));
    }
  }

  return ret;
}

int ObLogService::check_palf_exist(const ObLSID &id, bool &exist) const
{
  int ret = OB_SUCCESS;
  PalfHandle handle;
  exist = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogService is not inited", K(ret), K(id));
  } else if (OB_FAIL(palf_env_->open(id.id(), handle))) {
    if (OB_ENTRY_NOT_EXIST == ret ) {
      ret = OB_SUCCESS;
      exist = false;
    } else {
      CLOG_LOG(WARN, "open palf failed", K(ret), K(id));
    }
  }

  if (true == handle.is_valid()) {
    palf_env_->close(handle);
  }
  return ret;
}

int ObLogService::add_ls(const ObLSID &id,
                         const ObReplicaType &replica_type,
                         ObLogHandler &log_handler,
                         ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  PalfHandle palf_handle;
  PalfRoleChangeCb *rc_cb = &role_change_service_;
  PalfLocationCacheCb *loc_cache_cb = &location_adapter_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_FAIL(palf_env_->open(id.id(), palf_handle))) {
    CLOG_LOG(WARN, "failed to get palf_handle", K(ret), K(id));
  } else if (OB_FAIL(apply_service_.add_ls(id))) {
    CLOG_LOG(WARN, "failed to add_ls for apply_service", K(ret), K(id));
  } else if (OB_FAIL(replay_service_.add_ls(id,
                                            replica_type))) {
    CLOG_LOG(WARN, "failed to add_ls for replay_service", K(ret), K(id));
  } else if (OB_FAIL(log_handler.init(id.id(), self_, &apply_service_, &replay_service_, 
          &role_change_service_, palf_handle, palf_env_, loc_cache_cb, &rpc_proxy_))) {
    CLOG_LOG(WARN, "ObLogHandler init failed", K(ret), K(id), KP(palf_env_), K(palf_handle));
  } else if (OB_FAIL(restore_handler.init(id.id(), palf_env_))) {
    CLOG_LOG(WARN, "ObLogRestoreHandler init failed", K(ret), K(id), KP(palf_env_));
  } else if (OB_FAIL(palf_handle.register_role_change_cb(rc_cb))) {
    CLOG_LOG(WARN, "register_role_change_cb failed", K(ret));
  } else if (OB_FAIL(palf_handle.set_location_cache_cb(loc_cache_cb))) {
    CLOG_LOG(WARN, "set_location_cache_cb failed", K(ret), K(id));
  } else {
    FLOG_INFO("add_ls success", K(ret), K(id), K(replica_type), KP(this));
  }

  if (OB_FAIL(ret)) {
    if (true == palf_handle.is_valid() && false == log_handler.is_valid()) {
      palf_env_->close(palf_handle);
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LS_NOT_EXIST;
  }

  return ret;
}

int ObLogService::open_palf(const share::ObLSID &id,
                            palf::PalfHandleGuard &palf_handle_guard)
{
  int ret = OB_SUCCESS;
  palf::PalfHandle palf_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_FAIL(palf_env_->open(id.id(), palf_handle))) {
    CLOG_LOG(WARN, "failed to get palf_handle", K(ret), K(id));
  } else if (FALSE_IT(palf_handle_guard.set(palf_handle, palf_env_))) {
  } else {
    CLOG_LOG(TRACE, "ObLogService open_palf success", K(ret), K(id));
  }

  if (OB_FAIL(ret)) {
    if (true == palf_handle.is_valid()) {
      palf_env_->close(palf_handle);
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LS_NOT_EXIST;
  }
  return ret;
}

int ObLogService::update_replayable_point(const int64_t replayable_point)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (OB_FAIL(replay_service_.update_replayable_point(replayable_point))) {
    CLOG_LOG(WARN, "update_replayable_point failed", K(ret), K(replayable_point));
  }
  return ret;
}

int ObLogService::get_palf_role(const share::ObLSID &id,
                                common::ObRole &role,
                                int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard palf_handle_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_FAIL(open_palf(id, palf_handle_guard))) {
    CLOG_LOG(WARN, "failed to open palf", K(ret), K(id));
  } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
    CLOG_LOG(WARN, "failed to get role", K(ret), K(id));
  }
  return ret;
}

int ObLogService::get_palf_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = palf_env_->get_disk_usage(used_size_byte, total_size_byte);
  }
  return ret;
}

int ObLogService::update_palf_disk_options(const palf::PalfDiskOptions &disk_options)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(palf_env_->update_disk_options(disk_options))) {
    CLOG_LOG(WARN, "update_disk_options failed", K(ret), K(disk_options));
  } else {
    CLOG_LOG(INFO, "update_palf_disk_options success", K(disk_options), K(MTL_ID()));
  }
  return ret;
}

int ObLogService::update_log_disk_util_threshold(const int64_t log_disk_utilization_threshold,
                                                 const int64_t log_disk_utilization_limit_threshold)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(update_disk_opts_lock_);
  PalfDiskOptions disk_opts;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(palf_env_->get_disk_options(disk_opts))) {
    CLOG_LOG(WARN, "get_disk_options failed", K(ret));
  } else if (FALSE_IT(disk_opts.log_disk_utilization_threshold_ = log_disk_utilization_threshold)
      || FALSE_IT(disk_opts.log_disk_utilization_limit_threshold_ = log_disk_utilization_limit_threshold)) {
  } else if (OB_FAIL(palf_env_->update_disk_options(disk_opts))) {
    CLOG_LOG(WARN, "update_disk_options failed", K(ret), K(log_disk_utilization_threshold), K(log_disk_utilization_limit_threshold));
  } else {
    CLOG_LOG(INFO, "update_log_disk_util_threshold success", K(log_disk_utilization_threshold),
             K(log_disk_utilization_limit_threshold), K(MTL_ID()));
  }
  return ret;
}

int ObLogService::update_log_disk_usage_limit_size(const int64_t log_disk_usage_limit_size)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(update_disk_opts_lock_);
  PalfDiskOptions disk_opts;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(palf_env_->get_disk_options(disk_opts))) {
    CLOG_LOG(WARN, "get_disk_options failed", K(ret), K(log_disk_usage_limit_size));
  } else if (FALSE_IT(disk_opts.log_disk_usage_limit_size_ = log_disk_usage_limit_size)) {
  } else if (OB_FAIL(palf_env_->update_disk_options(disk_opts))) {
    CLOG_LOG(WARN, "update_disk_options failed", K(ret), K(log_disk_usage_limit_size));
  } else {
    CLOG_LOG(INFO, "update_log_disk_usage_limit_size success", K(log_disk_usage_limit_size), K(MTL_ID()));
  }
  return ret;
}

int ObLogService::get_palf_disk_options(palf::PalfDiskOptions &opts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = palf_env_->get_disk_options(opts);
  }
  return ret;
}

int ObLogService::iterate_palf(const ObFunction<int(const PalfHandle&)> &func)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = palf_env_->for_each(func);
  }
  return ret;
}

int ObLogService::iterate_apply(const ObFunction<int(const ObApplyStatus&)> &func)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = apply_service_.stat_for_each(func);
  }
  return ret;
}

int ObLogService::iterate_replay(const ObFunction<int(const ObReplayStatus&)> &func)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = replay_service_.stat_for_each(func);
  }
  return ret;
}

int ObLogService::create_ls_(const share::ObLSID &id,
                             const common::ObReplicaType &replica_type,
                             const share::ObTenantRole &tenant_role,
                             const palf::PalfBaseInfo &palf_base_info,
                             const bool allow_log_sync,
                             ObLogHandler &log_handler,
                             ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  PalfHandle palf_handle;
  PalfRoleChangeCb *rc_cb = &role_change_service_;
  PalfLocationCacheCb *loc_cache_cb = &location_adapter_;
  if (false == id.is_valid() ||
      INVALID_TENANT_ROLE == tenant_role ||
      false == palf_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(id), K(id), K(tenant_role), K(palf_base_info));
  } else if (OB_FAIL(palf_env_->create(id.id(), get_palf_access_mode(tenant_role),
                                       palf_base_info, palf_handle))) {
    CLOG_LOG(WARN, "failed to get palf_handle", K(ret), K(id));
  } else if (false == allow_log_sync && OB_FAIL(palf_handle.disable_sync())) {
    CLOG_LOG(WARN, "failed to disable_sync", K(ret), K(id));
  } else if (OB_FAIL(apply_service_.add_ls(id))) {
    CLOG_LOG(WARN, "failed to add_ls for apply engine", K(ret), K(id));
  } else if (OB_FAIL(replay_service_.add_ls(id, replica_type))) {
    CLOG_LOG(WARN, "failed to add_ls", K(ret), K(id));
  } else if (OB_FAIL(log_handler.init(id.id(), self_, &apply_service_, &replay_service_, 
          &role_change_service_, palf_handle, palf_env_, loc_cache_cb, &rpc_proxy_))) {
    CLOG_LOG(WARN, "ObLogHandler init failed", K(ret), KP(palf_env_), K(palf_handle));
  } else if (OB_FAIL(restore_handler.init(id.id(), palf_env_))) {
    CLOG_LOG(WARN, "ObLogRestoreHandler init failed", K(ret), K(id), KP(palf_env_));
  } else if (OB_FAIL(palf_handle.register_role_change_cb(rc_cb))) {
    CLOG_LOG(WARN, "register_role_change_cb failed", K(ret), K(id));
  } else if (OB_FAIL(palf_handle.set_location_cache_cb(loc_cache_cb))) {
    CLOG_LOG(WARN, "set_location_cache_cb failed", K(ret), K(id));
  } else {
    CLOG_LOG(INFO, "ObLogService create_ls success", K(ret), K(id), K(log_handler));
  }
  if (OB_FAIL(ret)) {
    if (true == palf_handle.is_valid() && false == log_handler.is_valid()) {
      palf_env_->close(palf_handle);
    }
  }
  return ret;
}

int ObLogService::diagnose_role_change(RCDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (OB_FAIL(role_change_service_.diagnose(diagnose_info))) {
    CLOG_LOG(WARN, "role_change_service diagnose failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogService::diagnose_replay(const share::ObLSID &id,
                                  ReplayDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (OB_FAIL(replay_service_.diagnose(id, diagnose_info))) {
    CLOG_LOG(WARN, "replay_service diagnose failed", K(ret), K(id));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogService::diagnose_apply(const share::ObLSID &id,
                                 ApplyDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (OB_FAIL(apply_service_.diagnose(id, diagnose_info))) {
    CLOG_LOG(WARN, "apply_service diagnose failed", K(ret), K(id));
  } else {
    // do nothing
  }
  return ret;
}
}//end of namespace logservice
}//end of namespace oceanbase
