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
#include "log_service.h"
#include "lib/file/file_directory_utils.h"
#include "lib/ob_errno.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "logservice/palf/log_block_pool_interface.h"
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
#include "logservice/palf/palf_env.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_options.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace palf;

namespace palfcluster
{
using namespace oceanbase::share;
using namespace oceanbase::common;

LogService::LogService() :
    is_inited_(false),
    is_running_(false),
    self_(),
    palf_env_(NULL),
    apply_service_(),
    // replay_service_(),
    role_change_service_(),
    monitor_(),
    rpc_proxy_(),
    log_client_map_(),
    ls_adapter_()
  {}

LogService::~LogService()
{
  destroy();
}

int LogService::mtl_init(LogService* &logservice)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  const int64_t tenant_id = MTL_ID();
  observer::ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  //log_disk_usage_limit_size无法主动从配置项获取, 需要在mtl初始化时作为入参传入
  const palf::PalfOptions &palf_options = MTL_INIT_CTX()->palf_options_;
  const char *tenant_clog_dir = MTL_INIT_CTX()->tenant_clog_dir_;
  const char *clog_dir = OB_FILE_SYSTEM_ROUTER.get_clog_dir();
  logservice::ObServerLogBlockMgr *log_block_mgr = GCTX.log_block_mgr_;
  common::ObILogAllocator *alloc_mgr = NULL;
  if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr))) {
    CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(ret));
  } else if (OB_FAIL(logservice->init(palf_options,
                                      tenant_clog_dir,
                                      self,
                                      alloc_mgr,
                                      net_frame->get_req_transport(),
                                      log_block_mgr))) {
    CLOG_LOG(ERROR, "init LogService failed", K(ret), K(tenant_clog_dir));
  } else if (OB_FAIL(FileDirectoryUtils::fsync_dir(clog_dir))) {
    CLOG_LOG(ERROR, "fsync_dir failed", K(ret), K(clog_dir));
  } else {
    CLOG_LOG(INFO, "LogService mtl_init success");
  }
  return ret;
}

void LogService::mtl_destroy(LogService* &logservice)
{
  common::ob_delete(logservice);
  logservice = nullptr;
  // Free tenant_log_allocator for this tenant after destroy logservice.
  const int64_t tenant_id = MTL_ID();
  int ret = OB_SUCCESS;
  if (OB_FAIL(TMA_MGR_INSTANCE.delete_tenant_log_allocator(tenant_id))) {
    CLOG_LOG(WARN, "delete_tenant_log_allocator failed", K(ret));
  }
}

int LogService::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(apply_service_.start())) {
    CLOG_LOG(WARN, "failed to start apply_service_", K(ret));
  // } else if (OB_FAIL(replay_service_.start())) {
  //   CLOG_LOG(WARN, "failed to start replay_service_", K(ret));
  } else if (OB_FAIL(role_change_service_.start())) {
    CLOG_LOG(WARN, "failed to start role_change_service_", K(ret));
  } else {
    is_running_ = true;
    FLOG_INFO("LogService is started");
  }
  return ret;
}

void LogService::stop()
{
  is_running_ = false;
  CLOG_LOG(INFO, "begin to stop LogService");
  (void)apply_service_.stop();
  // (void)replay_service_.stop();
  (void)role_change_service_.stop();
  FLOG_INFO("LogService is stopped");
}

void LogService::wait()
{
  apply_service_.wait();
  // replay_service_.wait();
  role_change_service_.wait();
}

void LogService::destroy()
{
  is_inited_ = false;
  self_.reset();
  apply_service_.destroy();
  // replay_service_.destroy();
  role_change_service_.destroy();
  ls_adapter_.destroy();
  rpc_proxy_.destroy();
  if (NULL != palf_env_) {
    PalfEnv::destroy_palf_env(palf_env_);
    palf_env_ = NULL;
  }
  FLOG_INFO("LogService is destroyed");
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

int LogService::init(const PalfOptions &options,
                       const char *base_dir,
                       const common::ObAddr &self,
                       common::ObILogAllocator *alloc_mgr,
                       rpc::frame::ObReqTransport *transport,
                       palf::ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;

  const int64_t tenant_id = MTL_ID();
  if (OB_FAIL(check_and_prepare_dir(base_dir))) {
    CLOG_LOG(WARN, "check_and_prepare_dir failed", K(ret), K(base_dir));
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "LogService init twice", K(ret));
  } else if (false == options.is_valid() || OB_ISNULL(base_dir) || OB_UNLIKELY(!self.is_valid())
      || OB_ISNULL(alloc_mgr) || OB_ISNULL(transport)
      || OB_ISNULL(log_block_pool)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(options), KP(base_dir), K(self),
             KP(alloc_mgr), KP(transport), KP(log_block_pool));
  } else if (OB_FAIL(PalfEnv::create_palf_env(options, base_dir, self, transport,
                                              alloc_mgr, log_block_pool, &monitor_, palf_env_))) {
    CLOG_LOG(WARN, "failed to create_palf_env", K(base_dir), K(ret));
  } else if (OB_ISNULL(palf_env_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "palf_env_ is NULL", K(ret));
  } else if (OB_FAIL(ls_adapter_.init())) {
    CLOG_LOG(ERROR, "failed to init ls_adapter", K(ret));
  } else if (OB_FAIL(apply_service_.init(palf_env_, &ls_adapter_))) {
    CLOG_LOG(WARN, "failed to init apply_service", K(ret));
  // } else if (OB_FAIL(replay_service_.init(palf_env_, &ls_adapter_, alloc_mgr))) {
  //   CLOG_LOG(WARN, "failed to init replay_service", K(ret));
  } else if (OB_FAIL(role_change_service_.init(&log_client_map_, &apply_service_))) {
    CLOG_LOG(WARN, "failed to init role_change_service_", K(ret));
  } else if (OB_FAIL(rpc_proxy_.init(transport))) {
    CLOG_LOG(WARN, "LogServiceRpcProxy init failed", K(ret));
  } else if (OB_FAIL(log_client_map_.init("Client", MTL_ID()))) {
    CLOG_LOG(WARN, "failed to init log_client_map", K(ret));
  } else {
    self_ = self;
    is_inited_ = true;
    FLOG_INFO("LogService init success", K(ret), K(base_dir), K(self), KP(transport), K(tenant_id));
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}


int LogService::create_palf_replica(const int64_t palf_id,
                                    const common::ObMemberList &member_list,
                                    const int64_t replica_num,
                                    const int64_t leader_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    share::ObLSID ls_id(palf_id);
    ObLogClient *log_client = OB_NEW(ObLogClient, "client");
    if (OB_FAIL(log_client->init(self_, palf_id, &rpc_proxy_, this))) {
      CLOG_LOG(ERROR, "init fail", K(ret), K(palf_id));
    } else if (OB_FAIL(log_client_map_.insert(ls_id, log_client))) {
      CLOG_LOG(ERROR, "insert fail", K(ret), K(palf_id));
    } else if (OB_FAIL(log_client->create_palf_replica(member_list, replica_num, leader_idx))) {
      CLOG_LOG(ERROR, "create_replica fail", K(ret), K(palf_id));
      log_client_map_.erase(ls_id);
    } else {
      CLOG_LOG(ERROR, "create_palf_replica successfully", K(ret), K(palf_id), K(member_list), K(replica_num), K(leader_idx));
    }
  }
  return ret;
}

int LogService::create_log_clients(const int64_t thread_num,
                                     const int64_t log_size,
                                     const int64_t palf_group_num,
                                     std::vector<common::ObAddr> leader_list)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < thread_num; i++) {
    const int64_t palf_id = (i % palf_group_num) + 1;
    if (OB_FAIL(clients_[i].init_and_create(i, log_size, &rpc_proxy_, self_, leader_list[palf_id-1], palf_id))) {
      CLOG_LOG(ERROR, "init_and_create fail", K(palf_id));
    }
  }

  for (int64_t i = 0; i < thread_num; i++) {
    clients_[i].join();
  }
  return ret;
}

palf::AccessMode LogService::get_palf_access_mode(const share::ObTenantRole &tenant_role)
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
    case share::ObTenantRole::CLONE_TENANT:
      mode = palf::AccessMode::RAW_WRITE;
      break;
    default:
      mode = palf::AccessMode::INVALID_ACCESS_MODE;
      break;
  }
  return mode;
}

int LogService::create_ls(const share::ObLSID &id,
                            const common::ObReplicaType &replica_type,
                            const share::ObTenantRole &tenant_role,
                            const palf::PalfBaseInfo &palf_base_info,
                            const bool allow_log_sync,
                            logservice::ObLogHandler &log_handler)
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
                                log_handler))) {
    CLOG_LOG(WARN, "create ls failed", K(ret), K(id), K(replica_type),
        K(tenant_role), K(palf_base_info));
  } else {
    FLOG_INFO("LogService create_ls success", K(ret), K(id), K(replica_type), K(tenant_role), K(palf_base_info),
        K(log_handler));
  }
  return ret;
}

int LogService::remove_ls(const ObLSID &id, logservice::ObLogHandler &log_handler)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_FAIL(apply_service_.remove_ls(id))) {
    CLOG_LOG(WARN, "failed to remove from apply_service", K(ret), K(id));
  // } else if (OB_FAIL(replay_service_.remove_ls(id))) {
  //   CLOG_LOG(WARN, "failed to remove from replay_service", K(ret), K(id));
    // NB: remove palf_handle lastly.
  } else {
    // NB: can not execute destroy, otherwise, each interface in log_handler or restore_handler
    // may return OB_NOT_INIT.
    // TODO by runlin: create_ls don't init logservice::ObLogHandler and ObLogRestoreHandler.
    //
    // In normal case(for gc), stop has been executed, this stop has no effect.
    // In abnormal case(create ls failed, need remove ls directlly), there is no possibility for dead lock.
    log_handler.stop();
    if (OB_FAIL(palf_env_->remove(id.id()))) {
      CLOG_LOG(WARN, "failed to remove from palf_env_", K(ret), K(id));
    } else {
      FLOG_INFO("LogService remove_ls success", K(ret), K(id));
    }
  }

  return ret;
}

int LogService::check_palf_exist(const ObLSID &id, bool &exist) const
{
  int ret = OB_SUCCESS;
  PalfHandle handle;
  exist = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LogService is not inited", K(ret), K(id));
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

int LogService::add_ls(const ObLSID &id, logservice::ObLogHandler &log_handler)
{
  int ret = OB_SUCCESS;
  PalfHandle palf_handle;
  PalfHandle &log_handler_palf_handle = log_handler.palf_handle_;
  PalfRoleChangeCb *rc_cb = &role_change_service_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_FAIL(palf_env_->open(id.id(), palf_handle))) {
    CLOG_LOG(WARN, "failed to get palf_handle", K(ret), K(id));
  } else if (OB_FAIL(apply_service_.add_ls(id))) {
    CLOG_LOG(WARN, "failed to add_ls for apply_service", K(ret), K(id));
  // } else if (OB_FAIL(replay_service_.add_ls(id))) {
  //   CLOG_LOG(WARN, "failed to add_ls for replay_service", K(ret), K(id));
  // } else if (OB_FAIL(log_handler.init(id.id(), self_, &apply_service_, &replay_service_,
  //         &role_change_service_, palf_handle, palf_env_, loc_cache_cb, &rpc_proxy_))) {
  //   CLOG_LOG(WARN, "logservice::ObLogHandler init failed", K(ret), K(id), KP(palf_env_), K(palf_handle));
  } else if (OB_FAIL(log_handler_palf_handle.register_role_change_cb(rc_cb))) {
    CLOG_LOG(WARN, "register_role_change_cb failed", K(ret));
  } else {
    FLOG_INFO("add_ls success", K(ret), K(id), KP(this));
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

int LogService::open_palf(const share::ObLSID &id,
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
    CLOG_LOG(TRACE, "LogService open_palf success", K(ret), K(id));
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

int LogService::get_palf_role(const share::ObLSID &id,
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

int LogService::get_palf_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = palf_env_->get_disk_usage(used_size_byte, total_size_byte);
  }
  return ret;
}

int LogService::get_palf_stable_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = palf_env_->get_stable_disk_usage(used_size_byte, total_size_byte);
  }
  return ret;
}

int LogService::update_palf_options_except_disk_usage_limit_size()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ObSpinLockGuard guard(update_palf_opts_lock_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!tenant_config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "tenant_config is not valid", K(ret), K(MTL_ID()));
  } else {
    PalfOptions palf_opts;
    common::ObCompressorType compressor_type = LZ4_COMPRESSOR;
    if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor_type(
                tenant_config->log_transport_compress_func, compressor_type))) {
      CLOG_LOG(ERROR, "log_transport_compress_func invalid.", K(ret));
    //需要获取log_disk_usage_limit_size
    } else if (OB_FAIL(palf_env_->get_options(palf_opts))) {
      CLOG_LOG(WARN, "palf get_options failed", K(ret));
    } else {
      palf_opts.disk_options_.log_disk_utilization_threshold_ = tenant_config->log_disk_utilization_threshold;
      palf_opts.disk_options_.log_disk_utilization_limit_threshold_ = tenant_config->log_disk_utilization_limit_threshold;
      palf_opts.disk_options_.log_disk_throttling_percentage_ = tenant_config->log_disk_throttling_percentage;
      palf_opts.disk_options_.log_disk_throttling_maximum_duration_ = tenant_config->log_disk_throttling_maximum_duration;
      palf_opts.compress_options_.enable_transport_compress_ = tenant_config->log_transport_compress_all;
      palf_opts.compress_options_.transport_compress_func_ = compressor_type;
      palf_opts.rebuild_replica_log_lag_threshold_ = tenant_config->_rebuild_replica_log_lag_threshold;
      palf_opts.disk_options_.log_writer_parallelism_ = tenant_config->_log_writer_parallelism;
      palf_opts.enable_log_cache_ = tenant_config->_enable_log_cache;
      if (OB_FAIL(palf_env_->update_options(palf_opts))) {
        CLOG_LOG(WARN, "palf update_options failed", K(MTL_ID()), K(ret), K(palf_opts));
      } else {
        CLOG_LOG(INFO, "palf update_options success", K(MTL_ID()), K(ret), K(palf_opts));
      }
    }
  }
  return ret;
}

//log_disk_usage_limit_size无法主动感知,只能通过上层触发时传入
int LogService::update_log_disk_usage_limit_size(const int64_t log_disk_usage_limit_size)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(update_palf_opts_lock_);
  PalfOptions palf_opts;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(palf_env_->get_options(palf_opts))) {
    CLOG_LOG(WARN, "palf get_options failed", K(ret));
  } else if (FALSE_IT(palf_opts.disk_options_.log_disk_usage_limit_size_ = log_disk_usage_limit_size)) {
  } else if (OB_FAIL(palf_env_->update_options(palf_opts))) {
    CLOG_LOG(WARN, "palf update_options failed", K(ret), K(log_disk_usage_limit_size));
  } else {
    CLOG_LOG(INFO, "update_log_disk_usage_limit_size success", K(log_disk_usage_limit_size), K(MTL_ID()));
  }
  return ret;
}

int LogService::get_palf_options(palf::PalfOptions &opts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = palf_env_->get_options(opts);
  }
  return ret;
}

int LogService::iterate_palf(const ObFunction<int(const PalfHandle&)> &func)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = palf_env_->for_each(func);
  }
  return ret;
}

int LogService::create_ls_(const share::ObLSID &id,
                             const common::ObReplicaType &replica_type,
                             const share::ObTenantRole &tenant_role,
                             const palf::PalfBaseInfo &palf_base_info,
                             const bool allow_log_sync,
                             logservice::ObLogHandler &log_handler)
{
  int ret = OB_SUCCESS;
  PalfHandle palf_handle;
  PalfRoleChangeCb *rc_cb = &role_change_service_;
  const bool is_arb_replica = (replica_type == REPLICA_TYPE_ARBITRATION);
  PalfHandle &log_handler_palf_handle = log_handler.palf_handle_;
  if (false == id.is_valid() ||
      INVALID_TENANT_ROLE == tenant_role ||
      false == palf_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(id), K(id), K(tenant_role), K(palf_base_info));
  } else if (!is_arb_replica &&
      OB_FAIL(palf_env_->create(id.id(), get_palf_access_mode(tenant_role), palf_base_info, palf_handle))) {
    CLOG_LOG(WARN, "failed to get palf_handle", K(ret), K(id), K(replica_type));
  } else if (false == allow_log_sync && OB_FAIL(palf_handle.disable_sync())) {
    CLOG_LOG(WARN, "failed to disable_sync", K(ret), K(id));
  } else if (OB_FAIL(apply_service_.add_ls(id))) {
    CLOG_LOG(WARN, "failed to add_ls for apply engine", K(ret), K(id));
  // } else if (OB_FAIL(replay_service_.add_ls(id))) {
  //   CLOG_LOG(WARN, "failed to add_ls", K(ret), K(id));
  } else if (OB_FAIL(log_handler.init(id.id(), self_, &apply_service_, &replay_service_,
          NULL, palf_handle, palf_env_, &location_adapter_, &log_service_rpc_proxy_))) {
    CLOG_LOG(WARN, "logservice::ObLogHandler init failed", K(ret), KP(palf_env_), K(palf_handle));
  } else if (OB_FAIL(log_handler_palf_handle.register_role_change_cb(rc_cb))) {
    CLOG_LOG(WARN, "register_role_change_cb failed", K(ret), K(id));
  } else {
    CLOG_LOG(INFO, "LogService create_ls success", K(ret), K(id), K(log_handler));
  }
  if (OB_FAIL(ret)) {
    if (true == palf_handle.is_valid() && false == log_handler.is_valid()) {
      palf_env_->close(palf_handle);
    }
  }
  return ret;
}

int LogService::get_io_start_time(int64_t &last_working_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (OB_FAIL(palf_env_->get_io_start_time(last_working_time))) {
    CLOG_LOG(WARN, "palf_env get_io_start_time failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int LogService::check_disk_space_enough(bool &is_disk_enough)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else {
    is_disk_enough = palf_env_->check_disk_space_enough();
  }
  return ret;
}

}//end of namespace logservice
}//end of namespace oceanbase
