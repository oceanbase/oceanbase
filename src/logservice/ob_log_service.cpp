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
#include "ob_server_log_block_mgr.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "observer/ob_srv_network_frame.h"
#include "storage/ob_file_system_router.h"
#include "logservice/ob_net_keepalive_adapter.h"            // ObNetKeepAliveAdapter
#include "share/resource_manager/ob_resource_manager.h"       // ObResourceManager
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "logservice/libpalf/libpalf_env.h"
#endif

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
  enable_logservice_(false),
  self_(),
  palf_env_(NULL),
  net_keepalive_adapter_(NULL),
  alloc_mgr_(NULL),
  apply_service_(),
  replay_service_(),
  role_change_service_(),
  location_adapter_(),
  ls_adapter_(),
  rpc_proxy_(),
  reporter_(),
#ifdef OB_BUILD_ARBITRATION
  arb_service_(),
#endif
  restore_service_(),
  flashback_service_(),
  monitor_(),
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  shared_gc_(),
  configured_log_disk_size_(0),
#endif
  locality_adapter_()
{}

ObLogService::~ObLogService()
{
  destroy();
}

int ObLogService::mtl_init(ObLogService* &logservice)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  const int64_t tenant_id = MTL_ID();
  observer::ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  //log_disk_usage_limit_size无法主动从配置项获取, 需要在mtl初始化时作为入参传入
  const palf::PalfOptions &palf_options = MTL_INIT_CTX()->palf_options_;
  const char *tenant_clog_dir = MTL_INIT_CTX()->tenant_clog_dir_;
  const char *clog_dir = OB_FILE_SYSTEM_ROUTER.get_clog_dir();
  ObLocationService *location_service = GCTX.location_service_;
  observer::ObIMetaReport *reporter = GCTX.ob_service_;
  ObServerLogBlockMgr *log_block_mgr = GCTX.log_block_mgr_;
  common::ObILogAllocator *alloc_mgr = NULL;
  common::ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;
  obrpc::ObNetKeepAlive *net_keepalive = &(obrpc::ObNetKeepAlive::get_instance());
  ObNetKeepAliveAdapter *net_keepalive_adapter = NULL;
  storage::ObLocalityManager *locality_manager = GCTX.locality_manager_;
  if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr))) {
    CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(ret));
  } else if (OB_ISNULL(net_keepalive_adapter = MTL_NEW(ObNetKeepAliveAdapter, "logservice", net_keepalive))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc memory failed", KR(ret), KP(net_keepalive_adapter));
  } else if (OB_FAIL(logservice->init(palf_options,
                                      tenant_clog_dir,
                                      self,
                                      alloc_mgr,
                                      net_frame->get_req_transport(),
                                      GCTX.batch_rpc_,
                                      MTL(ObLSService*),
                                      location_service,
                                      reporter,
                                      log_block_mgr,
                                      mysql_proxy,
                                      net_keepalive_adapter,
                                      locality_manager))) {
    CLOG_LOG(ERROR, "init ObLogService failed", K(ret), K(tenant_clog_dir));
  } else if (
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    !GCONF.enable_logservice &&
#endif
    OB_FAIL(FileDirectoryUtils::fsync_dir(clog_dir))) {
    CLOG_LOG(ERROR, "fsync_dir failed", K(ret), K(clog_dir));
  } else {
    CLOG_LOG(INFO, "ObLogService mtl_init success");
  }
  if (OB_FAIL(ret) && NULL != net_keepalive_adapter) {
    MTL_DELETE(ObNetKeepAliveAdapter, "logservice", net_keepalive_adapter);
  }
  return ret;
}

void ObLogService::mtl_destroy(ObLogService* &logservice)
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

int ObLogService::start()
{
  int ret = OB_SUCCESS;
  palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogService not init", K(ret));
  } else if (!enable_logservice_ && OB_FAIL(palf_env->start())) {
    PALF_LOG(WARN, "start palf env failed", K(ret));
  } else if (OB_FAIL(apply_service_.start())) {
    CLOG_LOG(WARN, "failed to start apply_service_", K(ret));
  } else if (OB_FAIL(replay_service_.start())) {
    CLOG_LOG(WARN, "failed to start replay_service_", K(ret));
  } else if (OB_FAIL(role_change_service_.start())) {
    CLOG_LOG(WARN, "failed to start role_change_service_", K(ret));
  } else if (OB_FAIL(cdc_service_.start())) {
    CLOG_LOG(WARN, "failed to start cdc_service_", K(ret));
  } else if (OB_FAIL(restore_service_.start())) {
    CLOG_LOG(WARN, "failed to start restore_service_", K(ret));
#ifdef OB_BUILD_ARBITRATION
  } else if (!enable_logservice_ && OB_FAIL(arb_service_.start())) {
    CLOG_LOG(WARN, "failed to start arb_service_", K(ret));
#endif
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  } else if (enable_logservice_ && OB_FAIL(shared_gc_.start())) {
    CLOG_LOG(WARN, "failed to start shared garbage collector");
#endif
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
#ifdef OB_BUILD_ARBITRATION
  (void)arb_service_.stop();
#endif
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  if (enable_logservice_) {
    shared_gc_.stop();
  }
#endif
  FLOG_INFO("ObLogService is stopped");
}

void ObLogService::wait()
{
  apply_service_.wait();
  replay_service_.wait();
  role_change_service_.wait();
  cdc_service_.wait();
  restore_service_.wait();
#ifdef OB_BUILD_ARBITRATION
  arb_service_.wait();
#endif
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  if (enable_logservice_) {
    shared_gc_.wait();
  }
#endif
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
#ifdef OB_BUILD_ARBITRATION
  arb_service_.destroy();
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  if (enable_logservice_) {
    shared_gc_.stop();
  }
  configured_log_disk_size_ = 0;
#endif
  flashback_service_.destroy();
  if (NULL != palf_env_) {
    ipalf::destroy_palf_env(palf_env_);
  }
  cdc_service_.destroy();
  if (NULL != net_keepalive_adapter_) {
    MTL_DELETE(IObNetKeepAliveAdapter, "logservice", net_keepalive_adapter_);
    net_keepalive_adapter_ = NULL;
  }
  alloc_mgr_ = NULL;
  locality_adapter_.destroy();
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

int ObLogService::create_palf_env_(const PalfOptions &options,
                                   const char *base_dir,
                                   const common::ObAddr &self,
                                   common::ObILogAllocator *alloc_mgr,
                                   rpc::frame::ObReqTransport *transport,
                                   obrpc::ObBatchRpc *batch_rpc,
                                   palf::ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  if (!enable_logservice_) {
#else
  if (true) {
#endif
    CLOG_LOG(INFO, "init ob log service without enable_logservice");
    // build palf env
    ipalf::PalfEnvCreateParams palf_params = ipalf::PalfEnvCreateParams {
      .options_ = &options,
      .base_dir_ = base_dir,
      .self_ = &self,
      .transport_ = transport,
      .batch_rpc_ = batch_rpc,
      .log_alloc_mgr_ = alloc_mgr,
      .log_block_pool_ = log_block_pool,
      .monitor_ = &monitor_,
      .log_local_device_ = LOG_IO_DEVICE_WRAPPER.get_local_device(),
      .resource_manager_ = &G_RES_MGR,
      .io_manager_ = &OB_IO_MANAGER,
    };
    palf::PalfEnv *palf_env = NULL;
    // whether tenant_clog_dir is null is checked in check_and_prepare_dir
    if (OB_FAIL(check_and_prepare_dir(base_dir))) {
      CLOG_LOG(WARN, "check_and_prepare_dir failed", K(ret), K(base_dir));
    } else if (false == options.is_valid() || OB_ISNULL(base_dir) || OB_UNLIKELY(!self.is_valid())
               || OB_ISNULL(alloc_mgr) || OB_ISNULL(transport) || OB_ISNULL(batch_rpc) ||OB_ISNULL(log_block_pool)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(ret), K(options), KP(base_dir), K(self),
              KP(alloc_mgr), KP(transport), KP(batch_rpc), KP(log_block_pool));
    } else if (OB_FAIL(ipalf::create_palf_env(&palf_params, palf_env))) {
      CLOG_LOG(WARN, "failed to create_palf_env", K(base_dir), K(ret));
    } else if (OB_ISNULL(palf_env)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "palf_env_ is NULL", K(ret));
    } else {
      palf_env_ = palf_env;
    }
  }
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  else {
    CLOG_LOG(INFO, "init ob log service with enable_logservice");
    // build libpalf env
    ipalf::LibPalfEnvCreateParams libpalf_params = ipalf::LibPalfEnvCreateParams {
      .monitor_ = &monitor_
    };
    libpalf::LibPalfEnv *libpalf_env = NULL;
    if (OB_FAIL(ipalf::create_palf_env(&libpalf_params, libpalf_env))) {
      CLOG_LOG(WARN, "failed to create_palf_env", K(ret));
    } else if (OB_ISNULL(libpalf_env)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "palf_env_ is NULL", K(ret));
    } else {
      configured_log_disk_size_ = options.disk_options_.log_disk_usage_limit_size_;
      palf_env_ = libpalf_env;
    }
  }
#endif
  return ret;
}

int ObLogService::init(const PalfOptions &options,
                       const char *base_dir,
                       const common::ObAddr &self,
                       common::ObILogAllocator *alloc_mgr,
                       rpc::frame::ObReqTransport *transport,
                       obrpc::ObBatchRpc *batch_rpc,
                       ObLSService *ls_service,
                       ObLocationService *location_service,
                       observer::ObIMetaReport *reporter,
                       palf::ILogBlockPool *log_block_pool,
                       common::ObMySQLProxy *sql_proxy,
                       IObNetKeepAliveAdapter *net_keepalive_adapter,
                       storage::ObLocalityManager *locality_manager)
{
  int ret = OB_SUCCESS;

  const int64_t tenant_id = MTL_ID();
  palf::PalfEnv *palf_env = NULL;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogService init twice", K(ret));
  } else if (OB_ISNULL(ls_service) || OB_ISNULL(location_service) || OB_ISNULL(reporter)
             || OB_ISNULL(sql_proxy) || OB_ISNULL(net_keepalive_adapter) || OB_ISNULL(locality_manager)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(ls_service), KP(location_service), KP(reporter),
            KP(sql_proxy), KP(net_keepalive_adapter), KP(locality_manager));
  } else if (FALSE_IT(enable_logservice_ = GCONF.enable_logservice)) {
    // do nothing
  } else if (OB_FAIL(create_palf_env_(options, base_dir, self, alloc_mgr, transport, batch_rpc, log_block_pool))) {
    CLOG_LOG(ERROR, "failed to create palf env", K(ret));
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
#ifdef OB_BUILD_ARBITRATION
  } else if (!enable_logservice_ &&
             (palf_env = static_cast<palf::PalfEnv*>(palf_env_)) &&
             OB_FAIL(arb_service_.init(self, palf_env, &rpc_proxy_, net_keepalive_adapter, &monitor_, &location_adapter_))) {
    CLOG_LOG(WARN, "failed to init arb_service_", K(ret), K(self), KP(palf_env), KP(palf_env_));
#endif
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  } else if (enable_logservice_ && (OB_FAIL(shared_gc_.init(static_cast<libpalf::LibPalfEnv*>(palf_env_), sql_proxy)))) {
    CLOG_LOG(WARN, "failed to init shared garbage collector", KP(palf_env_), KP(sql_proxy));
#endif
  } else if (OB_FAIL(flashback_service_.init(self, &location_adapter_, &rpc_proxy_, sql_proxy))) {
    CLOG_LOG(WARN, "failed to init flashback_service_", K(ret));
  } else if (OB_FAIL(locality_adapter_.init(locality_manager))) {
    CLOG_LOG(WARN, "failed to init locality_adapter_", K(ret));
  } else {
    net_keepalive_adapter_ = net_keepalive_adapter;
    alloc_mgr_ = alloc_mgr;
    self_ = self;
    is_inited_ = true;
    FLOG_INFO("ObLogService init success", K(ret), K(base_dir), K(self), KP(transport), KP(batch_rpc),
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
    case share::ObTenantRole::CLONE_TENANT:
      mode = palf::AccessMode::RAW_WRITE;
      break;
    default:
      mode = palf::AccessMode::INVALID_ACCESS_MODE;
      break;
  }
  return mode;
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
  } else {
    // NB: can not execute destroy, otherwise, each interface in log_handler or restore_handler
    // may return OB_NOT_INIT.
    // TODO by runlin: create_ls don't init ObLogHandler and ObLogRestoreHandler.
    //
    // In normal case(for gc), stop has been executed, this stop has no effect.
    // In abnormal case(create ls failed, need remove ls directlly), there is no possibility for dead lock.
    log_handler.stop();
    restore_handler.stop();
    if (!enable_logservice_ && OB_FAIL(palf_env_->remove(id.id()))) {
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
  ipalf::IPalfHandle *palf_handle = NULL;
  exist = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogService is not inited", K(ret), K(id));
  } else if (OB_FAIL(palf_env_->open(id.id(), palf_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      exist = false;
    } else {
      CLOG_LOG(WARN, "open palf failed", K(ret), K(id));
    }
  }

  if (OB_NOT_NULL(palf_handle) && true == palf_handle->is_valid()) {
    palf_env_->close(palf_handle);
  }
  return ret;
}

int ObLogService::add_ls(const ObLSID &id,
                         ObLogHandler &log_handler,
                         ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  ipalf::IPalfHandle *&log_handler_palf_handle = log_handler.palf_handle_;
  PalfRoleChangeCb *rc_cb = &role_change_service_;
  PalfLocationCacheCb *loc_cache_cb = &location_adapter_;
  ipalf::IPalfHandle *palf_handle = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_FAIL(palf_env_->load(id.id(), palf_handle))) {
    CLOG_LOG(WARN, "failed to load palf handle", K(id));
  } else if (OB_FAIL(apply_service_.add_ls(id))) {
    CLOG_LOG(WARN, "failed to add_ls for apply_service", K(ret), K(id));
  } else if (OB_FAIL(replay_service_.add_ls(id))) {
    CLOG_LOG(WARN, "failed to add_ls for replay_service", K(ret), K(id));
  } else if (OB_FAIL(log_handler.init(id.id(), self_, &apply_service_, &replay_service_,
          &role_change_service_, palf_env_, loc_cache_cb, &rpc_proxy_, alloc_mgr_))) {
    CLOG_LOG(WARN, "ObLogHandler init failed", K(ret), K(id), KP(palf_env_));
  } else if (OB_ISNULL(log_handler_palf_handle)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ObLogHandler init failed, log_handler_palf_handle is nullptr");
  } else if (OB_FAIL(restore_handler.init(id.id(), palf_env_))) {
    CLOG_LOG(WARN, "ObLogRestoreHandler init failed", K(ret), K(id), KP(palf_env_));
  } else if (OB_FAIL(log_handler_palf_handle->register_role_change_cb(rc_cb))) {
    CLOG_LOG(WARN, "register_role_change_cb failed", K(ret));
  } else if (OB_FAIL(log_handler_palf_handle->set_location_cache_cb(loc_cache_cb))) {
    CLOG_LOG(WARN, "set_location_cache_cb failed", K(ret), K(id));
  } else if (!enable_logservice_ && OB_FAIL(do_set_shared_nothing_cbs_(id, log_handler_palf_handle))) {
    CLOG_LOG(WARN, "do_set_shared_nothing_cbs_ failed", K(ret), K(id));
  } else {
    FLOG_INFO("add_ls success", K(ret), K(id), KP(this));
  }
  if (OB_NOT_NULL(palf_handle) && true == palf_handle->is_valid()) {
    palf_env_->close(palf_handle);
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LS_NOT_EXIST;
  }
  return ret;
}

int ObLogService::do_set_shared_nothing_cbs_(const ObLSID &id, ipalf::IPalfHandle *palf_handle)
{
  int ret = OB_SUCCESS;
  palf::PalfHandle *sn_palf_handle = static_cast<palf::PalfHandle*>(palf_handle);
  PalfLocalityInfoCb *locality_cb = &locality_adapter_;
  if (enable_logservice_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "enable_logservice_ is true", K(ret));
  } else if (OB_ISNULL(sn_palf_handle)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "palf_handle is null", K(ret));
  } else if (OB_FAIL(sn_palf_handle->set_locality_cb(locality_cb))) {
    CLOG_LOG(WARN, "set_locality_cb failed", K(ret), K(id));
  } else {
    CLOG_LOG(INFO, "set_shared_nothing_cbs_ success", K(id));
  }
  return ret;
}

int ObLogService::open_palf(const share::ObLSID &id,
                            palf::PalfHandleGuard &palf_handle_guard)
{
  int ret = OB_SUCCESS;
  ipalf::IPalfHandle *palf_handle = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret), K(id));
  } else if (OB_FAIL(palf_env_->open(id.id(), palf_handle))) {
    CLOG_LOG(WARN, "failed to get palf_handle", K(ret), K(id));
  } else if (OB_ISNULL(palf_handle)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error, ipalf handle should be valid but be null", K(ret), K(id));
  } else if (FALSE_IT(palf_handle_guard.set(palf_handle, palf_env_))) {
  } else {
    CLOG_LOG(TRACE, "ObLogService open_palf success", K(ret), K(id));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(palf_handle) && true == palf_handle->is_valid()) {
      palf_env_->close(palf_handle);
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LS_NOT_EXIST;
  }
  return ret;
}

int ObLogService::update_replayable_point(const SCN &replayable_point)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (OB_FAIL(replay_service_.update_replayable_point(replayable_point))) {
    CLOG_LOG(WARN, "update_replayable_point failed", K(ret), K(replayable_point));
    // should be removed in version 4.2.0.0
  } else if (OB_FAIL(palf_env_->update_replayable_point(replayable_point))) {
    CLOG_LOG(WARN, "update_replayable_point failed", K(replayable_point));
  }
  return ret;
}

int ObLogService::get_replayable_point(SCN &replayable_point)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (OB_FAIL(replay_service_.get_replayable_point(replayable_point))) {
    CLOG_LOG(WARN, "get_replayable_point failed", K(ret), K(replayable_point));
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
  } else if (enable_logservice_) {
    used_size_byte = 0;
    total_size_byte = 0;
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(ERROR, "in logservice mode, get_palf_disk_usage is meanless", K(ret));
  } else {
    palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
    ret = palf_env->get_disk_usage(used_size_byte, total_size_byte);
  }
  return ret;
}

int ObLogService::get_palf_stable_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte)
{
  int ret = OB_SUCCESS;
  palf::PalfEnv *palf_env = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  } else if (enable_logservice_) {
    used_size_byte = 0;
    total_size_byte = configured_log_disk_size_;
#endif
  } else {
    palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
    ret = palf_env->get_stable_disk_usage(used_size_byte, total_size_byte);
  }
  return ret;
}

int ObLogService::update_palf_options_except_disk_usage_limit_size()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ObSpinLockGuard guard(update_palf_opts_lock_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!tenant_config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "tenant_config is not valid", K(ret), K(MTL_ID()));
  } else if (enable_logservice_) {
    // in logservice mode, there is no palf options now
    CLOG_LOG(INFO, "in logservice mode, skip palf update_options", K(MTL_ID()), K(ret));
  } else {
    palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
    PalfOptions palf_opts;
    common::ObCompressorType compressor_type = LZ4_COMPRESSOR;
    if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor_type(
                tenant_config->log_transport_compress_func, compressor_type))) {
      CLOG_LOG(ERROR, "log_transport_compress_func invalid.", K(ret));
      //需要获取log_disk_usage_limit_size
    } else if (OB_FAIL(palf_env->get_options(palf_opts))) {
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
      if (OB_FAIL(palf_env->update_options(palf_opts))) {
        CLOG_LOG(WARN, "palf update_options failed", K(MTL_ID()), K(ret), K(palf_opts));
      } else {
        CLOG_LOG(INFO, "palf update_options success", K(MTL_ID()), K(ret), K(palf_opts));
      }
    }
  }
  return ret;
}

//log_disk_usage_limit_size无法主动感知,只能通过上层触发时传入
int ObLogService::update_log_disk_usage_limit_size(const int64_t log_disk_usage_limit_size)
{
  int ret = OB_SUCCESS;
  palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
  ObSpinLockGuard guard(update_palf_opts_lock_);
  PalfOptions palf_opts;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  } else if (enable_logservice_) {
    configured_log_disk_size_ = log_disk_usage_limit_size;
    // in logservice mode, disk usage limit size is meanless
    CLOG_LOG(INFO, "in logservice mode, skip update_log_disk_usage_limit_size", K(MTL_ID()));
#endif
  } else if (OB_FAIL(palf_env->get_options(palf_opts))) {
    CLOG_LOG(WARN, "palf get_options failed", K(ret));
  } else if (FALSE_IT(palf_opts.disk_options_.log_disk_usage_limit_size_ = log_disk_usage_limit_size)) {
  } else if (OB_FAIL(palf_env->update_options(palf_opts))) {
    CLOG_LOG(WARN, "palf update_options failed", K(ret), K(log_disk_usage_limit_size));
  } else {
    CLOG_LOG(INFO, "update_log_disk_usage_limit_size success", K(log_disk_usage_limit_size), K(MTL_ID()));
  }
  return ret;
}

int ObLogService::get_palf_options(palf::PalfOptions &opts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (enable_logservice_) {
    // make sure disk options is not valid
    opts.disk_options_.reset();
  } else {
    palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
    ret = palf_env->get_options(opts);
  }
  return ret;
}

int ObLogService::get_palf_disk_options(palf::PalfDiskOptions &palf_disk_options)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (enable_logservice_) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(ERROR, "palf disk options is not existed in logservice mode", KR(ret));
  } else {
    palf::PalfOptions opts;
    palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
    ret = palf_env->get_options(opts);
    palf_disk_options = opts.disk_options_;
  }
  return ret;
}

int ObLogService::iterate_palf(const ObFunction<int(const ipalf::IPalfHandle&)> &func)
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
  ipalf::IPalfHandle *palf_handle = NULL;
  PalfRoleChangeCb *rc_cb = &role_change_service_;
  PalfLocationCacheCb *loc_cache_cb = &location_adapter_;
  const bool is_arb_replica = (replica_type == REPLICA_TYPE_ARBITRATION);
  ipalf::IPalfHandle *&log_handler_palf_handle = log_handler.palf_handle_;
  bool palf_exist = false;
  if (false == id.is_valid() ||
      INVALID_TENANT_ROLE == tenant_role ||
      false == palf_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(id), K(tenant_role), K(palf_base_info));
  } else if (OB_FAIL(check_palf_exist(id, palf_exist))) {
    CLOG_LOG(WARN, "check_palf_exist failed", K(ret), K(id), K(tenant_role), K(palf_base_info));
  } else if (palf_exist) {
    ret = OB_ENTRY_EXIST;
    CLOG_LOG(WARN, "palf has exist", K(ret), K(id), K(tenant_role), K(palf_base_info));
  } else {
    if (!is_arb_replica &&
        OB_FAIL(palf_env_->create(id.id(), get_palf_access_mode(tenant_role), palf_base_info, palf_handle))) {
      CLOG_LOG(WARN, "failed to get palf_handle", K(ret), K(id), K(replica_type));
    } else if (false == allow_log_sync && OB_FAIL(palf_handle->disable_sync())) {
      CLOG_LOG(WARN, "failed to disable_sync", K(ret), K(id));
    } else if (OB_FAIL(apply_service_.add_ls(id))) {
      CLOG_LOG(WARN, "failed to add_ls for apply engine", K(ret), K(id));
    } else if (OB_FAIL(replay_service_.add_ls(id))) {
      CLOG_LOG(WARN, "failed to add_ls", K(ret), K(id));
    } else if (OB_FAIL(log_handler.init(id.id(), self_, &apply_service_, &replay_service_,
          &role_change_service_, palf_env_, loc_cache_cb, &rpc_proxy_, alloc_mgr_))) {
      CLOG_LOG(WARN, "ObLogHandler init failed", K(ret), KP(palf_env_), K(palf_handle));
    } else if (OB_ISNULL(log_handler_palf_handle)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "ObLogHandler init failed, log_handler_palf_handle is nullptr");
    } else if (OB_FAIL(restore_handler.init(id.id(), palf_env_))) {
      CLOG_LOG(WARN, "ObLogRestoreHandler init failed", K(ret), K(id), KP(palf_env_));
    } else if (OB_FAIL(log_handler_palf_handle->register_role_change_cb(rc_cb))) {
      CLOG_LOG(WARN, "register_role_change_cb failed", K(ret), K(id));
    } else if (OB_FAIL(log_handler_palf_handle->set_location_cache_cb(loc_cache_cb))) {
      CLOG_LOG(WARN, "set_location_cache_cb failed", K(ret), K(id));
    } else if (!enable_logservice_ && OB_FAIL(do_set_shared_nothing_cbs_(id, log_handler_palf_handle))) {
      CLOG_LOG(WARN, "do_set_shared_nothing_cbs_ failed", K(ret), K(id));
    } else {
      CLOG_LOG(INFO, "ObLogService create_ls success", K(ret), K(id), K(log_handler));
    }
    if (OB_NOT_NULL(palf_handle) && palf_handle->is_valid() && nullptr != palf_env_) {
      palf_env_->close(palf_handle);
    }
    if (OB_FAIL(ret)) {
      CLOG_LOG(ERROR, "create_ls failed!!!", KR(ret), K(id));
      restore_handler.destroy();
      replay_service_.remove_ls(id);
      apply_service_.remove_ls(id);
      log_handler.destroy();
      if (OB_NOT_NULL(palf_handle)) {
        palf_env_->close(palf_handle);
      }
      if (!enable_logservice_) {
        palf_env_->remove(id.id());
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObLogService::flashback(const uint64_t tenant_id,
                            const SCN &flashback_scn,
                            const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) ||
      !flashback_scn.is_valid() ||
      timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(flashback_scn), K(timeout_us));
  } else if (OB_FAIL(flashback_service_.flashback(tenant_id, flashback_scn, timeout_us))) {
    CLOG_LOG(WARN, "flashback failed", K(ret), K(tenant_id), K(flashback_scn), K(timeout_us));
  } else {
    CLOG_LOG(INFO, "flashback success", K(ret), K(tenant_id), K(flashback_scn), K(timeout_us));
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

#ifdef OB_BUILD_ARBITRATION
int ObLogService::diagnose_arb_srv(const share::ObLSID &id,
                                   LogArbSrvDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (enable_logservice_) {
    // skip
  } else if (OB_FAIL(arb_service_.diagnose(id, diagnose_info))) {
    CLOG_LOG(WARN, "arb_service_ diagnose failed", K(ret), K(id));
  } else {
    // do nothing
  }
  return ret;
}
#endif

int ObLogService::get_io_start_time(int64_t &last_working_time)
{
  int ret = OB_SUCCESS;
  palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (enable_logservice_) {
    last_working_time = OB_INVALID_TIMESTAMP;
  } else if (OB_FAIL(palf_env->get_io_start_time(last_working_time))) {
    CLOG_LOG(WARN, "palf_env get_io_start_time failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogService::check_disk_space_enough(bool &is_disk_enough)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (enable_logservice_) {
    is_disk_enough = true;
  } else {
    palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
    is_disk_enough = palf_env->check_disk_space_enough();
  }
  return ret;
}

int ObLogService::check_need_do_checkpoint(bool &need_do_checkpoint)
{
  int ret = OB_SUCCESS;
  palf::PalfEnv *palf_env = static_cast<palf::PalfEnv*>(palf_env_);
  need_do_checkpoint = false;
  int64_t total_size = 0;
  int64_t used_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log_service is not inited", K(ret));
  } else if (enable_logservice_) {
    need_do_checkpoint = false;
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "in logservice mode, " \
      "checking palf status to decide checkpoint flushing is not reasonable", K(ret));
  } else if (OB_FAIL(palf_env->get_disk_usage(used_size, total_size))) {
    CLOG_LOG(WARN, "get_disk_usage failed", K(ret));
  } else {
    const int64_t CHECKPOINT_PERCENTAGE = 30;
    ObLSService *ls_service = MTL(ObLSService*);
    ObSharedGuard<ObLSIterator> iterator;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ObLSService is nullptr", KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls_iter(iterator, ObLSGetMod::LOG_MOD))) {
      CLOG_LOG(WARN, "get_ls_iter failed", KP(ls_service));
    } else {
      ObLS *ls = NULL;
      GetUnrecycableLogDiskSizeFunctor functor;
      const int64_t &unrecycable_log_disk_size = functor.unrecycable_log_disk_size_;
      while (OB_SUCC(iterator->get_next(ls))) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(functor(ls))) {
          CLOG_LOG(WARN, "get unrecycable_log_disk_size failed", KR(tmp_ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        need_do_checkpoint = (unrecycable_log_disk_size * 100 >= total_size * CHECKPOINT_PERCENTAGE);
        CLOG_LOG(TRACE, "check_need_do_checkpoint", K(unrecycable_log_disk_size), K(total_size), K(need_do_checkpoint));
      }
    }
  }
  return ret;
}

int ObLogService::GetUnrecycableLogDiskSizeFunctor::operator()(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObLogHandler *log_handler = NULL;
  LSN end_lsn;
  LSN base_lsn;
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error, ObLS is nullptr", KP(ls));
  } else if (FALSE_IT(log_handler = ls->get_log_handler())) {
  } else if (OB_ISNULL(log_handler)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error, ObLogHandler is nullptr", KP(ls));
  } else if (FALSE_IT(base_lsn = ls->get_clog_base_lsn())) {
  } else if (OB_FAIL(log_handler->get_end_lsn(end_lsn))) {
    CLOG_LOG(WARN, "get_end_lsn failed", KP(ls), K(base_lsn));
  } else if (end_lsn < base_lsn) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "end_lsn is smaller than base_lsn", K(lbt()), K(end_lsn), K(base_lsn));
  } else {
    unrecycable_log_disk_size_ += (end_lsn - base_lsn);
  }
  return ret;
}

}//end of namespace logservice
}//end of namespace oceanbase
