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

#include "ob_log_restore_service.h"
#include "logservice/ob_log_service.h"        // ObLogService
#include "lib/ash/ob_active_session_guard.h"

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::palf;
ObLogRestoreService::ObLogRestoreService() :
  inited_(false),
  ls_svr_(NULL),
  proxy_(),
  location_adaptor_(),
  archive_driver_(),
  net_driver_(),
  fetch_log_impl_(),
  fetch_log_worker_(),
  writer_(),
  error_reporter_(),
  restore_source_(),
  query_restore_source_ts_(OB_INVALID_TIMESTAMP),
  schedule_fetch_log_ts_(OB_INVALID_TIMESTAMP),
  common_event_schedule_ts_(OB_INVALID_TIMESTAMP),
  allocator_(),
  scheduler_(),
  cond_(),
  lock_(),
  init_transport_(NULL),
  init_ls_svr_(NULL),
  init_log_service_(NULL)
{}

ObLogRestoreService::~ObLogRestoreService()
{
  destroy();
}

int ObLogRestoreService::init(rpc::frame::ObReqTransport *transport,
    ObLSService *ls_svr,
    ObLogService *log_service)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLogRestoreService init twice", K(ret), K(inited_));
  } else if (OB_ISNULL(transport) || OB_ISNULL(ls_svr) || OB_ISNULL(log_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(transport), K(ls_svr), K(log_service));
  } else if (OB_FAIL(proxy_.init(transport))) {
    LOG_WARN("proxy_ init failed", K(ret));
  } else if (OB_FAIL(location_adaptor_.init(tenant_id, ls_svr))) {
    LOG_WARN("location_adaptor_ init failed", K(ret));
  } else if (OB_FAIL(archive_driver_.init(tenant_id, ls_svr, log_service, &fetch_log_worker_))) {
    LOG_WARN("archive_driver_ init failed");
  } else if (OB_FAIL(net_driver_.init(tenant_id, ls_svr, log_service))) {
    LOG_WARN("net_driver_ init failed");
  } else if (OB_FAIL(fetch_log_impl_.init(tenant_id, &archive_driver_, &net_driver_))) {
    LOG_WARN("fetch_log_impl_ init failed", K(ret));
  } else if (OB_FAIL(fetch_log_worker_.init(tenant_id, &allocator_, this, ls_svr, &writer_))) {
    LOG_WARN("fetch_log_worker_ init failed", K(ret));
  } else if (OB_FAIL(writer_.init(tenant_id, ls_svr, this, &fetch_log_worker_))) {
    LOG_WARN("remote_log_writer init failed");
  } else if (OB_FAIL(error_reporter_.init(tenant_id, ls_svr))) {
    LOG_WARN("error_reporter_ init failed", K(ret));
  } else if (OB_FAIL(allocator_.init(tenant_id))) {
    LOG_WARN("allocator_ init failed", K(ret));
  } else if (OB_FAIL(scheduler_.init(tenant_id, &allocator_, &fetch_log_worker_))) {
    LOG_WARN("scheduler_ init failed", K(ret));
  } else {
    init_transport_ = transport;
    init_ls_svr_ = ls_svr;
    init_log_service_ = log_service;
    ls_svr_ = ls_svr;
    reset_restore_source_();
    query_restore_source_ts_ = OB_INVALID_TIMESTAMP;
    schedule_fetch_log_ts_ = OB_INVALID_TIMESTAMP;
    common_event_schedule_ts_ = OB_INVALID_TIMESTAMP;
    inited_ = true;
    LOG_INFO("ObLogRestoreService init succ");
  }
  return ret;
}

void ObLogRestoreService::destroy()
{
  stop();
  wait();
  inited_ = false;
  fetch_log_worker_.destroy();
  writer_.destroy();
  location_adaptor_.destroy();
  archive_driver_.destroy();
  net_driver_.destroy();
  fetch_log_impl_.destroy();
  error_reporter_.destroy();
  reset_restore_source_();
  query_restore_source_ts_ = OB_INVALID_TIMESTAMP;
  schedule_fetch_log_ts_ = OB_INVALID_TIMESTAMP;
  common_event_schedule_ts_ = OB_INVALID_TIMESTAMP;
  proxy_.destroy();
  allocator_.destroy();
  scheduler_.destroy();
  ls_svr_ = NULL;
  share::ObThreadPool::destroy();
  LOG_INFO("ObLogRestoreService destroy succ");
}

ERRSIM_POINT_DEF(ERRSIM_LOG_RESTORE_SERVICE_RESTART1);
int ObLogRestoreService::start()
{
  int ret = OB_SUCCESS;
  ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore service not init", K(ret), K(inited_));
  } else if (OB_FAIL(fetch_log_worker_.start())) {
    LOG_WARN("fetch_log_worker_ start failed", K(ret));
  } else if (OB_FAIL(net_driver_.start())) {
    LOG_WARN("net_driver_ start failed", K(ret));
  } else if (OB_UNLIKELY(ERRSIM_LOG_RESTORE_SERVICE_RESTART1)) {
    ret = ERRSIM_LOG_RESTORE_SERVICE_RESTART1;
    LOG_WARN("ERRSIM_LOG_RESTORE_SERVICE_RESTART1 opened", KR(ret));
  } else if (OB_FAIL(writer_.start())) {
    LOG_WARN("remote_log_writer start failed");
  } else if (OB_FAIL(ObThreadPool::start())) {
    LOG_WARN("restore service start failed", K(ret));
  } else {
    LOG_INFO("restore service start succ", "tenant_id", MTL_ID());
  }
  return ret;
}

void ObLogRestoreService::stop()
{
  net_driver_.stop();
  fetch_log_worker_.stop();
  writer_.stop();
  ObThreadPool::stop();
  LOG_INFO("ObLogRestoreService thread stop", "tenant_id", MTL_ID());
}

void ObLogRestoreService::wait()
{
  net_driver_.wait();
  fetch_log_worker_.wait();
  writer_.wait();
  ObThreadPool::wait();
  LOG_INFO("ObLogRestoreService thread wait", "tenant_id", MTL_ID());
}

ERRSIM_POINT_DEF(ERRSIM_LOG_RESTORE_SERVICE_RESTART2);
int ObLogRestoreService::restart()
{
  int ret = OB_SUCCESS;
  // Note that lock cannot prevent restore_service being restarted when unit is in deleting state;
  // This is the most simple implementation, it's more safe to implement logical_stop;

  ObSpinLockGuard guard(lock_);
  destroy();
  if (OB_FAIL(init(init_transport_, init_ls_svr_, init_log_service_))) {
    LOG_WARN("failed to reinit ObLogRestoreService", KP(init_transport_), KP(init_ls_svr_), KP(init_log_service_));
  } else if (OB_UNLIKELY(ERRSIM_LOG_RESTORE_SERVICE_RESTART2)) {
    ret = ERRSIM_LOG_RESTORE_SERVICE_RESTART2;
    LOG_WARN("ERRSIM_LOG_RESTORE_SERVICE_RESTART2 opened", KR(ret));
  } else if (OB_FAIL(start())) {
    LOG_WARN("failed to start ObLogService");
  } else {
    LOG_INFO("ObLogRestoreService restart succ", KP(init_transport_), KP(init_ls_svr_), KP(init_log_service_));
  }

  return ret;
}

void ObLogRestoreService::signal()
{
  cond_.signal();
}

void ObLogRestoreService::run1()
{
  LOG_INFO("ObLogRestoreService thread run", "tenant_id", MTL_ID());
  ObDIActionGuard ag("LogService", "LogRestoreService", "loop task");
  lib::set_thread_name("LogRessvr");
  ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR_RET(OB_NOT_INIT, "ObLogRestoreService not init", "tenant_id", MTL_ID());
  } else {
    while (! has_set_stop()) {
      int64_t begin_stamp = ObTimeUtility::fast_current_time();
      const bool is_primary = MTL_GET_TENANT_ROLE_CACHE() == share::ObTenantRole::PRIMARY_TENANT;
      const int64_t thread_interval = is_primary ? PRIMARY_THREAD_RUN_INTERVAL : STANDBY_THREAD_RUN_INTERVAL;
      do_thread_task_();
      int64_t end_tstamp = ObTimeUtility::fast_current_time();
      int64_t wait_interval = thread_interval - (end_tstamp - begin_stamp);
      if (wait_interval > 0) {
        common::ObBKGDSessInActiveGuard inactive_guard;
        cond_.timedwait(wait_interval);
      }
    }
  }
  LOG_INFO("ObLogRestoreService thread end", "tenant_id", MTL_ID());
}

void ObLogRestoreService::do_thread_task_()
{
  int ret = OB_SUCCESS;
  if (is_user_tenant(MTL_ID())) {
    share::ObLogRestoreSourceItem source;
    bool source_exist = false;
    const int64_t current_ts = ObTimeUtility::current_time();
    if (reach_time_interval_(current_ts, UPDATE_UPSTREAM_INTERVAL, query_restore_source_ts_)) {
      reset_restore_source_();
      if (OB_FAIL(update_upstream_(source, source_exist))) {
        // don't schedule if failed to update upstream
        LOG_WARN("update_upstream_ failed");
      } else if (source_exist) {
        restore_source_.deep_copy(source);
      } else {
        // tenant_role not match or log_restore_source not exist
        clean_resource_();
      }
    }

    if (reach_time_interval_(current_ts, SCHEDULE_FETCH_LOG_INTERVAL, schedule_fetch_log_ts_)) {
      if (restore_source_.is_valid()) {
        ObDIActionGuard(ObDIActionGuard::NS_ACTION, "SourceType[%s]", ObLogRestoreSourceItem::get_source_type_str(restore_source_.type_));
        // log restore source exist, do schedule
        // source_exist means tenant_role is standby or restore and log_restore_source exists
        schedule_fetch_log_(restore_source_);
      }
      ObDIActionGuard(ObDIActionGuard::NS_ACTION, "SourceType[%s]", ObLogRestoreSourceItem::get_source_type_str(restore_source_.type_));
      schedule_resource_(restore_source_.type_);
    }

    if (reach_time_interval_(current_ts, COMMON_EVENT_SCHEDULE_INTERVAL, common_event_schedule_ts_)) {
      report_error_();
      update_restore_upper_limit_();
      refresh_error_context_();
      set_compressor_type_();
    }
  }
}

int ObLogRestoreService::update_upstream_(share::ObLogRestoreSourceItem &source, bool &source_exist)
{
  return location_adaptor_.update_upstream(source, source_exist);
}

void ObLogRestoreService::schedule_fetch_log_(share::ObLogRestoreSourceItem &source)
{
  (void)fetch_log_impl_.do_schedule(source);
}

void ObLogRestoreService::clean_resource_()
{
  (void)fetch_log_impl_.clean_resource();
}

void ObLogRestoreService::schedule_resource_(const share::ObLogRestoreSourceType &source_type)
{
  (void)scheduler_.schedule(source_type);
}

void ObLogRestoreService::report_error_()
{
  (void)error_reporter_.report_error();
}

void ObLogRestoreService::update_restore_upper_limit_()
{
  fetch_log_impl_.update_restore_upper_limit();
}

void ObLogRestoreService::set_compressor_type_()
{
  int ret = OB_SUCCESS;
  logservice::ObLogService* log_service = MTL(logservice::ObLogService*);
  palf::PalfOptions options;
  common::ObCompressorType compressor_type = INVALID_COMPRESSOR;

  if (OB_NOT_NULL(log_service)) {
    if (OB_FAIL(log_service->get_palf_options(options))) {
      LOG_WARN("log_service get_palf_options failed", KR(ret));
    } else {
      if (options.compress_options_.enable_transport_compress_) {
        compressor_type = options.compress_options_.transport_compress_func_;
        fetch_log_impl_.set_compressor_type(compressor_type);
      } else {
        // close
        fetch_log_impl_.set_compressor_type(common::INVALID_COMPRESSOR);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log_service is nullptr", KR(ret));
  }
}

void ObLogRestoreService::refresh_error_context_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLogRestoreHandler *restore_handler = NULL;
  if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get ls iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("iter is NULL", K(ret), K(iter));
  } else {
    while (OB_SUCC(ret)) {
      ls = NULL;
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter ls get next failed", K(ret));
        } else {
          LOG_TRACE("iter to end", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is NULL", K(ret), K(ls));
      } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("restore_handler is NULL", K(ret), K(ls->get_ls_id()));
      } else if (OB_FAIL(restore_handler->refresh_error_context())) {
        LOG_WARN("refresh error failed", K(ls->get_ls_id()));
      }
    }
  }
}

void ObLogRestoreService::reset_restore_source_()
{
  restore_source_.tenant_id_ = OB_INVALID_TENANT_ID;
  restore_source_.id_ = OB_INVALID_DEST_ID;
  restore_source_.type_ = ObLogRestoreSourceType::INVALID;
  restore_source_.value_.reset();
  restore_source_.until_scn_.reset();
  restore_source_.allocator_.reset();
}

bool ObLogRestoreService::reach_time_interval_(const int64_t current_ts,
     const int64_t time_interval,
     int64_t &time_us)
{
  bool bret = (OB_INVALID_TIMESTAMP == time_us ||
        current_ts - time_us >= time_interval);
  if (bret) {
    time_us = current_ts;
  }
  return bret;
}

} // namespace logservice
} // namespace oceanbase
