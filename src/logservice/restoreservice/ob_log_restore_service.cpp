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
#include "share/restore/ob_log_restore_source.h"    //TODO delete

#include "ob_log_restore_service.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"                    // is_user_tenant
#include "lib/utility/ob_macro_utils.h"       // K*
#include "share/ob_ls_id.h"                   // ObLSID
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "logservice/ob_log_service.h"        // ObLogService
#include "ob_log_restore_handler.h"           // ObTenantRole
#include "observer/ob_server_struct.h"        // GCTX

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
  error_reporter_(),
  allocator_(),
  scheduler_(),
  cond_()
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
  } else if (OB_FAIL(restore_controller_.init(tenant_id, log_service))) {
    LOG_WARN("restore_controller_ init failed");
  } else if (OB_FAIL(location_adaptor_.init(tenant_id, ls_svr))) {
    LOG_WARN("location_adaptor_ init failed", K(ret));
  } else if (OB_FAIL(archive_driver_.init(tenant_id, ls_svr, log_service, &fetch_log_worker_))) {
    LOG_WARN("archive_driver_ init failed");
  } else if (OB_FAIL(net_driver_.init(tenant_id, &restore_controller_, ls_svr, log_service))) {
    LOG_WARN("net_driver_ init failed");
  } else if (OB_FAIL(fetch_log_impl_.init(tenant_id, &archive_driver_, &net_driver_))) {
    LOG_WARN("fetch_log_impl_ init failed", K(ret));
  } else if (OB_FAIL(fetch_log_worker_.init(tenant_id, &allocator_, &restore_controller_, this, ls_svr))) {
    LOG_WARN("fetch_log_worker_ init failed", K(ret));
  } else if (OB_FAIL(error_reporter_.init(tenant_id, ls_svr))) {
    LOG_WARN("error_reporter_ init failed", K(ret));
  } else if (OB_FAIL(allocator_.init(tenant_id))) {
    LOG_WARN("allocator_ init failed", K(ret));
  } else if (OB_FAIL(scheduler_.init(tenant_id, &allocator_, &fetch_log_worker_))) {
    LOG_WARN("scheduler_ init failed", K(ret));
  } else {
    ls_svr_ = ls_svr;
    inited_ = true;
    LOG_INFO("ObLogRestoreService init succ");
  }
  return ret;
}

void ObLogRestoreService::destroy()
{
  inited_ = false;
  fetch_log_worker_.destroy();
  stop();
  wait();
  restore_controller_.destroy();
  location_adaptor_.destroy();
  archive_driver_.destroy();
  net_driver_.destroy();
  fetch_log_impl_.destroy();
  error_reporter_.destroy();
  proxy_.destroy();
  allocator_.destroy();
  scheduler_.destroy();
  ls_svr_ = NULL;
}

int ObLogRestoreService::start()
{
  int ret = OB_SUCCESS;
  ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore service not init", K(ret), K(inited_));
  } else if (OB_FAIL(fetch_log_worker_.start())) {
    LOG_WARN("fetch_log_worker_ start failed", K(ret));
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
  ObThreadPool::stop();
  LOG_INFO("ObLogRestoreService thread stop", "tenant_id", MTL_ID());
}

void ObLogRestoreService::wait()
{
  net_driver_.wait();
  fetch_log_worker_.wait();
  ObThreadPool::wait();
  LOG_INFO("ObLogRestoreService thread wait", "tenant_id", MTL_ID());
}

void ObLogRestoreService::signal()
{
  cond_.signal();
}

void ObLogRestoreService::run1()
{
  LOG_INFO("ObLogRestoreService thread run", "tenant_id", MTL_ID());
  lib::set_thread_name("LogRessvr");
  ObCurTraceId::init(GCONF.self_addr_);

  const int64_t THREAD_RUN_INTERVAL = 1 * 1000 * 1000L;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR_RET(OB_NOT_INIT, "ObLogRestoreService not init", "tenant_id", MTL_ID());
  } else {
    while (! has_set_stop()) {
      int64_t begin_stamp = ObTimeUtility::current_time();
      do_thread_task_();
      int64_t end_tstamp = ObTimeUtility::current_time();
      int64_t wait_interval = THREAD_RUN_INTERVAL - (end_tstamp - begin_stamp);
      if (wait_interval > 0) {
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

    update_restore_quota_();

    if (OB_FAIL(update_upstream_(source, source_exist))) {
      LOG_WARN("update_upstream_ failed");
    } else if (source_exist) {
      // log restore source exist, do schedule
      // source_exist means tenant_role is standby or restore and log_restore_source exists
      schedule_fetch_log_(source);
    } else {
      // tenant_role not match or log_restore_source not exist
      clean_resource_();
    }

    schedule_resource_();
    report_error_();
  }
}

void ObLogRestoreService::update_restore_quota_()
{
  (void)restore_controller_.update_quota();
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

void ObLogRestoreService::schedule_resource_()
{
  (void)scheduler_.schedule();
}

void ObLogRestoreService::report_error_()
{
  (void)error_reporter_.report_error();
}

} // namespace logservice
} // namespace oceanbase
