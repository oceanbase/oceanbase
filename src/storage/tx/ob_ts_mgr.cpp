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

#include "ob_ts_mgr.h"
#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "share/ob_cluster_version.h"
#include "share/scn.h"
#include "ob_trans_event.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/location_cache/ob_location_service.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "ob_gts_rpc.h"
#include "storage/tx/ob_trans_factory.h"
#include "lib/thread/ob_thread_name.h"
#include "ob_location_adapter.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;

namespace transaction
{
ObTsSourceInfo::ObTsSourceInfo() : is_inited_(false),
                                   tenant_id_(OB_INVALID_TENANT_ID),
                                   last_access_ts_(0)
{
}

int ObTsSourceInfo::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    last_access_ts_ = ObClockGenerator::getClock();
    is_inited_ = true;
    TRANS_LOG(INFO, "ts source info init success", K(tenant_id));
  }
  return ret;
}

void ObTsSourceInfo::destroy()
{
  if (is_inited_) {
    const uint64_t tenant_id = tenant_id_;
    gts_source_.destroy();
    is_inited_ = false;
    TRANS_LOG(INFO, "ts source info destroyed", K(tenant_id));
  }
}

int ObTsSourceInfo::check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool &has_dropped)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(tenant_id_ != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(tenant_id_));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    TRANS_LOG(WARN, "fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, has_dropped))) {
    TRANS_LOG(WARN, "fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTsSourceInfo::gts_callback_interrupted(const int errcode)
{
  int ret = OB_SUCCESS;
  const int64_t task_count = gts_source_.get_task_count();
  if (0 != task_count) {
    ret = gts_source_.gts_callback_interrupted(errcode);
  }
  return ret;
}

ObTsSourceInfoGuard::~ObTsSourceInfoGuard()
{
  if (NULL != ts_source_info_ && NULL != mgr_) {
    mgr_->revert_ts_source_info_(*this);
  }
}

////////////////////////ObTsMgr实现///////////////////////////////////

int ObTsMgr::init(const ObAddr &server,
                  share::schema::ObMultiVersionSchemaService &schema_service,
                  share::ObLocationService &location_service,
                  rpc::frame::ObReqTransport *req_transport)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObTsMgr inited twice", KR(ret));
  } else if (!server.is_valid() || OB_ISNULL(req_transport)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(server), KP(req_transport));
  } else if (OB_FAIL(ts_source_info_map_.init("TsSourceInfoMap"))) {
    TRANS_LOG(WARN, "ts_source_info_map_ init failed", KR(ret));
  } else if (OB_FAIL(location_adapter_def_.init(&schema_service, &location_service))) {
    TRANS_LOG(ERROR, "location adapter init error", KR(ret));
  } else if (OB_ISNULL(gts_request_rpc_proxy_ = ObGtsRpcProxyFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc gts_reqeust_rpc_proxy fail", KR(ret));
  } else if (OB_ISNULL(gts_request_rpc_ = ObGtsRequestRpcFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc gts_reqeust_rpc fail", KR(ret));
  } else if (OB_FAIL(gts_request_rpc_proxy_->init(req_transport, server))) {
    TRANS_LOG(WARN, "rpc proxy init failed", KR(ret), KP(req_transport), K(server));
  } else if (OB_FAIL(ts_worker_.init(this, true))) {
    TRANS_LOG(WARN, "ts worker init failed", KR(ret));
  } else if (OB_FAIL(gts_request_rpc_->init(gts_request_rpc_proxy_, server, this, &ts_worker_))) {
    TRANS_LOG(WARN, "response rpc init failed", KR(ret), K(server));
  } else if (OB_FAIL(lock_.init(lib::ObMemAttr(OB_SERVER_TENANT_ID, "TsMgr")))) {
    TRANS_LOG(WARN, "ObQSyncLock init failed", KR(ret), K(OB_SERVER_TENANT_ID));
  } else {
    server_ = server;
    location_adapter_ = &location_adapter_def_;
    is_inited_ = true;
    TRANS_LOG(INFO, "ObTsMgr inited success", KP(this), K(server));
  }

  if (OB_FAIL(ret)) {
    if (NULL != gts_request_rpc_proxy_) {
      ObGtsRpcProxyFactory::release(gts_request_rpc_proxy_);
      gts_request_rpc_proxy_ = NULL;
    }
    if (NULL != gts_request_rpc_) {
      ObGtsRequestRpcFactory::release(gts_request_rpc_);
      gts_request_rpc_ = NULL;
    }
  }

  return ret;
}

void ObTsMgr::reset()
{
  is_inited_ = false;
  is_running_ = false;
  ts_source_info_map_.reset();
  server_.reset();
  location_adapter_ = NULL;
  gts_request_rpc_proxy_ = NULL;
  gts_request_rpc_ = NULL;
  for (int64_t i = 0; i < TS_SOURCE_INFO_CACHE_NUM; i++) {
    ts_source_infos_[i] = NULL;
  }
}

int ObTsMgr::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ObTsMgr is already running", KR(ret));
  } else if (OB_FAIL(gts_request_rpc_->start())) {
    TRANS_LOG(WARN, "gts request rpc start", KR(ret));
    // 启动gts任务刷新线程
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    TRANS_LOG(ERROR, "GTS local cache manager refresh worker thread start error", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObTsMgr start success");
  }
  return ret;
}

void ObTsMgr::stop()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", KR(ret));
  } else if (OB_FAIL(gts_request_rpc_->stop())) {
    TRANS_LOG(WARN, "gts request rpc stop", KR(ret));
  } else {
    (void)share::ObThreadPool::stop();
    (void)ts_worker_.stop();
    is_running_ = false;
    TRANS_LOG(INFO, "ObTsMgr stop success");
  }
}

void ObTsMgr::wait()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ObTsMgr is running", KR(ret));
  } else if (OB_FAIL(gts_request_rpc_->wait())) {
    TRANS_LOG(WARN, "gts request rpc wait", KR(ret));
  } else {
    (void)share::ObThreadPool::wait();
    (void)ts_worker_.wait();
    TRANS_LOG(INFO, "ObTsMgr wait success");
  }
}

void ObTsMgr::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    (void)share::ObThreadPool::destroy();
    (void)ts_worker_.destroy();

    ObSEArray<uint64_t, 1> ids;
    GetALLTenantFunctor get_all_tenant_functor(ids);
    ts_source_info_map_.for_each(get_all_tenant_functor);
    for (int64_t i = 0; i < ids.count(); i++) {
      const uint64_t tenant_id = ids.at(i);
      delete_tenant_(tenant_id);
    }
    ids.reset();
    ts_source_info_map_.destroy();

    location_adapter_def_.destroy();
    lock_.destroy();
    server_.reset();
    location_adapter_ = NULL;
    is_running_ = false;
    is_inited_ = false;
    TRANS_LOG(INFO, "ObTsMgr destroyed");
  }
  if (NULL != gts_request_rpc_proxy_) {
    ObGtsRpcProxyFactory::release(gts_request_rpc_proxy_);
    gts_request_rpc_proxy_ = NULL;
  }
  if (NULL != gts_request_rpc_) {
    ObGtsRequestRpcFactory::release(gts_request_rpc_);
    gts_request_rpc_ = NULL;
  }
}

// 执行gts任务刷新，由一个专门的线程来负责
void ObTsMgr::run1()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> ids;
  ObSEArray<uint64_t, 1> check_ids;
  ObGtsRefreshFunctor gts_refresh_funtor;
  GetObsoleteTenantFunctor get_obsolete_tenant_functor(TS_SOURCE_INFO_OBSOLETE_TIME, ids);
  CheckTenantFunctor check_tenant_functor(check_ids);
  // cluster版本小于2.0不会更新gts
  lib::set_thread_name("TsMgr");
  while (!has_set_stop()) {
    // sleep 100 * 1000 us
    ob_usleep(REFRESH_GTS_INTERVEL_US);
    ts_source_info_map_.for_each(gts_refresh_funtor);
    ts_source_info_map_.for_each(get_obsolete_tenant_functor);
    ts_source_info_map_.for_each(check_tenant_functor);
    for (int64_t i = 0; i < ids.count(); i++) {
      const uint64_t tenant_id = ids.at(i);
      if (OB_FAIL(delete_tenant_(tenant_id))) {
        TRANS_LOG(WARN, "delete tenant failed", K(ret), K(tenant_id));
        // ignore ret
        ret = OB_SUCCESS;
      }
    }
    ids.reset();
    for (int64_t i = 0; i < check_ids.count(); i++) {
      const uint64_t tenant_id = check_ids.at(i);
      if (OB_FAIL(remove_dropped_tenant_(tenant_id))) {
        TRANS_LOG(WARN, "remove dropped tenant failed", K(ret), K(tenant_id));
        // ignore ret
        ret = OB_SUCCESS;
      }
    }
    check_ids.reset();
  }
}

int ObTsMgr::handle_gts_err_response(const ObGtsErrResponse &msg)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("handle_gts_err_response", 100000);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(msg));
  } else {
    const uint64_t tenant_id = msg.get_tenant_id();
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *gts_source = NULL;
    ObTsSourceInfoGuard guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, guard, false, false))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", KR(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(gts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "gts source is NULL", KR(ret), K(tenant_id));
      } else if (OB_FAIL(gts_source->handle_gts_err_response(msg))) {
        TRANS_LOG(WARN, "handle gts err response error", KR(ret), K(msg));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObTsMgr::refresh_gts_location(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("refresh_gts_location", 100000);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *gts_source = NULL;
    ObTsSourceInfoGuard guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, guard, false, false))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(gts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "gts source is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(gts_source->refresh_gts_location())) {
        TRANS_LOG(WARN, "refresh gts location error", K(ret), K(tenant_id));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObTsMgr::handle_gts_result(const uint64_t tenant_id, const int64_t queue_index, const int ts_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *gts_source = NULL;
    ObTsSourceInfoGuard guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, guard, false, false))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", KR(ret), K(tenant_id));
    } else if (TS_SOURCE_GTS == ts_type) {
      if (OB_ISNULL(gts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "gts source is NULL", KR(ret), K(tenant_id));
      } else if (OB_FAIL(gts_source->handle_gts_result(tenant_id, queue_index))) {
        TRANS_LOG(WARN, "handle gts result error", KR(ret), K(tenant_id));
      } else {
        // do nothing
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected ts type", KR(ret), K(tenant_id), K(ts_type));
    }
  }
  return ret;
}

int ObTsMgr::update_gts(const uint64_t tenant_id,
                        const MonotonicTs srr,
                        const int64_t gts,
                        const int ts_type,
                        bool &update)
{
  int ret = OB_SUCCESS;
  const MonotonicTs receive_gts_ts = MonotonicTs::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) ||
      OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(0 >= gts)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(srr), K(gts));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *gts_source = NULL;
    ObTsSourceInfoGuard guard;
    const bool need_create_tenant = (TS_SOURCE_GTS == ts_type);
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, guard, need_create_tenant, false))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else {
        // rewrite ret
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(ts_source_info = guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", KR(ret), K(tenant_id));
    } else if (TS_SOURCE_GTS == ts_type) {
      if (OB_ISNULL(gts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "gts source is NULL", KR(ret), K(tenant_id));
      } else if (OB_FAIL(gts_source->update_gts(srr, gts, receive_gts_ts, update))) {
        TRANS_LOG(WARN, "update gts cache failed", KR(ret), K(tenant_id), K(srr), K(gts));
      } else {
        // do nothing
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected ts type", KR(ret), K(tenant_id), K(ts_type));
    }
  }

  return ret;
}

int ObTsMgr::delete_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else {
    ret = delete_tenant_(tenant_id);
  }
  return ret;
}

int ObTsMgr::delete_tenant_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTsTenantInfo tenant_info(tenant_id);
  do {
    ObTsSourceInfo *ts_source_info = NULL;
    ObTsSourceInfoGuard info_guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, false, false))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", KR(ret), K(tenant_id));
    } else {
      TRANS_LOG(INFO, "set tenant gts invalid success", K(tenant_id));
    }
  } while (0);
  if (OB_SUCCESS == ret) {
    if (OB_FAIL(ts_source_info_map_.del(tenant_info))) {
      TRANS_LOG(WARN, "delete tenant from hashmap failed", K(ret), K(tenant_id));
    }
    // ignore ret
    if (OB_LIKELY(tenant_id < TS_SOURCE_INFO_CACHE_NUM)) {
      lock_.wrlock();
      ObTsSourceInfo *ts_source_info = NULL;
      ObTsSourceInfo **ptr = &(ts_source_infos_[tenant_id]);
      ts_source_info = ATOMIC_LOAD(ptr);
      ATOMIC_STORE(ptr, NULL);
      if (NULL != ts_source_info) {
        (void)ts_source_info_map_.revert(ts_source_info);
        ts_source_info = NULL;
      }
      lock_.wrunlock();
    }
  }
  if (OB_SUCCESS == ret) {
    TRANS_LOG(INFO, "delete tenant success", K(tenant_id));
  } else {
    TRANS_LOG(WARN, "delete tenant failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTsMgr::remove_dropped_tenant_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTsTenantInfo tenant_info(tenant_id);
  ObTsSourceInfo *ts_source_info = NULL;
  ObTsSourceInfoGuard info_guard;
  if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, false, false))) {
    TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts source info is NULL", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ts_source_info->gts_callback_interrupted(OB_TENANT_NOT_EXIST))) {
    TRANS_LOG(WARN, "interrupt gts callback failed", KR(ret), K(tenant_id));
  } else {
    TRANS_LOG(INFO, "interrupt gts callback success", K(tenant_id));
  }
  return ret;
}

int ObTsMgr::update_gts(const uint64_t tenant_id, const int64_t gts, bool &update)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) ||
             OB_UNLIKELY(0 >= gts) ||
             OB_UNLIKELY(gts > ObTimeUtility::current_time_ns() + 86400000000000L)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(gts));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *ts_source = NULL;
    ObTsSourceInfoGuard info_guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(ts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source is NULL", K(ret));
      } else if (OB_FAIL(ts_source->update_gts(gts, update))) {
        TRANS_LOG(WARN, "update gts cache failed", K(ret), K(tenant_id), K(gts));
      }
    }
  }

  return ret;
}

int ObTsMgr::get_gts(const uint64_t tenant_id, ObTsCbTask *task, SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t gts = 0;//need be invalid value for SCN
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), KP(task));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *ts_source = NULL;
    ObTsSourceInfoGuard info_guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(ts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source is NULL", K(ret));
      } else if (OB_FAIL(ts_source->get_gts(task, gts))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "get gts error", K(ret), K(tenant_id), KP(task));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(scn.convert_for_gts(gts))) {
      TRANS_LOG(WARN, "failed to convert_for_gts", K(ret), K(tenant_id), K(gts));
    }
  }

  return ret;
}

int ObTsMgr::get_gts(const uint64_t tenant_id,
                     const MonotonicTs stc,
                     ObTsCbTask *task,
                     SCN &scn,
                     MonotonicTs &receive_gts_ts)
{
  int ret = OB_SUCCESS;
  int64_t gts = 0;//need be invalid value for SCN

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!stc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(stc), KP(task));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *ts_source = NULL;
    ObTsSourceInfoGuard info_guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(ts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source is NULL", K(ret));
      } else if (OB_FAIL(ts_source->get_gts(stc, task, gts, receive_gts_ts))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "get gts error", K(ret), K(tenant_id), K(stc), KP(task));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(scn.convert_for_gts(gts))) {
      TRANS_LOG(WARN, "failed to convert_for_gts", K(ret), K(tenant_id), K(gts));
    }
  }
  return ret;
}

int ObTsMgr::get_ts_sync(const uint64_t tenant_id,
                         const int64_t timeout_us,
                         SCN &scn,
                         bool &is_external_consistent)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  const MonotonicTs stc = MonotonicTs::current_time();
  MonotonicTs receive_gts_ts;
  int64_t sleep_us = 100 * 1000;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(timeout_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(timeout_us));
  } else {
    do {
      ObTsSourceInfo *ts_source_info = NULL;
      ObGtsSource *ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      int64_t ts = 0;
      if (OB_UNLIKELY(ObTimeUtility::current_time() >= start + timeout_us)) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else {
        if (OB_ISNULL(ts_source = ts_source_info->get_gts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->get_gts(stc, NULL, ts, receive_gts_ts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "get gts error", K(ret), K(tenant_id), K(stc));
          } else {
            ob_usleep(sleep_us);
            sleep_us = sleep_us * 2;
            sleep_us = (sleep_us >= 1000000 ? 1000000 : sleep_us);
            // rewrite ret
            ret = OB_SUCCESS;
          }
        } else {
          scn.convert_for_gts(ts);
          is_external_consistent = ts_source->is_external_consistent();
          break;
        }
      }
    } while (OB_SUCCESS == ret);
  }

  return ret;
}

bool ObTsMgr::is_external_consistent(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *ts_source = NULL;
    ObTsSourceInfoGuard info_guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(ts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source is NULL", K(ret), K(tenant_id));
      } else {
        bool_ret = ts_source->is_external_consistent();
      }
    }
  }

  return bool_ret;
}

int ObTsMgr::wait_gts_elapse(const uint64_t tenant_id, const SCN &scn,
    ObTsCbTask *task, bool &need_wait)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  // } else if (OB_UNLIKELY(!is_running_)) {
  //   ret = OB_NOT_RUNNING;
  //   TRANS_LOG(WARN, "ObTsMgr not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_UNLIKELY(!scn.is_valid())
      || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(scn), KP(task));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *ts_source = NULL;
    ObTsSourceInfoGuard info_guard;
    const int64_t ts = scn.get_val_for_gts();
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(ts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source is NULL", K(ret));
      } else if (OB_FAIL(ts_source->wait_gts_elapse(ts, task, need_wait))) {
        TRANS_LOG(WARN, "wait gts elapse failed", K(ret), K(ts), KP(task));
      }
    }
  }
  return ret;
}

int ObTsMgr::wait_gts_elapse(const uint64_t tenant_id, const SCN &scn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(scn));
  } else {
    ObTsSourceInfo *ts_source_info = NULL;
    ObGtsSource *ts_source = NULL;
    ObTsSourceInfoGuard info_guard;
    const int64_t ts = scn.get_val_for_gts();
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(ts_source = ts_source_info->get_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source is NULL", K(ret));
      } else if (OB_FAIL(ts_source->wait_gts_elapse(ts))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "wait gts elapse fail", K(ret), K(ts), K(tenant_id));
        }
      }
    }
  }

  return ret;
}

ObTsMgr *&ObTsMgr::get_instance_inner()
{
  static ObTsMgr instance;
  static ObTsMgr *instance2 = &instance;
  return instance2;
}

ObTsMgr &ObTsMgr::get_instance()
{
  return *get_instance_inner();
}

int ObTsMgr::get_ts_source_info_opt_(const uint64_t tenant_id, ObTsSourceInfoGuard &guard,
    const bool need_create_tenant, const bool need_update_access_ts)
{
  int ret = OB_SUCCESS;
  ObTsSourceInfo *ts_source_info = NULL;
  if (OB_LIKELY(tenant_id < TS_SOURCE_INFO_CACHE_NUM)) {
    lock_.rdlock();
    ObTsSourceInfo **ptr = &(ts_source_infos_[tenant_id]);
    ts_source_info = ATOMIC_LOAD(ptr);
    if (OB_ISNULL(ts_source_info)) {
      if (OB_FAIL(get_ts_source_info_(tenant_id, guard, need_create_tenant, need_update_access_ts))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else {
        if (ATOMIC_BCAS(ptr, NULL, guard.get_ts_source_info())) {
          guard.set_need_revert(false);
        }
      }
    } else {
      if (need_update_access_ts) {
        ts_source_info->update_last_access_ts();
      }
      guard.set(ts_source_info, this, false);
    }
    if (OB_FAIL(ret)) {
      lock_.rdunlock();
    }
  } else {
    if (OB_FAIL(get_ts_source_info_(tenant_id, guard, need_create_tenant, need_update_access_ts))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObTsMgr::get_ts_source_info_(const uint64_t tenant_id, ObTsSourceInfoGuard &guard,
    const bool need_create_tenant, const bool need_update_access_ts)
{
  int ret = OB_SUCCESS;
  ObTsTenantInfo tenant_info(tenant_id);
  ObTsSourceInfo *ts_source_info = NULL;
  if (OB_FAIL(ts_source_info_map_.get(tenant_info, ts_source_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (need_create_tenant) {
        // rewrite ret
        ret = OB_SUCCESS;
        if (OB_FAIL(add_tenant_(tenant_id))) {
          TRANS_LOG(WARN, "add tenant failed", K(ret), K(tenant_id));
        } else if (OB_FAIL(ts_source_info_map_.get(tenant_info, ts_source_info))) {
          TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
        } else {
          // do nothing
        }
      }
    } else {
      TRANS_LOG(WARN, "get ts source info failed", KR(ret), K(tenant_id));
    }
  }
  if (OB_SUCCESS == ret) {
    if (OB_ISNULL(ts_source_info)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "ts source info is NULL", K(ret), K(tenant_id));
    } else {
      if (need_update_access_ts) {
        ts_source_info->update_last_access_ts();
      }
      guard.set(ts_source_info, this, true);
    }
  }
  return ret;
}

void ObTsMgr::revert_ts_source_info_(ObTsSourceInfoGuard &guard)
{
  if (OB_LIKELY(guard.get_ts_source_info()->get_tenant_id() < TS_SOURCE_INFO_CACHE_NUM)) {
    lock_.rdunlock();
  }
  if (guard.need_revert()) {
    (void)ts_source_info_map_.revert(guard.get_ts_source_info());
  }
}

int ObTsMgr::add_tenant_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObTimeGuard timeguard("add ts tenant");
  ObMemAttr memattr(OB_SERVER_TENANT_ID, ObModIds::OB_GTS_TASK_QUEUE);
  SET_USE_500(memattr);
  ObTsTenantInfo tenant_info(tenant_id);
  ObTsSourceInfo *ts_source_info = NULL;

  if (OB_ISNULL(ptr = ob_malloc(sizeof(ObTsSourceInfo), memattr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "allocate memory failed", KR(ret), K(tenant_id));
  } else {
    if (OB_ISNULL(ts_source_info = new (ptr) ObTsSourceInfo())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(ERROR, "ts source info construct failed", KR(ret), KP(ptr));
    } else {
      ObGtsSource *gts_source = ts_source_info->get_gts_source();
      if (OB_FAIL(gts_source->init(tenant_id, server_, gts_request_rpc_, location_adapter_))) {
        TRANS_LOG(WARN, "gts_source init error", KR(ret));
      } else if (OB_FAIL(ts_source_info->init(tenant_id))) {
        TRANS_LOG(WARN, "ts source init failed", KR(ret));
      } else if (OB_FAIL(ts_source_info_map_.insert_and_get(tenant_info, ts_source_info))) {
        if (OB_ENTRY_EXIST != ret) {
          TRANS_LOG(WARN, "wait queue hashmap insert error", KR(ret), KP(ts_source_info));
        }
      } else {
        ts_source_info_map_.revert(ts_source_info);
      }
    }
    if (OB_FAIL(ret)) {
      ts_source_info->~ObTsSourceInfo();
      ob_free(ts_source_info);
      ts_source_info = NULL;
    }
  }
  if (OB_SUCCESS != ret) {
    if (OB_ENTRY_EXIST != ret) {
      TRANS_LOG(WARN, "ts source add tenant failed", KR(ret), K(tenant_id));
    } else {
      // rewrite ret
      ret = OB_SUCCESS;
    }
  } else {
    TRANS_LOG(INFO, "ts source add tenant success", K(tenant_id), K_(server), K(timeguard), K(lbt()));
  }

  return ret;
}

} // transaction
} // oceanbase
