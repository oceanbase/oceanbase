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
#include "ob_trans_event.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "ob_gts_rpc.h"
#include "storage/transaction/ob_trans_factory.h"
#include "lib/thread/ob_thread_name.h"
#include "ob_location_adapter.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;

namespace transaction {
ObTsSourceGuard::~ObTsSourceGuard()
{
  if (NULL != ts_source_ && NULL != ts_source_info_) {
    ts_source_info_->revert_ts_source_(*this);
  }
}

ObTsSourceInfo::ObTsSourceInfo()
    : is_inited_(false),
      is_valid_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      last_check_switch_ts_(0),
      last_access_ts_(0),
      last_obtain_switch_ts_(0),
      check_switch_interval_(DEFAULT_CHECK_SWITCH_INTERVAL_US),
      cur_ts_type_(TS_SOURCE_LTS)
{
  for (int64_t i = 0; i < MAX_TS_SOURCE; i++) {
    ts_source_[i] = NULL;
  }
}

int ObTsSourceInfo::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else {
    is_valid_ = true;
    tenant_id_ = tenant_id;
    last_access_ts_ = ObTimeUtility::current_time();
    last_obtain_switch_ts_ = ObTimeUtility::current_time();
    ts_source_[TS_SOURCE_GTS] = &gts_source_;
    ts_source_[TS_SOURCE_LTS] = &lts_;
    ts_source_[TS_SOURCE_HA_GTS] = &ha_gts_source_;
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

int ObTsSourceInfo::get_ts_source(const uint64_t tenant_id, ObTsSourceGuard& guard, bool& is_valid)
{
  int ret = OB_SUCCESS;
  rwlock_.rdlock();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(tenant_id_ != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(tenant_id_));
  } else {
    int cur_ts_type = cur_ts_type_;
    is_valid = is_valid_;
    guard.set(this, ts_source_[cur_ts_type], cur_ts_type);
  }
  if (OB_FAIL(ret)) {
    rwlock_.rdunlock();
  }
  return ret;
}

int ObTsSourceInfo::check_and_switch_ts_source(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObClockGenerator::getClock();
  // new ts type must be set TS_SOURCE_UNKNOWN
  int64_t new_ts_type = TS_SOURCE_UNKNOWN;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(tenant_id_ != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(tenant_id_));
  } else if (now - last_check_switch_ts_ < check_switch_interval_) {
    // do nothing
  } else {
    last_check_switch_ts_ = now;
    // ignore tmp_ret
    if (OB_FAIL(ObTsMgr::get_cur_ts_type(tenant_id, new_ts_type))) {
      TRANS_LOG(WARN, "get cur ts type failed", K(ret), K(tenant_id));
      check_switch_interval_ += DEFAULT_CHECK_SWITCH_INTERVAL_US;
      if (check_switch_interval_ > MAX_CHECK_SWITCH_INTERVAL_US) {
        check_switch_interval_ = MAX_CHECK_SWITCH_INTERVAL_US;
      }
    } else {
      check_switch_interval_ = DEFAULT_CHECK_SWITCH_INTERVAL_US;
      last_obtain_switch_ts_ = now;
      if (OB_UNLIKELY(cur_ts_type_ != new_ts_type)) {
        if (OB_SYS_TENANT_ID == tenant_id && TS_SOURCE_LTS != new_ts_type) {
          TRANS_LOG(WARN, "switch ts source to non-lts for sys tenant", K(tenant_id), K(new_ts_type), K_(cur_ts_type));
        }
        rwlock_.wrlock();
        if (TS_SOURCE_UNKNOWN != new_ts_type && cur_ts_type_ != new_ts_type) {
          if (OB_FAIL(switch_ts_source_(tenant_id, new_ts_type))) {
            TRANS_LOG(ERROR, "switch ts source failed", K(ret), K(new_ts_type));
          }
        }
        rwlock_.wrunlock();
      }
    }
  }
  return ret;
}

int ObTsSourceInfo::set_invalid()
{
  int ret = OB_SUCCESS;
  rwlock_.wrlock();
  if (is_valid_) {
    const int64_t task_count = gts_source_.get_task_count();
    if (0 == task_count) {
      is_valid_ = false;
    } else {
      ret = OB_EAGAIN;
    }
  }
  rwlock_.wrunlock();
  return ret;
}

int ObTsSourceInfo::switch_ts_source(const uint64_t tenant_id, const int ts_type)
{
  return switch_ts_source_(tenant_id, ts_type);
}

int ObTsSourceInfo::switch_ts_source_(const uint64_t tenant_id, const int ts_type)
{
  int ret = OB_SUCCESS;
  int64_t base_ts = 0;
  int64_t publish_version = 0;
  const int old_ts_type = cur_ts_type_;
  if (ts_type == cur_ts_type_) {
    TRANS_LOG(INFO, "timestamp source not changed", K(ts_type));
  } else {
    ObITsSource* cur_source = ts_source_[cur_ts_type_];
    ObITsSource* next_source = ts_source_[ts_type];
    if (NULL == cur_source || NULL == next_source) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected error, ts source is NULL", KR(ret), KP(cur_source), KP(next_source));
    } else if (OB_FAIL(cur_source->get_base_ts(base_ts, publish_version))) {
      TRANS_LOG(ERROR, "get base ts from current ts source failed", KR(ret));
    } else if (OB_FAIL(next_source->update_base_ts(base_ts + 1, publish_version))) {
      TRANS_LOG(ERROR, "set base ts to next ts source failed", KR(ret));
    } else {
      cur_ts_type_ = ts_type;
    }
    if (OB_SUCCESS != ret) {
      TRANS_LOG(WARN, "switch ts source failed", KR(ret), K(tenant_id), K(old_ts_type), K(ts_type));
    } else {
      TRANS_LOG(
          INFO, "switch ts source success", K(tenant_id), K(old_ts_type), K(ts_type), K(base_ts), K(publish_version));
    }
  }
  return ret;
}

void ObTsSourceInfo::revert_ts_source_(ObTsSourceGuard& guard)
{
  UNUSED(guard);
  rwlock_.rdunlock();
}

ObTsSourceInfoGuard::~ObTsSourceInfoGuard()
{
  if (NULL != ts_source_info_ && NULL != mgr_) {
    mgr_->revert_ts_source_info_(*this);
  }
}

////////////////////////ObTsMgr///////////////////////////////////

int ObTsMgr::init(const ObAddr& server, ObILocationAdapter* location_adapter, rpc::frame::ObReqTransport* req_transport,
    ObIGlobalTimestampService* global_timestamp_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObTsMgr inited twice", KR(ret));
  } else if (!server.is_valid() || OB_ISNULL(location_adapter) || OB_ISNULL(req_transport) ||
             OB_ISNULL(global_timestamp_service)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(server),
        KP(location_adapter),
        KP(req_transport),
        KP(global_timestamp_service));
  } else if (OB_ISNULL(gts_request_rpc_proxy_ = ObGtsRpcProxyFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc gts_reqeust_rpc_proxy fail", KR(ret));
  } else if (OB_ISNULL(gts_request_rpc_ = ObGtsRequestRpcFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc gts_reqeust_rpc fail", KR(ret));
  } else if (OB_FAIL(gts_request_rpc_proxy_->init(req_transport, server))) {
    TRANS_LOG(WARN, "rpc proxy init failed", KR(ret), KP(req_transport), K(server));
  } else if (OB_FAIL(gts_worker_.init(this, true))) {
    TRANS_LOG(WARN, "gts worker init failed", KR(ret));
  } else if (OB_FAIL(gts_request_rpc_->init(
                 gts_request_rpc_proxy_, server, this, &gts_worker_, global_timestamp_service))) {
    TRANS_LOG(WARN, "response rpc init failed", KR(ret), K(server));
  } else {
    server_ = server;
    location_adapter_ = location_adapter;
    global_timestamp_service_ = global_timestamp_service;
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
  global_timestamp_service_ = NULL;
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
    // Start the gts task refresh thread
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
    (void)gts_worker_.stop();
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
    (void)gts_worker_.wait();
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

// Perform gts task refresh, which is responsible for a dedicated thread
void ObTsMgr::run1()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> ids;
  ObGtsRefreshFunctor gts_refresh_funtor;
  GetObsoleteTenantFunctor get_obsolete_tenant_functor(TS_SOURCE_INFO_OBSOLETE_TIME, ids);
  // Cluster version less than 2.0 will not update gts
  lib::set_thread_name("TsMgr");
  while (!has_set_stop()) {
    // sleep 100 * 1000 us
    usleep(REFRESH_GTS_INTERVEL_US);
    ts_source_info_map_.for_each(gts_refresh_funtor);
    ts_source_info_map_.for_each(get_obsolete_tenant_functor);
    for (int64_t i = 0; i < ids.count(); i++) {
      const uint64_t tenant_id = ids.at(i);
      if (OB_FAIL(delete_tenant_(tenant_id))) {
        TRANS_LOG(WARN, "delete tenant failed", K(ret), K(tenant_id));
        // ignore ret
        ret = OB_SUCCESS;
      }
    }
    ids.reset();
  }
}

int ObTsMgr::handle_gts_err_response(const ObGtsErrResponse& msg)
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
    ObTsSourceInfo* ts_source_info = NULL;
    ObGtsSource* gts_source = NULL;
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
  int tmp_ret = OB_SUCCESS;
  ObTimeGuard timeguard("refresh_gts_location", 100000);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else {
    ObTsSourceInfo* ts_source_info = NULL;
    ObGtsSource* gts_source = NULL;
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
    ObTsSourceInfo* ts_source_info = NULL;
    ObGtsSource* gts_source = NULL;
    ObHaGtsSource* ha_gts_source = NULL;
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
    } else if (TS_SOURCE_HA_GTS == ts_type) {
      if (OB_ISNULL(ha_gts_source = ts_source_info->get_ha_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ha gts source is NULL", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ha_gts_source->handle_gts_result(tenant_id, queue_index))) {
        TRANS_LOG(WARN, "handle ha gts result error", KR(ret), K(tenant_id));
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

int ObTsMgr::update_gts(
    const uint64_t tenant_id, const MonotonicTs srr, const int64_t gts, const int ts_type, bool& update)
{
  int ret = OB_SUCCESS;
  const MonotonicTs receive_gts_ts = MonotonicTs::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(0 >= gts)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(srr), K(gts));
  } else {
    ObTsSourceInfo* ts_source_info = NULL;
    ObGtsSource* gts_source = NULL;
    ObHaGtsSource* ha_gts_source = NULL;
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
    } else if (TS_SOURCE_HA_GTS == ts_type) {
      if (OB_ISNULL(ha_gts_source = ts_source_info->get_ha_gts_source())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ha gts source is NULL", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ha_gts_source->update_gts(srr, gts, receive_gts_ts, update))) {
        TRANS_LOG(WARN, "update ha gts cache failed", KR(ret), K(tenant_id), K(srr), K(gts));
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
    ObTsSourceInfo* ts_source_info = NULL;
    ObTsSourceInfoGuard info_guard;
    ObTsSourceGuard source_guard;
    if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, false, false))) {
      TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts source info is NULL", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ts_source_info->set_invalid())) {
      TRANS_LOG(WARN, "set tenant gts invalid failed", KR(ret), K(tenant_id));
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
      ObTsSourceInfo* ts_source_info = NULL;
      ObTsSourceInfo** ptr = &(ts_source_infos_[tenant_id]);
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

int ObTsMgr::update_gts(const uint64_t tenant_id, const int64_t gts, bool& update)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(0 >= gts)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(gts));
  } else {
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->update_gts(gts, update))) {
          TRANS_LOG(WARN, "update gts cache failed", K(ret), K(tenant_id), K(gts));
        } else {
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }

  return ret;
}

int ObTsMgr::update_local_trans_version(const uint64_t tenant_id, const int64_t version, bool& update)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(0 >= version)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(version));
  } else {
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->update_local_trans_version(version, update))) {
          TRANS_LOG(WARN, "update local trans version failed", K(ret), K(tenant_id), K(version));
        } else {
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }

  return ret;
}

// No need to get the latest value of gts, if gts does not meet the conditions,
// you need to register the asynchronous callback task
int ObTsMgr::get_gts(const uint64_t tenant_id, ObTsCbTask* task, int64_t& gts)
{
  const int64_t start = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;

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
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->get_gts(task, gts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "get gts error", K(ret), K(tenant_id), KP(task));
          }
        } else {
          break;
        }
      }
    } while (OB_SUCCESS == ret);
  }
  ObTransStatistic::get_instance().add_gts_acquire_total_count(tenant_id, 1);
  // If the gts acquisition fails, you need to register the wait task,
  // and you don't need to count the total time of the gts acquire at this time.
  if (OB_SUCC(ret)) {
    const int64_t end = ObTimeUtility::fast_current_time();
    ObTransStatistic::get_instance().add_gts_acquire_total_time(tenant_id, end - start);
  }

  return ret;
}

int ObTsMgr::get_local_trans_version(const uint64_t tenant_id, ObTsCbTask* task, int64_t& gts)
{
  const int64_t start = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), KP(task));
  } else {
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->get_local_trans_version(task, gts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "get local trans version error", K(ret), K(tenant_id), KP(task));
          }
        } else {
          break;
        }
      }
    } while (OB_SUCCESS == ret);
  }
  ObTransStatistic::get_instance().add_gts_acquire_total_count(tenant_id, 1);
  // If the gts acquisition fails, you need to register the wait task,
  // and you don't need to count the total time of the gts acquire at this time.
  if (OB_SUCC(ret)) {
    const int64_t end = ObTimeUtility::fast_current_time();
    ObTransStatistic::get_instance().add_gts_acquire_total_time(tenant_id, end - start);
  }

  return ret;
}

int ObTsMgr::get_local_trans_version(
    const uint64_t tenant_id, const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts)
{
  const int64_t start = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(stc.mts_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(stc), KP(task));
  } else {
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->get_local_trans_version(stc, task, gts, receive_gts_ts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "get local trans version error", K(ret), K(tenant_id), K(stc), KP(task));
          }
        } else {
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }
  ObTransStatistic::get_instance().add_gts_acquire_total_count(tenant_id, 1);
  // If the gts acquisition fails, you need to register the wait task,
  // and you don't need to count the total time of the gts acquire at this time.
  if (OB_SUCC(ret)) {
    const int64_t end = ObTimeUtility::fast_current_time();
    ObTransStatistic::get_instance().add_gts_acquire_total_time(tenant_id, end - start);
  }

  return ret;
}

int ObTsMgr::get_gts(
    const uint64_t tenant_id, const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts)
{
  const int64_t start = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;

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
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->get_gts(stc, task, gts, receive_gts_ts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "get gts error", K(ret), K(tenant_id), K(stc), KP(task));
          }
        } else {
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }
  ObTransStatistic::get_instance().add_gts_acquire_total_count(tenant_id, 1);
  // If the gts acquisition fails, you need to register the wait task,
  // and you do not need to count the total time of the gts acquire at this time
  if (OB_SUCC(ret)) {
    const int64_t end = ObTimeUtility::fast_current_time();
    ObTransStatistic::get_instance().add_gts_acquire_total_time(tenant_id, end - start);
  }

  return ret;
}

int ObTsMgr::get_ts_sync(const uint64_t tenant_id, const int64_t timeout_us, int64_t& ts, bool& is_external_consistent)
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
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_UNLIKELY(ObTimeUtility::current_time() >= start + timeout_us)) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->get_gts(stc, NULL, ts, receive_gts_ts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "get gts error", K(ret), K(tenant_id), K(stc));
          } else {
            usleep(sleep_us);
            sleep_us = sleep_us * 2;
            sleep_us = (sleep_us >= 1000000 ? 1000000 : sleep_us);
            // rewrite ret
            ret = OB_SUCCESS;
          }
        } else {
          is_external_consistent = ts_source->is_external_consistent();
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }
  ObTransStatistic::get_instance().add_gts_acquire_total_count(tenant_id, 1);
  // If the gts acquisition fails, you need to register the wait task,
  // and you don't need to count the total time of the gts acquire at this time.
  if (OB_SUCC(ret)) {
    const int64_t end = ObTimeUtility::current_time();
    ObTransStatistic::get_instance().add_gts_acquire_total_time(tenant_id, end - start);
  }

  return ret;
}

int ObTsMgr::update_base_ts(const int64_t base_ts, const int64_t publish_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(0 > base_ts) || OB_UNLIKELY(0 > publish_version)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(base_ts), K(publish_version));
  } else {
    UpdateBaseTs functor(base_ts, publish_version);
    if (OB_FAIL(ts_source_info_map_.for_each(functor))) {
      TRANS_LOG(WARN, "iterate all ts source info failed", KR(ret));
    }
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(ERROR, "update base ts failed", KR(ret), K(base_ts), K(publish_version));
  } else {
    TRANS_LOG(INFO, "update base ts success", K(base_ts), K(publish_version));
  }
  return ret;
}

int ObTsMgr::get_base_ts(int64_t& base_ts, int64_t& publish_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", KR(ret));
  } else {
    GetBaseTs functor;
    if (OB_FAIL(ts_source_info_map_.for_each(functor))) {
      TRANS_LOG(WARN, "iterate all ts source info failed", KR(ret));
    } else {
      base_ts = functor.get_base_ts();
      publish_version = functor.get_publish_version();
    }
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(ERROR, "get base ts failed", KR(ret));
  } else {
    TRANS_LOG(INFO, "get base ts success", K(base_ts), K(publish_version));
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
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else {
          bool_ret = ts_source->is_external_consistent();
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }

  return bool_ret;
}

int ObTsMgr::update_publish_version(const uint64_t tenant_id, const int64_t publish_version, const bool for_replay)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(0 > publish_version)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(publish_version));
  } else {
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (for_replay && ts_source->is_external_consistent()) {
          // During playback, there is no need to push up the gts cache in gts mode
          break;
        } else if (OB_FAIL(ts_source->update_publish_version(publish_version))) {
          TRANS_LOG(WARN, "update publish version failed", K(ret), K(tenant_id), K(publish_version));
        } else {
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }

  return ret;
}

int ObTsMgr::get_publish_version(const uint64_t tenant_id, int64_t& publish_version)
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
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->get_publish_version(publish_version))) {
          TRANS_LOG(WARN, "get publish version error", K(ret), K(tenant_id));
        } else {
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }

  return ret;
}

int ObTsMgr::wait_gts_elapse(const uint64_t tenant_id, const int64_t ts, ObTsCbTask* task, bool& need_wait)
{
  const int64_t start = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(ts <= 0) || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(ts), KP(task));
  } else {
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->wait_gts_elapse(ts, task, need_wait))) {
          TRANS_LOG(WARN, "wait gts elapse failed", K(ret), K(ts), KP(task));
        } else {
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }
  ObTransStatistic::get_instance().add_gts_wait_elapse_total_count(tenant_id, 1);
  if (OB_SUCC(ret)) {
    const int64_t end = ObTimeUtility::fast_current_time();
    ObTransStatistic::get_instance().add_gts_wait_elapse_total_time(tenant_id, end - start);
  }
  return ret;
}

int ObTsMgr::get_gts_and_type(const uint64_t tenant_id, const MonotonicTs stc, int64_t& gts, int64_t& ts_type)
{
  int ret = OB_SUCCESS;
  MonotonicTs unused_receive_gts_ts;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr is not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else {
          ts_type = source_guard.get_ts_type();
          if (OB_FAIL(ts_source->get_gts(stc, NULL, gts, unused_receive_gts_ts))) {
            if (OB_EAGAIN != ret) {
              TRANS_LOG(WARN, "get gts failed", K(ret), K(tenant_id), K(ts_source));
            }
          } else {
            break;
          }
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }
  ObTransStatistic::get_instance().add_gts_try_acquire_total_count(tenant_id, 1);

  return ret;
}

int ObTsMgr::wait_gts_elapse(const uint64_t tenant_id, const int64_t ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(ts));
  } else {
    do {
      bool is_valid = false;
      ObTsSourceInfo* ts_source_info = NULL;
      ObITsSource* ts_source = NULL;
      ObTsSourceInfoGuard info_guard;
      ObTsSourceGuard source_guard;
      if (OB_FAIL(get_ts_source_info_opt_(tenant_id, info_guard, true, true))) {
        TRANS_LOG(WARN, "get ts source info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ts_source_info = info_guard.get_ts_source_info())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ts source info is NULL", K(ret), K(tenant_id));
      } else if (OB_FAIL(ts_source_info->get_ts_source(tenant_id, source_guard, is_valid))) {
        TRANS_LOG(WARN, "get ts source failed", K(ret), K(tenant_id));
      } else if (is_valid) {
        if (OB_ISNULL(ts_source = source_guard.get_ts_source())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ts source is NULL", K(ret));
        } else if (OB_FAIL(ts_source->wait_gts_elapse(ts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "wait gts elapse fail", K(ret), K(ts), K(tenant_id));
          }
        } else {
          break;
        }
      } else {
        PAUSE();
      }
    } while (OB_SUCCESS == ret);
  }
  ObTransStatistic::get_instance().add_gts_try_wait_elapse_total_count(tenant_id, 1);

  return ret;
}

int ObTsMgr::get_cur_ts_type(const uint64_t tenant_id, int64_t& cur_ts_type)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService& mvss = ObMultiVersionSchemaService::get_instance();
  ObSchemaGetterGuard guard;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mvss.get_tenant_full_schema_guard(tenant_id, guard))) {
    TRANS_LOG(WARN, "get schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_timestamp_service_type(tenant_id, cur_ts_type))) {
    TRANS_LOG(WARN, "get cur ts type failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

ObTsMgr& ObTsMgr::get_instance()
{
  static ObTsMgr instance;
  return instance;
}

int ObTsMgr::get_ts_source_info_opt_(const uint64_t tenant_id, ObTsSourceInfoGuard& guard,
    const bool need_create_tenant, const bool need_update_access_ts)
{
  int ret = OB_SUCCESS;
  ObTsSourceInfo* ts_source_info = NULL;
  if (OB_LIKELY(tenant_id < TS_SOURCE_INFO_CACHE_NUM)) {
    lock_.rdlock();
    ObTsSourceInfo** ptr = &(ts_source_infos_[tenant_id]);
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

int ObTsMgr::get_ts_source_info_(const uint64_t tenant_id, ObTsSourceInfoGuard& guard, const bool need_create_tenant,
    const bool need_update_access_ts)
{
  int ret = OB_SUCCESS;
  ObTsTenantInfo tenant_info(tenant_id);
  ObTsSourceInfo* ts_source_info = NULL;
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

void ObTsMgr::revert_ts_source_info_(ObTsSourceInfoGuard& guard)
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
  void* ptr = NULL;
  ObTimeGuard timeguard("add ts tenant");
  ObMemAttr memattr(tenant_id, ObModIds::OB_GTS_TASK_QUEUE);
  ObTsTenantInfo tenant_info(tenant_id);
  ObTsSourceInfo* ts_source_info = NULL;

  if (OB_ISNULL(ptr = ob_malloc(sizeof(ObTsSourceInfo), memattr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "allocate memory failed", KR(ret), K(tenant_id));
  } else {
    if (OB_ISNULL(ts_source_info = new (ptr) ObTsSourceInfo())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(ERROR, "ts source info construct failed", KR(ret), KP(ptr));
    } else {
      ObGtsSource* gts_source = ts_source_info->get_gts_source();
      ObHaGtsSource* ha_gts_source = ts_source_info->get_ha_gts_source();
      if (OB_FAIL(
              gts_source->init(tenant_id, server_, gts_request_rpc_, location_adapter_, global_timestamp_service_))) {
        TRANS_LOG(WARN, "gts_source init error", KR(ret));
      } else if (OB_FAIL(ha_gts_source->init(tenant_id, server_, location_adapter_))) {
        TRANS_LOG(WARN, "ha_gts_source init error", KR(ret));
      } else if (OB_FAIL(ts_source_info->init(tenant_id))) {
        TRANS_LOG(WARN, "ts source init failed", KR(ret));
      } else if (is_valid_no_sys_tenant_id(tenant_id) &&
                 OB_FAIL(ts_source_info->switch_ts_source(tenant_id, TS_SOURCE_GTS))) {
        TRANS_LOG(WARN, "switch ts source failed", K(ret));
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

int ObTsMgr::handle_ha_gts_response(uint64_t tenant_id, MonotonicTs srr, int64_t gts)
{
  int ret = OB_SUCCESS;
  bool update = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTsMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTsMgr not running", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(srr), K(gts));
  } else if (OB_FAIL(update_gts(tenant_id, srr, gts, TS_SOURCE_HA_GTS, update))) {
  } else if (!update) {
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "gts local cache not updated", K(tenant_id), K(srr), K(gts));
    }
  } else {
    transaction::ObGtsResponseTask* task = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < transaction::ObGtsSource::TOTAL_GTS_QUEUE_COUNT; ++i) {
      if (OB_ISNULL(task = transaction::ObGtsResponseTaskFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(ERROR, "alloc memory failed", KR(ret), KP(task));
      } else {
        if (OB_FAIL(task->init(tenant_id, i, this, TS_SOURCE_HA_GTS))) {
          TRANS_LOG(WARN, "gts task init error", KR(ret), KP(task), K(i), K(tenant_id));
        } else if (OB_FAIL(gts_worker_.push_task(tenant_id, task))) {
          TRANS_LOG(WARN, "push gts task failed", KR(ret), KP(task), K(tenant_id));
        } else {
          TRANS_LOG(DEBUG, "push gts task success", KP(task), K(tenant_id));
        }
        if (OB_SUCCESS != ret) {
          transaction::ObGtsResponseTaskFactory::free(task);
          task = NULL;
        }
      }
    }
  }

  return ret;
}

void ObQSyncLock::rdlock()
{
  do {
    if (OB_UNLIKELY(0 != ATOMIC_LOAD(&write_flag_))) {
      sched_yield();
    } else {
      const int64_t idx = qsync_.acquire_ref();
      if (OB_UNLIKELY(0 != ATOMIC_LOAD(&write_flag_))) {
        qsync_.release_ref(idx);
        sched_yield();
      } else {
        break;
      }
    }
  } while (true);
}

void ObQSyncLock::rdunlock()
{
  qsync_.release_ref();
}

void ObQSyncLock::wrlock()
{
  do {
    if (!ATOMIC_BCAS(&write_flag_, 0, 1)) {
      sched_yield();
    } else {
      if (qsync_.try_sync()) {
        break;
      } else {
        ATOMIC_STORE(&write_flag_, 0);
        sched_yield();
      }
    }
  } while (true);
}

void ObQSyncLock::wrunlock()
{
  ATOMIC_STORE(&write_flag_, 0);
}

}  // namespace transaction
}  // namespace oceanbase
