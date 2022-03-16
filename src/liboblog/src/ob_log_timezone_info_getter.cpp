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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_timezone_info_getter.h"

#include "ob_log_systable_helper.h"                       // ObLogSysTableHelper
#include "ob_log_instance.h"                              // IObLogErrHandler
#include "share/ob_time_zone_info_manager.h"              // FETCH_TZ_INFO_SQL

namespace oceanbase
{
namespace liboblog
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

ObLogTimeZoneInfoGetter::ObLogTimeZoneInfoGetter() : inited_(false),
                                         tz_tid_(0),
                                         tz_cond_(),
                                         stop_flag_(true),
                                         mysql_proxy_(NULL),
                                         systable_helper_(NULL),
                                         err_handler_(NULL),
                                         lock_(),
                                         tenant_mgr_(NULL),
                                         timezone_str_(NULL)
{
}

ObLogTimeZoneInfoGetter::~ObLogTimeZoneInfoGetter()
{
  destroy();
}

int ObLogTimeZoneInfoGetter::init(const char *timezone_str,
    common::ObMySQLProxy &mysql_proxy,
    IObLogSysTableHelper &systable_helper,
    IObLogTenantMgr &tenant_mgr,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("schema getter has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(timezone_str)) {
    LOG_ERROR("invalid argument", K(timezone_str));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tz_tid_ = 0;
    stop_flag_ = false;
    timezone_str_ = timezone_str;
    mysql_proxy_ = &mysql_proxy;
    systable_helper_ = &systable_helper;
    tenant_mgr_ = &tenant_mgr;
    err_handler_ = &err_handler;
    inited_ = true;

    LOG_INFO("init timezone info getter succ");
  }
  return ret;
}

void ObLogTimeZoneInfoGetter::destroy()
{
  stop();

  inited_ = false;
  tz_tid_ = 0;

  timezone_str_ = NULL;
  mysql_proxy_ = NULL;
  systable_helper_ = NULL;
  tenant_mgr_ = NULL;
  err_handler_ = NULL;
}

int ObLogTimeZoneInfoGetter::start()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;

  if (OB_UNLIKELY(0 != tz_tid_)) {
    LOG_ERROR("timezone info thread has been started", K(tz_tid_));
    ret = OB_NOT_SUPPORTED;
  } else if (0 != (pthread_ret = pthread_create(&tz_tid_, NULL, tz_thread_func_, this))) {
    LOG_ERROR("start timezone info thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_INFO("start timezone info thread succ");
  }

  return ret;
}

void ObLogTimeZoneInfoGetter::stop()
{
  stop_flag_ = true;

  if (0 != tz_tid_) {
    tz_cond_.signal();

    int pthread_ret = pthread_join(tz_tid_, NULL);
    if (0 != pthread_ret) {
      LOG_ERROR("join timezone info thread fail", K(tz_tid_), K(pthread_ret),
          KERRNOMSG(pthread_ret));
    } else {
      LOG_INFO("stop timezone info thread succ");
    }

    tz_tid_ = 0;
  }
}

void *ObLogTimeZoneInfoGetter::tz_thread_func_(void *args)
{
  if (NULL != args) {
    ObLogTimeZoneInfoGetter *tz_info_getter = static_cast<ObLogTimeZoneInfoGetter*>(args);
    tz_info_getter->tz_routine();
  }

  return NULL;
}

void ObLogTimeZoneInfoGetter::tz_routine()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    while (! stop_flag_ && OB_SUCCESS == ret && tenant_mgr_->is_inited()) {
      if (OB_FAIL(query_timezone_info_version_and_update_())) {
        LOG_ERROR("query_timezone_info_version_and_update_ fail", KR(ret));
      }

      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
      }

      tz_cond_.timedwait(QUERY_TIMEZONE_INFO_VERSION_INTERVAL);
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
      if (NULL != err_handler_) {
        err_handler_->handle_error(ret, "timezone info thread exits, err=%d", ret);
      }
      stop_flag_ = true;
    }
  }

  LOG_INFO("timezone info thread exits", KR(ret), K_(stop_flag));
}

int ObLogTimeZoneInfoGetter::query_timezone_info_version_and_update_()
{
  int ret = OB_SUCCESS;

  // Version change, active refresh
  if (OB_FAIL(refresh_timezone_info_())) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("timezone_info_getter_ refresh_timezone_info_ fail", KR(ret));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("timezone_info_getter_ refresh_timezone_info_ fail", KR(ret));
    }
  } else {
    LOG_INFO("timezone_info_getter_ refresh_timezone_info_ succ");
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::refresh_timezone_info_()
{
  int ret = OB_SUCCESS;
  // Requires locking to prevent multi-threaded access: formatter and ObLogTimeZoneInfoGetter query threads themselves
  ObSpinLockGuard guard(lock_);
  const bool fetch_timezone_info_by_tennat = need_fetch_timezone_info_by_tennat_();

  if (! fetch_timezone_info_by_tennat) {
    // Global use of a time zone table
    if (OB_FAIL(refresh_tenant_timezone_info_based_on_version_(OB_SYS_TENANT_ID))) {
      LOG_WARN("refresh_sys_tenant_timezone_info_based_on_version_ fail", KR(ret));
    }
  } else {
    // refresh by tenant
    if (OB_FAIL(refresh_all_tenant_timezone_info_())) {
     LOG_WARN("fail to refresh all tenant timezone info", KR(ret));
    }
  }

  return ret;
}

bool ObLogTimeZoneInfoGetter::need_fetch_timezone_info_by_tennat_() const
{
  return GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260;
}

int ObLogTimeZoneInfoGetter::refresh_tenant_timezone_info_based_on_version_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t tz_info_version = OB_INVALID_TIMESTAMP;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;

  if (OB_ISNULL(tenant_mgr_)) {
    LOG_ERROR("tenant_mgr_ is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(query_timezone_info_version_(tenant_id, tz_info_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // Not present, normal, tenant has not imported time zone table
      LOG_INFO("query_timezone_info_version_, timezone_info_version not exist", K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("query_timezone_info_version_ fail", KR(ret), K(tz_info_version));
    }
  } else if (OB_FAIL(tenant_mgr_->get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // No need to deal with tenant non-existence, deletion
      LOG_INFO("tenant not exist, do nothing", K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("get tenant fail", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("invalid tenant", K(tenant_id), K(tenant));
    ret = OB_ERR_UNEXPECTED;
  } else if (tz_info_version == tenant->get_timezone_info_version()) {
    // do nothing
    LOG_INFO("timezone_info_version is unchanged, don't need to update timezone info", K(tenant_id),
        "current_tz_info_version", tenant->get_timezone_info_version(), K(tz_info_version));
  } else {
    // Version change, active refresh
    if (OB_FAIL(refresh_tenant_timezone_info_(tenant_id, tenant->get_tz_info_map()))) {
      LOG_ERROR("refresh_tenant_timezone_info_ fail", KR(ret), K(tenant_id));
    } else {
      // update version
      tenant->update_timezone_info_version(tz_info_version);
    }
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::refresh_tenant_timezone_info_(const uint64_t tenant_id,
    common::ObTZInfoMap *tz_info_map)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tz_info_map)) {
    LOG_WARN("get tenant timezone info map fail", KR(ret), K(tenant_id), K(tz_info_map));
    ret = OB_ERR_UNEXPECTED;
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_ISNULL(mysql_proxy_)) {
        LOG_ERROR("mysql_proxy_ is null", K(mysql_proxy_));
        ret = OB_ERR_UNEXPECTED;
      } else if (! need_fetch_timezone_info_by_tennat_()) {
        if (OB_FAIL(mysql_proxy_->read(res, ObTimeZoneInfoManager::FETCH_TZ_INFO_SQL))) {
          LOG_WARN("fail to execute sql", KR(ret));
          ret = OB_NEED_RETRY;
        }
      } else {
        if (OB_FAIL(mysql_proxy_->read(res, tenant_id, ObTimeZoneInfoManager::FETCH_TENANT_TZ_INFO_SQL))) {
          LOG_WARN("fail to execute sql", KR(ret));
          ret = OB_NEED_RETRY;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(result = res.get_result())) {
        LOG_WARN("fail to get result", K(result));
        ret = OB_NEED_RETRY;
      } else if (OB_FAIL(ObTimeZoneInfoManager::fill_tz_info_map(*result, *tz_info_map))) {
        LOG_ERROR("fill_tz_info_map fail", KR(ret), K(tenant_id));
      }
    }
  }

  return ret;
}

// 226 does a tenant split of the time zone table and needs to maintain a tz_info_map for each tenant
int ObLogTimeZoneInfoGetter::refresh_all_tenant_timezone_info_()
{
  int ret = OB_SUCCESS;
  std::vector<uint64_t> all_tenant_ids;

  if (OB_FAIL(tenant_mgr_->get_all_tenant_ids(all_tenant_ids))) {
    LOG_WARN("fail to get all tenant ids", KR(ret));
  } else if (OB_ISNULL(mysql_proxy_)) {
    LOG_ERROR("mysql_proxy_ is null", K(mysql_proxy_));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < all_tenant_ids.size(); idx++) {
      const uint64_t tenant_id = all_tenant_ids[idx];

      if (OB_FAIL(refresh_tenant_timezone_info_based_on_version_(tenant_id))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("refresh_tenant_timezone_info_based_on_version_ fail", KR(ret), K(tenant_id));
        } else {
          // tenant not exist, reset ret
          ret = OB_SUCCESS;
        }
      }
    } // for
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::init_tz_info_wrap(const uint64_t tenant_id,
    int64_t &tz_info_version,
    ObTZInfoMap &tz_info_map,
    ObTimeZoneInfoWrap &tz_info_wrap)
{
  int ret = OB_SUCCESS;
  // 1. query the initial timezone_info_version
  // 2. refresh timezone_info until successful
  // 3. initialize tz_info_wrap_
  tz_info_version = OB_INVALID_TIMESTAMP;

  if (OB_ISNULL(timezone_str_)) {
    LOG_ERROR("timezone_str is null", K(timezone_str_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(query_timezone_info_version_(tenant_id, tz_info_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // Not present, normal, tenant has not imported time zone table
      LOG_INFO("query_timezone_info_version_, timezone_info_version not exist", K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("query_timezone_info_version_ fail", KR(ret), K(tenant_id), K(tz_info_version));
    }
  } else if (OB_FAIL(fetch_tenant_timezone_info_util_succ(tenant_id, &tz_info_map))) {
    LOG_ERROR("fetch_tenant_timezone_info_util_succ fail", KR(ret), K(tenant_id));
  } else {
    // succ
  }


  if (OB_SUCC(ret)) {
    if (OB_FAIL(tz_info_wrap.init_time_zone(ObString(timezone_str_), tz_info_version, tz_info_map))) {
      LOG_ERROR("tz_info_wrap init_time_zone fail", KR(ret), K(tenant_id), "timezone", timezone_str_,
          K(tz_info_version), K(tz_info_wrap));
    } else {
      LOG_INFO("tz_info_wrap init_time_zone succ", K(tenant_id), "timezone", timezone_str_,
          K(tz_info_version), K(tz_info_wrap));
    }
  }

  return ret;
}


int ObLogTimeZoneInfoGetter::query_timezone_info_version_(const uint64_t tenant_id,
    int64_t &timezone_info_version)
{
  int ret = OB_SUCCESS;
  bool done = false;

  if (OB_ISNULL(systable_helper_)) {
    LOG_ERROR("systable_helper_ is null", K(systable_helper_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    while (! done && OB_SUCCESS == ret) {
      if (OB_FAIL(systable_helper_->query_timezone_info_version(tenant_id, timezone_info_version))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("systable_helper_ query_timezone_info_version fail", KR(ret), K(tenant_id),
              K(timezone_info_version));
        }
      } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == timezone_info_version)) {
        LOG_ERROR("timezone_info_version is not valid", K(tenant_id), K(timezone_info_version));
        ret = OB_ERR_UNEXPECTED;
      } else {
        done = true;
      }

      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
        usleep(100L * 1000L);
      }
    }
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::fetch_tenant_timezone_info_util_succ(const uint64_t tenant_id,
    ObTZInfoMap *tz_info_map)
{
  int ret = OB_SUCCESS;
  bool done = false;

  while (! done && OB_SUCCESS == ret) {
    if (OB_FAIL(refresh_tenant_timezone_info_(tenant_id, tz_info_map))) {
      LOG_WARN("refresh_tenant_timezone_info_ fail", KR(ret), K(tenant_id));
    } else {
      done = true;
    }

    if (OB_NEED_RETRY == ret) {
      ret = OB_SUCCESS;
      usleep(100L * 1000L);
    }
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::get_tenant_timezone_map(const uint64_t tenant_id,
    ObTZMapWrap &tz_mgr_wrap)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *log_tenant_mgr = nullptr;
  ObTZInfoMap *tz_info_map = nullptr;

  if (OB_ISNULL(log_tenant_mgr = TCTX.tenant_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("log tenant mgr not init", K(ret));
  } else if (OB_FAIL(log_tenant_mgr->get_tenant_tz_map(tenant_id, tz_info_map))) {
    LOG_WARN("log tenant mgr get tenant tz map failed", KR(ret), K(tenant_id));
  } else {
    tz_mgr_wrap.set_tz_map(tz_info_map);
  }

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
