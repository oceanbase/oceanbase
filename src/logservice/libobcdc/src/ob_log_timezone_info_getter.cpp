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
 *
 * TimeZone Info Getter
 */

#define USING_LOG_PREFIX OBLOG_SCHEMA

#include "ob_log_timezone_info_getter.h"

#include "ob_log_systable_helper.h"                       // ObLogSysTableHelper
#include "ob_log_instance.h"                              // IObLogErrHandler
#include "share/ob_time_zone_info_manager.h"              // FETCH_TZ_INFO_SQL

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

int ObCDCTenantTimeZoneInfo::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("oblog_tz_info already inited", KR(ret), K(tenant_id));
  } else if (OB_FAIL(
      tz_info_map_.init(ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_HASH_BUCKET_TIME_ZONE_INFO_MAP)))) {
    // must use OB_SERVER_TENANT_ID cause alloc memory require user tenant should has its own ObMallocAllocator
    LOG_ERROR("init tz_info_map_ failed", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }

  return ret;
}

void ObCDCTenantTimeZoneInfo::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    timezone_info_version_ = OB_INVALID_VERSION;
    tz_info_map_.destroy();
  }
}

int ObCDCTenantTimeZoneInfo::set_time_zone(const ObString &timezone_str)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCDCTenantTimeZoneInfo not init", KR(ret), KPC(this));
  } else if (OB_FAIL(tz_info_wrap_.init_time_zone(timezone_str, timezone_info_version_, tz_info_map_))) {
    LOG_ERROR("tz_info_wrap set_time_zone failed", KR(ret), K(timezone_str), KPC(this));
  }

  return ret;
}

int get_tenant_tz_map_function(
    const uint64_t tenant_id,
    common::ObTZMapWrap &tz_map_wrap)
{
  return ObCDCTimeZoneInfoGetter::get_instance().get_tenant_timezone_map(tenant_id, tz_map_wrap);
}

ObCDCTimeZoneInfoGetter& ObCDCTimeZoneInfoGetter::get_instance()
{
  static ObCDCTimeZoneInfoGetter instance;
  return instance;
}

ObCDCTimeZoneInfoGetter::ObCDCTimeZoneInfoGetter()
  : inited_(false),
    tz_tid_(0),
    tz_cond_(),
    stop_flag_(true),
    mysql_proxy_(NULL),
    systable_helper_(NULL),
    err_handler_(NULL),
    lock_(ObLatchIds::OBCDC_TIMEZONE_GETTER_LOCK),
    timezone_str_(NULL),
    oblog_tz_info_map_(),
    allocator_()
{
}

ObCDCTimeZoneInfoGetter::~ObCDCTimeZoneInfoGetter()
{
  destroy();
}

int ObCDCTimeZoneInfoGetter::init(
    const char *timezone_str,
    common::ObMySQLProxy &mysql_proxy,
    IObLogSysTableHelper &systable_helper,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;
  lib::ObLabel label = ObModIds::OB_HASH_BUCKET_TIME_ZONE_INFO_MAP;
  lib::ObMemAttr tz_info_attr(OB_SYS_TENANT_ID, label);

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("schema getter has been initialized", KR(ret));
  } else if (OB_ISNULL(timezone_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(timezone_str));
  } else if (OB_FAIL(allocator_.init(TENANT_TZ_INFO_VALUE_SIZE,
      OB_MALLOC_NORMAL_BLOCK_SIZE, common::default_blk_alloc, tz_info_attr))) {
    LOG_ERROR("init allocator failed", KR(ret));
  } else if (OB_FAIL(oblog_tz_info_map_.create(MAP_BUCKET_NUM, label))) {
    LOG_ERROR("init tz_info_map_ failed", KR(ret));
  } else {
    tz_tid_ = 0;
    stop_flag_ = false;
    timezone_str_ = timezone_str;
    mysql_proxy_ = &mysql_proxy;
    systable_helper_ = &systable_helper;
    err_handler_ = &err_handler;
    inited_ = true;

    LOG_INFO("init timezone info getter succ",
        "enable_refresh_timezone_info_by_sql", need_fetch_tz_info_online_());
  }

  return ret;
}

void ObCDCTimeZoneInfoGetter::destroy()
{
  stop();

  if (inited_) {
    LOG_INFO("destroy ObCDCTimeZoneInfoGetter begin");
    inited_ = false;
    tz_tid_ = 0;

    timezone_str_ = NULL;
    mysql_proxy_ = NULL;
    systable_helper_ = NULL;
    err_handler_ = NULL;
    oblog_tz_info_map_.clear();
    oblog_tz_info_map_.destroy();
    LOG_INFO("destroy ObCDCTimeZoneInfoGetter success");
  }
}

int ObCDCTimeZoneInfoGetter::start()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;

  if (!need_fetch_tz_info_online_()) {
    // do nothing
  } else {
    if (OB_UNLIKELY(0 != tz_tid_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("timezone info thread has been started", KR(ret), K(tz_tid_));
    } else if (0 != (pthread_ret = pthread_create(&tz_tid_, NULL, tz_thread_func_, this))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("start timezone info thread fail", KR(ret), K(pthread_ret), KERRNOMSG(pthread_ret));
    } else {
      LOG_INFO("start timezone info thread succ");
    }
  }

  return ret;
}

void ObCDCTimeZoneInfoGetter::stop()
{
  if (! stop_flag_) {
    LOG_INFO("stop ObCDCTimeZoneInfoGetter begin");
    stop_flag_ = true;

    if (! need_fetch_tz_info_online_()) {
      // do nothing
    } else {
      if (0 != tz_tid_) {
        tz_cond_.signal();

        int pthread_ret = pthread_join(tz_tid_, NULL);
        if (0 != pthread_ret) {
          LOG_ERROR_RET(OB_ERR_SYS, "join timezone info thread fail", K(tz_tid_), K(pthread_ret),
              KERRNOMSG(pthread_ret));
        } else {
          LOG_INFO("stop timezone info thread succ");
        }

        tz_tid_ = 0;
      }
    }
    LOG_INFO("stop ObCDCTimeZoneInfoGetter end");
  }
}

void *ObCDCTimeZoneInfoGetter::tz_thread_func_(void *args)
{
  if (NULL != args) {
    ObCDCTimeZoneInfoGetter *tz_info_getter = static_cast<ObCDCTimeZoneInfoGetter*>(args);
    tz_info_getter->tz_routine();
  }

  return NULL;
}

int ObCDCTimeZoneInfoGetter::init_tenant_tz_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObCDCTenantTimeZoneInfo *tenant_tz_info = nullptr;

  if (OB_FAIL(create_tenant_tz_info_(tenant_id, tenant_tz_info))) {
    LOG_ERROR("create_tenant_tz_info failed", KR(ret), K(tenant_id));
  } else {
    // get or init success
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::get_tenant_tz_info(
    const uint64_t tenant_id,
    ObCDCTenantTimeZoneInfo *&tenant_tz_info)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id);
  SpinRLockGuard guard(lock_);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ob_log_timezone_info_getter not initted", KR(ret), K(tenant_id), K(exec_tenant_id));
  } else if (OB_FAIL(oblog_tz_info_map_.get_refactored(exec_tenant_id, tenant_tz_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("tenant_tz_info not exist", KR(ret), K(tenant_id), K(exec_tenant_id));
    } else {
      LOG_ERROR("get_oblog_tz_info failed", KR(ret), K(tenant_id), K(exec_tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tenant_tz_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("oblog_tz_info should not be null", KR(ret), K(tenant_id), K(exec_tenant_id));
  } else if (OB_UNLIKELY(!tenant_tz_info->is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("oblog_tz_info not init", KR(ret), K(tenant_id), K(exec_tenant_id));
  } else {
    // success
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::refresh_tenant_timezone_info_until_succ(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObCDCTenantTimeZoneInfo *tenant_tz_info = nullptr;

  if (OB_FAIL(get_tenant_tz_info(tenant_id, tenant_tz_info))) {
    LOG_ERROR("get_tenant_tz_info failed", KR(ret), K(tenant_id));
  } else {
    SpinWLockGuard guard(lock_);
    const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id);

    if (OB_FAIL(refresh_tenant_timezone_info_until_succ_(exec_tenant_id, *tenant_tz_info))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("refresh_tenant_timezone_info_until_succ_ failed", KR(ret), K(tenant_id), K(exec_tenant_id), KPC(tenant_tz_info));
      }
    } else {
      LOG_TRACE("refresh_tenant_timezone_info succ", K(tenant_id));
    }
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::get_tenant_timezone_map(
    const uint64_t tenant_id,
    ObTZMapWrap &tz_mgr_wrap)
{
  int ret = OB_SUCCESS;
  ObCDCTenantTimeZoneInfo *tenant_tz_info = nullptr;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCTimeZoneInfoGetter not inited", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_tz_info(tenant_id, tenant_tz_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_tenant_tz_info_(tenant_id, tenant_tz_info))) {
        LOG_WARN("tenant_tz_info not exist, tenant may already dropped, will ignore", KR(ret), K(tenant_id));
      }
    } else {
      LOG_ERROR("get_tenant_tz_info failed", KR(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    tz_mgr_wrap.set_tz_map(&tenant_tz_info->get_tz_map());
  }

  return ret;
}

void ObCDCTimeZoneInfoGetter::remove_tenant_tz_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObCDCTenantTimeZoneInfo *tenant_tz_info = nullptr;

  if (OB_UNLIKELY(! inited_)) {
    // ignore
  } else if (OB_FAIL(oblog_tz_info_map_.get_refactored(tenant_id, tenant_tz_info))) {
    // ignore
  } else if (OB_ISNULL(tenant_tz_info)) {
    // ignore
  } else {
    SpinWLockGuard guard(lock_);

    if (OB_FAIL(oblog_tz_info_map_.erase_refactored(tenant_id))) {
      LOG_WARN("erase_refactored tenant_tz_info from oblog_tz_info_map_ failed", KR(ret), K(tenant_id));
    }

    allocator_.free(tenant_tz_info);
    tenant_tz_info = nullptr;
  }

  LOG_INFO("remove_tenant_tz_info", KR(ret), K(tenant_id));
}

void ObCDCTimeZoneInfoGetter::tz_routine()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    while (! stop_flag_ && OB_SUCC(ret)) {
      if (OB_FAIL(refresh_all_tenant_timezone_info_())) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_WARN("timezone_info_getter_ refresh_timezone_info_ fail", KR(ret));
          ret = OB_SUCCESS;
        }
      } else {
        LOG_INFO("timezone_info_getter_ refresh_timezone_info_ succ");
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

bool ObCDCTimeZoneInfoGetter::need_fetch_tz_info_online_() const
{
  return ! is_data_dict_refresh_mode(TCTX.refresh_mode_);
}

int ObCDCTimeZoneInfoGetter::create_tenant_tz_info_(
    const uint64_t tenant_id,
    ObCDCTenantTimeZoneInfo *&tenant_tz_info)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id);
  SpinWLockGuard guard(lock_);

  // double check in case of create_tenant_tz_info_ invoked multi-times
  if (OB_FAIL(oblog_tz_info_map_.get_refactored(exec_tenant_id, tenant_tz_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // 1. query the initial timezone_info_version
      // 2. refresh timezone_info until successful
      // 3. initialize tz_info_wrap_
      const int64_t start_ts = get_timestamp();
      if (OB_ISNULL(tenant_tz_info = static_cast<ObCDCTenantTimeZoneInfo*>(allocator_.alloc()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("oblog_tz_info is not valid", KR(ret), K(tenant_id), KP(tenant_tz_info));
      } else if (OB_ISNULL(timezone_str_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("timezone_str is null", KR(ret), K(tenant_id), K(timezone_str_));
      } else {
        new(tenant_tz_info) ObCDCTenantTimeZoneInfo();

        if (OB_FAIL(tenant_tz_info->init(exec_tenant_id))) {
          LOG_ERROR("tenant_tz_info init failed", KR(ret), K(tenant_id), K(exec_tenant_id), KPC(tenant_tz_info));
        } else if (need_fetch_tz_info_online_()) {
          if (OB_FAIL(query_timezone_info_version_(exec_tenant_id, tenant_tz_info->timezone_info_version_))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              // Not present, normal, tenant has not imported time zone table
              LOG_TRACE("query_timezone_info_version_, timezone_info_version not exist", KR(ret), K(tenant_id), K(exec_tenant_id));
              ret = OB_SUCCESS;
            } else {
              LOG_ERROR("query_timezone_info_version_ fail", KR(ret), K(tenant_tz_info));
            }
          } else if (OB_FAIL(refresh_tenant_timezone_info_until_succ_(exec_tenant_id, *tenant_tz_info))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("refresh_tenant_timezone_info_util_succ fail", KR(ret), K(tenant_id));
            }
          }
        } else {
          if (OB_FAIL(refresh_tenant_timezone_info_from_local_file_(tenant_id, tenant_tz_info->tz_info_map_))) {
            if (OB_IO_ERROR == ret) {
              LOG_INFO("refresh_tenant_timezone_info_from_local_file_ tz_info may not exist "
                  "or tenant is not oracle mode, ignore.", KR(ret), K(tenant_id));
              ret = OB_SUCCESS;
            } else {
              LOG_ERROR("refresh_tenant_timezone_info_from_local_file_ failed", KR(ret), K(tenant_id));
            }
          } else {
            LOG_INFO("refresh_tenant_timezone_info_from_local_file_ success", K(tenant_id));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(tenant_tz_info->set_time_zone(ObString(timezone_str_)))) {
          LOG_ERROR("set tenant_tz_info failed", KR(ret), K(tenant_id), KPC(tenant_tz_info));
          // regist into tz_info_map even if OB_ENTRY_NOT_EXIST(tenant doesn't have timezone info)
        } else if (OB_FAIL(oblog_tz_info_map_.set_refactored(exec_tenant_id, tenant_tz_info))) {
          LOG_ERROR("insert obcdc_tenant_tz_info into tz_info_map failed", KR(ret), K(tenant_id));
        } else {
          const int64_t cost_time_usec = get_timestamp() - start_ts;
          LOG_INFO("create tenant timezone info for obcdc success", KR(ret), K(tenant_id), K(exec_tenant_id), K(cost_time_usec));
        }
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(tenant_tz_info)) {
        allocator_.free(tenant_tz_info);
        tenant_tz_info = nullptr;
      }
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(tenant_tz_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_tz_info should no be null", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::refresh_tenant_timezone_info_based_on_version_(
    const uint64_t tenant_id,
    ObCDCTenantTimeZoneInfo &oblog_tz_info)
{
  int ret = OB_SUCCESS;
  int64_t tz_info_version = OB_INVALID_TIMESTAMP;

  if (! inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("oblog_timezone_info_getter not inited");
  } else if (OB_FAIL(query_timezone_info_version_(tenant_id, tz_info_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // Not present, normal, tenant has not imported time zone table
      LOG_TRACE("query_timezone_info_version_, timezone_info_version not exist", K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("query_timezone_info_version_ fail", KR(ret), K(tz_info_version));
    }
  } else if (! oblog_tz_info.need_update_tz_info(tz_info_version)) {
    // do nothing
    LOG_INFO("timezone_info_version is unchanged, don't need to update timezone info", K(oblog_tz_info));
  } else {
    // Version change, active refresh
    if (OB_FAIL(refresh_tenant_timezone_info_map_(tenant_id, oblog_tz_info))) {
      LOG_ERROR("refresh_tenant_timezone_info_map_ fail", KR(ret), K(oblog_tz_info));
    } else {
      oblog_tz_info.update_timezone_info_version(tz_info_version);
      LOG_TRACE("update tenant timezone info for obcdc success", KR(ret), K(tenant_id), K(tz_info_version));
    }
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::refresh_tenant_timezone_info_map_(
    const uint64_t tenant_id,
    ObCDCTenantTimeZoneInfo &tenant_tz_info)
{
  int ret = OB_SUCCESS;
  ObTZInfoMap &tz_info_map = tenant_tz_info.tz_info_map_;

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = nullptr;
    if (OB_ISNULL(mysql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mysql_proxy_ is null", KR(ret), K(mysql_proxy_));
    } else if (! need_fetch_timezone_info_by_tennat_()) {
      if (OB_FAIL(mysql_proxy_->read(res, ObTimeZoneInfoManager::FETCH_TZ_INFO_SQL))) {
        LOG_WARN("fail to execute sql", KR(ret));
        ret = OB_NEED_RETRY;
      }
    } else if (OB_FAIL(mysql_proxy_->read(res, tenant_id, ObTimeZoneInfoManager::FETCH_TENANT_TZ_INFO_SQL))) {
      LOG_WARN("fail to execute sql", KR(ret));
      ret = OB_NEED_RETRY;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(result = res.get_result())) {
      LOG_WARN("fail to get result", K(result));
      ret = OB_NEED_RETRY;
    } else if (OB_FAIL(ObTimeZoneInfoManager::fill_tz_info_map(*result, tz_info_map))) {
      LOG_ERROR("fill_tz_info_map fail", KR(ret), K(tenant_id));
    } else if (OB_FAIL(export_timezone_info_(tz_info_map))) {
      LOG_ERROR("export_timezone_info failed", KR(ret), K(tenant_id));
    } else {
      // success
    }
  }

  return ret;
}

// 226 does a tenant split of the time zone table and needs to maintain a tz_info_map for each tenant
int ObCDCTimeZoneInfoGetter::refresh_all_tenant_timezone_info_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mysql_proxy_ is null", KR(ret), K(mysql_proxy_));
  } else {
    // Requires locking to prevent multi-threaded access: formatter and ObCDCTimeZoneInfoGetter query threads themselves
    SpinWLockGuard guard(lock_);

    for (ObLogTZInfoMap::iterator iter = oblog_tz_info_map_.begin(); iter != oblog_tz_info_map_.end(); iter++) {
      const uint64_t tenant_id = iter->first;
      ObCDCTenantTimeZoneInfo *tenant_tz_info = iter->second;

      if (OB_FAIL(refresh_tenant_timezone_info_based_on_version_(tenant_id, *tenant_tz_info))) {
        LOG_WARN("refresh_tenant_timezone_info_based_on_version_ failed", KR(ret), K(tenant_id));
      }
    }
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::query_timezone_info_version_(
    const uint64_t tenant_id,
    int64_t &timezone_info_version)
{
  int ret = OB_SUCCESS;
  bool done = false;

  if (OB_ISNULL(systable_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("systable_helper_ is null", KR(ret), K(systable_helper_));
  } else {
    while (! done && OB_SUCC(ret) && ! stop_flag_) {
      if (OB_FAIL(systable_helper_->query_timezone_info_version(tenant_id, timezone_info_version))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("systable_helper_ query_timezone_info_version fail", KR(ret), K(tenant_id),
              K(timezone_info_version));
        } else {
          LOG_TRACE("query_timezone_info_version failed, tenant may not has timezone info(mysql mode), \
              or not exist(may be already dropped)", KR(ret), K(tenant_id));
        }
      } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == timezone_info_version)) {
        LOG_ERROR("timezone_info_version is not valid", K(tenant_id), K(timezone_info_version));
        ret = OB_ERR_UNEXPECTED;
      } else {
        done = true;
      }

      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
        ob_usleep(100L * 1000L);
      }
    }
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::refresh_tenant_timezone_info_until_succ_(
    const uint64_t tenant_id,
    ObCDCTenantTimeZoneInfo &tenant_tz_info)
{
  int ret = OB_SUCCESS;
  bool done = false;
  int64_t retry_cnt = 0;
  if (! need_fetch_tz_info_online_()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support update timezone_info cause online schema is not support in current mode", KR(ret),
        "refresh_mode", TCTX.refresh_mode_,
        "fetch_log_mode", TCTX.fetching_mode_);
  }

  while (! done && OB_SUCC(ret) && ! stop_flag_) {
    if (OB_FAIL(refresh_tenant_timezone_info_map_(tenant_id, tenant_tz_info))) {
      LOG_WARN("refresh_tenant_timezone_info_map_ fail", KR(ret), K(tenant_id));
    } else {
      done = true;
    }

    if (OB_NEED_RETRY == ret) {
      retry_cnt++;
      if (retry_cnt % 1000) {
        // LOG retry  info every 10 sec.
        LOG_WARN("retry to refresh tenant_timezone_info", KR(ret), K(tenant_id), K(retry_cnt));
      }
      ret = OB_SUCCESS;
      ob_usleep(10L * 1000L); // retry interval 10 ms
    }
  }

  if (stop_flag_) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::export_timezone_info_(common::ObTZInfoMap &tz_info_map)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  common::ObRequestTZInfoResult tz_info_res;
  const char *tz_info_fpath = TCONF.timezone_info_fpath.str();
  // 1. serialize timezone_info;
  // 2. create tmp file;
  // 3. write to tmp_file;
  // 4. rename tmp_file to real file_name.
  auto tz_info_op = [&tz_info_res](const ObTZIDKey &key, ObTimeZoneInfoPos *tz_info)
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(tz_info)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid tz_info", KR(tmp_ret), K(key));
    } else if (OB_TMP_FAIL(tz_info_res.tz_array_.push_back(*tz_info))) {
      LOG_ERROR_RET(tmp_ret, "push_back tz_info into ObRequestTZInfoResult failed", KR(tmp_ret), K(key));
    }
    return OB_SUCCESS == tmp_ret;
  };

  if (OB_FAIL(tz_info_map.id_map_->for_each(tz_info_op))) {
    LOG_ERROR("generate ObRequestTZInfoResult failed", KR(ret));
  } else if (0 >= tz_info_res.tz_array_.count()) {
    // skip empty tz_info_result.
  } else if (OB_UNLIKELY(0 >= (buf_len = tz_info_res.get_serialize_size()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ObRequestTZInfoResult serialize size", KR(ret), K(buf_len));
  } else if (OB_ISNULL(buf = static_cast<char*>(ob_cdc_malloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc buf for ObRequestTZInfoResult failed", KR(ret), K(buf_len));
  } else if (OB_FAIL(tz_info_res.serialize(buf, buf_len, pos))) {
    LOG_ERROR("serialize ObRequestTZInfoResult failed", KR(ret), K(buf_len), K(pos), KP(buf));
  } else if (OB_FAIL(write_to_file(tz_info_fpath, buf, pos))) {
    LOG_ERROR("write timezone info to file failed", KR(ret), K(buf_len), KP(buf), KCSTRING(tz_info_fpath));
  } else {
    LOG_DEBUG("export_timezone_info succ", K(tz_info_res));
  }

  if (OB_NOT_NULL(buf)) {
    ob_cdc_free(buf);
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::refresh_tenant_timezone_info_from_local_file_(
    const uint64_t tenant_id,
    common::ObTZInfoMap &tz_info_map)
{
  int ret = OB_SUCCESS;
  ObDictTenantInfoGuard dict_tenant_info_guard;
  ObDictTenantInfo *tenant_info = nullptr;

  if (need_fetch_tz_info_online_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("only effect while online schema not available)", KR(ret));
  } else if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
      tenant_id,
      dict_tenant_info_guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
    } else {
      LOG_INFO("get_tenant_info_guard failed cause tenant_meta not exist, ignore.", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_info is nullptr", KR(ret), K(tenant_id));
  } else if (common::ObCompatibilityMode::MYSQL_MODE == tenant_info->get_compatibility_mode()) {
    // ignore if mysql mode
  } else if (OB_FAIL(import_timezone_info_(tz_info_map))) {
    LOG_ERROR("import_timezone_info failed", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObCDCTimeZoneInfoGetter::import_timezone_info_(common::ObTZInfoMap &tz_info_map)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  // currently timezone_info.conf is about 3.7M, prealloc 12M in case of extend of timezone_info
  const int64_t buf_len = 12 * _M_;
  int64_t pos = 0;
  common::ObRequestTZInfoResult tz_info_res;
  const char *tzinfo_fpath = TCONF.timezone_info_fpath.str();

  if (OB_ISNULL(buf = static_cast<char*>(ob_cdc_malloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory to load tz_info from local file failed", KR(ret), K(buf_len));
  } else if (OB_FAIL(read_from_file(tzinfo_fpath, buf, buf_len))) {
    LOG_ERROR("read timezone_info from local file failed", KR(ret), K(buf_len), KP(buf), KCSTRING(tzinfo_fpath));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid timezone_info load from local config file", KR(ret), K(buf_len), KP(buf));
  } else if (OB_FAIL(tz_info_res.deserialize(buf, buf_len, pos))) {
    LOG_ERROR("deserialize tz_info_res failed", KR(ret), K(buf_len), K(pos), KP(buf));
  } else {
    const int64_t tz_info_cnt = tz_info_res.tz_array_.count();
    common::ObTimeZoneInfoPos *stored_tz_info = nullptr;

    for (int idx= 0; OB_SUCC(ret) && idx < tz_info_cnt; idx++) {
      ObTimeZoneInfoPos &new_tz_info = tz_info_res.tz_array_.at(idx);

      if (OB_FAIL(tz_info_map.id_map_->get(new_tz_info.get_tz_id(), stored_tz_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get stored_tz_info, should not happened", KR(ret),
              "tz_id", new_tz_info.get_tz_id(),
              "tz_name", new_tz_info.get_tz_name());
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTimeZoneInfoManager::set_tz_info_map(stored_tz_info, new_tz_info, tz_info_map))) {
          LOG_WARN("fail to set tz_info map", K(ret));
        } else {
          // NO NEED TO UPDATE TENANT_TZ_VERSION WHILE USING LOCAL TZ_INFO_FILE
          // tenant->update_timezone_info_version(tz_info_res.last_version_);
        }
      }

      if (OB_NOT_NULL(stored_tz_info)) {
        tz_info_map.id_map_->revert(stored_tz_info);
        stored_tz_info = nullptr;
      }
    } // end for
  }

  if (OB_NOT_NULL(buf)) {
    ob_cdc_free(buf);
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
