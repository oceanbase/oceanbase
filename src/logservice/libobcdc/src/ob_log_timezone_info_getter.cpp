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

#define USING_LOG_PREFIX OBLOG

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

ObLogTimeZoneInfoGetter::ObLogTimeZoneInfoGetter()
  : inited_(false),
    tz_tid_(0),
    tz_cond_(),
    stop_flag_(true),
    mysql_proxy_(NULL),
    systable_helper_(NULL),
    err_handler_(NULL),
    lock_(ObLatchIds::OBCDC_TIMEZONE_GETTER_LOCK),
    tenant_mgr_(NULL),
    timezone_str_(NULL)
{
}

ObLogTimeZoneInfoGetter::~ObLogTimeZoneInfoGetter()
{
  destroy();
}

int ObLogTimeZoneInfoGetter::init(
    const char *timezone_str,
    common::ObMySQLProxy &mysql_proxy,
    IObLogSysTableHelper &systable_helper,
    IObLogTenantMgr &tenant_mgr,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("schema getter has been initialized", KR(ret));
  } else if (OB_ISNULL(timezone_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(timezone_str));
  } else {
    tz_tid_ = 0;
    stop_flag_ = false;
    timezone_str_ = timezone_str;
    mysql_proxy_ = &mysql_proxy;
    systable_helper_ = &systable_helper;
    tenant_mgr_ = &tenant_mgr;
    err_handler_ = &err_handler;
    inited_ = true;

    LOG_INFO("init timezone info getter succ", "is_online_tz_info_available", is_online_tz_info_available());
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

  if (is_online_tz_info_available()) {
    if (OB_UNLIKELY(0 != tz_tid_)) {
      LOG_ERROR("timezone info thread has been started", K(tz_tid_));
      ret = OB_NOT_SUPPORTED;
    } else if (0 != (pthread_ret = pthread_create(&tz_tid_, NULL, tz_thread_func_, this))) {
      LOG_ERROR("start timezone info thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_INFO("start timezone info thread succ");
    }
  }

  return ret;
}

void ObLogTimeZoneInfoGetter::stop()
{
  stop_flag_ = true;

  if (is_online_tz_info_available()) {
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
    if (OB_FAIL(refresh_tenant_timezone_info_(OB_SYS_TENANT_ID))) {
      LOG_WARN("refresh_sys_tenant_timezone_info fail", KR(ret));
    }
  } else {
    // refresh by tenant
    if (OB_FAIL(refresh_all_tenant_timezone_info_())) {
     LOG_WARN("fail to refresh all tenant timezone info", KR(ret));
    }
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::refresh_tenant_timezone_info_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (! is_online_tz_info_available()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("refresh tenant_timezone_info only avaliable when obcdc is using online schema", KR(ret));
  } else {
    ret = refresh_tenant_timezone_info_based_on_version_(tenant_id);
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::refresh_tenant_timezone_info_based_on_version_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t tz_info_version = OB_INVALID_TIMESTAMP;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("timezone_info_getter_ not inited", KR(ret), K_(inited));
  } else if (OB_ISNULL(tenant_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_mgr_ is NULL", KR(ret));
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
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant", KR(ret), K(tenant_id), K(tenant));
  } else if (tz_info_version <= tenant->get_timezone_info_version()) {
    // do nothing
    LOG_INFO("timezone_info_version is unchanged, don't need to update timezone info", K(tenant_id),
        "current_tz_info_version", tenant->get_timezone_info_version(), K(tz_info_version));
  } else {
    // Version change, active refresh
    if (OB_FAIL(fetch_tenant_timezone_info_(tenant_id, tenant->get_tz_info_map()))) {
      LOG_ERROR("fetch_tenant_timezone_info_ fail", KR(ret), K(tenant_id));
    } else {
      // update version
      tenant->update_timezone_info_version(tz_info_version);
    }
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::refresh_tenant_timezone_info_from_local_file_(
    const uint64_t tenant_id,
    common::ObTZInfoMap &tz_info_map)
{
  int ret = OB_SUCCESS;
  ObDictTenantInfoGuard dict_tenant_info_guard;
  ObDictTenantInfo *tenant_info = nullptr;

  if (is_online_tz_info_available()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("only effect in data_dict mode)", KR(ret));
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

int ObLogTimeZoneInfoGetter::fetch_tenant_timezone_info_(
    const uint64_t tenant_id,
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
      } else if (OB_FAIL(export_timezone_info_(*tz_info_map))) {
        LOG_ERROR("export_timezone_info failed", KR(ret), K(tenant_id));
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

      if (OB_FAIL(refresh_tenant_timezone_info_(tenant_id))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("refresh_tenant_timezone_info_ fail", KR(ret), K(tenant_id));
        } else {
          // tenant not exist, reset ret
          ret = OB_SUCCESS;
        }
      }
    } // for
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::init_tz_info_wrap(
    const uint64_t tenant_id,
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
  } else if (is_online_tz_info_available()) {
    if (OB_FAIL(query_timezone_info_version_(tenant_id, tz_info_version))) {
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
  } else {
    if (OB_FAIL(refresh_tenant_timezone_info_from_local_file_(tenant_id, tz_info_map))) {
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


int ObLogTimeZoneInfoGetter::query_timezone_info_version_(
    const uint64_t tenant_id,
    int64_t &timezone_info_version)
{
  int ret = OB_SUCCESS;
  bool done = false;

  if (OB_ISNULL(systable_helper_)) {
    LOG_ERROR("systable_helper_ is null", K(systable_helper_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    while (! done && OB_SUCC(ret)) {
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
        ob_usleep(100L * 1000L);
      }
    }
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::fetch_tenant_timezone_info_util_succ(
    const uint64_t tenant_id,
    ObTZInfoMap *tz_info_map)
{
  int ret = OB_SUCCESS;
  bool done = false;
  if (! is_online_tz_info_available()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support update timezone_info cause online schema is not support in current mode", KR(ret),
        "refresh_mode", TCTX.refresh_mode_,
        "fetch_log_mode", TCTX.fetching_mode_);
  }

  while (! done && OB_SUCC(ret)) {
    if (OB_FAIL(fetch_tenant_timezone_info_(tenant_id, tz_info_map))) {
      LOG_WARN("fetch_tenant_timezone_info_ fail", KR(ret), K(tenant_id));
    } else {
      done = true;
    }

    if (OB_NEED_RETRY == ret) {
      ret = OB_SUCCESS;
      ob_usleep(100L * 1000L);
    }
  }

  return ret;
}

int ObLogTimeZoneInfoGetter::get_tenant_timezone_map(
    const uint64_t tenant_id,
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

int ObLogTimeZoneInfoGetter::export_timezone_info_(common::ObTZInfoMap &tz_info_map)
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

  if (OB_FAIL(tz_info_map.id_map_.for_each(tz_info_op))) {
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

int ObLogTimeZoneInfoGetter::import_timezone_info_(common::ObTZInfoMap &tz_info_map)
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

      if (OB_FAIL(tz_info_map.id_map_.get(new_tz_info.get_tz_id(), stored_tz_info))) {
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
        tz_info_map.id_map_.revert(stored_tz_info);
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
