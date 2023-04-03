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

#define USING_LOG_PREFIX SHARE_LOCATION

#include "share/location_cache/ob_tablet_ls_service.h"
#include "share/ob_share_util.h"
#include "share/cache/ob_cache_name_define.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/string/ob_sql_string.h"
#include "lib/ob_running_mode.h"
#include "observer/ob_server_struct.h"
#include "common/ob_timeout_ctx.h"
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService

namespace oceanbase
{
using namespace common;

namespace share
{
int ObTabletLSService::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t user_thread_cnt =
      lib::is_mini_mode()
      ? MINI_MODE_UPDATE_THREAD_CNT
      : static_cast<int64_t>(GCONF.location_refresh_thread_count);
  const int64_t user_queue_size =
      lib::is_mini_mode()
      ? MINI_MODE_USER_TASK_QUEUE_SIZE
      : USER_TASK_QUEUE_SIZE;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(inner_cache_.init())) {
    LOG_WARN("inner_cache init failed", KR(ret));
  } else if (OB_FAIL(async_queue_.init(this, user_thread_cnt, user_queue_size, "TbltLSSrv"))) {
    LOG_WARN("async_queue init failed",
        KR(ret), K(user_thread_cnt), K(user_queue_size));
  } else {
    sql_proxy_ = &sql_proxy;
    inited_ = true;
  }
  return ret;
}

int ObTabletLSService::get(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  is_cache_hit = false;
  ls_id.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (!is_valid_key_(tenant_id, tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid key for tablet get",
        KR(ret),
        K(tenant_id),
        K(tablet_id));
  } else if (is_sys_tenant(tenant_id) || tablet_id.is_sys_tablet()) {
    is_cache_hit = true;
    ls_id = SYS_LS;
  } else {
    ObTabletLSCache tablet_cache;
    ret = get_from_cache_(tenant_id, tablet_id, tablet_cache);
    if (OB_SUCCESS != ret && OB_CACHE_NOT_HIT != ret) {
      LOG_WARN("get tablet location from cache failed",
          KR(ret), K(tenant_id), K(tablet_id));
    } else if (OB_CACHE_NOT_HIT == ret
        || tablet_cache.get_renew_time() <= expire_renew_time) {
      if (OB_FAIL(renew_cache_(tenant_id, tablet_id, tablet_cache))) {
        LOG_WARN("renew tablet location failed",
            KR(ret), K(tenant_id), K(tablet_id));
      } else {
        ls_id = tablet_cache.get_ls_id();
      }
    } else { // valid cache
      is_cache_hit = true;
      ls_id = tablet_cache.get_ls_id();
    }
  }
  if (OB_SUCC(ret) && is_cache_hit) {
    EVENT_INC(LOCATION_CACHE_HIT);
  } else {
    EVENT_INC(LOCATION_CACHE_MISS);
  }
  return ret;
}

int ObTabletLSService::nonblock_get(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  ObTabletLSCache tablet_cache;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if(!is_valid_key_(tenant_id, tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key for get",
        KR(ret), K(tenant_id), K(tablet_id));
  } else if (is_sys_tenant(tenant_id) || tablet_id.is_sys_tablet()) {
    ls_id = SYS_LS;
  } else if (OB_FAIL(get_from_cache_(tenant_id, tablet_id, tablet_cache))) {
    if (OB_CACHE_NOT_HIT != ret) {
      LOG_WARN("get tablet_cache from inner_cache failed",
          KR(ret), K(tenant_id), K(tablet_id));
    }
  } else {
    ls_id = tablet_cache.get_ls_id();
  }
  if (OB_SUCC(ret)) {
    EVENT_INC(LOCATION_CACHE_NONBLOCK_HIT);
  } else if (OB_CACHE_NOT_HIT == ret) {
    ret = OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST;
    EVENT_INC(LOCATION_CACHE_NONBLOCK_MISS);
  }
  return ret;
}

int ObTabletLSService::nonblock_renew(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (!is_valid_key_(tenant_id, tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log stream key",
        KR(ret), K(tenant_id), K(tablet_id));
  } else if (is_sys_tenant(tenant_id) || tablet_id.is_sys_tablet()) {
    // do nothing
  } else {
    const int64_t now = ObTimeUtility::current_time();
    ObTabletLSUpdateTask task(tenant_id, tablet_id, now);
    if (OB_FAIL(add_update_task(task))) {
      LOG_WARN("add tablet_cache update task failed", KR(ret), K(task));
    } else {
      LOG_TRACE("add update task succeed", KR(ret), K(task));
    }
  }
  return ret;
}

int ObTabletLSService::add_update_task(const ObTabletLSUpdateTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  } else if (OB_FAIL(async_queue_.add(task))) {
    LOG_WARN("fail to add task", KR(ret), K(task));
  }
  return ret;
}

int ObTabletLSService::batch_process_tasks(
    const common::ObIArray<ObTabletLSUpdateTask> &tasks,
    bool &stopped)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  UNUSED(stopped);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (tasks.count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task count", KR(ret));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tasks.at(0).get_tenant_id());
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.schema_service_ is null", KR(ret));
    } else if (!GCTX.schema_service_->is_tenant_full_schema(meta_tenant_id)) {
      // do not process tasks if tenant schema is not ready
      if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
        LOG_WARN("tenant schema is not ready, need wait", KR(ret), K(meta_tenant_id), K(tasks));
      }
    } else {
      ObTabletLSCache tablet_cache;
      ARRAY_FOREACH_NORET(tasks, i) {
        tablet_cache.reset();
        const ObTabletLSUpdateTask &task = tasks.at(i);
        if (OB_UNLIKELY(!task.is_valid())) {
          tmp_ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid task", KR(tmp_ret), K(task));
        } else {
          tmp_ret = renew_cache_(
              task.get_tenant_id(),
              task.get_tablet_id(),
              tablet_cache);
          if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("fail to renew tablet_cache", KR(ret), K(task));
          }
        }
      } // end foreach
    }
  }
  return ret;
}

int ObTabletLSService::process_barrier(
    const ObTabletLSUpdateTask &task,
    bool &stopped)
{
  UNUSEDx(task, stopped);
  return OB_NOT_SUPPORTED;
}

void ObTabletLSService::stop()
{
  async_queue_.stop();
}

void ObTabletLSService::wait()
{
  async_queue_.wait();
}

int ObTabletLSService::destroy()
{
  int ret = OB_SUCCESS;
  stopped_ = true;
  inited_ = false;
  return ret;
}

int ObTabletLSService::reload_config()
{
  int ret = OB_SUCCESS;
  int64_t thread_cnt = GCONF.location_refresh_thread_count;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_FAIL(async_queue_.set_thread_count(thread_cnt))) {
    LOG_WARN("async_queue set thread count failed", KR(ret), K(thread_cnt));
  }
  return ret;
}

int ObTabletLSService::get_from_cache_(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    ObTabletLSCache &tablet_cache)
{
  int ret = OB_SUCCESS;
  ObTabletLSKey cache_key(tenant_id, tablet_id);
  tablet_cache.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if(OB_UNLIKELY(!cache_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_id));
  } else if (is_sys_tenant(tenant_id) || tablet_id.is_sys_tablet()) {
    const int64_t now = ObTimeUtility::current_time();
    if (OB_FAIL(tablet_cache.init(tenant_id,
        tablet_id,
        SYS_LS,
        now,
        INT64_MAX))) {
      LOG_WARN("init tablet_cache failed",
          KR(ret), K(cache_key), K(SYS_LS), K(now));
    }
  } else {
    if (OB_FAIL(inner_cache_.get(cache_key, tablet_cache))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        EVENT_INC(TABLET_LS_CACHE_MISS);
        ret = OB_CACHE_NOT_HIT;
        LOG_TRACE("tablet is not hit in inner cache", KR(ret), K(cache_key));
      } else {
        LOG_WARN("get tablet from inner cache failed", K(cache_key), KR(ret));
      }
    } else {
      EVENT_INC(TABLET_LS_CACHE_HIT);
      LOG_TRACE("tablet_cache hit in inner cache", KR(ret), K(cache_key), K(tablet_cache));
    }
  }
  return ret;
}

int ObTabletLSService::renew_cache_(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    ObTabletLSCache &tablet_cache)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  tablet_cache.reset();
  ObLSID ls_id;
  int64_t row_scn;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (!is_valid_key_(tenant_id, tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_id));
  } else if (is_sys_tenant(tenant_id) || tablet_id.is_sys_tablet()) {
    const int64_t now = ObTimeUtility::current_time();
    if (OB_FAIL(tablet_cache.init(tenant_id,
        tablet_id,
        SYS_LS,
        now,
        INT64_MAX))) {
      LOG_WARN("init tablet_cache failed",
          KR(ret), K(tenant_id), K(tablet_id), K(SYS_LS), K(now));
    }
  } else if (OB_FAIL(set_timeout_ctx_(ctx))) {
    LOG_WARN("failed to set timeout ctx", KR(ret));
  } else if (OB_FAIL(inner_get_by_sql_(tenant_id, tablet_id, tablet_cache))) {
    LOG_WARN("fail to get log stream info", KR(ret), K(tenant_id), K(tablet_id));
    if (ObLocationServiceUtility::treat_sql_as_timeout(ret)) {
      ret = OB_GET_LOCATION_TIME_OUT;
    }
  } else if (OB_FAIL(update_cache_(tablet_cache))) {
    LOG_WARN("fail to update cache", KR(ret), K(tablet_cache));
  } else {
    FLOG_INFO("[TABLET_LOCATION]success to renew tablet cache", K(tablet_cache));
  }
  return ret;
}

int ObTabletLSService::update_cache_(const ObTabletLSCache &tablet_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (!tablet_cache.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet_cache", KR(ret), K(tablet_cache));
  } else if (OB_FAIL(inner_cache_.update(tablet_cache))) {
    LOG_WARN("put tablet_cache to user inner_cache failed", KR(ret), K(tablet_cache));
  } else {
    LOG_TRACE("renew tablet_cache in inner_cache succeed", KR(ret), K(tablet_cache));
  }
  return ret;
}

int ObTabletLSService::inner_get_by_sql_(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    ObTabletLSCache &tablet_cache)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  tablet_cache.reset();
  int64_t row_scn = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_INVALID_TENANT_ID == tenant_id || !tablet_id.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(tablet_id));
      } else if (OB_FAIL(sql.assign_fmt(
          "SELECT ls_id, ORA_ROWSCN from %s WHERE tablet_id = %lu",
          OB_ALL_TABLET_TO_LS_TNAME, tablet_id.id()))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST;
          LOG_TRACE("fail to get tablet by sql", KR(ret), K(tenant_id), K(tablet_id));
        } else {
          LOG_WARN("result next failed", KR(ret));
        }
      } else {
        int64_t int_ls_id = ObLSID::INVALID_LS_ID;
        EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", int_ls_id, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "ORA_ROWSCN", row_scn, int64_t);
        ObLSID ls_id(int_ls_id);
        const int64_t now = ObTimeUtility::current_time();
        if (OB_FAIL(tablet_cache.init(
            tenant_id,
            tablet_id,
            ls_id,
            now,
            row_scn))) {
          LOG_WARN("init tablet_cache failed", KR(ret), K(tenant_id),
              K(tablet_id), K(ls_id), K(now), K(row_scn));
        } else {
          LOG_TRACE("success to get tablet by sql", KR(ret), K(tablet_cache));
        }
      }
    }
  }
  return ret;
}

int ObTabletLSService::set_timeout_ctx_(common::ObTimeoutCtx &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t default_timeout = GCONF.location_cache_refresh_sql_timeout;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
    LOG_WARN("fail to set default_timeout_ctx", KR(ret));
  }
  return ret;
}

bool ObTabletLSService::is_valid_key_(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id) const
{
  return tablet_id.is_valid_with_tenant(tenant_id) && !tablet_id.is_reserved_tablet();
}

} // end namespace share
} // end namespace oceanbase
