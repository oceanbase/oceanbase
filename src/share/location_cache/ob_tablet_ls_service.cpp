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
#include "share/tablet/ob_tablet_to_ls_operator.h" // ObTabletToLSOperator

namespace oceanbase
{
using namespace common;
using namespace common::hash;

namespace share
{
int ObTabletLSService::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &srv_rpc_proxy)
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
  } else if (OB_FAIL(async_queue_.init(this, user_thread_cnt, user_queue_size, "TabletLSAUp"))) {
    LOG_WARN("async_queue init failed",
        KR(ret), K(user_thread_cnt), K(user_queue_size));
  } else if (OB_FAIL(TG_SCHEDULE(
      lib::TGDefIDs::ServerGTimer,
      clear_expired_cache_task_,
      CLEAR_EXPIRED_CACHE_INTERVAL_US,
      true/*repeat*/))) {
    LOG_WARN("schedule clear expired cache timer task failed", KR(ret));
  } else if (OB_FAIL(auto_refresh_service_.init(*this, schema_service, sql_proxy))) {
    LOG_WARN("fail to init auto refresh service", KR(ret));
  } else if (OB_FAIL(broadcast_sender_.init(&srv_rpc_proxy))) {
    LOG_WARN("broadcast_sender init failed", KR(ret));
  } else if (OB_FAIL(broadcast_updater_.init(this))) {
    LOG_WARN("broadcast_updater init failed", KR(ret));
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
  } else if (belong_to_sys_ls_(tenant_id, tablet_id)) {
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
  } else if (belong_to_sys_ls_(tenant_id, tablet_id)) {
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
  } else if (belong_to_sys_ls_(tenant_id, tablet_id)) {
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
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to add task", KR(ret), K(task));
    } else {
      ret = OB_SUCCESS;
      LOG_TRACE("task already exists", KR(ret), K(task));
    }
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
    const uint64_t tenant_id = tasks.at(0).get_tenant_id();
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.schema_service_ is null", KR(ret));
    } else if (!GCTX.schema_service_->is_tenant_full_schema(meta_tenant_id)) {
      // do not process tasks if tenant schema is not ready
      if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
        LOG_WARN("tenant schema is not ready, need wait", KR(ret), K(meta_tenant_id), K(tasks));
      }
    } else {
      ObArenaAllocator allocator;
      ObList<ObTabletID, ObIAllocator> tablet_list(allocator);
      ObArray<ObTabletLSCache> tablet_ls_caches;
      ARRAY_FOREACH(tasks, idx) {
        const ObTabletLSUpdateTask &task = tasks.at(idx);
        if (OB_UNLIKELY(tenant_id != task.get_tenant_id())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid task", KR(ret), K(tenant_id), K(task));
        } else if (belong_to_sys_ls_(task.get_tenant_id(), task.get_tablet_id())) {
          // skip
        } else if (OB_FAIL(tablet_list.push_back(task.get_tablet_id()))) {
          LOG_WARN("push back failed", KR(ret), K(idx), K(task));
        }
      }
      if (OB_FAIL(ret) || 0 == tablet_list.size()) {
      } else if (OB_FAIL(batch_renew_tablet_ls_cache(tenant_id, tablet_list, tablet_ls_caches))) {
        LOG_WARN("batch renew tablet ls mapping failed", KR(ret), K(tenant_id), K(tablet_list));
      }
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

int ObTabletLSService::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(auto_refresh_service_.start())) {
    LOG_WARN("fail to start auto refresh service", KR(ret));
  }
  return ret;
}

void ObTabletLSService::stop()
{
  async_queue_.stop();
  auto_refresh_service_.stop();
  broadcast_sender_.stop();
  broadcast_updater_.stop();
}

void ObTabletLSService::wait()
{
  async_queue_.wait();
  auto_refresh_service_.wait();
  broadcast_sender_.wait();
  broadcast_updater_.wait();
}

int ObTabletLSService::destroy()
{
  int ret = OB_SUCCESS;
  stopped_ = true;
  inited_ = false;
  inner_cache_.destroy();
  async_queue_.destroy();
  auto_refresh_service_.destroy();
  broadcast_sender_.destroy();
  broadcast_updater_.destroy();
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
  } else if (belong_to_sys_ls_(tenant_id, tablet_id)) {
    const int64_t now = ObTimeUtility::current_time();
    if (OB_FAIL(tablet_cache.init(tenant_id,
        tablet_id,
        SYS_LS,
        now,
        0 /*transfer_seq*/))) {
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
  ObArenaAllocator allocator;
  ObList<ObTabletID, ObIAllocator> tablet_list(allocator);
  ObSEArray<ObTabletLSCache, 1> tablet_ls_caches;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (!is_valid_key_(tenant_id, tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_id));
  } else if (belong_to_sys_ls_(tenant_id, tablet_id)) {
    const int64_t now = ObTimeUtility::current_time();
    if (OB_FAIL(tablet_cache.init(tenant_id,
        tablet_id,
        SYS_LS,
        now,
        0 /*transfer_seq*/))) {
      LOG_WARN("init tablet_cache failed",
          KR(ret), K(tenant_id), K(tablet_id), K(SYS_LS), K(now));
    }
  } else if (OB_FAIL(tablet_list.push_back(tablet_id))) {
    LOG_WARN("push back failed", KR(ret), K(tablet_id));
  } else if (OB_FAIL(batch_renew_tablet_ls_cache(tenant_id, tablet_list, tablet_ls_caches))) {
    LOG_WARN("batch renew tablet ls cache failed", KR(ret), K(tablet_list));
  } else if (tablet_ls_caches.empty()) {
    ret = OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST;
    LOG_WARN("tablet ls mapping not exist in inner table", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(tablet_cache.assign(tablet_ls_caches.at(0)))) {
    LOG_WARN("assign failed", KR(ret), K(tablet_ls_caches));
  } else {
    FLOG_INFO("[TABLET_LOCATION]success to renew tablet cache", K(tablet_cache));
  }
  return ret;
}

int ObTabletLSService::update_cache(
    const ObTabletLSCache &tablet_cache,
    const bool update_only)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (!tablet_cache.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet_cache", KR(ret), K(tablet_cache));
  } else if (OB_FAIL(inner_cache_.update(tablet_cache, update_only))) {
    LOG_WARN("put tablet_cache to user inner_cache failed", KR(ret), K(tablet_cache), K(update_only));
  } else {
    LOG_TRACE("renew tablet_cache in inner_cache succeed", KR(ret), K(tablet_cache), K(update_only));
  }
  return ret;
}

int ObTabletLSService::get_tablet_ids_from_cache(
    const uint64_t tenant_id,
    common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  tablet_ids.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_FAIL(inner_cache_.get_tablet_ids(tenant_id, tablet_ids))) {
    LOG_WARN("fail to get tablet_ids", KR(ret), K(tenant_id));
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

int ObTabletLSService::batch_renew_tablet_ls_cache(
    const uint64_t tenant_id,
    const ObList<common::ObTabletID, common::ObIAllocator> &tablet_list,
    common::ObIArray<ObTabletLSCache> &tablet_ls_caches)
{
  int ret = OB_SUCCESS;
  tablet_ls_caches.reset();
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || 0 == tablet_list.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(tablet_list));
  } else if (OB_FAIL(tablet_ls_caches.reserve(tablet_list.size()))) {
    LOG_WARN("reserve failed", KR(ret), "count", tablet_list.size());
  } else {
    ObArray<ObTabletID> user_tablet_ids;
    ObArray<ObTabletLSCache> user_tablet_ls_caches;
    const int64_t now = ObTimeUtility::current_time();
    ObTimeoutCtx ctx;
    // mock cache for sys tablet and filter out user tablet
    FOREACH_X(tablet_id, tablet_list, OB_SUCC(ret)) {
      if (belong_to_sys_ls_(tenant_id, *tablet_id)) {
        ObTabletLSCache cache;
        if (OB_FAIL(cache.init(tenant_id, *tablet_id, SYS_LS, now,  0 /*transfer_seq*/))) {
          LOG_WARN("init cache failed", KR(ret), K(tenant_id), K(*tablet_id), K(now));
        } else if (OB_FAIL(tablet_ls_caches.push_back(cache))) {
          LOG_WARN("push back failed", KR(ret), K(cache));
        }
      } else if (OB_FAIL(user_tablet_ids.push_back(*tablet_id))) {
        LOG_WARN("push back failed", KR(ret), K(tenant_id), K(*tablet_id));
      }
    }

    if (OB_FAIL(ret) || user_tablet_ids.empty()) {
      // skip
    } else {
      const int64_t single_get_timeout = GCONF.location_cache_refresh_sql_timeout;
      // calculate timeout by count of inner_sql
      const int64_t batch_get_timeout = (user_tablet_ids.count() / ObTabletToLSTableOperator::MAX_BATCH_COUNT + 1) * single_get_timeout;
      if (OB_FAIL(auto_refresh_service_.try_init_base_point(tenant_id))) {
        LOG_WARN("fail to init base point", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, batch_get_timeout))) {
        LOG_WARN("fail to set default_timeout_ctx", KR(ret));
      } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_tablet_ls_cache(
          *sql_proxy_,
          tenant_id,
          user_tablet_ids,
          user_tablet_ls_caches))) {
        if (ObLocationServiceUtility::treat_sql_as_timeout(ret)) {
          int previous_ret = ret;
          ret = OB_GET_LOCATION_TIME_OUT;
          LOG_WARN("the sql used to get tablets locations error, treat as timeout",
              KR(ret), K(previous_ret), K(tablet_list));
        } else {
          LOG_WARN("batch get tablet ls cache failed", KR(ret), K(tenant_id), K(user_tablet_ids));
        }
      }
    }
    // update user tablet ls cache
    bool update_only = false;
    ARRAY_FOREACH(user_tablet_ls_caches, idx) {
      const ObTabletLSCache &tablet_ls = user_tablet_ls_caches.at(idx);
      if (OB_FAIL(update_cache(tablet_ls, update_only))) {
        LOG_WARN("update cache failed", KR(ret), K(tablet_ls));
      } else if (OB_FAIL(tablet_ls_caches.push_back(tablet_ls))) {
        LOG_WARN("push back faled", KR(ret), K(tablet_ls));
      }
    } // end ARRAY_FOREACH
    // erase nonexistent user tablet ls cache
    if (OB_SUCC(ret) && (user_tablet_ls_caches.count() != user_tablet_ids.count())) {
      int64_t erase_count = 0;
      ARRAY_FOREACH(user_tablet_ids, i) {
        const ObTabletID &tablet_id = user_tablet_ids.at(i);
        bool found = false;
        ARRAY_FOREACH(user_tablet_ls_caches, j) {
          if (user_tablet_ls_caches.at(j).get_tablet_id() == tablet_id) {
            found = true;
            break;
          }
        }
        if (OB_SUCC(ret) && !found) {
          if (OB_FAIL(erase_cache_(tenant_id, tablet_id))) {
            LOG_WARN("erase cache failed", KR(ret), K(tenant_id), K(tablet_id));
          } else {
            ++erase_count;
            LOG_INFO("[TABLET_LOCATION] tablet ls mapping not exist", KR(ret), K(tenant_id), K(tablet_id));
          }
        }
      } // end ARRAY_FOREACH tablet_ids
    }
  }
  return ret;
}

int ObTabletLSService::erase_cache_(const uint64_t tenant_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_key_(tenant_id, tablet_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key for tablet get", KR(ret), K(tenant_id), K(tablet_id));
  } else if (belong_to_sys_ls_(tenant_id, tablet_id)) {
    // skip
  } else {
    ObTabletLSKey cache_key(tenant_id, tablet_id);
    if (OB_FAIL(inner_cache_.del(cache_key))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_TRACE("not exist in inner_cache_", K(cache_key));
      } else {
        LOG_WARN("fail to erase cache from inner_cache_", KR(ret), K(cache_key));
      }
    } else {
      LOG_TRACE("erase cache from inner_cache_", K(cache_key));
    }
  }
  return ret;
}

bool ObTabletLSService::belong_to_sys_ls_(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id) const
{
  return is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id) || tablet_id.is_sys_tablet();
}

class ObTabletLSService::IsDroppedTenantCacheFunctor
{
public:
  explicit IsDroppedTenantCacheFunctor(
      ObHashSet<uint64_t> &dropped_tenant_set_)
      : dropped_tenant_set_(dropped_tenant_set_) {}
  ~IsDroppedTenantCacheFunctor() {}
  bool operator()(const ObTabletLSCache &cache)
  {
    int ret = OB_SUCCESS;
    bool is_expired_cache = false;
    const uint64_t tenant_id = cache.get_tenant_id();
    ret = dropped_tenant_set_.exist_refactored(tenant_id);
    if (OB_HASH_EXIST == ret) {
      is_expired_cache = true;
    } else if (OB_HASH_NOT_EXIST == ret) {
      is_expired_cache = false;
    } else {
      LOG_WARN("error unexpected", KR(ret), K(tenant_id));
    }
    return is_expired_cache;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(IsDroppedTenantCacheFunctor);
  ObHashSet<uint64_t> &dropped_tenant_set_;
};

// Only clear cache for dropped tenant now
// TODO: need a better clear strategy for each tenant expired caches
int ObTabletLSService::clear_expired_cache()
{
  int ret = OB_SUCCESS;
  bool sys_tenant_schema_ready = false;
  ObArray<uint64_t> dropped_tenant_ids;
  ObHashSet<uint64_t> dropped_tenant_set;
  const int64_t cache_size = inner_cache_.size();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (!GCTX.schema_service_->is_tenant_refreshed(OB_SYS_TENANT_ID)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("can not clear expiered cache because sys tenant schema is not ready", KR(ret), K(cache_size));
  } else if (OB_FAIL(GCTX.schema_service_->get_dropped_tenant_ids(dropped_tenant_ids))) {
    LOG_WARN("get tenant ids failed", KR(ret));
  } else if (OB_FAIL(dropped_tenant_set.create(dropped_tenant_ids.count()))) {
    LOG_WARN("create failed", KR(ret), "count", dropped_tenant_ids.count());
  } else {
    // use hashset to improve performance
    ARRAY_FOREACH(dropped_tenant_ids, idx) {
      const uint64_t tenant_id = dropped_tenant_ids.at(idx);
      if (!is_user_tenant(tenant_id)) {
        // skip
      } else if (OB_FAIL(dropped_tenant_set.set_refactored(tenant_id))) {
        // OB_HASH_EXIST is also unexpected
        LOG_WARN("set_refactored failed", KR(ret), K(idx), K(tenant_id));
      }
    }
    IsDroppedTenantCacheFunctor functor(dropped_tenant_set);
    if (FAILEDx(inner_cache_.for_each_and_delete_if(functor))) {
      LOG_WARN("for each and delete if is dropped tenant cache failed", KR(ret));
    } else {
      LOG_INFO("[TABLET_LOCATION] clear dropped tenant tablet ls cache successfully",
          "cache_size_before_clear", cache_size, "cache_size_after_clear", inner_cache_.size());
    }
  }
  return ret;
}

int ObTabletLSService::submit_broadcast_task(const ObTabletLocationBroadcastTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_FAIL(broadcast_sender_.submit_broadcast_task(task))) {
    LOG_WARN("failed to submit broadcast task by sender", KR(ret), K(task));
  }
  return ret;
}

int ObTabletLSService::submit_update_task(const ObTabletLocationBroadcastTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_FAIL(broadcast_updater_.submit_update_task(task))) {
    LOG_WARN("failed to submit broadcast task by sender", KR(ret));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
