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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "rootserver/parallel_ddl/ob_tablet_balance_allocator.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "share/ob_share_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;


ObNonPartitionedTableTabletCache::ObNonPartitionedTableTabletCache(
  const uint64_t tenant_id,
  common::ObMySQLProxy &sql_proxy)
  : mutex_(),
    tenant_id_(tenant_id),
    sql_proxy_(sql_proxy),
    allocator_(ObMemAttr(OB_SYS_TENANT_ID, "NonPartTabtCac", ObCtxIds::SCHEMA_SERVICE),
               PAGE_SIZE),
    cache_(ARRAY_BLOCK_SIZE, ModulePageAllocator(allocator_)),
    loaded_timestamp_(OB_INVALID_TIMESTAMP),
    dump_timestamp_(OB_INVALID_TIMESTAMP)
{
}

void ObNonPartitionedTableTabletCache::reset_cache()
{
  lib::ObMutexGuard guard(mutex_);
  (void) inner_reset_cache_();
}

void ObNonPartitionedTableTabletCache::inner_reset_cache_()
{
  cache_.reset();
  allocator_.reset();
  loaded_timestamp_ = OB_INVALID_TIMESTAMP;
  LOG_INFO("[NON PARTITIONED TABLET CACHE] reset cache", K_(tenant_id));
}

// In the following cases, cache will be reload first:
// 1. cache_ is empty
// 2. cache_ is expire (consider transfer may change the placement of related tablets)
// 3. cache_ and avaliable_ls_ids are not matched (ls cnt or status changed)
bool ObNonPartitionedTableTabletCache::should_reload_cache_(
     const common::ObIArray<share::ObLSID> &avaliable_ls_ids)
{
  bool bret = false;
  int64_t interval = INT64_MAX;
  {
    omt::ObTenantConfigGuard tenant_config(OTC_MGR.get_tenant_config_with_lock(tenant_id_));
    if (tenant_config.is_valid()) {
      interval = tenant_config->partition_balance_schedule_interval;
    }
  }
  if (loaded_timestamp_ < 0) {
    bret = true; // case 1
    LOG_INFO("[NON PARTITIONED TABLET CACHE] failure/non parallel ddl occur or cache is empty, should be reloaded", K_(tenant_id));
  } else if (ObTimeUtility::current_time() - loaded_timestamp_ >= interval) {
    bret = true; // case 2
    LOG_INFO("[NON PARTITIONED TABLET CACHE] cache is expire, should be reloaded", K_(tenant_id));
  } else {
    // case 3
    if (avaliable_ls_ids.count() != cache_.count()) {
      bret = true;
    } else {
      for (int64_t i = 0; !bret && i < cache_.count(); i++) {
        ObLSID ls_id = cache_.at(i).get_ls_id();
        if (!has_exist_in_array(avaliable_ls_ids, ls_id)) {
          bret = true;
        }
      } // end for
    }
    if (bret) {
      LOG_INFO("[NON PARTITIONED TABLET CACHE] ls is changed, should be reloaded", K_(tenant_id));
    }
  }
  return bret;
}

int ObNonPartitionedTableTabletCache::reload_cache_(
    const common::ObIArray<share::ObLSID> &avaliable_ls_ids)
{
  int ret = OB_SUCCESS;
  (void) inner_reset_cache_();

  ObBalanceGroupLSStatOperator op;
  common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
  ObBalanceGroupID bg_id(0, 0); // for non-partitioned table
  ObString bg_name(rootserver::ObBalanceGroup::NON_PART_BG_NAME);
  const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(op.init(&sql_proxy_))) {
    LOG_WARN("fail to init ObBalanceGroupLSStatOperator", KR(ret));
  } else if (OB_FAIL(op.get_balance_group_ls_stat(
             default_timeout,
             sql_proxy_,
             tenant_id_,
             bg_id,
             false, /*for update*/
             bg_ls_stat_array))) {
    LOG_WARN("fail to get balance ls stat array", KR(ret), K_(tenant_id));
  } else {
    // 1. get existed ls stat
    common::ObArray<ObBalanceGroupLSStat> new_ls_stat_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < bg_ls_stat_array.count(); i++) {
      const ObBalanceGroupLSStat &ls_stat = bg_ls_stat_array.at(i);
      ObLSID ls_id = ls_stat.get_ls_id();
      if (has_exist_in_array(avaliable_ls_ids, ls_id)) {
        if (OB_FAIL(new_ls_stat_array.push_back(ls_stat))) {
          LOG_WARN("fail to push back ObBalanceGroupLSStat", KR(ret), K(ls_stat));
        }
      }
    } // end for

    // 2. insert missing ls stat
    common::ObArray<ObBalanceGroupLSStat> miss_ls_stat_array;
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < avaliable_ls_ids.count(); i++) {
        const ObLSID &ls_id = avaliable_ls_ids.at(i);
        bool finded = false;
        for (int64_t j = 0; !finded && OB_SUCC(ret) && j < bg_ls_stat_array.count(); j++) {
          const ObBalanceGroupLSStat &ls_stat = bg_ls_stat_array.at(j);
          if (ls_id == ls_stat.get_ls_id()) {
            finded = true;
          }
        } // end for
        if (OB_SUCC(ret) && !finded) {
          ObBalanceGroupLSStat ls_stat;
          if (OB_FAIL(ls_stat.build(tenant_id_, bg_id, ls_id, 0 /*bg cnt*/, bg_name))) {
            LOG_WARN("fail to build ls_stat", KR(ret), K_(tenant_id), K(ls_id));
          } else if (OB_FAIL(miss_ls_stat_array.push_back(ls_stat))) {
            LOG_WARN("fail to push back miss ls stat", KR(ret), K(ls_stat));
          }
        }
      } // end for

      if (OB_SUCC(ret) && miss_ls_stat_array.count() > 0) {
        if (OB_FAIL(op.insert_update_balance_group_ls_stat(
            default_timeout, tenant_id_, bg_id, miss_ls_stat_array))) {
          LOG_WARN("fail to insert miss ls stat", KR(ret), K_(tenant_id), K(miss_ls_stat_array));
        }
      }
    }

    // 3. store in cache
    if (FAILEDx(append(new_ls_stat_array, miss_ls_stat_array))) {
      LOG_WARN("fail to append ls stat array", KR(ret), K_(tenant_id), K(miss_ls_stat_array));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < new_ls_stat_array.count(); i++) {
        const ObBalanceGroupLSStat &ls_stat = new_ls_stat_array.at(i);
        Pair pair(ls_stat.get_ls_id(), ls_stat.get_tablet_group_count());
        if (OB_FAIL(cache_.push_back(pair))) {
          LOG_WARN("fail to push back pair", KR(ret), K_(tenant_id), K(ls_stat));
        }
      } // end for
      if (OB_FAIL(ret)) {
        (void) inner_reset_cache_();
      }
    }

    if (OB_SUCC(ret)) {
      loaded_timestamp_ = ObTimeUtility::current_time();
    }
  }
  LOG_INFO("[NON PARTITIONED TABLET CACHE] reload cache",
           KR(ret), K_(tenant_id), "cost", ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObNonPartitionedTableTabletCache::alloc_tablet(
    const common::ObIArray<share::ObLSID> &avaliable_ls_ids,
    share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(avaliable_ls_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(avaliable_ls_ids.count()));
  } else if (should_reload_cache_(avaliable_ls_ids)) {
    if (OB_FAIL(reload_cache_(avaliable_ls_ids))) {
      LOG_WARN("fail to reload cache", KR(ret), K_(tenant_id), K(avaliable_ls_ids));
    }
  }
  // find ls which has min tablet cnt
  if (OB_SUCC(ret)) {
    int64_t min_tablet_cnt = INT64_MAX;
    int64_t pos = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < cache_.count(); i++) {
      if (min_tablet_cnt > cache_.at(i).get_tablet_cnt()) {
        min_tablet_cnt = cache_.at(i).get_tablet_cnt();
        pos = i;
      }
    } // end for
    if (OB_UNLIKELY(OB_INVALID_INDEX == pos
        || pos >= cache_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to find ls has min tablet cnt",
               KR(ret), K_(tenant_id), K(pos), K_(cache));
    } else {
      Pair &target_pair = cache_.at(pos);
      const int64_t tablet_cnt = target_pair.get_tablet_cnt() + 1;
      ls_id = target_pair.get_ls_id();
      target_pair.set_tablet_cnt(tablet_cnt);
    }
  }
  (void) dump_cache_();
  return ret;
}

void ObNonPartitionedTableTabletCache::dump_cache_()
{
  const int64_t DUMP_INTERVAL = 10 * 60 * 1000 * 1000L; // 10min
  const int64_t current_time = ObTimeUtility::current_time();
  if (current_time - dump_timestamp_ >= DUMP_INTERVAL) {
    LOG_INFO("[NON PARTITIONED TABLET CACHE] dump cache", K_(tenant_id),
             K_(loaded_timestamp), K_(dump_timestamp), K_(cache));
    dump_timestamp_ = current_time;
  }
}

ObNonPartitionedTableTabletAllocator::ObNonPartitionedTableTabletAllocator()
  : rwlock_(),
    allocator_(ObMemAttr(OB_SYS_TENANT_ID, "NonPartTenCac", ObCtxIds::SCHEMA_SERVICE)),
    tenant_cache_(),
    sql_proxy_(NULL),
    inited_(false)
{
}

ObNonPartitionedTableTabletAllocator::~ObNonPartitionedTableTabletAllocator()
{
  destroy();
}

int ObNonPartitionedTableTabletAllocator::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    const int64_t BUCKET_NUM = 1024;
    if (OB_FAIL(tenant_cache_.create(BUCKET_NUM, "NonPartTenMap", "NonPartTenMap"))) {
      LOG_WARN("fail to create hash map", KR(ret));
    } else {
      sql_proxy_ = &sql_proxy;
      inited_ = true;
    }
  }
  return ret;
}

void ObNonPartitionedTableTabletAllocator::destroy()
{
  SpinWLockGuard guard(rwlock_);
  if (inited_) {
    FOREACH(it, tenant_cache_) {
      if (OB_NOT_NULL(it->second)) {
        (it->second)->~ObNonPartitionedTableTabletCache();
        it->second = NULL;
      }
    }
    tenant_cache_.destroy();
    allocator_.reset();
    sql_proxy_ = NULL;
    inited_ = false;
  }
}

void ObNonPartitionedTableTabletAllocator::reset_all_cache()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (inited_) {
    FOREACH(it, tenant_cache_) {
      if (OB_NOT_NULL(it->second)) {
        (void) (it->second)->reset_cache();
      }
    }
  }
}

int ObNonPartitionedTableTabletAllocator::reset_cache(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObNonPartitionedTableTabletCache *cache = NULL;
    if (OB_FAIL(tenant_cache_.get_refactored(tenant_id, cache))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get refactored", KR(ret), K(tenant_id));
      } else {
        // tenant not in cache, just skip
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache is null", KR(ret), K(tenant_id));
    } else {
      (void) cache->reset_cache();
    }
  }
  return ret;
}

int ObNonPartitionedTableTabletAllocator::try_init_cache_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObNonPartitionedTableTabletCache *cache = NULL;
    if (OB_FAIL(tenant_cache_.get_refactored(tenant_id, cache))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get cache", KR(ret), K(tenant_id));
      } else {
        ret = OB_SUCCESS;
        cache = NULL;
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObNonPartitionedTableTabletCache)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (FALSE_IT(cache = new (buf) ObNonPartitionedTableTabletCache(tenant_id, *sql_proxy_))) {
        } else if (OB_FAIL(tenant_cache_.set_refactored(tenant_id, cache))) {
          LOG_WARN("fail to set cache", KR(ret), K(tenant_id));
        }
      }
    } else {
      // cache exist, just skip
    }
  }
  return ret;
}

int ObNonPartitionedTableTabletAllocator::alloc_tablet(
    const uint64_t tenant_id,
    const common::ObIArray<share::ObLSID> &avaliable_ls_ids,
    share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || avaliable_ls_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(avaliable_ls_ids.count()));
  } else if (OB_FAIL(try_init_cache_(tenant_id))) {
    LOG_WARN("try to init cache failed", KR(ret), K(tenant_id));
  } else {
    {
      SpinRLockGuard guard(rwlock_);
      ObNonPartitionedTableTabletCache *cache = NULL;
      if (OB_FAIL(tenant_cache_.get_refactored(tenant_id, cache))) {
        LOG_WARN("fail to get refactored", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache is null", KR(ret));
      } else if (OB_FAIL(cache->alloc_tablet(avaliable_ls_ids, ls_id))) {
        LOG_WARN("fail to alloc tablet", KR(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      // try update ls stat
      ObBalanceGroupLSStat ls_stat;
      const ObBalanceGroupID bg_id(0, 0); // for non-partitioned table
      const ObString bg_name(rootserver::ObBalanceGroup::NON_PART_BG_NAME);
      const int64_t inc_tablet_cnt = 1;
      const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
      ObBalanceGroupLSStatOperator op;
      if (OB_FAIL(op.init(sql_proxy_))) {
        LOG_WARN("fail to init ObBalanceGroupLSStatOperator", KR(ret));
      } else if (OB_FAIL(ls_stat.build(tenant_id, bg_id, ls_id, inc_tablet_cnt, bg_name))) {
        LOG_WARN("fail to build ls_stat", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(op.inc_balance_group_ls_stat(
                 default_timeout, *sql_proxy_, tenant_id, ls_stat))) {
        LOG_WARN("fail to inc ls stat", KR(ret), K(tenant_id), K(ls_stat));
      }
    }
  }
  return ret;
}
