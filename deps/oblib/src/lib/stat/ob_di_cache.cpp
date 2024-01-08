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

#include "lib/stat/ob_di_cache.h"
#include "lib/random/ob_random.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/ob_lib_config.h"

namespace oceanbase
{
namespace common
{

ObDISessionCollect::ObDISessionCollect()
  : session_id_(0),
    base_value_(),
    lock_()
{
}

ObDISessionCollect::~ObDISessionCollect()
{
}

void ObDISessionCollect::clean()
{
  session_id_ = 0;
  base_value_.reset();
}

ObDITenantCollect::ObDITenantCollect(ObIAllocator *allocator)
  : tenant_id_(0),
    last_access_time_(0),
    base_value_(allocator)
{
}

ObDITenantCollect::~ObDITenantCollect()
{
}

void ObDITenantCollect::clean()
{
  tenant_id_ = 0;
  last_access_time_ = 0;
  base_value_.reset();
}

/*
 * -------------------------------------------------------ObDICache---------------------------------------------------------------
 */
ObDISessionCache::ObDISessionCache()
  : di_map_(),
    collects_()
{
}

ObDISessionCache::~ObDISessionCache()
{
}

ObDISessionCache &ObDISessionCache::get_instance()
{
  static ObDISessionCache instance_;
  return instance_;
}

int ObDISessionCache::get_node(uint64_t session_id, ObDISessionCollect *&session_collect)
{
  int ret = OB_SUCCESS;
  thread_local ObRandom random;
  ObSessionBucket &bucket = di_map_[session_id % MAX_SESSION_COLLECT_NUM];
  while (1) {
    bucket.lock_.rdlock();
    if (OB_SUCCESS == (ret = bucket.get_the_node(session_id, session_collect))) {
      if (OB_SUCCESS == (ret = session_collect->lock_.try_rdlock())) {
        bucket.lock_.unlock();
        break;
      }
    }
    if (OB_SUCCESS != ret) {
      bucket.lock_.unlock();
      int64_t pos = 0;
      while (1) {
        pos = random.get(0, MAX_SESSION_COLLECT_NUM - 1);
        if (OB_SUCCESS == (ret = collects_[pos].lock_.try_wrlock())) {
          break;
        }
      }
      if (OB_SUCCESS == ret) {
        if (0 != collects_[pos].session_id_) {
          ObSessionBucket &des_bucket = di_map_[collects_[pos].session_id_ % MAX_SESSION_COLLECT_NUM];
          des_bucket.lock_.wrlock();
          des_bucket.list_.remove(&collects_[pos]);
          collects_[pos].clean();
          des_bucket.lock_.unlock();
        }
        bucket.lock_.wrlock();
        if (OB_SUCCESS != (ret = bucket.get_the_node(session_id, session_collect))) {
          ret = OB_SUCCESS;
          bucket.list_.add_last(&collects_[pos]);
          collects_[pos].session_id_ = session_id;
          bucket.lock_.unlock();
          session_collect = &collects_[pos];
          collects_[pos].lock_.wr2rdlock();
          break;
        } else {
          if (OB_SUCCESS == (ret = session_collect->lock_.try_rdlock())) {
            bucket.lock_.unlock();
            collects_[pos].lock_.unlock();
            break;
          } else {
            bucket.lock_.unlock();
            collects_[pos].lock_.unlock();
          }
        }
      }
    }
  }
  return ret;
}

int ObDISessionCache::get_all_diag_info(ObIArray<std::pair<uint64_t, ObDISessionCollect*> > &diag_infos)
{
  int ret = OB_SUCCESS;
  std::pair<uint64_t, ObDISessionCollect*> pair;
  ObDISessionCollect *head = NULL;
  ObDISessionCollect *node = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_SESSION_COLLECT_NUM; ++i) {
    ObSessionBucket &bucket = di_map_[i];
    bucket.lock_.rdlock();
    head = bucket.list_.get_header();
    node = bucket.list_.get_first();
    while (head != node && NULL != node && OB_SUCC(ret)) {
      pair.first = node->session_id_;
      pair.second = node;
      if (OB_SUCCESS != (ret = diag_infos.push_back(pair))) {
      } else {
        node = node->next_;
      }
    }
    bucket.lock_.unlock();
  }
  return ret;
}

int ObDISessionCache::get_the_diag_info(
  uint64_t session_id,
  ObDISessionCollect *&diag_infos)
{
  int ret = OB_SUCCESS;
  ObSessionBucket &bucket = di_map_[session_id % MAX_SESSION_COLLECT_NUM];
  bucket.lock_.rdlock();
  ObDISessionCollect *collect = NULL;
  if (OB_SUCCESS == (ret = bucket.get_the_node(session_id, collect))) {
    diag_infos = collect;
  }
  bucket.lock_.unlock();
  return ret;
}

ObDIBaseTenantCacheIterator::ObDIBaseTenantCacheIterator()
{
  reset();
}

ObDIBaseTenantCacheIterator::ObDIBaseTenantCacheIterator(const ObDITenantCollect *collects, const int64_t count)
  : collects_(collects),
    count_(count),
    idx_(0)
{
}

int ObDIBaseTenantCacheIterator::init(const ObDITenantCollect *collects, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (nullptr == collects || 0 == count) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    collects_ = collects;
    count_ = count;
    idx_ = 0;
  }
  return ret;
}

int ObDIBaseTenantCacheIterator::get_next(const ObDITenantCollect *&collect)
{
  int ret = OB_SUCCESS;
  collect = nullptr;
  if (nullptr != collects_ && count_ > 0) {
    for (; idx_ < count_ && nullptr == collect; idx_++) {
      if (0 != collects_[idx_].last_access_time_) {
        collect = collects_ + idx_;
      }
    }
  }
  if (collect == nullptr) {
    ret = OB_ITER_END;
  }
  return ret;
}


ObDIThreadTenantCache::ObDIThreadTenantCache()
  : tenant_cache_(),
    extend_tenant_cache_(nullptr)
{
  ObDIGlobalTenantCache::get_instance().link(this);
}

ObDIThreadTenantCache::~ObDIThreadTenantCache()
{
  ObDIGlobalTenantCache::get_instance().unlink(this);
  lib::ObDisableDiagnoseGuard disable_diagnose_guard;
  if (extend_tenant_cache_ != nullptr) {
    ob_delete(extend_tenant_cache_);
    extend_tenant_cache_ = nullptr;
  }
}

int ObDIThreadTenantCache::get_node(uint64_t tenant_id, ObDITenantCollect *&tenant_collect)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_collect = tenant_cache_.get_node(tenant_id))) {
    if (nullptr == extend_tenant_cache_) {
      extend_tenant_cache_ = OB_NEW(ObDIBaseTenantCache<MAX_TENANT_NUM_PER_SERVER>,
                                    SET_USE_UNEXPECTED_500("di_tenant_cache"));
    }
    if (nullptr != extend_tenant_cache_) {
      tenant_collect = extend_tenant_cache_->get_node(tenant_id, true /*replace*/);
    }
    if (nullptr == tenant_collect) {
      ret = OB_TENANT_NOT_EXIST;
    }
  }
  return ret;
}

void ObDIThreadTenantCache::get_the_diag_info(
  uint64_t tenant_id,
  ObDiagnoseTenantInfo &diag_infos)
{
  tenant_cache_.get_the_diag_info(tenant_id, diag_infos);
  if (nullptr != extend_tenant_cache_) {
    extend_tenant_cache_->get_the_diag_info(tenant_id, diag_infos);
  }
}

int ObDIThreadTenantCache::get_cache_iter(ObDIBaseTenantCacheIterator &cache_iter, const bool extend ) const
{
  int ret = OB_SUCCESS;
  if (!extend) {
    ret = tenant_cache_.get_cache_iter(cache_iter);
  } else if (nullptr != extend_tenant_cache_) {
    ret = extend_tenant_cache_->get_cache_iter(cache_iter);
  } else {
    cache_iter.reset();
  }
  return ret;
}

ObDIGlobalTenantCache::ObDIGlobalTenantCache()
  : list_(),
    cnt_(0),
    lock_(),
    unlinked_tenant_cache_()
{
}

ObDIGlobalTenantCache::~ObDIGlobalTenantCache()
{
}

ObDIGlobalTenantCache &ObDIGlobalTenantCache::get_instance()
{
  static ObDIGlobalTenantCache instance_;
  return instance_;
}

void ObDIGlobalTenantCache::link(ObDIThreadTenantCache *node)
{
  lock_.wrlock();
  list_.add_last(node);
  cnt_++;
  lock_.unlock();
}

void ObDIGlobalTenantCache::unlink(ObDIThreadTenantCache *node)
{
  int ret = OB_SUCCESS;
  lock_.wrlock();
  list_.remove(node);
  cnt_--;

  if (NULL != node) {
    ObDIBaseTenantCacheIterator cache_iter;
    if (OB_FAIL(node->get_cache_iter(cache_iter))) {
    } else if (!cache_iter.is_valid()) {
    } else if (OB_FAIL(add_tenant_stat_(cache_iter))) {
    } else if (OB_FAIL(node->get_cache_iter(cache_iter, true))) {
    } else if (!cache_iter.is_valid()) {
    } else if (OB_FAIL(add_tenant_stat_(cache_iter))) {
    }
  }
  lock_.unlock();
}

// call this func with lock
int ObDIGlobalTenantCache::add_tenant_collect_(const ObDITenantCollect &collect)
{
  int ret = OB_SUCCESS;
  ObDITenantCollect *local_collect = NULL;
  if (OB_ISNULL((local_collect = unlinked_tenant_cache_.get_node(collect.tenant_id_)))) {
      ret = OB_ERR_SYS;
    // cannot print log in di
  } else {
    local_collect->base_value_.add(collect.base_value_); //has no ret
  }
  return ret;
}

int ObDIGlobalTenantCache::add_tenant_stat_(ObDIBaseTenantCacheIterator &cache_iter)
{
  int ret = OB_SUCCESS;
  if (!cache_iter.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObDITenantCollect *collect = NULL;
    ObDITenantCollect *local_collect = NULL;

    while (OB_SUCC(ret)) {
      ret = cache_iter.get_next(collect);
      if (OB_FAIL(ret)) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_ISNULL(collect)) {
        ret = OB_ERR_SYS;
        // cannot print log in di
      } else {
        ret = add_tenant_collect_(*collect);
      }
    }
  }

  return ret;
}

int ObDIGlobalTenantCache::get_the_diag_info(
  uint64_t tenant_id,
  ObDiagnoseTenantInfo &diag_infos)
{
  int ret = OB_SUCCESS;
  diag_infos.reset();
  lock_.rdlock();
  if (0 != cnt_) {
    ObDIThreadTenantCache *tenant_cache = list_.get_first();
    while (list_.get_header() != tenant_cache && NULL != tenant_cache) {
      tenant_cache->get_the_diag_info(tenant_id, diag_infos);
      tenant_cache = tenant_cache->next_;
    }
  }

  if (OB_SUCC(ret)) {
    unlinked_tenant_cache_.get_the_diag_info(tenant_id, diag_infos); // has no ret
  }
  lock_.unlock();
  return ret;
}

int ObDIGlobalTenantCache::get_all_wait_event(
    ObIAllocator &allocator,
    ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos)
{
  int ret = OB_SUCCESS;
  AddWaitEvent adder;
  ret = get_all_diag_info(allocator, diag_infos, adder);
  return ret;
}

int ObDIGlobalTenantCache::get_all_stat_event(
    ObIAllocator &allocator,
    ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos)
{
  int ret = OB_SUCCESS;
  AddStatEvent adder;
  ret = get_all_diag_info(allocator, diag_infos, adder);
  return ret;
}

int ObDIGlobalTenantCache::get_all_latch_stat(
    ObIAllocator &allocator,
    ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos)
{
  int ret = OB_SUCCESS;
  AddLatchStat adder;
  ret = get_all_diag_info(allocator, diag_infos, adder);
  return ret;
}

template<class _callback>
int ObDIGlobalTenantCache::get_all_diag_info(
    ObIAllocator &allocator,
    ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos, _callback &callback)
{
  int ret = OB_SUCCESS;
  std::pair<uint64_t, ObDiagnoseTenantInfo*> pair;
  ObTenantBucket *di_map = NULL;
  void *di_map_buf = NULL;

  if (OB_ISNULL(di_map_buf = allocator.alloc(OB_MAX_SERVER_TENANT_CNT * sizeof(ObTenantBucket)))) {
    ret= OB_ALLOCATE_MEMORY_FAILED;
    // cannot print log in di
  } else {
    di_map = new (di_map_buf) ObTenantBucket[OB_MAX_SERVER_TENANT_CNT];
  }

  lock_.rdlock();
  ObDIBaseTenantCacheIterator cache_iter;
  if (OB_SUCC(ret) && 0 != cnt_) {
    ObDIThreadTenantCache *tenant_cache = list_.get_first();
    while (OB_SUCC(ret) && list_.get_header() != tenant_cache && NULL != tenant_cache) {
      if (OB_FAIL(tenant_cache->get_cache_iter(cache_iter))) {
      } else if (!cache_iter.is_valid()) {
      } else if (OB_FAIL(get_tenant_stat(allocator, cache_iter, callback, di_map, OB_MAX_SERVER_TENANT_CNT))) {
      } else if (OB_FAIL(tenant_cache->get_cache_iter(cache_iter, true))) {
      } else if (!cache_iter.is_valid()) {
      } else if (OB_FAIL(get_tenant_stat(allocator, cache_iter, callback, di_map, OB_MAX_SERVER_TENANT_CNT))) {
      }
      if (OB_SUCC(ret)) {
        tenant_cache = tenant_cache->next_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(unlinked_tenant_cache_.get_cache_iter(cache_iter))) {
    } else if (!cache_iter.is_valid()) {
    } else if (OB_FAIL(get_tenant_stat(allocator, cache_iter, callback, di_map, OB_MAX_SERVER_TENANT_CNT))) {
      // cannot print log in di
    }
  }
  lock_.unlock();
  ObDITenantCollect *head = NULL;
  ObDITenantCollect *node = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < OB_MAX_SERVER_TENANT_CNT; ++i) {
    ObTenantBucket &bucket = di_map[i];
    head = bucket.list_.get_header();
    node = bucket.list_.get_first();
    while (head != node && NULL != node && OB_SUCC(ret)) {
      pair.first = node->tenant_id_;
      pair.second = &(node->base_value_);
      if (OB_SUCCESS != (ret = diag_infos.push_back(pair))) {
      } else {
        node = node->next_;
      }
    }
  }
  return ret;
}

template <class CALLBACK_FUNC>
int ObDIGlobalTenantCache::get_tenant_stat(ObIAllocator &allocator,
    ObDIBaseTenantCacheIterator &cache_iter, CALLBACK_FUNC &callback,
    ObTenantBucket *di_map, const int64_t di_map_bucket_num)
{
  int ret = OB_SUCCESS;
  const ObDITenantCollect *collect = NULL;
  void *buf = NULL;

  if (OB_ISNULL(di_map) || di_map_bucket_num <= 0 || !cache_iter.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  }
  while (OB_SUCC(ret)) {
    ret = cache_iter.get_next(collect);
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      break;
    } else if (OB_ISNULL(collect)) {
      ret = OB_ERR_SYS;
      // cannot print log in di
    } else if (0 != collect->last_access_time_) {
      ObTenantBucket &bucket = di_map[collect->tenant_id_ % di_map_bucket_num];
      ObDITenantCollect *got_collect = NULL;
      ret = bucket.get_the_node(collect->tenant_id_, got_collect);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (NULL == (buf = allocator.alloc(sizeof(ObDITenantCollect)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          got_collect = new (buf) ObDITenantCollect(&allocator);
          got_collect->tenant_id_ = collect->tenant_id_;
          bucket.list_.add_last(got_collect);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(got_collect)) {
          ret = OB_ERR_SYS;
        } else {
          callback(got_collect->base_value_, collect->base_value_);
        }
      }
    }
  }

  return ret;
}

}
}
