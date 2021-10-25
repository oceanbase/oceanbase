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

#define USING_LOG_PREFIX SHARE

#include "ob_sequence_cache.h"
using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObSequenceCache::ObSequenceCache() : inited_(false)
{}

int ObSequenceCache::init(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy,
    obrpc::ObSrvRpcProxy* srv_proxy, share::ObAliveServerTracer* server_tracer)
{
  int ret = OB_SUCCESS;
  // const int64_t SEQUENCE_CACHE_BUCKET_SIZE = 1024;
  dml_proxy_.init(schema_service, sql_proxy);
  inited_ = true;
  ObSequenceSyncProxy::get_instance().init(srv_proxy, server_tracer, &sequence_cache_, &cache_mutex_);
  // ret = sequence_cache_.create(SEQUENCE_CACHE_BUCKET_SIZE, ObModIds::OB_SCHEMA_SEQUENCE);
  ret = sequence_cache_.init();
  return ret;
}

ObSequenceCache& ObSequenceCache::get_instance()
{
  static ObSequenceCache instance_;
  return instance_;
}

int ObSequenceCache::move_next(
    const ObSequenceSchema& schema, ObSequenceCacheItem& cache, ObIAllocator& allocator, ObSequenceValue& nextval)
{
  int ret = OB_SUCCESS;
  bool need_refill = false;
  if (OB_FAIL(need_refill_cache(schema, cache, need_refill))) {
    LOG_WARN("fail check if need refill cache", K(schema), K(ret));
  } else if (need_refill) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (OB_FAIL(nextval.set(cache.curr_node_.start()))) {
      LOG_WARN("fail deep copy node value", K(ret));
    } else {
      ObNumber new_start;
      if (OB_FAIL(cache.curr_node_.start().add(schema.get_increment_by(), new_start, allocator))) {
        LOG_WARN("fail calc new_start", K(ret));
      } else {
        if (OB_FAIL(cache.curr_node_.set_start(new_start))) {
          LOG_WARN("fail update new_start value to cache.curr_node_", K(ret));
        }
      }
    }
    
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cache.set_last_number(nextval.val()))) {
        LOG_WARN("fail to cache last_number", K(nextval), K(ret));
      } else {
        cache.base_on_last_number_ = true;
      }

      if ((schema.get_increment_by() > static_cast<int64_t>(0) && cache.curr_node_.start() >= cache.curr_node_.end()) || 
          (schema.get_increment_by() < static_cast<int64_t>(0) && cache.curr_node_.start() <= cache.curr_node_.end())){
        cache.enough_cache_node_ = false;
      }
    }
  }
  return ret;
}

int ObSequenceCache::need_refill_cache(
    const ObSequenceSchema& schema, ObSequenceCacheItem& cache, bool& refill)
{
  int ret = OB_SUCCESS;
  refill = false;

  if (OB_UNLIKELY(!cache.enough_cache_node_ ||
      (schema.get_increment_by() > static_cast<int64_t>(0) && cache.curr_node_.start() >= cache.curr_node_.end()) || 
      (schema.get_increment_by() < static_cast<int64_t>(0) && cache.curr_node_.start() <= cache.curr_node_.end()))) {
    refill = true;  // cache not init or cache run out
  } 

  if (refill && cache.with_prefetch_node_ && OB_SUCC(ret)) {
    if (OB_FAIL(cache.combine_prefetch_node())) {
      LOG_WARN("fail combine prefetch node", K(ret));
    } else if ((schema.get_increment_by() > static_cast<int64_t>(0) && cache.curr_node_.start() >= cache.curr_node_.end()) || 
        (schema.get_increment_by() < static_cast<int64_t>(0) && cache.curr_node_.start() <= cache.curr_node_.end())) {
      refill = true;  // cache not init or cache run out
    } else {
      refill = false;
      cache.enough_cache_node_ = true;
    }
    
    LOG_INFO("after combine prefetch node",
        "id",
        schema.get_sequence_id(),
        "increment_by",
        schema.get_increment_by().format(),
        "inclusive_start",
        cache.curr_node_.start().format(),
        "exclusive_end",
        cache.curr_node_.end().format(),
        "last_number",
        cache.last_number().format(),
        K(refill),
        K(ret));
  }
  return ret;
}

int ObSequenceCache::refill_sequence_cache(const ObSequenceSchema& schema, ObSequenceCacheItem& cache)
{
  int ret = OB_SUCCESS;
  SequenceCacheNode next_range;
  bool need_refetch = false;
  int times = 0;

  do {
    times++;
    need_refetch = false;
    if (OB_FAIL(dml_proxy_.next_batch(
            schema.get_tenant_id(), schema.get_sequence_id(), schema.get_sequence_option(), next_range))) {
      LOG_WARN("fail get next sequence batch", K(schema), K(ret));
    } else {
      if (schema.get_cycle_flag()) {
        if (schema.get_increment_by() > static_cast<int64_t>(0)) {
          if (cache.curr_node_.start() > next_range.start()) {
            LOG_INFO("got next batch in a new cycle", K(cache));
          } else if (cache.curr_node_.start() >= next_range.end()) {
            need_refetch = true;
            LOG_INFO("next batch not enough, need refetch", K(need_refetch), K(cache), K(schema));
          }
        } else {
          if (cache.curr_node_.start() < next_range.start()) {
            LOG_INFO("got next batch in a new cycle", K(cache));
          } else if (cache.curr_node_.start() <= next_range.end()) {
            need_refetch = true;
            LOG_INFO("next batch not enough, need refetch", K(need_refetch), K(cache), K(schema));
          }
        }
      }

      if (!need_refetch) {
        if (OB_FAIL(cache.curr_node_.set_start(next_range.start()))) {
          LOG_WARN("fail set start", K(next_range), K(ret));
        } else if (OB_FAIL(cache.curr_node_.set_end(next_range.end()))) {
          LOG_WARN("fail set end", K(next_range), K(ret));
        } else if (OB_FAIL(cache.curr_node_.set_round(next_range.round()))) {
          LOG_WARN("fail set round", K(next_range), K(ret));
        } else {
          LOG_INFO("update sequence curr_node cache success", K(cache));
        }
        cache.enough_cache_node_ = true;
        cache.last_refresh_ts_ = ObTimeUtility::current_time();
      }
    }
    if (times == 2) {
      LOG_INFO("refetch next batch result", K(cache), K(ret));
    }
  } while (OB_SUCC(ret) && need_refetch);
  return ret;
}

int ObSequenceCache::prefetch_sequence_cache(const ObSequenceSchema& schema, ObSequenceCacheItem& cache)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml_proxy_.prefetch_next_batch(
          schema.get_tenant_id(), schema.get_sequence_id(), schema.get_sequence_option(), cache.prefetch_node_))) {
    LOG_WARN("fail get next sequence batch", K(schema), K(ret));
  } else {
    cache.last_refresh_ts_ = ObTimeUtility::current_time();
    LOG_INFO("prefetch sequence", "id", schema.get_sequence_id(), K(cache));
  }
  return ret;
}

int ObSequenceCache::get_item(CacheItemKey& key, ObSequenceCacheItem*& item)
{
  int ret = OB_SUCCESS;
  if (OB_ENTRY_NOT_EXIST == (ret = sequence_cache_.get(key, item))) {
    lib::ObMutexGuard guard(cache_mutex_);
    if (OB_ENTRY_NOT_EXIST == (ret = sequence_cache_.get(key, item))) {
      if (NULL == (item = op_alloc(ObSequenceCacheItem))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc cache node", K(key), K(ret));
      } else if (OB_FAIL(sequence_cache_.insert_and_get(key, item))) {
        LOG_WARN("fail set cache item", K(key), K(ret));
      }
      if (OB_FAIL(ret) && nullptr != item) {
        op_free(item);
        item = nullptr;
      }
    }
  }
  return ret;
}

int ObSequenceCache::nextval(const ObSequenceSchema& schema, ObIAllocator& allocator, ObSequenceValue& nextval)
{
  int ret = OB_SUCCESS;
  /* 1. if cache item not exist, create a obsolete cache item
   * 2. read and lock cache item
   * 3. if cache item obsolete
   *      - update cache item value via sql
   *      - read cache item
   *      - if cache item obsolete, fail
   * 4. update cache item
   * 5. cleanup: unlock cache item
   */
  bool need_prefetch = false;
  CacheItemKey key(schema.get_sequence_id());
  ObSequenceCacheItem* item = nullptr;
  if (OB_FAIL(get_item(key, item))) {
    LOG_WARN("fail get item", K(key), K(ret));
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    lib::ObMutexGuard guard(item->alloc_mutex_);
    share::DisableSchedInterGuard sched_guard;
    {
      LOG_DEBUG("nextval", K(schema));

      // step 1. get next val in cache
      ret = move_next(schema, *item, allocator, nextval);

      // setp 2. cache resources used up, refill cache
      if (OB_SIZE_OVERFLOW == ret) {
        LOG_INFO("no more avaliable value in current cache, try refill cache", K(*item), K(ret));
        if (OB_FAIL(refill_sequence_cache(schema, *item))) {
          LOG_WARN("fail refill sequence cache", K(*item), K(ret));
        } else if (OB_FAIL(move_next(schema, *item, allocator, nextval))) {
          LOG_WARN("fail move next", K(*item), K(ret));
        }
      }

      // step 3. try prefetch
      if (OB_SUCC(ret) && !item->prefetching_ &&
          schema.get_cache_size() > static_cast<int64_t>(1) && /* cache size = 1, disable prefetch */
          schema.get_order_flag() == false /* order prefetch */) {
        if (OB_UNLIKELY(!item->with_prefetch_node_)) {
          // const int64_t rest = std::abs(item->curr_node_.end() - item->curr_node_.start());
          // const int64_t full = std::abs(schema.get_increment_by() * schema.get_cache_size());
          ObNumber rest;
          ObNumber full;
          ObNumberCalc calc(item->curr_node_.end(), allocator);
          static const int64_t PREFETCH_OP_THRESHOLD = 2;
          //
          // const int64_t rest = std::abs(item->curr_node_.end_ - item->curr_node_.start_) * PREFETCH_OP_THRESHOLD;
          // const int64_t full = std::abs(schema.get_increment_by() * schema.get_cache_size());
          // if (rest < full) {
          //    enable prefetch
          // }
          //
          if (OB_FAIL(calc.sub(item->curr_node_.start()).mul(PREFETCH_OP_THRESHOLD).get_result(rest))) {
            LOG_WARN("fail do number sub", K(ret));
          } else if (OB_FAIL(schema.get_increment_by().mul(schema.get_cache_size(), full, allocator))) {
            LOG_WARN("fail do number multiply", K(ret));
          } else if (rest.abs() <= full.abs()) {
            item->prefetching_ = true;
            need_prefetch = true;
          }
        }
      }
    }

    if (OB_SUCC(ret) && need_prefetch) {
      ObSequenceCacheItem mock_item;
      if (OB_FAIL(prefetch_sequence_cache(schema, mock_item))) {
        int prefetch_err = ret;
        ret = OB_SUCCESS;
        LOG_WARN("fail refill sequence cache. ignore prefrech error", K(prefetch_err), K(ret));
      } else {
        LOG_INFO("dump item", K(mock_item), K(*item));
        if (item->with_prefetch_node_) {
          LOG_INFO("new item has been fetched by other, ignore");
        } else {
          item->last_refresh_ts_ = mock_item.last_refresh_ts_;
          item->with_prefetch_node_ = true;
          if (OB_FAIL(item->prefetch_node_.set_start(mock_item.prefetch_node_.start()))) {
            LOG_WARN("fail set start for pretch node", K(ret));
          } else if (OB_FAIL(item->prefetch_node_.set_end(mock_item.prefetch_node_.end()))) {
            LOG_WARN("fail set end for pretch node", K(ret));
          } else if (OB_FAIL(item->prefetch_node_.set_round(mock_item.prefetch_node_.round()))) {
            LOG_WARN("fail set end for pretch node", K(ret));
          }
        }
      }
      item->prefetching_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("sequence current node", K(item->curr_node_.start()), K(item->curr_node_.end()), K(item->curr_node_.round()));
    LOG_DEBUG("sequence prefetching node", K(item->with_prefetch_node_), K(item->prefetch_node_.start()), 
        K(item->prefetch_node_.end()), K(item->prefetch_node_.round()));
  } else {
    LOG_WARN("sequence nextval failed");
  }

  if (nullptr != item) {
    sequence_cache_.revert(item);
  }

  return ret;
}

int ObSequenceCache::lastval(const ObSequenceSchema& schema, ObSequenceValue& lastval, bool& inited)
{
  int ret = OB_SUCCESS;

  CacheItemKey key(schema.get_sequence_id());
  ObSequenceCacheItem* item = nullptr;
  if (OB_FAIL(get_item(key, item))) {
    LOG_WARN("fail get item", K(key), K(ret));
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    lib::ObMutexGuard guard(item->alloc_mutex_);
    share::DisableSchedInterGuard sched_guard;
    {
      LOG_DEBUG("lastval", K(schema));

      if(OB_LIKELY(item->base_on_last_number_)){ // last_value is initialized
        if (OB_FAIL(lastval.set(item->last_number()))) {
          LOG_WARN("get lastval fail", K(ret), K(item->curr_node_.start()));
        } else {
          inited = true;
        }
      } else {
        inited = false;
      }
    }
  }

  return ret;
}

int ObSequenceCache::setval(const share::schema::ObSequenceSchema& schema, const ObSequenceValue& new_next_val, const ObSequenceValue& new_round, 
    ObSequenceValue& value, bool& valid)
{
  int ret = OB_SUCCESS;
  valid = true;

  LOG_DEBUG("sequence set value", K(schema), K(new_next_val), K(new_round));
  CacheItemKey key(schema.get_sequence_id());
  ObSequenceCacheItem* item = nullptr;
  bool increment_pos = schema.get_increment_by() > static_cast<int64_t>(0) ? true : false;
  if (OB_FAIL(get_item(key, item))) {
    LOG_WARN("fail get item", K(key), K(ret));
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (item->enough_cache_node_ && OB_FAIL(check_setval(increment_pos, item->curr_node_, new_next_val, new_round, valid))) {
    ret = OB_ERR_UNEXPECTED;
    valid = false;
  } 
  
  if (OB_SUCC(ret) && valid) {
    lib::ObMutexGuard guard(item->alloc_mutex_);
    share::DisableSchedInterGuard sched_guard;
    {
      // setval gurantees the next value of sequence must large than the parameter in function
      // it doesn't gurantee the next value is the new_next_val
      // actually, setval will clean the sequence cache of all observers

      if (OB_SUCC(dml_proxy_.set_next_batch(schema.get_tenant_id(), schema.get_sequence_id(), 
          increment_pos, new_next_val.val(), new_round.val(), ObSequenceSyncProxy::get_instance()))) {
        value.set(new_next_val.val());
      } else {
        LOG_WARN("setval failed", K(ret));
      }
    }
  }

  return ret;
}

int ObSequenceCache::check_setval(const bool increment_pos, const SequenceCacheNode& node, 
    const ObSequenceValue& new_next_val, const ObSequenceValue& new_round, bool& valid) {
  int ret = OB_SUCCESS;
  if (new_round.val() < node.round()) {
    valid = false;
  } else if (new_round.val() > node.round()){
    valid = true;
  } else { // new_round.val() == node.round()
    if ((increment_pos && new_next_val.val() < node.start()) ||
        (!increment_pos && new_next_val.val() > node.start())) {
      valid = false;
    } else {
      valid = true;
    }
  } 

  return ret;
}
