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
#include "lib/ash/ob_active_session_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

OB_DEF_SERIALIZE(SequenceCacheNode)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(start_);
  OB_UNIS_ENCODE(end_);
  return ret;
}

OB_DEF_DESERIALIZE(SequenceCacheNode)
{
  int ret = OB_SUCCESS;
  share::ObSequenceValue start;
  share::ObSequenceValue end;
  OB_UNIS_DECODE(start);
  OB_UNIS_DECODE(end);
  // deep copy is needed to ensure that the memory of start and end will not be reclaimed
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(start_.assign(start))) {
    LOG_WARN("fail to assign start", K(ret));
  } else if (OB_FAIL(end_.assign(end))) {
    LOG_WARN("fail to assign end", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(SequenceCacheNode)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(start_);
  OB_UNIS_ADD_LEN(end_);
  return len;
}

int SequenceCacheNode::assign(const SequenceCacheNode &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(start_.assign(other.start_))) {
    LOG_WARN("fail to assign start", K(ret));
  } else if (OB_FAIL(end_.assign(other.end_))) {
    LOG_WARN("fail to assign end", K(ret));
  }
  return ret;
}


ObSequenceCache::ObSequenceCache()
  : inited_(false),
    cache_mutex_(common::ObLatchIds::SEQUENCE_CACHE_LOCK)
{
}

int ObSequenceCache::init(share::schema::ObMultiVersionSchemaService &schema_service,
                           common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  //const int64_t SEQUENCE_CACHE_BUCKET_SIZE = 1024;
  dml_proxy_.init(schema_service, sql_proxy);
  inited_ = true;
  //ret = sequence_cache_.create(SEQUENCE_CACHE_BUCKET_SIZE, ObModIds::OB_SCHEMA_SEQUENCE);
  ret = sequence_cache_.init();
  return ret;
}

ObSequenceCache &ObSequenceCache::get_instance()
{
  static ObSequenceCache instance_;
  return instance_;
}


/* move_next 逻辑的复杂性来自于两个需求要考虑：
 * 1. cycle 模式下循环取值
 * 2. 开始消费一个新的范围缓存时，要尽可能要让 gap 小
 */
int ObSequenceCache::move_next(const ObSequenceSchema &schema,
                               ObSequenceCacheItem &cache,
                               ObIAllocator &allocator, // 仅用于临时运算
                               ObSequenceValue &nextval)
{
  int ret = OB_SUCCESS;
  bool need_refill = false;
  if (OB_FAIL(need_refill_cache(schema, cache, allocator, need_refill))) {
    LOG_WARN("fail check if need refill cache", K(schema), K(ret));
  } else if (need_refill) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (OB_UNLIKELY(!cache.base_on_last_number_)) {
      // cache 中的第一个值，不做递增
      // nextval = cache.curr_node_.start();
      if (OB_SUCC(nextval.set(cache.curr_node_.start()))) {
        cache.base_on_last_number_ = true;
      }
    } else if (OB_UNLIKELY(schema.get_cycle_flag() && // 仅当 cycle 模式下才会出现本elif分支场景
            ((schema.get_increment_by() > static_cast<int64_t>(0) &&
              cache.curr_node_.start() < cache.last_number()) ||
             (schema.get_increment_by() < static_cast<int64_t>(0) &&
             cache.curr_node_.start() > cache.last_number())))) {
      // 出现了 cycle 的场景
      //  只要 start 值比 last_number 小，那么就认为**当前机器**遇到了一次循环
      //  对于出现一次循环的场景，不考虑 last_number 和 increment by，
      //  直接取 start 值为 nextval。这么做是合理的。
      // 注意：出现一次循环后，由于并发的存在，即使循环了，也可能 start > last_number
      //  这时候我们当成没有循环出现的场景来处理
      // nextval = cache.curr_node_.start();
      if (OB_FAIL(nextval.set(cache.curr_node_.start()))) {
        LOG_WARN("fail deep copy node value", K(ret));
      }
    } else {
      ObNumber new_start;
      if (OB_FAIL(cache.last_number().add(schema.get_increment_by(), new_start, allocator))) {
        LOG_WARN("fail calc new_start", K(ret));
      } else if (schema.get_increment_by() > static_cast<int64_t>(0)) {
        // 当取值跨越缓存时，为了尽可能减少 gap，
        // 让两个 nextval 的差尽可能接近 increment_by
        //
        //      last                       start
        //  |____o________|_ _ _ _ o'_ _ _ _ _|_____o''___________
        //
        //  _____ 表示缓存空间
        //  _ _ _ 表示缓存空洞
        //
        //  o 加上 increment by 之后，可能落在 o' 处，此时则保持 start 不变，作为 nextval
        //  如果落在 o''处，则需要更新 start 值为 o''，并将其作为 nextval
        if (new_start > cache.curr_node_.start()) {
          if (OB_FAIL(cache.curr_node_.set_start(new_start))) {
            LOG_WARN("fail update new_start value to cache.curr_node_", K(ret));
          }
        } else {
          // start unchanged, use start as nextval
        }
      } else {
        if (new_start < cache.curr_node_.start()) {
          if (OB_FAIL(cache.curr_node_.set_start(new_start))) {
            LOG_WARN("fail update new_start value to cache.curr_node_", K(ret));
          }
        } else {
          // start unchanged, use start as nextval
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(nextval.set(cache.curr_node_.start()))) {
          LOG_WARN("fail deep copy node value", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (schema.get_min_value() > nextval.val()) {
        ret = OB_ERR_SEQ_VALUE_EXCEED_LIMIT;
        LOG_USER_ERROR(OB_ERR_SEQ_VALUE_EXCEED_LIMIT, "MINVALUE");
      } else if (schema.get_max_value() < nextval.val()) {
        ret = OB_ERR_SEQ_VALUE_EXCEED_LIMIT;
        LOG_USER_ERROR(OB_ERR_SEQ_VALUE_EXCEED_LIMIT, "MAXVALUE");
      }
      // If the error is OB_ERR_SEQ_VALUE_EXCEED_LIMIT and the sequence has CYCLE flag,
      // we should refill the cache first to see whether the cache data has expired.
      if (OB_UNLIKELY(OB_ERR_SEQ_VALUE_EXCEED_LIMIT == ret && schema.get_cycle_flag())) {
        ret = OB_SIZE_OVERFLOW;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(cache.set_last_number(nextval.val()))) {
        LOG_WARN("fail to cache last_number", K(nextval), K(ret));
      }
    }
  }
  return ret;
}

// 函数说明：
// 用于判断是否需要向缓存中填入新值。
// 判断的一般原则是：下一次调用 nextval 是否能从 cache 中取到一个值
// 如果取不到，则返回 refill = true，通知缓存刷新
// 如果缓存刷新后还取不到合法值，则失败
int ObSequenceCache::need_refill_cache(const ObSequenceSchema &schema,
                                        ObSequenceCacheItem &cache,
                                        common::ObIAllocator &allocator,
                                        bool &refill)
{
  int ret = OB_SUCCESS;
  /* refill 条件：
   * - last_number 与 end 之间的间隙不够一次 inc
   * - 启用 prefetch 后，依然不够一次 inc
   */
  refill = false;

  if (OB_UNLIKELY(cache.curr_node_.start() == cache.curr_node_.end() && !cache.with_prefetch_node_)) {
    refill = true; // cache not init
  } else if (OB_UNLIKELY(!cache.base_on_last_number_)) {
    refill = false; // 没有last num 并且当前 current_node 有值，则不需要做 refill，取首个值
  } else if (schema.get_increment_by() > static_cast<int64_t>(0)) {
    if (OB_UNLIKELY(cache.curr_node_.start() < cache.last_number())) {
      refill = false;
    } else {
      // refill = (cache.curr_node_.end() - cache.last_number() <= schema.get_increment_by());
      ObNumber diff;
      if (OB_FAIL(cache.curr_node_.end().sub(cache.last_number(), diff, allocator))) {
        LOG_ERROR("fail sub number, unexpected", K(ret));
      } else {
        refill = (diff <= schema.get_increment_by());
      }
    }
    // 如果发现需要 refill，则尝试启用 prefetch 缓存，以避免 refill
    if (refill && cache.with_prefetch_node_ && OB_SUCC(ret)) {
      if (OB_FAIL(cache.combine_prefetch_node())) {
        LOG_WARN("fail combine prefetch node", K(ret));
      } else if (cache.curr_node_.start() < cache.last_number()) {
        refill = false;
      } else {
        // 考虑 increment 值在 prefetch 后变大的场景
        // 可能 prefetch 的值也不够一次 increment
        // refill = (cache.curr_node_.end() - cache.last_number() <= schema.get_increment_by());
        ObNumber diff;
        if (OB_FAIL(cache.curr_node_.end().sub(cache.last_number(), diff, allocator))) {
          LOG_ERROR("fail sub number, unexpected", K(ret));
        } else {
          refill = (diff <= schema.get_increment_by());
        }
      }
      LOG_INFO("after combine prefetch node",
               "id", schema.get_sequence_id(),
               "increment_by", schema.get_increment_by().format(),
               "inclusive_start", cache.curr_node_.start().format(),
               "exclusive_end", cache.curr_node_.end().format(),
               "last_number", cache.last_number().format(),
               K(refill),
               K(ret));
    }
  } else {
    if (OB_UNLIKELY(cache.curr_node_.start() > cache.last_number())) {
      refill = false;
    } else {
      // refill = (cache.curr_node_.end() - cache.last_number() >= schema.get_increment_by());
      ObNumber diff;
      if (OB_FAIL(cache.curr_node_.end().sub(cache.last_number(), diff, allocator))) {
        LOG_ERROR("fail sub number, unexpected", K(ret));
      } else {
        refill = (diff >= schema.get_increment_by());
      }
    }
    if (refill && cache.with_prefetch_node_ && OB_SUCC(ret)) {
      if (OB_FAIL(cache.combine_prefetch_node())) {
        LOG_WARN("fail combine prefetch node", K(ret));
      } else if (cache.curr_node_.start() > cache.last_number()) {
        refill = false;
      } else {
        // refill = (cache.curr_node_.end() - cache.last_number() >= schema.get_increment_by());
        ObNumber diff;
        if (OB_FAIL(cache.curr_node_.end().sub(cache.last_number(), diff, allocator))) {
          LOG_ERROR("fail sub number, unexpected", K(ret));
        } else {
          refill = (diff >= schema.get_increment_by());
        }
      }
      LOG_INFO("after combine prefetch node",
               "id", schema.get_sequence_id(),
               "increment_by", schema.get_increment_by().format(),
               "inclusive_start", cache.curr_node_.start().format(),
               "exclusive_end", cache.curr_node_.end().format(),
               "last_number", cache.last_number().format(),
               K(refill));
    }
  }
  return ret;
}

int ObSequenceCache::refill_sequence_cache(const ObSequenceSchema &schema,
                                           common::ObIAllocator &allocator,
                                           ObSequenceCacheItem &cache)
{
  int ret = OB_SUCCESS;
  SequenceCacheNode next_range;
  bool need_refetch = false; // 尾部剩余范围不足一次 increment by 的情况下需要重新 fetch
  int times = 0;

  ObNumber next_number;
  do {
    times++;
    need_refetch = false;
    if (OB_FAIL(dml_proxy_.next_batch(schema.get_tenant_id(),
                                      schema.get_sequence_id(),
                                      schema.get_schema_version(),
                                      schema.get_sequence_option(),
                                      next_range,
                                      cache))) {
      LOG_WARN("fail get next sequence batch", K(schema), K(ret));
    } else {
      // 判断是否需要重取，确保取得的值够一次 increment
      if (schema.get_cycle_flag() && cache.base_on_last_number_) {
        if (schema.get_increment_by() > static_cast<int64_t>(0)) {
          if (cache.curr_node_.start() > next_range.start()) {
            cache.base_on_last_number_ = false;
            LOG_INFO("got next batch in a new cycle", K(cache));
          } else if (OB_FAIL(cache.last_number().add(schema.get_increment_by(),
                                                    next_number,
                                                    allocator))) {
            LOG_WARN("fail add numbers", K(ret));
          } else if (next_number >= next_range.end()) {
            need_refetch = true;
            cache.base_on_last_number_ = false;
            LOG_INFO("next batch not enough, need refetch",
                     K(need_refetch),
                     K(cache),
                     K(schema));
          }
        } else {
          if (cache.curr_node_.start() < next_range.start()) {
            cache.base_on_last_number_ = false;
            LOG_INFO("got next batch in a new cycle", K(cache));
          } else if (OB_FAIL(cache.last_number().add(schema.get_increment_by(),
                                                     next_number,
                                                     allocator))) {
            LOG_WARN("fail add numbers", K(ret));
          } else if (next_number <= next_range.end()) {
            need_refetch = true;
            cache.base_on_last_number_ = false;
            LOG_INFO("next batch not enough, need refetch",
                     K(need_refetch),
                     K(cache),
                     K(schema));
          }
        }
      }

      if (!need_refetch) {
        // 注意：cache 中的 number 内存管理，其生命周期较长
        //       需要做好封装
        if (OB_FAIL(cache.curr_node_.set_start(next_range.start()))) {
          LOG_WARN("fail set start", K(next_range), K(ret));
        } else if (OB_FAIL(cache.curr_node_.set_end(next_range.end()))) {
          LOG_WARN("fail set end", K(next_range), K(ret));
        } else {
          LOG_INFO("update sequence curr_node cache success", K(cache), K(schema.get_tenant_id()),
                                                              K(schema.get_sequence_id()));
        }
        cache.last_refresh_ts_ = ObTimeUtility::current_time();
      }
    }
    if (times == 2) {
      LOG_INFO("refetch next batch result", K(cache), K(ret));
    }
  } while (OB_SUCC(ret) && need_refetch);
  return ret;
}


int ObSequenceCache::prefetch_sequence_cache(const ObSequenceSchema &schema,
                                             ObSequenceCacheItem &cache,
                                             ObSequenceCacheItem &old_cache)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml_proxy_.prefetch_next_batch(schema.get_tenant_id(),
                                             schema.get_sequence_id(),
                                             schema.get_schema_version(),
                                             schema.get_sequence_option(),
                                             cache.prefetch_node_,
                                             old_cache))) {
    LOG_WARN("fail get next sequence batch", K(schema), K(ret));
  } else {
    cache.last_refresh_ts_ = ObTimeUtility::current_time();
    LOG_INFO("prefetch sequence",
             "id", schema.get_sequence_id(),
             K(cache));
  }
  return ret;
}


int ObSequenceCache::get_item(CacheItemKey &key, ObSequenceCacheItem *&item)
{
  int ret = OB_SUCCESS;
  if (OB_ENTRY_NOT_EXIST == (ret = sequence_cache_.get(key, item))) {
    lib::ObMutexGuard guard(cache_mutex_); // 加锁再次确认，避免并发加入新节点
    if (OB_ENTRY_NOT_EXIST == (ret = sequence_cache_.get(key, item))) {
      if (OB_FAIL(sequence_cache_.alloc_value(item))) {
        LOG_WARN("fail alloc value", K(ret));
      } else if (OB_FAIL(sequence_cache_.insert_and_get(key, item))) {
        LOG_WARN("fail set cache item", K(key), K(ret));
      }
      if (OB_FAIL(ret) && nullptr != item) {
        sequence_cache_.free_value(item);
        item = nullptr;
      }
    }
  }
  return ret;
}

int ObSequenceCache::del_item(uint64_t tenant_id, CacheItemKey &key, obrpc::ObSeqCleanCacheRes &cache_res)
{
  int ret = OB_SUCCESS;

  if (OB_ENTRY_EXIST == (ret = sequence_cache_.contains_key(key))) {
    lib::ObMutexGuard guard(cache_mutex_); // 加锁再次确认，避免并发加入新节点
    ObSequenceCacheItem *item = nullptr;
    uint64_t compat_version = 0;
    if (OB_FAIL(sequence_cache_.get(key, item))) {
      // no cache, do nothing
    } else if (OB_FAIL(sequence_cache_.del(key))) {
      LOG_WARN("del sequence cache failed", K(ret));
    } else if (FAILEDx(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else if ((compat_version >= MOCK_DATA_VERSION_4_2_5_0
                && compat_version < DATA_VERSION_4_3_0_0)
               || compat_version >= DATA_VERSION_4_3_5_0) {
      lib::ObMutexGuard guard(item->alloc_mutex_);
      if (OB_FAIL(ret) || item->last_refresh_ts_ <= SequenceCacheStatus::INITED) {
        // do nothing
      } else if (OB_FAIL(cache_res.cache_node_.set_start(item->last_number()))) {
        LOG_WARN("fail to set cache value", K(ret));
      } else if (OB_FAIL(cache_res.cache_node_.set_end(item->curr_node_.end()))) {
        LOG_WARN("fail to set cache end", K(ret));
      } else if (item->with_prefetch_node_
                  && OB_FAIL(cache_res.prefetch_node_.assign(item->prefetch_node_))) {
        LOG_WARN("fail to assign prefetch node", K(ret));
      } else {
        item->last_refresh_ts_ = SequenceCacheStatus::DELETED;
        cache_res.inited_ = true;
        cache_res.with_prefetch_node_ = item->with_prefetch_node_;
      }
    } else {
      LOG_INFO("fail check if key in cache", K(ret), K(key));
    }
    if (nullptr != item) {
      sequence_cache_.revert(item);
    }
  }
  return (ret == OB_ENTRY_NOT_EXIST) ? OB_SUCCESS : ret;
}

int ObSequenceCache::nextval(const ObSequenceSchema &schema,
                             ObIAllocator &allocator, // 用于各种临时计算
                             ObSequenceValue &nextval)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_sequence_load);
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
  CacheItemKey key(schema.get_tenant_id(), schema.get_sequence_id());
  ObSequenceCacheItem *item = nullptr;
  if (OB_FAIL(get_item(key, item))) {
    LOG_WARN("fail get item", K(key), K(ret));
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    lib::ObMutexGuard guard(item->fetch_);
    item->alloc_mutex_.lock();
    if (item->last_refresh_ts_ == SequenceCacheStatus::DELETED) {
      ret = OB_AUTOINC_CACHE_NOT_EQUAL;
      LOG_WARN("cache has been cleared", K(ret), K(*item));
    } else {
      /* refill_sequence_cache 期间禁止调度器挂起 query */
      lib::DisableSchedInterGuard sched_guard;
      {
        LOG_DEBUG("nextval", K(schema));

        // step 1. 从 cache 中获取下一个值
        ret = move_next(schema, *item, allocator, nextval);

        // setp 2. cache 中的值已经使用完，需要重填 cache
        // 注意：预取功能正常的情况下，不会走到这个分支
        if (OB_SIZE_OVERFLOW == ret) {
          LOG_INFO("no more avaliable value in current cache, try refill cache", K(*item), K(ret));
          if (OB_FAIL(refill_sequence_cache(schema, allocator, *item))) {
            LOG_WARN("fail refill sequence cache", K(*item), K(ret));
          } else if (OB_FAIL(move_next(schema, *item, allocator, nextval))) {
            LOG_WARN("fail move next", K(*item), K(ret));
          }
          if (OB_SIZE_OVERFLOW == ret) {
            ret = OB_ERR_SEQ_VALUE_EXCEED_LIMIT;
            if (schema.get_increment_by() < static_cast<int64_t>(0)) {
              LOG_USER_ERROR(OB_ERR_SEQ_VALUE_EXCEED_LIMIT, "MINVALUE");
            } else if (schema.get_increment_by() > static_cast<int64_t>(0)) {
              LOG_USER_ERROR(OB_ERR_SEQ_VALUE_EXCEED_LIMIT, "MAXVALUE");
            }
          }
        }

        // step 3. 尝试预取
        if (OB_SUCC(ret) &&
            !item->prefetching_ &&
            schema.get_cache_size() > static_cast<int64_t>(1)  && /* cache size = 1 时禁止 prefetch */
            schema.get_order_flag() == false /* 有 order 时禁止 prefetch */) {
          if (OB_UNLIKELY(!item->with_prefetch_node_)) {
            //const int64_t rest = std::abs(item->curr_node_.end() - item->curr_node_.start());
            //const int64_t full = std::abs(schema.get_increment_by() * schema.get_cache_size());
            ObNumber rest;
            ObNumber full;
            ObNumberCalc calc(item->curr_node_.end(), allocator);
            // 拍脑袋的值，表示使用了 1/2 的值后就开始预取
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
        if (OB_FAIL(prefetch_sequence_cache(schema, mock_item, *item))) {
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
            }
          }
        }
        item->prefetching_ = false;
      }
    }
  }
  if (nullptr != item) {
    item->alloc_mutex_.unlock();
    sequence_cache_.revert(item);
  }
  return ret;
}

int ObSequenceCache::remove(uint64_t tenant_id, uint64_t sequence_id, obrpc::ObSeqCleanCacheRes &cache_res)
{
  CacheItemKey key(tenant_id, sequence_id);
  return del_item(tenant_id, key, cache_res);
}
