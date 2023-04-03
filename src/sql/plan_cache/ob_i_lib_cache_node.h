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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_NODE_
#define OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_NODE_

#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/stat/ob_latch_define.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace sql
{
class ObPlanCache;
class ObILibCacheKey;
class ObILibCacheCtx;
class ObILibCacheObject;

struct StmtStat
{
  int64_t memory_used_;
  int64_t last_active_timestamp_;           // used now
  int64_t execute_average_time_;
  int64_t execute_slowest_time_;
  int64_t execute_slowest_timestamp_;
  int64_t execute_count_;                   // used now
  int64_t execute_slow_count_;
  int64_t ps_count_;
  bool to_delete_;
  StmtStat()
      : memory_used_(0),
        last_active_timestamp_(0),
        execute_average_time_(0),
        execute_slowest_time_(0),
        execute_slowest_timestamp_(0),
        execute_count_(0),
        execute_slow_count_(0),
        ps_count_(0),
        to_delete_(false)
  {
  }

  void reset()
  {
    memory_used_ = 0;
    last_active_timestamp_ = 0;
    execute_average_time_ = 0;
    execute_slowest_time_ = 0;
    execute_slowest_timestamp_ = 0;
    execute_count_ = 0;
    execute_slow_count_ = 0;
    ps_count_ = 0;
    to_delete_ = false;
  }

  double weight()
  {
    int64_t time_interval = common::ObTimeUtility::current_time() - last_active_timestamp_;
    double weight = common::OB_PC_WEIGHT_NUMERATOR / static_cast<double>(time_interval);
    return weight;
  }

  TO_STRING_KV(K_(memory_used),
               K_(last_active_timestamp),
               K_(execute_average_time),
               K_(execute_slowest_time),
               K_(execute_slowest_timestamp),
               K_(execute_count),
               K_(execute_slow_count),
               K_(ps_count),
               K_(to_delete));
};

typedef ObList<ObILibCacheObject*, ObIAllocator> CacheObjList;

// The abstract interface class of library cache node, each object in the ObLibCacheNameSpace
// enum structure needs to inherit from this interface and implement its own implementation class
class ObILibCacheNode
{
friend class ObLCNodeFactory;
public:
  ObILibCacheNode(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
    : mem_context_(mem_context),
      allocator_(mem_context->get_safe_arena_allocator()),
      rwlock_(),
      ref_count_(0),
      lib_cache_(lib_cache),
      co_list_lock_(common::ObLatchIds::PLAN_SET_LOCK),
      co_list_(allocator_)
  {
    lock_timeout_ts_ = GCONF.large_query_threshold;
  }
  virtual ~ObILibCacheNode();
  /**
   * @brief initialize library cache node
   * @param cache_obj[in], object that need to be cached
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init(ObILibCacheCtx &ctx, const ObILibCacheObject *cache_obj);
  /**
   * @brief decrease the reference count of all cache objects in the library cache value by one
   */
  void free_cache_obj_array();
  /**
   * @brief remove all plan stat from all cache objects in the library cache node
   */
  int remove_all_plan_stat();
  /**
   * @brief remove cache obj from cache_obj_list_
   * @param obj_id[in], obj id to remove
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int remove_cache_obj_entry(const ObCacheObjID obj_id);
  /**
   * @brief get cache object from library cache
   * @param ctx[in], library cache context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  
  int get_cache_obj(ObILibCacheCtx &ctx, ObILibCacheKey *key, ObILibCacheObject *&cache_obj);
  /**
   * @brief add cache object to library cache 
   * @param ctx[in], library cache context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int add_cache_obj(ObILibCacheCtx &ctx, ObILibCacheKey *key, ObILibCacheObject *cache_obj);
  /**
   * @brief erase cache object from library cache 
   * @param ctx[in], library cache context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  //int erase_cache_obj(ObILibCacheCtx &context, ObILibCacheObject *cache_obj);
  virtual int lock(bool is_rdlock);
  virtual int update_node_stat(ObILibCacheCtx &ctx);
  StmtStat *get_node_stat() { return &node_stat_; }
  int unlock() { return rwlock_.unlock(); }
  int64_t inc_ref_count(const CacheRefHandleID ref_handle);
  int64_t dec_ref_count(const CacheRefHandleID ref_handle);
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  common::ObIAllocator *get_allocator() { return &allocator_; }
  common::ObIAllocator &get_allocator_ref() { return allocator_; }
  lib::MemoryContext &get_mem_context() { return mem_context_; }
  int64_t get_mem_size();
  ObPlanCache *get_lib_cache() const { return lib_cache_; }

  VIRTUAL_TO_STRING_KV(K_(ref_count), K_(lock_timeout_ts));

protected:
  void set_lock_timeout_threshold(int64_t threshold)
  {
    lock_timeout_ts_ = threshold;
  }
  /**
   * @brief called by get_cache_obj(), each object in the ObLibCacheNameSpace enumeration structure
   * needs to inherit this interface and implement its own inner get implementation
   * @param ctx[in], library cache context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */ 
  virtual int inner_get_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *&cache_obj) = 0;
  /**
   * @brief called by add_cache_obj(), each object in the ObLibCacheNameSpace enumeration structure
   * needs to inherit this interface and implement its own inner add implementation
   * @param ctx[in], library cache context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int inner_add_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *cache_obj) = 0;
  /**
   * @brief called by erase_cache_obj(), each object in the ObLibCacheNameSpace enumeration structure
   * needs to inherit this interface and implement its own inner erase implementation
   * @param ctx[in], library cache context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  //int inner_erase_cache_obj(ObILibCacheCtx &ctx, ObILibCacheObject *cache_obj) = 0;
  /**
   * @brief called to do something before cache evicted
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int before_cache_evicted();

protected:
  lib::MemoryContext mem_context_;
  // Note: all memory allocations in ObILibCacheNode can only use allocator_, when the ObILibCacheNode
  // node is destructed, all memory allocated by allocator_ will be released
  common::ObIAllocator &allocator_;
  common::TCRWLock rwlock_;
  int64_t ref_count_;
  int64_t lock_timeout_ts_;
  StmtStat node_stat_;
  ObPlanCache *lib_cache_;
  common::SpinRWLock co_list_lock_;
  CacheObjList co_list_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_NODE_
