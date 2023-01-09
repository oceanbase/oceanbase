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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_OBJECT_
#define OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_OBJECT_

#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace sql
{
// The abstract interface class of library cache object, each object in the ObLibCacheNameSpace
// enum structure needs to inherit from this interface and implement its own implementation class
class ObILibCacheObject
{
friend class ObLCObjectManager;
public:
  enum CacheObjStatus
  {
    ACTIVE = 0,
    ERASED = 1,
    MARK_ERASED = 2
  };

  ObILibCacheObject(ObLibCacheNameSpace ns, lib::MemoryContext &mem_context);
  virtual ~ObILibCacheObject() {}

  inline ObLibCacheNameSpace get_ns() const { return ns_; }
  inline void set_ns(ObLibCacheNameSpace ns) { ns_ = ns; }
  inline bool is_sql_crsr() const { return ObLibCacheNameSpace::NS_CRSR == ns_; }
  inline bool is_prcr() const { return ObLibCacheNameSpace::NS_PRCR == ns_; }
  inline bool is_sfc() const { return ObLibCacheNameSpace::NS_SFC == ns_; }
  inline bool is_pkg() const { return ObLibCacheNameSpace::NS_PKG == ns_; }
  inline bool is_anon() const { return ObLibCacheNameSpace::NS_ANON == ns_; }
  inline bool is_valid_cache_obj() const { return ns_ > NS_INVALID && ns_ < NS_MAX; }
  inline uint64_t get_object_id() const { return object_id_; }
  inline int64_t get_mem_size() const { return allocator_.total(); }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count(const CacheRefHandleID ref_handle);
  inline common::ObIAllocator &get_allocator() { return allocator_; }
  inline lib::MemoryContext &get_mem_context() { return mem_context_; }
  inline bool added_lc() const { return added_to_lc_; }
  inline void set_added_lc(const bool added_to_lc) { added_to_lc_ = added_to_lc; }
  inline int64_t get_logical_del_time() const { return log_del_time_; }
  inline void set_logical_del_time(const int64_t timestamp) { log_del_time_ = timestamp; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline CacheRefHandleID get_dynamic_ref_handle() const { return dynamic_ref_handle_; }
  inline bool should_release(const int64_t safe_timestamp) const
  {
    // only free leaked cache object
    return 0 != get_ref_count() && get_logical_del_time() < safe_timestamp;
  }
  inline void set_dynamic_ref_handle(CacheRefHandleID ref_handle)
  {
    dynamic_ref_handle_ = ref_handle;
  }
  inline void set_obj_status(CacheObjStatus status) { obj_status_ = status; }
  inline CacheObjStatus get_obj_status() const { return obj_status_; }

  ///
  /// The following interfaces need to be inherited and implemented by derived classes
  virtual void reset();
  virtual void dump_deleted_log_info(const bool is_debug_log = true) const;
  virtual int before_cache_evicted();
  virtual int check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add);
  virtual int update_cache_obj_stat(ObILibCacheCtx &ctx);
  
  VIRTUAL_TO_STRING_KV(K_(ref_count),
                       K_(object_id),
                       K_(log_del_time),
                       K_(added_to_lc),
                       K_(ns),
                       K_(tenant_id));
                       
private:
  int64_t dec_ref_count(const CacheRefHandleID ref_handle);
protected:
  lib::MemoryContext mem_context_;
  common::ObIAllocator &allocator_;
  volatile int64_t ref_count_;
  uint64_t object_id_;
  int64_t log_del_time_;
  bool added_to_lc_;
  ObLibCacheNameSpace ns_;
  uint64_t tenant_id_;
  CacheRefHandleID dynamic_ref_handle_;
  CacheObjStatus obj_status_;
};


} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_OBJECT_
