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

#ifndef _OB_MALLOC_ALLOCATOR_H_
#define _OB_MALLOC_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/alloc/ob_tenant_ctx_allocator.h"
#include "lib/alloc/alloc_func.h"
#include "lib/lock/ob_rwlock.h"

namespace oceanbase
{
namespace lib
{
using std::nullptr_t;
class ObTenantCtxAllocatorGuard
{
public:
  [[nodiscard]] ObTenantCtxAllocatorGuard()
    : ObTenantCtxAllocatorGuard(nullptr) {}
  [[nodiscard]] ObTenantCtxAllocatorGuard(ObTenantCtxAllocator *allocator, const bool lock=true)
    : allocator_(allocator), lock_(lock)
  {
    if (OB_LIKELY(allocator_ != nullptr && lock)) {
      allocator_->inc_ref_cnt(1);
    }
  }
  ~ObTenantCtxAllocatorGuard()
  {
    revert();
  }
  ObTenantCtxAllocatorGuard(ObTenantCtxAllocatorGuard &&other)
  {
    *this = std::move(other);
  }
  ObTenantCtxAllocatorGuard &operator=(ObTenantCtxAllocatorGuard &&other)
  {
    revert();
    allocator_ = other.allocator_;
    lock_ = other.lock_;
    other.allocator_ = nullptr;
    return *this;
  }
  ObTenantCtxAllocator* operator->() const
  {
    return allocator_;
  }
  ObTenantCtxAllocator* ref_allocator() const
  {
    return allocator_;
  }
  void revert()
  {
    if (OB_LIKELY(allocator_ != nullptr && lock_)) {
      allocator_->inc_ref_cnt(-1);
    }
    allocator_ = nullptr;
  }
private:
  ObTenantCtxAllocator *allocator_;
  bool lock_;
};

inline bool operator==(const ObTenantCtxAllocatorGuard &__a, nullptr_t)
{ return __a.ref_allocator() == nullptr; }

inline bool operator==(nullptr_t, const ObTenantCtxAllocatorGuard &__b)
{ return nullptr == __b.ref_allocator(); }

inline bool operator!=(const ObTenantCtxAllocatorGuard &__a, nullptr_t)
{ return __a.ref_allocator() != nullptr; }

inline bool operator!=(nullptr_t, const ObTenantCtxAllocatorGuard &__b)
{ return nullptr != __b.ref_allocator(); }

inline bool operator==(const ObTenantCtxAllocatorGuard &__a, const ObTenantCtxAllocatorGuard &__b)
{ return __a.ref_allocator() == __b.ref_allocator(); }

inline bool operator!=(const ObTenantCtxAllocatorGuard &__a, const ObTenantCtxAllocatorGuard &__b)
{ return __a.ref_allocator() != __b.ref_allocator(); }

// It's the implement of ob_malloc/ob_free/ob_realloc interface.  The
// class separates allocator for each tenant thus tenant vaolates each
// other.
class ObMallocAllocator
    : public common::ObIAllocator
{
private:
  static const uint64_t PRESERVED_TENANT_COUNT = 10000;
public:
  ObMallocAllocator();
  virtual ~ObMallocAllocator();

  void *alloc(const int64_t size);
  void *alloc(const int64_t size, const ObMemAttr &attr);
  void *realloc(const void *ptr, const int64_t size, const ObMemAttr &attr);
  void free(void *ptr);

  void set_root_allocator();
  static ObMallocAllocator *get_instance();

  ObTenantCtxAllocatorGuard get_tenant_ctx_allocator_without_tlcache(uint64_t tenant_id,
                                                                     uint64_t ctx_id) const;
  ObTenantCtxAllocatorGuard get_tenant_ctx_allocator(uint64_t tenant_id, uint64_t ctx_id) const;
  int create_and_add_tenant_allocator(uint64_t tenant_id);
  ObTenantCtxAllocatorGuard get_tenant_ctx_allocator_unrecycled(uint64_t tenant_id,
                                                               uint64_t ctx_id) const;
  void get_unrecycled_tenant_ids(uint64_t *ids, int cap, int &cnt) const;

  // statistic relating
  void set_urgent(int64_t bytes);
  int64_t get_urgent() const;
  void set_reserved(int64_t bytes);
  int64_t get_reserved() const;
  int set_tenant_limit(uint64_t tenant_id, int64_t bytes);
  int64_t get_tenant_limit(uint64_t tenant_id);
  int64_t get_tenant_hold(uint64_t tenant_id);
  int64_t get_tenant_cache_hold(uint64_t tenant_id);
  int64_t get_tenant_remain(uint64_t tenant_id);
  int64_t get_tenant_ctx_hold(const uint64_t tenant_id, const uint64_t ctx_id) const;
  void get_tenant_label_usage(uint64_t tenant_id, ObLabel &label, common::ObLabelItem &item) const;

  void print_tenant_ctx_memory_usage(uint64_t tenant_id) const;
  void print_tenant_memory_usage(uint64_t tenant_id) const;
  int set_tenant_ctx_idle(
      const uint64_t tenant_id, const uint64_t ctx_id, const int64_t size, const bool reserve = false);
  int64_t sync_wash(uint64_t tenant_id, uint64_t from_ctx_id, int64_t wash_size);
  int64_t sync_wash();
  int recycle_tenant_allocator(uint64_t tenant_id);
  int64_t get_max_used_tenant_id() { return max_used_tenant_id_; }
  void make_allocator_create_on_demand() { create_on_demand_ = true; }
  static bool is_inited_;
private:
  using InvokeFunc = std::function<int (ObTenantMemoryMgr*)>;
  static int with_resource_handle_invoke(uint64_t tenant_id, InvokeFunc func);
  int create_tenant_allocator(uint64_t tenant_id, ObTenantCtxAllocator *&allocator);
  void destroy_tenant_allocator(ObTenantCtxAllocator *allocator);
  int create_tenant_allocator(uint64_t tenant_id, void *buf, ObTenantCtxAllocator *&allocator);
  int add_tenant_allocator(ObTenantCtxAllocator *allocator);
  ObTenantCtxAllocator *take_off_tenant_allocator(uint64_t tenant_id);
  void add_tenant_allocator_unrecycled(ObTenantCtxAllocator *allocator);
  ObTenantCtxAllocator *take_off_tenant_allocator_unrecycled(uint64_t tenant_id);
#ifdef ENABLE_SANITY
  int get_chunks(ObTenantCtxAllocator *ta, AChunk **chunks, int cap, int &cnt);
  void modify_tenant_memory_access_permission(ObTenantCtxAllocator *ta, bool accessible);
public:
  bool enable_tenant_leak_memory_protection_ = true;
#endif
public:
  bool force_explict_500_malloc_ = false;
  bool pl_leaked_times_ = 0;
  bool force_malloc_for_absent_tenant_ = false;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMallocAllocator);
  class BucketLock
  {
  public:
    static const uint64_t BUCKET_COUNT = 32;
    int rdlock(const int bucket_idx)
    {
      return locks_[bucket_idx].rdlock(ObLatchIds::OB_ALLOCATOR_LOCK);
    }
    int wrlock_all()
    {
      int ret = OB_SUCCESS;
      int last_succ_idx = -1;
      for (int i = 0; OB_SUCC(ret) && i < BUCKET_COUNT; i++) {
        ret = locks_[i].wrlock(ObLatchIds::OB_ALLOCATOR_LOCK);
        if (OB_SUCC(ret)) {
          last_succ_idx = i;
        }
      }
      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i <= last_succ_idx; ++i) {
          locks_[i].unlock();
        }
      }
      return ret;
    }
    void unlock(const int bucket_idx)
    {
      locks_[bucket_idx].unlock();
    }
    void unlock_all()
    {
      for (int i = 0; i < BUCKET_COUNT; i++) {
        locks_[i].unlock();
      }
    }
    void enable_record_stat(const bool need_record)
    {
      for (int64_t i = 0; i < BUCKET_COUNT; ++i) {
        locks_[i].enable_record_stat(need_record);
      }
    }
  private:
    ObLatch locks_[BUCKET_COUNT];
  };
  class BucketRLockGuard
  {
  public:
    [[nodiscard]] explicit BucketRLockGuard(BucketLock &lock, const int bucket_idx)
      : lock_(lock), bucket_idx_(bucket_idx)
    {
      lock_.rdlock(bucket_idx_);
    }
    ~BucketRLockGuard()
    {
      lock_.unlock(bucket_idx_);
    }
  private:
    BucketLock &lock_;
    const int bucket_idx_;
  };
  class BucketWLockGuard
  {
  public:
    [[nodiscard]] explicit BucketWLockGuard(BucketLock &lock)
      : lock_(lock)
    {
      lock_.wrlock_all();
    }

    ~BucketWLockGuard()
    {
      lock_.unlock_all();
    }
  private:
    BucketLock &lock_;
  };
private:
  BucketLock locks_[PRESERVED_TENANT_COUNT];
  ObTenantCtxAllocator *allocators_[PRESERVED_TENANT_COUNT];
  ObLatch unrecycled_lock_;
  ObTenantCtxAllocator *unrecycled_allocators_;
  int64_t reserved_;
  int64_t urgent_;
  uint64_t max_used_tenant_id_;
  bool create_on_demand_;
}; // end of class ObMallocAllocator

extern __thread ObTenantCtxAllocator *tl_ta;
class ObTLTaGuard
{
public:
  [[nodiscard]] ObTLTaGuard();
  [[nodiscard]] explicit ObTLTaGuard(const int64_t tenant_id);
  ~ObTLTaGuard();
  void switch_to(const int64_t tenant_id);
  void revert();
  ObTenantCtxAllocator *ta_bak_;
  ObTenantCtxAllocatorGuard ta_;
  bool restore_;
};

extern int64_t mtl_id();

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OB_MALLOC_ALLOCATOR_H_ */
