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

#ifndef OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_
#define OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_
#include "ob_handle_list.h"
#include "ob_fifo_arena.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/throttle/ob_share_throttle_define.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtable;
};

namespace share
{

// record the throttled alloc size of memstore in this thread
OB_INLINE int64_t &memstore_throttled_alloc()
{
  RLOCAL_INLINE(int64_t, throttled_alloc);
  return throttled_alloc;
}

struct FrozenMemstoreInfoLogger
{
  FrozenMemstoreInfoLogger(char* buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
  ~FrozenMemstoreInfoLogger() {}
  int operator()(ObDLink* link);
  char* buf_;
  int64_t limit_;
  int64_t pos_;
};

struct ActiveMemstoreInfoLogger
{
  ActiveMemstoreInfoLogger(char* buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
  ~ActiveMemstoreInfoLogger() {}
  int operator()(ObDLink* link);
  char* buf_;
  int64_t limit_;
  int64_t pos_;
};


class ObMemstoreAllocator
{
public:
  DEFINE_CUSTOM_FUNC_FOR_THROTTLE(Memstore);

  typedef ObSpinLock Lock;
  typedef ObSpinLockGuard LockGuard;
  typedef ObMemstoreAllocator GAlloc;
  typedef ObFifoArena Arena;
  typedef ObHandleList HandleList;
  typedef HandleList::Handle ListHandle;
  typedef Arena::Handle ArenaHandle;

  class AllocHandle: public ListHandle, public ObIAllocator
  {
  public:
    memtable::ObMemtable& mt_;
    GAlloc* host_;
    ArenaHandle arena_handle_;
    AllocHandle(memtable::ObMemtable& mt): mt_(mt), host_(NULL) {
      do_reset();
    }
    void do_reset() {
      ListHandle::reset();
      arena_handle_.reset();
      host_ = NULL;
    }
    int64_t get_group_id() const { return id_ < 0? INT64_MAX: (id_ % Arena::MAX_CACHED_GROUP_COUNT); }
    int init();
    void set_host(GAlloc* host) { host_ = host; }
    void destroy() {
      if (NULL != host_) {
        host_->destroy_handle(*this);
        host_ = NULL;
      }
    }
    int64_t get_protection_clock() const { return get_clock(); }
    int64_t get_retire_clock() const
    {
      int64_t retire_clock = INT64_MAX;
      if (NULL != host_) {
        retire_clock = host_->get_retire_clock();
      }
      return retire_clock;
    }
    int64_t get_size() const { return arena_handle_.get_allocated(); }
    int64_t get_occupied_size() const { return get_size(); }
    void* alloc(const int64_t size) {
      return NULL == host_? NULL: host_->alloc(*this, size);
    }
    void* alloc(const int64_t size, const ObMemAttr &attr)
    {
      UNUSEDx(attr);
      return alloc(size);
    }
    void free(void* ptr) {
      UNUSED(ptr);
    }
    void set_frozen() {
      if (NULL != host_) {
        host_->set_frozen(*this);
      }
    }
    INHERIT_TO_STRING_KV("ListHandle", ListHandle, KP_(host), K_(arena_handle));
  };

public:
  ObMemstoreAllocator()
      : throttle_tool_(nullptr), lock_(common::ObLatchIds::MEMSTORE_ALLOCATOR_LOCK), hlist_(), arena_() {}
  ~ObMemstoreAllocator() {}
public:
  int init();
  int start() { return OB_SUCCESS; }
  void stop() {}
  void wait() {}
  void destroy() {}
  void init_handle(AllocHandle& handle);
  void destroy_handle(AllocHandle& handle);
  void* alloc(AllocHandle& handle, int64_t size, const int64_t expire_ts = 0);
  void set_frozen(AllocHandle& handle);
  template<typename Func>
  int for_each(Func& f, const bool reverse=false) {
    int ret = common::OB_SUCCESS;
    ObDLink* iter = NULL;
    LockGuard guard(lock_);
    while(OB_SUCC(ret) && NULL != (iter = (reverse ? hlist_.prev(iter) : hlist_.next(iter)))) {
      ret = f(iter);
    }
    return ret;
  }
public:
  int64_t get_active_memstore_used() {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: (arena_.allocated() - hazard);
  }
  int64_t get_freezable_active_memstore_used() {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: (arena_.retired() - hazard);
  }
  int64_t get_max_cached_memstore_size() const {
    return arena_.get_max_cached_memstore_size();
  }
  int64_t hold() const { return arena_.hold(); }
  int64_t get_total_memstore_used() const { return arena_.hold(); }
  int64_t get_frozen_memstore_pos() const {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: hazard;
  }
  int64_t get_memstore_reclaimed_pos() const { return arena_.reclaimed(); }
  int64_t get_memstore_allocated_pos() const { return arena_.allocated(); }
  int64_t get_retire_clock() const { return arena_.retired(); }
  void log_frozen_memstore_info(char* buf, int64_t limit) {
    if (NULL != buf && limit > 0) {
      FrozenMemstoreInfoLogger logger(buf, limit);
      buf[0] = 0;
      (void)for_each(logger, true /* reverse  */);
    }
  }
  void log_active_memstore_info(char *buf, int64_t limit) {
    if (NULL != buf && limit > 0) {
      ActiveMemstoreInfoLogger logger(buf, limit);
      buf[0] = 0;
      (void)for_each(logger, true /* reverse */);
    }
  }

public:
  int set_memstore_threshold();

private:
  int64_t nway_per_group();
  int set_memstore_threshold_without_lock();
private:
  share::TxShareThrottleTool *throttle_tool_;
  Lock lock_;
  HandleList hlist_;
  Arena arena_;
};

};     // namespace share
};     // namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_ */
