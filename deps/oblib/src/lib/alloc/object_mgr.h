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

#ifndef _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_
#define _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_

#include "lib/allocator/ob_ctx_define.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/random/ob_random.h"
#include "lib/ob_abort.h"
#include "lib/ob_define.h"
#include "lib/alloc/alloc_interface.h"
#ifndef ENABLE_SANITY
#include "lib/lock/ob_latch.h"
#else
#include "lib/alloc/ob_latch_v2.h"
#endif
#include "object_set.h"

namespace oceanbase
{
namespace lib
{
// object_set needs to be lightweight, and some large or logically optional members need to be stripped out
// SubObjectMgr is a combination of object_set and attributes stripped from object_set, such as block_set, mutex, etc.
class SubObjectMgr : public IBlockMgr
{
  friend class ObTenantCtxAllocator;
public:
  SubObjectMgr(ObTenantCtxAllocator &ta,
               const bool enable_no_log,
               const uint32_t ablock_size,
               const bool enable_dirty_list,
               IBlockMgr *blk_mgr);
  virtual ~SubObjectMgr() {}
  OB_INLINE void lock() { locker_.lock(); }
  OB_INLINE void unlock() { locker_.unlock(); }
  OB_INLINE bool trylock() { return locker_.trylock(); }
  OB_INLINE AObject *alloc_object(uint64_t size, const ObMemAttr &attr)
  {
    return os_.alloc_object(size, attr);
  }
  OB_INLINE AObject *realloc_object(AObject *obj,  const uint64_t size, const ObMemAttr &attr)
  {
    return os_.realloc_object(obj, size, attr);
  }
  void free_object(AObject *object);
  OB_INLINE ABlock *alloc_block(uint64_t size, const ObMemAttr &attr) override
  {
    return bs_.alloc_block(size, attr);
  }
  void free_block(ABlock *block) override;
  int64_t sync_wash(int64_t wash_size) override;
  OB_INLINE int64_t get_hold() { return bs_.get_total_hold(); }
  OB_INLINE int64_t get_payload() { return bs_.get_total_payload(); }
  OB_INLINE int64_t get_used() { return bs_.get_total_used(); }
  OB_INLINE bool check_has_unfree()
  {
    return bs_.check_has_unfree();
  }
  OB_INLINE bool check_has_unfree(char *first_label, char *first_bt)
  {
    return os_.check_has_unfree(first_label, first_bt);
  }
private:
  ObTenantCtxAllocator &ta_;
#ifndef ENABLE_SANITY
  lib::ObMutex mutex_;
#else
  lib::ObMutexV2 mutex_;
#endif
  SetLocker<decltype(mutex_)> normal_locker_;
  SetLockerNoLog<decltype(mutex_)> no_log_locker_;
  ISetLocker &locker_;
  BlockSet bs_;
  ObjectSet os_;
};

class ObjectMgr final : public IBlockMgr
{
  static const int N = 32;
  friend class SubObjectMgr;
public:
  struct Stat
  {
    int64_t hold_;
    int64_t payload_;
    int64_t used_;
    int64_t last_washed_size_;
    int64_t last_wash_ts_;
  };
public:
  ObjectMgr(ObTenantCtxAllocator &ta,
            bool enable_no_log,
            uint32_t ablock_size,
            int parallel,
            bool enable_dirty_list,
            IBlockMgr *blk_mgr);
  ~ObjectMgr();
  void reset();

  AObject *alloc_object(uint64_t size, const ObMemAttr &attr);
  AObject *realloc_object(
      AObject *obj, const uint64_t size, const ObMemAttr &attr);
  void free_object(AObject *obj);

  ABlock *alloc_block(uint64_t size, const ObMemAttr &attr) override;
  void free_block(ABlock *block) override;

  void print_usage() const;
  int64_t sync_wash(int64_t wash_size) override;
  Stat get_stat();
  bool check_has_unfree();
  bool check_has_unfree(char *first_label, char *first_bt);
private:
  SubObjectMgr *create_sub_mgr();
  void destroy_sub_mgr(SubObjectMgr *sub_mgr);

public:
  ObTenantCtxAllocator &ta_;
  bool enable_no_log_;
  uint32_t ablock_size_;
  int parallel_;
  bool enable_dirty_list_;
  IBlockMgr *blk_mgr_;
  int sub_cnt_;
  SubObjectMgr root_mgr_;
  SubObjectMgr *sub_mgrs_[N];
  int64_t last_wash_ts_;
  int64_t last_washed_size_;
}; // end of class ObjectMgr

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_ */
