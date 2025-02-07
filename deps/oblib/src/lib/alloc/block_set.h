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

#ifndef _OCEABASE_LIB_ALLOC_BLOCK_SET_H_
#define _OCEABASE_LIB_ALLOC_BLOCK_SET_H_

#include "lib/ob_define.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/alloc_interface.h"
#include "lib/resource/achunk_mgr.h"

namespace oceanbase
{
namespace lib
{

class ObTenantCtxAllocator;
class ISetLocker;
class BlockSet
{
  friend class ObTenantCtxAllocator;
public:
  BlockSet();
  ~BlockSet();

  ABlock *alloc_block(const uint64_t size, const ObMemAttr &attr);
  void free_block(ABlock *block);

  inline void lock();
  inline void unlock();
  inline bool trylock();

  inline uint64_t get_total_hold() const;
  inline uint64_t get_total_payload() const;
  inline uint64_t get_total_used() const;

  void set_tenant_ctx_allocator(ObTenantCtxAllocator &allocator);
  void reset();
  void set_locker(ISetLocker *locker) { locker_ = locker; }
  int64_t sync_wash(int64_t wash_size=INT64_MAX);
  bool check_has_unfree();
  ObTenantCtxAllocator *get_tenant_ctx_allocator() const { return  tallocator_; }
  void set_chunk_mgr(IChunkMgr *chunk_mgr) { chunk_mgr_ = chunk_mgr; }

private:
  DISALLOW_COPY_AND_ASSIGN(BlockSet);

  void add_free_block(ABlock *block, int nblocks, AChunk *chunk);
  ABlock *get_free_block(const int cls, const ObMemAttr &attr);
  void take_off_free_block(ABlock *block, int nblocks, AChunk *chunk);
  AChunk *alloc_chunk(const uint64_t size, const ObMemAttr &attr);
  bool add_chunk(const ObMemAttr &attr);
  void free_chunk(AChunk *const chunk);

private:
  ObTenantCtxAllocator *tallocator_;
  ObMemAttr attr_;
  ISetLocker *locker_;
  IChunkMgr *chunk_mgr_;
  // block_list_ can not be initialized, the state is maintained by avail_bm_
  union {
    ABlock *block_list_[BLOCKS_PER_CHUNK+1];
  };
  AChunk *clist_;  // using chunk list
  ABitSet avail_bm_;
  char avail_bm_buf_[ABitSet::buf_len(BLOCKS_PER_CHUNK+1)];
  uint64_t total_hold_;
  uint64_t total_payload_;
  uint64_t total_used_;
}; // end of class BlockSet

void BlockSet::lock()
{
  ObDisableDiagnoseGuard diagnose_disable_guard;
  locker_->lock();
}

void BlockSet::unlock()
{
  ObDisableDiagnoseGuard diagnose_disable_guard;
  locker_->unlock();
}

bool BlockSet::trylock()
{
  return locker_->trylock();
}

uint64_t BlockSet::get_total_hold() const
{
  return ATOMIC_LOAD(&total_hold_);
}

uint64_t BlockSet::get_total_payload() const
{
  return ATOMIC_LOAD(&total_payload_);
}

uint64_t BlockSet::get_total_used() const
{
  return ATOMIC_LOAD(&total_used_);
}

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_BLOCK_SET_H_ */
