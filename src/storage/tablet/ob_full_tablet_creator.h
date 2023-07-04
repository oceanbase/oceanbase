/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_FULL_TABLET_CREATOR
#define OCEANBASE_STORAGE_OB_FULL_TABLET_CREATOR

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/list/ob_dlist.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{

class ObFullTabletCreator final
{
  static const int64_t ONE_ROUND_PERSIST_COUNT_THRESHOLD = 200L;
  static const int64_t FIFO_START_OFFSET =
      sizeof(ObFIFOAllocator::NormalPageHeader) + sizeof(ObFIFOAllocator::AllocHeader) + 16 - 1;
public:
  ObFullTabletCreator();
  ~ObFullTabletCreator() = default;
public:
  int init(const uint64_t tenant_id);
  void reset();
  int create_tablet(ObTabletHandle &tablet_handle);
  int persist_tablet();
  void destroy_queue(); // used to release tablets when t3m::destroy
  common::ObIAllocator &get_allocator() { return mstx_allocator_; }
    /* ATTENTION: below functions should be called without any ls_tablet or t3m locks */
  int throttle_tablet_creation();
  int push_tablet_to_queue(const ObTabletHandle &tablet_handle);
  int remove_tablet_from_queue(const ObTabletHandle &tablet_handle);
  void free_tablet(ObTablet *tablet);
  OB_INLINE int64_t total() const { return tiny_allocator_.total() + mstx_allocator_.total(); }
  OB_INLINE int64_t used() const { return tiny_allocator_.used() + mstx_allocator_.used(); }
  OB_INLINE int64_t get_used_obj_cnt() const { return ATOMIC_LOAD(&created_tablets_cnt_); }
  TO_STRING_KV(K(mstx_allocator_.used()), K(mstx_allocator_.total()),
               K(tiny_allocator_.used()), K(tiny_allocator_.total()),
               "full allocator total", total());
private:
  int pop_tablet(ObTabletHandle &tablet_handle);
private:
  bool is_inited_;
  common::ObFIFOAllocator mstx_allocator_;
  common::ObFIFOAllocator tiny_allocator_;
  ObTabletHandle transform_head_; // for transform thread
  ObTabletHandle transform_tail_; // for transform thread
  int64_t wait_create_tablets_cnt_; // tablets waiting to be created
  int64_t created_tablets_cnt_; // tablets has been created
  int64_t persist_queue_cnt_; // tablets in persist queue
  lib::ObMutex mutex_;
  DISALLOW_COPY_AND_ASSIGN(ObFullTabletCreator);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_FULL_TABLET_CREATOR
