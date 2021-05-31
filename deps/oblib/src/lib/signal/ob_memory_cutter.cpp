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

#define USING_LOG_PREFIX LIB

#include "lib/signal/ob_memory_cutter.h"
#include "lib/signal/ob_signal_utils.h"
#include "lib/resource/achunk_mgr.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/coro/co_protected_stack_allocator.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace lib {
ObMemoryCutter g_lib_mem_cutter;
ObIMemoryCutter* g_mem_cutter = &g_lib_mem_cutter;

uint64_t ObMemoryCutter::chunk_size(AChunk* chunk)
{
  return lib::AChunkMgr::aligned(chunk->alloc_bytes_);
}

void ObMemoryCutter::free_chunk(const void* ptr, const uint64_t size)
{
  auto& mgr = AChunkMgr::instance();
  mgr.low_free(ptr, size);
}

// TODO: Change the segmented linked list to solve the problem that a single object cannot go on if it is written badly
void ObMemoryCutter::free_chunk(int64_t& total_size)
{
  auto& mgr = AChunkMgr::instance();
  auto& free_list = mgr.free_list_;
  AChunk* head = free_list.header_;
  while (head) {
    if (head->check_magic_code()) {
      AChunk* next = head->next_;
      uint64_t all_size = chunk_size(head);
      free_chunk(head, all_size);
      total_size += all_size;
      head = next;
    } else {
      DLOG(WARN, "invalid chunk magic");
      break;
    }
  }
}

// TODO: Change the segmented linked list to solve the problem that a single object cannot go on if it is written badly
void ObMemoryCutter::free_stack(int64_t& total_size)
{
  uint64_t self_pth = pthread_self();
  ObStackHeader* dummy = &g_stack_mgr.dummy_;
  ObStackHeader* cur = dummy->next_;
  while (cur != dummy) {
    auto* next = cur->next_;
    if (cur->check_magic()) {
      if (cur->pth_ != self_pth) {
        int64_t size = cur->size_;
        munmap(cur, size);
        total_size += size;
      }
    } else {
      DLOG(WARN, "invalid stack magic");
      break;
    }
    cur = next;
  }
}

void ObMemoryCutter::free_memstore(int64_t& total_size)
{
  if (ObMallocAllocator::instance_ != nullptr) {
    for (int64_t slot = 0; slot < ObMallocAllocator::PRESERVED_TENANT_COUNT; ++slot) {
      bool has_crash = false;
      do_with_crash_restore(
          [&]() {
            ObTenantCtxAllocator* ta = ObMallocAllocator::instance_->allocators_[slot][ObCtxIds::MEMSTORE_CTX_ID];
            while (ta != nullptr) {
              bool has_crash = false;
              do_with_crash_restore(
                  [&]() {
                    AChunk* cur = ta->using_list_head_.next2_;
                    while (cur != &ta->using_list_head_) {
                      if (cur->check_magic_code()) {
                        AChunk* next = cur->next2_;
                        uint64_t all_size = chunk_size(cur);
                        free_chunk(cur, all_size);
                        total_size += all_size;
                        cur = next;
                      } else {
                        DLOG(WARN, "invalid chunk magic");
                        break;
                      }
                    }
                  },
                  has_crash);
              if (has_crash) {
                DLOG(WARN, "restore from crash, let's goon~");
              }
              ta = ta->get_next();
            }
          },
          has_crash);
      if (has_crash) {
        DLOG(WARN, "restore from crash, let's goon~");
      }
    }
  }
}

void ObMemoryCutter::cut(int64_t& total_size)
{
  bool has_crash = false;

  int64_t total_size_bak = total_size;
  do_with_crash_restore([&]() { free_chunk(total_size); }, has_crash);
  if (has_crash) {
    DLOG(WARN, "restore from crash, let's goon~");
  } else {
    DLOG(INFO, "free_chunk, freed: %ld", total_size - total_size_bak);
  }

  total_size_bak = total_size;
  do_with_crash_restore([&]() { free_stack(total_size); }, has_crash);
  if (has_crash) {
    DLOG(WARN, "restore from crash, let's goon~");
  } else {
    DLOG(INFO, "free_stack, freed: %ld", total_size - total_size_bak);
  }

  total_size_bak = total_size;
  do_with_crash_restore([&]() { free_memstore(total_size); }, has_crash);
  if (has_crash) {
    DLOG(WARN, "restore from crash, let's goon~");
  } else {
    DLOG(INFO, "free_memstore, freed: %ld", total_size - total_size_bak);
  }
}

}  // namespace lib
}  // namespace oceanbase
