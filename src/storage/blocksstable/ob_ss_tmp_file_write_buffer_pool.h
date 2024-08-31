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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SS_TMP_WRITE_BUFFER_POOL_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SS_TMP_WRITE_BUFFER_POOL_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_array.h"
#include "lib/queue/ob_link.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace blocksstable
{

class ObITmpFile;
class ObSSTmpWriteBufferPool;

struct ObPageEntry final
{
public:
  ObPageEntry() : fd_(-1), next_page_id_(UINT32_MAX), buf_(nullptr) {}
  ObPageEntry(const int64_t fd, const uint32_t next_page_id, char *buf)
      : fd_(fd), next_page_id_(next_page_id), buf_(buf) {}
  TO_STRING_KV(K_(fd), K_(next_page_id), KP_(buf));

public:
  int64_t fd_;
  uint32_t next_page_id_;
  char *buf_;
};

class ObSSTmpWriteBufferPool final
{
public:
  static const int64_t PAGE_SIZE = 8 * 1024;                               // page size: 8KB
  static const int64_t BLOCK_SIZE = 2 * 1024 * 1024 - 24 * 1024;           // block size: 2MB - 24KB (header)
  static const int64_t BLOCK_PAGE_NUMS = BLOCK_SIZE / PAGE_SIZE;           // 253 pages per block (24KB header)
  static const int64_t INITIAL_POOL_SIZE = BLOCK_SIZE;
  static const int64_t INITIAL_PAGE_NUMS = INITIAL_POOL_SIZE / PAGE_SIZE;
  static const uint32_t INVALID_PAGE_ID = UINT32_MAX;

public:
  ObSSTmpWriteBufferPool();
  ~ObSSTmpWriteBufferPool();
  int init();
  void destroy();
  int fetch_page(const int64_t fd, const uint32_t page_id, char *&buf, uint32_t &next_page_id);
  int alloc_page(const int64_t fd, uint32_t &new_page_id, char *&buf);
  int link_page(const int64_t fd, const uint32_t page_id, const uint32_t last_page_id);
  int free_page(const uint32_t page_id);

public:
  OB_INLINE bool is_reach_high_watermark() const
  {
    int64_t high_water_level = ATOMIC_LOAD(&capacity_) * 0.8;
    return ATOMIC_LOAD(&used_pages_) * PAGE_SIZE >= high_water_level;
  }
  OB_INLINE int64_t get_wash_expect_size() const
  {
    int64_t expect_wash_size = ATOMIC_LOAD(&capacity_) * 0.2;
    return expect_wash_size;
  }
  void get_watermark_status(int64_t & watermark, int64_t & currmark) const;

private:
  int inner_alloc_page(const int64_t fd, uint32_t &new_page_id, char *&buf);
  OB_INLINE bool is_valid_page_id(const uint32_t page_id) const
  {
    return page_id >= 0 && page_id < fat_.count() && OB_NOT_NULL(fat_[page_id].buf_);
  }
  int64_t get_memory_limit();
  int wbp_status();
  int expand();
  int reduce();

private:
  struct StatusUpdateOp
  {
    void operator()(common::hash::HashMapPair<int64_t, int64_t> &pair)
    {
      pair.second++;
    }
  };
  struct StatusPrintOp
  {
    int operator()(common::hash::HashMapTypes<int64_t, int64_t>::pair_type &pair)
    {
      STORAGE_LOG(INFO, "print write buffer pool status <fd, page_counts>", K(pair.first), K(pair.second));
      return OB_SUCCESS;
    }
  };

private:
  common::ObArray<ObPageEntry> fat_;
  common::SpinRWLock lock_;
  common::ObFIFOAllocator allocator_;
  int64_t capacity_;    // in bytes
  int64_t used_pages_;  // in pages
  uint32_t first_free_page_id_;
  int64_t temporary_file_memory_limit_;  // in bytes
  int64_t last_access_tenant_config_ts_;
  bool is_inited_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif