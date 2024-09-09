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

#define USING_LOG_PREFIX STORAGE

#include "share/config/ob_server_config.h"
#include "storage/blocksstable/ob_ss_tmp_file_write_buffer_pool.h"
#include "storage/blocksstable/ob_ss_tmp_file_manager.h"

namespace oceanbase
{
namespace blocksstable
{

ObSSTmpWriteBufferPool::ObSSTmpWriteBufferPool()
    : fat_(),
      lock_(),
      allocator_(),
      capacity_(0),
      used_pages_(0),
      first_free_page_id_(INVALID_PAGE_ID),
      temporary_file_memory_limit_(-1),
      last_access_tenant_config_ts_(-1),
      is_inited_(false)
{
}

ObSSTmpWriteBufferPool::~ObSSTmpWriteBufferPool()
{
  is_inited_ = false;
  destroy();
}

int ObSSTmpWriteBufferPool::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init wbp, init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(allocator_.init(
                     lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE,
                     ObMemAttr(MTL_ID(), "SSTmpFileWBP", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("wbp fail to init fifo allocator", K(ret));
  } else if (OB_FAIL(expand())) {
    LOG_WARN("wbp fail to expand capacity", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSTmpWriteBufferPool::destroy()
{
  int ret = OB_SUCCESS;
  reduce();
  fat_.destroy();
  allocator_.reset();
}

int ObSSTmpWriteBufferPool::fetch_page(const int64_t fd, const uint32_t page_id,
                                     char *&buf, uint32_t &next_page_id)
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  next_page_id = INVALID_PAGE_ID;
  SpinRLockGuard guard(lock_);
  // defensive check
  if (!is_valid_page_id(page_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wbp fail to fetch page, invalid page id", K(ret), K(page_id), K(fd));
  } else if (fd != fat_[page_id].fd_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wbp fail to fetch page, PageEntry fd not equal", K(ret), K(page_id), K(fd), K(fat_[page_id]));
  } else {
    buf = fat_[page_id].buf_;
    next_page_id = fat_[page_id].next_page_id_;
  }
  return ret;
}

int ObSSTmpWriteBufferPool::inner_alloc_page(const int64_t fd,
                                           uint32_t &new_page_id,
                                           char *&new_page_buf)
{
  int ret = OB_SUCCESS;
  uint32_t curr_first_free_page_id = INVALID_PAGE_ID;
  uint32_t next_first_free_page_id = INVALID_PAGE_ID;

  SpinRLockGuard guard(lock_);

  // Remove a page from the free list through CAS operation.
  bool cas_succeed = false;
  do {
    curr_first_free_page_id = ATOMIC_LOAD(&first_free_page_id_);
    if (!is_valid_page_id(curr_first_free_page_id)) {
      ret = OB_SEARCH_NOT_FOUND;
      break;
    }
    next_first_free_page_id = fat_[curr_first_free_page_id].next_page_id_;
    cas_succeed = ATOMIC_BCAS(&first_free_page_id_, curr_first_free_page_id, next_first_free_page_id);
  } while (OB_SUCC(ret) && !cas_succeed);

  // Fill in meta info into PageEntry if remove page succeed.
  if (OB_SUCC(ret) && is_valid_page_id(curr_first_free_page_id)) {
    fat_[curr_first_free_page_id].fd_ = fd;
    fat_[curr_first_free_page_id].next_page_id_ = INVALID_PAGE_ID;
    new_page_id = curr_first_free_page_id;
    new_page_buf = fat_[new_page_id].buf_;
    ATOMIC_INC(&used_pages_);
  }

  if (new_page_id == INVALID_PAGE_ID) {
    ret = OB_SEARCH_NOT_FOUND;
  }

  return ret;
}

int ObSSTmpWriteBufferPool::alloc_page(const int64_t fd,
                                     uint32_t &new_page_id,
                                     char *&new_page_buf)
{
  int ret = OB_SUCCESS;
  const int LOOP_MAX_COUNT = 3;

  new_page_id = INVALID_PAGE_ID;
  new_page_buf = nullptr;

  // Validate input argument.
  if (fd == ObSSTenantTmpFileManager::INVALID_TMP_FILE_FD) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wbp fail to alloc_page, invalid fd", K(ret), K(fd));
  }

  int64_t memory_limit = 0;
  int64_t current_capacity = -1;
  // Continuously trying to allocate page and expand pool size, until capacity reach maximum memory limit.
  while (OB_SUCC(ret) && new_page_id == INVALID_PAGE_ID && current_capacity < memory_limit) {
    if (OB_FAIL(inner_alloc_page(fd, new_page_id, new_page_buf)) &&
        ret != OB_SEARCH_NOT_FOUND) {
      LOG_WARN("wbp fail to inner alloc page", K(ret), K(fd), K(new_page_id), KP(new_page_buf));
    } else if (ret == OB_SEARCH_NOT_FOUND) {
      ret = OB_SUCCESS;
      memory_limit = get_memory_limit();
      current_capacity = ATOMIC_LOAD(&capacity_);
      // Fetch and set first free page failed, may try to expand memory pool.
      if (current_capacity < memory_limit && OB_FAIL(expand())) {
        LOG_WARN("wbp fail to expand", K(ret), K(fd), K(ATOMIC_LOAD(&capacity_)));
      }
    }
  }

  if (OB_SUCC(ret) && new_page_id == INVALID_PAGE_ID) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (REACH_COUNT_INTERVAL(10000)) {
      wbp_status();
      LOG_INFO("wbp cannot alloc page", K(fd), K(memory_limit), K(current_capacity));
    }
  }

  return ret;
}

int ObSSTmpWriteBufferPool::link_page(const int64_t fd,
                                    const uint32_t page_id,
                                    const uint32_t last_page_id)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);

  // Validate argument.
  if (!is_valid_page_id(page_id) || !is_valid_page_id(last_page_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to link page, invalid page id", K(ret), K(page_id), K(last_page_id), K(fd));
  } else if (fat_[page_id].fd_ != fd || fat_[last_page_id].fd_ != fd ||
             fat_[last_page_id].next_page_id_ != INVALID_PAGE_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to link page, unexpected page id", K(ret), K(fd),
              K(page_id), K(fat_[page_id]), K(last_page_id),
              K(fat_[last_page_id]));
    // TODO(baichangmin): remove this abort.
    ob_abort();
  } else {
    // Link page.
    fat_[last_page_id].next_page_id_ = page_id;
  }

  return ret;
}

int ObSSTmpWriteBufferPool::free_page(const uint32_t page_id)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  // defensive check
  if (!is_valid_page_id(page_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wbp fail to free page, invalid page id", K(ret), K(page_id));
  } else {
    // reset PageEntry info and add this page to free list head
    ATOMIC_SET(&(fat_[page_id].fd_), -1);
    ATOMIC_SET(&(fat_[page_id].next_page_id_), INVALID_PAGE_ID);
    bool cas_succeed = false;
    do {
      uint32_t first_free_page_id_before = ATOMIC_LOAD(&first_free_page_id_);
      ATOMIC_SET(&(fat_[page_id].next_page_id_), first_free_page_id_before);
      cas_succeed = ATOMIC_BCAS(&first_free_page_id_, first_free_page_id_before, page_id);
    } while (false == cas_succeed);
    ATOMIC_DEC(&used_pages_);
  }
  return ret;
}

int ObSSTmpWriteBufferPool::expand()
{
  int ret = OB_SUCCESS;

  // The expand size should be should be equal to the current capacity, with a
  // minimum of 2MB and not exceeding the memory limit.
  const int64_t memory_limit = get_memory_limit();
  int64_t current_capacity = ATOMIC_LOAD(&capacity_);
  const int64_t expect_capacity = std::min(
      memory_limit, std::max(current_capacity * 2, int64_t(BLOCK_SIZE)));

  // Continuously allocate 2MB blocks and add them into the buffer pool.
  while (OB_SUCC(ret) && current_capacity < expect_capacity) {
    SpinWLockGuard guard(lock_);
    current_capacity = ATOMIC_LOAD(&capacity_);
    if (current_capacity < expect_capacity) {
      char * new_expand_buf = nullptr;
      // allocate `BLOCK_SIZE` memory each iteration.
      if (OB_ISNULL(new_expand_buf = static_cast<char *>(allocator_.alloc(BLOCK_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("wbp fail to allocate new expand buffer", K(ret));
      } else {
        // split by `PAGE_SIZE` and push back to `fat_`
        uint32_t new_page_id = fat_.count();
        for (uint32_t count = 0; OB_SUCC(ret) && count < BLOCK_PAGE_NUMS; ++new_page_id, ++count) {
          if (OB_FAIL(fat_.push_back(ObPageEntry(
                  -1, INVALID_PAGE_ID, new_expand_buf + count * PAGE_SIZE)))) {
            LOG_WARN("wbp fail to push back page into fat", K(ret), K(count), K(new_page_id));
          } else {
            fat_[new_page_id].next_page_id_ = ATOMIC_LOAD(&first_free_page_id_);
            ATOMIC_SET(&first_free_page_id_, new_page_id);
            ATOMIC_FAA(&capacity_, PAGE_SIZE);
          }
        }
        current_capacity += BLOCK_SIZE;
      }
    } else {
      // maybe another thread has finish allocation, do nothing.
    }
  }

  LOG_INFO("wbp expand", K(expect_capacity), K(memory_limit), K(ATOMIC_LOAD(&capacity_)));

  return ret;
}

int ObSSTmpWriteBufferPool::reduce()
{
  int ret = OB_SUCCESS;
  // TODO(baichangmin): now, reduce just simply release all memory wbp held.
  SpinWLockGuard guard(lock_);
  for (int64_t i = 0; i < fat_.count(); i += BLOCK_PAGE_NUMS) {
    char * buf = fat_.at(i).buf_;
    allocator_.free(buf);
  }
  return ret;
}

int ObSSTmpWriteBufferPool::wbp_status()
{
  int ret = OB_SUCCESS;

  common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> hash_map;
  if (OB_FAIL(hash_map.create(1024, "WBPStat", "WBPStat", MTL_ID()))) {
    LOG_WARN("wbp fail to create hash map", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    StatusUpdateOp update_op;
    for (int i = 0; OB_SUCC(ret) && i < fat_.count(); ++i) {
      int64_t fd = fat_.at(i).fd_;
      LOG_INFO("wbp status", K(i), K(fat_[i]));
      if (OB_FAIL(hash_map.set_or_update(fd, 1, update_op))) {
        LOG_WARN("wbp fail to set or update hash map", K(ret), K(fd));
      }
    }
    int tmp_ret = OB_SUCCESS;
    StatusPrintOp print_op;
    if (OB_TMP_FAIL(hash_map.foreach_refactored(print_op))) {
      LOG_WARN("wbp fail to foreach hash map", K(tmp_ret), K(hash_map.size()));
    }
    LOG_INFO("wbp status", K(ret), K(tmp_ret), K(fat_.count()),
             K(ATOMIC_LOAD(&capacity_)), K(used_pages_),
             K(ATOMIC_LOAD(&first_free_page_id_)),
             K(temporary_file_memory_limit_), K(last_access_tenant_config_ts_));
  }

  return ret;
}

int64_t ObSSTmpWriteBufferPool::get_memory_limit()
{
  int64_t memory_limit = -1;
  int64_t last_access_ts = ATOMIC_LOAD(&last_access_tenant_config_ts_);
  if (last_access_ts > 0 && common::ObClockGenerator::getClock() - last_access_ts < 10000000) {
    memory_limit = ATOMIC_LOAD(&temporary_file_memory_limit_);
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (!tenant_config.is_valid()) {
      LOG_INFO("failed to get tenant config", K(MTL_ID()));
    } else if (0 == tenant_config->_temporary_file_io_area_size) {
      memory_limit = 1L;
    } else {
      memory_limit = common::lower_align(
          lib::get_tenant_memory_limit(MTL_ID()) * tenant_config->_temporary_file_io_area_size / 100,
          BLOCK_SIZE);
    }
    ATOMIC_STORE(&temporary_file_memory_limit_, memory_limit);
    ATOMIC_STORE(&last_access_tenant_config_ts_, common::ObClockGenerator::getClock());
  }
  return memory_limit;
}

void ObSSTmpWriteBufferPool::get_watermark_status(int64_t & watermark, int64_t & currmark) const
{
  watermark = ATOMIC_LOAD(&capacity_) * 0.8;
  currmark = ATOMIC_LOAD(&used_pages_) * PAGE_SIZE;
}

}  // end namespace blocksstable
}  // end namespace oceanbase