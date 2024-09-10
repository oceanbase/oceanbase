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
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/blocksstable/ob_block_manager.h"

namespace oceanbase
{
namespace tmp_file
{

int ObPageEntry::switch_state(const int64_t op)
{
  int ret = OB_SUCCESS;
  static const int64_t N = State::N;
  static const int64_t INV = State::INVALID;
  static const int64_t INITED = State::INITED;
  static const int64_t LOADING = State::LOADING;
  static const int64_t CACHED = State::CACHED;
  static const int64_t DIRTY = State::DIRTY;
  static const int64_t WRITE_BACK = State::WRITE_BACK;
  static const int64_t MAX = State::MAX;

  static const int64_t STATE_MAP[State::MAX][Ops::MAX] = {
  // ALLOC,     LOAD,      LOAD_FAIL,   LOAD_SUCC,  DELETE,   WRITE,    WRITE_BACK,   WRITE_BACK_FAILED,   WRITE_BACK_SUCC
    {INITED,    N,         N,           N,          INV,      N,        N,            N,                   N},          //INVALID
    {N,         LOADING,   N,           N,          INV,      DIRTY,    N,            N,                   N},          //INITED
    {N,         N,         INITED,      CACHED,     N,        N,        N,            N,                   N},          //LOADING
    {N,         N,         N,           N,          INV,      DIRTY,    N,            N,                   CACHED},     //CACHED
    {N,         N,         N,           N,          INV,      DIRTY,    WRITE_BACK,   N,                   N},          //DIRTY
    {N,         N,         N,           N,          INV,      DIRTY,    WRITE_BACK,   DIRTY,               CACHED}      //WRITE_BACK
  };

  if (OB_UNLIKELY(!Ops::is_valid(op))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid operation", KR(ret), K(op));
  } else if (OB_UNLIKELY(!State::is_valid(state_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid state", KR(ret), K(state_));
  } else {
    const int64_t n_stat = STATE_MAP[state_][op];
    if (OB_UNLIKELY(!State::is_valid(n_stat))) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("invalid state transition", KR(ret), K(state_), K(op), K(n_stat));
    } else {
      state_ = n_stat;
    }
  }
  return ret;
}

double ObTmpWriteBufferPool::MAX_DATA_PAGE_USAGE_RATIO = 0.9;

ObTmpWriteBufferPool::ObTmpWriteBufferPool()
    : fat_(),
      lock_(),
      allocator_(),
      is_inited_(false),
      capacity_(0),
      dirty_page_num_(0),
      used_page_num_(0),
      first_free_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      wbp_memory_limit_(-1),
      default_wbp_memory_limit_(-1),
      last_access_tenant_config_ts_(-1),
      meta_page_cnt_(0),
      data_page_cnt_(0),
      dirty_meta_page_cnt_(0),
      dirty_data_page_cnt_(0),
      write_back_data_cnt_(0),
      write_back_meta_cnt_(0)
{
}

ObTmpWriteBufferPool::~ObTmpWriteBufferPool()
{
  destroy();
}

int ObTmpWriteBufferPool::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init wbp, init twice", KR(ret), K(is_inited_));
  } else if (OB_FAIL(allocator_.init(
                     lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE,
                     ObMemAttr(MTL_ID(), "TmpFileWBP", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("wbp fail to init fifo allocator", KR(ret));
  } else if (FALSE_IT(fat_.set_attr(ObMemAttr(MTL_ID(), "TmpFileWBP")))) {
  } else if (OB_FAIL(expand_())) {
    LOG_WARN("wbp fail to expand capacity", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTmpWriteBufferPool::destroy()
{
  reduce_();
  capacity_ = 0;
  dirty_page_num_ = 0;
  used_page_num_ = 0;
  first_free_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  last_access_tenant_config_ts_ = -1;
  data_page_cnt_ = 0;
  meta_page_cnt_ = 0;
  dirty_meta_page_cnt_ = 0;
  dirty_data_page_cnt_ = 0;
  write_back_data_cnt_ = 0;
  write_back_meta_cnt_ = 0;
  wbp_memory_limit_ = -1;
  default_wbp_memory_limit_ = -1;
  fat_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

// limit data pages to use a maximum of 90% of the total space in the write buffer pool;
// considering that the total amount of meta pages for a single file accounts for less than 1% of data pages,
// there is no limit set for meta pages when allocating pages.
int ObTmpWriteBufferPool::inner_alloc_page_(const int64_t fd,
                                            const ObTmpFilePageUniqKey page_key,
                                            uint32_t &new_page_id,
                                            char *&new_page_buf)
{
  int ret = OB_SUCCESS;
  uint32_t curr_first_free_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t next_first_free_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  common::TCRWLock::RLockGuard guard(lock_);
  // limit page allocation depending on page type
  if (has_free_page_(page_key.type_)) {
    // fetch a page from the free list through CAS operation
    bool cas_succeed = false;
    do {
      curr_first_free_page_id = ATOMIC_LOAD(&first_free_page_id_);
      if (!is_valid_page_id_(curr_first_free_page_id)) {
        ret = OB_SEARCH_NOT_FOUND;
        break;
      }
      next_first_free_page_id = fat_[curr_first_free_page_id].next_page_id_;
      cas_succeed = ATOMIC_BCAS(&first_free_page_id_, curr_first_free_page_id, next_first_free_page_id);
    } while (OB_SUCC(ret) && !cas_succeed);

    if (OB_SUCC(ret) && is_valid_page_id_(curr_first_free_page_id)) {
      fat_[curr_first_free_page_id].fd_ = fd;
      fat_[curr_first_free_page_id].next_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      fat_[curr_first_free_page_id].page_key_ = page_key;
      fat_[curr_first_free_page_id].switch_state(ObPageEntry::Ops::ALLOC);
      new_page_id = curr_first_free_page_id;
      new_page_buf = fat_[new_page_id].buf_;
      ATOMIC_INC(&used_page_num_);
    }
  }

  if (ObTmpFileGlobal::INVALID_PAGE_ID == new_page_id) {
    ret = OB_SEARCH_NOT_FOUND;
  }

  return ret;
}

int ObTmpWriteBufferPool::alloc_page_(const int64_t fd,
                                      const ObTmpFilePageUniqKey page_key,
                                      uint32_t &new_page_id,
                                      char *&new_page_buf)
{
  int ret = OB_SUCCESS;

  new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  new_page_buf = nullptr;

  // validate input argument.
  if (ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wbp fail to alloc_page, invalid fd", KR(ret), K(fd));
  }

  int64_t memory_limit = 0;
  int64_t current_capacity = -1;
  // continuously trying to allocate page and expand pool size, until capacity reach maximum memory limit.
  while (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID == new_page_id && current_capacity < memory_limit) {
    if (OB_FAIL(inner_alloc_page_(fd, page_key, new_page_id, new_page_buf))) {
      if (OB_SEARCH_NOT_FOUND != ret) {
        LOG_WARN("wbp fail to inner alloc page", KR(ret), K(fd), K(page_key), K(new_page_id), KP(new_page_buf));
      } else {  // no free pages, try to expand pool size
        ret = OB_SUCCESS;
        memory_limit = get_memory_limit();
        current_capacity = ATOMIC_LOAD(&capacity_);
        if (current_capacity < memory_limit && OB_FAIL(expand_())) {
          LOG_WARN("wbp fail to expand", KR(ret), K(fd), K(ATOMIC_LOAD(&capacity_)));
        }
      }
    }
  }

  if (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID == new_page_id) {
    ret = OB_ALLOCATE_TMP_FILE_PAGE_FAILED;  // reaches maximum memory limit, can not allocate page
  }

  return ret;
}

int ObTmpWriteBufferPool::alloc_page(const int64_t fd,
                                     const ObTmpFilePageUniqKey page_key,
                                     uint32_t &new_page_id,
                                     char *&new_page_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd || !page_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_key));
  } else if (OB_FAIL(alloc_page_(fd, page_key, new_page_id, new_page_buf))) {
    LOG_WARN("wbp fail to alloc page", KR(ret), K(fd), K(page_key));
  } else if (page_key.type_ == PageEntryType::META) {
    ATOMIC_INC(&meta_page_cnt_);
    LOG_INFO("alloc meta page", KR(ret), K(new_page_id), K(fd));
  } else {
    ATOMIC_INC(&data_page_cnt_);
    LOG_DEBUG("alloc data page", KR(ret), K(new_page_id), K(fd));
  }
  return ret;
}

int ObTmpWriteBufferPool::get_next_page_id(
    const int64_t fd,
    const uint32_t page_id,
    const ObTmpFilePageUniqKey page_key,
    uint32_t &next_page_id)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                  !is_valid_page_id_(page_id) || !page_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key));
  } else if (OB_UNLIKELY(fd != fat_[page_id].fd_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("fd not match", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  } else if (OB_UNLIKELY(page_key != fat_[page_id].page_key_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("page key not match", KR(ret), K(page_key), K(page_id), K(fat_[page_id]));
  } else {
    next_page_id = ATOMIC_LOAD(&fat_[page_id].next_page_id_);
  }
  return ret;
}

int ObTmpWriteBufferPool::read_page(
    const int64_t fd,
    const uint32_t page_id,
    const ObTmpFilePageUniqKey page_key,
    char *&buf,
    uint32_t &next_page_id)
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  common::TCRWLock::RLockGuard guard(lock_);

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                  !is_valid_page_id_(page_id) ||
                  !page_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wbp fail to read page, invalid page id", KR(ret), K(page_id), K(fd), K(page_key));
  } else if (OB_UNLIKELY(fd != fat_[page_id].fd_ || page_key != fat_[page_id].page_key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wbp fail to fetch page, PageEntry fd or offset not equal", KR(ret), K(page_id), K(fd),
                                                              K(page_key), K(fat_[page_id]));
  } else {
    buf = fat_[page_id].buf_;
    next_page_id = fat_[page_id].next_page_id_;
  }
  return ret;
}

int ObTmpWriteBufferPool::get_page_id_by_virtual_id(const int64_t fd,
                                                    const int64_t virtual_page_id,
                                                    const uint32_t begin_page_id,
                                                    uint32_t &page_id)
{
  int ret = OB_SUCCESS;
  page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                  ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == virtual_page_id ||
                  !is_valid_page_id_(begin_page_id) ||
                  fd != fat_[begin_page_id].fd_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(virtual_page_id), K(begin_page_id));
  } else if (virtual_page_id < fat_[begin_page_id].page_key_.virtual_page_id_) {
    ret = OB_SEARCH_NOT_FOUND;
    LOG_WARN("virtual_page_id is smaller than that of page of begin_page_id",
             KR(ret), K(virtual_page_id), K(begin_page_id),
             K(fat_[begin_page_id].page_key_.virtual_page_id_));
  } else {
    uint32_t cur_page_id = begin_page_id;
    while (cur_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) { // iter to the end of this file
      if (fat_[cur_page_id].page_key_.virtual_page_id_ >= virtual_page_id) {
        if (fat_[cur_page_id].page_key_.virtual_page_id_ >= virtual_page_id) {
          page_id = cur_page_id;
        }
        break;
      } else {
        cur_page_id = fat_[cur_page_id].next_page_id_;
      }
    }
    if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == page_id)) {
      ret = OB_ITER_END;
      LOG_WARN("wbp fail to find page by given offset", KR(ret), K(virtual_page_id), K(begin_page_id), K(fat_[begin_page_id]));
    }
  }
  return ret;
}

int ObTmpWriteBufferPool::get_page_virtual_id(const int64_t fd, const uint32_t page_id, int64_t &virtual_page_id)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd || !is_valid_page_id_(page_id))) {
    ret = OB_SEARCH_NOT_FOUND;
    LOG_WARN("wbp fail to get page offset in file, invalid page id", KR(ret), K(fd), K(page_id));
  } else if (fd != fat_[page_id].fd_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("wbp fail to get page offset in file, fd not match", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  } else {
    virtual_page_id = fat_[page_id].page_key_.virtual_page_id_;
  }
  return ret;
}

int ObTmpWriteBufferPool::truncate_page(const int64_t fd, const uint32_t page_id,
                                        const ObTmpFilePageUniqKey page_key,
                                        const int64_t truncate_size)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                  !is_valid_page_id_(page_id) || !page_key.is_valid() ||
                  truncate_size > ObTmpFileGlobal::PAGE_SIZE || truncate_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key), K(truncate_size));
  } else if (fd != fat_[page_id].fd_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("wbp fail to truncate page, fd not match", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  } else if (page_key != fat_[page_id].page_key_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("wbp fail to truncate page, page_key not match", KR(ret), K(page_key), K(page_id), K(fat_[page_id]));
  } else {
    MEMSET(fat_[page_id].buf_, 0, truncate_size);
  }
  return ret;
}

int ObTmpWriteBufferPool::link_page(
    const int64_t fd,
    const uint32_t page_id,
    const uint32_t prev_page_id,
    const ObTmpFilePageUniqKey prev_page_key)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                  !is_valid_page_id_(page_id) || !is_valid_page_id_(prev_page_id) ||
                  !prev_page_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(page_id), K(prev_page_id), K(prev_page_key));
  } else if (OB_UNLIKELY(fat_[page_id].fd_ != fd || fat_[prev_page_id].fd_ != fd ||
                         fat_[prev_page_id].next_page_id_ != ObTmpFileGlobal::INVALID_PAGE_ID ||
                         prev_page_key != fat_[prev_page_id].page_key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to link page, unexpected page id or offset", KR(ret), K(fd),
              K(page_id), K(fat_[page_id]), K(prev_page_key), K(prev_page_id),
              K(fat_[prev_page_id]));
  } else if (prev_page_key.type_ == PageEntryType::META) {
    //just for meta page check
    ObTmpFilePageUniqKey page_key(prev_page_key.tree_level_, prev_page_key.level_page_index_ + 1);
    if (OB_UNLIKELY(prev_page_key != fat_[prev_page_id].page_key_
                    || page_key != fat_[page_id].page_key_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to link meta page, unexpected page offset", KR(ret), K(fd), K(prev_page_key), K(page_key),
                                                                K(fat_[prev_page_id]), K(fat_[page_id]));
    }
  }

  if (OB_SUCC(ret)) {
    fat_[prev_page_id].next_page_id_ = page_id;
    if (prev_page_key.type_ == PageEntryType::META) {
      LOG_INFO("link meta page", KR(ret), K(fd), K(page_id), K(prev_page_id));
    } else {
      LOG_DEBUG("link data page", KR(ret), K(fd), K(page_id), K(prev_page_id));
    }
  }
  return ret;
}

int ObTmpWriteBufferPool::free_page(
    const int64_t fd,
    const uint32_t page_id,
    const ObTmpFilePageUniqKey page_key,
    uint32_t &next_page_id)
{
  int ret = OB_SUCCESS;
  next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  common::TCRWLock::RLockGuard guard(lock_);

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                  !is_valid_page_id_(page_id) ||
                  !page_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key));
  } else if (OB_UNLIKELY(fd != fat_[page_id].fd_
                         || page_key != fat_[page_id].page_key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wbp fail to free page, fd or offset not equal", KR(ret), K(page_id), K(fd),
                                                                K(page_key), K(fat_[page_id]));
  } else {
    // reset PageEntry, add it to free list head and update buffer pool statistics
    next_page_id = ATOMIC_LOAD(&(fat_[page_id].next_page_id_));
    PageEntryType page_type = fat_[page_id].page_key_.type_;
    if (PageEntryType::DATA == page_type) {
      LOG_DEBUG("free data page", KR(ret), K(page_id), K(fd), K(fat_[page_id]));
    } else {
      LOG_INFO("free meta page", KR(ret), K(page_id), K(fd), K(fat_[page_id]));
    }
    ATOMIC_SET(&(fat_[page_id].fd_), -1);
    ATOMIC_SET(&(fat_[page_id].next_page_id_), ObTmpFileGlobal::INVALID_PAGE_ID);
    if (ObPageEntry::State::DIRTY == ATOMIC_LOAD(&fat_[page_id].state_)) {
      ATOMIC_DEC(&dirty_page_num_);
      if (PageEntryType::DATA == page_type) {
        ATOMIC_DEC(&dirty_data_page_cnt_);
      } else if (PageEntryType::META == page_type) {
        ATOMIC_DEC(&dirty_meta_page_cnt_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid tmp file page type", KR(ret), K(page_id), K(fd), K(page_type));
      }
    }
    if (ObPageEntry::State::WRITE_BACK == ATOMIC_LOAD(&fat_[page_id].state_)) {
      if (PageEntryType::DATA == page_type) {
        ATOMIC_DEC(&write_back_data_cnt_);
      } else if (PageEntryType::META == page_type) {
        ATOMIC_DEC(&write_back_meta_cnt_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid tmp file page type", KR(ret), K(page_id), K(fd), K(page_type));
      }
    }
    fat_[page_id].switch_state(ObPageEntry::Ops::DELETE);
    fat_[page_id].page_key_.reset();

    bool cas_succeed = false;
    do {
      uint32_t first_free_page_id_before = ATOMIC_LOAD(&first_free_page_id_);
      ATOMIC_SET(&(fat_[page_id].next_page_id_), first_free_page_id_before);
      cas_succeed = ATOMIC_BCAS(&first_free_page_id_, first_free_page_id_before, page_id);
    } while (false == cas_succeed);

    ATOMIC_DEC(&used_page_num_);

    if (PageEntryType::DATA == page_type) {
      ATOMIC_DEC(&data_page_cnt_);
    } else if (PageEntryType::META == page_type) {
      ATOMIC_DEC(&meta_page_cnt_);
    } else {
      LOG_ERROR("invalid tmp file page type", KR(ret), K(page_id), K(fd), K(page_type));
    }
  }
  return ret;
}

// allocate a block size of WBP_BLOCK_SIZE each iteration
// therefore max capacity may slightly exceed memory_limit
int ObTmpWriteBufferPool::expand_()
{
  int ret = OB_SUCCESS;

  // expand the buffer pool to twice the current size, with a minimum of WBP_BLOCK_SIZE
  const int64_t memory_limit = get_memory_limit();
  int64_t current_capacity = ATOMIC_LOAD(&capacity_);
  const int64_t expect_capacity = std::min(
      memory_limit, std::max(current_capacity * 2, int64_t(WBP_BLOCK_SIZE)));

  // continuously allocate 2MB blocks and add them into the buffer pool.
  while (OB_SUCC(ret) && current_capacity < expect_capacity) {
    common::TCRWLock::WLockGuard guard(lock_);
    current_capacity = ATOMIC_LOAD(&capacity_);
    if (current_capacity < expect_capacity) {
      char * new_expand_buf = nullptr;
      // allocate a chunk of WBP_BLOCK_SIZE each time
      if (OB_ISNULL(new_expand_buf = static_cast<char *>(allocator_.alloc(WBP_BLOCK_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("wbp fail to allocate new expand buffer", KR(ret));
      } else {
        uint32_t new_page_id = fat_.count();
        for (uint32_t count = 0; OB_SUCC(ret) && count < BLOCK_PAGE_NUMS; ++new_page_id, ++count) {
          if (OB_FAIL(fat_.push_back(ObPageEntry(-1,
                                                ObTmpFileGlobal::INVALID_PAGE_ID,
                                                new_expand_buf + count * ObTmpFileGlobal::PAGE_SIZE)))) {
            LOG_WARN("wbp fail to push back page into fat", KR(ret), K(count), K(new_page_id));
          } else {
            fat_[new_page_id].next_page_id_ = ATOMIC_LOAD(&first_free_page_id_);
            ATOMIC_SET(&first_free_page_id_, new_page_id);
            ATOMIC_FAA(&capacity_, ObTmpFileGlobal::PAGE_SIZE);
          }
        }
        current_capacity += WBP_BLOCK_SIZE;
      }
    } else {
      // maybe another thread has finish allocation, do nothing.
    }
  }

  LOG_INFO("wbp expand", K(expect_capacity), K(memory_limit), K(ATOMIC_LOAD(&capacity_)));

  return ret;
}

int ObTmpWriteBufferPool::reduce_()
{
  int ret = OB_SUCCESS;
  // TODO(wendongbo): write buffer pool shrinking is currently not supported, use it as destroy() now
  common::TCRWLock::WLockGuard guard(lock_);
  for (int64_t i = 0; i < fat_.count(); i += BLOCK_PAGE_NUMS) {
    char * buf = fat_.at(i).buf_;
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("wbp get unexpected page buffer", KR(ret), K(i), KP(buf));
    } else {
      allocator_.free(buf);
    }
  }
  return ret;
}

int64_t ObTmpWriteBufferPool::get_memory_limit()
{
  int64_t memory_limit = 0;
  int64_t last_access_ts = ATOMIC_LOAD(&last_access_tenant_config_ts_);
  if (default_wbp_memory_limit_ > 0) {
    memory_limit = default_wbp_memory_limit_;
  } else if (last_access_ts > 0 && common::ObClockGenerator::getClock() - last_access_ts < 10000000) { // 10s
    memory_limit = ATOMIC_LOAD(&wbp_memory_limit_);
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (!tenant_config.is_valid()) {
      static const int64_t DEFAULT_MEMORY_LIMIT = 64 * 2 * 1024 * 1024; // 128MB
      memory_limit = wbp_memory_limit_ == 0 ? DEFAULT_MEMORY_LIMIT : wbp_memory_limit_;
      LOG_INFO("failed to get tenant config", K(MTL_ID()), K(memory_limit), K(wbp_memory_limit_));
    } else if (0 == tenant_config->_temporary_file_io_area_size) {
      memory_limit = 0;
    } else {
      int64_t config_memory_limit =
        lib::get_tenant_memory_limit(MTL_ID()) * tenant_config->_temporary_file_io_area_size / 100;
      memory_limit = ((config_memory_limit + WBP_BLOCK_SIZE - 1) / WBP_BLOCK_SIZE) * WBP_BLOCK_SIZE;
    }
    ATOMIC_STORE(&wbp_memory_limit_, memory_limit);
    ATOMIC_STORE(&last_access_tenant_config_ts_, common::ObClockGenerator::getClock());
  }
  return memory_limit;
}

// expect swap min(10% * page cache memory, 20MB) each time
// if clean data size smaller than this min_swap_size return 0
int64_t ObTmpWriteBufferPool::get_swap_size()
{
  const int64_t HIGH_WATERMARK_PECENTAGE = 55;
  const int64_t LOW_WATERMARK_PECENTAGE = 30;

  int64_t memory_limit = get_memory_limit();
  int64_t used_page_num = ATOMIC_LOAD(&used_page_num_);

  int64_t high_watermark_bytes = (double)HIGH_WATERMARK_PECENTAGE / 100 * memory_limit;
  int64_t low_watermark_bytes = (double)LOW_WATERMARK_PECENTAGE / 100 * memory_limit;
  int64_t used_bytes = used_page_num * ObTmpFileGlobal::PAGE_SIZE;

  const int64_t MACRO_BLOCK_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_object_size();

  int64_t swap_size = 0;
  if (used_bytes > high_watermark_bytes) {
    int64_t expected_swap_size = used_bytes - low_watermark_bytes;
    int64_t dirty_data_bytes = ATOMIC_LOAD(&dirty_page_num_) * ObTmpFileGlobal::PAGE_SIZE;
    swap_size = min(used_bytes - dirty_data_bytes, expected_swap_size);
  }

  return swap_size;
}

bool ObTmpWriteBufferPool::is_exist(const int64_t fd, const uint32_t page_id,
                                    const ObTmpFilePageUniqKey page_key)
{
  common::TCRWLock::RLockGuard guard(lock_);
  bool exist = false;
  if (OB_LIKELY(is_valid_page_id_(page_id)
      && fd == fat_[page_id].fd_
      && page_key.is_valid()
      && page_key == fat_[page_id].page_key_)) {
    exist = ObPageEntry::State::INVALID < fat_[page_id].state_;
  } else {
    int ret = OB_ERR_UNEXPECTED; // TODO: too many warn logs
    LOG_WARN("wbp get unexpected page entry", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  }
  return exist;
}

bool ObTmpWriteBufferPool::is_inited(const int64_t fd, const uint32_t page_id,
                                     const ObTmpFilePageUniqKey page_key)
{
  common::TCRWLock::RLockGuard guard(lock_);
  bool inited = false;
  if (OB_LIKELY(is_valid_page_id_(page_id)
      && fd == fat_[page_id].fd_
      && page_key.is_valid()
      && page_key == fat_[page_id].page_key_)) {
    inited = ObPageEntry::State::INITED == fat_[page_id].state_;
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wbp get unexpected page entry", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  }
  return inited;
}

bool ObTmpWriteBufferPool::is_loading(const int64_t fd, const uint32_t page_id,
                                      const ObTmpFilePageUniqKey page_key)
{
  common::TCRWLock::RLockGuard guard(lock_);
  bool loading = false;
  if (OB_LIKELY(is_valid_page_id_(page_id)
      && fd == fat_[page_id].fd_
      && page_key.is_valid()
      && page_key == fat_[page_id].page_key_)) {
    loading = ObPageEntry::State::LOADING == fat_[page_id].state_;
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wbp get unexpected page entry", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  }
  return loading;
}

bool ObTmpWriteBufferPool::is_cached(
     const int64_t fd,
     const uint32_t page_id,
     const ObTmpFilePageUniqKey page_key)
{
  common::TCRWLock::RLockGuard guard(lock_);
  bool cached = false;
  if (OB_LIKELY(is_valid_page_id_(page_id)
      && fd == fat_[page_id].fd_
      && page_key.is_valid()
      && page_key == fat_[page_id].page_key_)) {
    cached = ObPageEntry::State::CACHED == fat_[page_id].state_;
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wbp get unexpected page entry", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  }
  return cached;
}

bool ObTmpWriteBufferPool::is_write_back(const int64_t fd, const uint32_t page_id,
                                         const ObTmpFilePageUniqKey page_key)
{
  common::TCRWLock::RLockGuard guard(lock_);
  bool write_back = false;
  if (OB_LIKELY(is_valid_page_id_(page_id)
      && fd == fat_[page_id].fd_
      && page_key.is_valid()
      && page_key == fat_[page_id].page_key_)) {
    write_back = ObPageEntry::State::WRITE_BACK == fat_[page_id].state_;
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wbp get unexpected page entry", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  }
  return write_back;
}

bool ObTmpWriteBufferPool::is_dirty(
     const int64_t fd,
     const uint32_t page_id,
     const ObTmpFilePageUniqKey page_key)
{
  common::TCRWLock::RLockGuard guard(lock_);
  bool dirty = false;
  if (OB_LIKELY(is_valid_page_id_(page_id)
      && fd == fat_[page_id].fd_
      && page_key.is_valid()
      && page_key == fat_[page_id].page_key_)) {
    dirty = ObPageEntry::State::DIRTY == fat_[page_id].state_;
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wbp get unexpected page entry", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  }
  return dirty;
}

// 允许 INITED/CACHED/DIRTY/WRITE_BACK 状态页面切换为 DIRTY 状态
int ObTmpWriteBufferPool::notify_dirty(
    const int64_t fd,
    const uint32_t page_id,
    const ObTmpFilePageUniqKey page_key)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  bool is_already_dirty = false;
  bool is_write_back = false;
  if (OB_UNLIKELY(!is_valid_page_id_(page_id)
                  || INVALID_FD == fd
                  || fd != fat_[page_id].fd_
                  || !page_key.is_valid()
                  || page_key != fat_[page_id].page_key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  } else if (FALSE_IT(is_already_dirty = (ObPageEntry::State::DIRTY == fat_[page_id].state_))) {
  } else if (FALSE_IT(is_write_back = (ObPageEntry::State::WRITE_BACK == fat_[page_id].state_))) {
  } else if (OB_FAIL(fat_[page_id].switch_state(ObPageEntry::Ops::WRITE))) {
    LOG_WARN("fail to switch state to DIRTY", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  } else {
    if (!is_already_dirty) {
      ATOMIC_INC(&dirty_page_num_);
      if (PageEntryType::DATA == page_key.type_) {
        ATOMIC_INC(&dirty_data_page_cnt_);
      } else if (PageEntryType::META == page_key.type_) {
        ATOMIC_INC(&dirty_meta_page_cnt_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid page type", K(page_key));
      }
    }
    if (is_write_back) {
      if (PageEntryType::DATA == page_key.type_) {
        ATOMIC_DEC(&write_back_data_cnt_);
      } else if (PageEntryType::META == page_key.type_) {
        ATOMIC_DEC(&write_back_meta_cnt_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid page type", K(page_key));
      }
    }
  }
  return ret;
}

int ObTmpWriteBufferPool::notify_load(
    const int64_t fd,
    const uint32_t page_id,
    const ObTmpFilePageUniqKey page_key)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_page_id_(page_id)
                  || INVALID_FD == fd
                  || fd != fat_[page_id].fd_
                  || !page_key.is_valid()
                  || page_key != fat_[page_id].page_key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  } else if (OB_FAIL(fat_[page_id].switch_state(ObPageEntry::Ops::LOAD))) {
    LOG_WARN("fail to switch state from INITED to LOADING", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  }
  return ret;
}

int ObTmpWriteBufferPool::notify_load_succ(const int64_t fd, const uint32_t page_id,
                                           const ObTmpFilePageUniqKey page_key)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_page_id_(page_id)
                  || INVALID_FD == fd
                  || fd != fat_[page_id].fd_
                  || !page_key.is_valid()
                  || page_key != fat_[page_id].page_key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  } else if (OB_FAIL(fat_[page_id].switch_state(ObPageEntry::Ops::LOAD_SUCC))) {
    LOG_WARN("fail to switch state from LOADING to CACHED", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  }
  return ret;
}

int ObTmpWriteBufferPool::notify_load_fail(
    const int64_t fd,
    const uint32_t page_id,
    const ObTmpFilePageUniqKey page_key)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_page_id_(page_id)
                  || INVALID_FD == fd
                  || fd != fat_[page_id].fd_
                  || !page_key.is_valid()
                  || page_key != fat_[page_id].page_key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  } else if (OB_FAIL(fat_[page_id].switch_state(ObPageEntry::Ops::LOAD_FAIL))) {
    LOG_WARN("fail to switch state from LOADING to INITED", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  }
  return ret;
}

int ObTmpWriteBufferPool::notify_write_back(
    const int64_t fd,
    const uint32_t page_id,
    const ObTmpFilePageUniqKey page_key)
{
  int ret = OB_SUCCESS;
  bool is_dirty = false;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_page_id_(page_id)
                  || INVALID_FD == fd
                  || fd != fat_[page_id].fd_
                  || !page_key.is_valid()
                  || page_key != fat_[page_id].page_key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  } else if (FALSE_IT(is_dirty = (ObPageEntry::State::DIRTY == fat_[page_id].state_))) {
  } else if (OB_FAIL(fat_[page_id].switch_state(ObPageEntry::Ops::WRITE_BACK))) {
    LOG_WARN("fail to switch state from DIRTY to WRITE_BACK", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  } else if (is_dirty) {
    ATOMIC_DEC(&dirty_page_num_);
    if (PageEntryType::DATA == fat_[page_id].page_key_.type_) {
      ATOMIC_DEC(&dirty_data_page_cnt_);
      ATOMIC_INC(&write_back_data_cnt_);
      LOG_DEBUG("notify data write back", K(fd), K(page_id), K(fat_[page_id]));
    } else if (PageEntryType::META == fat_[page_id].page_key_.type_) {
      ATOMIC_DEC(&dirty_meta_page_cnt_);
      ATOMIC_INC(&write_back_meta_cnt_);
      LOG_INFO("notify meta write back", K(fd), K(page_id), K(fat_[page_id]));
    }
  }
  return ret;
}

int ObTmpWriteBufferPool::notify_write_back_succ(
    const int64_t fd,
    const uint32_t page_id,
    const ObTmpFilePageUniqKey page_key)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_page_id_(page_id)
                  || INVALID_FD == fd
                  || fd != fat_[page_id].fd_
                  || !page_key.is_valid()
                  || page_key != fat_[page_id].page_key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  } else if (OB_FAIL(fat_[page_id].switch_state(ObPageEntry::Ops::WRITE_BACK_SUCC))) {
    LOG_WARN("fail to switch state from WRITE_BACK to CACHED", KR(ret), K(fd), K(page_id), K(fat_[page_id]));
  } else {
    if (PageEntryType::DATA == fat_[page_id].page_key_.type_) {
      ATOMIC_DEC(&write_back_data_cnt_);
      LOG_DEBUG("notify data write back succ", K(fd), K(page_id), K(fat_[page_id]));
    } else if (PageEntryType::META == fat_[page_id].page_key_.type_) {
      ATOMIC_DEC(&write_back_meta_cnt_);
      LOG_INFO("notify meta write back succ", K(fd), K(page_id), K(fat_[page_id]));
    }
  }
  return ret;
}

int ObTmpWriteBufferPool::notify_write_back_fail(int64_t fd, uint32_t page_id,
                                                 const ObTmpFilePageUniqKey page_key)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_page_id_(page_id)
                  || INVALID_FD == fd
                  || fd != fat_[page_id].fd_
                  || !page_key.is_valid()
                  || page_key != fat_[page_id].page_key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(page_id), K(page_key), K(fat_[page_id]));
  } else if (OB_FAIL(fat_[page_id].switch_state(ObPageEntry::Ops::WRITE_BACK_FAILED))) {
    LOG_WARN("fail to switch state from WRITE_BACK to DIRTY", K(fd), K(page_id), K(fat_[page_id]));
  } else {
    ATOMIC_INC(&dirty_page_num_);
    if (PageEntryType::DATA == fat_[page_id].page_key_.type_) {
      ATOMIC_INC(&dirty_data_page_cnt_);
      ATOMIC_DEC(&write_back_data_cnt_);
      LOG_INFO("notify data write back fail", K(fd), K(page_id), K(fat_[page_id]));
    } else if (PageEntryType::META == fat_[page_id].page_key_.type_) {
      ATOMIC_INC(&dirty_meta_page_cnt_);
      ATOMIC_DEC(&write_back_meta_cnt_);
      LOG_INFO("notify meta write back fail", K(fd), K(page_id), K(fat_[page_id]));
    }
  }
  return ret;
}

// return write buffer pool maximum page number, which is determined by tenant memory and config
int64_t ObTmpWriteBufferPool::get_max_page_num()
{
  int64_t mem_limit = get_memory_limit();
  return mem_limit / ObTmpFileGlobal::PAGE_SIZE;
}

// return dirty page percentage
int64_t ObTmpWriteBufferPool::get_dirty_page_percentage()
{
  int64_t max_page_num = get_max_page_num();
  int64_t dirty_page_num = ATOMIC_LOAD(&dirty_page_num_);
  return max_page_num == 0 ? 0 : dirty_page_num * 100 / max_page_num;
}

int64_t ObTmpWriteBufferPool::get_cannot_be_evicted_page_percentage()
{
  int64_t max_page_num = get_max_page_num();

  int64_t dirty_page_num = ATOMIC_LOAD(&dirty_page_num_);

  int64_t write_back_data_num = ATOMIC_LOAD(&write_back_data_cnt_);
  int64_t write_back_meta_num = ATOMIC_LOAD(&write_back_meta_cnt_);
  int64_t total_write_back_num = write_back_data_num + write_back_meta_num;

  return max_page_num == 0 ? 0 : (dirty_page_num + total_write_back_num) * 100 / max_page_num;
}

int64_t ObTmpWriteBufferPool::get_dirty_page_num()
{
  return ATOMIC_LOAD(&dirty_page_num_);
}

int64_t ObTmpWriteBufferPool::get_dirty_meta_page_num()
{
  return ATOMIC_LOAD(&dirty_meta_page_cnt_);
}

int64_t ObTmpWriteBufferPool::get_dirty_data_page_num()
{
  return ATOMIC_LOAD(&dirty_data_page_cnt_);
}

int64_t ObTmpWriteBufferPool::get_data_page_num()
{
  return ATOMIC_LOAD(&data_page_cnt_);
}

int64_t ObTmpWriteBufferPool::get_max_data_page_num()
{
  return get_max_page_num() * MAX_DATA_PAGE_USAGE_RATIO;
}

int64_t ObTmpWriteBufferPool::get_meta_page_num()
{
  return ATOMIC_LOAD(&meta_page_cnt_);
}

int64_t ObTmpWriteBufferPool::get_free_data_page_num()
{
  int64_t data_page_cnt = ATOMIC_LOAD(&data_page_cnt_);
  int64_t meta_page_cnt = ATOMIC_LOAD(&meta_page_cnt_);

  int64_t total_free_page_cnt = get_max_page_num() - data_page_cnt - meta_page_cnt;
  int64_t data_free_page_cnt = get_max_data_page_num() - data_page_cnt;

  return MIN(total_free_page_cnt, data_free_page_cnt);
}

bool ObTmpWriteBufferPool::has_free_page_(PageEntryType type)
{
  int ret = OB_SUCCESS;
  bool b_ret = true;
  if (PageEntryType::DATA == type) {
    b_ret = get_data_page_num() <  get_max_data_page_num();
  } else if (PageEntryType::META == type) {
    b_ret = true; // no limit for meta page
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected page type", KR(ret), K(type));
  }
  return b_ret;
}

void ObTmpWriteBufferPool::print_statistics()
{
  int64_t dirty_page_percentage = get_dirty_page_percentage();
  int64_t max_page_num = get_max_page_num();
  int64_t meta_page_num = get_meta_page_num();
  int64_t data_page_num = get_data_page_num();
  int64_t dirty_page_num = get_dirty_page_num();
  int64_t dirty_meta_page_num = get_dirty_meta_page_num();
  int64_t dirty_data_page_num = get_dirty_data_page_num();
  int64_t write_back_data_num = ATOMIC_LOAD(&write_back_data_cnt_);
  int64_t write_back_meta_num = ATOMIC_LOAD(&write_back_meta_cnt_);
  int64_t data_page_watermark = data_page_num * 100 / max(max_page_num, 1);
  int64_t meta_page_watermark = meta_page_num * 100 / max(max_page_num, 1);
  int64_t total_write_back_num = write_back_data_num + write_back_meta_num;
  LOG_INFO("tmp file write buffer pool statistics",
      K(dirty_page_percentage), K(max_page_num), K(dirty_page_num), K(total_write_back_num),
      K(meta_page_num), K(dirty_meta_page_num), K(write_back_meta_num),
      K(data_page_num), K(dirty_data_page_num), K(write_back_data_num),
      K(data_page_watermark), K(meta_page_watermark));
}

}  // end namespace tmp_file
}  // end namespace oceanbase
