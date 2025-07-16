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

#include "storage/tmp_file/ob_tmp_file_block.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/blocksstable/ob_block_manager.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{

//---------------------------ObTmpFileBlockPageBitmap---------------------------
ObTmpFileBlockPageBitmap &ObTmpFileBlockPageBitmap::operator=(const ObTmpFileBlockPageBitmap &other)
{
  if (this != &other) {
    MEMCPY(bitmap_, other.bitmap_, PAGE_BYTE_CAPACITY);
  }
  return *this;
}

int ObTmpFileBlockPageBitmap::get_value(const int64_t offset, bool &value) const
{
  int ret = OB_SUCCESS;
  const int64_t capacity = PAGE_CAPACITY;

  if (OB_UNLIKELY(capacity <= offset || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(capacity), K(offset));
  } else {
    value = (bitmap_[offset / 8] & (1 << (offset % 8))) != 0;
  }
  return ret;
}

int ObTmpFileBlockPageBitmap::set_bitmap(const int64_t offset, const bool value)
{
  int ret = OB_SUCCESS;
  const int64_t capacity = PAGE_CAPACITY;
  if (OB_UNLIKELY(capacity <= offset || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(capacity), K(offset));
  } else if (value) {
    bitmap_[offset / 8] |= (1 <<(offset % 8));
  } else {
    bitmap_[offset / 8] &= ~(1 <<(offset % 8));
  }
  return ret;
}

int ObTmpFileBlockPageBitmap::set_bitmap_batch(const int64_t offset, const int64_t count, const bool value)
{
  int ret = OB_SUCCESS;
  const int64_t capacity = PAGE_CAPACITY;
  uint8_t start_byte_pos = offset / 8;
  uint8_t end_byte_pos = (offset + count - 1) / 8;
  uint8_t start_bit_pos = offset % 8;
  uint8_t end_bit_pos = (offset + count - 1) % 8;
  if (OB_UNLIKELY(capacity < (offset + count) || count <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(capacity), K(offset), K(count));
  } else if (start_byte_pos == end_byte_pos) {
    // only one byte
    uint8_t mask = ((1 << (end_bit_pos + 1)) - 1) & ~((1 << start_bit_pos) - 1);
    if (value) {
      bitmap_[start_byte_pos] |= mask;
    } else {
      bitmap_[start_byte_pos] &= ~mask;
    }
  } else {
    uint8_t start_mask = ~((1 << start_bit_pos) - 1);
    uint8_t end_mask = (1 << (end_bit_pos + 1)) - 1;
    if (value) {
      bitmap_[start_byte_pos] |= start_mask;
      bitmap_[end_byte_pos] |= end_mask;
    } else {
      bitmap_[start_byte_pos] &= ~start_mask;
      bitmap_[end_byte_pos] &= ~end_mask;
    }
    for (uint8_t i = start_byte_pos + 1; i < end_byte_pos; ++i) {
      bitmap_[i] = value ? (1 << 8) - 1 : 0;
    }
  }

  return ret;
}

int ObTmpFileBlockPageBitmap::is_all_true(const int64_t start, const int64_t end, bool &is_all_true) const
{
  int ret = OB_SUCCESS;
  const int64_t capacity = PAGE_CAPACITY;
  const uint8_t start_byte_pos = start / 8;
  const uint8_t end_byte_pos = end / 8;
  const uint8_t start_bit_pos = start % 8;
  const uint8_t end_bit_pos = end % 8;
  is_all_true = false;

  if (OB_UNLIKELY(start > end || start < 0 || end >= capacity)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(capacity), K(start), K(end));
  } else if (start_byte_pos == end_byte_pos) {
    // only one byte
    uint8_t mask = ((1 << (end_bit_pos + 1)) - 1) & ~((1 << start_bit_pos) - 1);
    is_all_true = (bitmap_[start_byte_pos] & mask) == mask;
  } else {
    uint8_t start_mask = ~((1 << start_bit_pos) - 1);
    uint8_t end_mask = (1 << (end_bit_pos + 1)) - 1;

    if ((bitmap_[start_byte_pos] & start_mask) != start_mask ||
        (bitmap_[end_byte_pos] & end_mask) != end_mask) {
      is_all_true = false;
    } else {
      is_all_true = true;
      for (int64_t i = start_byte_pos + 1; is_all_true && i < end_byte_pos; ++i) {
        if (bitmap_[i] != 0XFF) {
          is_all_true = false;
        }
      }
    }
  }

  return ret;
}

int ObTmpFileBlockPageBitmap::is_all_false(const int64_t start, const int64_t end, bool &is_all_false) const
{
  int ret = OB_SUCCESS;
  const int64_t capacity = PAGE_CAPACITY;
  const uint8_t start_byte_pos = start / 8;
  const uint8_t end_byte_pos = end / 8;
  const uint8_t start_bit_pos = start % 8;
  const uint8_t end_bit_pos = end % 8;
  is_all_false = false;

  if (OB_UNLIKELY(start > end || start < 0 || end >= capacity)) {
    is_all_false = false;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(capacity), K(start), K(end));
  } else if (start_byte_pos == end_byte_pos) {
    // only one byte
    uint8_t mask = ((1 << (end_bit_pos + 1)) - 1) & ~((1 << start_bit_pos) - 1);
    is_all_false = (bitmap_[start_byte_pos] & mask) == 0;
  } else {
    uint8_t start_mask = ~((1 << start_bit_pos) - 1);
    uint8_t end_mask = (1 << (end_bit_pos + 1)) - 1;

    if ((bitmap_[start_byte_pos] & start_mask) != 0 ||
        (bitmap_[end_byte_pos] & end_mask) != 0) {
      is_all_false = false;
    } else {
      is_all_false = true;
      for (int64_t i = start_byte_pos + 1; is_all_false && i < end_byte_pos; ++i) {
        if (bitmap_[i] != 0) {
          is_all_false = false;
        }
      }
    }
  }

  return ret;
}

int ObTmpFileBlockPageBitmap::is_all_true(bool &b_ret) const
{
  return is_all_true(0, PAGE_CAPACITY - 1, b_ret);
}

int ObTmpFileBlockPageBitmap::is_all_false(bool &b_ret) const
{
  return is_all_false(0, PAGE_CAPACITY - 1, b_ret);
}

int64_t ObTmpFileBlockPageBitmap::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this));
  for (int i = 0; i < PAGE_BYTE_CAPACITY / sizeof(uint64_t); ++i) {
    int64_t offset = i * sizeof(uint64_t);
    J_COMMA();
    char bitmap_id[11];
    snprintf(bitmap_id, sizeof(bitmap_id), "bitmap_[%d]", i);
    J_KV(bitmap_id, *(reinterpret_cast<const uint64_t*>(bitmap_ + offset)));
  }
  J_OBJ_END();
  return pos;
}

//-----------------------ObTmpFileBlock-----------------------
ObTmpFileBlock::~ObTmpFileBlock()
{
  destroy();
}

int ObTmpFileBlock::destroy()
{
  int ret = OB_SUCCESS;

  if (!is_valid_without_lock()) {
    // block has not been initializing, do nothing
  } else if (macro_block_id_.is_valid() && OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_block_id_))) {
    LOG_ERROR("failed to dec macro block ref cnt", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!flushing_page_list_.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("flushing page list is not empty", KR(ret), KPC(this));
  } else {
    LOG_DEBUG("destroying block", KPC(this), K(lbt()));
    if (OB_NOT_NULL(flush_blk_node_.get_next()) || OB_NOT_NULL(prealloc_blk_node_.get_prev())) {
      LOG_ERROR("invalid list", KPC(this));
    }
    block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
    macro_block_id_.reset();
    is_in_deleting_ = false;
    is_in_flushing_ = false;
    ref_cnt_ = 0;
    type_ = ObTmpFileBlock::INVALID;
    free_page_num_ = 0;
    prealloc_blk_node_.unlink();
    flush_blk_node_.unlink();
    alloc_page_bitmap_.reset();
    flushed_page_bitmap_.reset();
    flushing_page_bitmap_.reset();
    flushing_page_list_.clear();
    tmp_file_blk_mgr_ = nullptr;
  }
  return ret;
}

int ObTmpFileBlock::init(const int64_t block_index, BlockType type, ObTmpFileBlockManager *tmp_file_blk_mgr)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX == block_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block_index", KR(ret), K(block_index));
  } else if (OB_UNLIKELY(block_index_ != ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block has been inited", KR(ret), K(block_index_));
  } else if (OB_ISNULL(tmp_file_blk_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file block mgr is null", KR(ret), K(tmp_file_blk_mgr));
  } else if (OB_UNLIKELY(type == ObTmpFileBlock::INVALID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block type", KR(ret), K(type));
  } else {
    tmp_file_blk_mgr_ = tmp_file_blk_mgr;
    block_index_ = block_index;
    type_ = type;
    free_page_num_ = ObTmpFileGlobal::BLOCK_PAGE_NUMS;

    LOG_DEBUG("init block successfully", K(block_index), KPC(this));
  }
  return ret;
}

int ObTmpFileBlock::set_macro_block_id(const blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(macro_block_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro block id has been set", KR(ret), KPC(this));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_block_id))) {
    LOG_ERROR("failed to inc macro block ref cnt", KR(ret), K(macro_block_id));
  } else {
    macro_block_id_ = macro_block_id;
    LOG_DEBUG("set macro block id successfully", K(block_index_), K(macro_block_id_));
  }
  return ret;
}

blocksstable::MacroBlockId ObTmpFileBlock::get_macro_block_id() const
{
  SpinRLockGuard guard(lock_);
  return macro_block_id_;
}

int ObTmpFileBlock::alloc_pages(const int64_t page_num)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  LOG_DEBUG("alloc pages start", K(page_num), KPC(this));
  bool not_be_allocated_page = false;
  const int64_t old_free_page_num = free_page_num_;
  const int64_t allocating_begin_page_id = get_begin_page_id_without_lock();
  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(old_free_page_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no free page", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(page_num <= 0 ||
             allocating_begin_page_id + page_num > ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_index_), K(allocating_begin_page_id), K(page_num));
  } else if (OB_UNLIKELY(is_in_deleting_)) {
    ret = OB_RESOURCE_RELEASED;
    LOG_INFO("block is in deleting", KR(ret), KPC(this));
  } else  if (OB_FAIL(alloc_page_bitmap_.is_all_false(
                      allocating_begin_page_id, allocating_begin_page_id + page_num - 1, not_be_allocated_page))) {
    LOG_WARN("fail to check whether the pages are all false", KR(ret), K(block_index_), K(macro_block_id_),
             K(allocating_begin_page_id), K(page_num), K(allocating_begin_page_id));
  } else if (OB_UNLIKELY(!not_be_allocated_page)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to allocate a allocated page", KR(ret), K(block_index_), K(macro_block_id_),
             K(allocating_begin_page_id), K(page_num), K(allocating_begin_page_id));
  } else if (OB_FAIL(alloc_page_bitmap_.set_bitmap_batch(allocating_begin_page_id, page_num, true))) {
    LOG_WARN("fail to set bitmap", KR(ret), K(allocating_begin_page_id), K(page_num), K(alloc_page_bitmap_));
  } else if (FALSE_IT(free_page_num_ -= page_num)) {
  } else if (is_shared_block()) {
    if (free_page_num_ == 0) {
      if (OB_FAIL(tmp_file_blk_mgr_->remove_block_from_alloc_priority_mgr(old_free_page_num, *this))) {
        LOG_WARN("fail to remove block from alloc priority mgr", KR(ret), KPC(this));
      }
    } else {
      if (prealloc_blk_node_.get_next() == nullptr) {
        if (OB_FAIL(tmp_file_blk_mgr_->insert_block_into_alloc_priority_mgr(free_page_num_, *this))) {
          LOG_WARN("fail to insert block into alloc priority mgr", KR(ret), KPC(this));
        }
      } else {
        if (OB_FAIL(tmp_file_blk_mgr_->adjust_block_alloc_priority(old_free_page_num, free_page_num_, *this))) {
          LOG_WARN("fail to adjust block alloc priority", KR(ret), K(old_free_page_num), K(free_page_num_), KPC(this));
        }
      }
    }
  }

  LOG_DEBUG("alloc pages over", KR(ret), K(allocating_begin_page_id), K(page_num), KPC(this));
  return ret;
}

int ObTmpFileBlock::release_pages(const int64_t begin_page_id, const int64_t page_num, bool &can_remove)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  LOG_DEBUG("release pages start", K(begin_page_id), K(page_num), KPC(this));
  bool release_allocated_page = false;
  const int64_t old_flushing_page_num = flushing_page_list_.get_size();
  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(page_num <= 0 || begin_page_id < 0 ||
             begin_page_id + page_num > ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_index_), K(begin_page_id), K(page_num));
  } else if (OB_FAIL(alloc_page_bitmap_.is_all_true(
                     begin_page_id, begin_page_id + page_num - 1, release_allocated_page))) {
    LOG_WARN("fail to check whether the pages are all true", KR(ret), K(block_index_), K(macro_block_id_),
             K(begin_page_id), K(page_num), K(alloc_page_bitmap_));
  } else if (OB_UNLIKELY(!release_allocated_page)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to release a released page", KR(ret), K(block_index_), K(macro_block_id_),
                                                   K(begin_page_id), K(page_num),
                                                   K(alloc_page_bitmap_), K(release_allocated_page));
  } else if (OB_FAIL(alloc_page_bitmap_.set_bitmap_batch(begin_page_id, page_num, false))) {
    LOG_WARN("fail to set bitmap", KR(ret), K(begin_page_id), K(page_num), K(alloc_page_bitmap_));
  } else {
    for (int64_t page_id = begin_page_id; OB_SUCC(ret) && page_id < begin_page_id + page_num; ++page_id) {
      if (OB_FAIL(remove_page_from_flushing_status_(page_id))) {
        LOG_WARN("fail to remove page from flushing status", KR(ret), K(page_id), KPC(this));
      }
    }
    if (OB_SUCC(ret) && old_flushing_page_num > 0) {
      if (OB_FAIL(update_block_flush_level_(old_flushing_page_num))) {
        LOG_WARN("fail to update block flush level", KR(ret), K(old_flushing_page_num), KPC(this));
      }
    }
  }

  if (FAILEDx(can_remove_(can_remove))) {
    LOG_WARN("fail to check whether the pages can be removed", KR(ret), K(begin_page_id), K(page_num), KPC(this));
  } else if (can_remove) {
    if (is_shared_block() && prealloc_blk_node_.get_next() != nullptr) {
      if (OB_FAIL(tmp_file_blk_mgr_->remove_block_from_alloc_priority_mgr(free_page_num_, *this))) {
        LOG_ERROR("fail to remove block from alloc priority mgr", KR(ret), K(free_page_num_), KPC(this));
      }
    }
    is_in_deleting_ = true;
    LOG_DEBUG("block can be deleted", KR(ret), KPC(this));
  }

  LOG_DEBUG("release pages over", KR(ret), K(begin_page_id), K(page_num), K(can_remove), KPC(this));
  return ret;
}

int ObTmpFileBlock::can_remove_(bool &can_remove) const
{
  int ret = OB_SUCCESS;
  can_remove = false;

  bool all_page_released = false;
  bool no_flushing_pages = false;
  if (OB_FAIL(alloc_page_bitmap_.is_all_false(all_page_released))) {
    LOG_WARN("fail to check whether the pages are all false", KR(ret), KPC(this));
  } else if (OB_FAIL(flushing_page_bitmap_.is_all_false(no_flushing_pages))) {
    LOG_WARN("fail to check whether the pages are all false", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!flushing_page_list_.is_empty() && no_flushing_pages)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid flushing status", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(all_page_released && !no_flushing_pages)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("block is empty, but still has flushing pages", KR(ret), KPC(this));
  } else {
    can_remove = all_page_released;
  }
  return ret;
}

int ObTmpFileBlock::remove_page_from_flushing_status_(const int64_t page_id)
{
  int ret = OB_SUCCESS;
  bool is_flushing = false;
  if (OB_FAIL(flushing_page_bitmap_.get_value(page_id, is_flushing))) {
    LOG_WARN("fail to get value from bitmap", KR(ret), KPC(this));
  } else if (is_flushing) {
    ObTmpFilePage::PageListNode *node = flushing_page_list_.get_header();
    bool find = false;
    while(OB_SUCC(ret) && !find && node->get_next() != flushing_page_list_.get_header()) {
      node = node->get_next();
      if (node->page_.get_page_id().page_index_in_block_ == page_id) {
        find = true;
        flushing_page_list_.remove(node);
        node->page_.dec_ref();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!find)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find the page in flushing list", KR(ret), KPC(this));
    } else if (OB_FAIL(flushing_page_bitmap_.set_bitmap(page_id, false))) {
      LOG_WARN("fail to set bitmap", KR(ret), KPC(this));
    }
  }
  LOG_DEBUG("remove page from flushing status over", KR(ret), K(page_id), KPC(this));
  return ret;
}

int ObTmpFileBlock::reinsert_into_flush_prio_mgr()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (OB_FAIL(reinsert_into_flush_prio_mgr_())) {
    LOG_WARN("fail to reinsert into flush prio mgr", KR(ret), KPC(this));
  }

  return ret;
}

int ObTmpFileBlock::reinsert_into_flush_prio_mgr_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (is_in_deleting_) {
    // all pages have been released when the block is picked up for pre-alloc, do nothing
    LOG_INFO("block is in deleting, do nothing", KPC(this));
  } else if (OB_UNLIKELY(!is_in_flushing_)) {
    LOG_DEBUG("block is not in flushing", KR(ret), KPC(this));
  } else if (flushing_page_list_.get_size() > 0 &&
             OB_FAIL(tmp_file_blk_mgr_->insert_block_into_flush_priority_mgr(flushing_page_list_.get_size(), *this))) {
    LOG_WARN("fail to insert block into flush priority mgr", KR(ret), K(flushing_page_list_.get_size()), KPC(this));
  } else {
    is_in_flushing_ = false;
  }
  LOG_DEBUG("reinsert into flush prio mgr over", KR(ret), K(flushing_page_list_.get_size()), KPC(this));
  return ret;
}

// No action needed if this page is already in the flushing list
int ObTmpFileBlock::insert_page_into_flushing_list(ObTmpFilePageHandle &page_handle)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const int64_t old_flushing_page_num = flushing_page_list_.get_size();

  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_in_deleting_)) {
    ret = OB_RESOURCE_RELEASED;
    LOG_WARN("block is in deleting", KR(ret), KPC(this));
  } else if (OB_FAIL(insert_page_into_flushing_list_(page_handle))) {
    LOG_WARN("fail to insert page into flushing list", KR(ret), K(page_handle), KPC(this));
  }  else if (OB_FAIL(update_block_flush_level_(old_flushing_page_num))) {
    LOG_WARN("fail to update block flush level", KR(ret), K(old_flushing_page_num), KPC(this));
  }
  LOG_DEBUG("insert page into flushing list over", KR(ret), K(old_flushing_page_num), K(flushing_page_list_.get_size()), K(page_handle), KPC(this));
  return ret;
}

// page_arr is a sorted array
int ObTmpFileBlock::insert_pages_into_flushing_list(ObIArray<ObTmpFilePageHandle> &page_arr)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const int64_t old_flushing_page_num = flushing_page_list_.get_size();

  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(page_arr.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_in_deleting_)) {
    ret = OB_RESOURCE_RELEASED;
    LOG_WARN("block is in deleting", KR(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < page_arr.count(); ++i) {
      if (OB_FAIL(insert_page_into_flushing_list_(page_arr.at(i)))) {
        LOG_WARN("fail to insert page into flushing list", KR(ret), K(page_arr.at(i)), KPC(this));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_in_flushing_) {
      // do nothing
      LOG_DEBUG("block is already in flushing, skip insertion", KPC(this));
    } else if (OB_FAIL(update_block_flush_level_(old_flushing_page_num))) {
      LOG_WARN("fail to update block flush level", KR(ret), K(old_flushing_page_num), KPC(this));
    }
  }
  LOG_DEBUG("insert pages into flushing list over", KR(ret), K(old_flushing_page_num),
            K(flushing_page_list_.get_size()), K(page_arr.count()), KPC(this));
  return ret;
}

int ObTmpFileBlock::insert_page_into_flushing_list_(ObTmpFilePageHandle &page_handle)
{
  int ret = OB_SUCCESS;
  bool is_allocated = false;
  bool is_flushing = false;

  if (OB_UNLIKELY(!page_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(page_handle));
  } else {
    ObTmpFilePage *page = page_handle.get_page();
    ObTmpFilePageId page_id = page->get_page_id();

    if (OB_UNLIKELY(!page->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", KR(ret), KPC(page));
    } else if (OB_UNLIKELY(page_id.block_index_ != block_index_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", KR(ret), K(page_id), KPC(this));
    } else if (OB_FAIL(flushing_page_bitmap_.get_value(page_id.page_index_in_block_, is_flushing))) {
      LOG_WARN("fail to get value from bitmap", KR(ret), KPC(this));
    } else if (is_flushing) {
      // do nothing
      LOG_DEBUG("page is already in flushing list, ignore", K(is_flushing), KPC(page), KPC(this));
    } else if (OB_FAIL(alloc_page_bitmap_.get_value(page_id.page_index_in_block_, is_allocated))) {
      LOG_WARN("fail to get value from bitmap", KR(ret), KPC(this));
    } else if (OB_UNLIKELY(!is_allocated)) {
      ret = OB_RESOURCE_RELEASED;
      LOG_WARN("page is not allocated", KR(ret), KPC(this));
    } else if (OB_UNLIKELY(!flushing_page_list_.add_first(&page->get_list_node()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to push back page into list", KR(ret), KPC(page), KPC(this));
    } else if (FALSE_IT(page->inc_ref())) {
    } else if (OB_FAIL(flushing_page_bitmap_.set_bitmap(page_id.page_index_in_block_, true))) {
      LOG_WARN("fail to set bitmap", KR(ret), KPC(this));
    }
  }
  LOG_DEBUG("insert_page_into_flushing_list_ over", KR(ret), K(is_flushing), K(is_allocated), K(page_handle), KPC(this));
  return ret;
}

int ObTmpFileBlock::update_block_flush_level_(const int64_t old_flushing_page_num)
{
  int ret = OB_SUCCESS;
  const int64_t new_flushing_page_num = flushing_page_list_.get_size();
  if (OB_UNLIKELY(new_flushing_page_num < 0 || old_flushing_page_num < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flushing page num", KR(ret), K(old_flushing_page_num), K(new_flushing_page_num), KPC(this));
  } else if (is_in_flushing_) {
    // do nothing
  } else if (old_flushing_page_num == 0) {
    if (OB_UNLIKELY(new_flushing_page_num == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid flushing page num", KR(ret), K(old_flushing_page_num), K(new_flushing_page_num), KPC(this));
    } else if (OB_UNLIKELY(flush_blk_node_.get_next() != nullptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block is in flush priority mgr", KR(ret), KPC(this));
    } else if (OB_FAIL(tmp_file_blk_mgr_->insert_block_into_flush_priority_mgr(new_flushing_page_num, *this))) {
      LOG_WARN("fail to insert block into flush priority mgr", KR(ret), K(new_flushing_page_num), KPC(this));
    }
  } else {
    if (new_flushing_page_num == 0) {
      if (OB_FAIL(tmp_file_blk_mgr_->remove_block_from_flush_priority_mgr(old_flushing_page_num, *this))) {
        LOG_WARN("fail to remove block from flush priority mgr", KR(ret), K(old_flushing_page_num), KPC(this));
      }
    } else {
      if (OB_FAIL(tmp_file_blk_mgr_->adjust_block_flush_priority(old_flushing_page_num, new_flushing_page_num, *this))) {
        LOG_WARN("fail to adjust block flush priority", KR(ret), K(old_flushing_page_num), K(new_flushing_page_num), KPC(this));
      }
    }
  }
  return ret;
}

// ATTENTION!
// only if the page has been locked successful, it will be put in flushing_list and set true in the bitmap.
int ObTmpFileBlock::init_flushing_page_iterator(ObTmpFileBlockFlushingPageIterator &iter,
                                                int64_t &flushing_page_num)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockPageBitmap iter_flushing_page_bitmap;
  ObIArray<ObTmpFilePageHandle> &page_arr = iter.page_arr_;
  SpinWLockGuard guard(lock_);
  bool no_flushing = false;

  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(page_arr.count() < ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(page_arr.count()));
  } else if (OB_FAIL(flushing_page_bitmap_.is_all_false(no_flushing))) {
    LOG_WARN("fail to check whether the pages are all flushed", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(no_flushing)) {
    // concurrent release_page may cause blocks become empty after we pop it from flush mgr
    LOG_INFO("tmp file block flushing bitmap is all false when flushing", KPC(this));
  } else if (!no_flushing) {
    ObTmpFileWriteCache &write_cache = MTL(ObTenantTmpFileManager*)->get_sn_file_manager().get_write_cache();
    PageNode *node = flushing_page_list_.get_first();
    while (OB_SUCC(ret) && OB_NOT_NULL(node) && node != flushing_page_list_.get_header()) {
      PageNode *next = node->get_next();
      ObTmpFilePage &page = node->page_;
      int64_t fd = page.get_page_key().fd_;
      int tmp_ret = OB_SUCCESS;
      bool succ = false;
      if (page.is_full()) {
        succ = true;
      } else if ((!page.is_full() || PageType::META == page.get_page_key().type_)
                && write_cache.is_memory_sufficient_()) {
        // incomplete pages and meta pages will only be flushed when write cache memory
        // becomes insufficient, ensuring optimal cache utilization and preventing premature flushing.
        succ = false;
        if (TC_REACH_COUNT_INTERVAL(1000)) {
          LOG_INFO("skip flush special page when write cache memory is sufficient", K(page));
        }
      } else if (OB_SUCCESS == (tmp_ret = write_cache.try_exclusive_lock(fd))) {
        // release lock after flushing over. if one file contains multiple
        // incomplete pages, only the first incomplete page can be flushed now
        succ = true;
        LOG_DEBUG("get bucket lock for incomplete page", K(page));
      } else if (OB_EAGAIN == tmp_ret) {
        succ = false;
        write_cache.metrics_.record_skip_incomplete_page(1);
        LOG_DEBUG("skip bucket lock for incomplete page", KR(ret), K(page));
      } else {
        LOG_ERROR("fail to try exclusive lock", KR(ret), K(page), KPC(this));
      }

      if (succ) {
        const int64_t page_index = page.get_page_id().page_index_in_block_;
        ObTmpFilePageHandle &page_handle = page_arr.at(page_index);
        if (OB_FAIL(iter_flushing_page_bitmap.set_bitmap(page_index, true))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(page)); // move flushing page from block to flushing_list
        } else if (OB_FAIL(page_handle.init(&page))) {
          LOG_WARN("fail to init page handle", KR(ret), K(page_handle), K(page));
        } else if (OB_FAIL(flushing_page_bitmap_.set_bitmap(page_index, false))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(page));
        } else if (OB_ISNULL(flushing_page_list_.remove(node))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("fail to remove page from list", KR(ret), K(page));
        } else {
          page.dec_ref();
          flushing_page_num += 1;
        }
      }
      node = next;
    }
  }

  if (OB_SUCC(ret) && flushing_page_num > 0) {
    if (OB_FAIL(iter.flushing_bitmap_iter_.init(iter_flushing_page_bitmap, 0, ObTmpFileGlobal::BLOCK_PAGE_NUMS - 1))) {
      LOG_WARN("fail to init flushing bitmap iterator", KR(ret), KPC(this));
    }
  }

  LOG_DEBUG("acquire flushing status over", KR(ret), K(flushing_page_num), K(iter_flushing_page_bitmap), KPC(this));
  return ret;
}

// if 'is_in_flushing_' = true, the block will be reinserted into flush priority mgr when the flush is over.
// each time the block could only be flushed once.
// otherwise, when iterate flush priority lists, the order might be wrong due to the position of block in the list
// might be modified by other operations.
int ObTmpFileBlock::set_flushing_status(bool &lock_succ)
{
  int ret = OB_SUCCESS;
  lock_succ = lock_.try_wrlock();
  if (lock_succ) {
    if (OB_UNLIKELY(!is_valid_without_lock())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
    } else if (OB_UNLIKELY(is_in_flushing_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tmp file block is already in flushing", KR(ret), KPC(this));
    } else {
      is_in_flushing_ = true;
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(lock_.unlock())) {
      LOG_ERROR("fail to unlock", KR(tmp_ret), KPC(this));
    }
    ret = ret == OB_SUCCESS ? tmp_ret : ret;
  }

  LOG_DEBUG("set flushing status over", KR(ret), K(lock_succ), KPC(this));
  return ret;
}

int ObTmpFileBlock::flush_pages_succ(const int64_t begin_page_id, const int64_t page_num)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(begin_page_id < 0 || page_num <= 0 || begin_page_id + page_num > ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(begin_page_id), K(page_num), KPC(this));
  } else if (OB_FAIL(flushed_page_bitmap_.set_bitmap_batch(
                     begin_page_id, page_num, true))) {
    LOG_WARN("fail to set bitmap", KR(ret), K(begin_page_id), K(page_num), KPC(this));
  } else if (reinsert_into_flush_prio_mgr_()) {
    LOG_DEBUG("reinsert into flush prio mgr", K(begin_page_id), K(page_num), KPC(this));
  }

  LOG_DEBUG("flush pages succ", KR(ret), K(begin_page_id), K(page_num), KPC(this));
  return ret;
}

int ObTmpFileBlock::is_page_flushed(const ObTmpFilePageId &page_id, bool &is_flushed) const
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  bool value = false;

  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(page_id.block_index_ != block_index_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else if (OB_FAIL(flushed_page_bitmap_.get_value(page_id.page_index_in_block_, value))) {
    LOG_WARN("fail to get value from bitmap", KR(ret), K(page_id), KPC(this));
  } else {
    is_flushed = value;
  }

  return ret;
}

bool ObTmpFileBlock::is_valid() const
{
  SpinRLockGuard guard(lock_);
  bool b_ret = is_valid_without_lock();

  return b_ret;
}

bool ObTmpFileBlock::is_deleting() const
{
  SpinRLockGuard guard(lock_);
  return is_in_deleting_;
}

bool ObTmpFileBlock::is_valid_without_lock() const
{
  int ret = OB_SUCCESS;
  bool b_ret = false;
  bool not_flushed = false;
  if (OB_FAIL(flushed_page_bitmap_.is_all_false(not_flushed))) {
    LOG_WARN("fail to check whether the pages are all flushed", KR(ret), KPC(this));
  } else {
    b_ret = block_index_ != ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX &&
            type_ != BlockType::INVALID &&
            tmp_file_blk_mgr_ != nullptr &&
            (not_flushed ? true : macro_block_id_.is_valid());
  }

  return b_ret;
}

int ObTmpFileBlock::get_disk_usage(int64_t &flushed_page_num) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  bool not_flushed = false;
  flushed_page_num = 0;
  if (OB_UNLIKELY(!is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_FAIL(flushed_page_bitmap_.is_all_false(not_flushed))) {
    LOG_WARN("fail to check whether the pages are all flushed", KR(ret), KPC(this));
  } else if (!macro_block_id_.is_valid()) {
    // flushed_page_num = 0;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ObTmpFileBlockPageBitmap::get_capacity(); ++i) {
      bool value = false;
      if (OB_FAIL(flushed_page_bitmap_.get_value(i, value))) {
        LOG_WARN("fail to get value from bitmap", KR(ret), K(i), KPC(this));
      } else if (value) {
        flushed_page_num++;
      }
    } // end for
  }
  return ret;
}

//-----------------------ObTmpFileBlockHandle-----------------------

ObTmpFileBlockHandle::ObTmpFileBlockHandle(ObTmpFileBlock *block)
  : ptr_(nullptr)
{
  if (nullptr != block) {
    ptr_ = block;
    ptr_->inc_ref_cnt();
  }
}

ObTmpFileBlockHandle::ObTmpFileBlockHandle(const ObTmpFileBlockHandle &handle)
  : ptr_(nullptr)
{
  operator=(handle);
}

ObTmpFileBlockHandle & ObTmpFileBlockHandle::operator=(const ObTmpFileBlockHandle &other)
{
  if (other.ptr_ != ptr_) {
    reset();
    ptr_ = other.ptr_;
    if (is_inited()) {
      ptr_->inc_ref_cnt();
    }
  }
  return *this;
}

void ObTmpFileBlockHandle::reset()
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    int64_t cur_ref_cnt = ptr_->dec_ref_cnt();
    if (0 == cur_ref_cnt) {
      ObTmpFileBlockManager *tmp_file_blk_mgr = ptr_->get_tmp_file_blk_mgr();
      if (OB_ISNULL(tmp_file_blk_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tmp file block mgr is null", KR(ret), KPC(ptr_));
      } else {
        ObIAllocator &allocator = tmp_file_blk_mgr->get_block_allocator();
        ptr_->~ObTmpFileBlock();
        allocator.free(ptr_);
      }
    } else if (cur_ref_cnt < 0) {
      LOG_ERROR("invalid ref cnt", KR(ret), KPC(ptr_));
    }
    ptr_ = nullptr;
  }
}

int ObTmpFileBlockHandle::init(ObTmpFileBlock *block)
{
  int ret = OB_SUCCESS;

  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(ptr_));
  } else if (OB_ISNULL(block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(block));
  } else {
    block->inc_ref_cnt();
    ptr_ = block;
  }

  return ret;
}

//-----------------------ObTmpFileBlockPageBitmapIterator-----------------------
int ObTmpFileBlockPageBitmapIterator::init(const ObTmpFileBlockPageBitmap &bitmap,
                                           const int64_t start_idx, int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(start_idx < 0 || end_idx < 0 || start_idx > end_idx ||
                         end_idx >= ObTmpFileBlockPageBitmap::get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(bitmap), K(start_idx), K(end_idx));
  } else {
    is_inited_ = true;
    bitmap_ = bitmap;
    cur_idx_ = start_idx;
    end_idx_ = end_idx;
  }
  return ret;
}

void ObTmpFileBlockPageBitmapIterator::reset()
{
  is_inited_ = false;
  bitmap_.reset();
  cur_idx_ = OB_INVALID_INDEX;
  end_idx_ = OB_INVALID_INDEX;
}

int ObTmpFileBlockPageBitmapIterator::next_range(bool &value, int64_t &start_page_id, int64_t &end_page_id)
{
  int ret = OB_SUCCESS;
  start_page_id = OB_INVALID_INDEX;
  end_page_id = OB_INVALID_INDEX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (has_next()) {
    start_page_id = cur_idx_;
    if (OB_FAIL(bitmap_.get_value(cur_idx_++, value))) {
      LOG_WARN("fail to get value", KR(ret), K(cur_idx_), KPC(this));
    } else {
      while(cur_idx_ <= end_idx_ && OB_SUCC(ret)) {
        bool cur_value = false;
        if (OB_FAIL(bitmap_.get_value(cur_idx_, cur_value))) {
          LOG_WARN("fail to get value", KR(ret), K(cur_idx_), KPC(this));
        } else if (cur_value != value) {
          break;
        } else {
          cur_idx_ += 1;
        }
      }
      end_page_id = cur_idx_ - 1;
    }
  } else {
    ret = OB_ITER_END;
  }
  LOG_DEBUG("next range", KR(ret), KP(this), K(value), K(start_page_id), K(end_page_id));
  return ret;
}

//---------------------------ObTmpFileBlockFlushingPageIterator---------------------------
int ObTmpFileBlockFlushingPageIterator::init(ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;
  int64_t flushing_page_num = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(block));
  } else if (page_arr_.count() == 0 &&
             OB_FAIL(page_arr_.prepare_allocate(ObTmpFileGlobal::BLOCK_PAGE_NUMS))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  } else if (OB_FAIL(block.init_flushing_page_iterator(*this, flushing_page_num))) {
    LOG_WARN("fail to init iterator", KR(ret), K(block));
  } else if (OB_UNLIKELY(flushing_page_num == 0)) {
    // maybe all flushing pages are locked failed, we should reinsert the block into flush prio mgr
    ret = OB_ITER_END;
    LOG_INFO("no flushing page", KR(ret), K(block));
  } else if (OB_FAIL(block_handle_.init(&block))) {
    LOG_WARN("fail to init block handle", KR(ret), K(block), K(block_handle_));
  } else {
    page_arr_.set_attr(ObMemAttr(MTL_ID(), "TFBlkPFlushIter"));
    is_inited_ = true;
  }
  LOG_DEBUG("flushing page iterator init over", KR(ret), K(cur_range_start_idx_),
           K(cur_range_end_idx_), K(cur_idx_), K(flushing_page_num),
           K(flushing_bitmap_iter_), K(block), KP(this));
  return ret;
}

void ObTmpFileBlockFlushingPageIterator::reset()
{
  if (IS_INIT) {
    ObTmpFileWriteCache &write_cache = MTL(ObTenantTmpFileManager*)->get_sn_file_manager().get_write_cache();
    ObSEArray<ObTmpFilePageHandle, ObTmpFileGlobal::BLOCK_PAGE_NUMS> flushing_page_arr;
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < ObTmpFileGlobal::BLOCK_PAGE_NUMS && OB_SUCC(ret); i++) {
      if (i >= cur_idx_ && page_arr_[i].is_valid()) {
        ObTmpFilePage *page = page_arr_[i].get_page();
        if (OB_ISNULL(page)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("page is null", KR(ret), K(i), K(block_handle_), KP(this));
        } else if (!page->is_full() && OB_FAIL(write_cache.unlock(page->get_page_key().fd_))) {
          LOG_ERROR("fail to unlock page", KR(ret), KPC(page), K(block_handle_));
        } else if (OB_FAIL(flushing_page_arr.push_back(page_arr_[i]))) {
          LOG_WARN("fail to push back", KR(ret), K(block_handle_), K(flushing_page_arr), K(page_arr_[i]));
        }
      }
      page_arr_[i].reset();
    }
    if (OB_SUCC(ret) && flushing_page_arr.count() > 0) {
      if (OB_FAIL(block_handle_.get()->insert_pages_into_flushing_list(flushing_page_arr))) {
        LOG_ERROR("fail to insert into flushing list", KR(ret), K(block_handle_), K(flushing_page_arr));
      }
    }
    block_handle_.reset();
    flushing_bitmap_iter_.reset();
    cur_range_start_idx_ = -1;
    cur_range_end_idx_ = -1;
    cur_idx_ = 0;
    is_inited_ = false;
    LOG_DEBUG("flushing page iterator reset over", KR(ret), K(flushing_page_arr.count()), KP(this));
  }
}

void ObTmpFileBlockFlushingPageIterator::destroy()
{
  reset();
  page_arr_.reset();
}

int ObTmpFileBlockFlushingPageIterator::next_range()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    bool is_flushing = false;
    int64_t start_idx = 0;
    int64_t end_idx = 0;
    while (OB_SUCC(ret) && !is_flushing) {
      if (OB_FAIL(flushing_bitmap_iter_.next_range(is_flushing, start_idx, end_idx))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("fail to get next range", KR(ret), K(block_handle_), K(flushing_bitmap_iter_));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(start_idx > end_idx || end_idx < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid range", KR(ret), K(start_idx), K(end_idx));
    } else {
      cur_range_start_idx_ = start_idx;
      cur_range_end_idx_ = end_idx;
    }
  }
  LOG_DEBUG("flushing page iterator get next range over", KR(ret), KPC(this));
  return ret;
}

int ObTmpFileBlockFlushingPageIterator::next_page_in_range(ObTmpFilePageHandle& page_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(cur_range_start_idx_ > cur_range_end_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid range", KR(ret), K(cur_range_start_idx_), K(cur_range_end_idx_));
  } else if (cur_idx_ > cur_range_end_idx_) {
    ret = OB_ITER_END;
  } else if (cur_idx_ < cur_range_start_idx_) {
    page_handle = page_arr_[cur_range_start_idx_];
    cur_idx_ = cur_range_start_idx_ + 1;
  } else {
    page_handle = page_arr_[cur_idx_];
    cur_idx_ += 1;
  }
  LOG_DEBUG("flushing page iterator get next page over", KR(ret), KPC(this));
  return ret;
}

} // end namespace tmp_file
} // end namespace oceanbase
