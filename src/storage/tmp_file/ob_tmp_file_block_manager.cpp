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

#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_cache.h"
namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{
//-----------------------ObTmpFileBlockPageBitmapIterator-----------------------
int ObTmpFileBlockPageBitmapIterator::init(const ObTmpFileBlockPageBitmap *bitmap,
                                           const int64_t start_idx, int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(bitmap) ||
             OB_UNLIKELY(start_idx < 0 || end_idx < 0 || start_idx > end_idx ||
                         end_idx >= ObTmpFileBlockPageBitmap::get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(bitmap), K(start_idx), K(end_idx));
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
  bitmap_ = nullptr;
  is_inited_ = false;
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
    LOG_WARN("not init", KR(ret));
  } else if (has_next()) {
    start_page_id = cur_idx_;
    if (OB_FAIL(bitmap_->get_value(cur_idx_++, value))) {
      LOG_WARN("fail to get value", KR(ret), K(cur_idx_));
    } else {
      while(cur_idx_ <= end_idx_ && OB_SUCC(ret)) {
        bool cur_value = false;
        if (OB_FAIL(bitmap_->get_value(cur_idx_, cur_value))) {
          LOG_WARN("fail to get value", KR(ret), K(cur_idx_));
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
    LOG_WARN("iter end", KR(ret));
  }
  return ret;
}

//---------------------------ObTmpFileBlockPageBitmap---------------------------
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
  for (int i = 0; i < PAGE_BYTE_CAPACITY / sizeof(uint64_t); ++i) {
    int64_t offset = i * sizeof(uint64_t);
    if (i > 0) {
      J_COMMA();
    }
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
  reset();
}

int ObTmpFileBlock::reset()
{
  int ret = OB_SUCCESS;
  if (is_valid() && BlockState::ON_DISK == block_state_ && OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_block_id_))) {
    LOG_ERROR("failed to dec macro block ref cnt", KR(ret), KPC(this));
  } else {
    block_state_ = BlockState::INVALID;
    page_bitmap_.reset();
    block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
    macro_block_id_.reset();
    ref_cnt_ = 0;
  }
  return ret;
}

int ObTmpFileBlock::init_block(const int64_t block_index,
                               const int64_t begin_page_id, const int64_t page_num)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  bool use_released_pages = false;

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX == block_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block_index", KR(ret), K(block_index), K(begin_page_id), K(page_num));
  } else if (OB_UNLIKELY(page_num <= 0 || begin_page_id < 0 ||
             begin_page_id + page_num > ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_index), K(begin_page_id), K(page_num));
  } else if (OB_UNLIKELY(block_index_ != ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block has been inited", KR(ret), K(block_index_));
  } else if (block_state_ != BlockState::INVALID) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("attempt to reinit a block not in invalid state", KR(ret), KPC(this));
  } else if (OB_FAIL(page_bitmap_.is_all_false(begin_page_id, begin_page_id + page_num - 1, use_released_pages))) {
    LOG_WARN("fail to check whether the pages are all false", KR(ret), K(begin_page_id), K(page_num), K(page_bitmap_));
  } else if (OB_UNLIKELY(!use_released_pages)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to use a allocated page", KR(ret), K(block_index), K(begin_page_id), K(page_num),
                                                K(page_bitmap_), K(use_released_pages));
  } else if (OB_FAIL(page_bitmap_.set_bitmap_batch(begin_page_id, page_num, true))) {
    LOG_WARN("fail to set bitmap", KR(ret), K(begin_page_id), K(page_num), K(page_bitmap_));
  } else {
    block_index_ = block_index;
    block_state_ = BlockState::IN_MEMORY;
    LOG_DEBUG("init block successfully", K(block_index), K(begin_page_id), K(page_num), KPC(this));
  }
  return ret;
}

int ObTmpFileBlock::write_back_start()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (block_state_ != BlockState::IN_MEMORY && block_state_ != BlockState::WRITE_BACK) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("write back a block not in in_memory state and not in write back state",
        KR(ret), KPC(this));
  } else {
    block_state_ = BlockState::WRITE_BACK;
    LOG_DEBUG("switch tmp file block to write back state", KPC(this));
  }
  return ret;
}

int ObTmpFileBlock::write_back_failed()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (block_state_ != BlockState::WRITE_BACK) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("notify a block write_back_failed, but the block not in write back state",
        KR(ret), KPC(this));
  } else {
    block_state_ = BlockState::IN_MEMORY;
    LOG_DEBUG("switch tmp file block to in memory state", KPC(this));
  }
  return ret;
}

int ObTmpFileBlock::write_back_succ(blocksstable::MacroBlockId macro_block_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!macro_block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_block_id));
  } else if (block_state_ != BlockState::WRITE_BACK) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("attempt to set macro_block_id for a block not in write_back state", KR(ret), KPC(this));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_block_id))) {
    LOG_ERROR("failed to inc macro block ref cnt", KR(ret), K(macro_block_id), KPC(this));
  } else {
    macro_block_id_ = macro_block_id;
    block_state_ = BlockState::ON_DISK;
    LOG_DEBUG("switch tmp file block to on disk state", KPC(this));
  }
  return ret;
}

int ObTmpFileBlock::release_pages(const int64_t begin_page_id, const int64_t page_num)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  bool release_allocated_page = false;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(page_num <= 0 || begin_page_id < 0 ||
             begin_page_id + page_num > ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_index_), K(begin_page_id), K(page_num));
  } else if (OB_FAIL(page_bitmap_.is_all_true(begin_page_id, begin_page_id + page_num - 1, release_allocated_page))) {
    LOG_WARN("fail to check whether the pages are all true", KR(ret), K(block_index_), K(macro_block_id_),
                                                             K(begin_page_id), K(page_num), K(page_bitmap_));
  } else if (OB_UNLIKELY(!release_allocated_page)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to release a released page", KR(ret), K(block_index_), K(macro_block_id_),
                                                   K(begin_page_id), K(page_num),
                                                   K(page_bitmap_), K(release_allocated_page));
  } else if (OB_FAIL(page_bitmap_.set_bitmap_batch(begin_page_id, page_num, false))) {
    LOG_WARN("fail to set bitmap", KR(ret), K(begin_page_id), K(page_num), K(page_bitmap_));
  }
  return ret;
}

int ObTmpFileBlock::get_page_usage(int64_t &page_num) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  page_num = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ObTmpFileBlockPageBitmap::get_capacity(); ++i) {
      bool value = false;
      if (OB_FAIL(page_bitmap_.get_value(i, value))) {
        LOG_WARN("fail to get value from bitmap", KR(ret), K(i), KPC(this));
      } else if (value) {
        page_num++;
      }
    } // end for
  }
  return ret;
}

int ObTmpFileBlock::inc_ref_cnt()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else {
    ref_cnt_++;
  }

  return ret;
}

int ObTmpFileBlock::dec_ref_cnt(int64_t &ref_cnt)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(ref_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ref cnt", KR(ret), KPC(this));
  } else {
    ref_cnt_ -= 1;
    ref_cnt = ref_cnt_;
  }

  return ret;
}

bool ObTmpFileBlock::on_disk() const
{
  return BlockState::ON_DISK == block_state_;
}

int ObTmpFileBlock::can_remove(bool &can_remove) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);

  can_remove = false;
  bool is_all_page_released = false;
  if (OB_FAIL(page_bitmap_.is_all_false(is_all_page_released))) {
    LOG_WARN("fail to check whether the pages are all false", KR(ret), KPC(this));
  } else {
    can_remove = is_all_page_released && BlockState::WRITE_BACK != block_state_;
  }
  return ret;
}

blocksstable::MacroBlockId ObTmpFileBlock::get_macro_block_id() const
{
  SpinRLockGuard guard(lock_);
  return macro_block_id_;
}

int64_t ObTmpFileBlock::get_block_index() const
{
  SpinRLockGuard guard(lock_);
  return block_index_;
}
//-----------------------ObTmpFileBlockHandle-----------------------

ObTmpFileBlockHandle::ObTmpFileBlockHandle(ObTmpFileBlock *block, ObTmpFileBlockManager *tmp_file_blk_mgr)
  : ptr_(nullptr), tmp_file_blk_mgr_(nullptr)
{
  if (nullptr != block && nullptr != tmp_file_blk_mgr) {
    ptr_ = block;
    tmp_file_blk_mgr_ = tmp_file_blk_mgr;
    ptr_->inc_ref_cnt();
  }
}

ObTmpFileBlockHandle::ObTmpFileBlockHandle(const ObTmpFileBlockHandle &handle)
  : ptr_(nullptr), tmp_file_blk_mgr_(nullptr)
{
  operator=(handle);
}

ObTmpFileBlockHandle & ObTmpFileBlockHandle::operator=(const ObTmpFileBlockHandle &other)
{
  if (other.ptr_ != ptr_) {
    reset();
    ptr_ = other.ptr_;
    tmp_file_blk_mgr_ = other.tmp_file_blk_mgr_;
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
    int64_t cur_ref_cnt = -1;
    int64_t block_index = ptr_->get_block_index();
    if (OB_FAIL(ptr_->dec_ref_cnt(cur_ref_cnt))) {
      LOG_ERROR("fail to dec block ref cnt", KR(ret), KPC(ptr_));
    } else if (0 == cur_ref_cnt) {
      ptr_->reset();
      tmp_file_blk_mgr_->get_block_allocator().free(ptr_);
    }
    ptr_ = nullptr;
  }
}

int ObTmpFileBlockHandle::init(ObTmpFileBlock *block, ObTmpFileBlockManager *tmp_file_blk_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(ptr_));
  } else if (OB_ISNULL(block) || OB_ISNULL(tmp_file_blk_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(block), KP(tmp_file_blk_mgr));
  } else if (OB_FAIL(block->inc_ref_cnt())) {
    LOG_WARN("fail to inc block ref cnt", KR(ret), KPC(block));
  } else {
    ptr_ = block;
    tmp_file_blk_mgr_ = tmp_file_blk_mgr;
  }

  return ret;
}

//-----------------------ObTmpFileBlockManager-----------------------
ObTmpFileBlockManager::ObTmpFileBlockManager() :
                             is_inited_(false),
                             tenant_id_(OB_INVALID_TENANT_ID),
                             used_page_num_(0),
                             physical_block_num_(0),
                             block_index_generator_(0),
                             block_map_(),
                             block_allocator_(),
                             stat_lock_()
                             {}

ObTmpFileBlockManager::~ObTmpFileBlockManager()
{
  destroy();
}

void ObTmpFileBlockManager::destroy()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  used_page_num_ = 0;
  physical_block_num_ = 0;
  block_index_generator_ = 0;
  block_map_.destroy();
  block_allocator_.reset();
}

int ObTmpFileBlockManager::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileBlockManager init twice", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id));
  } else if (OB_FAIL(block_map_.init("TmpFileBlkMgr", tenant_id))) {
    LOG_WARN("fail to init tenant temporary file block manager", KR(ret), K(tenant_id));
  } else if (OB_FAIL(block_allocator_.init(common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                           ObModIds::OB_TMP_BLOCK_MANAGER, tenant_id_, INT64_MAX))) {
    LOG_WARN("fail to init temporary file block allocator", KR(ret), K(tenant_id_));
  } else {
    block_allocator_.set_attr(ObMemAttr(tenant_id, "TmpFileBlk"));
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }

  return ret;
}

int ObTmpFileBlockManager::create_tmp_file_block(const int64_t begin_page_id, const int64_t page_num,
                                                 int64_t &block_index)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  ObTmpFileBlock* blk = nullptr;
  const uint64_t blk_size = sizeof(ObTmpFileBlock);
  void *buf = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(buf = block_allocator_.alloc(blk_size, lib::ObMemAttr(tenant_id_, "TmpFileBlk")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for tmp file block", KR(ret), K(blk_size));
  } else if (FALSE_IT(blk = new (buf) ObTmpFileBlock())) {
  } else if (FALSE_IT(block_index = ATOMIC_AAF(&block_index_generator_, 1))) {
  } else if (OB_FAIL(blk->init_block(block_index, begin_page_id, page_num))) {
    LOG_WARN("fail to init tmp file block", KR(ret), K(block_index));
  } else if (OB_FAIL(handle.init(blk, this))) {
    LOG_WARN("fail to init tmp file block handle", KR(ret), K(block_index));
  } else {
    if (OB_FAIL(block_map_.insert(ObTmpFileBlockKey(block_index), handle))) {
      LOG_WARN("fail to insert tmp file block into map", KR(ret), K(block_index));
    }
    LOG_DEBUG("create tmp file block succ", KR(ret), K(block_index));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(blk)) {
    blk->~ObTmpFileBlock();
    block_allocator_.free(blk);
    blk = nullptr;
  }

  return ret;
}

int ObTmpFileBlockManager::write_back_start(const int64_t block_index)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  ObTmpFileBlock *blk = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (OB_ISNULL(blk = handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is null", KR(ret), K(block_index));
  } else if (OB_FAIL(blk->write_back_start())) {
    LOG_WARN("fail to write back start", KR(ret), K(block_index));
  }

  return ret;
}

int ObTmpFileBlockManager::write_back_failed(const int64_t block_index)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  ObTmpFileBlock *blk = nullptr;
  bool can_remove = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (OB_ISNULL(blk = handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is null", KR(ret), K(block_index));
  } else if (OB_FAIL(blk->write_back_failed())) {
    LOG_WARN("fail to notify block write_back_failed", KR(ret), K(handle));
  }

  if (FAILEDx(handle.get()->can_remove(can_remove))) {
    LOG_WARN("check block can remove failed", KR(ret), K(handle));
  } else if (can_remove) {
    if (OB_FAIL(remove_tmp_file_block_(block_index))) {
      LOG_ERROR("fail to remove tmp file block", KR(ret), K(handle));
    }
  }
  return ret;
}

int ObTmpFileBlockManager::write_back_succ(const int64_t block_index, const blocksstable::MacroBlockId macro_block_id)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  ObTmpFileBlock *blk = nullptr;
  bool can_remove = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (OB_ISNULL(blk = handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is null", KR(ret), K(block_index));
  } else if (OB_FAIL(blk->write_back_succ(macro_block_id))) {
    LOG_WARN("fail to notify write_back_succ", KR(ret), K(block_index), K(macro_block_id), K(handle));
  } else {
    SpinWLockGuard guard(stat_lock_);
    int64_t used_page_num = 0;
    if (OB_FAIL(blk->get_page_usage(used_page_num))) {
      LOG_WARN("fail to get page usage", KR(ret), K(handle));
    } else {
      used_page_num_ += used_page_num;
      physical_block_num_ += 1;
    }
  }

  if (FAILEDx(handle.get()->can_remove(can_remove))) {
    LOG_WARN("check block can remove failed", KR(ret), K(handle));
  } else if (can_remove) {
    if (OB_FAIL(remove_tmp_file_block_(block_index))) {
      LOG_ERROR("fail to remove tmp file block", KR(ret), K(handle));
    }
  }
  return ret;
}

// if a block is released all pages in this function,
// when ObTmpFileBlockHandle destructed, the handle will remove itself from block_map
int ObTmpFileBlockManager::release_tmp_file_page(const int64_t block_index,
                                                 const int64_t begin_page_id, const int64_t page_num)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  bool can_remove = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be null", KR(ret), K(block_index));
  } else if (OB_FAIL(handle.get()->release_pages(begin_page_id, page_num))) {
    LOG_WARN("fail to release pages", KR(ret), K(begin_page_id), K(page_num), K(handle));
  } else if (handle.get()->on_disk()) {
    SpinWLockGuard guard(stat_lock_);
    used_page_num_ -= page_num;
  }

  LOG_DEBUG("release_tmp_file_page", KR(ret), K(block_index), K(begin_page_id), K(page_num), K(handle));

  if (FAILEDx(handle.get()->can_remove(can_remove))) {
    LOG_WARN("check block can remove failed", KR(ret), K(handle));
  } else if (can_remove) {
    if (OB_FAIL(remove_tmp_file_block_(block_index))) {
      LOG_ERROR("fail to remove tmp file block", KR(ret), K(handle));
    }
  }

  return ret;
}

// only called by ObTmpFileBlockHandle::reset()
int ObTmpFileBlockManager::remove_tmp_file_block_(const int64_t block_index)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlock* blk = nullptr;
  ObTmpFileBlockHandle handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else {
    if (OB_FAIL(block_map_.erase(ObTmpFileBlockKey(block_index), handle))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("fail to erase tmp file block", KR(ret), K(block_index));
      } else {
        LOG_DEBUG("erase tmp file block succ", KR(ret), K(block_index));
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(nullptr == (blk = handle.get()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KR(ret), K(block_index));
    } else {
      ObTmpBlockCache::get_instance().erase(ObTmpBlockCacheKey(block_index, MTL_ID()));
      LOG_DEBUG("erase tmp file block from map succ", KR(ret), K(handle));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(blk) && blk->on_disk()) {
    SpinWLockGuard guard(stat_lock_);
    int64_t used_page_num = 0;
    if (OB_FAIL(blk->get_page_usage(used_page_num))) {
      LOG_WARN("fail to get page usage", KR(ret), K(block_index), K(handle));
    } else {
      used_page_num_ -= used_page_num;
      physical_block_num_ -= 1;
    }
  }

  return ret;
}

int ObTmpFileBlockManager::get_block_usage_stat(int64_t &used_page_num, int64_t &macro_block_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else {
    SpinRLockGuard guard(stat_lock_);
    used_page_num = used_page_num_;
    macro_block_count = physical_block_num_;
  }

  return ret;
}

void ObTmpFileBlockManager::print_block_usage()
{
  int ret = OB_SUCCESS;
  int64_t used_page_num = 0;
  int64_t block_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(get_block_usage_stat(used_page_num, block_num))) {
    LOG_WARN("fail to get block usage stat", KR(ret));
  } else if (OB_UNLIKELY(0 == block_num)) {
    LOG_INFO("temporary file module use no blocks");
  } else {
    int64_t occupied_page_num = block_num * ObTmpFileGlobal::BLOCK_PAGE_NUMS;
    double disk_fragment_ratio = static_cast<double>(used_page_num) / static_cast<double>(occupied_page_num);
    LOG_INFO("the block usage for temporary files",
             K(used_page_num), K(occupied_page_num), K(block_num), K(disk_fragment_ratio));
  }
}

int ObTmpFileBlockManager::get_macro_block_count(int64_t &macro_block_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else {
    SpinRLockGuard guard(stat_lock_);
    macro_block_count = physical_block_num_;
  }
  return ret;
}

int ObTmpFileBlockManager::get_macro_block_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  CollectMacroBlockIdFunctor func(macro_id_list);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.for_each(func))) {
    LOG_WARN("fail to collect macro block ids", KR(ret));
  }

  return ret;
}

bool ObTmpFileBlockManager::CollectMacroBlockIdFunctor::operator()(const ObTmpFileBlockKey &block_index, const ObTmpFileBlockHandle &handle)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle should not be null", KR(ret), K(block_index));
  } else if (!handle.get()->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle should not be invalid", KR(ret), K(block_index), K(handle));
  } else if (!handle.get()->on_disk()) {
    // do nothing
  } else if (OB_FAIL(macro_id_list_.push_back(handle.get()->get_macro_block_id()))) {
    LOG_WARN("failed to push back", KR(ret), K(block_index), K(handle));
  }
  return OB_SUCCESS == ret;
}

int ObTmpFileBlockManager::get_macro_block_id(const int64_t block_index, blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be null", KR(ret), K(block_index));
  } else if (OB_UNLIKELY(!handle.get()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be invalid", KR(ret), K(block_index), K(handle));
  } else {
    macro_block_id = handle.get()->get_macro_block_id();
  }

  return ret;
}

int ObTmpFileBlockManager::get_tmp_file_block_handle(const int64_t block_index, ObTmpFileBlockHandle &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be null", KR(ret), K(block_index));
  } else if (OB_UNLIKELY(!handle.get()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be invalid", KR(ret), K(block_index), K(handle));
  }

  return ret;
}

} // end namespace tmp_file
} // end namespace oceanbase
