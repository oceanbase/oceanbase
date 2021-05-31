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

#include "storage/ob_all_micro_block_range_iterator.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;

namespace storage {

ObAllMicroBlockRangeIterator::ObAllMicroBlockRangeIterator()
    : range_(nullptr),
      sstable_(nullptr),
      macro_block_cnt_(0),
      cur_macro_block_cursor_(-1),
      prefetch_macro_block_cursor_(-1),
      cur_micro_block_cursor_(-1),
      is_reverse_scan_(false),
      is_inited_(false),
      file_handle_()
{}

ObAllMicroBlockRangeIterator::~ObAllMicroBlockRangeIterator()
{}

int ObAllMicroBlockRangeIterator::init(
    const uint64_t index_id, const ObTableHandle& base_store, const ObExtStoreRange& range, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  const ObSSTable* sstable = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObAllMicroBlockRangeIterator inited twice", K(ret));
  } else if (OB_ISNULL(base_store.get_table())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "base store must not null", K(ret));
  } else if (OB_FAIL(base_store.get_sstable(sstable))) {
    STORAGE_LOG(WARN, "failed to get base ssstore");
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "store is null or store is not major ssstore", K(ret), KP(sstable));
  } else if (OB_FAIL(file_handle_.assign(sstable->get_storage_file_handle()))) {
    STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(sstable->get_storage_file_handle()));
  } else if (OB_FAIL(const_cast<ObExtStoreRange&>(range).to_collation_free_range_on_demand_and_cutoff_range(
                 allocators_[0]))) {
    STORAGE_LOG(WARN, "failed to get collation free range", K(ret));
  } else if (OB_FAIL(
                 const_cast<ObSSTable*>(sstable)->scan_macro_block(range, macro_block_iterator_, is_reverse_scan))) {
    STORAGE_LOG(WARN, "failed to scan macro block", K(index_id), K(range), K(ret));
  } else if (OB_FAIL(macro_block_iterator_.get_macro_block_count(macro_block_cnt_))) {
    STORAGE_LOG(WARN, "failed to get macro block cnt", K(ret));
  } else if (OB_FAIL(prefetch_block_index())) {
    STORAGE_LOG(WARN, "failed to prefetch_block_index", K(ret), K_(prefetch_macro_block_cursor), K_(macro_block_cnt));
  } else {
    range_ = &range;
    is_reverse_scan_ = is_reverse_scan;
    sstable_ = const_cast<ObSSTable*>(sstable);
    is_inited_ = true;
  }
  return ret;
}

void ObAllMicroBlockRangeIterator::reset()
{
  if (is_inited_) {
    macro_block_iterator_.reset();
    index_reader_.reset();
    for (int64_t i = 0; i < HANDLE_CNT; ++i) {
      macro_handles_[i].reset();
      macro_block_start_keys_[i].reset();
      allocators_[i].reset();
    }
    end_keys_.reset();
    micro_range_.reset();
    range_ = nullptr;
    macro_block_cnt_ = 0;
    cur_macro_block_cursor_ = -1;
    prefetch_macro_block_cursor_ = -1;
    cur_micro_block_cursor_ = -1;
    is_reverse_scan_ = false;
    is_inited_ = false;
    file_handle_.reset();
  }
}

int ObAllMicroBlockRangeIterator::get_next_range(const ObStoreRange*& range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObAllMicroBlockRangeIterator is not inited", K(ret));
  } else {
    range = nullptr;
    while (OB_SUCC(ret) && (nullptr == range)) {
      if (OB_FAIL(advance_micro_block_cursor())) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to advance_micro_block_cursor", K(ret));
        } else if (OB_FAIL(open_next_macro_block())) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "failed to open next macro block", K(ret));
          }
        }
      } else if (OB_FAIL(get_cur_micro_range())) {
        STORAGE_LOG(WARN, "failed to get cur micro range", K(ret));
      } else {
        range = &micro_range_;
      }
    }
  }
  return ret;
}

int ObAllMicroBlockRangeIterator::open_next_macro_block()
{
  int ret = OB_SUCCESS;
  if (cur_macro_block_cursor_ + 1 >= macro_block_cnt_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(prefetch_block_index())) {
    STORAGE_LOG(WARN, "failed to prefetch block index", K(ret));
  } else {
    ++cur_macro_block_cursor_;
    const int64_t handle_idx = cur_macro_block_cursor_ % HANDLE_CNT;
    ObFullMacroBlockMeta full_meta;
    ObMacroBlockHandle& macro_handle = macro_handles_[handle_idx];
    ObArenaAllocator& allocator = allocators_[handle_idx];
    const char* index_buf = nullptr;
    index_reader_.reset();
    end_keys_.reuse();
    const int64_t io_timeout_ms = max(THIS_WORKER.get_timeout_remain() / 1000, 0);
    if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
      STORAGE_LOG(WARN, "failed to wait io handle", K(ret));
    } else if (OB_ISNULL(index_buf = macro_handle.get_buffer())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "return io buffer is null", K(ret));
    } else if (OB_FAIL(sstable_->get_meta(macro_handle.get_macro_id(), full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(macro_handle.get_macro_id()));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, meta must not be null", K(ret), K(full_meta));
    } else if (OB_FAIL(index_reader_.init(full_meta, index_buf))) {
      STORAGE_LOG(WARN, "failed to set index transformer buffer", K(ret));
    } else if (OB_FAIL(get_end_keys(allocator))) {
      STORAGE_LOG(WARN, "failed to fetch_end_keys", K(ret));
    } else {
      reset_micro_block_cursor();
    }
  }
  return ret;
}

int ObAllMicroBlockRangeIterator::prefetch_block_index()
{
  int ret = OB_SUCCESS;
  if (prefetch_macro_block_cursor_ + 1 < macro_block_cnt_) {
    ++prefetch_macro_block_cursor_;
    const int64_t handle_idx = prefetch_macro_block_cursor_ % HANDLE_CNT;
    ObMacroBlockHandle& macro_handle = macro_handles_[handle_idx];
    ObStoreRowkey& macro_block_start_key = macro_block_start_keys_[handle_idx];
    ObArenaAllocator& allocator = allocators_[handle_idx];
    ObFullMacroBlockMeta full_meta;
    ObMacroBlockDesc macro_desc;
    ObStorageFile* file = NULL;

    allocator.reuse();
    if (OB_FAIL(macro_block_iterator_.get_next_macro_block(macro_desc))) {
      STORAGE_LOG(WARN, "failed to get next macro block id", K(ret));
    } else if (OB_FAIL(macro_desc.range_.get_start_key().deep_copy(macro_block_start_key, allocator))) {
      STORAGE_LOG(WARN, "failed to deep copy macro block start key", K(ret));
    } else if (OB_FAIL(macro_desc.macro_block_ctx_.sstable_->get_meta(
                   macro_desc.macro_block_ctx_.get_macro_block_id(), full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro meta is invalid", K(ret), "block_ctx", macro_desc.macro_block_ctx_);
    } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle_));
    } else {
      macro_handle.set_file(file);
      const ObMacroBlockMetaV2* macro_meta = full_meta.meta_;
      ObMacroBlockReadInfo read_info;
      read_info.macro_block_ctx_ = &macro_desc.macro_block_ctx_;
      read_info.offset_ = macro_meta->micro_block_index_offset_;
      read_info.size_ = macro_meta->get_block_index_size();
      read_info.io_desc_.category_ = USER_IO;
      read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_INDEX_READ;
      if (OB_FAIL(file->async_read_block(read_info, macro_handle))) {
        STORAGE_LOG(WARN, "failed to async read block index", K(ret), K(read_info));
      }
    }
  }
  return ret;
}

int ObAllMicroBlockRangeIterator::get_end_keys(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (0 == cur_macro_block_cursor_ || macro_block_cnt_ - 1 == cur_macro_block_cursor_) {
    if (OB_FAIL(index_reader_.get_end_keys(range_->get_range(), allocator, end_keys_))) {
      STORAGE_LOG(WARN, "failed to get end keys from index reader", K(ret));
    }
  } else if (OB_FAIL(index_reader_.get_all_end_keys(allocator, end_keys_))) {
    STORAGE_LOG(WARN, "failed to get all end keys from index reader", K(ret));
  }
  return ret;
}

int ObAllMicroBlockRangeIterator::get_cur_micro_range()
{
  int ret = OB_SUCCESS;
  micro_range_.set_left_open();
  micro_range_.set_right_closed();
  micro_range_.set_end_key(end_keys_[cur_micro_block_cursor_]);
  // set start key
  if (0 == cur_micro_block_cursor_) {
    // set border start key
    if (is_first_macro_block()) {
      micro_range_.set_start_key(range_->get_range().get_start_key());
      if (range_->get_range().get_border_flag().inclusive_start()) {
        micro_range_.set_left_closed();
      }
    } else {
      micro_range_.set_start_key(macro_block_start_keys_[cur_macro_block_cursor_ % HANDLE_CNT]);
    }
  } else {
    micro_range_.set_start_key(end_keys_[cur_micro_block_cursor_ - 1]);
  }

  // set end key
  if (end_keys_.count() - 1 == cur_micro_block_cursor_) {
    // set border end key
    if (is_last_macro_block()) {
      micro_range_.set_end_key(range_->get_range().get_end_key());
      if (!range_->get_range().get_border_flag().inclusive_end()) {
        micro_range_.set_right_open();
      }
    }
  }
  return ret;
}

OB_INLINE bool ObAllMicroBlockRangeIterator::is_end_of_macro_block() const
{
  bool bret = true;
  if (end_keys_.count() > 0) {
    if (!is_reverse_scan_) {
      bret = cur_micro_block_cursor_ >= end_keys_.count();
    } else {
      bret = cur_micro_block_cursor_ < 0;
    }
  }
  return bret;
}

OB_INLINE int ObAllMicroBlockRangeIterator::advance_micro_block_cursor()
{
  int ret = OB_SUCCESS;
  if (!is_reverse_scan_) {
    ++cur_micro_block_cursor_;
  } else {
    --cur_micro_block_cursor_;
  }
  if (is_end_of_macro_block()) {
    ret = OB_ITER_END;
  }
  return ret;
}

OB_INLINE void ObAllMicroBlockRangeIterator::reset_micro_block_cursor()
{
  if (!is_reverse_scan_) {
    cur_micro_block_cursor_ = -1;
  } else {
    cur_micro_block_cursor_ = end_keys_.count();
  }
}

OB_INLINE bool ObAllMicroBlockRangeIterator::is_first_macro_block() const
{
  bool bret = false;
  if (!is_reverse_scan_) {
    bret = (0 == cur_macro_block_cursor_);
  } else {
    bret = (macro_block_cnt_ - 1 == cur_macro_block_cursor_);
  }
  return bret;
}

OB_INLINE bool ObAllMicroBlockRangeIterator::is_last_macro_block() const
{
  bool bret = false;
  if (!is_reverse_scan_) {
    bret = (macro_block_cnt_ - 1 == cur_macro_block_cursor_);
  } else {
    bret = (0 == cur_macro_block_cursor_);
  }
  return bret;
}

}  // namespace storage
}  // namespace oceanbase
