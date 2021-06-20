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

#include "ob_sstable_row_scanner.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {
/**
 * --------------------------------------------------------------ObSSTableRowScanner------------------------------------------------------------
 */
ObSSTableRowScanner::ObSSTableRowScanner()
    : has_find_macro_(false),
      prefetch_macro_idx_(0),
      macro_block_cnt_(0),
      prefetch_macro_order_(0),
      last_range_(),
      new_range_(),
      last_gap_range_idx_(-1),
      last_gap_macro_idx_(-1),
      last_gap_micro_idx_(-1),
      curr_row_(NULL)
{}

ObSSTableRowScanner::~ObSSTableRowScanner()
{}

void ObSSTableRowScanner::reset()
{
  ObSSTableRowIterator::reset();
  has_find_macro_ = false;
  prefetch_macro_idx_ = 0;
  prefetch_macro_order_ = 0;
  last_range_.reset();
  new_range_.reset();
  last_gap_range_idx_ = -1;
  last_gap_macro_idx_ = -1;
  last_gap_micro_idx_ = -1;
  curr_row_ = NULL;
}

void ObSSTableRowScanner::reuse()
{
  ObSSTableRowIterator::reuse();
  has_find_macro_ = false;
  prefetch_macro_idx_ = 0;
  prefetch_macro_order_ = 0;
  last_range_.reset();
  new_range_.reset();
  last_gap_range_idx_ = -1;
  last_gap_macro_idx_ = -1;
  last_gap_micro_idx_ = -1;
  curr_row_ = NULL;
}

int ObSSTableRowScanner::get_handle_cnt(const void* query_range, int64_t& read_handle_cnt, int64_t& micro_handle_cnt)
{
  int ret = OB_SUCCESS;
  if (NULL == query_range) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else {
    read_handle_cnt = SCAN_READ_HANDLE_CNT;
    micro_handle_cnt = SCAN_MICRO_HANDLE_CNT;
  }
  return ret;
}

int ObSSTableRowScanner::prefetch_read_handle(ObSSTableReadHandle& read_handle)
{
  return prefetch_range(0L /*prefetch range idx*/, *range_, read_handle);
}

int ObSSTableRowScanner::fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(scan_row(read_handle, store_row))) {
    curr_row_ = store_row;
  } else {
    curr_row_ = NULL;
  }
  return ret;
}

int ObSSTableRowScanner::prefetch_range(
    const int64_t range_idx, const common::ObExtStoreRange& ext_range, ObSSTableReadHandle& read_handle)
{
  int ret = OB_SUCCESS;
  read_handle.reset();

  if (!has_find_macro_) {
    // try find macro
    if (OB_FAIL(macro_block_iter_.open(*sstable_, ext_range, access_ctx_->query_flag_.is_reverse_scan()))) {
      STORAGE_LOG(WARN, "Fail to find macros, ", K(ret), K(ext_range));
    } else if (OB_FAIL(macro_block_iter_.get_macro_block_count(macro_block_cnt_))) {
      STORAGE_LOG(WARN, "Fail to get macro block count, ", K(ret), K(ext_range));
    } else {
      has_find_macro_ = true;
      prefetch_macro_idx_ = access_ctx_->query_flag_.is_reverse_scan() ? macro_block_cnt_ - 1 : 0;
      prefetch_macro_order_ = 0;
      STORAGE_LOG(DEBUG, "find macro count", K(macro_block_cnt_), KP(this));
    }
  }

  while (OB_SUCC(ret)) {
    ObSSTableSkipRangeCtx* skip_ctx = NULL;
    if (OB_FAIL(macro_block_iter_.get_next_macro_block(read_handle.macro_block_ctx_, read_handle.full_meta_))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to get next macro block, ", K(ret), K(ext_range));
      }
    } else {
      // try to filter using bloomfilter cache
      if (access_ctx_->enable_bf_cache() && 1 == macro_block_cnt_ && read_handle.full_meta_.is_valid()) {
        ObStoreRowkey common_rowkey;
        if (OB_FAIL(ext_range.get_range().get_common_store_rowkey(common_rowkey))) {
          STORAGE_LOG(WARN, "Fail to get common key, ", K(ret));
          ret = OB_SUCCESS;
        } else if (common_rowkey.is_valid()) {
          bool is_contain = true;
          if (OB_FAIL(OB_STORE_CACHE.get_bf_cache().may_contain(iter_param_->table_id_,
                  read_handle.macro_block_ctx_.get_macro_block_id(),
                  storage_file_->get_file_id(),
                  common_rowkey,
                  is_contain))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              STORAGE_LOG(WARN, "Fail to filter from bloom filter, ", K(ret));
            }
          }

          if (OB_FAIL(ret)) {
            // try read range from block
            ret = OB_SUCCESS;
          } else {
            if (!is_contain) {
              // not exist range
              ret = OB_ITER_END;
              ++access_ctx_->access_stat_.bf_filter_cnt_;
              ++table_store_stat_.bf_filter_cnt_;
            } else {
              read_handle.is_bf_contain_ = true;
            }
          }
          access_ctx_->access_stat_.rowkey_prefix_ = common_rowkey.get_obj_cnt();
          ++access_ctx_->access_stat_.bf_access_cnt_;
          ++table_store_stat_.bf_access_cnt_;
        }
      }

      if (OB_SUCC(ret)) {
        read_handle.is_get_ = false;
        read_handle.is_left_border_ = (0 == prefetch_macro_idx_);
        read_handle.is_right_border_ = (macro_block_cnt_ - 1 == prefetch_macro_idx_);
        read_handle.range_idx_ = static_cast<int32_t>(range_idx);
        read_handle.ext_range_ = &ext_range;
        read_handle.state_ = ObSSTableRowState::IN_BLOCK;
        read_handle.macro_idx_ = prefetch_macro_order_++;
        prefetch_macro_idx_ += scan_step_;
        if (OB_FAIL(get_skip_range_ctx(read_handle, INT64_MAX, skip_ctx))) {
          STORAGE_LOG(WARN, "fail to get skip range ctx", K(ret));
        } else if (OB_UNLIKELY(NULL != skip_ctx && skip_ctx->need_skip_)) {
          STORAGE_LOG(DEBUG, "skip current macro block", K(ret), K(read_handle), K(*skip_ctx), K(ext_range), KP(this));
        } else {
          STORAGE_LOG(DEBUG, "get_next_read_handle", K(read_handle), K(ext_range), KP(this));
          break;
        }
      }
    }
  }

  return ret;
}

int ObSSTableRowScanner::skip_range(int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key)
{
  int ret = OB_SUCCESS;
  ObExtStoreRange* new_range = NULL;
  if (OB_UNLIKELY(range_idx < 0 || NULL == gap_key)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(range_idx), KP(gap_key));
  } else if (OB_FAIL(generate_new_range(range_idx, *gap_key, include_gap_key, *range_, new_range))) {
    STORAGE_LOG(WARN, "fail to get skip range", K(ret));
  } else if (NULL != new_range) {
    if (OB_FAIL(skip_range_impl(range_idx, *range_, *new_range))) {
      STORAGE_LOG(WARN, "fail to skip range", K(ret));
    }
  }
  return ret;
}

int ObSSTableRowScanner::skip_range_impl(
    const int64_t range_idx, const ObExtStoreRange& org_range, const ObExtStoreRange& new_range)
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::ObMacroBlockCtx> org_macro_block_ctxs;
  ObArray<blocksstable::ObMacroBlockCtx> new_macro_block_ctxs;
  const bool is_new_skip_range = range_idx != skip_ctx_.range_idx_;
  ObExtStoreRange empty_range;
  new_range_ = is_new_skip_range ? empty_range : new_range_;
  last_range_ = is_new_skip_range ? empty_range : last_range_;
  if (OB_UNLIKELY(range_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(range_idx));
  } else if (OB_FAIL(sstable_->find_macros(org_range, org_macro_block_ctxs))) {
    STORAGE_LOG(WARN, "fail to find macros", K(ret));
  } else if (OB_FAIL(sstable_->find_macros(new_range, new_macro_block_ctxs))) {
    STORAGE_LOG(WARN, "fail to find macros", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "skip range before", K(new_range_), K(last_range_));
    last_range_ = new_range_.get_range().get_start_key().is_valid() ? new_range_ : org_range;
    new_range_ = new_range;
    skip_ctx_.reset();
    skip_ctx_.range_idx_ = range_idx;
    skip_ctx_.macro_idx_ = org_macro_block_ctxs.count() - new_macro_block_ctxs.count();
    skip_ctx_.org_range_macro_cnt_ = org_macro_block_ctxs.count();
    STORAGE_LOG(DEBUG,
        "skip range",
        K(skip_ctx_),
        K(new_macro_block_ctxs),
        K(org_macro_block_ctxs),
        K(org_range),
        K(new_range),
        K(new_range_),
        K(last_range_));
  }
  return ret;
}

int ObSSTableRowScanner::generate_new_range(const int64_t range_idx, const ObStoreRowkey& gap_key,
    const bool include_gap_key, const ObExtStoreRange& org_range, ObExtStoreRange*& new_range)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey gap_key_copy;
  ObExtStoreRange* ext_range = NULL;
  bool need_actual_skip = false;
  new_range = NULL;
  void* buf = NULL;
  bool can_skip = false;
  if (OB_FAIL(check_can_skip_range(range_idx, gap_key, can_skip))) {
    STORAGE_LOG(WARN, "fail to check can skip range", K(ret));
  } else if (!can_skip) {
    // do nothing
  } else if (OB_FAIL(skip_batch_rows(range_idx, gap_key, include_gap_key, need_actual_skip))) {
    STORAGE_LOG(WARN, "fail to skip batch rows", K(ret), K(range_idx), K(gap_key), K(need_actual_skip));
  } else if (!need_actual_skip) {
    // do nothing
  } else if (OB_FAIL(gap_key.deep_copy(gap_key_copy, *access_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy rowkey", K(ret));
  } else if (OB_ISNULL(buf = access_ctx_->allocator_->alloc(sizeof(ObExtStoreRange)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else {
    ext_range = new (buf) ObExtStoreRange();
    *ext_range = org_range;
    ext_range->change_boundary(gap_key_copy, access_ctx_->query_flag_.is_reverse_scan(), !include_gap_key);
    if (OB_FAIL(ext_range->to_collation_free_range_on_demand_and_cutoff_range(*access_ctx_->allocator_))) {
      STORAGE_LOG(WARN, "fail to collation free range on demand", K(ret));
    } else {
      new_range = ext_range;
      STORAGE_LOG(
          DEBUG, "generate_new_range", K(org_range), K(*new_range), K(access_ctx_->query_flag_.is_reverse_scan()));
    }
  }
  return ret;
}

struct GapCompare {
public:
  explicit GapCompare(const bool is_reverse, int& ret, bool* is_equal = NULL)
      : is_reverse_(is_reverse), ret_(ret), is_equal_(is_equal)
  {}

  // implementation of rowkey < gap_key
  inline bool operator()(const ObStoreRow& row, const ObStoreRowkey& gap_key) const
  {
    bool bret = false;
    if (!row.is_valid() || !gap_key.is_valid()) {
      ret_ = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret_));
    } else {
      const ObStoreRowkey cur_key(row.row_val_.cells_, gap_key.get_obj_cnt());
      const int32_t compare_result = cur_key.compare(gap_key);
      if (is_reverse_) {
        bret = compare_result > 0;
      } else {
        bret = compare_result < 0;
      }
      if (OB_NOT_NULL(is_equal_)) {
        *is_equal_ = (0 == compare_result);
      }
    }
    return bret;
  }

private:
  bool is_reverse_;
  int& ret_;
  bool* is_equal_;
};

int ObSSTableRowScanner::skip_batch_rows(
    const int64_t range_idx, const ObStoreRowkey& gap_key, const bool include_gap_key, bool& need_actual_skip)
{
  int ret = OB_SUCCESS;
  need_actual_skip = false;
  const int64_t cur_range_idx = get_cur_range_idx();
  STORAGE_LOG(DEBUG,
      "skip batch rows",
      K(range_idx),
      K(cur_range_idx),
      KP(batch_rows_),
      K(batch_row_count_),
      K(batch_row_pos_));
  if (range_idx < cur_range_idx) {
    need_actual_skip = false;
  } else if (range_idx > cur_range_idx || !include_gap_key) {
    need_actual_skip = true;
  } else if (OB_ISNULL(batch_rows_) || batch_row_pos_ >= batch_row_count_) {
    need_actual_skip = true;
  } else {
    const bool is_reverse = access_ctx_->query_flag_.is_reverse_scan();
    const ObStoreRowkey last_batch_key((batch_rows_ + batch_row_count_ - 1)->row_val_.cells_, iter_param_->rowkey_cnt_);
    bool is_equal = false;
    GapCompare gap_compare(is_reverse, ret, &is_equal);
    bool is_last_row_need_skip = gap_compare(*(batch_rows_ + batch_row_count_ - 1), gap_key);
    STORAGE_LOG(DEBUG,
        "try to search gap key in batch rows",
        K(ret),
        KP(batch_rows_),
        K(batch_row_count_),
        K(batch_row_pos_),
        K(is_reverse),
        K(is_last_row_need_skip),
        K(is_equal),
        "last_batch_row",
        *(batch_rows_ + batch_row_count_ - 1),
        K(gap_key));
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN,
          "fail to compare last batch row and gap key",
          K(ret),
          "last_batch_row",
          *(batch_rows_ + batch_row_count_ - 1),
          K(gap_key));
    } else if (is_last_row_need_skip) {
      need_actual_skip = true;
    } else if (is_equal) {
      batch_row_pos_ = batch_row_count_ - 1;
      need_actual_skip = false;
    } else {
      const ObStoreRow* found_row =
          std::lower_bound(batch_rows_ + batch_row_pos_, batch_rows_ + batch_row_count_, gap_key, gap_compare);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(WARN, "fail to compare last batch row and gap key", K(ret));
      } else {
        const int64_t gap_key_pos = found_row - batch_rows_;
        if (gap_key_pos < batch_row_pos_ || gap_key_pos >= batch_row_count_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR,
              "fail to find gap row",
              K(ret),
              K(gap_key_pos),
              K(batch_row_pos_),
              K(batch_row_count_),
              "last_batch_row",
              *(batch_rows_ + batch_row_count_ - 1),
              K(gap_key));
        } else {
          batch_row_pos_ = gap_key_pos;
          need_actual_skip = false;
        }
      }
    }
  }

  if (OB_SUCC(ret) && need_actual_skip) {
    batch_rows_ = NULL;
    batch_row_count_ = 0;
    batch_row_pos_ = 0;
    STORAGE_LOG(DEBUG, "clear all batch rows");
  }
  return ret;
}

int ObSSTableRowScanner::check_can_skip_range(const int64_t range_idx, const ObStoreRowkey& gap_key, bool& can_skip)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey compare_rowkey;
  int32_t compare_result = 0;
  if (range_idx > get_cur_range_idx()) {
    can_skip = true;
  } else if (NULL == curr_row_) {
    can_skip = false;
  } else {
    compare_rowkey.assign(curr_row_->row_val_.cells_, iter_param_->rowkey_cnt_);
    compare_result = compare_rowkey.compare(gap_key);
    can_skip = access_ctx_->query_flag_.is_reverse_scan() ? compare_result > 0 : compare_result < 0;
  }
  STORAGE_LOG(DEBUG, "check can skip range", K(compare_rowkey), K(gap_key), K(compare_result), K(can_skip));
  return ret;
}

int ObSSTableRowScanner::get_skip_range_ctx(
    ObSSTableReadHandle& read_handle, const int64_t cur_micro_idx, ObSSTableSkipRangeCtx*& ctx)
{
  int ret = OB_SUCCESS;
  ctx = NULL;
  if (read_handle.range_idx_ < skip_ctx_.range_idx_) {
    ctx = &skip_ctx_;
    skip_ctx_.need_skip_ = true;
  } else if (read_handle.range_idx_ == skip_ctx_.range_idx_) {
    bool need_update = false;
    skip_ctx_.need_skip_ = false;
    ctx = &skip_ctx_;
    if (read_handle.macro_idx_ < skip_ctx_.macro_idx_) {
      skip_ctx_.need_skip_ = true;
    } else if (read_handle.macro_idx_ == skip_ctx_.macro_idx_) {
      if (-1 == skip_ctx_.micro_idx_ && INT64_MAX != cur_micro_idx) {
        int64_t new_range_block_count = 0;
        int64_t old_range_block_count = 0;
        const blocksstable::ObMicroBlockIndexMgr* block_index_mgr = NULL;
        const bool is_left_border = true;
        const bool is_right_border = true;
        ObMicroBlockIndexHandle* handle = NULL;
        if (OB_FAIL(read_handle.get_index_handle(handle))) {
          STORAGE_LOG(WARN, "Fail to get index handle, ", K(ret), K(read_handle));
        } else if (OB_FAIL(handle->get_block_index_mgr(block_index_mgr))) {
          STORAGE_LOG(WARN, "fail to get block index mgr", K(ret));
        } else if (OB_FAIL(block_index_mgr->get_block_count(new_range_.get_range(),
                       is_left_border,
                       is_right_border,
                       new_range_block_count,
                       get_rowkey_cmp_funcs()))) {
          STORAGE_LOG(WARN, "fail to search micro blocks", K(ret));
        } else if (OB_FAIL(block_index_mgr->get_block_count(last_range_.get_range(),
                       is_left_border,
                       is_right_border,
                       old_range_block_count,
                       get_rowkey_cmp_funcs()))) {
          STORAGE_LOG(WARN, "fail to search micro blocks", K(ret));
        } else {
          STORAGE_LOG(
              DEBUG, "skip range micro infos", K(read_handle), K(new_range_block_count), K(old_range_block_count));
          const int64_t org_micro_block_cnt = read_handle.micro_end_idx_ - read_handle.micro_begin_idx_ + 1;
          if (org_micro_block_cnt == new_range_block_count) {
            skip_ctx_.micro_idx_ = read_handle.micro_begin_idx_;
          } else if (org_micro_block_cnt == old_range_block_count) {
            read_handle.micro_begin_idx_ += old_range_block_count - new_range_block_count;
            skip_ctx_.micro_idx_ = read_handle.micro_begin_idx_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN,
                "read handle micro block cnt is not invalid",
                K(read_handle),
                K(skip_ctx_),
                K(old_range_block_count),
                K(new_range_block_count));
          }
          STORAGE_LOG(DEBUG,
              "update micro begin idx info",
              K(old_range_block_count),
              K(new_range_block_count),
              K(read_handle),
              K(skip_ctx_),
              K(new_range_),
              K(last_range_));
          need_update = true;
        }
      }
      if (OB_SUCC(ret)) {
        skip_ctx_.need_skip_ = cur_micro_idx < skip_ctx_.micro_idx_;
      }
    } else {
      skip_ctx_.need_skip_ = false;
      need_update = true;
    }
    if (OB_SUCC(ret) && need_update) {
      read_handle.ext_range_ = &new_range_;
      if (access_ctx_->query_flag_.is_reverse_scan()) {
        read_handle.is_right_border_ = read_handle.macro_idx_ == skip_ctx_.macro_idx_;
        read_handle.is_left_border_ = read_handle.macro_idx_ == skip_ctx_.org_range_macro_cnt_ - 1;
      } else {
        read_handle.is_left_border_ = read_handle.macro_idx_ == skip_ctx_.macro_idx_;
        read_handle.is_right_border_ = read_handle.macro_idx_ == skip_ctx_.org_range_macro_cnt_ - 1;
      }
      STORAGE_LOG(DEBUG, "skip range micro infos", K(read_handle), K(skip_ctx_), K(cur_micro_idx), K(common::lbt()));
    }
  }

  return ret;
}

int ObSSTableRowScanner::prefetch_block_index(const uint64_t table_id,
    const blocksstable::ObMacroBlockCtx& macro_block_ctx, ObMicroBlockIndexHandle& block_index_handle)
{
  int ret = OB_SUCCESS;
  block_index_handle.reset();
  block_index_handle.io_handle_.set_file(storage_file_);
  if (OB_FAIL(ObStorageCacheSuite::get_instance().get_micro_index_cache().get_cache_block_index(table_id,
          macro_block_ctx.get_macro_block_id(),
          storage_file_->get_file_id(),
          block_index_handle.cache_handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "Fail to get cache block index, ", K(ret));
    }
    ret = OB_SUCCESS;
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_micro_index_cache().prefetch(
            table_id, macro_block_ctx, storage_file_, block_index_handle.io_handle_, access_ctx_->query_flag_))) {
      STORAGE_LOG(WARN, "Fail to prefetch micro block index, ", K(ret));
    }
  }
  return ret;
}

int ObSSTableRowScanner::get_gap_range_idx(int64_t& range_idx)
{
  int ret = OB_SUCCESS;
  ObSSTableReadHandle* first_handle = NULL;
  if (OB_FAIL(get_cur_read_handle(first_handle))) {
    STORAGE_LOG(WARN, "fail to get curr read handle", K(ret));
  } else if (OB_ISNULL(first_handle)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, first handle must not be NULL", K(ret));
  } else {
    range_idx = first_handle->range_idx_;
  }
  return ret;
}

int ObSSTableRowScanner::get_gap_end_impl(const ObExtStoreRange& org_range, ObStoreRowkey& gap_key, int64_t& gap_size)
{
  int ret = OB_SUCCESS;
  gap_key.reset();
  gap_size = 0;
  ObSSTableReadHandle* first_handle = NULL;
  ObMicroBlockDataHandle* micro_handle = NULL;
  const bool is_left_border = true;
  const bool is_right_border = true;
  if (OB_FAIL(get_cur_read_handle(first_handle))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret));
  } else if (OB_FAIL(get_cur_read_micro_handle(micro_handle))) {
    STORAGE_LOG(WARN, "fail to get cur read micro handle", K(ret));
  } else {
    int64_t range_idx = first_handle->range_idx_;
    ObFullMacroBlockMeta meta;
    int64_t first_idx = first_handle->macro_idx_;
    int64_t curr_idx = -1;
    int64_t prev_idx = -1;
    ObMacroBlockCtx curr_block_ctx;
    ObMacroBlockCtx prev_block_ctx;
    int64_t final_macro_idx = -1;
    ObMacroBlockIterator macro_iter;
    ObMacroBlockCtx macro_block_ctx;
    if (OB_FAIL(macro_iter.open(*sstable_, org_range, access_ctx_->query_flag_.is_reverse_scan()))) {
      STORAGE_LOG(WARN, "fail to open macro iterator", K(ret));
    } else {
      int64_t i = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(macro_iter.get_next_macro_block(macro_block_ctx, meta))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "fail to get next macro block", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (i < first_idx) {
          // do nothing
        } else if (!meta.meta_->macro_block_deletion_flag_) {
          curr_idx = i;
          curr_block_ctx = macro_block_ctx;
          break;
        } else {
          gap_size += meta.meta_->row_count_;
          prev_idx = i;
          prev_block_ctx = macro_block_ctx;
        }
        ++i;
      }
    }

    if (OB_SUCC(ret)) {
      // find last mark deleted micro in curr_handle
      common::ObSEArray<blocksstable::ObMicroBlockInfo, 16> micro_infos;
      int32_t last_mark_deleted_micro_index = -1;
      ObMicroBlockIndexHandle index_handle;
      bool is_new_skip_range = range_idx != skip_ctx_.range_idx_;
      ObExtStoreRange range;
      bool can_skip = false;
      if (is_new_skip_range) {
        range = org_range;
      } else {
        range = new_range_.get_range().get_start_key().is_valid() ? new_range_ : org_range;
      }
      if (-1 != curr_idx) {
        if (OB_FAIL(prefetch_block_index(iter_param_->table_id_, curr_block_ctx, index_handle))) {
          STORAGE_LOG(WARN, "fail to get block index handle", K(ret));
        } else if (OB_FAIL(index_handle.search_blocks(
                       range.get_range(), is_left_border, is_right_border, micro_infos, get_rowkey_cmp_funcs()))) {
          if (OB_BEYOND_THE_RANGE != ret) {
            STORAGE_LOG(WARN, "Fail to search blocks", K(ret), K(range));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < micro_infos.count(); ++i) {
            const int64_t idx = access_ctx_->query_flag_.is_reverse_scan() ? micro_infos.count() - 1 - i : i;
            bool need_skip = false;
            if (first_idx == curr_idx) {
              need_skip = access_ctx_->query_flag_.is_reverse_scan()
                              ? micro_handle->micro_info_.index_ < micro_infos.at(idx).index_
                              : micro_handle->micro_info_.index_ > micro_infos.at(idx).index_;
            }

            if (need_skip) {
              // do nothing
            } else if (!micro_infos.at(idx).mark_deletion_) {
              break;
            } else {
              const int64_t average_row_count =
                  meta.meta_->micro_block_count_ != 0 ? meta.meta_->row_count_ / meta.meta_->micro_block_count_ : 0;
              last_mark_deleted_micro_index = micro_infos.at(idx).index_;
              gap_size += average_row_count;
              final_macro_idx = curr_idx;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (-1 == last_mark_deleted_micro_index && prev_idx != curr_idx && -1 != prev_idx) {
          // try to get from prev handle
          if (OB_FAIL(prefetch_block_index(iter_param_->table_id_, prev_block_ctx, index_handle))) {
            STORAGE_LOG(WARN, "fail to get block index handle", K(ret), K(prev_block_ctx));
          } else if (OB_FAIL(index_handle.search_blocks(
                         range.get_range(), is_left_border, is_right_border, micro_infos, get_rowkey_cmp_funcs()))) {
            if (OB_BEYOND_THE_RANGE != ret) {
              STORAGE_LOG(WARN, "Fail to search blocks", K(ret), K(range));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            const int64_t idx = access_ctx_->query_flag_.is_reverse_scan() ? 0 : micro_infos.count() - 1;
            last_mark_deleted_micro_index = micro_infos.at(idx).index_;
            final_macro_idx = prev_idx;
          }
        }
      }

      if (OB_SUCC(ret) && -1 != last_mark_deleted_micro_index) {
        const blocksstable::ObMicroBlockIndexMgr* block_index_mgr = NULL;
        if (range_idx != last_gap_range_idx_) {
          can_skip = true;
        } else {
          if (final_macro_idx > last_gap_macro_idx_) {
            can_skip = true;
          } else if (final_macro_idx == last_gap_macro_idx_) {
            can_skip = access_ctx_->query_flag_.is_reverse_scan() ? last_mark_deleted_micro_index < last_gap_micro_idx_
                                                                  : last_mark_deleted_micro_index > last_gap_micro_idx_;
          } else {
            can_skip = false;
          }
        }
        if (can_skip) {
          if (OB_FAIL(index_handle.get_block_index_mgr(block_index_mgr))) {
            STORAGE_LOG(WARN, "fail to get block index mgr", K(ret));
          } else if (OB_ISNULL(block_index_mgr)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "error unexpected, block index mgr must not be NULL", K(ret));
          } else if (OB_FAIL(block_index_mgr->get_endkey(
                         last_mark_deleted_micro_index, *access_ctx_->allocator_, gap_key))) {
            STORAGE_LOG(WARN, "fail to get endkey", K(ret));
          } else {
            ObStoreRowkey curr_rowkey;
            bool is_rewind = false;
            gap_key.assign(const_cast<ObObj*>(gap_key.get_obj_ptr()), iter_param_->rowkey_cnt_);
            if (NULL != curr_row_) {
              int compare_ret = 0;
              curr_rowkey.assign(curr_row_->row_val_.cells_, iter_param_->rowkey_cnt_);
              compare_ret = gap_key.compare(curr_rowkey);
              is_rewind = access_ctx_->query_flag_.is_reverse_scan() ? compare_ret >= 0 : compare_ret <= 0;
            }
            if (is_rewind) {
              last_mark_deleted_micro_index = -1;
              final_macro_idx = -1;
              gap_size = 0;
              gap_key.reset();
            } else {
              last_gap_range_idx_ = range_idx;
              last_gap_macro_idx_ = final_macro_idx;
              last_gap_micro_idx_ = last_mark_deleted_micro_index;
            }
          }
        } else {
          gap_size = 0;
          gap_key.reset();
        }
      }
      if (-1 != last_mark_deleted_micro_index && can_skip) {
        STORAGE_LOG(TRACE,
            "get_gap_end",
            K(gap_size),
            K(range_idx),
            K(gap_key),
            K(curr_idx),
            K(prev_idx),
            K(last_mark_deleted_micro_index),
            K(final_macro_idx),
            K(curr_block_ctx),
            K(prev_block_ctx));
      }
    }
  }

  return ret;
}

int ObSSTableRowScanner::get_range_count(const void* query_range, int64_t& range_count) const
{
  int ret = OB_SUCCESS;
  UNUSED(query_range);
  range_count = 1;
  return ret;
}

int ObSSTableRowScanner::get_row_iter_flag_impl(uint8_t& flag)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDataHandle* micro_handle = NULL;
  flag = 0;
  if (NULL == curr_row_ || !curr_row_->is_delete()) {
    // do nothing
  } else if (OB_FAIL(get_cur_read_micro_handle(micro_handle))) {
    STORAGE_LOG(WARN, "fail to get curr read micro handle", K(ret));
  } else if (OB_ISNULL(micro_handle)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid micro handle", K(ret), KP(micro_handle));
  } else {
    if (!micro_handle->micro_info_.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "invalid micro info", K(ret), K(micro_handle->micro_info_));
    } else {
      flag = curr_row_->is_delete() && micro_handle->micro_info_.mark_deletion_
                 ? STORE_ITER_ROW_BIG_GAP_HINT | STORE_ITER_ROW_IN_GAP
                 : 0;
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
