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
#include "ob_macro_block_iterator.h"
#include "lib/container/ob_fixed_array_iterator.h"
#include "ob_sstable.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {
ObMacroBlockDesc::ObMacroBlockDesc()
    : data_version_(0),
      block_idx_(0),
      block_cnt_(0),
      macro_block_ctx_(),
      full_meta_(),
      range_(),
      row_store_type_(FLAT_ROW_STORE),
      schema_version_(0),
      data_seq_(0),
      row_count_(0),
      occupy_size_(0),
      micro_block_count_(0),
      data_checksum_(0),
      snapshot_version_(0),
      row_count_delta_(0),
      progressive_merge_round_(0),
      contain_uncommitted_row_(true)
{}
/**
 * -----------------------------------------------------------ObMacroBlockRowComparor-------------------------------------------------------
 */

ObMacroBlockRowComparor::ObMacroBlockRowComparor()
    : ret_(OB_SUCCESS),
      use_collation_free_(false),
      is_prefix_check_(false),
      full_meta_(),
      macro_block_rowkey_(),
      sstable_(nullptr)
{}

ObMacroBlockRowComparor::~ObMacroBlockRowComparor()
{}

int ObMacroBlockRowComparor::compare_(
    const blocksstable::MacroBlockId& block_id, const common::ObStoreRowkey& rowkey, int& cmp_ret)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sstable_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, sstable must not be null", K(ret));
  } else if (OB_FAIL(sstable_->get_meta(block_id, full_meta_))) {
    LOG_WARN("fail to get meta", K(ret), K(block_id));
  } else if (OB_UNLIKELY(!full_meta_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "Invalid macro meta, ", K(ret), K(full_meta_));
  } else {
    const ObMacroBlockMetaV2* macro_meta = full_meta_.meta_;
    if (use_collation_free_) {
      if (OB_ISNULL(macro_meta->collation_free_endkey_)) {
        macro_block_rowkey_.assign(macro_meta->endkey_, macro_meta->rowkey_column_number_);
      } else {
        macro_block_rowkey_.assign(macro_meta->collation_free_endkey_, macro_meta->rowkey_column_number_);
      }
    } else {
      macro_block_rowkey_.assign(macro_meta->endkey_, macro_meta->rowkey_column_number_);
    }
    if (is_prefix_check_) {
      ret = macro_block_rowkey_.compare_prefix(rowkey, cmp_ret);
    } else {
      ret = macro_block_rowkey_.compare(rowkey, cmp_ret);
    }
  }

  return ret;
}

bool ObMacroBlockRowComparor::operator()(
    const blocksstable::MacroBlockId& block_id, const common::ObStoreRowkey& rowkey)
{
  bool bret = false;
  int32_t cmp_ret = 0;

  if (OB_UNLIKELY(OB_SUCCESS != (ret_ = compare_(block_id, rowkey, cmp_ret)))) {
    STORAGE_LOG(WARN, "Failed to compare block and rowkey", K(rowkey), K_(ret));
  } else {
    bret = cmp_ret < 0;
  }

  return bret;
}

bool ObMacroBlockRowComparor::operator()(
    const common::ObStoreRowkey& rowkey, const blocksstable::MacroBlockId& block_id)
{
  bool bret = false;
  int32_t cmp_ret = 0;

  if (OB_UNLIKELY(OB_SUCCESS != (ret_ = compare_(block_id, rowkey, cmp_ret)))) {
    STORAGE_LOG(WARN, "Failed to compare block and rowkey", K(block_id), K(rowkey), K_(ret));
  } else {
    bret = cmp_ret > 0;
  }

  return bret;
}

void ObMacroBlockRowComparor::reset()
{
  ret_ = OB_SUCCESS;
  use_collation_free_ = false;
  full_meta_.reset();
  is_prefix_check_ = false;
  sstable_ = nullptr;
}

/**
 * -----------------------------------------------------------ObMacroBlockIterator-----------------------------------------------------------
 */
ObMacroBlockIterator::ObMacroBlockIterator()
    : sstable_(NULL), is_reverse_scan_(false), step_(1), cur_idx_(-1), begin_(-1), end_(-1), check_lob_(false)
{}

ObMacroBlockIterator::~ObMacroBlockIterator()
{}

void ObMacroBlockIterator::reset()
{
  sstable_ = NULL;
  is_reverse_scan_ = false;
  step_ = 1;
  cur_idx_ = -1;
  begin_ = -1;
  end_ = -1;
  check_lob_ = false;
}

int ObMacroBlockIterator::open(ObSSTable& sstable, const ObExtStoreRowkey& ext_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(sstable));
  } else {
    sstable_ = &sstable;
    if (OB_FAIL(locate_macro_block(ext_rowkey, cur_idx_))) {
      STORAGE_LOG(WARN, "Fail to locate macro block, ", K(ret), K(ext_rowkey));
    } else {
      begin_ = end_ = cur_idx_;
    }
  }
  return ret;
}

int ObMacroBlockIterator::open(ObSSTable& sstable, const bool is_reverse, const bool check_lob)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(sstable));
  } else if (OB_UNLIKELY(check_lob && is_reverse)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Macro block scan with lob conflict with reverse scan", K(ret));
  } else {
    sstable_ = &sstable;
    check_lob_ = check_lob;
    is_reverse_scan_ = is_reverse;
    begin_ = 0;
    if (check_lob) {
      end_ = sstable.get_total_macro_blocks().count() - 1;
    } else {
      end_ = sstable.get_macro_block_ids().count() - 1;
    }
    if (is_reverse) {
      cur_idx_ = end_;
      step_ = -1;
    } else {
      cur_idx_ = begin_;
      step_ = 1;
    }
  }

  return ret;
}

int ObMacroBlockIterator::open(ObSSTable& sstable, const common::ObExtStoreRange& ext_range, const bool is_reverse)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(sstable));
  } else {
    sstable_ = &sstable;
    is_reverse_scan_ = is_reverse;
    step_ = is_reverse ? -1 : 1;

    if (OB_FAIL(locate_macro_block(ext_range.get_ext_start_key(), begin_))) {
      STORAGE_LOG(WARN, "Fail to locate start macro block, ", K(ret));
    } else if (-1 == begin_) {
      // not exist range
      end_ = cur_idx_ = -1;
    } else if (OB_FAIL(locate_macro_block(ext_range.get_ext_end_key(), end_))) {
      STORAGE_LOG(WARN, "Fail to locate end macro block, ", K(ret));
    } else {
      if (-1 == end_) {
        end_ = sstable.get_macro_block_ids().count() - 1;
      }
      if (!ext_range.get_range().get_border_flag().inclusive_start()) {
        const MacroBlockId& block_id = sstable_->get_macro_block_ids().at(begin_);
        ObFullMacroBlockMeta full_meta;
        if (OB_FAIL(sstable_->get_meta(block_id, full_meta))) {
          STORAGE_LOG(WARN, "fail to get meta", K(ret));
        } else {
          const ObMacroBlockMetaV2* meta = full_meta.meta_;
          ObStoreRowkey rowkey(meta->endkey_, meta->rowkey_column_number_);
          if (ext_range.get_range().get_start_key() == rowkey) {
            ++begin_;
          }
        }
      }

      cur_idx_ = is_reverse ? end_ : begin_;
    }
  }
  return ret;
}

int ObMacroBlockIterator::get_next_macro_block(blocksstable::ObMacroBlockCtx& macro_block_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(check_lob_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected check lob mode in ObMacroBlockIterator", K(ret));
  } else if (cur_idx_ < begin_ || cur_idx_ > end_ || cur_idx_ < 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(sstable_->get_macro_block_ctx(cur_idx_, macro_block_ctx))) {
    STORAGE_LOG(WARN, "Fail to get_macro_ctx", K(ret), K(cur_idx_));
  } else {
    STORAGE_LOG(DEBUG, "Success to get macro block id, ", K_(cur_idx), K(macro_block_ctx));
    cur_idx_ += step_;
  }
  return ret;
}

int ObMacroBlockIterator::get_next_macro_block(
    blocksstable::ObMacroBlockCtx& macro_block_ctx, ObFullMacroBlockMeta& macro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(check_lob_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected check lob mode in ObMacroBlockIterator", K(ret));
  } else if (cur_idx_ < begin_ || cur_idx_ > end_ || cur_idx_ < 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(sstable_->get_macro_block_ctx(cur_idx_, macro_block_ctx))) {
    STORAGE_LOG(WARN, "Fail to get_macro_ctx", K(ret), K(cur_idx_));
  } else if (OB_FAIL(sstable_->get_meta(macro_block_ctx.sstable_block_id_.macro_block_id_, macro_meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret));
  } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, ", K(ret), K(macro_meta));
  } else {
    STORAGE_LOG(DEBUG, "Success to get macro block id, ", K_(cur_idx), K(macro_block_ctx));
    cur_idx_ += step_;
  }
  return ret;
}

int ObMacroBlockIterator::get_next_macro_block(ObMacroBlockDesc& block_desc)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;

  if (cur_idx_ < begin_ || cur_idx_ > end_ || cur_idx_ < 0) {
    ret = OB_ITER_END;
  } else if (cur_idx_ >= sstable_->get_macro_block_count()) {
    const int64_t lob_idx = cur_idx_ - sstable_->get_macro_block_count();
    if (OB_UNLIKELY(!check_lob_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "Unexpected macro block iter index without check lob",
          K_(cur_idx),
          "macro_block_count",
          sstable_->get_macro_block_count(),
          K(ret));
    } else if (OB_FAIL(sstable_->get_lob_macro_block_ctx(lob_idx, block_desc.macro_block_ctx_))) {
      LOG_WARN("Failed to get lob macro block ctx", K(ret), K(lob_idx));
    } else if (OB_FAIL(sstable_->get_meta(block_desc.macro_block_ctx_.sstable_block_id_.macro_block_id_, full_meta))) {
      LOG_WARN("fail to get meta", K(ret));
    } else {
      block_desc.range_.get_start_key().assign(full_meta.meta_->endkey_, full_meta.meta_->rowkey_column_number_);
      block_desc.range_.get_end_key().assign(full_meta.meta_->endkey_, full_meta.meta_->rowkey_column_number_);
    }
  } else {
    if (0 == cur_idx_) {
      block_desc.range_.get_start_key().set_min();
    } else {
      if (OB_FAIL(sstable_->get_meta(sstable_->get_macro_block_ids().at(cur_idx_ - 1), full_meta))) {
        STORAGE_LOG(WARN, "fail to get meta", K(ret));
      } else if (!full_meta.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Invalid macro meta, ", K(ret), K(full_meta));
      } else {
        block_desc.range_.get_start_key().assign(full_meta.meta_->endkey_, full_meta.meta_->rowkey_column_number_);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_->get_meta(sstable_->get_macro_block_ids().at(cur_idx_), full_meta))) {
        STORAGE_LOG(WARN, "fail to get meta", K(ret));
      } else if (!full_meta.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Invalid macro meta, ", K(ret), K(full_meta));
      } else if (OB_FAIL(sstable_->get_macro_block_ctx(cur_idx_, block_desc.macro_block_ctx_))) {
        STORAGE_LOG(WARN, "failed to get macro block ctx", K(ret), K(cur_idx_));
      } else {
        if (cur_idx_ == sstable_->get_macro_block_ids().count() - 1) {
          block_desc.range_.get_end_key().set_max();
        } else {
          block_desc.range_.get_end_key().assign(full_meta.meta_->endkey_, full_meta.meta_->rowkey_column_number_);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG,
        "Success to get macro block id, ",
        K_(cur_idx),
        "macro_block_id",
        sstable_->get_macro_block_ids().at(cur_idx_));
    block_desc.block_idx_ = cur_idx_;
    block_desc.data_version_ = full_meta.meta_->data_version_;
    block_desc.row_store_type_ = full_meta.meta_->row_store_type_;
    if (check_lob_) {
      block_desc.block_cnt_ = sstable_->get_total_macro_blocks().count();
    } else {
      block_desc.block_cnt_ = sstable_->get_macro_block_ids().count();
    }
    block_desc.full_meta_ = full_meta;
    block_desc.range_.get_border_flag().unset_inclusive_start();
    block_desc.range_.get_border_flag().set_inclusive_end();
    block_desc.schema_version_ = full_meta.meta_->schema_version_;
    block_desc.data_seq_ = full_meta.meta_->data_seq_;
    block_desc.row_count_ = full_meta.meta_->row_count_;
    block_desc.occupy_size_ = full_meta.meta_->occupy_size_;
    block_desc.micro_block_count_ = full_meta.meta_->micro_block_count_;
    block_desc.data_checksum_ = full_meta.meta_->data_checksum_;
    block_desc.snapshot_version_ = full_meta.meta_->snapshot_version_;
    block_desc.row_count_delta_ = full_meta.meta_->row_count_delta_;
    block_desc.progressive_merge_round_ = full_meta.meta_->progressive_merge_round_;
    block_desc.contain_uncommitted_row_ = full_meta.meta_->contain_uncommitted_row_;
    cur_idx_ += step_;
  }
  return ret;
}

int ObMacroBlockIterator::get_macro_block_count(int64_t& macro_block_count)
{
  int ret = OB_SUCCESS;
  if (begin_ < 0 || end_ < 0) {
    macro_block_count = 0;
  } else {
    macro_block_count = end_ - begin_ + 1;
  }
  return ret;
}

int ObMacroBlockIterator::locate_macro_block(const ObExtStoreRowkey& ext_rowkey, int64_t& block_idx)
{
  int ret = OB_SUCCESS;
  block_idx = -1;

  if (sstable_->get_macro_block_count() == 0) {
    // pass empty sstable
  } else if (sstable_->is_rowkey_helper_valid() &&
             sstable_->get_rowkey_helper().is_oracle_mode() == share::is_oracle_mode()) {
    if (OB_FAIL(sstable_->get_rowkey_helper().locate_block_idx(ext_rowkey, cur_idx_, block_idx))) {
      STORAGE_LOG(WARN, "Failed to locate macro block with rowkey helper", K(ret));
    }
  } else {
    ret = locate_macro_block_without_helper(ext_rowkey, block_idx);
  }

  return ret;
}

int ObMacroBlockIterator::locate_macro_block_without_helper(const ObExtStoreRowkey& ext_rowkey, int64_t& block_idx)
{
  int ret = OB_SUCCESS;
  const ObStoreRowkey* cmp_rowkey = NULL;
  bool use_collation_free = false;
  block_idx = -1;

  if (OB_FAIL(ext_rowkey.check_use_collation_free(sstable_->exist_invalid_collation_free_meta_, use_collation_free))) {
    STORAGE_LOG(WARN, "Fail to check use collation free, ", K(ret), K(ext_rowkey));
  } else {
    ObSSTable::MacroBlockArray::iterator begin = sstable_->meta_.macro_block_array_.begin();
    ObSSTable::MacroBlockArray::iterator end = sstable_->meta_.macro_block_array_.end();

    comparor_.reset();
    comparor_.set_use_collation_free(use_collation_free);
    comparor_.set_sstable(*sstable_);
    if (use_collation_free) {
      cmp_rowkey = &(ext_rowkey.get_collation_free_store_rowkey());
    } else {
      cmp_rowkey = &(ext_rowkey.get_store_rowkey());
    }
    comparor_.set_prefix_check(cmp_rowkey->get_obj_cnt() < sstable_->meta_.rowkey_column_count_);
    if (!comparor_.is_prefix_check() && cur_idx_ >= 0 && cur_idx_ < sstable_->get_macro_block_ids().count()) {
      // the rowkey may be in the same macro block, so check last macro block first
      if (!comparor_(sstable_->get_macro_block_ids().at(cur_idx_), *cmp_rowkey)) {
        if (0 == cur_idx_ || comparor_(sstable_->get_macro_block_ids().at(cur_idx_ - 1), *cmp_rowkey)) {
          block_idx = cur_idx_;
        }
      }
    }

    if (-1 == block_idx) {
      // binary search
      ObSSTable::MacroBlockArray::iterator iter = std::lower_bound(begin, end, *cmp_rowkey, comparor_);
      if (OB_FAIL(comparor_.get_ret())) {
        STORAGE_LOG(ERROR, "Fail to find macro, ", K(ret));
      } else {
        if (iter < sstable_->meta_.macro_block_array_.end()) {
          block_idx = iter - sstable_->meta_.macro_block_array_.begin();
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
