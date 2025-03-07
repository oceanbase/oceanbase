/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_ddl_index_block_row_iterator.h"
#include "storage/access/ob_rows_info.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
namespace blocksstable
{

/******************             ObDDLIndexBlockRowIterator              **********************/
ObDDLIndexBlockRowIterator::ObDDLIndexBlockRowIterator()
  : is_iter_start_(false),
    is_iter_finish_(true),
    is_co_sstable_(false),
    btree_iter_(),
    block_meta_tree_(nullptr),
    cur_tree_value_(nullptr)
{

}

ObDDLIndexBlockRowIterator::~ObDDLIndexBlockRowIterator()
{
  reset();
}

void ObDDLIndexBlockRowIterator::reset()
{
  ObIndexBlockRowIterator::reset();
  is_iter_finish_ = true;
  is_iter_start_ = false;
  is_co_sstable_ = false;
  btree_iter_.reset();
  block_meta_tree_ = nullptr;
  cur_tree_value_ = nullptr;
}

void ObDDLIndexBlockRowIterator::reuse()
{
  is_iter_finish_ = true;
  is_iter_start_ = false;
  is_co_sstable_ = false;
  btree_iter_.reset();
  block_meta_tree_ = nullptr;
  cur_tree_value_ = nullptr;
}

int ObDDLIndexBlockRowIterator::init(const ObMicroBlockData &idx_block_data,
                                     const ObStorageDatumUtils *datum_utils,
                                     ObIAllocator *allocator,
                                     const bool is_reverse_scan,
                                     const ObIndexBlockIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_utils) || !datum_utils->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), KP(allocator), KPC(datum_utils));
  } else {
    block_meta_tree_ = reinterpret_cast<const ObBlockMetaTree *>(idx_block_data.buf_);
    is_reverse_scan_ = is_reverse_scan;
    iter_step_ = is_reverse_scan_ ? -1 : 1;
    datum_utils_ = datum_utils;
    is_co_sstable_ = iter_param.is_valid() ? iter_param.sstable_->is_co_sstable() || iter_param.sstable_->is_ddl_mem_co_cg_sstable() : false;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::set_iter_param(const ObStorageDatumUtils *datum_utils,
                                               bool is_reverse_scan,
                                               const storage::ObBlockMetaTree *block_meta_tree,
                                               const bool is_co_sstable,
                                               const int64_t iter_step)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_utils) || OB_UNLIKELY(!datum_utils->is_valid()) || OB_ISNULL(block_meta_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), KP(block_meta_tree), KPC(datum_utils));
  } else {
    block_meta_tree_ = block_meta_tree;
    is_reverse_scan_ = is_reverse_scan;
    iter_step_ = iter_step == INT64_MAX ? (is_reverse_scan_ ? -1 : 1) : iter_step;
    datum_utils_ = datum_utils;
    is_co_sstable_ = is_co_sstable;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::locate_key(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey));
  } else {
    ObDatumRange range;
    range.set_start_key(rowkey);
    range.set_end_key(rowkey);
    range.set_left_closed();
    range.set_right_closed();
    if (OB_ISNULL(block_meta_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block meta tree is null", K(ret));
    } else if (OB_FAIL(block_meta_tree_->locate_key(range,
                                                    *datum_utils_,
                                                    btree_iter_,
                                                    cur_tree_value_))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("locate rowkey failed", K(ret), K(range), K(*this));
      } else {
        is_iter_finish_ = true;
      }
    } else if (OB_ISNULL(cur_tree_value_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur tree value is null", K(ret), KP(cur_tree_value_));
    } else {
      is_iter_start_ = true;
      is_iter_finish_ = false;
    }
    LOG_TRACE("Binary search rowkey in ddl block", K(ret), K(rowkey), KPC(this));
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::locate_range(const ObDatumRange &range,
                                             const bool is_left_border,
                                             const bool is_right_border,
                                             const bool is_normal_cg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range", K(ret), K(range));
  } else if (OB_ISNULL(block_meta_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block meta tree is null", K(ret));
  } else if (OB_FAIL(block_meta_tree_->locate_range(range,
                                                    *datum_utils_,
                                                    is_left_border,
                                                    is_right_border,
                                                    is_reverse_scan_,
                                                    btree_iter_,
                                                    cur_tree_value_))) {
    is_iter_finish_ = true;
    LOG_WARN("block meta tree locate range failed", K(ret), K(range));
  } else if (OB_ISNULL(cur_tree_value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur tree value is null", K(ret), KP(cur_tree_value_));
  } else {
    is_iter_start_ = true;
    is_iter_finish_ = false;
  }
  LOG_TRACE("Locate range in ddl block by range", K(ret), K(range), KPC(this));
  return ret;
}

int ObDDLIndexBlockRowIterator::locate_range()
{
  int ret = OB_SUCCESS;
  ObDatumRange range;
  range.set_start_key(ObDatumRowkey::MIN_ROWKEY);
  range.set_end_key(ObDatumRowkey::MAX_ROWKEY);
  range.set_left_open();
  range.set_right_open();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_ISNULL(block_meta_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block meta tree is null", K(ret));
  } else if (OB_FAIL(block_meta_tree_->locate_range(range,
                                                    *datum_utils_,
                                                    false, /*is_left_border*/
                                                    false, /*is_right_border*/
                                                    is_reverse_scan_,
                                                    btree_iter_,
                                                    cur_tree_value_))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      LOG_WARN("block meta tree locate range failed", K(ret), K(range));
    } else {
      is_iter_finish_ = true;
      LOG_INFO("no data to locate", K(ret));
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(cur_tree_value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur tree value is null", K(ret), KP(cur_tree_value_));
  } else {
    is_iter_start_ = true;
    is_iter_finish_ = false;
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::skip_to_next_valid_position(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  storage::ObBlockMetaTreeValue *tmp_tree_value = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_ISNULL(block_meta_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block meta tree is null", K(ret));
  } else if (OB_FAIL(block_meta_tree_->skip_to_next_valid_position(rowkey,
                                                                   *datum_utils_,
                                                                   btree_iter_,
                                                                   tmp_tree_value))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to skip to next valid position in block meta tree", K(ret), K(rowkey));
    } else {
      is_iter_finish_ = true;
    }
  } else {
    cur_tree_value_ = tmp_tree_value;
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::find_rowkeys_belong_to_same_idx_row(ObMicroIndexInfo &idx_block_row, int64_t &rowkey_begin_idx, int64_t &rowkey_end_idx, const ObRowsInfo *&rows_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_ISNULL(rows_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rows info", K(ret));
  } else {
    const ObDatumRowkey *cur_rowkey = cur_tree_value_->rowkey_;
    bool is_decided = false;
    for (; OB_SUCC(ret) && rowkey_begin_idx < rowkey_end_idx; ++rowkey_begin_idx) {
      if (rows_info->is_row_skipped(rowkey_begin_idx)) {
        continue;
      }
      const ObDatumRowkey &rowkey = rows_info->get_rowkey(rowkey_begin_idx);
      int32_t cmp_ret = 0;
      if (OB_ISNULL(cur_rowkey)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null rowkey", K(ret), K(cur_tree_value_), KP(cur_rowkey));
      } else if (OB_FAIL(rowkey.compare(*cur_rowkey, *datum_utils_, cmp_ret, false))) {
        LOG_WARN("Failed to compare rowkey", K(ret), K(rowkey), KPC(cur_rowkey));
      }

      if (OB_FAIL(ret)) {
      } else if (cmp_ret > 0) {
        idx_block_row.rowkey_end_idx_ = rowkey_begin_idx;
        is_decided = true;
        break;
      } else if (cmp_ret == 0) {
        idx_block_row.rowkey_end_idx_ = rowkey_begin_idx + 1;
        is_decided = true;
        break;
      }
    }

    if (OB_SUCC(ret) && !is_decided) {
      idx_block_row.rowkey_end_idx_ = rowkey_begin_idx;
    }
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan)
{
  int ret = OB_SUCCESS;
  can_blockscan = false;
  return ret;
}

int ObDDLIndexBlockRowIterator::get_current(const ObIndexBlockRowHeader *&idx_row_header,
                                            ObCommonDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  bool is_start_key = false;
  bool is_end_key = false;
  idx_row_header = nullptr;
  endkey.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_ISNULL(cur_tree_value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur tree value is null", K(ret));
  } else {
    idx_row_header = &(cur_tree_value_->header_);
    endkey.set_compact_rowkey(&(cur_tree_value_->block_meta_->end_key_));
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::inner_get_current(const ObIndexBlockRowHeader *&idx_row_header,
                                                  ObCommonDatumRowkey &endkey,
                                                  int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  bool is_start_key = false;
  bool is_end_key = false;
  idx_row_header = nullptr;
  endkey.reset();
  row_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_ISNULL(cur_tree_value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur tree value is null", K(ret));
  } else {
    idx_row_header = &(cur_tree_value_->header_);
    endkey.set_compact_rowkey(&(cur_tree_value_->block_meta_->end_key_));
    row_offset = cur_tree_value_->co_sstable_row_offset_;
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::get_next(const ObIndexBlockRowHeader *&idx_row_header,
                                         ObCommonDatumRowkey &endkey,
                                         bool &is_scan_left_border,
                                         bool &is_scan_right_border,
                                         const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                                         const char *&agg_row_buf,
                                         int64_t &agg_buf_size,
                                         int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  is_scan_left_border = false;
  is_scan_right_border = false;
  idx_minor_info = nullptr;
  agg_row_buf = nullptr;
  agg_buf_size = 0;
  row_offset = 0;
  bool is_start_key = false;
  bool is_end_key = false;
  int64_t co_sstable_row_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(inner_get_current(idx_row_header, endkey, co_sstable_row_offset))) {
    LOG_WARN("read cur idx row failed", K(ret), KPC(idx_row_header), K(endkey), K(co_sstable_row_offset));
  } else if (OB_UNLIKELY(nullptr == idx_row_header || !endkey.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block row header/endkey", K(ret), KP(idx_row_header), K(endkey));
  } else if (OB_UNLIKELY((idx_row_header->is_data_index() && !idx_row_header->is_major_node()) ||
                         idx_row_header->is_pre_aggregated() ||
                         !idx_row_header->is_major_node())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index row header", K(ret), KPC(idx_row_header));
  } else {
    row_offset = is_co_sstable_ ? co_sstable_row_offset : 0;
  }

  if (OB_SUCC(ret)) {
    if (is_iter_start_) {
      is_start_key = true;
      is_iter_start_ = false;
    }
    storage::ObBlockMetaTreeValue *tmp_tree_value = nullptr;
    if (OB_ISNULL(block_meta_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block meta iterator is null", K(ret));
    } else if (OB_FAIL(block_meta_tree_->get_next_tree_value(btree_iter_, std::abs(iter_step_), tmp_tree_value))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get index block row header failed", K(ret), K(*this));
      } else {
        is_iter_finish_ = true;
        is_end_key = true;
        ret = OB_SUCCESS;
      }
    } else {
      cur_tree_value_ = tmp_tree_value;
    }
    if (OB_SUCC(ret)) {
      is_scan_left_border = is_reverse_scan_ ? is_end_key : is_start_key;
      is_scan_right_border = is_reverse_scan_ ? is_start_key : is_end_key;
    }
  }
  return ret;
}

int ObDDLIndexBlockRowIterator::get_next_meta(const ObDataMacroBlockMeta *&meta)
{
  int ret = OB_SUCCESS;
  meta = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_ISNULL(cur_tree_value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur tree value is null", K(ret));
  } else {
    meta = cur_tree_value_->block_meta_;
    if (is_iter_start_) {
      is_iter_start_ = false;
    }
    storage::ObBlockMetaTreeValue *tmp_tree_value = nullptr;
    if (OB_ISNULL(block_meta_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block meta iterator is null", K(ret));
    } else if (OB_FAIL(block_meta_tree_->get_next_tree_value(btree_iter_, std::abs(iter_step_), tmp_tree_value))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get index block row header failed", K(ret), K(*this));
      } else {
        is_iter_finish_ = true;
        ret = OB_SUCCESS;
      }
    } else {
      cur_tree_value_ = tmp_tree_value;
    }
  }
  return ret;
}

bool ObDDLIndexBlockRowIterator::end_of_block() const
{
  return is_iter_finish_;
}

int ObDDLIndexBlockRowIterator::get_index_row_count(const ObDatumRange &range,
                                                    const bool is_left_border,
                                                    const bool is_right_border,
                                                    int64_t &index_row_count,
                                                    int64_t &data_row_count)
{
  int ret = OB_SUCCESS;
  index_row_count = 0;
  DDLBtreeIterator tmp_iter;
  ObBlockMetaTreeValue *cur_tree_value = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(range));
  } else if (OB_ISNULL(block_meta_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block meta tree is null", K(ret));
  } else if (OB_FAIL(block_meta_tree_->locate_range(range,
                                                    *datum_utils_,
                                                    is_left_border,
                                                    is_right_border,
                                                    is_reverse_scan_,
                                                    tmp_iter,
                                                    cur_tree_value))) {
    LOG_WARN("locate rowkey failed", K(ret), K(range), KPC(datum_utils_), KPC(cur_tree_value));
  } else {
    if (OB_NOT_NULL(cur_tree_value)) {
      ++index_row_count; //first
    }
    while (OB_SUCC(ret)) {
      ObDatumRowkeyWrapper rowkey_wrapper;
      if (OB_FAIL(tmp_iter.get_next(rowkey_wrapper, cur_tree_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ++index_row_count;
      }
    }
    if (OB_FAIL(ret)) {
      index_row_count = 0;
    }
  }
  return ret;
}

/******************             ObDDLSStableAllRangeIterator              **********************/
ObDDLSStableAllRangeIterator::ObDDLSStableAllRangeIterator()
  : is_iter_start_(false),
    is_iter_finish_(true),
    rowkey_read_info_(nullptr),
    index_macro_iter_(),
    iter_param_(),
    cur_index_info_(),
    macro_iter_allocator_("DDLMerge_Iter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    idx_row_allocator_("DDL_IdxRow", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObDDLSStableAllRangeIterator::~ObDDLSStableAllRangeIterator()
{
  reset();
}

void ObDDLSStableAllRangeIterator::reset()
{
  ObIndexBlockRowIterator::reset();
  is_iter_finish_ = true;
  is_iter_start_ = false;
  rowkey_read_info_ = nullptr;
  index_macro_iter_.reset();
  cur_index_info_.reset();
  macro_iter_allocator_.reset();
  idx_row_allocator_.reset();
  iter_param_.reset();
}

void ObDDLSStableAllRangeIterator::reuse()
{
  is_iter_finish_ = true;
  is_iter_start_ = false;
  rowkey_read_info_ = nullptr;
  index_macro_iter_.reset();
  cur_index_info_.reset();
  macro_iter_allocator_.reset();
  idx_row_allocator_.reset();
  iter_param_.reset();
}

int ObDDLSStableAllRangeIterator::init(const ObMicroBlockData &idx_block_data,
                                       const ObStorageDatumUtils *datum_utils,
                                       ObIAllocator *allocator,
                                       const bool is_reverse_scan,
                                       const ObIndexBlockIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_utils) || !datum_utils->is_valid() || !iter_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), KPC(datum_utils), K(iter_param));
  } else {
    ObTablet *cur_tablet = const_cast<ObTablet *>(iter_param.tablet_);
    if (iter_param.sstable_->is_normal_cg_sstable()) {
      if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(rowkey_read_info_))) {
        LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", K(ret));
      }
    } else {
      rowkey_read_info_ = &cur_tablet->get_rowkey_read_info();
    }
    if (OB_SUCC(ret)) {
      iter_param_ = iter_param;

      is_reverse_scan_ = is_reverse_scan;
      iter_step_ = is_reverse_scan_ ? -1 : 1;
      datum_utils_ = datum_utils;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDDLSStableAllRangeIterator::locate_key(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey));
  } else {
    index_macro_iter_.reset();
    ObDatumRange range;
    range.set_start_key(rowkey);
    range.set_end_key(rowkey);
    range.set_left_closed();
    range.set_right_closed();
    ObSSTable *sstable = const_cast<ObSSTable *>(iter_param_.sstable_);
    if (OB_FAIL(index_macro_iter_.open(*sstable, range, *rowkey_read_info_, macro_iter_allocator_, is_reverse_scan_))) {
      LOG_WARN("Fail to open micro block range iterator", K(ret), KPC(iter_param_.sstable_), K(range), KPC(rowkey_read_info_), K(is_reverse_scan_));
      is_iter_finish_ = true;
    } else if (index_macro_iter_.is_iter_end()) {
      ret = OB_BEYOND_THE_RANGE;
    } else {
      is_iter_start_ = true;
      is_iter_finish_ = false;
    }
  }
  LOG_TRACE("locate rowkey in ddl merge sstable", K(ret), K(rowkey), KPC(this));
  return ret;
}

int ObDDLSStableAllRangeIterator::locate_range(const ObDatumRange &range,
                                               const bool is_left_border,
                                               const bool is_right_border,
                                               const bool is_normal_cg)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = const_cast<ObSSTable *>(iter_param_.sstable_);
  index_macro_iter_.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range", K(ret), K(range));
  } else if (OB_FAIL(index_macro_iter_.open(*sstable, range, *rowkey_read_info_, macro_iter_allocator_, is_reverse_scan_))) {
    is_iter_finish_ = true;
    LOG_WARN("block meta tree locate range failed", K(ret), K(range));
  } else if (index_macro_iter_.is_iter_end()) {
    ret = OB_BEYOND_THE_RANGE;
    is_iter_finish_ = true;
  } else {
    is_iter_start_ = true;
    is_iter_finish_ = false;
  }
  LOG_TRACE("Locate range in ddl merge sstable", K(ret), K(range), KPC(this));
  return ret;
}

int ObDDLSStableAllRangeIterator::check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan)
{
  can_blockscan = false;
  return OB_SUCCESS;
}

int ObDDLSStableAllRangeIterator::get_current(const ObIndexBlockRowHeader *&idx_row_header,
                                              ObCommonDatumRowkey &endkey)
{
  endkey.set_compact_rowkey(cur_index_info_.endkey_);
  idx_row_header = cur_index_info_.idx_row_header_;
  return OB_SUCCESS;
}

int ObDDLSStableAllRangeIterator::get_next(const ObIndexBlockRowHeader *&idx_row_header,
                                           ObCommonDatumRowkey &endkey,
                                           bool &is_scan_left_border,
                                           bool &is_scan_right_border,
                                           const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                                           const char *&agg_row_buf,
                                           int64_t &agg_buf_size,
                                           int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  is_scan_left_border = false;
  is_scan_right_border = false;
  idx_minor_info = nullptr;
  agg_row_buf = nullptr;
  agg_buf_size = 0;
  row_offset = 0;

  bool is_start_key = false;
  bool is_end_key = false;
  bool reach_cursor_end = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(index_macro_iter_.get_next_idx_row(idx_row_allocator_, cur_index_info_, row_offset, reach_cursor_end))) {
    LOG_WARN("fail to get next idx info", K(ret), K(cur_index_info_), K(reach_cursor_end), K(index_macro_iter_));
  } else if (OB_UNLIKELY(nullptr == cur_index_info_.idx_row_header_ || nullptr == cur_index_info_.endkey_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block row endkey", K(ret), K(cur_index_info_));
  } else {
    idx_row_header = cur_index_info_.idx_row_header_;
    endkey.set_compact_rowkey(cur_index_info_.endkey_);
    idx_minor_info = cur_index_info_.idx_minor_info_;
    agg_row_buf = cur_index_info_.agg_row_buf_;
    agg_buf_size = cur_index_info_.agg_buf_size_;
    if (is_iter_start_) {
      is_start_key = true;
      is_iter_start_ = false;
    }

    if (OB_SUCC(ret)) {
      if (reach_cursor_end) {
        is_iter_finish_ = true;
        is_end_key = true;
      }
      is_scan_left_border = is_reverse_scan_ ? is_end_key : is_start_key;
      is_scan_right_border = is_reverse_scan_ ? is_start_key : is_end_key;
    }
  }
  return ret;
}

bool ObDDLSStableAllRangeIterator::end_of_block() const
{
  return is_iter_finish_;
}

int ObDDLSStableAllRangeIterator::get_index_row_count(const ObDatumRange &range,
                                                      const bool is_left_border,
                                                      const bool is_right_border,
                                                      int64_t &index_row_count,
                                                      int64_t &data_row_count)
{
  // only single ddl_merge_sstable_without kv can come here, so just return real row_cnt in sstable
  // todo @qilu: refine this, ddl_merge_sstable use RAW or TRANSFORM format iter
  int ret = OB_SUCCESS;
  index_row_count = 0;
  ObIndexBlockMacroIterator tmp_index_macro_iter;
  ObSSTable *sstable = const_cast<ObSSTable *>(iter_param_.sstable_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(range));
  } else if (OB_FAIL(tmp_index_macro_iter.open(*sstable, range, *rowkey_read_info_, macro_iter_allocator_, is_reverse_scan_))) {
    LOG_WARN("tmp all range iter locate range failed", K(ret), K(range));
  } else {
    bool tmp_reach_cursor_end = false;
    ObMicroIndexRowItem tmp_index_item;
    int64_t tmp_row_offset = 0;
    ObArenaAllocator idx_row_allocator("DDL_Row_Cnt", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tmp_index_macro_iter.get_next_idx_row(idx_row_allocator, tmp_index_item, tmp_row_offset, tmp_reach_cursor_end))) {
        if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next idx info", K(ret), K(tmp_index_item), K(tmp_reach_cursor_end), K(tmp_index_macro_iter));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ++index_row_count;
      }
    }
    if (OB_FAIL(ret)) {
      index_row_count = 0;
    }
    tmp_index_item.reset();
    idx_row_allocator.reset();
  }
  return ret;
}

/******************             ObDDLMergeEmptyIterator              **********************/
ObDDLMergeEmptyIterator::ObDDLMergeEmptyIterator()
{
}

ObDDLMergeEmptyIterator::~ObDDLMergeEmptyIterator()
{
}


void ObDDLMergeEmptyIterator::reuse()
{
}

int ObDDLMergeEmptyIterator::init(const ObMicroBlockData &idx_block_data,
                                  const ObStorageDatumUtils *datum_utils,
                                  ObIAllocator *allocator,
                                  const bool is_reverse_scan,
                                  const ObIndexBlockIterParam &iter_param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObDDLMergeEmptyIterator::locate_key(const ObDatumRowkey &rowkey)
{
  return OB_BEYOND_THE_RANGE;
}

int ObDDLMergeEmptyIterator::locate_range(const ObDatumRange &range,
                                          const bool is_left_border,
                                          const bool is_right_border,
                                          const bool is_normal_cg)
{
  return OB_BEYOND_THE_RANGE;
}

int ObDDLMergeEmptyIterator::check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan)
{
  can_blockscan = false;
  return OB_SUCCESS;
}

int ObDDLMergeEmptyIterator::get_current(const ObIndexBlockRowHeader *&idx_row_header,
                                         ObCommonDatumRowkey &endkey)
{
  idx_row_header = nullptr;
  endkey.reset();
  return OB_SUCCESS;
}

int ObDDLMergeEmptyIterator::get_next(const ObIndexBlockRowHeader *&idx_row_header,
                                      ObCommonDatumRowkey &endkey,
                                      bool &is_scan_left_border,
                                      bool &is_scan_right_border,
                                      const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                                      const char *&agg_row_buf,
                                      int64_t &agg_buf_size,
                                      int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  is_scan_left_border = false;
  is_scan_right_border = false;
  idx_minor_info = nullptr;
  agg_row_buf = nullptr;
  agg_buf_size = 0;
  row_offset = 0;
  bool is_start_key = false;
  bool is_end_key = false;

  return OB_SUCCESS;
}

bool ObDDLMergeEmptyIterator::end_of_block() const
{
  return true;
}

int ObDDLMergeEmptyIterator::get_index_row_count(const ObDatumRange &range,
                                                 const bool is_left_border,
                                                 const bool is_right_border,
                                                 int64_t &index_row_count,
                                                 int64_t &data_row_count)
{
  index_row_count = 0;
  return OB_SUCCESS;
}

/******************             ObDDLMergeBlockRowIterator              **********************/
ObDDLMergeBlockRowIterator::ObDDLMergeBlockRowIterator()
  : is_single_sstable_(true),
    is_iter_start_(false),
    is_iter_finish_(true),
    allocator_(nullptr),
    idx_block_data_(nullptr),
    ddl_memtables_(),
    raw_iter_(nullptr),
    transformed_iter_(nullptr),
    empty_merge_iter_(nullptr),
    all_range_iter_(nullptr),
    ddl_memtable_iters_(),
    iters_(),
    consumers_(nullptr),
    consumer_cnt_(0),
    compare_(),
    simple_merge_(nullptr),
    loser_tree_(nullptr),
    endkey_merger_(nullptr),
    query_range_(),
    first_index_item_(),
    iter_param_()
{

}

ObDDLMergeBlockRowIterator::~ObDDLMergeBlockRowIterator()
{
  reset();
}

void ObDDLMergeBlockRowIterator::free_iters(ObIAllocator *allocator)
{
  if (OB_NOT_NULL(transformed_iter_)) {
    transformed_iter_->reset();
    if (OB_NOT_NULL(allocator)) {
      allocator->free(transformed_iter_);
      transformed_iter_ = nullptr;
    }
  }
  if (OB_NOT_NULL(raw_iter_)) {
    raw_iter_->reset();
    if (OB_NOT_NULL(allocator)) {
      allocator->free(raw_iter_);
      raw_iter_ = nullptr;
    }
  }
  if (OB_NOT_NULL(empty_merge_iter_)) {
    empty_merge_iter_->reset();
    if (OB_NOT_NULL(allocator)) {
      allocator->free(empty_merge_iter_);
      empty_merge_iter_ = nullptr;
    }
  }
  if (OB_NOT_NULL(all_range_iter_)) {
    all_range_iter_->reset();
    if (OB_NOT_NULL(allocator)) {
      allocator->free(all_range_iter_);
      all_range_iter_ = nullptr;
    }
  }
  for (int64_t i = 0; i < ddl_memtable_iters_.count(); ++i) {
    ObDDLIndexBlockRowIterator *cur_iter = ddl_memtable_iters_.at(i);
    if (OB_NOT_NULL(cur_iter)) {
      cur_iter->reset();
      if (OB_NOT_NULL(allocator)) {
        allocator->free(cur_iter);
        ddl_memtable_iters_.at(i) = nullptr;
      }
    }
  }
  ddl_memtable_iters_.reset();
  iters_.reset();
}

void ObDDLMergeBlockRowIterator::reset()
{
  is_single_sstable_ = true;
  is_iter_start_ = false;
  is_iter_finish_ = true;
  ObIndexBlockRowIterator::reset();
  free_iters(allocator_);
  // merger
  if (OB_NOT_NULL(simple_merge_)) {
    simple_merge_->reset();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(simple_merge_);
      simple_merge_ = nullptr;
    }
  }
  if (OB_NOT_NULL(loser_tree_)) {
    loser_tree_->reset();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(loser_tree_);
      loser_tree_ = nullptr;
    }
  }
  if (OB_NOT_NULL(consumers_)) {
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(consumers_);
      consumers_ = nullptr;
    }
  }
  compare_.reset();
  consumer_cnt_ = 0;
  endkey_merger_ = nullptr;
  query_range_.reset();
  first_index_item_.reset();
  idx_block_data_ = nullptr;
  ddl_memtables_.reset();
  iter_param_.reset();

  allocator_ = nullptr;
}

void ObDDLMergeBlockRowIterator::reuse()
{
  is_iter_start_ = false;
  is_iter_finish_ = true;
  is_single_sstable_ = true;

  if (OB_NOT_NULL(transformed_iter_)) {
    transformed_iter_->reuse();
  }
  if (OB_NOT_NULL(raw_iter_)) {
    raw_iter_->reuse();
  }
  if (OB_NOT_NULL(empty_merge_iter_)) {
    empty_merge_iter_->reuse();
  }
  if (OB_NOT_NULL(all_range_iter_)) {
    all_range_iter_->reuse();
  }

  for (int64_t i = 0; i < ddl_memtable_iters_.count(); ++i) {
    if (OB_NOT_NULL(ddl_memtable_iters_.at(i))) {
      ddl_memtable_iters_.at(i)->reuse();
    }
  }
  iters_.reuse();

  if (OB_NOT_NULL(simple_merge_)) {
    simple_merge_->reuse();
  }
  if (OB_NOT_NULL(loser_tree_)) {
    loser_tree_->reuse();
  }
  if (OB_NOT_NULL(consumers_)) {
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(consumers_);
      consumers_ = nullptr;
    }
  }
  consumer_cnt_ = 0;
  idx_block_data_ = nullptr;
  ddl_memtables_.reuse();
  endkey_merger_ = nullptr;
  iter_param_.reset();
  ObIndexBlockRowIterator::reset();
}

int ObDDLMergeBlockRowIterator::init(const ObMicroBlockData &idx_block_data,
                                     const ObStorageDatumUtils *datum_utils,
                                     ObIAllocator *allocator,
                                     const bool is_reverse_scan,
                                     const ObIndexBlockIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!iter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(iter_param));
  } else {
    ret = inner_init(idx_block_data, datum_utils, allocator, is_reverse_scan, iter_param, nullptr);
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::init(const ObMicroBlockData &idx_block_data,
                                     const ObStorageDatumUtils *datum_utils,
                                     ObIAllocator *allocator,
                                     const bool is_reverse_scan,
                                     const ObIndexBlockIterParam &iter_param,
                                     const ObIArray<ObDDLMemtable *> &ddl_memtables)
{
  return inner_init(idx_block_data, datum_utils, allocator, is_reverse_scan, iter_param, &ddl_memtables);
}

int ObDDLMergeBlockRowIterator::inner_init(const ObMicroBlockData &idx_block_data,
                                           const ObStorageDatumUtils *datum_utils,
                                           ObIAllocator *allocator,
                                           const bool is_reverse_scan,
                                           const ObIndexBlockIterParam &iter_param,
                                           const ObIArray<ObDDLMemtable *> *ddl_memtables)
{
  int ret = OB_SUCCESS;
  reuse();
  ddl_memtable_iters_.set_attr(ObMemAttr(MTL_ID(), "ddl_mem_iters"));
  iters_.set_attr(ObMemAttr(MTL_ID(), "index_iters"));
  if (OB_UNLIKELY(ObMicroBlockData::DDL_MERGE_INDEX_BLOCK != idx_block_data.type_
        || OB_ISNULL(datum_utils) || !datum_utils->is_valid()
        || OB_ISNULL(allocator)
        || OB_ISNULL(iter_param.tablet_))) { // sstable in iter_param maybe null when slice only contains ddl memtables
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(idx_block_data.type_), KP(allocator), KPC(datum_utils), K(iter_param));
  } else {
    // init ddl_memtables_
    if (nullptr != ddl_memtables) {
      if (OB_FAIL(ddl_memtables_.assign(*ddl_memtables))) {
        LOG_WARN("assign ddl memtables failed", K(ret));
      }
    } else {
      if (OB_FAIL(get_readable_ddl_kvs(iter_param, ddl_memtables_))) {
        LOG_WARN("fail to get readable ddl kvs", K(ret));
      }
    }

    ObIndexBlockRowIterator *sst_index_iter = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_ddl_kv_index_iters(idx_block_data,
                                               datum_utils,
                                               allocator,
                                               is_reverse_scan,
                                               ddl_memtables_))) {
      LOG_WARN("fail to init ddl kv index iters", K(ret), K(iters_), K(ddl_memtables));
    } else if (OB_FAIL(init_sstable_index_iter(idx_block_data,
                                           datum_utils,
                                           allocator,
                                           is_reverse_scan,
                                           iter_param,
                                           sst_index_iter))) {
      LOG_WARN("fail to init sstable index iter", K(ret), K(iters_), KPC(sst_index_iter));
    } else if (iters_.count() > 1) {
      is_single_sstable_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    is_reverse_scan_ = is_reverse_scan;
    iter_step_ = is_reverse_scan_ ? -1 : 1;
    datum_utils_ = datum_utils;
    allocator_ = allocator;
    idx_block_data_ = &idx_block_data;
    iter_param_ = iter_param;
    compare_.reverse_scan_ = is_reverse_scan_;
    compare_.datum_utils_ = datum_utils_;
    if (!is_single_sstable_ && OB_FAIL(init_merger())) {
      LOG_WARN("fail to init merger", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    free_iters(allocator);
  }
  return ret;
}

// reuse or alloc sstable iter
int ObDDLMergeBlockRowIterator::init_sstable_index_iter(const ObMicroBlockData &idx_block_data,
                                                        const ObStorageDatumUtils *datum_utils,
                                                        ObIAllocator *allocator,
                                                        const bool is_reverse_scan,
                                                        const ObIndexBlockIterParam &iter_param,
                                                        ObIndexBlockRowIterator *&sst_index_iter)
{
  int ret = OB_SUCCESS;
  sst_index_iter = nullptr;

  if (OB_SUCC(ret)) {
    void *iter_buf = nullptr;
    if (OB_ISNULL(allocator) || OB_ISNULL(datum_utils) || !datum_utils->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguement", K(ret), KP(allocator), KPC(datum_utils), K(iter_param));
    } else if (OB_ISNULL(iter_param.sstable_) || iter_param.sstable_->is_ddl_merge_empty_sstable()) { // need mock empty_merge_iter when sstable is null, because ddl memtable not support reverse scan
      // EMPTY DDL_MERGE_SSTABLE
      if (OB_NOT_NULL(empty_merge_iter_)) {
      } else if (OB_ISNULL(iter_buf = allocator->alloc(sizeof(ObDDLMergeEmptyIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObDDLMergeEmptyIterator)));
      } else {
        empty_merge_iter_ = new (iter_buf) ObDDLMergeEmptyIterator;
      }
      if (OB_SUCC(ret)) {
        sst_index_iter = empty_merge_iter_;
        FLOG_INFO("empty ddl merge sstable", K(iter_param), K(idx_block_data));
      }
    } else {
      ObSSTableMetaHandle sstable_meta_handle;
      int64_t real_index_tree_height = 0;
      if (OB_FAIL(iter_param.sstable_->get_meta(sstable_meta_handle))) {
        LOG_WARN("failed to get sstable meta handle", K(ret));
      } else {
        real_index_tree_height = sstable_meta_handle.get_sstable_meta().get_index_tree_height(false);
        if (real_index_tree_height < 2) {
          ret= OB_ERR_UNEXPECTED;
          LOG_WARN("invalid index tree_height", K(ret), KPC(iter_param.sstable_), K(real_index_tree_height));
        } else if (real_index_tree_height == 2) { // use basic iter
          if (nullptr == idx_block_data.get_extra_buf()) {
            // RAW
            if (OB_NOT_NULL(raw_iter_)) {
            } else if (OB_ISNULL(iter_buf = allocator->alloc(sizeof(ObRAWIndexBlockRowIterator)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObRAWIndexBlockRowIterator)));
            } else {
              raw_iter_ = new (iter_buf) ObRAWIndexBlockRowIterator;
            }
            if (OB_SUCC(ret)) {
              sst_index_iter = raw_iter_;
            }
          } else {
            // TRANSFORMED
            if (OB_NOT_NULL(transformed_iter_)) {
            } else if (OB_ISNULL(iter_buf = allocator->alloc(sizeof(ObTFMIndexBlockRowIterator)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObTFMIndexBlockRowIterator)));
            } else {
              transformed_iter_ = new (iter_buf) ObTFMIndexBlockRowIterator;
            }
            if (OB_SUCC(ret)) {
              sst_index_iter = transformed_iter_;
            }
          }
        } else {
          // DDL_MERGE_SSTABLE with tree_height > 3
          if (OB_NOT_NULL(all_range_iter_)) {
          } else if (OB_ISNULL(iter_buf = allocator->alloc(sizeof(ObDDLSStableAllRangeIterator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObDDLSStableAllRangeIterator)));
          } else {
            all_range_iter_ = new (iter_buf) ObDDLSStableAllRangeIterator;
          }
          if (OB_SUCC(ret)) {
            sst_index_iter = all_range_iter_;
            FLOG_INFO("ddl merge sstable with higher tree", K(iter_param), K(idx_block_data), KPC(iter_param.sstable_), K(real_index_tree_height));
          }
        }
      }
    }
  }


  if (OB_SUCC(ret)) {
    if (OB_ISNULL(sst_index_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", K(idx_block_data.type_), K(ret));
    } else if (OB_FAIL(sst_index_iter->init(idx_block_data, datum_utils, allocator, is_reverse_scan, iter_param))) {
      LOG_WARN("fail to init iter", K(ret), K(idx_block_data), KPC(sst_index_iter));
    } else if (OB_FAIL(iters_.push_back(sst_index_iter))) {
      LOG_WARN("push back sstable iter failed", K(ret));
    }
  }
  LOG_INFO("init ddl merge iter", K(ret), KPC(sst_index_iter), K(iter_param), K(idx_block_data), KPC(iter_param.sstable_));
  return ret;
}

int ObDDLMergeBlockRowIterator::get_readable_ddl_kvs(const ObIndexBlockIterParam &iter_param,
                                                     ObArray<storage::ObDDLMemtable *> &ddl_memtables)
{
  int ret = OB_SUCCESS;
  // todo qilu :get DDLKV from ls or from tablet_handle now, opt this get DDLKV from MTL() after refactor ddl_kv_mgr
  ddl_memtables.reset();
  ObTabletHandle tmp_tablet_handle;
  int64_t sstable_cg_idx = -1;
  int64_t sstable_slice_idx = -1;
  if (OB_UNLIKELY(!iter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter param", K(ret), K(iter_param), K(lbt()));
  } else {
    ObTablet *cur_tablet = const_cast<ObTablet *>(iter_param.tablet_);
    sstable_cg_idx = iter_param.sstable_->get_key().get_column_group_id();
    sstable_slice_idx = iter_param.sstable_->get_slice_idx();
    ObArray<ObDDLKV *> ddl_kvs;
    if (OB_ISNULL(cur_tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), KP(cur_tablet));
    } else if (OB_FAIL(cur_tablet->get_ddl_kvs(ddl_kvs))) {
      LOG_WARN("failed to get ddl kvs array from tablet", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kvs.count(); ++i) {
        ObDDLKV *ddl_kv = ddl_kvs.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < ddl_kv->get_ddl_memtables().count(); ++j) {
          bool skip = false;
          ObDDLMemtable *cur_ddl_memtable = ddl_kv->get_ddl_memtables().at(j);
          if (OB_NOT_NULL(cur_ddl_memtable)) {
            if (cur_ddl_memtable->is_table_with_scn_range() && OB_NOT_NULL(iter_param.sstable_)) {
              if (cur_ddl_memtable->get_scn_range().is_valid() && iter_param.sstable_->get_end_scn() >= cur_ddl_memtable->get_scn_range().end_scn_) {
                LOG_INFO("smaller scn, skip ddl memtable", K(iter_param.sstable_->get_end_scn()), K(cur_ddl_memtable->get_scn_range()), K(sstable_cg_idx));
                skip = true;
              }
            }
            if (cur_ddl_memtable->get_key().get_column_group_id() != sstable_cg_idx || cur_ddl_memtable->get_slice_idx() != sstable_slice_idx) {
              LOG_INFO("unmatch cg_idx or slice_idx, skip ddl memtable", K(sstable_cg_idx), K(sstable_slice_idx), K(cur_ddl_memtable->get_key()));
              skip = true;
            }
            if (!skip) {
              if (OB_FAIL(ddl_memtables.push_back(cur_ddl_memtable))) {
                LOG_WARN("fail to push back ddl_memtable", K(ret));
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null ddl_memtable", K(ret));
          }
        }
      }
    }
  }
  FLOG_INFO("get ddl readable memtables", K(ret), K(sstable_cg_idx), K(sstable_slice_idx), K(iters_.count()), K(ddl_memtables.count()), K(ddl_memtables));
  return ret;
}

// reuse or alloc ddl_kv iters
int ObDDLMergeBlockRowIterator::init_ddl_kv_index_iters(const ObMicroBlockData &idx_block_data,
                                                        const ObStorageDatumUtils *datum_utils,
                                                        ObIAllocator *allocator,
                                                        const bool is_reverse_scan,
                                                        const ObIArray<storage::ObDDLMemtable *> &ddl_memtables)
{
  int ret = OB_SUCCESS;
  if (!idx_block_data.is_valid() || OB_ISNULL(datum_utils) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter param", K(ret), K(idx_block_data), KP(datum_utils), KP(allocator));
  } else {
    // reset iters count
    if (ddl_memtables.count() > (ddl_memtable_iters_.count())) {
      while (OB_SUCC(ret) && ddl_memtables.count() > (ddl_memtable_iters_.count())) {
        ObDDLIndexBlockRowIterator *cur_ddl_kv_index_iter = nullptr;
        void *iter_buf = nullptr;
        if (OB_ISNULL(iter_buf = allocator->alloc(sizeof(ObDDLIndexBlockRowIterator)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObDDLIndexBlockRowIterator)));
        } else if (FALSE_IT(cur_ddl_kv_index_iter = new (iter_buf) ObDDLIndexBlockRowIterator)) {
        } else if (OB_FAIL(ddl_memtable_iters_.push_back(cur_ddl_kv_index_iter))) {
          LOG_WARN("push back ddl iter failed", K(ret));
          if (OB_NOT_NULL(cur_ddl_kv_index_iter)) {
            cur_ddl_kv_index_iter->~ObDDLIndexBlockRowIterator();
            allocator->free(cur_ddl_kv_index_iter);
          }
        }
      }
    } else if (ddl_memtables.count() < (ddl_memtable_iters_.count())) {
      while (OB_SUCC(ret) && ddl_memtables.count() < (ddl_memtable_iters_.count())) {
        ObDDLIndexBlockRowIterator *tmp_iter = ddl_memtable_iters_.at(ddl_memtable_iters_.count() - 1);
        if (OB_NOT_NULL(tmp_iter)) {
          tmp_iter->~ObDDLIndexBlockRowIterator();
          if (OB_NOT_NULL(allocator)) {
            allocator->free(tmp_iter);
            tmp_iter = nullptr;
          }
        }
        ddl_memtable_iters_.pop_back();
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ddl_memtable_iters_.count() != ddl_memtables.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid iter count", K(ddl_memtable_iters_), K(ddl_memtables));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_memtable_iters_.count(); ++i) {
        if (OB_ISNULL(ddl_memtable_iters_.at(i)) || OB_ISNULL(ddl_memtables.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur iter is null", K(ret), KPC(ddl_memtable_iters_.at(i)));
        } else {
          ObDDLIndexBlockRowIterator *cur_ddl_kv_index_iter = static_cast<ObDDLIndexBlockRowIterator *>(ddl_memtable_iters_.at(i));
          cur_ddl_kv_index_iter->reuse();
          if (OB_FAIL(ddl_memtables.at(i)->init_ddl_index_iterator(datum_utils, is_reverse_scan, cur_ddl_kv_index_iter))) {
            LOG_WARN("fail to init ddl iter", K(ret), K(datum_utils), KPC(cur_ddl_kv_index_iter));
          } else if (OB_FAIL(iters_.push_back(cur_ddl_kv_index_iter))) {
            LOG_WARN("push back into iter array failed", K(ret), K(i), KPC(cur_ddl_kv_index_iter));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::init_merger()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  // step 1:alloc merger
  if (iters_.count() <= ObScanSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    if (OB_NOT_NULL(simple_merge_)) {
      endkey_merger_ = simple_merge_;
    } else {
      if (OB_ISNULL(buf = allocator_->alloc(sizeof(SimpleMerger)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(sizeof(SimpleMerger)));
      } else if (FALSE_IT(simple_merge_ = new (buf) SimpleMerger(compare_))) {
      } else {
        endkey_merger_ = simple_merge_;
      }
    }
  } else {
    if (OB_NOT_NULL(loser_tree_)) {
      endkey_merger_ = loser_tree_;
    } else {
      if (OB_ISNULL(buf = allocator_->alloc(sizeof(MergeLoserTree)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(sizeof(MergeLoserTree)));
      } else if (FALSE_IT(loser_tree_ = new (buf) MergeLoserTree(compare_))) {
      } else {
        endkey_merger_ = loser_tree_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    // step 2:init consumers
    if (OB_ISNULL(consumers_ = static_cast<int64_t *>(
                    allocator_->alloc(sizeof(int64_t) * iters_.count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(iters_.count()));
    } else {
      for (int64_t i = 0; i < iters_.count(); ++i) {
        consumers_[i] = 0;
      }
      consumer_cnt_ = 0;
    }
  }

  // step 3:init merger
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(endkey_merger_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merger is null", K(ret));
    } else if (FALSE_IT(endkey_merger_->reset())) {
    } else if (OB_FAIL(endkey_merger_->init(iters_.count(), *allocator_))) {
      LOG_WARN("fail to init rows merger", K(ret), K(iters_.count()));
    } else if (OB_FAIL(endkey_merger_->open(iters_.count()))) {
      LOG_WARN("fail to open rows merger", K(ret), K(iters_.count()));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(simple_merge_)) {
      simple_merge_->reset();
      if (OB_NOT_NULL(allocator_)) {
        allocator_->free(simple_merge_);
        simple_merge_ = nullptr;
      }
    }
    if (OB_NOT_NULL(loser_tree_)) {
      loser_tree_->reset();
      if (OB_NOT_NULL(allocator_)) {
        allocator_->free(loser_tree_);
        loser_tree_ = nullptr;
      }
    }
    if (OB_NOT_NULL(consumers_)) {
      if (OB_NOT_NULL(allocator_)) {
        allocator_->free(consumers_);
        consumers_ = nullptr;
      }
    }
    consumer_cnt_ = 0;
    endkey_merger_ = nullptr;
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::locate_key(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey));
  } else if (is_single_sstable_) {
    if (OB_UNLIKELY(iters_.count() != 1) || OB_ISNULL(iters_.at(0))  || OB_UNLIKELY(!iters_.at(0)->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid iter", K(ret), K(is_single_sstable_), K(iters_));
    } else if (OB_FAIL(iters_.at(0)->locate_key(rowkey))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("locate rowkey failed", K(ret), K(rowkey), K(iters_));
      }
    }
  } else {
    consumer_cnt_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
      ObIndexBlockRowIterator *cur_iter = iters_.at(i);
      if (OB_ISNULL(cur_iter)  || OB_UNLIKELY(!cur_iter->is_inited())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is null", K(ret), K(i));
      } else if (OB_FAIL(cur_iter->locate_key(rowkey))) {
        if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
          LOG_WARN("locate rowkey failed", K(ret), K(rowkey), KPC(cur_iter));
        } else {
          ret = OB_SUCCESS; // get next iter
        }
      } else {
        consumers_[consumer_cnt_] = i;
        ++consumer_cnt_;
      }
    }

    if (OB_SUCC(ret) && consumer_cnt_ > 0) {
      query_range_.reset();
      query_range_.set_start_key(rowkey);
      query_range_.set_end_key(rowkey);
      query_range_.set_left_closed();
      query_range_.set_right_closed();
      is_iter_start_ = true;
      is_iter_finish_ = false;
      if (is_reverse_scan_ && OB_FAIL(locate_first_endkey())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to locate first endkey", K(ret));
        } else {
          is_iter_finish_ = true;
          ret = OB_SUCCESS; // return OB_ITER_END when get_next
        }
      }
    } else {
      is_iter_finish_ = true;
      if (OB_SUCC(ret)) {
        ret = OB_BEYOND_THE_RANGE;
      }
    }
  }
  LOG_TRACE("merge iter locate key", K(ret), K(rowkey), K(iters_), K(consumer_cnt_));
  return ret;
}

int ObDDLMergeBlockRowIterator::locate_range(const ObDatumRange &range,
                                             const bool is_left_border,
                                             const bool is_right_border,
                                             const bool is_normal_cg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range", K(ret), K(range));
  } else if (is_single_sstable_) {
    if (OB_UNLIKELY(iters_.count() != 1) || OB_ISNULL(iters_.at(0))  || OB_UNLIKELY(!iters_.at(0)->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid iter", K(ret), K(is_single_sstable_), K(iters_));
    } else if (OB_FAIL(iters_.at(0)->locate_range(range, is_left_border, is_right_border, is_normal_cg))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("locate rowkey failed", K(ret), K(range), K(iters_));
      }
    }
  } else {
    consumer_cnt_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
      ObIndexBlockRowIterator *cur_iter = iters_.at(i);
      if (OB_ISNULL(cur_iter) || OB_UNLIKELY(!cur_iter->is_inited())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is null or not inited", K(ret), K(i), KPC(cur_iter));
      } else if (OB_FAIL(cur_iter->locate_range(range, is_left_border, is_right_border, is_normal_cg))) {
        if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
          LOG_WARN("Fail to locate range", K(ret), K(range), K(is_left_border), K(is_right_border), KPC(cur_iter));
        } else {
          ret = OB_SUCCESS; // next iter
        }
      } else {
        consumers_[consumer_cnt_] = i;
        ++consumer_cnt_;
      }
    }

    if (OB_SUCC(ret)) {
      query_range_.reset();
      query_range_ = range;
      if (consumer_cnt_ == 0) {
        ret = OB_BEYOND_THE_RANGE;
        is_iter_finish_ = true;
      } else if (consumer_cnt_ > 0) {
        is_iter_start_ = true;
        is_iter_finish_ = false;
        if (is_reverse_scan_ && OB_FAIL(locate_first_endkey())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to locate first endkey", K(ret));
          } else {
            is_iter_finish_ = true;
            ret = OB_BEYOND_THE_RANGE;
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::get_current(const ObIndexBlockRowHeader *&idx_row_header,
                                            ObCommonDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (is_single_sstable_) {
    // direct get next row from sstable iter
    if (OB_UNLIKELY(iters_.count() != 1) || OB_ISNULL(iters_.at(0))  || OB_UNLIKELY(!iters_.at(0)->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid iters count or iter is nll", K(ret), K(iters_));
    } else if (OB_FAIL(iters_.at(0)->get_current(idx_row_header, endkey))) {
      LOG_WARN("read cur idx row failed", K(ret), KPC(idx_row_header), K(endkey), KPC(iters_.at(0)));
    }
  } else {
    // get next row from loser tree
    bool tmp_border = false;
    int64_t size = 0;
    int64_t offset = 0;
    const char *agg_row_buf = nullptr;
    const ObIndexBlockRowMinorMetaInfo *idx_minor_info = nullptr;
    if (consumer_cnt_ > 0 && OB_FAIL(supply_consume())) {
      LOG_WARN("supply consume failed", K(ret));
    } else if (OB_FAIL(inner_get_next(idx_row_header,
                                      endkey,
                                      tmp_border,
                                      tmp_border,
                                      idx_minor_info,
                                      agg_row_buf,
                                      size,
                                      offset))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to do inner get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::get_next(const ObIndexBlockRowHeader *&idx_row_header,
                                         ObCommonDatumRowkey &endkey,
                                         bool &is_scan_left_border,
                                         bool &is_scan_right_border,
                                         const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                                         const char *&agg_row_buf,
                                         int64_t &agg_buf_size,
                                         int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  is_scan_left_border = false;
  is_scan_right_border = false;
  idx_minor_info = nullptr;
  agg_row_buf = nullptr;
  agg_buf_size = 0;
  row_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (is_single_sstable_) {
    // direct get next row from sstable iter
    if (OB_UNLIKELY(iters_.count() != 1) || OB_ISNULL(iters_.at(0))  || OB_UNLIKELY(!iters_.at(0)->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid iters count or iter is nll", K(ret), K(iters_));
    } else if (OB_FAIL(iters_.at(0)->get_next(idx_row_header,
                                              endkey,
                                              is_scan_left_border,
                                              is_scan_right_border,
                                              idx_minor_info,
                                              agg_row_buf,
                                              agg_buf_size,
                                              row_offset))) {
      LOG_WARN("read cur idx row failed", K(ret), KPC(idx_row_header), K(endkey), KPC(iters_.at(0)));
    }
  } else {
    // get next row from loser tree
    if (is_iter_finish_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(inner_get_next(idx_row_header,
                                      endkey,
                                      is_scan_left_border,
                                      is_scan_right_border,
                                      idx_minor_info,
                                      agg_row_buf,
                                      agg_buf_size,
                                      row_offset))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to do inner get next row", K(ret), KPC(this));
      }
    }
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::supply_consume()
{
  int ret = OB_SUCCESS;
  ObDDLSSTableMergeLoserTreeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < consumer_cnt_; ++i) {
    const int64_t iter_idx = consumers_[i];
    const ObIndexBlockRowHeader *idx_row_header = nullptr;
    ObIndexBlockRowIterator *cur_iter = iters_.at(iter_idx);
    if (OB_ISNULL(cur_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", K(ret), KPC(cur_iter));
    } else if (cur_iter->end_of_block()) {
      //ignore
    } else if (OB_FAIL(cur_iter->get_next(item.header_,
                                          item.end_key_,
                                          item.is_scan_left_border_,
                                          item.is_scan_right_border_,
                                          item.idx_minor_info_,
                                          item.agg_row_buf_,
                                          item.agg_buf_size_,
                                          item.row_offset_))) {
      LOG_WARN("fail to get next row from scanner", K(ret));
    } else {
      item.iter_idx_ = iter_idx;
      if (OB_FAIL(endkey_merger_->push(item))) {
        LOG_WARN("fail to push to loser tree", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // if no new items pushed, the rebuild will quickly exit
    if (OB_FAIL(endkey_merger_->rebuild())) {
      LOG_WARN("fail to rebuild loser tree", K(ret), K(consumer_cnt_));
    } else {
      consumer_cnt_ = 0;
    }
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::inner_get_next(const ObIndexBlockRowHeader *&idx_row_header,
                                               ObCommonDatumRowkey &endkey,
                                               bool &is_scan_left_border,
                                               bool &is_scan_right_border,
                                               const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                                               const char *&agg_row_buf,
                                               int64_t &agg_buf_size,
                                               int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  endkey.reset();
  is_scan_left_border = false;
  is_scan_right_border = false;
  idx_minor_info = nullptr;
  agg_row_buf = nullptr;
  agg_buf_size = 0;
  row_offset = 0;
  const ObDDLSSTableMergeLoserTreeItem *top_item = nullptr;
  int64_t cur_iter_idx = INT64_MAX;
  if (is_reverse_scan_ && is_iter_start_) {
    // reverse scan will save info when locate_first_endkey
    if (!first_index_item_.is_valid()) {
      ret = OB_ITER_END;
    } else {
      idx_row_header = first_index_item_.idx_row_header_;
      endkey.set_compact_rowkey(first_index_item_.rowkey_);
      is_scan_left_border = first_index_item_.is_scan_left_border_;
      is_scan_right_border = first_index_item_.is_scan_right_border_;
      idx_minor_info = first_index_item_.idx_minor_info_;
      agg_row_buf = first_index_item_.agg_row_buf_;
      agg_buf_size = first_index_item_.agg_buf_size_;
      row_offset = first_index_item_.row_offset_;
    }
    is_iter_start_ = false;
    if (OB_SUCC(ret)) {
      if (consumer_cnt_ == 0 && endkey_merger_->empty()) {
        is_iter_finish_ = true;
      }
    }
  } else {
    if (OB_FAIL(supply_consume())) {
      LOG_WARN("supply consume failed", K(ret));
    } else if (endkey_merger_->empty()) {
      ret = OB_ITER_END;
    }

    if (OB_SUCC(ret)) {
      while (OB_SUCC(ret) && !endkey_merger_->empty() && !endkey.is_valid()) {
        bool skip_iter = false;
        if (OB_FAIL(endkey_merger_->top(top_item))) {
          LOG_WARN("fail to get top item", K(ret));
        } else if (OB_LIKELY(endkey_merger_->is_unique_champion())) {
          endkey = top_item->end_key_;
          idx_row_header = top_item->header_;
          cur_iter_idx = top_item->iter_idx_;
          is_scan_left_border = top_item->is_scan_left_border_;
          is_scan_right_border = top_item->is_scan_right_border_;
          idx_minor_info = top_item->idx_minor_info_;
          agg_row_buf = top_item->agg_row_buf_;
          agg_buf_size = top_item->agg_buf_size_;
          row_offset = top_item->row_offset_;
          if (OB_UNLIKELY(nullptr == idx_row_header || !endkey.is_valid() || cur_iter_idx >= iters_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected null index block row header/endkey", K(ret), KP(idx_row_header), K(endkey));
          } else {
            ObIndexBlockRowIterator *cur_iter = iters_.at(cur_iter_idx);
            if (OB_ISNULL(cur_iter)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("cur iter is null", K(ret), KPC(cur_iter));
            } else if (cur_iter->end_of_block()) {
              skip_iter = true;
            }
          }

          if (OB_SUCC(ret) && !is_reverse_scan_) { // not_reverse_scan can quit early
            int tmp_cmp_ret = 0;
            if (OB_FAIL(endkey.compare(query_range_.get_end_key(), *datum_utils_, tmp_cmp_ret))) {
              LOG_WARN("fail to cmp rowkey", K(ret), K(query_range_.get_end_key()), K(endkey), KPC(datum_utils_));
            } else if (tmp_cmp_ret >= 0) {
              // reach endkey, stop get_next
              is_iter_finish_ = true;
              while (OB_SUCC(ret) && !endkey_merger_->empty()) {
                if (OB_FAIL(endkey_merger_->pop())) {
                  LOG_WARN("fail to pop top item", K(ret));
                } else {
                  consumer_cnt_ = 0;
                }
              }
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("exist same endkey!!!", K(ret), KPC(top_item), KPC(endkey_merger_));
        }

        if (OB_SUCC(ret) && !endkey_merger_->empty()) {
          if (OB_FAIL(endkey_merger_->pop())) {
            LOG_WARN("fail to pop top item", K(ret));
          } else if (!skip_iter) {
            consumers_[consumer_cnt_] = cur_iter_idx;
            ++consumer_cnt_;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (consumer_cnt_ == 0 && endkey_merger_->empty()) {
        is_iter_finish_ = true;
      }
    }
  }
  return ret;
}

void ObDDLMergeBlockRowIterator::MergeIndexItem::reset()
{
  if (OB_NOT_NULL(item_allocator_)) {
    if (OB_NOT_NULL(rowkey_)){
      rowkey_->~ObDatumRowkey();
      item_allocator_->free(rowkey_);
      rowkey_ = nullptr;
    }
    if (OB_NOT_NULL(idx_row_header_)){
      idx_row_header_->~ObIndexBlockRowHeader();
      item_allocator_->free(idx_row_header_);
      idx_row_header_ = nullptr;
    }
    if (OB_NOT_NULL(idx_minor_info_)){
      idx_minor_info_->~ObIndexBlockRowMinorMetaInfo();
      item_allocator_->free(idx_minor_info_);
      idx_minor_info_ = nullptr;
    }
    if (OB_NOT_NULL(agg_row_buf_)){
      item_allocator_->free(agg_row_buf_);
      agg_row_buf_ = nullptr;
    }
  }
  item_allocator_ = nullptr;

  is_scan_left_border_ = false;
  is_scan_right_border_ = false;
  agg_buf_size_ = 0;
  row_offset_ = 0;
  iter_index_ = INT64_MAX;
}

int ObDDLMergeBlockRowIterator::MergeIndexItem::init(ObIAllocator *allocator,
                                                     const ObIndexBlockRowHeader *idx_row_header,
                                                     const ObCommonDatumRowkey &endkey,
                                                     const bool is_scan_left_border,
                                                     const bool is_scan_right_border,
                                                     const ObIndexBlockRowMinorMetaInfo *idx_minor_info,
                                                     const char *agg_row_buf,
                                                     const int64_t agg_buf_size,
                                                     const int64_t row_offset,
                                                     const int64_t iter_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator) || OB_ISNULL(idx_row_header) || !endkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemen", K(ret), KP(allocator), KP(idx_row_header), K(endkey), KP(idx_minor_info), KP(agg_row_buf));
  } else {
    item_allocator_ = allocator;
    void *key_buf = nullptr;
    void *header_buf = nullptr;
    void *minor_info_buf = nullptr;
    void *agg_buf = nullptr;
    if (OB_ISNULL(key_buf = item_allocator_->alloc(sizeof(ObDatumRowkey)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObDatumRowkey)));
    } else if (FALSE_IT(rowkey_ = new (key_buf) ObDatumRowkey())) {
    } else if (OB_FAIL(endkey.deep_copy(*rowkey_, *allocator))) {
      LOG_WARN("fail to deep copy rowkey", K(ret), KPC(rowkey_), K(endkey));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(header_buf = item_allocator_->alloc(sizeof(ObIndexBlockRowHeader)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObIndexBlockRowHeader)));
    } else if (FALSE_IT(idx_row_header_ = new (header_buf) ObIndexBlockRowHeader())) {
    } else {
      *idx_row_header_ =*idx_row_header;
    }

    if (OB_FAIL(ret) || OB_ISNULL(idx_minor_info)) {
    } else if (OB_ISNULL(minor_info_buf = item_allocator_->alloc(sizeof(ObIndexBlockRowMinorMetaInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObIndexBlockRowMinorMetaInfo)));
    } else if (FALSE_IT(idx_minor_info_ = new (minor_info_buf) ObIndexBlockRowMinorMetaInfo())) {
    } else {
      *idx_minor_info_ = *idx_minor_info;
    }

    if (OB_FAIL(ret) || OB_ISNULL(agg_row_buf)) {
    } else if (OB_ISNULL(agg_buf = item_allocator_->alloc(agg_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(agg_buf_size));
    } else {
      MEMCPY(agg_buf, agg_row_buf, agg_buf_size);
      agg_row_buf_ = reinterpret_cast<char *>(agg_buf);
    }
  }

  if (OB_SUCC(ret)) {
    is_scan_left_border_ = is_scan_left_border;
    is_scan_right_border_ = is_scan_right_border;
    agg_buf_size_ = agg_buf_size;
    row_offset_ = row_offset;
    iter_index_ = iter_idx;
  }
  return ret;
}

bool ObDDLMergeBlockRowIterator::MergeIndexItem::is_valid()
{
  return OB_NOT_NULL(idx_row_header_)
         && OB_NOT_NULL(rowkey_);
}

int ObDDLMergeBlockRowIterator::locate_first_endkey()
{
  // for reverse scan, find first useful endkey
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_FAIL(supply_consume())) {
    LOG_WARN("supply consume failed", K(ret));
  } else {
    if (endkey_merger_->empty()) {
      ret = OB_ITER_END;
    } else {
      first_index_item_.reset();
      bool find = false;
      while (OB_SUCC(ret) && !endkey_merger_->empty() && !find) {
        if (!first_index_item_.is_valid()) {
          // first round
          const ObDDLSSTableMergeLoserTreeItem *top_item = nullptr;
          bool skip_iter = false;

          if (OB_FAIL(endkey_merger_->top(top_item))) {
            LOG_WARN("fail to get top item", K(ret));
          } else if (OB_LIKELY(endkey_merger_->is_unique_champion())) {
            if (OB_UNLIKELY(nullptr == top_item->header_ || !top_item->end_key_.is_valid() || top_item->iter_idx_ >= iters_.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected null index block row header/endkey", K(ret), KP(top_item->header_), K(top_item->end_key_));
            } else {
              ObIndexBlockRowIterator *tmp_iter = iters_.at(top_item->iter_idx_);
              if (OB_ISNULL(tmp_iter)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("cur iter is null", K(ret), KPC(tmp_iter));
              } else if (tmp_iter->end_of_block()) {
                skip_iter = true;
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("exist same endkey!!!", K(ret), KPC(top_item));
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(first_index_item_.init(allocator_,
                                               top_item->header_,
                                               top_item->end_key_,
                                               top_item->is_scan_left_border_,
                                               top_item->is_scan_right_border_,
                                               top_item->idx_minor_info_,
                                               top_item->agg_row_buf_,
                                               top_item->agg_buf_size_,
                                               top_item->row_offset_,
                                               top_item->iter_idx_))) {
              LOG_WARN("fail to init first_index_item_", K(ret));
            } else if (!endkey_merger_->empty() && OB_FAIL(endkey_merger_->pop())) {
              LOG_WARN("fail to pop top item", K(ret), K(endkey_merger_));
            } else if (!skip_iter) {
              consumers_[consumer_cnt_] = first_index_item_.iter_index_;
              ++consumer_cnt_;
            }
          }
        } else {
          // regular round
          if (OB_FAIL(supply_consume())) {
            LOG_WARN("supply consume failed", K(ret));
          } else {
            const ObDDLSSTableMergeLoserTreeItem *top_item = nullptr;
            bool skip_iter = false;
            if (OB_FAIL(endkey_merger_->top(top_item))) {
              LOG_WARN("fail to get top item", K(ret));
            } else if (OB_LIKELY(endkey_merger_->is_unique_champion())) {
              if (OB_UNLIKELY(nullptr == top_item->header_ || !top_item->end_key_.is_valid() || top_item->iter_idx_ >= iters_.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("Unexpected null index block row header/endkey", K(ret), K(top_item->end_key_), KP(top_item->header_));
              } else if (top_item->iter_idx_ == first_index_item_.iter_index_) {
                // continuous item from same iter, find
                find = true; //first_index_item_
              } else {
                int tmp_cmp_ret = 0;
                // top_item->end_key_ means first_index_item_.start_key
                if (OB_FAIL(top_item->end_key_.compare(query_range_.get_end_key(), *datum_utils_, tmp_cmp_ret))) {
                  LOG_WARN("fail to cmp rowkey", K(ret), K(query_range_.get_end_key()), K(top_item->end_key_), KPC(datum_utils_));
                } else if (tmp_cmp_ret < 0) {
                  find = true; //first_index_item_
                } else {
                  if (tmp_cmp_ret == 0) {
                    find = true;
                  }

                  ObIndexBlockRowIterator *cur_iter = iters_.at(top_item->iter_idx_);
                  bool tmp_is_iter_end = false;
                  if (OB_ISNULL(cur_iter)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("cur iter is null", K(ret), KPC(cur_iter));
                  } else if (cur_iter->end_of_block()) {
                    skip_iter = true;
                  }

                  if (OB_SUCC(ret)) {
                    first_index_item_.reset();
                    if (OB_FAIL(first_index_item_.init(allocator_,
                                                       top_item->header_,
                                                       top_item->end_key_,
                                                       top_item->is_scan_left_border_,
                                                       top_item->is_scan_right_border_,
                                                       top_item->idx_minor_info_,
                                                       top_item->agg_row_buf_,
                                                       top_item->agg_buf_size_,
                                                       top_item->row_offset_,
                                                       top_item->iter_idx_))) {
                      LOG_WARN("fail to init first_index_item_", K(ret));
                    } else if (!endkey_merger_->empty() && OB_FAIL(endkey_merger_->pop())) {
                      LOG_WARN("fail to pop top item", K(ret), K(endkey_merger_));
                    } else if (!skip_iter) {
                      consumers_[consumer_cnt_] = first_index_item_.iter_index_;
                      ++consumer_cnt_;
                    }
                  }
                }
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("exist same endkey!!!", K(ret), KPC(top_item));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !find && !first_index_item_.is_valid()) {
        ret = OB_ITER_END;
        is_iter_finish_ = true;
      }
    }
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey));
  } else {
    if (is_single_sstable_) {
      if (iters_.count() != 1 || OB_ISNULL(iters_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid iters count or iter is nll", K(ret), K(iters_));
      } else if (OB_FAIL(iters_.at(0)->check_blockscan(rowkey, can_blockscan))) {
        LOG_WARN("fail to check blockscan", K(ret), KPC(iters_.at(0)), K(rowkey));
      }
    } else {
      // with ddl kvs, cannot blockscan
      // todo @qilu :reopen later
      can_blockscan = false;
    }
  }
  return ret;
}

int ObDDLMergeBlockRowIterator::switch_context(ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("datum utils is null", K(ret), KP(datum_utils));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
      ObIndexBlockRowIterator *cur_iter = iters_.at(i);
      if (OB_UNLIKELY(!cur_iter->is_inited())) {
        ret = OB_NOT_INIT;
        LOG_WARN("not init yet", K(ret), KPC(cur_iter));
      } else if (OB_FAIL(cur_iter->switch_context(datum_utils))) {
        LOG_WARN("fail to switch context", K(ret), KPC(datum_utils));
      }
    }
    if (OB_SUCC(ret)) {
      datum_utils_ = datum_utils;
      compare_.datum_utils_ = datum_utils_;
    }
  }
  return ret;
}

bool ObDDLMergeBlockRowIterator::end_of_block() const
{
  bool bret = true;
  int ret = OB_SUCCESS;
  if (is_single_sstable_) {
    // direct get next row from sstable iter
    if (OB_UNLIKELY(iters_.count() != 1) || OB_ISNULL(iters_.at(0))  || OB_UNLIKELY(!iters_.at(0)->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid iters count or iter is nll", K(ret), K(iters_));
    } else {
      bret = iters_.at(0)->end_of_block();
    }
  } else {
    bret = is_iter_finish_;
  }
  return bret;
}

int ObDDLMergeBlockRowIterator::get_index_row_count(const ObDatumRange &range,
                                                    const bool is_left_border,
                                                    const bool is_right_border,
                                                    int64_t &index_row_count,
                                                    int64_t &data_row_count)
{
  int ret = OB_SUCCESS;
  index_row_count = 0;
  data_row_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Iter not opened yet", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(range));
  } else if (is_single_sstable_) {
    if (OB_UNLIKELY(iters_.count() != 1) || OB_ISNULL(iters_.at(0))  || OB_UNLIKELY(!iters_.at(0)->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid iters count or iter is nll", K(ret), K(iters_));
    } else if (OB_FAIL(iters_.at(0)->get_index_row_count(range, is_left_border, is_right_border, index_row_count, data_row_count))) {
      LOG_WARN("fail to check blockscan", K(ret), KPC(iters_.at(0)), K(range));
    } else {
      // the index block iterator of single sstable does NOT set data_row_count within get_index_row_count interface.
      // just get sstable row count from meta cache
      data_row_count = OB_ISNULL(iter_param_.sstable_) ? 0 : iter_param_.sstable_->get_row_count();
    }
  } else {
    ObDDLMergeBlockRowIterator *tmp_merge_iter = nullptr;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObDDLMergeBlockRowIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObDDLMergeBlockRowIterator)));
    } else if (FALSE_IT(tmp_merge_iter = new (buf) ObDDLMergeBlockRowIterator())) {
    } else if (OB_ISNULL(idx_block_data_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("idx block data is null", K(ret));
    } else if (OB_FAIL(tmp_merge_iter->inner_init(*idx_block_data_,
                                            datum_utils_,
                                            allocator_,
                                            is_reverse_scan_,
                                            iter_param_,
                                            &ddl_memtables_))) {
      LOG_WARN("fail to init iter", K(ret), KPC(idx_block_data_), KPC(tmp_merge_iter));
    } else if (OB_FAIL(tmp_merge_iter->locate_range(range, is_left_border, is_right_border, true/*is_normal_cg*/))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to locate range", K(ret), K(range), K(is_left_border), K(is_right_border), KPC(tmp_merge_iter));
      }
    } else {
      int ret = OB_SUCCESS;
      ObCommonDatumRowkey endkey;
      const ObIndexBlockRowHeader *idx_row_header = nullptr;
      const ObIndexBlockRowMinorMetaInfo *idx_minor_info = nullptr;
      const char *idx_data_buf = nullptr;
      const char *agg_row_buf = nullptr;
      int64_t agg_buf_size = 0;
      int64_t row_offset = 0;
      bool is_scan_left_border = false;
      bool is_scan_right_border = false;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(tmp_merge_iter->get_next(idx_row_header, endkey, is_scan_left_border, is_scan_right_border, idx_minor_info, agg_row_buf, agg_buf_size, row_offset))) {
          LOG_WARN("get next idx block row failed", K(ret), KP(idx_row_header), K(endkey), K(is_reverse_scan_));
        } else if (OB_UNLIKELY(nullptr == idx_row_header || !endkey.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null index block row header/endkey", K(ret), KPC(tmp_merge_iter), KP(idx_row_header), K(endkey));
        } else {
          ++index_row_count;
          data_row_count += idx_row_header->get_row_count();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("get merge idx row cnt success", K(index_row_count), K(data_row_count));
      }
    }

    //free iter buf
    if (OB_NOT_NULL(allocator_)) {
      if (OB_NOT_NULL(tmp_merge_iter)) {
        tmp_merge_iter->~ObDDLMergeBlockRowIterator();
        allocator_->free(tmp_merge_iter);
      }
    }
  }
  return ret;
}

ObUnitedSliceRowIterator::ObUnitedSliceRowIterator()
  : allocator_(nullptr), merge_iter_(nullptr), idx_block_data_(nullptr),
    slice_count_(0), start_slice_idx_(-1), end_slice_idx_(-1), cur_slice_idx_(-1), is_iter_end_(false)
{
  LOG_TRACE("[slice index iter]: construct", KP(this));
}

ObUnitedSliceRowIterator::~ObUnitedSliceRowIterator()
{
  reset();
}

void ObUnitedSliceRowIterator::reset()
{
  if (OB_NOT_NULL(merge_iter_)) {
    if (OB_NOT_NULL(allocator_)) {
      merge_iter_->~ObDDLMergeBlockRowIterator();
      allocator_->free(merge_iter_);
      merge_iter_ = nullptr;
    }
  }
  idx_block_data_ = nullptr;
  iter_param_.reset();
  range_.reset();
  row_offsets_.reset();
  start_datum_offset_.reuse();
  end_datum_offset_.reuse();
  slice_count_ = 0;
  start_slice_idx_ = -1;
  end_slice_idx_ = -1;
  cur_slice_idx_ = -1;
  is_iter_end_ = false;
  slice_sstable_handle_.reset();
  slice_root_block_.reset();
  abs_datum_offset_.reuse();
  abs_endkey_.reset();
  allocator_ = nullptr;
  ObIndexBlockRowIterator::reset();
}

void ObUnitedSliceRowIterator::reuse()
{
  if (OB_NOT_NULL(merge_iter_)) {
    merge_iter_->reuse();
  }
  idx_block_data_ = nullptr;
  iter_param_.reset();
  range_.reset();
  row_offsets_.reset();
  start_datum_offset_.reuse();
  end_datum_offset_.reuse();
  slice_count_ = 0;
  start_slice_idx_ = -1;
  end_slice_idx_ = -1;
  cur_slice_idx_ = -1;
  is_iter_end_ = false;
  slice_sstable_handle_.reset();
  slice_root_block_.reset();
  abs_datum_offset_.reuse();
  abs_endkey_.reuse();
}

int ObUnitedSliceRowIterator::init(
    const ObMicroBlockData &idx_block_data,
    const ObStorageDatumUtils *datum_utils,
    ObIAllocator *allocator,
    const bool is_reverse_scan,
    const ObIndexBlockIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_UNLIKELY(!idx_block_data.is_valid() || ObMicroBlockData::DDL_MERGE_INDEX_BLOCK != idx_block_data.type_
      || OB_ISNULL(datum_utils) || !datum_utils->is_valid()
      || OB_ISNULL(allocator)
      || !iter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(idx_block_data), KPC(datum_utils), KP(allocator), K(iter_param));
  } else {
    if (OB_FAIL(init_slice_info(iter_param))) {
      LOG_WARN("init slice info failed", K(ret));
    } else if (iter_param.sstable_->is_normal_cg_sstable() && abs_endkey_.assign(&abs_datum_offset_, 1)) {
      LOG_WARN("assign datum offset failed", K(ret));
    } else if (OB_ISNULL(merge_iter_) && OB_ISNULL(merge_iter_ = OB_NEWx(ObDDLMergeBlockRowIterator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed for ddl merge iter", K(ret));
    } else {
      merge_iter_->reuse();
    }
    if (OB_SUCC(ret)) {
      allocator_ = allocator;
      idx_block_data_ = &idx_block_data;
      datum_utils_ = datum_utils;
      is_reverse_scan_ = is_reverse_scan;
      iter_param_ = iter_param;
      iter_step_ = is_reverse_scan_ ? -1 : 1;
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(merge_iter_)) {
      merge_iter_->reset();
      if (OB_NOT_NULL(allocator)) {
        allocator->free(merge_iter_);
        merge_iter_ = nullptr;
      }
    }
    is_inited_ = false;
  }
  return ret;
}

struct AddSliceRowCountOp
{
public:
  explicit AddSliceRowCountOp(const int64_t row_count) : row_count_(row_count) {}
  ~AddSliceRowCountOp() {}
  void  operator() (hash::HashMapPair<int64_t, int64_t> &entry) { entry.second += row_count_; }
private:
  int64_t row_count_;
};

int ObUnitedSliceRowIterator::init_slice_info(const ObIndexBlockIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator ddl_sstable_iter;
  ObArray<ObDDLKV *> ddl_kvs;
  hash::ObHashMap<int64_t/*slice_idx*/, int64_t/*row_count*/> slice_row_count_map;
  int64_t cg_idx = -1;
  if (OB_UNLIKELY(!iter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(iter_param));
  } else if (OB_FAIL(iter_param.tablet_->get_ddl_sstables(ddl_sstable_iter))) {
    LOG_WARN("get ddl sstable iter failed", K(ret));
  } else if (OB_FAIL(iter_param.tablet_->get_ddl_kvs(ddl_kvs))) {
    LOG_WARN("failed to get ddl kvs array from tablet", K(ret));
  } else if (0 == ddl_sstable_iter.count() && 0 == ddl_kvs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("both ddl sstables and ddl kv is empty", K(ret), K(ddl_sstable_iter), K(ddl_kvs));
  } else if (OB_FAIL(slice_row_count_map.create(97, ObMemAttr(MTL_ID(), "slice_rc_map")))) {
    LOG_WARN("create slice row count map failed", K(ret));
  } else {
    cg_idx = iter_param.sstable_->get_column_group_id();
    ObArray<ObSSTable *> cg_slices; // slices for cg idx 0(rowkey cg)
    int64_t max_slice_idx = -1;
    while (OB_SUCC(ret)) {
      ObITable *cur_ddl_sstable = nullptr;
      if (OB_FAIL(ddl_sstable_iter.get_next(cur_ddl_sstable))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get ddl sstable failed", K(ret), KP(cur_ddl_sstable));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(cur_ddl_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl sstable is null", K(ret), KP(cur_ddl_sstable));
      } else if (cur_ddl_sstable->get_column_group_id() != 0) {
        // do nothing, only count rows for cg 0
      } else if (OB_FAIL(cg_slices.push_back(static_cast<ObSSTable *>(cur_ddl_sstable)))) {
        LOG_WARN("push back cg sstable failed", K(ret), KPC(cur_ddl_sstable));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kvs.count(); ++i) {
      ObDDLKV *cur_ddl_kv = ddl_kvs.at(i);
      if (OB_ISNULL(cur_ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current ddl kv is null", K(ret), K(i), K(cur_ddl_kv));
      } else {
        const ObIArray<ObDDLMemtable *> &ddl_memtables = cur_ddl_kv->get_ddl_memtables();
        for (int64_t j = 0; OB_SUCC(ret) && j < ddl_memtables.count(); ++j) {
          ObDDLMemtable *cur_ddl_memtable = ddl_memtables.at(j);
          if (OB_ISNULL(cur_ddl_memtable)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ddl memtable is null", K(ret), KP(cur_ddl_memtable), K(i), K(j), KPC(cur_ddl_kv));
          } else if (cur_ddl_memtable->get_column_group_id() != 0) {
            // do nothing, only count rows for cg 0
          } else if (OB_FAIL(cg_slices.push_back(cur_ddl_memtable))) {
            LOG_WARN("push back cg sstable failed", K(ret), KPC(cur_ddl_memtable));
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < cg_slices.count(); ++i) {
      ObSSTable *cur_ddl_sstable = cg_slices.at(i);
      ObSSTableMetaHandle sstable_meta_handle;
      if (OB_FAIL(cur_ddl_sstable->get_meta(sstable_meta_handle))) {
        LOG_WARN("get sstable meta handle failed", K(ret));
      } else {
        int64_t cur_slice_idx = cur_ddl_sstable->get_slice_idx();
        int64_t cur_row_count = sstable_meta_handle.get_sstable_meta().get_row_count();
        AddSliceRowCountOp add_op(cur_row_count);
        int64_t accumulate_row_count = 0;
        if (cur_ddl_sstable->get_column_group_id() != 0) {
          // do nothing, only count rows for cg 0
        } else if (OB_FAIL(slice_row_count_map.atomic_refactored(cur_slice_idx, add_op))) {
          if (OB_HASH_NOT_EXIST == ret) {
            if (OB_FAIL(slice_row_count_map.set_refactored(cur_slice_idx, cur_row_count))) {
              LOG_WARN("set row count into map failed", K(ret), K(cur_slice_idx), K(cur_row_count));
            }
          } else {
            LOG_WARN("get slice idx from map failed", K(ret), K(cur_slice_idx));
          }
        }
        if (OB_SUCC(ret)) {
          max_slice_idx = MAX(max_slice_idx, cur_ddl_sstable->get_slice_idx());
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (max_slice_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid max slice id", K(ret), K(max_slice_idx));
      }
    }
    if (OB_SUCC(ret)) {
      row_offsets_.reuse();
      slice_count_ = max_slice_idx + 1;
      int64_t accumulate_row_count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < slice_count_; ++i) {
        int64_t cur_slice_row_count = 0;
        if (OB_FAIL(row_offsets_.push_back(accumulate_row_count))) {
          LOG_WARN("push back row offset failed", K(ret));
        } else if (OB_FAIL(slice_row_count_map.get_refactored(i, cur_slice_row_count))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("get row count from map failed", K(ret), K(i));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (cur_slice_row_count < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid row count", K(ret), K(cur_slice_row_count));
        }
        if (OB_SUCC(ret)) {
          accumulate_row_count += cur_slice_row_count;
        }
      }
    }
  }
  FLOG_INFO("[slice index iter] init slice info", KP(this), K(ret), K(slice_count_), K(row_offsets_), K(cg_idx));
  return ret;
}

int ObUnitedSliceRowIterator::convert_slice_offset(int64_t abs_row_offset, int64_t &slice_idx, int64_t &slice_row_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(abs_row_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(abs_row_offset));
  } else {
    ObArray<int64_t>::iterator found_it = std::lower_bound(row_offsets_.begin(), row_offsets_.end(), abs_row_offset);
    if (found_it == row_offsets_.end()) {
      slice_idx = slice_count_ - 1;
    } else if (*found_it == abs_row_offset) {
      slice_idx = found_it - row_offsets_.begin();
    } else {
      slice_idx = MAX(0, found_it - row_offsets_.begin() - 1);
    }
    if (slice_idx < 0 || slice_idx >= row_offsets_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid slice idx", K(ret), K(slice_idx), K(row_offsets_.count()), K(abs_row_offset), K(row_offsets_));
    } else {
      slice_row_offset = abs_row_offset - row_offsets_.at(slice_idx);
    }
  }
  return ret;
}

int ObUnitedSliceRowIterator::locate_slice_idx_by_key(const ObDatumRowkey &rowkey, int64_t &slice_idx)
{
  int ret = OB_SUCCESS;
  slice_idx = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(rowkey), K(iter_param_.sstable_->get_key()));
  } else {
    ObDDLMergeBlockRowIterator slice_merge_iter;
    ObIndexBlockIterParam slice_iter_param;
    ObArray<ObDDLMemtable *> slice_ddl_memtables;
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < slice_count_; ++i) {
      slice_merge_iter.reuse();
      slice_iter_param.reset();
      slice_ddl_memtables.reuse();

      const ObIndexBlockRowHeader *idx_row_header = nullptr;
      ObCommonDatumRowkey endkey;
      bool is_scan_left_border;
      bool is_scan_right_border;
      const ObIndexBlockRowMinorMetaInfo *idx_minor_info = nullptr;
      const char *agg_row_buf = nullptr;
      int64_t agg_buf_size = -1;
      int64_t row_offset = -1;

      if (OB_FAIL(prepare_slice_query_param(i, slice_iter_param, slice_ddl_memtables))) {
        LOG_WARN("prepare slice query param", K(ret), K(i));
      } else if (OB_FAIL(slice_merge_iter.init(slice_root_block_, datum_utils_, allocator_, false/*is_reverse_scan*/, slice_iter_param, slice_ddl_memtables))) {
        LOG_WARN("init slice merge iter failed", K(ret), K(i));
      } else if (OB_FAIL(slice_merge_iter.locate_key(rowkey))) {
        if (OB_BEYOND_THE_RANGE != ret) {
          LOG_WARN("locate key failed", K(ret), K(rowkey), K(i));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (slice_merge_iter.end_of_block()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(slice_merge_iter.get_next(idx_row_header, endkey, is_scan_left_border, is_scan_right_border, idx_minor_info, agg_row_buf, agg_buf_size, row_offset))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next failed", K(ret), K(i));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        slice_idx = i;
        is_found = true;
      }
    }

    if (OB_SUCC(ret) && !is_found) {
      ret = OB_BEYOND_THE_RANGE;
    }
  }
  return ret;
}

int ObUnitedSliceRowIterator::locate_range(
    const ObDatumRange &range,
    const bool is_left_border,
    const bool is_right_border,
    const bool is_normal_cg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid() || (iter_param_.sstable_->is_normal_cg_sstable() ^ is_normal_cg))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(range), K(iter_param_.sstable_->get_key()), K(is_normal_cg));
  } else if ((!is_left_border && !is_right_border) || range.is_whole_range()) {
    start_slice_idx_ = 0;
    end_slice_idx_ = slice_count_ - 1;
    range_ = range;
    range_.set_whole_range();
  } else if (!iter_param_.sstable_->is_normal_cg_sstable()) {
    start_slice_idx_ = 0;
    end_slice_idx_ = slice_count_ - 1;
    range_ = range;
    if (OB_SUCC(ret) && !range_.get_start_key().is_min_rowkey()) {
      if (OB_FAIL(locate_slice_idx_by_key(range_.get_start_key(), start_slice_idx_))) {
        if (OB_BEYOND_THE_RANGE != ret) {
          LOG_WARN("locate slice idx failed", K(ret), K(range_.get_start_key()));
        } else {
          is_iter_end_ = true;
        }
      }
    }

    if (OB_SUCC(ret) && !range_.get_end_key().is_max_rowkey()) {
      if (OB_FAIL(locate_slice_idx_by_key(range_.get_end_key(), end_slice_idx_))) {
        if (OB_BEYOND_THE_RANGE != ret) {
          LOG_WARN("locate slice idx failed", K(ret), K(range_.get_end_key()));
        } else {
          end_slice_idx_ = slice_count_ - 1;
          ret = OB_SUCCESS;
        }
      }
    }
  } else {
    start_slice_idx_ = 0;
    end_slice_idx_ = slice_count_ - 1;
    range_ = range;
    int64_t slice_offset = -1;
    if (OB_SUCC(ret) && !range_.get_start_key().is_min_rowkey()) {
      const int64_t abs_start_offset = range.start_key_.get_datum(0).get_int();
      if (OB_FAIL(convert_slice_offset(abs_start_offset, start_slice_idx_, slice_offset))) {
        LOG_WARN("locate slice offset failed", K(ret), K(abs_start_offset), K(row_offsets_));
      } else {
        start_datum_offset_.set_int(slice_offset);
        range_.start_key_.assign(&start_datum_offset_, 1);
      }
    }

    if (OB_SUCC(ret) && !range_.get_end_key().is_max_rowkey()) {
      const int64_t abs_end_offset = range.end_key_.get_datum(0).get_int();
      if (OB_FAIL(convert_slice_offset(abs_end_offset, start_slice_idx_, slice_offset))) {
        LOG_WARN("locate slice offset failed", K(ret), K(abs_end_offset), K(row_offsets_));
      } else {
        end_datum_offset_.set_int(slice_offset);
        range_.end_key_.assign(&start_datum_offset_, 1);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (start_slice_idx_ < 0 || end_slice_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid slice idx", K(ret), K(start_slice_idx_), K(end_slice_idx_));
    } else {
      merge_iter_->reuse();
      cur_slice_idx_ = is_reverse_scan_ ? end_slice_idx_ : start_slice_idx_;
    }
  }
  FLOG_INFO("[slice index iter] locate range", KP(this), K(ret), K(range_), K(cur_slice_idx_), K(start_slice_idx_), K(end_slice_idx_), K(is_iter_end_), K(is_reverse_scan_), K(iter_step_));
  return ret;
}

int ObUnitedSliceRowIterator::locate_key(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    range_.start_key_ = rowkey;
    range_.end_key_ = rowkey;
    range_.set_left_closed();
    range_.set_right_closed();
    const bool is_left_border = true;
    const bool is_right_border = true;
    const bool is_normal_cg = iter_param_.sstable_->is_normal_cg_sstable();
    if (OB_FAIL(locate_range(range_, is_left_border, is_right_border, is_normal_cg))) {
      LOG_WARN("locate range failed", K(ret), K(range_), K(rowkey), K(is_normal_cg));
    }
  }
  return ret;
}

int ObUnitedSliceRowIterator::get_current(const ObIndexBlockRowHeader *&idx_row_header, ObCommonDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(merge_iter_->get_current(idx_row_header, endkey))) {
    LOG_WARN("get current failed", K(ret));
  } else if (iter_param_.sstable_->is_normal_cg_sstable()) {
    endkey.set_compact_rowkey(&abs_endkey_);
  }
  return ret;
}

int ObUnitedSliceRowIterator::prepare_slice_query_param(const int64_t slice_idx, ObIndexBlockIterParam &slice_iter_param, ObIArray<ObDDLMemtable *> &slice_ddl_memtables)
{
  int ret = OB_SUCCESS;
  slice_root_block_.reset();
  slice_iter_param.reset();
  slice_ddl_memtables.reset();
  ObTableStoreIterator ddl_sstable_iter;
  ObArray<ObDDLKV *> ddl_kvs;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_idx));
  } else if (OB_FAIL(iter_param_.tablet_->get_ddl_sstables(ddl_sstable_iter))) {
    LOG_WARN("get ddl sstable iter failed", K(ret));
  } else if (OB_FAIL(iter_param_.tablet_->get_ddl_kvs(ddl_kvs))) {
    LOG_WARN("failed to get ddl kvs array from tablet", K(ret));
  } else {
    const int64_t cg_idx = iter_param_.sstable_->get_column_group_id();
    // prepare slice iter param
    slice_iter_param.tablet_ = iter_param_.tablet_;
    bool found_sstable = false;
    while (OB_SUCC(ret) && !found_sstable) {
      ObTableHandleV2 cur_table_handle;
      if (OB_FAIL(ddl_sstable_iter.get_next(cur_table_handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get ddl sstable failed", K(ret), K(cur_table_handle));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(cur_table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl sstable is null", K(ret), K(cur_table_handle));
      } else if (slice_idx == cur_table_handle.get_table()->get_slice_idx()) {
        ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(cur_table_handle.get_table());
        ObSSTableWrapper cg_sstable_wrapper;
        ObSSTable *cg_sstable = nullptr;
        if (OB_FAIL(co_sstable->fetch_cg_sstable(cg_idx, cg_sstable_wrapper))) {
          LOG_WARN("get all tables failed", K(ret));
        } else if (OB_FAIL(cg_sstable_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
          LOG_WARN("get sstable failed", K(ret));
        } else if (OB_FAIL(cg_sstable->get_index_tree_root(slice_root_block_))) {
          LOG_WARN("get index tree root block failed", K(ret));
        } else {
          slice_sstable_handle_ = cg_sstable_wrapper.get_meta_handle();
          slice_iter_param.sstable_ = cg_sstable;
          found_sstable = true;
        }
      }
    }
    if (OB_SUCC(ret) && !found_sstable) {
      slice_root_block_.type_ = ObMicroBlockData::DDL_MERGE_INDEX_BLOCK;
      slice_root_block_.buf_ = DDL_EMPTY_SSTABLE_DUMMY_INDEX_DATA_BUF;
      slice_root_block_.size_ = DDL_EMPTY_SSTABLE_DUMMY_INDEX_DATA_SIZE;
      LOG_TRACE("[slice index iter] slice sstable not found, mock slice root block", K(ret), K(slice_idx), K(cg_idx), K(slice_root_block_));
    }

    // prepare slice ddl memtables
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kvs.count(); ++i) {
      ObDDLKV *cur_ddl_kv = ddl_kvs.at(i);
      if (OB_ISNULL(cur_ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current ddl kv is null", K(ret), K(i), K(cur_ddl_kv));
      } else {
        const ObIArray<ObDDLMemtable *> &kv_ddl_memtables = cur_ddl_kv->get_ddl_memtables();
        for (int64_t j = 0; OB_SUCC(ret) && j < kv_ddl_memtables.count(); ++j) {
          ObDDLMemtable *cur_ddl_memtable = kv_ddl_memtables.at(j);
          ObSSTableMetaHandle sstable_meta_handle;
          if (OB_ISNULL(cur_ddl_memtable)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ddl memtable is null", K(ret), KP(cur_ddl_memtable), K(i), K(j), KPC(cur_ddl_kv));
          } else if (cur_ddl_memtable->get_column_group_id() == cg_idx && cur_ddl_memtable->get_slice_idx() == slice_idx) {
            if (OB_FAIL(slice_ddl_memtables.push_back(cur_ddl_memtable))) {
              LOG_WARN("push back ddl memtable", K(ret), KPC(cur_ddl_memtable));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitedSliceRowIterator::get_next(
    const ObIndexBlockRowHeader *&idx_row_header,
    ObCommonDatumRowkey &endkey,
    bool &is_scan_left_border,
    bool &is_scan_right_border,
    const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
    const char *&agg_row_buf,
    int64_t &agg_buf_size,
    int64_t &row_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    const bool is_normal_cg = iter_param_.sstable_->is_normal_cg_sstable();
    while (OB_SUCC(ret) & !is_iter_end_) {

      if (!merge_iter_->is_inited()) {
        ObDatumRange query_range = range_;
        bool is_left_border = cur_slice_idx_ == start_slice_idx_;
        bool is_right_border = cur_slice_idx_ == end_slice_idx_;
        if (is_normal_cg) { // only normal cg need convert query range for each slice
          if (!is_left_border) {
            query_range.start_key_.set_min_rowkey();
          }
          if (!is_right_border) {
            query_range.end_key_.set_max_rowkey();
          }
        } else {
          // force locate
          is_left_border = true;
          is_right_border = true;
        }
        ObIndexBlockIterParam slice_iter_param;
        ObArray<ObDDLMemtable *> slice_ddl_memtables;
        if (OB_FAIL(prepare_slice_query_param(cur_slice_idx_, slice_iter_param, slice_ddl_memtables))) {
          LOG_WARN("get slice param failed", K(ret), K(cur_slice_idx_));
        } else if (OB_FAIL(merge_iter_->init(slice_root_block_, datum_utils_, allocator_, is_reverse_scan_, slice_iter_param, slice_ddl_memtables))) {
          LOG_WARN("init merge iter for next slice failed", K(ret), K(slice_iter_param), K(cur_slice_idx_));
        } else if (OB_FAIL(merge_iter_->locate_range(query_range, is_left_border, is_right_border, is_normal_cg))) {
          if (OB_BEYOND_THE_RANGE != ret) {
            LOG_WARN("merge iter locate range failed", K(ret), K(cur_slice_idx_));
          } else {
            ret = OB_ITER_END;
          }
        }
        LOG_TRACE("[slice index iter] init slice iter", K(ret), K(cur_slice_idx_), K(query_range), K(slice_iter_param), K(slice_ddl_memtables));
      }

      if (OB_SUCC(ret)) {
        if (merge_iter_->end_of_block()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(merge_iter_->get_next(idx_row_header, endkey, is_scan_left_border, is_scan_right_border, idx_minor_info, agg_row_buf, agg_buf_size, row_offset))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next failed", K(ret), K(is_reverse_scan_), K(cur_slice_idx_));
          }
        } else {
          // convert slice row offset to tablet row offset
          row_offset += row_offsets_.at(cur_slice_idx_);
          if (is_normal_cg) {
            abs_endkey_.datums_[0].set_int(row_offset);
            endkey.set_compact_rowkey(&abs_endkey_);
          }
          is_scan_left_border = cur_slice_idx_ == start_slice_idx_ && is_scan_left_border;
          is_scan_right_border = cur_slice_idx_ == end_slice_idx_ && is_scan_right_border;
          break;
        }
      }

      if (OB_ITER_END == ret) {
        // check has next slice
        if ((!is_reverse_scan_ && cur_slice_idx_ >= end_slice_idx_) || (is_reverse_scan_ && cur_slice_idx_ <= start_slice_idx_)) {
          is_iter_end_ = true;
        } else {
          cur_slice_idx_ += iter_step_;
          merge_iter_->reuse();
          ret = OB_SUCCESS;
        }
      }

    }
  }
  LOG_TRACE("[slice index iter] get next", KP(this), K(ret), K(is_reverse_scan_), K(is_iter_end_), K(cur_slice_idx_), K(row_offset), K(endkey), KPC(idx_row_header));
  return ret;
}

int ObUnitedSliceRowIterator::check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan)
{
  int ret = OB_SUCCESS;
  UNUSED(rowkey);
  can_blockscan = false;
  return ret;
}

bool ObUnitedSliceRowIterator::end_of_block() const
{
  return is_iter_end_;
}

int ObUnitedSliceRowIterator::get_index_row_count(
    const ObDatumRange &range,
    const bool is_left_border,
    const bool is_right_border,
    int64_t &index_row_count,
    int64_t &data_row_count)
{
  int ret = OB_SUCCESS;
  index_row_count = 0;
  data_row_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range));
  } else {
    ObUnitedSliceRowIterator tmp_united_iter;
    if (OB_FAIL(tmp_united_iter.init(*idx_block_data_, datum_utils_, allocator_, false/*is_reverse_scan*/, iter_param_))) {
      LOG_WARN("init united slice iter failed", K(ret));
    } else if (OB_FAIL(tmp_united_iter.locate_range(range, is_left_border, is_right_border, iter_param_.sstable_->is_normal_cg_sstable()))) {
      if (OB_BEYOND_THE_RANGE != ret) {
        LOG_WARN("locate range failed", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      const int64_t start_idx = tmp_united_iter.start_slice_idx_;
      const int64_t end_idx = tmp_united_iter.end_slice_idx_;
      ObDDLMergeBlockRowIterator slice_merge_iter;
      ObIndexBlockIterParam slice_iter_param;
      ObArray<ObDDLMemtable *> slice_ddl_memtables;
      for (int64_t i = start_idx; OB_SUCC(ret) && i <= end_idx; ++i) {
        slice_merge_iter.reuse();
        slice_iter_param.reset();
        slice_ddl_memtables.reuse();
        int64_t cur_index_row_count = 0;
        int64_t cur_data_row_count = 0;
        if (OB_FAIL(prepare_slice_query_param(i, slice_iter_param, slice_ddl_memtables))) {
          LOG_WARN("prepare slice query param failed", K(ret), K(i));
        } else if (OB_FAIL(slice_merge_iter.init(slice_root_block_, datum_utils_, allocator_, is_reverse_scan_, slice_iter_param, slice_ddl_memtables))) {
          LOG_WARN("init merge iter failed", K(ret), K(i));
        } else if (OB_FAIL(slice_merge_iter.get_index_row_count(range, is_left_border, is_right_border, cur_index_row_count, cur_data_row_count))) {
          if (OB_BEYOND_THE_RANGE != ret) {
            LOG_WARN("get index row count failed", K(ret), K(i), K(range));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (cur_index_row_count < 0 || cur_data_row_count < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected row count", K(ret), K(cur_data_row_count), K(cur_index_row_count), K(i), K(slice_iter_param), K(slice_ddl_memtables));
        } else {
          index_row_count += cur_index_row_count;
          data_row_count += cur_data_row_count;
        }
      }
    }
  }
  LOG_TRACE("[slice index iter] get index row count", KP(this), K(ret), K(index_row_count), K(data_row_count), K(range));
  return ret;
}

int ObUnitedSliceRowIterator::switch_context(ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(datum_utils));
  } else if (OB_FAIL(merge_iter_->switch_context(datum_utils))) {
    LOG_WARN("merge iter switch context failed", K(ret));
  } else {
    datum_utils_ = datum_utils;
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
