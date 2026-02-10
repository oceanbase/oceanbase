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

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "ob_i_compaction_filter.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/ob_trans_version_skip_index_util.h"
#include "storage/compaction/ob_partition_merge_iter.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace blocksstable;

namespace compaction
{

const char *ObICompactionFilter::ObFilterRetStr[] =
{
  "KEEP",
  "REMOVE",
};

bool ObICompactionFilter::is_valid_filter_ret(const ObFilterRet filter_ret)
{
  return filter_ret >= FILTER_RET_KEEP && filter_ret < FILTER_RET_MAX;
}

void ObICompactionFilter::ObFilterStatistics::add(const ObFilterStatistics &other)
{
  filter_block_row_cnt_ += other.filter_block_row_cnt_;
  filter_sstable_cnt_ += other.filter_sstable_cnt_;
  for (int i = 0; i < FILTER_RET_MAX; ++i) {
    row_cnt_[i] += other.row_cnt_[i];
  }
  for (int i = 0; i < ObBlockOp::OP_MAX; ++i) {
    micro_cnt_[i] += other.micro_cnt_[i];
    macro_cnt_[i] += other.macro_cnt_[i];
  }
}

void ObICompactionFilter::ObFilterStatistics::row_inc(ObFilterRet filter_ret)
{
  if (OB_LIKELY(is_valid_filter_ret(filter_ret))) {
    row_cnt_[filter_ret]++;
  }
}

void ObICompactionFilter::ObFilterStatistics::micro_inc(ObBlockOp::BlockOp block_op, const int64_t filter_row_cnt)
{
  if (OB_LIKELY(block_op < ObBlockOp::OP_MAX)) {
    micro_cnt_[block_op]++;
    if (ObBlockOp::OP_FILTER == block_op) {
      filter_block_row_cnt_ += filter_row_cnt;
    }
  }
}

void ObICompactionFilter::ObFilterStatistics::macro_inc(ObBlockOp::BlockOp block_op, const int64_t filter_row_cnt)
{
  if (OB_LIKELY(block_op < ObBlockOp::OP_MAX)) {
    macro_cnt_[block_op]++;
    if (ObBlockOp::OP_FILTER == block_op) {
      filter_block_row_cnt_ += filter_row_cnt;
    }
  }
}

void ObICompactionFilter::ObFilterStatistics::reset()
{
  MEMSET(this, 0, sizeof(*this));
}

const char *ObICompactionFilter::get_filter_ret_str(const int64_t idx)
{
  STATIC_ASSERT(static_cast<int64_t>(FILTER_RET_MAX) == ARRAYSIZEOF(ObFilterRetStr), "filter ret string is mismatch");
  const char * ret_str = nullptr;
  if (idx < 0 || idx >= FILTER_RET_MAX) {
    ret_str = "invalid_ret";
  } else {
    ret_str = ObFilterRetStr[idx];
  }
  return ret_str;
}

const char *ObICompactionFilter::ObFilterTypeStr[] =
{
  "TX_DATA_MINOR",
  "MDS_MINOR_FILTER_DATA",
  "MDS_MINOR_CROSS_LS",
  "MDS_IN_MEDIUM_INFO",
  "MEMBER_TABLE_MINOR",
  "ROWSCN_FILTER",
  "MLOG_PURGE_FILTER",
  "FILTER_TYPE_MAX"
};

const char *ObICompactionFilter::get_filter_type_str(const int64_t idx)
{
  STATIC_ASSERT(static_cast<int64_t>(FILTER_TYPE_MAX + 1) == ARRAYSIZEOF(ObFilterTypeStr), "filter type string is mismatch");
  const char * ret_str = nullptr;
  if (idx < 0 || idx >= FILTER_TYPE_MAX) {
    ret_str = "invalid_type";
  } else {
    ret_str = ObFilterTypeStr[idx];
  }
  return ret_str;
}

int64_t ObICompactionFilter::ObFilterStatistics::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    gene_info(buf, buf_len, pos);
  }
  return pos;
}

void ObICompactionFilter::ObFilterStatistics::gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || pos >= buf_len) {
  } else {
    int64_t array_idx = 0;
    J_OBJ_START();
    if (filter_sstable_cnt_ > 0) {
      J_KV(K_(filter_sstable_cnt));
      J_COMMA();
    }
    if (filter_block_row_cnt_ > 0) {
      J_KV(K_(filter_block_row_cnt));
      J_COMMA();
    }
  #define PRINT(array, array_max, array_name,TYPE, str_func) \
    for (int i = 0; i < array_max; ++i) { \
      if (array[i] > 0) { \
        if (print_array_cnt++ > 0) { \
          J_COMMA(); \
        } \
        J_NAME(array_name); \
        J_COLON(); \
        J_OBJ_START(); \
        int64_t item_idx = 0; \
        for (int i = 0; i < array_max; ++i) { \
          if (array[i] > 0) { \
            if (item_idx++ > 0) { \
              J_COMMA(); \
            } \
            J_KV(str_func(static_cast<TYPE>(i)), array[i]); \
          } \
        } \
        J_OBJ_END(); \
        break; \
      } \
    }
    int64_t print_array_cnt = 0;
    PRINT(row_cnt_, FILTER_RET_MAX, "row", ObFilterRet, get_filter_ret_str);
    PRINT(micro_cnt_, ObBlockOp::OP_MAX, "micro", ObBlockOp::BlockOp, ObBlockOp::get_block_op_str);
    PRINT(macro_cnt_, ObBlockOp::OP_MAX, "macro", ObBlockOp::BlockOp, ObBlockOp::get_block_op_str);
  #undef PRINT
    J_OBJ_END();
  }
}

/*
 * ObCompactionFilterHandle
 */
int ObCompactionFilterHandle::init(ObICompactionFilter *compaction_filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(compaction_filter)) {
    // do nothing
  } else if (OB_NOT_NULL(compaction_filter_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret), KP(compaction_filter));
  } else {
    compaction_filter_ = compaction_filter;
    filter_statistics_.reset();
  }
  return ret;
}

int ObCompactionFilterHandle::filter(
  const ObDatumRow &row,
  ObICompactionFilter::ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  if (row.is_uncommitted_row()) {
    filter_ret = ObICompactionFilter::FILTER_RET_KEEP;
  } else if (OB_ISNULL(compaction_filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter", K(ret), K(row), KP_(compaction_filter));
  } else if (OB_FAIL(compaction_filter_->filter(row, filter_ret))) {
    LOG_WARN("Failed to filter row", K(ret), K(row));
  } else if (OB_UNLIKELY(!ObICompactionFilter::is_valid_filter_ret(filter_ret))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get wrong filter ret", K(filter_ret));
  } else {
    filter_statistics_.row_inc(filter_ret);
  }
  return ret;
}

int ObCompactionFilterHandle::get_block_op_from_filter(
  const ObMacroBlockDesc &macro_desc,
  ObBlockOp &block_op)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(inner_get_block_op_from_filter(macro_desc, block_op))) {
    filter_statistics_.macro_inc(
      block_op.block_op_,
      macro_desc.row_count_);
  }
  return ret;
}

int ObCompactionFilterHandle::inner_get_block_op_from_filter(
  const ObMacroBlockDesc &macro_desc,
  ObBlockOp &block_op)
{
  int ret = OB_SUCCESS;
  block_op.reset();
  if (macro_desc.contain_uncommitted_row_) {
    block_op.set_open(); // max version for uncommitted macro block is not accurate
  } else if (OB_UNLIKELY(nullptr == compaction_filter_ || !macro_desc.is_valid_with_macro_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter or invalid macro block", K(ret), K(macro_desc));
  } else if (compaction_filter_->get_trans_version_col_idx() < 0) {
    block_op.set_open();
  } else {
    ObTransVersionSkipIndexInfo skip_index_info;
    if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
        macro_desc, compaction_filter_->get_trans_version_col_idx(), skip_index_info))) {
      LOG_WARN("Failed to read min max snapshot", K(ret), K(macro_desc));
    } else if (OB_FAIL(compaction_filter_->get_filter_op(skip_index_info.min_snapshot_, skip_index_info.max_snapshot_, block_op))) {
      LOG_WARN("Failed to get filter op", K(ret), K(macro_desc));
    } else {
      LOG_INFO("[COMPACTION FILTER] get_block_op_from_filter macro", K(block_op), K(skip_index_info), K(macro_desc));
    }
  }
  return ret;
}

int ObCompactionFilterHandle::get_block_op_from_filter_for_minor(
  const blocksstable::ObMacroBlockDesc &macro_desc,
  const ObMinorRowkeyOutputState &rowkey_state,
  ObBlockOp &block_op)
{
  int ret = OB_SUCCESS;
  ObBlockOp orig_block_op;
  ObBlockOp new_block_op;
  if (OB_SUCC(inner_get_block_op_from_filter(macro_desc, orig_block_op))) {
    new_block_op = orig_block_op;
    if (new_block_op.is_filter()) {
      if (rowkey_state.have_rowkey_output_row()) {
        new_block_op.set_open();
      }
    } else if (new_block_op.is_none() && rowkey_state.is_recycling()) {
      new_block_op.set_open();
    }
    if (OB_UNLIKELY(!new_block_op.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected block op", K(ret), K(new_block_op));
    } else {
      block_op = new_block_op;
      LOG_TRACE("[COMPACTION FILTER] get_block_op_from_filter_for_minor", K(block_op), K(orig_block_op), K(new_block_op), K(rowkey_state), K(macro_desc));
      filter_statistics_.macro_inc(
        block_op.block_op_,
        macro_desc.row_count_);
    }
  }
  return ret;
}

int ObCompactionFilterHandle::get_block_op_from_filter(
  const ObMicroBlock &micro_block,
  ObBlockOp &filter_op)
{
  int ret = OB_SUCCESS;
  filter_op.reset();
  if (micro_block.header_.contain_uncommitted_rows_) {
    filter_op.set_open();
  } else if (OB_UNLIKELY(nullptr == compaction_filter_ || !micro_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter orinvalid micro block", K(ret), K(micro_block));
  } else if (compaction_filter_->get_trans_version_col_idx() < 0) {
    filter_op.set_open();
  } else {
    ObTransVersionSkipIndexInfo skip_index_info;
    if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
        micro_block, compaction_filter_->get_trans_version_col_idx(), skip_index_info))) {
      LOG_WARN("Failed to read min max snapshot", K(ret), K(micro_block));
    } else if (OB_FAIL(compaction_filter_->get_filter_op(skip_index_info.min_snapshot_, skip_index_info.max_snapshot_, filter_op))) {
      LOG_WARN("Failed to get filter op", K(ret), K(micro_block));
    } else {
      filter_statistics_.micro_inc(
        filter_op.block_op_,
        micro_block.header_.row_count_);
      LOG_INFO("[COMPACTION FILTER] get_block_op_from_filter micro", K(filter_op), K(skip_index_info), K(micro_block), K(filter_statistics_));
    }
  }
  return ret;
}



} // namespace compaction
} // namespace oceanbase
