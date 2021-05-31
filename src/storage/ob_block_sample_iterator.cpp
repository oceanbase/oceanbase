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

#include "storage/ob_block_sample_iterator.h"

namespace oceanbase {
using namespace common;
namespace storage {

ObBlockSampleIterator::ObBlockSampleIterator(const SampleInfo& sample_info)
    : ObISampleIterator(sample_info),
      scan_merge_(nullptr),
      block_num_(0),
      row_cnt_from_cur_block_(0),
      has_open_block_(false),
      table_handle_(),
      access_ctx_(nullptr)
{}

ObBlockSampleIterator::~ObBlockSampleIterator()
{}

int ObBlockSampleIterator::open(ObMultipleScanMerge& scan_merge, ObTableAccessContext& access_ctx,
    const ObExtStoreRange& range, const ObGetTableParam& get_table_param, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObTableHandle table_handle;
  ObSSTable* sstable = NULL;
  if (OB_UNLIKELY(NULL == get_table_param.partition_store_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(get_table_param));
  } else if (OB_FAIL(get_table_param.partition_store_->get_last_major_sstable(
                 get_table_param.sample_info_.table_id_, table_handle))) {
    STORAGE_LOG(WARN, "fail to get last major sstable", K(ret));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    STORAGE_LOG(WARN, "fail to get last major sstable", K(ret));
  } else if (OB_FAIL(table_handle_.set_table(sstable))) {
    STORAGE_LOG(WARN, "fail to set sstable", K(ret));
  } else if (OB_FAIL(micro_block_iterator_.init(sample_info_->table_id_, table_handle_, range, is_reverse_scan))) {
    STORAGE_LOG(WARN, "failed to init micro_block_iterator_", K(ret));
  } else {
    range_ = range;
    scan_merge_ = &scan_merge;
    has_open_block_ = false;
    block_num_ = 0;
    row_cnt_from_cur_block_ = 0;
    access_ctx_ = &access_ctx;
  }
  return ret;
}

void ObBlockSampleIterator::reuse()
{
  micro_block_iterator_.reset();
  range_.reset();
  block_num_ = 0;
  row_cnt_from_cur_block_ = 0;
  has_open_block_ = false;
  table_handle_.reset();
}

void ObBlockSampleIterator::reset()
{
  micro_block_iterator_.reset();
  range_.reset();
  scan_merge_ = nullptr;
  block_num_ = 0;
  row_cnt_from_cur_block_ = 0;
  has_open_block_ = false;
  table_handle_.reset();
}

int ObBlockSampleIterator::get_next_row(ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  const ObStoreRange* range = nullptr;
  row = nullptr;
  if (OB_ISNULL(scan_merge_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "block sample iterator is not opened", K(ret));
  } else {
    while (OB_SUCC(ret) && (!has_open_block_ || ++row_cnt_from_cur_block_ > ROW_CNT_LIMIT_PER_BLOCK ||
                               OB_FAIL(scan_merge_->get_next_row(row)))) {
      if (OB_ITER_END == ret || OB_SUCCESS == ret) {
        ret = OB_SUCCESS;
        has_open_block_ = false;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(micro_block_iterator_.get_next_range(range))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "failed to get next range", K(ret));
          }
        } else if (OB_ISNULL(range)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "range is null", K(ret), K_(block_num));
        } else if (return_this_sample(block_num_++)) {
          STORAGE_LOG(DEBUG, "open a range", K(*range), K_(block_num));
          micro_range_.reset();
          micro_range_.get_range() = *range;
          if (OB_FAIL(open_range(micro_range_))) {
            STORAGE_LOG(WARN, "failed to open range", K(ret), K_(micro_range));
          }
        }
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to get next row from ObBlockSampleIterator", K(ret));
    }
  }
  return ret;
}

int ObBlockSampleIterator::open_range(ObExtStoreRange& range)
{
  int ret = OB_SUCCESS;
  scan_merge_->reuse();
  allocator_.reuse();
  access_ctx_->allocator_->reuse();
  if (OB_FAIL(range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
    STORAGE_LOG(WARN, "failed to covert to collation free range", K(range), K(ret));
  } else if (OB_FAIL(scan_merge_->open(range))) {
    STORAGE_LOG(WARN, "failed to open ObMultipleScanMerge", K(ret), K(range));
  } else {
    has_open_block_ = true;
    row_cnt_from_cur_block_ = 0;
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
