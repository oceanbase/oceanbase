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

#include "ob_block_sample_iterator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/access/ob_ddl_block_sample_iterator.h"
#include "lib/random/ob_random.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

/*
 * -------------------------------------------- ObDDLBlockSampleRangeIterator --------------------------------------------
 */
void ObDDLBlockSampleIterator::reuse()
{
  is_opened_ = false;
  ObBlockSampleIterator::reuse();
  reservoir_.reuse();
}

void ObDDLBlockSampleIterator::reset()
{
  is_opened_ = false;
  ObBlockSampleIterator::reset();
  reservoir_.reset();
}

int ObDDLBlockSampleIterator::open(ObMultipleScanMerge &scan_merge,
                                   ObTableAccessContext &access_ctx,
                                   const blocksstable::ObDatumRange &range,
                                   ObGetTableParam &get_table_param,
                                   const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  uint64_t batch_size = 0;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "the ddl block sample iterator has been initialized", K(ret));
  } else if (OB_UNLIKELY(!access_ctx.is_valid() || !get_table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "there are invalid argument", K(ret), K(access_ctx), K(get_table_param));
  }  else if (OB_FAIL(range_iterator_.open(get_table_param,
                                           range,
                                           *access_ctx.stmt_allocator_,
                                           sample_info_->percent_,
                                           is_reverse_scan,
                                           SampleInfo::SampleMethod::DDL_BLOCK_SAMPLE))) {
    STORAGE_LOG(WARN, "fail to initialize micro block iterator", K(ret));
  } else {
    scan_merge_ = &scan_merge;
    has_opened_range_ = false;
    access_ctx_ = &access_ctx;
    read_info_ = &get_table_param.tablet_iter_.get_tablet()->get_rowkey_read_info();
  }
  if (FAILEDx(reservoir_block_sample())) {
    STORAGE_LOG(WARN, "fail to do reservoir sampling", K(ret));
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObDDLBlockSampleIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRange *range = nullptr;
  row = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "block sample iterator is not opened", K(ret));
  } else {
    while (OB_SUCC(ret) && (!has_opened_range_ || OB_FAIL(scan_merge_->get_next_row(row)))) {
      if (OB_ITER_END == ret || OB_SUCCESS == ret) {
        ret = OB_SUCCESS;
        has_opened_range_ = false;
      }
      if (OB_SUCC(ret)) {
        if (!reservoir_.empty()) {
          ObDatumRange *micro_range = nullptr;
          micro_range_.reset();
          if (OB_FAIL(reservoir_.pop_back(micro_range))) {
            STORAGE_LOG(WARN, "failed to pop back range", K(ret));
          } else if (OB_UNLIKELY(nullptr == micro_range)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "the micro range is null", K(ret));
          } else {
            micro_range_ = *micro_range;
            if (OB_FAIL(open_range(micro_range_))) {
              STORAGE_LOG(WARN, "failed to open range", K(ret), K(micro_range_));
            }
          }
        } else {
          ret = OB_ITER_END;
        }
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret)  {
      STORAGE_LOG(WARN, "failed to get next row from ObDDLBlockSampleIterator", K(ret), K(block_num_));
    }
  }
  return ret;
}

int ObDDLBlockSampleIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRange *range = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "block sample iterator is not opened", K(ret));
  } else {
    while (OB_SUCC(ret) && (!has_opened_range_ || OB_FAIL(inner_get_next_rows(count, capacity)))) {
      if (OB_ITER_END == ret || OB_SUCCESS == ret) {
        ret = OB_SUCCESS;
        has_opened_range_ = false;
      }
      if (OB_SUCC(ret)) {
        if (!reservoir_.empty()) {
          ObDatumRange *micro_range = nullptr;
          micro_range_.reset();
          if (OB_FAIL(reservoir_.pop_back(micro_range))) {
            STORAGE_LOG(WARN, "failed to pop back range", K(ret));
          } else if (OB_UNLIKELY(nullptr == micro_range)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "the micro range is null", K(ret));
          } else {
            micro_range_ = *micro_range;
            if (OB_FAIL(open_range(micro_range_))) {
              STORAGE_LOG(WARN, "failed to open range", K(ret), K(micro_range_));
            }
          }
        } else {
          ret = OB_ITER_END;
        }
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret)  {
      STORAGE_LOG(WARN, "failed to get next row from ObDDLBlockSampleIterator", K(ret));
    }
  }
  return ret;
}

int ObDDLBlockSampleIterator::open_range(blocksstable::ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  scan_merge_->reuse();
  access_ctx_->scan_mem_->reuse_arena();
  if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "the are invalid argument", K(ret), K(range));
  } else if (OB_FAIL(inner_open_range(range))) {
    STORAGE_LOG(WARN, "failed to inner open range", K(ret), K(range));
  }
  return ret;
}

// start private function
int ObDDLBlockSampleIterator::reservoir_block_sample()
{
  int ret = OB_SUCCESS;
  const ObDatumRange *next_range = nullptr;
  common::ObRandom random;
  uint64_t curr_num = 0;
  reservoir_.reuse();
  random.seed(ObTimeUtility::current_time());

  while (OB_SUCC(ret)) {
    ObDatumRange *range = nullptr;
    if (OB_FAIL(range_iterator_.get_next_range(next_range))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to get next range", K(ret));
      }
    } else if (OB_ISNULL(next_range)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "range is null", K(ret));
    } else if (OB_UNLIKELY(nullptr == (range = OB_NEWx(ObDatumRange, &range_allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory for datum range", K(ret));
    } else if (OB_FAIL(range->partial_copy(/* src */ *next_range, range_allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy datum range", K(ret));
    } else if (reservoir_.count() < ObBlockSampleRangeIterator::EXPECTED_OPEN_RANGE_NUM) {
      if (OB_FAIL(reservoir_.push_back(range))) {
        STORAGE_LOG(WARN, "fail to push back range", K(ret), KPC(range));
      } else {
        ++curr_num;
      }
    } else {
      uint64_t idx = random.get(0, curr_num++);
      if (idx < ObBlockSampleRangeIterator::EXPECTED_OPEN_RANGE_NUM) {
        if (OB_UNLIKELY(idx >= reservoir_.count() || nullptr == reservoir_.at(idx))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "the idx is invalid or range is null", K(ret));
        } else {
          reservoir_.at(idx)->~ObDatumRange();
          reservoir_.at(idx) = nullptr;
          reservoir_.at(idx) = range;
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

} // end storage
} // end oceabase
