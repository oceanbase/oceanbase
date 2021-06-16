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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_SCANNER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_SCANNER_H_

#include "lib/container/ob_bit_set.h"
#include "ob_micro_block_reader.h"
#include "ob_sparse_micro_block_reader.h"

namespace oceanbase {
namespace common {
class ObStoreRange;
}
namespace storage {
class ObStoreRow;
}
namespace blocksstable {
class ObMicroBlockScanner {
public:
  ObMicroBlockScanner();
  ~ObMicroBlockScanner();
  void reset();
  // when schema version changed use column map to read row
  int set_scan_param(const ObColumnMap& column_map, const common::ObStoreRange& range,
      const ObMicroBlockData& block_data, const bool is_bound_block, const bool is_reverse_scan,
      const common::ObRowStoreType store_type, common::ObIAllocator& allocator);
  // use with no set_scan_param
  int estimate_row_count(common::ObIAllocator& allocator, const common::ObStoreRange& range,
      const ObColumnMap& column_map, const ObMicroBlockData& block_data, const common::ObRowStoreType store_type,
      bool consider_multi_version, int64_t& logical_row_count, int64_t& physical_row_count);
  int get_next_row(const storage::ObStoreRow*& row);
  //
  template <typename Filter>
  int filter_pushdown(Filter& filter, common::ObBitSet<>& output);

  int advance_cursor(const int64_t adv)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(adv < 0)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "adv is smaller than 0", K(ret), K(adv));
    } else if (reverse_scan_) {
      current_ = (adv > current_) ? ObIMicroBlockReader::INVALID_ROW_INDEX : current_ - adv;
    } else {
      current_ = (current_ + adv) > last_ ? ObIMicroBlockReader::INVALID_ROW_INDEX : current_ + adv;
    }
    if (OB_SUCC(ret) && ObIMicroBlockReader::INVALID_ROW_INDEX == current_) {
      ret = common::OB_BEYOND_THE_RANGE;
    }
    return ret;
  }

  int64_t get_current_cursor() const
  {
    return current_;
  }

  int set_current_cursor(const int64_t pos)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(pos < -1)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected pos", K(ret), K(pos));
    } else {
      current_ = pos;
    }
    return ret;
  }

  // Used for multi-version reversed scan, we need swith the scan direction to forward
  // 1. reverse scan to find the first row of the current schema rowkey
  // 2. scan forward from the first row until the last row of the current schema rowkey
  int switch_to_forward_scan()
  {
    int ret = common::OB_SUCCESS;
    if (reverse_scan_) {
      int64_t tmp = start_;
      start_ = last_;
      last_ = tmp;
      reverse_scan_ = false;
      // current_ is the next read postion -_-
      // - in reverse scan, current_ = TRUE_current - 1
      // - in forward scan, current_ = TRUE_current + 1
      // we need add the current plus two when switch scan direction
      current_ += 2;
    }
    return ret;
  }

  // Used for multi-version reversed scan, we need swith the scan direction to forward
  // 1. reverse scan to find the first row of the current schema rowkey
  // 2. scan forward from the first row until the last row of the current schema rowkey
  // 3. reverse scan to find the first row of the next schema rowkey
  int switch_to_reverse_scan()
  {
    int ret = common::OB_SUCCESS;
    if (!reverse_scan_) {
      int64_t tmp = start_;
      start_ = last_;
      last_ = tmp;
      reverse_scan_ = true;
      // current_ is the next read postion -_-
      // - in reverse scan, current_ = TRUE_current - 1
      // - in forward scan, current_ = TRUE_current + 1
      // we need sub the current with two when switch scan direction
      if (ObIMicroBlockReader::INVALID_ROW_INDEX != current_) {
        current_ -= 2;
      }
    }
    return ret;
  }

private:
  int locate_range_pos(const common::ObStoreRange& range, const bool is_left_bound_block,
      const bool is_right_bound_block, int64_t& begin, int64_t& end);
  int set_reader(const common::ObRowStoreType sotre_type);

private:
  int set_base_scan_param(const common::ObStoreRange& range, const bool is_bound_block, const bool is_reverse_scan);
  inline int end_of_block() const
  {
    int ret = common::OB_SUCCESS;
    if (!is_setuped_) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObMicroBlockScanner should be setuped first, ", K(ret));
    } else if (ObIMicroBlockReader::INVALID_ROW_INDEX == current_) {
      // there is no intersection with query rowkey and range of micro blcok
      ret = common::OB_BEYOND_THE_RANGE;
    } else {
      if (reverse_scan_ && current_ < last_) {
        // reach the border row under reverse scan
        ret = common::OB_ITER_END;
      } else if (!reverse_scan_ && current_ > last_) {
        // reach the border row under forward scan
        ret = common::OB_ITER_END;
      }
    }
    return ret;
  }

private:
  ObIMicroBlockReader* reader_;
  ObMicroBlockReader flat_reader_;
  ObSparseMicroBlockReader sparse_reader_;  // for dumpsstable
  int64_t current_;                         // current cursor
  int64_t start_;
  int64_t last_;  // end of scan, inclusive.
  bool reverse_scan_;
  bool is_setuped_;
  bool need_read_meta_;
  int64_t step_;
  storage::ObStoreRow row_;
  transaction::ObTransID trans_id_;
  char obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj)];
};

template <typename Filter>
int ObMicroBlockScanner::filter_pushdown(Filter& filter, common::ObBitSet<>& output)
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(!is_setuped_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "scanner not inited", K(ret));
  } else if (OB_ISNULL(reader_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "reader_ is null", K(ret));
  } else if (common::OB_SUCCESS != (ret = end_of_block())) {
    if (OB_UNLIKELY(common::OB_ITER_END != ret && common::OB_BEYOND_THE_RANGE != ret)) {
      STORAGE_LOG(WARN, "fail to judge end of block or not", K(ret));
    } else {
      // end of block, do not do filter pushdown
      // overwrite the ret and clear output bitset
      ret = common::OB_SUCCESS;
      output.clear_all();
    }
  } else {
    // always from the smaller cursor to the larger cursor
    int64_t start = reverse_scan_ ? last_ : current_;
    int64_t end = reverse_scan_ ? current_ : last_;
    output.clear_all();
    if (OB_FAIL(filter(start, end, reader_, output))) {
      STORAGE_LOG(WARN, "do filter failed", K(ret), K(start), K(end));
    }
  }

  return ret;
}

}  // namespace blocksstable
}  // end namespace oceanbase
#endif
