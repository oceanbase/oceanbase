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

#ifndef OCEANBASE_STORAGE_OB_RANGE_ITERATOR_
#define OCEANBASE_STORAGE_OB_RANGE_ITERATOR_

#include "storage/ob_storage_struct.h"

namespace oceanbase {
namespace common {
struct ObSimpleBatch;
}
namespace storage {
struct ObBatch final {
  enum ObBatchType {
    T_NONE,
    T_GET,
    T_MULTI_GET,
    T_SCAN,
    T_MULTI_SCAN,
  };
  ObBatchType type_;
  union {
    const common::ObExtStoreRowkey* rowkey_;
    const common::ObExtStoreRange* range_;
    const GetRowkeyArray* rowkeys_;
    const ScanRangeArray* ranges_;
  };
  OB_INLINE bool is_valid() const
  {
    return (T_NONE != type_ && (NULL != rowkey_ || NULL != range_ || NULL != rowkeys_ || NULL != ranges_));
  }
  OB_INLINE int64_t to_string(char* buffer, const int64_t length) const
  {
    int64_t pos = 0;
    if (T_NONE == type_) {
      common::databuff_printf(buffer, length, pos, "NONE:");
    } else if (T_GET == type_) {
      common::databuff_printf(buffer, length, pos, "GET:");
      pos += rowkey_->to_string(buffer + pos, length - pos);
    } else if (T_MULTI_GET == type_) {
      common::databuff_printf(buffer, length, pos, "MULTI GET:");
      pos += rowkeys_->to_string(buffer + pos, length - pos);
    } else if (T_SCAN == type_) {
      common::databuff_printf(buffer, length, pos, "SCAN:");
      pos += range_->to_string(buffer + pos, length - pos);
    } else if (T_MULTI_SCAN == type_) {
      common::databuff_printf(buffer, length, pos, "MULTI SCAN:");
      pos += ranges_->to_string(buffer + pos, length - pos);
    } else {
      common::databuff_printf(buffer, length, pos, "invalid type:%d", type_);
    }
    return pos;
  }

  static int get_storage_batch(
      const common::ObSimpleBatch& sql_batch, common::ObIAllocator& allocator, storage::ObBatch& batch);
};

class ObRangeIterator final {
public:
  ObRangeIterator()
      : scan_param_(NULL),
        cur_idx_(0),
        order_ranges_(),
        rowkey_(),
        range_(),
        rowkeys_(),
        rowkey_column_orders_(nullptr),
        rowkey_column_cnt_(0),
        is_inited_(false)
  {}
  ~ObRangeIterator()
  {}

  int get_next(ObBatch& batch);
  void reset();
  void reuse();
  int set_scan_param(ObTableScanParam& scan_param);
  int get_org_range_array_idx(const int64_t range_idx, int64_t& org_range_array_idx);

private:
  int convert_key_ranges(const int64_t range_begin_pos, const int64_t range_end_pos, const int64_t range_array_idx,
      ObIAllocator& allocator, common::ObIArray<common::ObExtStoreRange>& store_ranges);
  template <typename T>
  void set_range_array_idx(const int64_t range_array_idx, T& range);

private:
  ObTableScanParam* scan_param_;
  int64_t cur_idx_;
  ScanRangeArray order_ranges_;
  common::ObExtStoreRowkey rowkey_;
  common::ObExtStoreRange range_;
  GetRowkeyArray rowkeys_;
  const common::ObIArray<common::ObOrderType>* rowkey_column_orders_;
  int64_t rowkey_column_cnt_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObRangeIterator);
};

template <typename T>
void ObRangeIterator::set_range_array_idx(const int64_t range_array_idx, T& range)
{
  range.set_range_array_idx(range_array_idx);
}

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_RANGE_ITERATOR_
