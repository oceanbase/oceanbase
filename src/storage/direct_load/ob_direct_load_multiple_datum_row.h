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
#pragma once

#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_rowkey.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadMultipleDatumRow();
  ~ObDirectLoadMultipleDatumRow();
  void reset();
  void reuse();
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObDirectLoadMultipleDatumRow &src, char *buf, const int64_t len,
                int64_t &pos);
  // not deep copy
  int from_datums(const common::ObTabletID &tablet_id, blocksstable::ObStorageDatum *datums,
                  int64_t column_count, int64_t rowkey_column_count,
                  const table::ObTableLoadSequenceNo &seq_no);
  int to_datums(blocksstable::ObStorageDatum *datums, int64_t column_count) const;
  OB_INLINE bool is_valid() const
  {
    return rowkey_.is_valid() && seq_no_.is_valid() && (buf_size_ == 0 || nullptr != buf_);
  }
  OB_INLINE int64_t get_raw_size() const { return buf_size_; }
  TO_STRING_KV(K_(rowkey), K_(seq_no), K_(buf_size), KP_(buf));

public:
  common::ObArenaAllocator allocator_;
  ObDirectLoadMultipleDatumRowkey rowkey_;
  table::ObTableLoadSequenceNo seq_no_;
  int64_t buf_size_;
  const char *buf_;
};

} // namespace storage
} // namespace oceanbase
