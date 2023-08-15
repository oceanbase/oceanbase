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

#include "storage/direct_load/ob_direct_load_external_row.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadExternalMultiPartitionRow
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadExternalMultiPartitionRow() = default;
  void reset();
  void reuse();
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObDirectLoadExternalMultiPartitionRow &src, char *buf, const int64_t len,
                int64_t &pos);
  OB_INLINE bool is_valid() const { return tablet_id_.is_valid() && external_row_.is_valid(); }
  TO_STRING_KV(K_(tablet_id), K_(external_row));
  int64_t get_raw_size() const { return external_row_.get_raw_size(); }
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadExternalRow external_row_;
};

class ObDirectLoadConstExternalMultiPartitionRow
{
public:
  ObDirectLoadConstExternalMultiPartitionRow();
  ObDirectLoadConstExternalMultiPartitionRow(
    const ObDirectLoadConstExternalMultiPartitionRow &other) = delete;
  ~ObDirectLoadConstExternalMultiPartitionRow();
  void reset();
  ObDirectLoadConstExternalMultiPartitionRow &operator=(
    const ObDirectLoadConstExternalMultiPartitionRow &other);
  ObDirectLoadConstExternalMultiPartitionRow &operator=(
    const ObDirectLoadExternalMultiPartitionRow &other);
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObDirectLoadConstExternalMultiPartitionRow &src, char *buf, const int64_t len,
                int64_t &pos);
  int to_datums(blocksstable::ObStorageDatum *datums, int64_t column_count) const;
  bool is_valid() const
  {
    return tablet_id_.is_valid() && rowkey_datum_array_.is_valid() && seq_no_.is_valid() &&
           buf_size_ > 0 && nullptr != buf_;
  }
  TO_STRING_KV(K_(tablet_id), K_(rowkey_datum_array), K_(seq_no), K_(buf_size), KP_(buf));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadConstDatumArray rowkey_datum_array_;
  table::ObTableLoadSequenceNo seq_no_;
  int64_t buf_size_;
  const char *buf_;
};

}  // namespace storage
}  // namespace oceanbase
