/**
 * Copyright (c) 2024 OceanBase
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
#include "storage/blocksstable/ob_storage_datum.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDatumRow
{
public:
  ObDirectLoadDatumRow();
  ~ObDirectLoadDatumRow();
  void reset();
  void reuse();
  int init(const int64_t count, ObIAllocator *allocator = nullptr);
  bool is_valid() const { return count_ > 0 && nullptr != storage_datums_ && seq_no_.is_valid(); }
  int64_t get_column_count() const { return count_; }

  DECLARE_TO_STRING;

public:
  common::ObArenaAllocator allocator_;
  int64_t count_;
  blocksstable::ObStorageDatum *storage_datums_;
  table::ObTableLoadSequenceNo seq_no_;
  bool is_delete_;
  bool is_ack_;
};

} // namespace storage
} // namespace oceanbase
