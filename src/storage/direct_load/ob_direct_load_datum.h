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

#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDatumSerialization
{
public:
  static int serialize(char *buf, const int64_t buf_len, int64_t &pos,
                       const blocksstable::ObStorageDatum &datum);
  static int deserialize(const char *buf, const int64_t data_len, int64_t &pos,
                         blocksstable::ObStorageDatum &datum);
  static int64_t get_serialize_size(const blocksstable::ObStorageDatum &datum);
};

struct ObDirectLoadDatumArray
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadDatumArray();
  ObDirectLoadDatumArray(const ObDirectLoadDatumArray &other) = delete;
  ~ObDirectLoadDatumArray();
  void reset();
  void reuse();
  int assign(blocksstable::ObStorageDatum *datums, int32_t count);
  int assign(const ObDirectLoadDatumArray &other);
  ObDirectLoadDatumArray &operator=(const ObDirectLoadDatumArray &other);
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObDirectLoadDatumArray &src, char *buf, const int64_t len, int64_t &pos);
  bool is_valid() const { return 0 == count_ || nullptr != datums_; }
  DECLARE_TO_STRING;
public:
  common::ObArenaAllocator allocator_;
  int64_t capacity_;
  int64_t count_;
  blocksstable::ObStorageDatum *datums_;
};

struct ObDirectLoadConstDatumArray
{
public:
  ObDirectLoadConstDatumArray();
  ObDirectLoadConstDatumArray(const ObDirectLoadConstDatumArray &other) = delete;
  ~ObDirectLoadConstDatumArray();
  void reset();
  void reuse();
  int assign(blocksstable::ObStorageDatum *datums, int32_t count);
  ObDirectLoadConstDatumArray &operator=(const ObDirectLoadConstDatumArray &other);
  ObDirectLoadConstDatumArray &operator=(const ObDirectLoadDatumArray &other);
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObDirectLoadConstDatumArray &src, char *buf, const int64_t len, int64_t &pos);
  OB_INLINE bool is_valid() const { return 0 == count_ || nullptr != datums_; }
  DECLARE_TO_STRING;
public:
  int64_t count_;
  blocksstable::ObStorageDatum *datums_;
};

} // namespace storage
} // namespace oceanbase
