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

#include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"
#include "observer/table_load/ob_table_load_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadExternalMultiPartitionRow
 */

void ObDirectLoadExternalMultiPartitionRow::reset()
{
  tablet_id_.reset();
  external_row_.reset();
}

void ObDirectLoadExternalMultiPartitionRow::reuse()
{
  tablet_id_.reset();
  external_row_.reuse();
}

int64_t ObDirectLoadExternalMultiPartitionRow::get_deep_copy_size() const
{
  return external_row_.get_deep_copy_size();
}

int ObDirectLoadExternalMultiPartitionRow::deep_copy(
  const ObDirectLoadExternalMultiPartitionRow &src, char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = src.get_deep_copy_size();
  if (OB_UNLIKELY(!src.is_valid() || len - pos < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src), K(len), K(deep_copy_size));
  } else {
    reuse();
    tablet_id_ = src.tablet_id_;
    if (OB_FAIL(external_row_.deep_copy(src.external_row_, buf, len, pos))) {
      LOG_WARN("fail to deep copy external row", KR(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadExternalMultiPartitionRow)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tablet_id_.id(), external_row_);
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadExternalMultiPartitionRow)
{
  int ret = OB_SUCCESS;
  uint64_t id = 0;
  LST_DO_CODE(OB_UNIS_DECODE, id, external_row_);
  if (OB_SUCC(ret)) {
    tablet_id_ = id;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadExternalMultiPartitionRow)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tablet_id_.id(), external_row_);
  return len;
}

/**
 * ObDirectLoadConstExternalMultiPartitionRow
 */

ObDirectLoadConstExternalMultiPartitionRow::ObDirectLoadConstExternalMultiPartitionRow()
  : buf_size_(0), buf_(nullptr)
{
}

ObDirectLoadConstExternalMultiPartitionRow::~ObDirectLoadConstExternalMultiPartitionRow()
{
}

void ObDirectLoadConstExternalMultiPartitionRow::reset()
{
  tablet_id_.reset();
  rowkey_datum_array_.reset();
  seq_no_.reset();
  buf_size_ = 0;
  buf_ = nullptr;
}

ObDirectLoadConstExternalMultiPartitionRow &ObDirectLoadConstExternalMultiPartitionRow::operator=(
  const ObDirectLoadConstExternalMultiPartitionRow &other)
{
  if (this != &other) {
    reset();
    tablet_id_ = other.tablet_id_;
    rowkey_datum_array_ = other.rowkey_datum_array_;
    buf_size_ = other.buf_size_;
    seq_no_ = other.seq_no_;
    buf_ = other.buf_;
  }
  return *this;
}

ObDirectLoadConstExternalMultiPartitionRow &ObDirectLoadConstExternalMultiPartitionRow::operator=(
  const ObDirectLoadExternalMultiPartitionRow &other)
{
  tablet_id_ = other.tablet_id_;
  rowkey_datum_array_ = other.external_row_.rowkey_datum_array_;
  buf_size_ = other.external_row_.buf_size_;
  seq_no_ = other.external_row_.seq_no_;
  buf_ = other.external_row_.buf_;
  return *this;
}

int64_t ObDirectLoadConstExternalMultiPartitionRow::get_deep_copy_size() const
{
  int64_t size = 0;
  if (OB_LIKELY(is_valid())) {
    size += rowkey_datum_array_.get_deep_copy_size();
    size += buf_size_;
  }
  return size;
}

int ObDirectLoadConstExternalMultiPartitionRow::deep_copy(
  const ObDirectLoadConstExternalMultiPartitionRow &src, char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = src.get_deep_copy_size();
  if (OB_UNLIKELY(!src.is_valid() || len - pos < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src), K(len), K(deep_copy_size));
  } else {
    reset();
    tablet_id_ = src.tablet_id_;
    if (OB_FAIL(rowkey_datum_array_.deep_copy(src.rowkey_datum_array_, buf, len, pos))) {
      LOG_WARN("fail to deep copy datum array", KR(ret));
    } else {
      buf_size_ = src.buf_size_;
      seq_no_ = src.seq_no_;
      buf_ = buf + pos;
      MEMCPY(buf + pos, src.buf_, buf_size_);
      pos += buf_size_;
    }
  }
  return ret;
}

int ObDirectLoadConstExternalMultiPartitionRow::to_datums(ObStorageDatum *datums,
                                                          int64_t column_count) const
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, transfer_datum_row_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid row", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == datums || column_count < rowkey_datum_array_.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datums), K(column_count), K(rowkey_datum_array_.count_));
  } else {
    // from rowkey datum array
    for (int64_t i = 0; i < rowkey_datum_array_.count_; ++i) {
      datums[i] = rowkey_datum_array_.datums_[i];
    }
    // from deserialize datum array
    ObDirectLoadDatumArray deserialize_datum_array;
    int64_t pos = 0;
    if (OB_FAIL(deserialize_datum_array.assign(datums + rowkey_datum_array_.count_,
                                               column_count - rowkey_datum_array_.count_))) {
      LOG_WARN("fail to assign datum array", KR(ret));
    } else if (OB_FAIL(deserialize_datum_array.deserialize(buf_, buf_size_, pos))) {
      LOG_WARN("fail to deserialize datum array", KR(ret));
    } else if (OB_UNLIKELY(rowkey_datum_array_.count_ + deserialize_datum_array.count_ !=
                           column_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", KR(ret), K(rowkey_datum_array_.count_),
               K(deserialize_datum_array.count_), K(column_count));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
