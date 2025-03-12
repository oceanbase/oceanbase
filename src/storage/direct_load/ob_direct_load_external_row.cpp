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

#include "storage/direct_load/ob_direct_load_external_row.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace table;

ObDirectLoadExternalRow::ObDirectLoadExternalRow()
  : allocator_("TLD_ExtRow"), is_delete_(false), is_ack_(false), buf_size_(0), buf_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
}

void ObDirectLoadExternalRow::reset()
{
  rowkey_datum_array_.reset();
  seq_no_.reset();
  is_delete_ = false;
  is_ack_ = false;
  buf_size_ = 0;
  buf_ = nullptr;
  allocator_.reset();
}

void ObDirectLoadExternalRow::reuse()
{
  rowkey_datum_array_.reuse();
  seq_no_.reset();
  is_delete_ = false;
  is_ack_ = false;
  buf_size_ = 0;
  buf_ = nullptr;
  allocator_.reuse();
}

int64_t ObDirectLoadExternalRow::get_deep_copy_size() const
{
  int64_t size = 0;
  if (OB_LIKELY(is_valid())) {
    size += rowkey_datum_array_.get_deep_copy_size();
    size += buf_size_;
  }
  return size;
}

int ObDirectLoadExternalRow::deep_copy(const ObDirectLoadExternalRow &src, char *buf,
                                       const int64_t len, int64_t &pos)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_deep_copy_time_us);
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = src.get_deep_copy_size();
  if (OB_UNLIKELY(!src.is_valid() || len - pos < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src), K(len), K(deep_copy_size));
  } else {
    reuse();
    if (OB_FAIL(rowkey_datum_array_.deep_copy(src.rowkey_datum_array_, buf, len, pos))) {
      LOG_WARN("fail to deep copy datum array", KR(ret));
    } else {
      seq_no_ = src.seq_no_;
      is_delete_ = src.is_delete_;
      is_ack_ = src.is_ack_;
      buf_size_ = src.buf_size_;
      buf_ = buf + pos;
      MEMCPY(buf + pos, src.buf_, buf_size_);
      pos += buf_size_;
    }
  }
  return ret;
}

int ObDirectLoadExternalRow::from_datum_row(const ObDirectLoadDatumRow &datum_row,
                                            const int64_t rowkey_column_count)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, transfer_external_row_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!datum_row.is_valid() || datum_row.get_column_count() < rowkey_column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), K(rowkey_column_count));
  } else {
    reuse();
    seq_no_ = datum_row.seq_no_;
    is_delete_ = datum_row.is_delete_;
    is_ack_ = datum_row.is_ack_;
    if (rowkey_column_count > 0 &&
        OB_FAIL(rowkey_datum_array_.assign(datum_row.storage_datums_, rowkey_column_count))) {
      LOG_WARN("fail to assign datum array", KR(ret));
    } else if (datum_row.count_ > rowkey_column_count) {
      ObDirectLoadDatumArray serialize_datum_array;
      if (OB_FAIL(serialize_datum_array.assign(datum_row.storage_datums_ + rowkey_column_count,
                                               datum_row.count_ - rowkey_column_count))) {
        LOG_WARN("fail to assign datum array", KR(ret));
      } else {
        const int64_t buf_size = serialize_datum_array.get_serialize_size();
        char *buf = nullptr;
        int64_t pos = 0;
        if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret), K(buf_size));
        } else if (OB_FAIL(serialize_datum_array.serialize(buf, buf_size, pos))) {
          LOG_WARN("fail to serialize datum array", KR(ret));
        } else {
          buf_ = buf;
          buf_size_ = buf_size;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadExternalRow::to_datum_row(ObDirectLoadDatumRow &datum_row) const
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, transfer_datum_row_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid row", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(buf_size_ > 0
                           ? datum_row.get_column_count() <= rowkey_datum_array_.count_
                           : datum_row.get_column_count() != rowkey_datum_array_.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), KPC(this));
  } else {
    datum_row.seq_no_ = seq_no_;
    datum_row.is_delete_ = is_delete_;
    datum_row.is_ack_ = is_ack_;
    // from rowkey datum array
    for (int64_t i = 0; i < rowkey_datum_array_.count_; ++i) {
      datum_row.storage_datums_[i] = rowkey_datum_array_.datums_[i];
    }
    // from deserialize datum array
    if (buf_size_ > 0) {
      ObDirectLoadDatumArray deserialize_datum_array;
      int64_t pos = 0;
      if (OB_FAIL(
            deserialize_datum_array.assign(datum_row.storage_datums_ + rowkey_datum_array_.count_,
                                           datum_row.count_ - rowkey_datum_array_.count_))) {
        LOG_WARN("fail to assign datum array", KR(ret));
      } else if (OB_FAIL(deserialize_datum_array.deserialize(buf_, buf_size_, pos))) {
        LOG_WARN("fail to deserialize datum array", KR(ret));
      } else if (OB_UNLIKELY(rowkey_datum_array_.count_ + deserialize_datum_array.count_ !=
                             datum_row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column count", KR(ret), K(rowkey_datum_array_.count_),
                 K(deserialize_datum_array.count_), K(datum_row.count_));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadExternalRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_serialize_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid external row", KR(ret), KPC(this));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
                rowkey_datum_array_,
                seq_no_,
                is_delete_,
                is_ack_,
                buf_size_);
    if (OB_SUCC(ret) && buf_size_ > 0) {
      MEMCPY(buf + pos, buf_, buf_size_);
      pos += buf_size_;
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadExternalRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_deserialize_time_us);
  int ret = OB_SUCCESS;
  reuse();
  LST_DO_CODE(OB_UNIS_DECODE,
              rowkey_datum_array_,
              seq_no_,
              is_delete_,
              is_ack_,
              buf_size_);
  if (OB_SUCC(ret) && buf_size_ > 0) {
    buf_ = buf + pos;
    pos += buf_size_;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadExternalRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_serialize_time_us);
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              rowkey_datum_array_,
              seq_no_,
              is_delete_,
              is_ack_,
              buf_size_);
  len += buf_size_;
  return len;
}

} // namespace storage
} // namespace oceanbase
