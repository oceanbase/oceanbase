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

#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace table;

ObDirectLoadMultipleDatumRow::ObDirectLoadMultipleDatumRow()
  : allocator_("TLD_MultiRow"), buf_size_(0), buf_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleDatumRow::~ObDirectLoadMultipleDatumRow()
{
}

void ObDirectLoadMultipleDatumRow::reset()
{
  rowkey_.reset();
  seq_no_.reset();
  buf_size_ = 0;
  buf_ = nullptr;
  allocator_.reset();
}

void ObDirectLoadMultipleDatumRow::reuse()
{
  rowkey_.reuse();
  seq_no_.reset();
  buf_size_ = 0;
  buf_ = nullptr;
  allocator_.reuse();
}

int64_t ObDirectLoadMultipleDatumRow::get_deep_copy_size() const
{
  int64_t size = 0;
  if (OB_LIKELY(is_valid())) {
    size += rowkey_.get_deep_copy_size();
    size += buf_size_;
  }
  return size;
}

int ObDirectLoadMultipleDatumRow::deep_copy(const ObDirectLoadMultipleDatumRow &src, char *buf,
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
    if (OB_FAIL(rowkey_.deep_copy(src.rowkey_, buf, len, pos))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
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

int ObDirectLoadMultipleDatumRow::from_datums(const ObTabletID &tablet_id, ObStorageDatum *datums,
                                              int64_t column_count, int64_t rowkey_column_count, const ObTableLoadSequenceNo &seq_no)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, transfer_external_row_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || nullptr == datums ||
                  column_count < rowkey_column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), KP(datums), K(column_count),
             K(rowkey_column_count));
  } else {
    reuse();
    ObDirectLoadDatumArray serialize_datum_array;
    if (OB_FAIL(rowkey_.assign(tablet_id, datums, rowkey_column_count))) {
      LOG_WARN("fail to assign rowkey", KR(ret));
    } else if (OB_FAIL(serialize_datum_array.assign(datums + rowkey_column_count,
                                                    column_count - rowkey_column_count))) {
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
        seq_no_ = seq_no;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleDatumRow::to_datums(ObStorageDatum *datums, int64_t column_count) const
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, transfer_datum_row_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid row", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == datums || column_count < rowkey_.datum_array_.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datums), K(column_count), K(rowkey_.datum_array_.count_));
  } else {
    // from rowkey datum array
    for (int64_t i = 0; i < rowkey_.datum_array_.count_; ++i) {
      datums[i] = rowkey_.datum_array_.datums_[i];
    }
    // from deserialize datum array
    ObDirectLoadDatumArray deserialize_datum_array;
    int64_t pos = 0;
    if (OB_FAIL(deserialize_datum_array.assign(datums + rowkey_.datum_array_.count_,
                                               column_count - rowkey_.datum_array_.count_))) {
      LOG_WARN("fail to assign datum array", KR(ret));
    } else if (OB_FAIL(deserialize_datum_array.deserialize(buf_, buf_size_, pos))) {
      LOG_WARN("fail to deserialize datum array", KR(ret));
    } else if (OB_UNLIKELY(rowkey_.datum_array_.count_ + deserialize_datum_array.count_ !=
                           column_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", KR(ret), K(rowkey_.datum_array_.count_),
               K(deserialize_datum_array.count_), K(column_count));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadMultipleDatumRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_serialize_time_us);
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rowkey_, seq_no_, buf_size_);
  if (OB_SUCC(ret) && OB_NOT_NULL(buf_)) {
    MEMCPY(buf + pos, buf_, buf_size_);
    pos += buf_size_;
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadMultipleDatumRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_deserialize_time_us);
  int ret = OB_SUCCESS;
  reuse();
  LST_DO_CODE(OB_UNIS_DECODE, rowkey_, seq_no_, buf_size_);
  if (OB_SUCC(ret)) {
    buf_ = buf + pos;
    pos += buf_size_;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadMultipleDatumRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_serialize_time_us);
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rowkey_, seq_no_, buf_size_);
  len += buf_size_;
  return len;
}

} // namespace storage
} // namespace oceanbase
