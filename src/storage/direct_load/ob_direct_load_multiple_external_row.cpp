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

#include "storage/direct_load/ob_direct_load_multiple_external_row.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace table;

ObDirectLoadMultipleExternalRow::ObDirectLoadMultipleExternalRow()
  : allocator_("TLD_ME_Row"), buf_size_(0), buf_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
}

void ObDirectLoadMultipleExternalRow::reset()
{
  seq_no_.reset();
  buf_size_ = 0;
  buf_ = nullptr;
  allocator_.reset();
}

void ObDirectLoadMultipleExternalRow::reuse()
{
  seq_no_.reset();
  buf_size_ = 0;
  buf_ = nullptr;
  allocator_.reuse();
}

int64_t ObDirectLoadMultipleExternalRow::get_deep_copy_size() const
{
  int64_t size = 0;
  size += buf_size_;
  return size;
}

int ObDirectLoadMultipleExternalRow::deep_copy(const ObDirectLoadMultipleExternalRow &src,
                                               char *buf, const int64_t len, int64_t &pos)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_deep_copy_time_us);
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = src.get_deep_copy_size();
  if (OB_UNLIKELY(!src.is_valid() || len - pos < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src), K(len), K(deep_copy_size));
  } else {
    reuse();
    buf_size_ = src.buf_size_;
    seq_no_ = src.seq_no_;
    buf_ = buf + pos;
    MEMCPY(buf + pos, src.buf_, buf_size_);
    pos += buf_size_;
  }
  return ret;
}

int ObDirectLoadMultipleExternalRow::from_datums(ObStorageDatum *datums, int64_t column_count, const ObTableLoadSequenceNo &seq_no)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, transfer_external_row_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datums), K(column_count));
  } else {
    reuse();
    ObDirectLoadDatumArray serialize_datum_array;
    if (OB_FAIL(serialize_datum_array.assign(datums, column_count))) {
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
        seq_no_ = seq_no;
        buf_size_ = buf_size;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleExternalRow::to_datums(ObStorageDatum *datums, int64_t column_count) const
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, transfer_datum_row_time_us);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid row", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == datums || column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datums), K(column_count));
  } else {
    int64_t pos = 0;
    ObDirectLoadDatumArray deserialize_datum_array;
    if (OB_FAIL(deserialize_datum_array.assign(datums, column_count))) {
      LOG_WARN("fail to assign datum array", KR(ret));
    } else if (OB_FAIL(deserialize_datum_array.deserialize(buf_, buf_size_, pos))) {
      LOG_WARN("fail to deserialize datum array", KR(ret));
    } else if (OB_UNLIKELY(deserialize_datum_array.count_ != column_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", KR(ret), K(deserialize_datum_array.count_),
               K(column_count));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadMultipleExternalRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_serialize_time_us);
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tablet_id_.id(), seq_no_, buf_size_);
  if (OB_SUCC(ret) && OB_NOT_NULL(buf_)) {
    MEMCPY(buf + pos, buf_, buf_size_);
    pos += buf_size_;
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadMultipleExternalRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_deserialize_time_us);
  int ret = OB_SUCCESS;
  reset();
  uint64_t id = 0;
  LST_DO_CODE(OB_UNIS_DECODE, id, seq_no_, buf_size_);
  if (OB_SUCC(ret)) {
    tablet_id_ = id;
    buf_ = buf + pos;
    pos += buf_size_;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadMultipleExternalRow)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, external_row_serialize_time_us);
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tablet_id_.id(), seq_no_, buf_size_);
  len += buf_size_;
  return len;
}

} // namespace storage
} // namespace oceanbase
