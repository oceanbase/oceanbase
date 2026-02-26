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
#include "share/ob_order_perserving_encoder.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace sql;

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
  : is_delete_(false), is_ack_(false), buf_size_(0), buf_(nullptr)
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
  is_delete_ = false;
  is_ack_ = false;
  buf_size_ = 0;
  buf_ = nullptr;
}

ObDirectLoadConstExternalMultiPartitionRow &ObDirectLoadConstExternalMultiPartitionRow::operator=(
  const ObDirectLoadConstExternalMultiPartitionRow &other)
{
  if (this != &other) {
    tablet_id_ = other.tablet_id_;
    rowkey_datum_array_ = other.rowkey_datum_array_;
    seq_no_ = other.seq_no_;
    is_delete_ = other.is_delete_;
    is_ack_ = other.is_ack_;
    buf_size_ = other.buf_size_;
    buf_ = other.buf_;
  }
  return *this;
}

ObDirectLoadConstExternalMultiPartitionRow &ObDirectLoadConstExternalMultiPartitionRow::operator=(
  const ObDirectLoadExternalMultiPartitionRow &other)
{
  tablet_id_ = other.tablet_id_;
  rowkey_datum_array_ = other.external_row_.rowkey_datum_array_;
  seq_no_ = other.external_row_.seq_no_;
  is_delete_ = other.external_row_.is_delete_;
  is_ack_ = other.external_row_.is_ack_;
  buf_size_ = other.external_row_.buf_size_;
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
    tablet_id_ = src.tablet_id_;
    if (OB_FAIL(rowkey_datum_array_.deep_copy(src.rowkey_datum_array_, buf, len, pos))) {
      LOG_WARN("fail to deep copy datum array", KR(ret));
    } else {
      seq_no_ = src.seq_no_;
      is_delete_ = src.is_delete_;
      is_ack_ = src.is_ack_;
      if (src.buf_size_ > 0) {
        buf_size_ = src.buf_size_;
        buf_ = buf + pos;
        MEMCPY(buf + pos, src.buf_, buf_size_);
        pos += buf_size_;
      } else {
        buf_size_ = 0;
        buf_ = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadConstExternalMultiPartitionRow::to_datum_row(ObDirectLoadDatumRow &datum_row) const
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

int ObDirectLoadConstExternalMultiPartitionRow::generate_aqs_store_row(
  unsigned char *encode_buf, const int64_t encode_buf_size, ObArray<share::ObEncParam> &enc_params,
  ObIAllocator &allocator, sql::ObChunkDatumStore::StoredRow *&store_row, bool &has_invalid_uni) const
{
  int ret = OB_SUCCESS;
  // enc_params |tablet id| rowkeys | seq_no |
  if (OB_UNLIKELY(!is_valid() || OB_ISNULL(encode_buf) || enc_params.count() != (rowkey_datum_array_.count_ + 2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid row", KR(ret), KPC(this), KP(encode_buf), K(enc_params));
  } else {
    // build StoreRow
    const int64_t size = sizeof(ObChunkDatumStore::StoredRow) + sizeof(ObDatum) * 2;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(size));
    } else {
      store_row = reinterpret_cast<ObChunkDatumStore::StoredRow *>(buf);
      // build sortkey datum
      ObDatum &sortkey_datum = store_row->cells()[0];
      if (OB_FAIL(build_sortkey_datum(encode_buf, encode_buf_size, enc_params, allocator,
                                      sortkey_datum, has_invalid_uni))) {
        LOG_WARN("fail to build sortkey datum", KR(ret));
      } else if (!has_invalid_uni) {
        // store data datum
        ObDatum &data_datum = store_row->cells()[1];
        data_datum.ptr_ = reinterpret_cast<const char *>((this));
        data_datum.len_ = 0; // non-sense
      }
    }
  }
  return ret;
}

int ObDirectLoadConstExternalMultiPartitionRow::build_sortkey_datum(
  unsigned char *encode_buf, const int64_t encode_buf_size,
  ObArray<share::ObEncParam> &enc_params, ObIAllocator &allocator, ObDatum &sortkey_datum,
  bool &has_invalid_uni) const
{
  int ret = OB_SUCCESS;
  int64_t data_len = 0;
  // encode tablet_id
  if (OB_FAIL(encode_table_id(encode_buf, encode_buf_size, enc_params.at(0), data_len,
                              has_invalid_uni))) {
    LOG_WARN("failed to encode tablet_id", KR(ret), K(encode_buf_size), K(enc_params.at(0)),
             K(data_len), K(has_invalid_uni));
  }
  // encode rowkey
  else if (!has_invalid_uni && OB_FAIL(encode_rowkey(encode_buf, encode_buf_size, enc_params,
                                                     data_len, has_invalid_uni))) {
    LOG_WARN("failed to encode rowkey", KR(ret), K(encode_buf_size), K(enc_params), K(data_len),
             K(has_invalid_uni));
  }
  // encode seq_no
  else if (!has_invalid_uni &&
           OB_FAIL(encode_seq_no(encode_buf, encode_buf_size, enc_params.at(enc_params.count() - 1),
                                 data_len, has_invalid_uni))) {
    LOG_WARN("failed to encode seq_no", KR(ret), K(encode_buf_size), K(enc_params), K(data_len),
             K(has_invalid_uni));
  } else {
    if (has_invalid_uni) {
      sortkey_datum.set_null();
    } else {
      // copy sortkey from encode_buf memory to sort memory
      char *buf = nullptr;
      if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(data_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(data_len));
      } else {
        MEMCPY(buf, encode_buf, data_len);
        sortkey_datum.set_string(ObString(data_len, buf));
      }
    }
  }
  return ret;
}

int ObDirectLoadConstExternalMultiPartitionRow::encode_table_id(unsigned char *encode_buf,
                                                                const int64_t encode_buf_size,
                                                                share::ObEncParam &enc_param,
                                                                int64_t &data_len,
                                                                bool &has_invalid_uni) const
{
  int ret = OB_SUCCESS;
  uint64_t tablet_id = tablet_id_.id();
  ObDatum tablet_id_datum(reinterpret_cast<char *>(&tablet_id), sizeof(uint64_t), false);
  enc_param.is_valid_uni_ = true; // reset to true
  int64_t tmp_data_len = 0;
  if (OB_FAIL(share::ObSortkeyConditioner::process_key_conditioning(
        tablet_id_datum, encode_buf + data_len, encode_buf_size - data_len, tmp_data_len,
        enc_param))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      LOG_WARN("failed  to encode sortkey", KR(ret));
    }
  } else {
    if (!enc_param.is_valid_uni_) has_invalid_uni = true;
    data_len += tmp_data_len;
  }
  return ret;
}

int ObDirectLoadConstExternalMultiPartitionRow::encode_rowkey(
  unsigned char *encode_buf, const int64_t encode_buf_size,
  ObArray<share::ObEncParam> &enc_params, int64_t &data_len, bool &has_invalid_uni) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 1; OB_SUCC(ret) && !has_invalid_uni && i < enc_params.count() - 1; i++) {
    ObDatum &data = rowkey_datum_array_.datums_[i - 1];
    enc_params.at(i).is_valid_uni_ = true; // reset to true
    int64_t tmp_data_len = 0;
    if (OB_FAIL(share::ObSortkeyConditioner::process_key_conditioning(
          data, encode_buf + data_len, encode_buf_size - data_len, tmp_data_len,
          enc_params.at(i)))) {
      if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        LOG_WARN("failed to encode sortkey", KR(ret));
      }
    } else {
      if (!enc_params.at(i).is_valid_uni_) {
        has_invalid_uni = true;
      }
      data_len += tmp_data_len;
    }
  }
  return ret;
}

int ObDirectLoadConstExternalMultiPartitionRow::encode_seq_no(unsigned char *encode_buf,
                                                              const int64_t encode_buf_size,
                                                              share::ObEncParam &enc_param,
                                                              int64_t &data_len,
                                                              bool &has_invalid_uni) const
{
  int ret = OB_SUCCESS;
  uint64_t seq_no = seq_no_.sequence_no_;
  ObDatum seq_no_datum(reinterpret_cast<char *>(&seq_no), sizeof(uint64_t), false);
  enc_param.is_valid_uni_ = true;
  int64_t tmp_data_len = 0;
  if (OB_FAIL(share::ObSortkeyConditioner::process_key_conditioning(
        seq_no_datum, encode_buf + data_len, encode_buf_size - data_len, tmp_data_len,
        enc_param))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      LOG_WARN("failed  to encode sortkey", KR(ret));
    }
  } else {
    if (!enc_param.is_valid_uni_) has_invalid_uni = true;
    data_len += tmp_data_len;
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
