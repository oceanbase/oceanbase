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

#include "storage/direct_load/ob_direct_load_datum.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

int ObDirectLoadDatumSerialization::serialize(char *buf, const int64_t buf_len, int64_t &pos,
                                              const ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(datum.pack_);
  if (OB_NOT_NULL(datum.ptr_)) {
    if (datum.len_ > 0) {
      MEMCPY(buf + pos, datum.ptr_, datum.len_);
      pos += datum.len_;
    }
  }
  return ret;
}

int ObDirectLoadDatumSerialization::deserialize(const char *buf, const int64_t data_len,
                                                int64_t &pos, ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  uint32_t pack = datum.pack_;
  OB_UNIS_DECODE(pack);
  if (OB_SUCC(ret)) {
    datum.pack_ = pack;
    datum.ptr_ = buf + pos;
    pos += datum.len_;
  }
  return ret;
}

int64_t ObDirectLoadDatumSerialization::get_serialize_size(const ObStorageDatum &datum)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(datum.pack_);
  len += datum.len_;
  return len;
}

/**
 * ObDirectLoadDatumArray
 */

ObDirectLoadDatumArray::ObDirectLoadDatumArray()
  : allocator_("TLD_DatumArray"), capacity_(0), count_(0), datums_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadDatumArray::~ObDirectLoadDatumArray()
{
  reset();
}

void ObDirectLoadDatumArray::reset()
{
  capacity_ = 0;
  count_ = 0;
  datums_ = nullptr;
  allocator_.reset();
}

void ObDirectLoadDatumArray::reuse()
{
  count_ = 0;
}

int ObDirectLoadDatumArray::assign(ObStorageDatum *datums, int32_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count > 0 && nullptr == datums)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datums), K(count));
  } else {
    reset();
    capacity_ = count;
    count_ = count;
    datums_ = (count > 0 ? datums : nullptr);
  }
  return ret;
}

int ObDirectLoadDatumArray::assign(const ObDirectLoadDatumArray &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other));
  } else {
    reset();
    capacity_ = other.capacity_;
    count_ = other.count_;
    datums_ = other.datums_;
  }
  return ret;
}

ObDirectLoadDatumArray &ObDirectLoadDatumArray::operator=(const ObDirectLoadDatumArray &other)
{
  if (this != &other) {
    reset();
    capacity_ = other.capacity_;
    count_ = other.count_;
    datums_ = other.datums_;
  }
  return *this;
}

int64_t ObDirectLoadDatumArray::get_deep_copy_size() const
{
  int64_t size = 0;
  if (OB_LIKELY(is_valid())) {
    size = count_ * sizeof(ObStorageDatum);
    for (int64_t i = 0; i < count_; ++i) {
      size += datums_[i].get_deep_copy_size();
    }
  }
  return size;
}

int ObDirectLoadDatumArray::deep_copy(const ObDirectLoadDatumArray &src, char *buf,
                                      const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = src.get_deep_copy_size();
  if (OB_UNLIKELY(!src.is_valid() || len - pos < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src), K(len), K(deep_copy_size));
  } else {
    reset();
    ObStorageDatum *datums = nullptr;
    const int64_t datum_cnt = src.count_;
    if (datum_cnt > 0) {
      datums = new (buf + pos) ObStorageDatum[datum_cnt];
      pos += sizeof(ObStorageDatum) * datum_cnt;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt; i++) {
      if (OB_FAIL(datums[i].deep_copy(src.datums_[i], buf, len, pos))) {
        LOG_WARN("fail to deep copy storage datum", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      capacity_ = datum_cnt;
      count_ = datum_cnt;
      datums_ = datums;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadDatumArray)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(count_);
  for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
    if (OB_FAIL(ObDirectLoadDatumSerialization::serialize(buf, buf_len, pos, datums_[i]))) {
      LOG_WARN("fail to serialize datum", KR(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadDatumArray)
{
  int ret = OB_SUCCESS;
  reuse();
  OB_UNIS_DECODE(count_);
  if (OB_SUCC(ret) && count_ > capacity_) {
    allocator_.reuse();
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageDatum) * count_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret));
    } else {
      datums_ = new (buf) ObStorageDatum[count_];
      capacity_ = count_;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
    if (OB_FAIL(ObDirectLoadDatumSerialization::deserialize(buf, data_len, pos, datums_[i]))) {
      LOG_WARN("fail to deserialize datum", KR(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadDatumArray)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(count_);
  for (int64_t i = 0; i < count_; ++i) {
    len += ObDirectLoadDatumSerialization::get_serialize_size(datums_[i]);
  }
  return len;
}

DEF_TO_STRING(ObDirectLoadDatumArray)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(count));
  if (nullptr != buf && buf_len >= 0) {
    if (nullptr != datums_) {
      J_ARRAY_START();
      for (int64_t i = 0; i < count_; ++i) {
        databuff_printf(buf, buf_len, pos, "col_id=%ld:", i);
        pos += datums_[i].storage_to_string(buf + pos, buf_len - pos);
        databuff_printf(buf, buf_len, pos, ",");
      }
      J_ARRAY_END();
    }
  }
  J_OBJ_END();
  return pos;
}

/**
 * ObDirectLoadConstDatumArray
 */

ObDirectLoadConstDatumArray::ObDirectLoadConstDatumArray()
  : count_(0), datums_(nullptr)
{
}

ObDirectLoadConstDatumArray::~ObDirectLoadConstDatumArray()
{
}

void ObDirectLoadConstDatumArray::reset()
{
  count_ = 0;
  datums_ = nullptr;
}

void ObDirectLoadConstDatumArray::reuse()
{
  reset();
}

int ObDirectLoadConstDatumArray::assign(ObStorageDatum *datums, int32_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count > 0 && nullptr == datums)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datums), K(count));
  } else {
    count_ = count;
    datums_ = (count > 0 ? datums : nullptr);
  }
  return ret;
}

ObDirectLoadConstDatumArray &ObDirectLoadConstDatumArray::operator=(
  const ObDirectLoadConstDatumArray &other)
{
  if (this != &other) {
    count_ = other.count_;
    datums_ = other.datums_;
  }
  return *this;
}

ObDirectLoadConstDatumArray &ObDirectLoadConstDatumArray::operator=(
  const ObDirectLoadDatumArray &other)
{
  count_ = other.count_;
  datums_ = other.datums_;
  return *this;
}

int64_t ObDirectLoadConstDatumArray::get_deep_copy_size() const
{
  int64_t size = 0;
  if (OB_LIKELY(is_valid())) {
    size = count_ * sizeof(ObStorageDatum);
    for (int64_t i = 0; i < count_; ++i) {
      size += datums_[i].get_deep_copy_size();
    }
  }
  return size;
}

int ObDirectLoadConstDatumArray::deep_copy(const ObDirectLoadConstDatumArray &src, char *buf,
                                           const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = src.get_deep_copy_size();
  if (OB_UNLIKELY(!src.is_valid() || len - pos < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src), K(len), K(deep_copy_size));
  } else {
    reset();
    ObStorageDatum *datums = nullptr;
    const int64_t datum_cnt = src.count_;
    if (datum_cnt > 0) {
      datums = new (buf + pos) ObStorageDatum[datum_cnt];
      pos += sizeof(ObStorageDatum) * datum_cnt;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt; i++) {
      if (OB_FAIL(datums[i].deep_copy(src.datums_[i], buf, len, pos))) {
        LOG_WARN("fail to deep copy storage datum", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      count_ = datum_cnt;
      datums_ = datums;
    }
  }
  return ret;
}

DEF_TO_STRING(ObDirectLoadConstDatumArray)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(count));
  if (nullptr != buf && buf_len >= 0) {
    if (nullptr != datums_) {
      J_ARRAY_START();
      for (int64_t i = 0; i < count_; ++i) {
        databuff_printf(buf, buf_len, pos, "col_id=%ld:", i);
        pos += datums_[i].storage_to_string(buf + pos, buf_len - pos);
        databuff_printf(buf, buf_len, pos, ",");
      }
      J_ARRAY_END();
    }
  }
  J_OBJ_END();
  return pos;
}

} // namespace storage
} // namespace oceanbase
