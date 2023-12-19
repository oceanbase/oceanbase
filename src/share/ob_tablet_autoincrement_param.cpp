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

#define USING_LOG_PREFIX SHARE
#include "share/ob_tablet_autoincrement_param.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
namespace share
{

OB_SERIALIZE_MEMBER(ObTabletAutoincInterval,
                    tablet_id_,
                    start_,
                    end_)

void ObTabletCacheInterval::set(uint64_t start, uint64_t end)
{
  next_value_ = start;
  start_ = start;
  end_ = end;
}

int ObTabletCacheInterval::next_value(uint64_t &next_value)
{
  int ret = OB_SUCCESS;
  next_value = ATOMIC_FAA(&next_value_, 1);
  if (next_value > end_) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTabletCacheInterval::get_value(uint64_t &value)
{
  int ret = OB_SUCCESS;
  value = next_value_;
  if (value + 1 > end_) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTabletCacheInterval::fetch(uint64_t count, ObTabletCacheInterval &dest)
{
  int ret = OB_SUCCESS;
  uint64_t start = ATOMIC_LOAD(&next_value_);
  uint64_t end = 0;
  uint64_t old_start = start;
  while ((end = start + count - 1) <= end_ &&
         old_start != (start = ATOMIC_CAS(&next_value_, old_start, end + 1))) {
    old_start = start;
    PAUSE();
  }
  if (end > end_) {
    ret = OB_EAGAIN;
  } else {
    dest.set(start, end);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletAutoincParam,
                    tenant_id_,
                    auto_increment_cache_size_);

OB_SERIALIZE_MEMBER(ObMigrateTabletAutoincSeqParam, src_tablet_id_, dest_tablet_id_, ret_code_, autoinc_seq_);


ObTabletAutoincSeq::ObTabletAutoincSeq()
  : version_(AUTOINC_SEQ_VERSION),
    allocator_(nullptr),
    intervals_(nullptr),
    intervals_count_(0)
{
}

ObTabletAutoincSeq::~ObTabletAutoincSeq()
{
  reset();
}

int ObTabletAutoincSeq::assign(common::ObIAllocator &allocator, const ObTabletAutoincSeq &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();

    void *buf = nullptr;
    if (0 == other.intervals_count_) {
      LOG_DEBUG("intervals count equals 0", K(ret));
    } else if (OB_ISNULL(buf = allocator.alloc(other.intervals_count_ * sizeof(ObTabletAutoincInterval)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      allocator_ = &allocator;
      intervals_count_ = other.intervals_count_;
      intervals_ = new (buf) ObTabletAutoincInterval[other.intervals_count_]();
      for (int64_t i = 0; i < other.intervals_count_; ++i) {
        intervals_[i] = other.intervals_[i];
      }
    }

    if (OB_FAIL(ret)) {
      reset();
    }
  }

  return ret;
}

int ObTabletAutoincSeq::deep_copy(
    char *buf,
    const int64_t buf_size,
    ObIStorageMetaObj *&value) const
{
  int ret = OB_SUCCESS;
  value = nullptr;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size < get_deep_copy_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaild argument", K(ret), KP(buf), K(buf_size), K(get_deep_copy_size()));
  } else {
    ObTabletAutoincSeq *new_seq = new (buf) ObTabletAutoincSeq();
    new_seq->intervals_ = new (buf + sizeof(ObTabletAutoincSeq)) ObTabletAutoincInterval[intervals_count_]();
    for (int64_t i = 0; i < intervals_count_; ++i) {
      new_seq->intervals_[i] = intervals_[i];
    }
    new_seq->intervals_count_ = intervals_count_;
    new_seq->allocator_ = nullptr;

    if (OB_SUCC(ret)) {
      value = new_seq;
    }
  }
  return ret;
}

int ObTabletAutoincSeq::deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src info", K(ret));
  }  else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid allocator", K(ret));
  } else if (OB_UNLIKELY(src->type() != type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret), K(src->type()), K(type()));
  } else {
    // free origin data
    reset();
    void *buf = nullptr;
    const ObTabletAutoincSeq *other = nullptr;
    if (OB_ISNULL(other = static_cast<const ObTabletAutoincSeq*>(src))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(ret), K(src->type()), K(type()));
    } else if (0 == other->intervals_count_) {
      LOG_DEBUG("intervals count equals 0");
    } else if (OB_ISNULL(buf = allocator->alloc(other->intervals_count_ * sizeof(ObTabletAutoincInterval)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      intervals_ = new (buf) ObTabletAutoincInterval[other->intervals_count_];
      MEMCPY(intervals_, other->intervals_, sizeof(share::ObTabletAutoincInterval) * other->intervals_count_);
      intervals_count_ = other->intervals_count_;
    }
  }
  return ret;
}

void ObTabletAutoincSeq::reset()
{
  if (0 != intervals_count_ && nullptr != intervals_) {
    for (int64_t i = 0; i < intervals_count_ ; ++i) {
      intervals_[i].~ObTabletAutoincInterval();
    }
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(intervals_);
      allocator_ = nullptr;
    }
    intervals_ = nullptr;
  }
  intervals_count_ = 0;
}

bool ObTabletAutoincSeq::is_valid() const
{
  return 0 != intervals_count_ && nullptr != intervals_;
}

int ObTabletAutoincSeq::get_autoinc_seq_value(uint64_t &autoinc_seq)
{
  int ret = OB_SUCCESS;
  if (0 == intervals_count_) {
    autoinc_seq = 1;
  } else if (1 == intervals_count_) {
    // currently, there will only be one interval
    for (int64_t i = 0; OB_SUCC(ret) && i < intervals_count_; i++) {
      const ObTabletAutoincInterval &interval = intervals_[i];
      if (interval.end_ > interval.start_) {
        autoinc_seq = interval.start_;
        break;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected autoinc seq interval count", K(ret));
  }
  return ret;
}

int ObTabletAutoincSeq::set_autoinc_seq_value(
    common::ObArenaAllocator &allocator,
    const uint64_t autoinc_seq)
{
  int ret = OB_SUCCESS;
  if (0 == intervals_count_) {
    ObTabletAutoincInterval interval;
    interval.start_ = autoinc_seq;
    interval.end_ = INT64_MAX;
    intervals_count_ = 1;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(share::ObTabletAutoincInterval) * intervals_count_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      intervals_ = new (buf) ObTabletAutoincInterval[intervals_count_];
      intervals_[intervals_count_ - 1] = interval;
    }
  } else if (1 == intervals_count_) {
    intervals_[0].start_ = autoinc_seq;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected autoinc seq interval count", K(ret));
  }
  return ret;
}

int ObTabletAutoincSeq::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(AUTOINC_SEQ_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize", K(ret), KP(buf), K(buf_len), K(pos));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t serial_size = pos - pos_bak;
      int64_t tmp_pos = 0;
      int64_t expect_size = get_serialize_size_();
      if (expect_size < serial_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect size < serial size", K(ret), KP(expect_size), K(serial_size));
      } else {
        ret = common::serialization::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes, size_nbytes, tmp_pos, serial_size);
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeq::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(intervals_, intervals_count_);
  return ret;
}

// multi source deserialize with ObIAllocator.
int ObTabletAutoincSeq::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  OB_UNIS_DECODE(version_);
  OB_UNIS_DECODE(len);
  if (OB_SUCC(ret)) {
    int64_t tmp_pos = 0;
    if (OB_FAIL(deserialize_(allocator, buf + pos, len, tmp_pos))) {
      LOG_WARN("fail to deserialize", K(ret), "slen", len, K(pos));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deserialize length is not correct", K(ret), K(len), K(pos));
    } else {
      pos = pos + tmp_pos;
    }
  }
  return ret;
}

int ObTabletAutoincSeq::deserialize_(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &intervals_count_))) {
    LOG_WARN("fail to decode ob array count", K(ret));
  } else if (0 == intervals_count_) {
    LOG_DEBUG("intervals count equals 0", K(ret), K_(intervals_count));
  } else if (OB_ISNULL(ptr = allocator.alloc(intervals_count_ * sizeof(ObTabletAutoincInterval)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(intervals_count_));
  } else {
    allocator_ = &allocator;
    intervals_ = new (ptr) ObTabletAutoincInterval[intervals_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < intervals_count_; i++) {
      ObTabletAutoincInterval &item = intervals_[i];
      if (OB_FAIL(item.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to decode array item", K(ret), K(i), K_(intervals_count), K(item));
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int64_t ObTabletAutoincSeq::get_serialize_size() const
{
  int64_t len = get_serialize_size_();
  SERIALIZE_SIZE_HEADER(version_);
  return len;
}

int64_t ObTabletAutoincSeq::get_serialize_size_(void) const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(intervals_, intervals_count_);
  return len;
}

}// end namespace share
}// end namespace oceanbase
