/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_tx_big_segment_buf.h"

namespace oceanbase
{

namespace transaction
{

OB_SERIALIZE_MEMBER(BigSegmentPartHeader, prev_part_id_, remain_length_, part_length_);

void ObTxBigSegmentBuf::reset()
{
  if (OB_NOT_NULL(segment_buf_)) {
    share::mtl_free(segment_buf_);
  }
  segment_buf_ = nullptr;
  segment_buf_len_ = 0;
  segment_data_len_ = 0;
  segment_pos_ = 0;
  is_complete_ = false;
  for_serialize_ = false;
  prev_part_id_ = UINT64_MAX;
}

int ObTxBigSegmentBuf::init_for_serialize(int64_t segment_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(basic_init_(segment_len, true))) {
    TRANS_LOG(WARN, "init big segment buf failed", K(ret), KPC(this));
  }

  return ret;
}

char *ObTxBigSegmentBuf::get_serialize_buf()
{
  char *buf = nullptr;
  if (!is_inited() || !for_serialize_) {
    buf = nullptr;
  } else {
    buf = segment_buf_;
  }

  return buf;
}

int64_t ObTxBigSegmentBuf::get_serialize_buf_len()
{
  int64_t buf_len = 0;
  if (!is_inited() || !for_serialize_) {
    buf_len = 0;
  } else {
    buf_len = segment_buf_len_;
  }
  return buf_len;
}

int64_t ObTxBigSegmentBuf::get_serialize_buf_pos()
{
  int64_t buf_pos = 0;
  if (!is_inited() || !for_serialize_) {
    buf_pos = INT64_MAX;
  } else {
    buf_pos = segment_data_len_;
  }
  return buf_pos;
}

int ObTxBigSegmentBuf::set_serialize_pos(const int64_t ser_pos)
{
  int ret = OB_SUCCESS;

  if (ser_pos < 0 || ser_pos > segment_buf_len_ || !is_inited() || !for_serialize_) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), K(ser_pos), KPC(this));
  } else {
    segment_data_len_ = ser_pos;
  }

  return ret;
}

const char *ObTxBigSegmentBuf::get_deserialize_buf()
{
  char *buf = nullptr;
  if (!is_inited() || for_serialize_) {
    buf = nullptr;
  } else {
    buf = segment_buf_;
  }

  return buf;
}

int64_t ObTxBigSegmentBuf::get_deserialize_buf_len()
{
  int64_t buf_len = 0;
  if (!is_inited() || for_serialize_) {
    buf_len = 0;
  } else {
    buf_len = segment_buf_len_;
  }
  return buf_len;
}

int64_t ObTxBigSegmentBuf::get_deserialize_buf_pos()
{
  int64_t buf_pos = 0;
  if (!is_inited() || for_serialize_) {
    buf_pos = INT64_MAX;
  } else {
    buf_pos = segment_pos_;
  }
  return buf_pos;
}

int ObTxBigSegmentBuf::set_deserialize_pos(const int64_t deser_pos)
{
  int ret = OB_SUCCESS;

  if (deser_pos < 0 || !is_inited() || for_serialize_) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), K(deser_pos), KPC(this));
  } else {
    segment_pos_ = deser_pos;
  }

  return ret;
}

int ObTxBigSegmentBuf::split_one_part(char *part_buf,
                                      const int64_t part_buf_len,
                                      int64_t &part_buf_pos,
                                      bool &need_fill_id_into_next_part)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = 0;

  need_fill_id_into_next_part = false;

  BigSegmentPartHeader part_header;
  part_header.part_length_ = part_buf_len - part_buf_pos;
  part_header.remain_length_ = segment_data_len_ - segment_pos_;

  // const int64_t max_part_data_len = part_buf_len - part_header.get_serialize_size();
  part_header.part_length_ = part_buf_len - part_header.get_serialize_size() - part_buf_pos;

  if (OB_ISNULL(part_buf) || part_buf_len <= 0 || !for_serialize_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(part_buf), K(part_buf_len), KPC(this));
  } else if (!is_inited() || !is_active()) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "big segment buf is not inited", K(ret), KPC(this));
  } else if (is_completed()) {
    ret = OB_ITER_END;
  } else {
    if (segment_pos_ == 0) {
      if (prev_part_id_ != INVALID_SEGMENT_PART_ID) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "init prev_part_id_ twice", K(ret), KPC(this));
      } else if (segment_data_len_ <= part_header.part_length_) {
        // only one part
        need_fill_id_into_next_part = false;
        part_header.part_length_ = segment_data_len_;
      } else {
        // data size  = part_header.part_length_
        need_fill_id_into_next_part = true;
      }
    } else {
      if (prev_part_id_ == INVALID_SEGMENT_PART_ID) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid prev_part_id_", K(ret), KPC(this));
      // } else if (OB_FALSE_IT(need_set_prev_id = false)) {
      } else if (part_header.remain_length_ <= part_header.part_length_) {
        // the last part
        part_header.prev_part_id_ = prev_part_id_;
        part_header.part_length_ = part_header.remain_length_;
        need_fill_id_into_next_part = false;
      } else {
        // data size  = part_header.part_length_
        part_header.prev_part_id_ = prev_part_id_;
        need_fill_id_into_next_part = true;
      }
    }

    TRANS_LOG(DEBUG, "init part header for split_one_part", K(ret), K(part_header), KPC(this));

    tmp_pos = part_buf_pos;
    if (OB_FAIL(part_header.serialize(part_buf, part_buf_len, tmp_pos))) {
      TRANS_LOG(WARN, "serialize part header failed", K(ret), KP(part_buf), K(part_buf_len),
                K(tmp_pos), KPC(this));
    } else if (tmp_pos + part_header.part_length_ > part_buf_len
               || segment_pos_ + part_header.part_length_ > segment_data_len_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected part buf len", K(ret), KP(part_buf), K(part_buf_len),
                K(part_buf_pos), K(part_header), KPC(this));
    } else {
      MEMCPY(part_buf + tmp_pos, segment_buf_ + segment_pos_, part_header.part_length_);
      segment_pos_ += part_header.part_length_;
      tmp_pos += part_header.part_length_;
      part_buf_pos = tmp_pos;
      prev_part_id_ = INVALID_SEGMENT_PART_ID;

      if (segment_pos_ < segment_data_len_) {
        is_complete_ = false;
      } else if (segment_pos_ == segment_data_len_) {
        is_complete_ = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected segment postition", K(ret), K(part_header), KPC(this));
      }
    }
  }
  return ret;
}

int ObTxBigSegmentBuf::collect_one_part(const char *part_buf,
                                        const int64_t part_buf_len,
                                        int64_t &part_buf_pos)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = part_buf_pos;

  BigSegmentPartHeader part_header;

  if (OB_ISNULL(part_buf) || part_buf_len <= 0 || for_serialize_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(part_buf), K(part_buf_len), KPC(this));
  } else if (is_active() && is_completed()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(part_header.deserialize(part_buf, part_buf_len, tmp_pos))) {
    TRANS_LOG(WARN, "deserialize part_header failed", K(ret), K(part_buf_len), K(tmp_pos),
              K(part_header));
  // } else if (INVALID_SEGMENT_PART_ID != prev_part_id_
  //            && prev_part_id_ != part_header.prev_part_id_) {
  //   ret = OB_ERR_UNEXPECTED;
  //   TRANS_LOG(WARN, "collect a discontiguous part", K(ret), K(part_header), KPC(this));
  } else {
    if (OB_ISNULL(segment_buf_) || segment_data_len_ <= 0) {
      // is not inited
      if (!part_header.is_first_part()) {
        ret = OB_START_LOG_CURSOR_INVALID;
        TRANS_LOG(WARN, "We need merge from the first part", K(ret), K(part_header), KPC(this));
      } else if (OB_FAIL(basic_init_(part_header.remain_length_, false))) {
        TRANS_LOG(WARN, "init for replay failed", K(ret), K(part_header), KPC(this));
      }
    } else {
      // is inited
      if (part_header.is_first_part()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "We can not merge from the first part without reset", K(ret),
                  K(part_header), KPC(this));
      }
    }
  }

  TRANS_LOG(DEBUG, "try to init big segment for deserialize", K(ret), K(part_header), KPC(this));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (part_header.remain_length_ != segment_buf_len_ - segment_data_len_
             || part_header.remain_length_ > segment_buf_len_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid remain_length in replay buf", K(ret), K(part_header), KPC(this));
  } else if (segment_data_len_ + part_header.part_length_ > segment_buf_len_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "the buf of segment is not enough", K(ret), K(part_header), KPC(this));
  } else {
    MEMCPY(segment_buf_ + segment_data_len_, part_buf + tmp_pos, part_header.part_length_);
    segment_data_len_ += part_header.part_length_;
    tmp_pos += part_header.part_length_;
    prev_part_id_ = part_header.prev_part_id_;

    if (segment_data_len_ < segment_buf_len_) {
      is_complete_ = false;
    } else if (segment_data_len_ == segment_buf_len_) {
      is_complete_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    part_buf_pos = tmp_pos;
  }

  return ret;
}

const char *ObTxBigSegmentBuf::get_segment_buf()
{
  const char *tmp_buf_ptr = nullptr;

  if (!for_serialize_ && is_complete_) {
    tmp_buf_ptr = segment_buf_;
  }

  return tmp_buf_ptr;
}

int64_t ObTxBigSegmentBuf::get_segment_len()
{
  int64_t segment_len = 0;

  if (!for_serialize_ && is_complete_) {
    segment_len = segment_data_len_;
  }

  return segment_len;
}

bool ObTxBigSegmentBuf::is_completed() { return is_complete_; }

bool ObTxBigSegmentBuf::is_inited() { return OB_NOT_NULL(segment_buf_); }

bool ObTxBigSegmentBuf::is_active() { return is_inited() && segment_data_len_ > 0; }

int ObTxBigSegmentBuf::set_prev_part_id(const uint64_t prev_part_id)
{
  int ret = OB_SUCCESS;

  if (prev_part_id_ != INVALID_SEGMENT_PART_ID) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "the valid prev_part_id_ can not be rewritten", K(ret), K(prev_part_id),
              KPC(this));
  } else {
    prev_part_id_ = prev_part_id;
  }

  return ret;
}

int ObTxBigSegmentBuf::basic_init_(const int64_t segment_len, bool for_serialize)
{
  int ret = OB_SUCCESS;

  if (is_inited()) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init segment twice", K(ret), KPC(this));
  } else if (segment_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid serialize size", K(ret), KPC(this), K(segment_len));
  } else if (OB_ISNULL(segment_buf_ =
                           static_cast<char *>(share::mtl_malloc(segment_len, "BigSegment")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc memory for big segment", K(ret), KPC(this), K(segment_len));
  } else {
    segment_buf_len_ = segment_len;
    segment_data_len_ = 0;
    segment_data_len_ = 0;
    segment_pos_ = 0;
    is_complete_ = false;
    for_serialize_ = for_serialize;
  }

  return ret;
}

} // namespace transaction

} // namespace oceanbase
