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

#ifndef OCEANBASE_TRANSACTION_OB_TX_BIG_SEGMENT_BUF
#define OCEANBASE_TRANSACTION_OB_TX_BIG_SEGMENT_BUF

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace transaction
{
static const uint64_t INVALID_SEGMENT_PART_ID = UINT64_MAX;

struct BigSegmentPartHeader
{
  uint64_t prev_part_id_;
  int64_t remain_length_;
  int64_t part_length_;

  void reset()
  {
    prev_part_id_ = INVALID_SEGMENT_PART_ID;
    remain_length_ = -1;
    part_length_ = -1;
  }

  bool is_first_part() { return prev_part_id_ == INVALID_SEGMENT_PART_ID; }

  BigSegmentPartHeader() { reset(); }

  TO_STRING_KV(K(prev_part_id_), K(remain_length_), K(part_length_));

  OB_UNIS_VERSION(1);
};

class ObTxBigSegmentBuf
{
public:
  ObTxBigSegmentBuf() : segment_buf_(nullptr) { reset(); }
  void reset();

public:
  int init_for_serialize(int64_t segment_len);
  template <typename T>
  int serialize_object(const T &obj);
  template <typename T>
  int deserialize_object(T &obj);

  char *get_serialize_buf();
  int64_t get_serialize_buf_len();
  int64_t get_serialize_buf_pos();
  int set_serialize_pos(const int64_t ser_pos);
  const char *get_deserialize_buf();
  int64_t get_deserialize_buf_len();
  int64_t get_deserialize_buf_pos();
  int set_deserialize_pos(const int64_t deser_pos);

  /**
   * OB_ITER_END : no part can be split or collect
   * */
  int split_one_part(char *part_buf,
                     const int64_t part_buf_len,
                     int64_t &part_buf_pos,
                     bool &need_fill_id_into_next_part);
  int collect_one_part(const char *part_buf, const int64_t part_buf_len, int64_t &part_buf_pos);

  const char *get_segment_buf();
  int64_t get_segment_len();

  bool is_completed();
  bool is_inited();
  bool is_active();

  int set_prev_part_id(const uint64_t prev_part_id);

  TO_STRING_KV(KP(segment_buf_),
               K(segment_buf_len_),
               K(segment_data_len_),
               K(segment_pos_),
               K(is_complete_),
               K(for_serialize_),
               K(prev_part_id_));

private:
  int basic_init_(const int64_t segment_len, bool for_serialize);

private:
  char *segment_buf_;
  int64_t segment_buf_len_;
  int64_t segment_data_len_;
  int64_t segment_pos_;
  bool is_complete_;
  bool for_serialize_;

  uint64_t prev_part_id_;
};

template <typename T>
int ObTxBigSegmentBuf::serialize_object(const T &obj)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = segment_data_len_;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "The big segment buf is not inited", K(ret));
  } else if (OB_FAIL(obj.serialize(segment_buf_, segment_buf_len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize object in big segment failed", K(ret));
  } else {
    segment_data_len_ = tmp_pos;
  }
  return ret;
}

template <typename T>
int ObTxBigSegmentBuf::deserialize_object(T &obj)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = segment_pos_;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "The big segment buf is not inited", K(ret));
  } else if (OB_FAIL(obj.deserialize(segment_buf_, segment_buf_len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize object in big segment failed", K(ret));
  } else {
    segment_pos_ = tmp_pos;
  }
  return ret;
}

} // namespace transaction

} // namespace oceanbase
#endif
