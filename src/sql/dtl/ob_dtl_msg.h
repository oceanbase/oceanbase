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

#ifndef OB_DTL_MSG_H
#define OB_DTL_MSG_H

#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/dtl/ob_dtl_msg_type.h"

namespace oceanbase {
namespace sql {
namespace dtl {

struct ObDtlMsgHeader {
  ObDtlMsgHeader()
      : hlen_(sizeof (*this)),
        type_(0),
        nbody_(0),
        checksum_(0)
  {}

  bool is_drain() {
    return type_ == (uint16_t)ObDtlMsgType::DRAIN_DATA_FLOW;
  }
  bool is_px_bloom_filter_data() {
    return type_ == (uint16_t)ObDtlMsgType::PX_BLOOM_FILTER_DATA;
  }
  TO_STRING_KV(
      K_(hlen),
      K_(type),
      K_(nbody),
      K_(checksum));
  NEED_SERIALIZE_AND_DESERIALIZE;
  uint16_t hlen_;
  uint16_t type_;
  int32_t nbody_;
  int64_t checksum_;
};

OB_INLINE DEFINE_SERIALIZE(ObDtlMsgHeader)
{
  using namespace common;
  using namespace common::serialization;

  int ret = OB_SUCCESS;
  if (buf_len - pos < static_cast<int64_t>(sizeof (*this))) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    IGNORE_RETURN encode_i16(buf, buf_len, pos, hlen_);
    IGNORE_RETURN encode_i16(buf, buf_len, pos, type_);
    IGNORE_RETURN encode_i32(buf, buf_len, pos, nbody_);
    IGNORE_RETURN encode_i64(buf, buf_len, pos, checksum_);
  }
  return ret;
}

OB_INLINE DEFINE_DESERIALIZE(ObDtlMsgHeader)
{
  using namespace common;
  using namespace common::serialization;

  int ret = OB_SUCCESS;
  if (data_len - pos < static_cast<int64_t>(sizeof (*this))) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    IGNORE_RETURN decode_i16(buf, data_len, pos, (int16_t*)&hlen_);
    IGNORE_RETURN decode_i16(buf, data_len, pos, (int16_t*)&type_);
    IGNORE_RETURN decode_i32(buf, data_len, pos, &nbody_);
    IGNORE_RETURN decode_i64(buf, data_len, pos, &checksum_);
  }
  return ret;
}

OB_INLINE DEFINE_GET_SERIALIZE_SIZE(ObDtlMsgHeader)
{
  return sizeof (*this);
}

class ObDtlMsg {
  OB_UNIS_VERSION_PV();
public:
  virtual ObDtlMsgType get_type() const = 0;
  inline bool is_data_msg() const { return ObDtlMsgType::PX_NEW_ROW == get_type()
                                           || ObDtlMsgType::PX_DATUM_ROW == get_type()
                                           || ObDtlMsgType::PX_VECTOR == get_type()
                                           || ObDtlMsgType::PX_VECTOR_ROW == get_type()
                                           || ObDtlMsgType::PX_VECTOR_FIXED == get_type(); }
  inline bool is_drain_msg() const { return ObDtlMsgType::DRAIN_DATA_FLOW == get_type(); }
protected:
  ~ObDtlMsg() = default;
};

template <ObDtlMsgType TYPE>
class ObDtlMsgTemp
    : public ObDtlMsg
{
public:
  ObDtlMsgType get_type() const override
  {
    return type();
  }
  static constexpr ObDtlMsgType type()
  {
    return TYPE;
  }
};


}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_MSG_H */
