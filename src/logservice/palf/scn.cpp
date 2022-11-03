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

#include "scn.h"
#include "lib/utility/ob_macro_utils.h"         // OB_UNLIKELY
namespace oceanbase
{
namespace palf
{

void SCN::reset()
{
  ts_ns_ = OB_INVALID_SCN_VAL;
  v_ = SCN_VERSION;
}

bool SCN::is_valid() const
{
  return ((OB_INVALID_SCN_VAL != ts_ns_) && (SCN_VERSION == v_));
}

int64_t SCN::convert_to_ts() const
{
  return ts_ns_ / 1000UL;
}

int SCN::convert_for_gts(int64_t ts_ns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_SCN_VAL == ts_ns
                  || 0 > ts_ns
                  || OB_MAX_SCN_TS_NS < ts_ns)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ts_ns), K(ret));
  } else {
    ts_ns_ = ts_ns;
    v_ = SCN_VERSION;
  }
  return ret;
}

int SCN::convert_for_lsn_allocator(uint64_t id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_SCN_VAL == id ||  OB_MAX_SCN_TS_NS < id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(id), K(ret));
  } else {
    val_ = id;
  }
  return ret;
}

int SCN::convert_for_inner_table_field(uint64_t value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_SCN_VAL == value || OB_MAX_SCN_TS_NS < value)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(value), K(ret));
  } else {
    val_ = value;
  }
  return ret;
}

uint64_t SCN::get_val_for_inner_table_field() const
{
  return val_;
}

bool SCN::operator==(const SCN &scn) const
{
  return (val_ == scn.val_);
}

bool SCN::operator!=(const SCN &scn) const
{
  return (val_ != scn.val_);
}

bool SCN::operator<(const SCN &scn) const
{
  return ts_ns_ < scn.ts_ns_;
}

bool SCN::operator<=(const SCN &scn) const
{

  return ts_ns_ <= scn.ts_ns_;
}

bool SCN::operator>(const SCN &scn) const
{
  return ts_ns_ > scn.ts_ns_;
}

bool SCN::operator>=(const SCN &scn) const
{
  return ts_ns_ >= scn.ts_ns_;
}

SCN &SCN::operator=(const SCN &scn)
{
  this->val_ = scn.val_;
  return *this;
}

DEFINE_SERIALIZE(SCN)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf && buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, val_))) {
    PALF_LOG(WARN, "failed to encode val_", K(buf), K(buf_len), K(new_pos), K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(SCN)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf && data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos,
                                               reinterpret_cast<int64_t*>(&val_)))) {
    PALF_LOG(WARN, "failed to decode val_", K(buf), K(data_len), K(new_pos), K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(SCN)
{
  int64_t size = 0 ;
  size += serialization::encoded_length_i64(val_);
  return size;
}

} // end namespace palf
} // end namespace oceanbase
