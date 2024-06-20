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
#include "share/ob_table_range.h"
#include "lib/utility/ob_macro_utils.h"         // OB_UNLIKELY
#include "lib/json/ob_yson.h"
namespace oceanbase
{
namespace share
{

void SCN::reset()
{
  val_ = OB_INVALID_SCN_VAL;
}

SCN SCN::atomic_get() const
{
  SCN result;
  const uint64_t val = ATOMIC_LOAD(&val_);
  result.val_ = val;
  return result;
}

SCN SCN::atomic_load() const
{
  SCN result;
  const uint64_t val = ATOMIC_LOAD(&val_);
  result.val_ = val;
  return result;
}

void SCN::atomic_store(const SCN &ref)
{
  ATOMIC_STORE(&val_, ATOMIC_LOAD(&ref.val_));
}

void SCN::atomic_set(const SCN &ref)
{
  ATOMIC_SET(&val_, ATOMIC_LOAD(&ref.val_));
}

bool SCN::atomic_bcas(const SCN &old_v, const SCN &new_val)
{
  return ATOMIC_BCAS(&val_, old_v.val_, new_val.val_);
}

SCN SCN::atomic_vcas(const SCN &old_v, const SCN &new_val)
{
  SCN tmp;
  tmp.val_ = ATOMIC_VCAS(&val_, old_v.val_, new_val.val_);
  return tmp;
}

bool SCN::is_valid() const
{
  return (SCN_VERSION == v_);
}

bool SCN::is_valid_and_not_min() const
{
  return ((SCN_VERSION == v_) && (OB_MIN_SCN_TS_NS != val_));
}

void SCN::set_invalid()
{
  reset();
}

void SCN::set_max()
{
  v_ = SCN_VERSION;
  ts_ns_ = OB_MAX_SCN_TS_NS;
}

bool SCN::is_max() const
{
  bool bool_ret = false;
  if (v_ == SCN_VERSION && ts_ns_ == OB_MAX_SCN_TS_NS) {
    bool_ret = true;
  }
  return bool_ret;
}

void SCN::set_min()
{
  v_ = SCN_VERSION;
  ts_ns_ = OB_MIN_SCN_TS_NS;
}

bool SCN::is_min() const
{
  bool bool_ret = false;
  if (v_ == SCN_VERSION && ts_ns_ == OB_MIN_SCN_TS_NS) {
    bool_ret = true;
  }
  return bool_ret;
}

bool SCN::is_base_scn() const
{
 return (SCN_VERSION == v_) && (OB_BASE_SCN_TS_NS == ts_ns_);
}

void SCN::set_base()
{
  v_ = SCN_VERSION;
  ts_ns_ = OB_BASE_SCN_TS_NS;
}

SCN SCN::invalid_scn()
{
  SCN scn;
  scn.set_invalid();
  return scn;
}

SCN SCN::max_scn()
{
  SCN scn;
  scn.set_max();
  return scn;
}

SCN SCN::min_scn()
{
  SCN scn;
  scn.set_min();
  return scn;
}

SCN SCN::base_scn()
{
  SCN scn;
  scn.set_base();
  return scn;
}

SCN SCN::max(const SCN &left, const SCN &right)
{
  SCN result;
  if (!left.is_valid()) {
    result = right;
  } else if (!right.is_valid()) {
    result = left;
  } else {
    result = right > left ? right : left;
  }
  return result;
}

SCN SCN::min(const SCN &left, const SCN &right)
{
  SCN result;
  if (!left.is_valid()) {
    result = left;
  } else if (!right.is_valid()) {
    result = right;
  } else {
    result = right < left ? right : left;
  }
  return result;
}

SCN SCN::plus(const SCN &ref, uint64_t delta)
{
  int ret = OB_SUCCESS;
  SCN result;
  uint64_t new_val = OB_INVALID_SCN_VAL;
  if (OB_UNLIKELY(delta >= OB_MAX_SCN_TS_NS || !ref.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(delta), K(ref), K(ret));
  } else if (OB_UNLIKELY((new_val = ref.val_ + delta) > OB_MAX_SCN_TS_NS)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR , "new_val is not valid", K(new_val), K(ref), K(delta), K(ret));
  } else {
    result.val_ = new_val;
  }
  return result;
}

SCN SCN::minus(const SCN &ref, uint64_t delta)
{
  int ret = OB_SUCCESS;
  SCN result;
  if (OB_UNLIKELY(delta >= OB_MAX_SCN_TS_NS || !ref.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(delta), K(ref), K(lbt()));
  } else if (OB_UNLIKELY(ref.val_ < delta)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "new_val is not valid", K(delta), K(ret), K(lbt()));
  } else {
    result.val_ = ref.val_ - delta;
  }
  return result;
}

SCN SCN::inc_update(const SCN &ref_scn)
{
  SCN old_scn;
  SCN new_scn;
  new_scn.atomic_set(*this);
  while ((old_scn = new_scn) < ref_scn) {
    if ((new_scn.val_ = ATOMIC_VCAS(&val_, old_scn.val_, ref_scn.val_)) == old_scn.val_) {
      new_scn = ref_scn;
    }
  }
  return new_scn;
}

SCN SCN::dec_update(const SCN &ref_scn)
{
  SCN old_scn;
  SCN new_scn;
  new_scn.atomic_set(*this);
  while ((old_scn = new_scn) > ref_scn) {
    if ((new_scn.val_ = ATOMIC_VCAS(&val_, old_scn.val_, ref_scn.val_)) == old_scn.val_) {
      new_scn = ref_scn;
    }
  }
  return new_scn;
}

SCN SCN::scn_inc(const SCN &ref)
{
  uint64_t ref_val = ATOMIC_LOAD(&ref.val_);
  SCN result;
  if (ref_val >= OB_MAX_SCN_TS_NS) {
    result.val_ = OB_INVALID_SCN_VAL;
  } else {
    result.val_ = ref_val + 1;
  }
  return result;
}

SCN SCN::scn_dec(const SCN &ref)
{
  uint64_t ref_val = ATOMIC_LOAD(&ref.val_);
  SCN result;
  if (OB_MIN_SCN_TS_NS == ref_val || !ref.is_valid()) {
    result.val_ = OB_INVALID_SCN_VAL;
  } else {
    result.val_ = ref_val - 1;
  }
  return result;
}

int64_t SCN::convert_to_ts(bool ignore_invalid) const
{
  int64_t ts_us = 0;
  if (is_valid()) {
    ts_us = ts_ns_ / 1000UL;
  } else {
    if (ignore_invalid) {
      PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid scn should not convert to ts ", K(val_));
    } else {
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid scn should not convert to ts ", K(val_), K(lbt()));
    }
  }
  return ts_us;
}

int SCN::convert_from_ts(uint64_t ts_us)
{
  int ret = OB_SUCCESS;
  const uint64_t ts_ns = ts_us * 1000L;
  if (OB_UNLIKELY(ts_us >= OB_MAX_SCN_TS_NS / 1000L + 1)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ts_us), K(ret));
  } else {
    ts_ns_ = ts_ns;
    v_ = SCN_VERSION;
  }
  return ret;
}

int SCN::convert_for_gts(int64_t ts_ns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > ts_ns
                  || OB_MAX_SCN_TS_NS < ts_ns)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ts_ns), K(ret));
  } else {
    ts_ns_ = ts_ns;
    v_ = SCN_VERSION;
  }
  return ret;
}

int SCN::convert_for_logservice(uint64_t scn_val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_MAX_SCN_TS_NS < scn_val)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(scn_val), K(ret), K(lbt()));
  } else {
    val_ = scn_val;
  }
  return ret;
}

int SCN::convert_for_inner_table_field(uint64_t value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_MAX_SCN_TS_NS < value)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(value), K(ret));
  } else {
    val_ = value;
  }
  return ret;
}

int SCN::convert_for_sql(uint64_t value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_MAX_SCN_TS_NS < value)) {
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

uint64_t SCN::get_val_for_sql() const
{
  return val_;
}

uint64_t SCN::get_val_for_gts() const
{
  return val_;
}

uint64_t SCN::get_val_for_logservice() const
{
  return val_;
}

int SCN::convert_for_tx(int64_t val)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == val) {
    val_ = OB_MAX_SCN_TS_NS;
  } else if (OB_UNLIKELY(val < 0 || OB_MAX_SCN_TS_NS < val)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(val), K(lbt()));
  } else {
    val_ = val;
  }
  return ret;
}

int64_t SCN::get_val_for_tx(const bool ignore_invalid_scn) const
{
  int64_t result_val = -1;
  if (!is_valid()) {
    if (!ignore_invalid_scn) {
      PALF_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid SCN", K(val_));
    } else {
      PALF_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid SCN", K(val_));
    }
  } else if (OB_MAX_SCN_TS_NS == ts_ns_) {
    result_val = INT64_MAX;
  } else {
    result_val = ts_ns_;
  }
  return result_val;
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
  bool result = false;
  if (!is_valid() && !scn.is_valid()) {
    result = false;
  } else if (!is_valid()) {
    result = true;
  } else if (!scn.is_valid()) {
    result = false;
  } else {
    result = ts_ns_ < scn.ts_ns_;
  }
  return result;
}

bool SCN::operator<=(const SCN &scn) const
{
  bool result = false;
  if (!is_valid() && !scn.is_valid()) {
    result = true;
  } else if (!is_valid()) {
    result = true;
  } else if (!scn.is_valid()) {
    result = false;
  } else {
    result = ts_ns_ <= scn.ts_ns_;
  }
  return result;
}

bool SCN::operator>(const SCN &scn) const
{
  bool result = false;
  if (!is_valid() && !scn.is_valid()) {
    result = false;
  } else if (!is_valid()) {
    result = false;
  } else if (!scn.is_valid()) {
    result = true;
  } else {
    result = ts_ns_ > scn.ts_ns_;
  }
  return result;
}

bool SCN::operator>=(const SCN &scn) const
{
  bool result = false;
  if (!is_valid() && !scn.is_valid()) {
    result = true;
  } else if (!is_valid()) {
    result = false;
  } else if (!scn.is_valid()) {
    result = true;
  } else {
    result = ts_ns_ >= scn.ts_ns_;
  }
  return result;
}

SCN &SCN::operator=(const SCN &scn)
{
  this->val_ = scn.val_;
  return *this;
}

int SCN::fixed_serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, val_))) {
    PALF_LOG(WARN, "failed to encode val_", K(buf), K(buf_len), K(new_pos), K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

int SCN::to_yson(char *buf, const int64_t buf_len, int64_t &pos) const
{
  return oceanbase::yson::databuff_encode_elements(buf, buf_len, pos, OB_ID(scn_val), val_);
}

int SCN::fixed_deserialize_without_transform(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || data_len <= 0) {
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

int SCN::fixed_deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fixed_deserialize_without_transform(buf, data_len, pos))) {
    PALF_LOG(WARN, "failed to fixed_deserialize_without_transform", K(buf), K(data_len), K(pos), K(ret));
  } else {
    (void)transform_max_();
  }
  return ret;
}

int64_t SCN::get_fixed_serialize_size(void) const
{
  int64_t size = 0 ;
  size += serialization::encoded_length_i64(val_);
  return size;
}

int SCN::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, new_pos, val_))) {
    PALF_LOG(WARN, "failed to encode val_", K(buf), K(buf_len), K(new_pos), K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

int SCN::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, new_pos, val_))) {
    PALF_LOG(WARN, "failed to decode val_", K(buf), K(data_len), K(new_pos), K(ret));
  } else {
    pos = new_pos;
    (void)transform_max_();
  }
  return ret;
}

int64_t SCN::get_serialize_size(void) const
{
  int64_t size = 0 ;
  size += serialization::encoded_length(val_);
  return size;
}

void SCN::transform_max_()
{
  if (INT64_MAX  == val_) {
    this->set_max();
  }
}


} // end namespace share
} // end namespace oceanbase
