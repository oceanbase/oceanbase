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
#define USING_LOG_PREFIX LIB
#include "ob_lob_base.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
namespace common {


int ObILobCursor::check_and_get(int64_t offset, int64_t len, const char *&ptr, int64_t &avail_len) const
{
  INIT_SUCC(ret);
  int64_t total_len = get_length();
  if (offset >= total_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset overflow", K(ret), K(len), K(offset), K(total_len));
  } else if ((avail_len = std::min(total_len - offset, len)) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len is zero", K(ret), K(len), K(offset), K(total_len), K(avail_len));
  } else if (OB_FAIL(get_ptr(offset, avail_len, ptr))) {
    LOG_WARN("get_ptr fail", K(ret), K(avail_len), K(len), K(offset), K(total_len), KP(ptr));
  } else if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), K(avail_len), K(len), K(offset), K(total_len), KP(ptr));
  }
  return ret;
}

int ObILobCursor::read_bool(int64_t offset, bool *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_bool_len = sizeof(bool);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  if (OB_FAIL(check_and_get(offset, max_bool_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_bool_len), K(avail_len));
  } else if (avail_len < max_bool_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len not enough", K(ret), K(avail_len), K(offset), K(max_bool_len));
  } else {
    *val = *reinterpret_cast<const bool*>(ptr);
  }
  return ret;
}

int ObILobCursor::read_i8(int64_t offset, int8_t *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_i8_len = sizeof(int8_t);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  if (OB_FAIL(check_and_get(offset, max_i8_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_i8_len), K(avail_len));
  } else if (avail_len < max_i8_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len not enough", K(ret), K(avail_len), K(offset), K(max_i8_len));
  } else {
    *val = *reinterpret_cast<const int8_t*>(ptr);
  }
  return ret;
}

int ObILobCursor::read_i16(int64_t offset, int16_t *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_i16_len = sizeof(int16_t);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  if (OB_FAIL(check_and_get(offset, max_i16_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_i16_len), K(avail_len));
  } else if (avail_len < max_i16_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len not enough", K(ret), K(avail_len), K(offset), K(max_i16_len));
  } else {
    *val = *reinterpret_cast<const int16_t*>(ptr);
  }
  return ret;
}

int ObILobCursor::read_i32(int64_t offset, int32_t *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_i32_len = sizeof(int32_t);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  if (OB_FAIL(check_and_get(offset, max_i32_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_i32_len), K(avail_len));
  } else if (avail_len < max_i32_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len not enough", K(ret), K(avail_len), K(offset), K(max_i32_len));
  } else {
    *val = *reinterpret_cast<const int32_t*>(ptr);
  }
  return ret;
}

int ObILobCursor::read_i64(int64_t offset, int64_t *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_i64_len = sizeof(int64_t);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  if (OB_FAIL(check_and_get(offset, max_i64_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_i64_len), K(avail_len));
  } else if (avail_len < max_i64_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len not enough", K(ret), K(avail_len), K(offset), K(max_i64_len));
  } else {
    *val = *reinterpret_cast<const int64_t*>(ptr);
  }
  return ret;
}

int ObILobCursor::read_float(int64_t offset, float *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_float_len = sizeof(float);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  if (OB_FAIL(check_and_get(offset, max_float_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_float_len), K(avail_len));
  } else if (avail_len < max_float_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len not enough", K(ret), K(avail_len), K(offset), K(max_float_len));
  } else {
    *val = *reinterpret_cast<const float*>(ptr);
  }
  return ret;
}

int ObILobCursor::read_double(int64_t offset, double *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_double_len = sizeof(double);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  if (OB_FAIL(check_and_get(offset, max_double_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_double_len), K(avail_len));
  } else if (avail_len < max_double_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len not enough", K(ret), K(avail_len), K(offset), K(max_double_len));
  } else {
    *val = *reinterpret_cast<const double*>(ptr);
  }
  return ret;
}

int ObILobCursor::decode_i16(int64_t &offset, int16_t *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_i16_len = sizeof(int16_t);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  int64_t pos = 0;
  if (OB_FAIL(check_and_get(offset, max_i16_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_i16_len), K(avail_len));
  } else if (OB_FAIL(serialization::decode_i16(ptr, avail_len, pos, val))) {
    LOG_WARN("decode_i16 fail", K(ret), K(avail_len), K(max_i16_len), K(offset), K(pos), KP(ptr));
  } else {
    offset += pos;
  }
  return ret;
}

int ObILobCursor::decode_vi64(int64_t &offset, int64_t *val) const
{
  INIT_SUCC(ret);
  static const int64_t max_vi64_len = serialization::encoded_length_vi64(UINT64_MAX);
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  int64_t pos = 0;
  if (OB_FAIL(check_and_get(offset, max_vi64_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_vi64_len), K(avail_len));
  } else if (OB_FAIL(serialization::decode_vi64(ptr, avail_len, pos, val))) {
    LOG_WARN("decode_i16 fail", K(ret), K(avail_len), K(max_vi64_len), K(offset), K(pos), KP(ptr));
  } else {
    offset += pos;
  }
  return ret;
}

int ObILobCursor::deserialize(int64_t &offset, number::ObNumber *number) const
{
  INIT_SUCC(ret);
  static const int64_t max_ob_number_len = sizeof(uint32_t) + sizeof(uint32_t) * number::ObNumber::MAX_CALC_LEN;
  int64_t avail_len = 0;
  const char* ptr = nullptr;
  int64_t pos = 0;
  if (OB_FAIL(check_and_get(offset, max_ob_number_len, ptr, avail_len))) {
    LOG_WARN("check_and_get fail", K(ret), K(offset), K(max_ob_number_len), K(avail_len));
  } else if (OB_FAIL(number->deserialize(ptr, avail_len, pos))) {
    LOG_WARN("decode_i16 fail", K(ret), K(avail_len), K(max_ob_number_len), K(offset), K(pos), KP(ptr));
  } else {
    offset += pos;
  }
  return ret;
}


int ObILobCursor::get_for_write(int64_t offset, int64_t len, ObString &data)
{
  INIT_SUCC(ret);
  int64_t total_len = get_length();
  int64_t avail_len = 0;
  char* ptr = nullptr;
  if (offset >= total_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset overflow", K(ret), K(len), K(offset), K(total_len));
  } else if ((avail_len = std::min(total_len - offset, len)) < len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len is zero", K(ret), K(len), K(offset), K(total_len), K(avail_len));
  } else if (OB_FAIL(get_ptr_for_write(offset, avail_len, ptr))) {
    LOG_WARN("get ptr fail", K(ret), K(avail_len), K(len), K(offset), K(total_len), KP(ptr));
  } else {
    data.assign_ptr(ptr, avail_len);
  }
  return ret;
}

int ObILobCursor::write_i8(int64_t offset, int8_t val)
{
  INIT_SUCC(ret);
  static const int64_t max_i8_len = sizeof(int8_t);
  int64_t total_len = get_length();
  int64_t avail_len = 0;
  char* ptr = nullptr;
  if (offset >= total_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset overflow", K(ret), K(max_i8_len), K(offset), K(total_len));
  } else if ((avail_len = std::min(total_len - offset, max_i8_len)) < max_i8_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len is not enough", K(ret), K(max_i8_len), K(offset), K(total_len), K(avail_len));
  } else if (OB_FAIL(get_ptr_for_write(offset, avail_len, ptr))) {
    LOG_WARN("ptr is null", K(ret), K(avail_len), K(max_i8_len), K(offset), K(total_len), KP(ptr));
  } else {
    int8_t *val_pos = reinterpret_cast<int8_t*>(ptr);
    *val_pos = static_cast<int8_t>(val);
  }
  return ret;
}

int ObILobCursor::write_i16(int64_t offset, int16_t val)
{
  INIT_SUCC(ret);
  static const int64_t max_i16_len = sizeof(int16_t);
  int64_t total_len = get_length();
  int64_t avail_len = 0;
  char* ptr = nullptr;
  if (offset >= total_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset overflow", K(ret), K(max_i16_len), K(offset), K(total_len));
  } else if ((avail_len = std::min(total_len - offset, max_i16_len)) < max_i16_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len is not enough", K(ret), K(max_i16_len), K(offset), K(total_len), K(avail_len));
  } else if (OB_FAIL(get_ptr_for_write(offset, avail_len, ptr))) {
    LOG_WARN("get ptr fail", K(ret), K(avail_len), K(max_i16_len), K(offset), K(total_len), KP(ptr));
  } else {
    int16_t *val_pos = reinterpret_cast<int16_t*>(ptr);
    *val_pos = static_cast<int16_t>(val);
  }
  return ret;
}

int ObILobCursor::write_i32(int64_t offset, int32_t val)
{
  INIT_SUCC(ret);
  static const int64_t max_i32_len = sizeof(int32_t);
  int64_t total_len = get_length();
  int64_t avail_len = 0;
  char* ptr = nullptr;
  if (offset >= total_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset overflow", K(ret), K(max_i32_len), K(offset), K(total_len));
  } else if ((avail_len = std::min(total_len - offset, max_i32_len)) < max_i32_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len is not enough", K(ret), K(max_i32_len), K(offset), K(total_len), K(avail_len));
  } else if (OB_FAIL(get_ptr_for_write(offset, avail_len, ptr))) {
    LOG_WARN("get ptr fail", K(ret), K(avail_len), K(max_i32_len), K(offset), K(total_len), KP(ptr));
  } else {
    int32_t *val_pos = reinterpret_cast<int32_t*>(ptr);
    *val_pos = static_cast<int32_t>(val);
  }
  return ret;
}

int ObILobCursor::write_i64(int64_t offset, int64_t val)
{
  INIT_SUCC(ret);
  static const int64_t max_i64_len = sizeof(int64_t);
  int64_t total_len = get_length();
  int64_t avail_len = 0;
  char* ptr = nullptr;
  if (offset >= total_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset overflow", K(ret), K(max_i64_len), K(offset), K(total_len));
  } else if ((avail_len = std::min(total_len - offset, max_i64_len)) < max_i64_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avail_len is not enough", K(ret), K(max_i64_len), K(offset), K(total_len), K(avail_len));
  } else if (OB_FAIL(get_ptr_for_write(offset, avail_len, ptr))) {
    LOG_WARN("get ptr fail", K(ret), K(avail_len), K(max_i64_len), K(offset), K(total_len), KP(ptr));
  } else {
    int64_t *val_pos = reinterpret_cast<int64_t*>(ptr);
    *val_pos = static_cast<int64_t>(val);
  }
  return ret;
}

int ObILobCursor::move_data(int64_t dst_offset, int64_t src_offset, int64_t move_len)
{
  INIT_SUCC(ret);
  int64_t total_len = get_length();
  char* src_ptr = nullptr;
  if (move_len == 0) { // skip
  } else if (dst_offset >= total_len || src_offset >= total_len || dst_offset + move_len > total_len || src_offset + move_len > total_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset overflow", K(ret), K(dst_offset), K(src_offset), K(move_len), K(total_len));
  } else if (OB_FAIL(get_ptr_for_write(src_offset, move_len, src_ptr))) {
    LOG_WARN("get src ptr fail", K(ret), K(src_offset), K(move_len), K(total_len));
  } else if (OB_FAIL(set(dst_offset, src_ptr, move_len, true))) {
    LOG_WARN("set src_data fail", K(ret), K(dst_offset), K(src_offset), K(move_len), K(total_len), KP(src_ptr));
  }
  return ret;
}

int ObLobInRowUpdateCursor::init(const ObILobCursor *cursor)
{
  INIT_SUCC(ret);
  ObString data;
  if (OB_ISNULL(cursor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cursor is null", K(ret));
  } else if (OB_FAIL(cursor->get_data(data))) {
    LOG_WARN("get_data fail", K(ret), KPC(cursor));
  } else if (OB_FAIL(data_.append(data))) {
    LOG_WARN("append fail", K(ret), KPC(cursor), K(data));
  }
  return ret;
}

int ObLobInRowUpdateCursor::set(int64_t offset, const char *buf, int64_t buf_len, bool use_memmove)
{
  INIT_SUCC(ret);
  int64_t append_len = offset + buf_len - data_.length();
  if (OB_FAIL(data_.reserve(append_len))) {
    LOG_WARN("reserve fail", K(ret), K(offset), K(buf_len), K(append_len), K(data_));
  } else if (append_len > 0 && OB_FAIL(data_.set_length(data_.length() + append_len))) {
    LOG_WARN("set_length fail", K(ret), K(offset), K(buf_len), K(append_len), K(data_));
  } else {
    if (use_memmove) {
      MEMMOVE(data_.ptr() + offset, buf, buf_len);
    } else {
      MEMCPY(data_.ptr() + offset, buf, buf_len);
    }
  }
  return ret;
}

} // common
} // oceanbase
