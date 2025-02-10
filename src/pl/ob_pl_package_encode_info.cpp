/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL

#include "pl/ob_pl_package_encode_info.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace common;
namespace pl
{

int ObPackageVarEncodeInfo::construct()
{
  int ret = OB_SUCCESS;
  CK (var_idx_ != common::OB_INVALID_INDEX);

  if (OB_SUCC(ret)) {
    if (encode_value_.is_null()) {
      value_type_ = PackageValueType::NULL_TYPE;
      value_len_ = 0;
    } else if (encode_value_.is_tinyint()) {
      value_type_ = PackageValueType::BOOL_TYPE;
      bool val = false;
      value_len_ = serialization::encoded_length(val);
    } else if (encode_value_.is_hex_string()) {
      value_type_ = PackageValueType::HEX_STRING_TYPE;
      value_len_ = encode_value_.get_hex_string().length();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value type", K(ret));
    }
  }

  return ret;
}

int ObPackageVarEncodeInfo::get_serialize_size(int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  size += serialization::encoded_length(var_idx_);
  size += serialization::encoded_length(value_type_);
  size += serialization::encoded_length(value_len_);
  size += value_len_;

  return ret;
}

int ObPackageVarEncodeInfo::encode(char *dst, const int64_t dst_len, int64_t &dst_pos)
{
  int ret = OB_SUCCESS;

  OZ (serialization::encode(dst, dst_len, dst_pos, var_idx_));
  OZ (serialization::encode(dst, dst_len, dst_pos, value_type_));
  OZ (serialization::encode(dst, dst_len, dst_pos, value_len_));
  if (OB_SUCC(ret)) {
    if (PackageValueType::NULL_TYPE == value_type_) {
      // do nothing
    } else if (PackageValueType::BOOL_TYPE == value_type_) {
      bool obj_val = encode_value_.get_bool();
      OZ (serialization::encode(dst, dst_len, dst_pos, obj_val));
    } else if (PackageValueType::HEX_STRING_TYPE == value_type_) {
      CK (value_len_ == encode_value_.get_hex_string().length());
      if (OB_SUCC(ret)) {
        MEMCPY(dst + dst_pos, encode_value_.get_hex_string().ptr(), encode_value_.get_hex_string().length());
        dst_pos += encode_value_.get_hex_string().length();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value type", K(ret));
    }
  }
  return ret;
}

int ObPackageVarEncodeInfo::decode(const char *src, const int64_t src_len, int64_t &src_pos)
{
  int ret = OB_SUCCESS;

  OZ (serialization::decode(src, src_len, src_pos, var_idx_));
  OZ (serialization::decode(src, src_len, src_pos, value_type_));
  OZ (serialization::decode(src, src_len, src_pos, value_len_));
  if (OB_SUCC(ret)) {
    if (PackageValueType::NULL_TYPE == value_type_) {
      CK (0 == value_len_);
      OX (encode_value_.set_null());
    } else if (PackageValueType::BOOL_TYPE == value_type_) {
      bool val = false;
      CK (1 == value_len_);
      OZ (serialization::decode(src, src_len, src_pos, val));
      OX (encode_value_.set_bool(val));
    } else if (PackageValueType::HEX_STRING_TYPE == value_type_) {
      // shallow copy
      OX (encode_value_.set_hex_string(ObString(value_len_, src + src_pos)));
      OX (src_pos += value_len_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value type", K(ret));
    }
  }

  return ret;
}

} // end namespace pl
} // end namespace oceanbase