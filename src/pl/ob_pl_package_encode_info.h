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

#ifndef SRC_PL_OB_PL_PACKAGE_ENCODE_INFO_H_
#define SRC_PL_OB_PL_PACKAGE_ENCODE_INFO_H_

#include <cstdint>
#include "lib/container/ob_iarray.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace pl
{

enum PackageValueType
{
  INVALID_VALUE_TYPE = -1,
  NULL_TYPE,
  BOOL_TYPE,
  HEX_STRING_TYPE
};

struct ObPackageVarEncodeInfo
{
  int64_t var_idx_;
  PackageValueType value_type_;
  int64_t value_len_;
  ObObj encode_value_;

  ObPackageVarEncodeInfo()
    : var_idx_(common::OB_INVALID_INDEX),
      value_type_(PackageValueType::INVALID_VALUE_TYPE),
      value_len_(0),
      encode_value_() {}

  int construct();

  int get_serialize_size(int64_t &size);
  int encode(char *dst, const int64_t dst_len, int64_t &dst_pos);
  int decode(const char *src, const int64_t src_len, int64_t &src_pos);

  TO_STRING_KV(K(var_idx_), K(value_type_), K(value_len_), K(encode_value_));
};

} //end namespace pl
} //end namespace oceanbase
#endif /* SRC_PL_OB_PL_PACKAGE_STATE_H_ */