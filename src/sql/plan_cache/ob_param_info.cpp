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

#include "sql/plan_cache/ob_param_info.h"

namespace oceanbase {
namespace sql {
int64_t ObParamInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(flag), K_(scale), K_(type), K_(ext_real_type), K_(is_oracle_empty_string));
  J_OBJ_END();
  return pos;
}

void ObParamInfo::reset()
{
  flag_.reset();
  scale_ = 0;
  type_ = common::ObNullType;
  ext_real_type_ = common::ObNullType;
  is_oracle_empty_string_ = false;
}

OB_SERIALIZE_MEMBER(ObParamInfo, flag_, scale_, type_, ext_real_type_, is_oracle_empty_string_);

}  // namespace sql
}  // namespace oceanbase
