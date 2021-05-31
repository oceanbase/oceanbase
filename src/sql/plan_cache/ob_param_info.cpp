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
