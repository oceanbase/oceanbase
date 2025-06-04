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

#include "ob_compute_property.h"

namespace oceanbase
{
namespace share
{

DEFINE_SERIALIZE(ObAggrParamProperty)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(mono_);
  OB_UNIS_ENCODE(is_null_prop_);
  return ret;
}

DEFINE_DESERIALIZE(ObAggrParamProperty)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(mono_);
  OB_UNIS_DECODE(is_null_prop_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObAggrParamProperty)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(mono_);
  OB_UNIS_ADD_LEN(is_null_prop_);
  return len;
}

} // namespace sql
} // namespace oceanbase
