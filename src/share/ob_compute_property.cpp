/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
