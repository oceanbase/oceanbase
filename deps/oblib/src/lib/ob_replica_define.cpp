/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_replica_define.h"

namespace oceanbase
{
namespace common
{

OB_SERIALIZE_MEMBER(ObReplicaProperty, property_);

int ObReplicaProperty::set_memstore_percent(int64_t memstore_percent)
{
  int ret = OB_SUCCESS;

  if (memstore_percent >= 0 && memstore_percent <= 100) {
    memstore_percent_ = memstore_percent & 0x7f;
  } else {
    ret = OB_INVALID_ARGUMENT;
  }

  return ret;
}

} // common
} // oceanbase
