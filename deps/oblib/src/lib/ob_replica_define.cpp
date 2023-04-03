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
