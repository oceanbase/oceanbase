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

#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"

#include "ob_resource_pool.h"

namespace oceanbase
{
using namespace common;
namespace share
{
ObResourcePool::ObResourcePool()
{
  reset();
}

void ObResourcePool::reset()
{
  resource_pool_id_ = OB_INVALID_ID;
  name_.reset();
  unit_count_ = 0;
  unit_config_id_ = OB_INVALID_ID;
  zone_list_.reset();
  tenant_id_ = OB_INVALID_ID;
  replica_type_ = REPLICA_TYPE_FULL;
}

bool ObResourcePool::is_valid() const
{
  return !name_.is_empty() && unit_count_ > 0;
}

int ObResourcePool::assign(const ObResourcePool &other)
{
  int ret = OB_SUCCESS;
  resource_pool_id_ = other.resource_pool_id_;
  name_ = other.name_;
  unit_count_ = other.unit_count_;
  unit_config_id_ = other.unit_config_id_;
  if (OB_FAIL(copy_assign(zone_list_, other.zone_list_))) {
   SHARE_LOG(WARN, "failed to assign zone_list_", KR(ret));
  }
  tenant_id_ = other.tenant_id_;
  replica_type_ = other.replica_type_;
  return ret;
}

DEF_TO_STRING(ObResourcePool)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(resource_pool_id),
       K_(name),
       K_(unit_count),
       K_(unit_config_id),
       K_(zone_list),
       K_(tenant_id),
       K_(replica_type));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObResourcePool,
                    resource_pool_id_,
                    name_,
                    unit_count_,
                    unit_config_id_,
                    zone_list_,
                    tenant_id_,
                    replica_type_);

}//end namespace share
}//end namespace oceanbase
