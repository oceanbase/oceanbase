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

#ifndef OCEANBASE_COMMON_UNIT_OB_RESOURCE_POOL_H_
#define OCEANBASE_COMMON_UNIT_OB_RESOURCE_POOL_H_

#include "lib/ob_define.h"                        // is_valid_tenant_id, ObReplicaType
#include "lib/utility/ob_unify_serialize.h"       // OB_UNIS_VERSION
#include "lib/string/ob_fixed_length_string.h"    // ObFixedLengthString
#include "lib/container/ob_se_array.h"            // ObSEArray
#include "common/ob_zone.h"                       // ObZone

namespace oceanbase
{
namespace share
{
typedef common::ObFixedLengthString<common::MAX_RESOURCE_POOL_LENGTH> ObResourcePoolName;
struct ObResourcePool
{
  OB_UNIS_VERSION(1);

public:
  ObResourcePool();
  ~ObResourcePool() {}
  void reset();
  bool is_valid() const;
  int assign(const ObResourcePool &other);
  bool is_granted_to_tenant() const { return is_valid_tenant_id(tenant_id_); }
  DECLARE_TO_STRING;

  uint64_t resource_pool_id_;
  ObResourcePoolName name_;
  int64_t unit_count_;
  uint64_t unit_config_id_;
  common::ObSEArray<common::ObZone, DEFAULT_ZONE_COUNT> zone_list_;
  uint64_t tenant_id_;
  common::ObReplicaType replica_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObResourcePool);
};
}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_UNIT_OB_RESOURCE_POOL_H_
