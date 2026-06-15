/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/compaction_ttl/ob_ttl_definition.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"

namespace oceanbase
{
namespace share
{

int ObTTLFlag::init(const uint64_t tenant_data_version,
                    const ObTTLDefinition::ObTTLType ttl_type,
                    const TTLColumnType ttl_column_type,
                    const int64_t user_ttl_column_id)
{
  int ret = OB_SUCCESS;

  reset();

  version_ = TTL_FLAG_VERSION_V1;

  if (tenant_data_version >= ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION) {
    version_ = TTL_FLAG_VERSION_V2;
    ttl_type_ = ttl_type;
    ttl_column_type_ = ttl_column_type;
    was_compaction_ttl_ = ttl_type == ObTTLDefinition::COMPACTION;
  }

  if (tenant_data_version >= ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION_V2) {
    version_ = TTL_FLAG_VERSION_V3;
    user_ttl_column_id_ = user_ttl_column_id;
  }

  return ret;
}

bool ObTTLFlag::is_valid(const uint64_t tenant_data_version) const
{
  bool bool_ret = true;

  if (tenant_data_version < ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION || version_ == TTL_FLAG_VERSION_V1) {
    bool_ret = version_ == TTL_FLAG_VERSION_V1
               && ttl_column_type_ == TTLColumnType::NONE
               && ttl_type_ == ObTTLDefinition::NONE
               && was_compaction_ttl_ == 0
               && reserved_ == 0
               && being_compaction_ttl_time_us_ == 0
               && user_ttl_column_id_ == 0;
  } else if (version_ == TTL_FLAG_VERSION_V2) {
    bool_ret = tenant_data_version >= ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION && reserved_ == 0 && user_ttl_column_id_ == 0 && ttl_column_type_ <= TTLColumnType::ROWSCN;
  } else if (version_ == TTL_FLAG_VERSION_V3) {
    bool_ret = tenant_data_version >= ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION_V2 && reserved_ == 0;
  }

  return bool_ret;
}

}
}