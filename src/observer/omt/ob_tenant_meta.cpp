/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER_OMT

#include "ob_tenant_meta.h"

namespace oceanbase
{
namespace omt
{

OB_SERIALIZE_MEMBER(ObTenantMeta, unit_, super_block_, create_status_, epoch_);

int ObTenantMeta::build(const share::ObUnitInfoGetter::ObTenantConfig &unit,
                        const storage::ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!unit.is_valid() || !super_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit), K(super_block));
  } else {
    unit_ = unit;
    super_block_ = super_block;
    create_status_ = storage::ObTenantCreateStatus::CREATING;
    epoch_ = 0;
  }

  return ret;
}


}  // end namespace omt
}  // end namespace oceanbase
