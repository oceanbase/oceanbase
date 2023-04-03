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

#define USING_LOG_PREFIX SERVER_OMT

#include "ob_tenant_meta.h"

namespace oceanbase
{
namespace omt
{

OB_SERIALIZE_MEMBER(ObTenantMeta, unit_, super_block_, create_status_);

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
    create_status_ = ObTenantCreateStatus::CREATING;
  }

  return ret;
}


}  // end namespace omt
}  // end namespace oceanbase
