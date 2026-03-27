/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OMT_TENANT_META_H_
#define OB_OMT_TENANT_META_H_

#include "share/ob_unit_getter.h"
#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
namespace omt
{

struct ObTenantMeta final
{
public:
  ObTenantMeta()
    : unit_(),
      super_block_(),
      create_status_(storage::ObTenantCreateStatus::CREATING),
      epoch_(0) {}
  ObTenantMeta(const ObTenantMeta &) = default;
  ObTenantMeta &operator=(const ObTenantMeta &) = default;

  ~ObTenantMeta() = default;

  bool is_valid() const
  {
    return unit_.is_valid() && super_block_.is_valid() && epoch_ >= 0;
  }

  int build(const share::ObUnitInfoGetter::ObTenantConfig &unit,
            const storage::ObTenantSuperBlock &super_block);

  TO_STRING_KV(K_(unit), K_(super_block), K_(create_status), K_(epoch));

  OB_UNIS_VERSION_V(1);

public:
  share::ObUnitInfoGetter::ObTenantConfig unit_;
  storage::ObTenantSuperBlock super_block_;
  storage::ObTenantCreateStatus create_status_;
  int64_t epoch_;
};

}  // end namespace omt
}  // end namespace oceanbase

#endif  // OB_OMT_TENANT_META_H_
