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

#ifndef OB_OMT_TENANT_META_H_
#define OB_OMT_TENANT_META_H_

#include "share/ob_unit_getter.h"
#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
namespace omt
{

enum class ObTenantCreateStatus
{
  CREATING = 0,
  CREATE_COMMIT, // 1
  CREATE_ABORT, // 2
  DELETING, // 3
  DELETE_COMMIT // 4
};

struct ObTenantMeta final
{
public:
  ObTenantMeta()
    : unit_(),
      super_block_(),
      create_status_(ObTenantCreateStatus::CREATING) {}
  ObTenantMeta(const ObTenantMeta &) = default;
  ObTenantMeta &operator=(const ObTenantMeta &) = default;

  ~ObTenantMeta() = default;

  bool is_valid() const
  {
    return unit_.is_valid() && super_block_.is_valid();
  }


  int build(const share::ObUnitInfoGetter::ObTenantConfig &unit,
            const storage::ObTenantSuperBlock &super_block);


  TO_STRING_KV(K_(unit), K_(super_block), K_(create_status));

  OB_UNIS_VERSION_V(1);

public:
  share::ObUnitInfoGetter::ObTenantConfig unit_;
  storage::ObTenantSuperBlock super_block_;
  ObTenantCreateStatus create_status_;
};

}  // end namespace omt
}  // end namespace oceanbase

#endif  // OB_OMT_TENANT_META_H_
