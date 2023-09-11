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

#ifndef OCEANBASE_SHARE_TABLET_OB_TENANT_TABLET_TO_LS_MAP_H
#define OCEANBASE_SHARE_TABLET_OB_TENANT_TABLET_TO_LS_MAP_H

#include "lib/alloc/alloc_struct.h"   // ObLabel
#include "lib/hash/ob_hashmap.h"      // ObHashMap
#include "share/ob_ls_id.h"           // ObLSID

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObTabletID;
class ObMySQLProxy;
}
namespace share
{
class ObTabletToLSTableOperator;

typedef common::hash::ObHashMap<common::ObTabletID, ObLSID> ObTabletToLSMap;

// Tenant all tablet to LS info
// It will build map in build() function.
class ObTenantTabletToLSMap final
{
public:
  ObTenantTabletToLSMap() : map_() {}
  ~ObTenantTabletToLSMap() {}

  int init(const int64_t bucket_num = 4096,
      const lib::ObLabel label = lib::ObLabel("TenantTabletToLSMap"),
      const uint64_t tenant_id = OB_SERVER_TENANT_ID)
  {
    return map_.create(bucket_num, label, ObModIds::OB_HASH_NODE, tenant_id);
  }
  void destroy() { map_.destroy(); }

  int build(const uint64_t tenant_id,
      common::ObMySQLProxy &sql_proxy);

  int clear() { return map_.clear(); }

  int get(const common::ObTabletID &tablet_id, ObLSID &ls_id) const
  {
    return map_.get_refactored(tablet_id, ls_id);
  }
  int64_t size() const { return map_.size(); }

private:
   ObTabletToLSMap map_;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_TABLET_OB_TENANT_TABLET_TO_LS_MAP_H
