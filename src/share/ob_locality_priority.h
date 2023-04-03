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

#ifndef OCEANBASE_SHARE_OB_LOCALITY_PRIORITY_H_
#define OCEANBASE_SHARE_OB_LOCALITY_PRIORITY_H_

#include "share/ob_locality_info.h"
namespace oceanbase
{
namespace share
{
class ObLocalityPriority
{
public:
  static int get_primary_region_prioriry(const char *primary_zone,
      const common::ObIArray<ObLocalityRegion> &locality_region_array,
      common::ObIArray<ObLocalityRegion> &tenant_region_array);
  static int get_region_priority(const ObLocalityInfo &locality_info,
    const common::ObIArray<ObLocalityRegion> &tenant_locality_region,
    uint64_t &region_priority);
  static int get_zone_priority(const ObLocalityInfo &locality_info,
      const common::ObIArray<ObLocalityRegion> &tenant_locality_region,
      uint64_t &zone_priority);
};

} // end namespace share
} // end namespace oceanbase
#endif
