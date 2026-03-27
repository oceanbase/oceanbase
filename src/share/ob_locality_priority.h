/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
