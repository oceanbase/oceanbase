/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ODPS_CATALOG_STATS_FRESHNESS_CHECKER_H
#define OB_ODPS_CATALOG_STATS_FRESHNESS_CHECKER_H

#include "share/stat/catalog/ob_catalog_stats_freshness_checker.h"

namespace oceanbase
{
namespace common
{

// ODPS uses schema_version + modify_ts for partition change detection
class ObOdpsCatalogStatsFreshnessChecker : public ObICatalogStatsFreshnessChecker
{
public:
  ObOdpsCatalogStatsFreshnessChecker()
  {
  }
  virtual ~ObOdpsCatalogStatsFreshnessChecker()
  {
  }

  virtual bool check_table_schema_changed(
      const ObCatalogExtPartitionInfo &new_part,
      const share::ObOptCatalogTableStat *existing_stat) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOdpsCatalogStatsFreshnessChecker);
};

} // namespace common
} // namespace oceanbase

#endif // OB_ODPS_CATALOG_STATS_FRESHNESS_CHECKER_H
