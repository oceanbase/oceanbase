/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_HMS_CATALOG_STATS_FRESHNESS_CHECKER_H
#define OB_HMS_CATALOG_STATS_FRESHNESS_CHECKER_H

#include "share/stat/catalog/ob_catalog_stats_freshness_checker.h"

namespace oceanbase
{
namespace common
{

// HMS uses {schema_version, modify_ts, file_num, data_size} for change detection
class ObHMSCatalogStatsFreshnessChecker : public ObICatalogStatsFreshnessChecker
{
public:
  ObHMSCatalogStatsFreshnessChecker()
  {
  }
  virtual ~ObHMSCatalogStatsFreshnessChecker()
  {
  }

  virtual bool check_table_schema_changed(
      const ObCatalogExtPartitionInfo &new_part,
      const share::ObOptCatalogTableStat *existing_stat) override;

protected:
  virtual int build_existing_stats_map(
      const ObIArray<share::ObOptCatalogTableStat *> &existing_partition_stats,
      hash::ObHashMap<ObString, share::ObOptCatalogTableStat *> &stats_map) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHMSCatalogStatsFreshnessChecker);
};

} // namespace common
} // namespace oceanbase

#endif // OB_HMS_CATALOG_STATS_FRESHNESS_CHECKER_H
