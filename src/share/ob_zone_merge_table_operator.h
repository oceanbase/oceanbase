/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_ZONE_MERGE_TABLE_OPERATOR_
#define OCEANBASE_SHARE_OB_ZONE_MERGE_TABLE_OPERATOR_

#include "common/ob_zone.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}
namespace share
{
class ObZoneMergeInfo;

// CRUD operation to __all_zone_merge_info table
class ObZoneMergeTableOperator
{
public:
  static int load_zone_merge_infos(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      common::ObIArray<share::ObZoneMergeInfo> &infos,
      const bool print_sql = false);
  static int insert_zone_merge_infos(
      common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const common::ObIArray<share::ObZoneMergeInfo> &infos);
  static int update_tenant_all_zone_merge_info(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const share::ObZoneMergeInfo &info);
  // sync __all_zone_merge_info to match zone_list: delete stale zones + insert new zones
  static int sync_zone_merge_info_with_zone_list(
      common::ObMySQLTransaction &trans, const uint64_t tenant_id,
      const common::ObIArray<common::ObZone> &zone_list);
  // delete rows whose zone is NOT in zone_list; if zone_list is empty, delete all rows
  static int delete_zone_merge_info_not_in_zones(
      common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const common::ObIArray<common::ObZone> &zone_list);
  // insert zone_list with default values, skip if already exists
  static int insert_ignore_zone_merge_infos(
      common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const common::ObIArray<common::ObZone> &zone_list);

private:
  enum class InsertMode {
    INSERT = 0,
    REPLACE = 1,
    INSERT_IGNORE = 2,
  };

  static int inner_load_zone_merge_infos_(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      common::ObIArray<share::ObZoneMergeInfo> &infos,
      const bool print_sql = false);
  static int inner_insert_zone_merge_infos_(
      common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const InsertMode mode,
      const common::ObIArray<share::ObZoneMergeInfo> &infos);
  static int construct_zone_merge_info_(
      common::sqlclient::ObMySQLResult &result,
      const bool need_check,
      common::ObIArray<share::ObZoneMergeInfo> &infos);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_ZONE_MERGE_TABLE_OPERATOR_
