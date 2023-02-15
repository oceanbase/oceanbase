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

#ifndef OCEANBASE_SHARE_OB_ZONE_MERGE_TABLE_OPERATOR_
#define OCEANBASE_SHARE_OB_ZONE_MERGE_TABLE_OPERATOR_

#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "common/ob_zone.h"

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
  static int get_zone_list(common::ObISQLClient &sql_client, 
                           const uint64_t tenant_id,
                           common::ObIArray<common::ObZone> &zone_list);
  static int load_zone_merge_info(common::ObISQLClient &sql_client,
                                  const uint64_t tenant_id,
                                  share::ObZoneMergeInfo &info,
                                  const bool print_sql = false);
  static int load_zone_merge_infos(common::ObISQLClient &sql_client,
                                   const uint64_t tenant_id,
                                   common::ObIArray<share::ObZoneMergeInfo> &infos,
                                   const bool print_sql = false);

  static int insert_zone_merge_info(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const share::ObZoneMergeInfo &info);
  static int insert_zone_merge_infos(common::ObISQLClient &sql_client,
                                     const uint64_t tenant_id,
                                     const common::ObIArray<share::ObZoneMergeInfo> &infos);
  // According to each filed's <need_update_> to decide whether need to be updated
  static int update_partial_zone_merge_info(common::ObISQLClient &sql_client,
                                            const uint64_t tenant_id,
                                            const share::ObZoneMergeInfo &info);
  static int update_zone_merge_info(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const share::ObZoneMergeInfo &info);
  static int update_zone_merge_infos(common::ObISQLClient &sql_client,
                                     const uint64_t tenant_id,
                                     const common::ObIArray<share::ObZoneMergeInfo> &infos);

  static int delete_tenant_merge_info(common::ObISQLClient &sql_client, const uint64_t tenant_id);

  // delete row whose 'zone' exist in zone_list
  static int delete_tenant_merge_info_by_zone(common::ObISQLClient &sql_client, 
                                              const uint64_t tenant_id,
                                              const common::ObIArray<common::ObZone> &zone_list);

private:
  static int inner_load_zone_merge_infos_(common::ObISQLClient &sql_client,
                                          const uint64_t tenant_id,
                                          common::ObIArray<share::ObZoneMergeInfo> &infos,
                                          const bool print_sql = false);
  static int inner_insert_or_update_zone_merge_infos_(common::ObISQLClient &sql_client,
                                                      const uint64_t tenant_id,
                                                      const bool is_update,
                                                      const common::ObIArray<share::ObZoneMergeInfo> &infos);
  static int construct_zone_merge_info_(common::sqlclient::ObMySQLResult &result,
                                        const bool need_check,
                                        common::ObIArray<share::ObZoneMergeInfo> &infos);

  static int check_scn_revert(common::ObISQLClient &sql_client,
                              const uint64_t tenant_id,
                              const share::ObZoneMergeInfo &info);
};

} // end namespace share
} // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_ZONE_MERGE_TABLE_OPERATOR_
