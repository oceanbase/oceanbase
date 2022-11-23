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

#ifndef OCEANBASE_SHARE_OB_TABLET_TO_LS_OPERATOR
#define OCEANBASE_SHARE_OB_TABLET_TO_LS_OPERATOR

#include "lib/container/ob_iarray.h" // ObIArray
#include "share/tablet/ob_tablet_info.h" // ObTabletToLSInfo

namespace oceanbase
{
namespace common
{
class ObISQLClient;

namespace sqlclient
{
class ObMySQLResult;
}
} // end nampspace common

namespace share
{
// This operator is used to manipulate inner table __all_tablet_to_ls.
class ObTabletToLSTableOperator
{
public:
  ObTabletToLSTableOperator() {}
  virtual ~ObTabletToLSTableOperator() {}
  // Gets ObTabletIDs sequentially by range
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] start_tablet_id, starting point of the range (not included in output!)
  //             Usually start from 0.
  // @param [in] range_size, range size of the query
  // @param [out] tablet_ls_pairs, sequential ObTabletLSPair in __all_tablet_to_ls
  // @return OB_SUCCESS if success
  static int range_get_tablet(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTabletID &start_tablet_id,
      const int64_t range_size,
      ObIArray<ObTabletLSPair> &tablet_ls_pairs);
  // Gets ObLSIDs according to ObTableIDs
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_ids, ObTabletIDs for query
  //             (should exist in __all_tablet_to_ls and have no duplicate values)
  // @param [out] ls_ids, ObLSIDs corresponding to tablet_ids (same order)
  // @return OB_SUCCESS if success;
  //         OB_NOT_SUPPORTED if tablet_ids have duplicates or
  //         tablet_id which is not recorded in __all_tablet_to_ls;
  //         Other error according to unexpected situation
  static int batch_get_ls(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      ObIArray<ObLSID> &ls_ids);
  // Updates ObTabletToLSInfos to __all_tablet_to_ls
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for updating
  // @param [in] infos, ObTabletToLSInfos for updating
  // @return OB_SUCCESS if success
  static int batch_update(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<ObTabletToLSInfo> &infos);
  // Removes tablet_id from __all_tablet_to_ls
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for removing
  // @param [in] tablet_ids, ObTabletIDs for removing
  // @return OB_SUCCESS if success
  static int batch_remove(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids);
private:
  static int construct_tablet_ls_pairs_(
      common::sqlclient::ObMySQLResult &res,
      ObIArray<ObTabletLSPair> &tablet_ls_pairs);
  static int inner_batch_get_ls_by_sql_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      const int64_t start_idx,
      const int64_t end_idx,
      ObIArray<ObLSID> &ls_ids);
  static int construct_ls_ids_(
      common::sqlclient::ObMySQLResult &res,
      ObIArray<ObLSID> &ls_ids);
  static int inner_batch_update_by_sql_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<ObTabletToLSInfo> &infos,
      const int64_t start_idx,
      const int64_t end_idx);
  static int inner_batch_remove_by_sql_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      const int64_t start_idx,
      const int64_t end_idx);
  const static int64_t MAX_BATCH_COUNT = 200;
};
} // end namespace share
} // end namespace oceanbase
#endif
