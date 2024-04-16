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

#include "lib/container/ob_iarray.h"     // ObIArray
#include "share/tablet/ob_tablet_info.h" // ObTabletToLSInfo
#include "share/location_cache/ob_location_struct.h" // ObTabletLSCache

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
  // Get tablets sequentially by range
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] start_tablet_id, starting point of the range (not included in output!)
  //             Usually start from 0.
  // @param [in] range_size, range size of the query
  // @param [out] tablet_ls_pairs, sequential tablets' info in __all_tablet_to_ls
  // @return OB_SUCCESS if success
  static int range_get_tablet(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTabletID &start_tablet_id,
      const int64_t range_size,
      common::ObIArray<ObTabletLSPair> &tablet_ls_pairs);

  // Get tablets sequentially by range
  //
  // Same function as the previous one, except that:
  // 1. you can specify a list of LS whitelists to get the tables on the specified LS
  // 2. you can get ObTabletToLSInfo instead of ObTabletLSPair
  //
  // @param [in] ls_white_list  LS whitelist, empty means ALL LS in white list
  static int range_get_tablet_info(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const common::ObIArray<ObLSID> &ls_white_list,
      const ObTabletID &start_tablet_id,
      const int64_t range_size,
      common::ObIArray<ObTabletToLSInfo> &tablets);

  // Gets ObLSIDs according to ObTableIDs
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_ids, ObTabletIDs for query
  //             (should exist in __all_tablet_to_ls and have no duplicate values)
  // @param [out] ls_ids, ObLSIDs corresponding to tablet_ids (same order)
  // @return OB_SUCCESS if success;
  //         OB_ITEM_NOT_MATCH if tablet_ids have duplicates or
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
  // Transferring a given tablet from the old ls to the new ls needs to
  // update ls_id and transfer_seq in table __all_tablet_to_ls.
  // This function replaces the old ls id with the new ls id
  // and updates the transfer sequence according to the given value.
  //
  // @param[in]  sql_proxy			sql client
  // @param[in]  tenant_id			the given Tenant ID
  // @param[in]  tablet_id			the given Tablet ID
  // @param[in]  old_transfer_seq	old Transfer Sequence
  // @param[in]  old_ls_id			old LS ID
  // @param[in]  new_transfer_seq	new Transfer Sequence
  // @param[in]  new_ls_id			new LS ID
  // @param[in]  group_id       rpc queue id
  //
  // @ret OB_SUCCESS 			    the updation is successful
  // @ret OB_ENTRY_NOT_EXIST	    affected rows = 0,
  //                                the reason might be the tablet is not in the old ls,
  //                                or old_transfer_seq does not match the transfer sequence value
  //                                in table __all_tablet_to_ls.
  // @ret other error code			failure
  static int update_ls_id_and_transfer_seq(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const int64_t old_transfer_seq,
      const ObLSID &old_ls_id,
      const int64_t new_transfer_seq,
      const ObLSID &new_ls_id,
      const int32_t group_id);
  static int update_table_to_tablet_id_mapping(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const common::ObTabletID &tablet_id);
  // Get rows from __all_tablet_to_ls according to ObTableIDs
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_ids, ObTabletIDs for query
  //             (should exist in __all_tablet_to_ls and have no duplicate values)
  // @param [out] infos, ObTabletToLSInfo corresponding to tablet_ids (not same order)
  //              not same order, not same order, not same order
  // @return OB_SUCCESS if success;
  //         OB_ITEM_NOT_MATCH if tablet_ids have duplicates or nonexistent tablets;
  //         Other error according to unexpected situation
  static int batch_get(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      ObIArray<ObTabletToLSInfo> &infos);
  // Get ls_id by tablet_id
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_id, target tablet_id
  // @param [out] ls_id,    ls_id which the tablet belongs to
  // @return OB_SUCCESS if success;
  //         OB_ENTRY_NOT_EXIST if tablet_id not exist
  //         Other error according to unexpected situation
  static int get_ls_by_tablet(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      ObLSID &ls_id);
  // Batch get ObTabletLSCache for location_service
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_id, target tablet_id
  // @param [out] tablet_ls_cache, ObTabletLSCache array
  // @return OB_SUCCESS if success
  static int batch_get_tablet_ls_cache(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<ObTabletLSCache> &tablet_ls_caches);

  // Gets ObTabletLSPair according to ObTableIDs
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_ids, ObTabletIDs for query (no duplicate values)
  // @param [out] tablet_ls_pairs, array of <TabletID, LSID>
  // @return OB_SUCCESS if success;
  //         OB_ITEM_NOT_MATCH if tablet_ids have duplicates or
  //         tablet_id which is not recorded in __all_tablet_to_ls;
  //         Other error according to unexpected situation
  static int batch_get_tablet_ls_pairs(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      ObIArray<ObTabletLSPair> &tablet_ls_pairs);
  static int get_tablet_ls_pairs_cnt(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      int64_t &input_cnt);
  const static int64_t MAX_BATCH_COUNT = 200;
private:
  static int inner_batch_get_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      const int64_t start_idx,
      const int64_t end_idx,
      ObIArray<ObLSID> &ls_ids);
  static int inner_batch_get_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      const int64_t start_idx,
      const int64_t end_idx,
      ObIArray<ObTabletToLSInfo> &infos);
  static int inner_batch_get_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObIArray<ObTabletLSCache> &tablet_ls_caches);
  static int inner_batch_get_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      const int64_t start_idx,
      const int64_t end_idx,
      ObIArray<ObTabletLSPair> &tablet_ls_pairs);

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

  static int construct_results_(
      common::sqlclient::ObMySQLResult &res,
      const uint64_t tenant_id,
      ObIArray<ObLSID> &ls_ids);
  static int construct_results_(
      common::sqlclient::ObMySQLResult &res,
      const uint64_t tenant_id,
      ObIArray<ObTabletToLSInfo> &infos);
  static int construct_results_(
      common::sqlclient::ObMySQLResult &res,
      const uint64_t tenant_id,
      ObIArray<ObTabletLSPair> &pairs);
  static int construct_results_(
      common::sqlclient::ObMySQLResult &res,
      const uint64_t tenant_id,
      common::ObIArray<ObTabletLSCache> &tablet_ls_caches);
  static int construct_ls_white_list_where_sql_(
      const ObIArray<ObLSID> &ls_white_list,
      ObSqlString &subsql);
};

} // end namespace share
} // end namespace oceanbase
#endif
