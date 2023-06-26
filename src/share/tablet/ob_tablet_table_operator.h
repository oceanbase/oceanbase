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

#ifndef OCEANBASE_SHARE_OB_TABLET_TABLE_OPERATOR
#define OCEANBASE_SHARE_OB_TABLET_TABLE_OPERATOR

#include "lib/container/ob_iarray.h" //ObIArray
#include "share/tablet/ob_tablet_info.h" // ObTabletReplica, ObTabletInfo

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObISQLClient;
class ObAddr;
class ObTabletID;

namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class ObDMLSqlSplicer;
class ObTabletLSPair;
class ObLSID;

// Operator for __all_tablet_meta_table.
// Providing (batch-)get, (batch-)update, remove capabilities by sql.
// Notes:
//   __all_tablet_meta_table in SYS_TENANT: record tablet meta for itself;
//   __all_tablet_meta_table in META_TENANT: recod tablet meta for META_TENANT and USER_TENANT;
//   USER_TENANT dosen't have __all_tablet_meta_table.
class ObTabletTableOperator
{
public:
  ObTabletTableOperator();
  virtual ~ObTabletTableOperator();
  int init(common::ObISQLClient &sql_proxy_);
  void reset();
  void set_batch_size(int64_t batch_size) {batch_size_ = batch_size;}
  int get(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const ObLSID &ls_id,
      const common::ObAddr &addr,
      ObTabletReplica &tablet_replica);
  // get ObTabletInfo according to tablet_id
  //
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_id, tablet id for query
  // @param [in] ls_id, the ls which tablet belongs to
  // @param [out] tablet_info, tablet info get from __all_tablet_meta_table.
  //              return empty tablet_info if tablet does not exist in meta table.
  int get(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const ObLSID &ls_id,
      ObTabletInfo &tablet_info);
  // update ObTabletReplica into __all_tablet_meta_table
  //
  // @param [in] replica, ObTabletReplica for update
  int update(const ObTabletReplica &ObTabletReplica);
  // batch get ObTabletInfos according to tenant_id, ls_id and tablet_ids
  //
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_ls_pairs, tablet_id with ls_id
  // @param [out] tablet_infos, array of tablet infos from __all_tablet_meta_table.
  // @return empty tablet_info if tablet does not exist in meta table.
  int batch_get(
      const uint64_t tenant_id,
      const ObIArray<ObTabletLSPair> &tablet_ls_pairs,
      ObIArray<ObTabletInfo> &tablet_infos);
  // range get tablet infos from start_tablet_id
  //
  // @param [in] tenant_id, tenant for query
  // @param [in] start_tablet_id, starting point of the range (not included in output!)
  //             Usually start from 0.
  // @param [in] range_size, range size of the query
  // @param [out] tablet_infos, ObTabletInfos from __all_tablet_meta_table
  // @return OB_SUCCESS if success
  int range_get(
      const uint64_t tenant_id,
      const common::ObTabletID &start_tablet_id,
      const int64_t range_size,
      ObIArray<ObTabletInfo> &tablet_infos);
  // batch update replicas into __all_tablet_meta_table
  //
  // @param [in] tenant_id, tenant for query
  // @param [in] replicas, ObTabletReplicas for updating(should belong to the same tenant!)
  int batch_update(
      const uint64_t tenant_id,
      const ObIArray<ObTabletReplica> &replicas);
  // batch update replicas into __all_tablet_meta_table
  // differ from above batch_update(), it will use @sql_client to commit, not inner sql_proxy_.
  int batch_update(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObIArray<ObTabletReplica> &replicas);
  // batch remove replicas from __all_tablet_meta_table
  //
  // @param [in] tenant_id, target tenant_id
  // @param [in] replicas, ObTabletReplicas for removing(should belong to the same tenant!)
  //             (only tenant_id, tablet_id, ls_id, server are used in this interface)
  int batch_remove(
      const uint64_t tenant_id,
      const ObIArray<ObTabletReplica> &replicas);
  // batch remove replicas from __all_tablet_meta_table
  // differ from above batch_remove(), it will use @sql_client to commit, not inner sql_proxy_.
  int batch_remove(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObIArray<ObTabletReplica> &replicas);
  // remove residual tablet in __all_tablet_meta_table for ObServerMetaTableChecker
  //
  // @param [in] sql_client, client for executing query
  // @param [in] tenant_id, tenant for query
  // @param [in] server, target ObAddr
  // @param [in] limit, limit number for delete sql
  // @param [out] residual_count, count of residual tablets in table
  int remove_residual_tablet(
      ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObAddr &server,
      const int64_t limit,
      int64_t &affected_rows);
public:
  static int get_tablet_info(
      common::ObISQLClient *sql_proxy,
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const ObLSID &ls_id,
      ObTabletInfo &tablet_info);
private:
  static int inner_batch_get_by_sql_(
      ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObIArray<ObTabletLSPair> &tablet_ls_pairs,
      const int64_t start_idx,
      const int64_t end_idx,
      ObIArray<ObTabletInfo> &tablet_infos);
  int inner_batch_update_by_sql_(
      const uint64_t tenant_id,
      const ObIArray<ObTabletReplica> &replicas,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObISQLClient &sql_client);
  static int construct_tablet_infos_(
      common::sqlclient::ObMySQLResult &res,
      ObIArray<ObTabletInfo> &tablet_infos);
  static int construct_tablet_replica_(
      common::sqlclient::ObMySQLResult &res,
      ObTabletReplica &replica);
  int fill_dml_splicer_(
      const ObTabletReplica &replica,
      ObDMLSqlSplicer &dml_splicer);
  int inner_batch_remove_by_sql_(
      const uint64_t tenant_id,
      const ObIArray<ObTabletReplica> &replicas,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObISQLClient &sql_client);
  int fill_remove_dml_splicer_(
      const ObTabletReplica &replica,
      ObDMLSqlSplicer &dml_splicer);
  const static int64_t MAX_BATCH_COUNT = 100;
  bool inited_;
  common::ObISQLClient *sql_proxy_;
  int64_t batch_size_;
};
} // end namespace share
} // end namespace oceanbase
#endif
