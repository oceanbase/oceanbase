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

#ifndef OCEANBASE_SHARE_TABLET_OB_TABLET_TO_GLOBAL_TEMPORARY_TABLE_OPERATOR_H
#define OCEANBASE_SHARE_TABLET_OB_TABLET_TO_GLOBAL_TEMPORARY_TABLE_OPERATOR_H

#include "common/ob_tablet_id.h"
#include "share/tablet/ob_tablet_info.h"

namespace oceanbase
{
namespace storage
{
struct ObSessionTabletInfoKey;
struct ObSessionTabletInfo;
}
namespace share
{

// This operator is used to manipulate inner table __all_tablet_to_global_temporary_table.
class ObTabletToGlobalTmpTableOperator
{
public:
  ObTabletToGlobalTmpTableOperator() {};
  ~ObTabletToGlobalTmpTableOperator() {};
  // Insert ObSessionTabletInfo into __all_tablet_to_global_temporary_table
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] infos, ObSessionTabletInfo for inserting
  // @return OB_SUCCESS if success
  static int batch_insert(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<storage::ObSessionTabletInfo> &infos);
  // Remove ObSessionTabletInfo from __all_tablet_to_global_temporary_table
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_ids, tablet_ids for removing
  // @return OB_SUCCESS if success
  static int batch_remove(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids);
  // Get all ObSessionTabletInfos from __all_tablet_to_global_temporary_table
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [out] infos, ObSessionTabletInfo for getting
  // @return OB_SUCCESS if success
  static int batch_get(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    ObIArray<storage::ObSessionTabletInfo> &infos);
  // Get ObSessionTabletInfo from __all_tablet_to_global_temporary_table
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] table_id, table_id for getting
  // @param [in] sequence, sequence for getting
  // @param [out] info, ObSessionTabletInfo for getting
  // @return OB_SUCCESS if exist, OB_ENTRY_NOT_EXIST if not exist
  static int point_get(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t sequence,
    storage::ObSessionTabletInfo &info);
  // Update ls_id and transfer_seq for a tablet in global temporary table
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_id, tablet_id for query
  // @param [in] old_transfer_seq, old transfer_seq for query
  // @param [in] old_ls_id, old ls_id for query
  // @param [in] new_transfer_seq, new transfer_seq for query
  // @param [in] new_ls_id, new ls_id for query
  // @param [in] group_id, group_id for query
  // @return OB_SUCCESS if success;
  static int update_ls_id_and_transfer_seq(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t old_transfer_seq,
    const ObLSID &old_ls_id,
    const int64_t new_transfer_seq,
    const ObLSID &new_ls_id,
    const int32_t group_id);
  // Get ObSessionTabletInfo from __all_tablet_to_global_temporary_table by table_ids
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] table_ids, table_ids for getting
  // @param [out] infos, ObSessionTabletInfo for getting
  // @return OB_SUCCESS if success
  static int batch_get_by_table_ids(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTableID> &table_ids,
    ObIArray<storage::ObSessionTabletInfo> &infos);
  // Get ObSessionTabletInfo from __all_tablet_to_global_temporary_table by table_id
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] table_id, table_id for query
  // @param [out] infos, ObSessionTabletInfo for getting
  // @return OB_SUCCESS if success
  static int get_by_table_id(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const common::ObTableID &table_id,
    ObIArray<storage::ObSessionTabletInfo> &infos);
  static int get_tablet_ids_by_table_id_with_schema_version(
    common::ObISQLClient &sql_proxy,
    const int64_t schema_version,
    const uint64_t tenant_id,
    const common::ObTableID &table_id,
    common::ObIArray<common::ObTabletID> &tablet_ids);
  static int check_tablet_exist(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const common::ObTableID &table_id,
    const common::ObTabletID &tablet_id,
    bool &exist);
  const static int64_t MAX_BATCH_COUNT = 200;
private:
  static int inner_batch_insert_by_sql(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<storage::ObSessionTabletInfo> &infos,
    const int64_t start_idx,
    const int64_t end_idx);
  static int inner_batch_remove_by_sql(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t start_idx,
    const int64_t end_idx);
  static int inner_batch_get_by_sql(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTableID> &table_ids,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<storage::ObSessionTabletInfo> &infos);
  static int construct_infos(
    common::sqlclient::ObMySQLResult &result,
    ObIArray<storage::ObSessionTabletInfo> &infos);
};

} // end namespace share
} // end namespace oceanbase

#endif
