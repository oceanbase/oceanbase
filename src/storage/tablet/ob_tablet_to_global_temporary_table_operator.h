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
  // @param [in] ls_ids, ls_ids for query
  // @param [out] infos, ObSessionTabletInfo for getting
  // @return OB_SUCCESS if success
  static int batch_get_by_ls_ids(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<share::ObLSID> &ls_ids,
    ObIArray<storage::ObSessionTabletInfo> &infos);
  // Get ObSessionTabletInfo from __all_tablet_to_global_temporary_table
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] table_id, table_id for getting
  // @param [in] sequence, sequence for getting
  // @param [in] session_id, session_id for getting
  // @param [out] info, ObSessionTabletInfo for getting
  // @return OB_SUCCESS if exist, OB_ENTRY_NOT_EXIST if not exist
  static int point_get(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t sequence,
    const uint64_t session_id,
    storage::ObSessionTabletInfo &info);
  // Get ObSessionTabletInfos from __all_tablet_to_global_temporary_table by
  // (session_id, table_id IN (...)) without filtering on sequence, used by
  // the truncate-tablet reuse path: at the first write of a new transaction,
  // the current trans sequence is not yet stamped on the row, so the lookup
  // must match the row regardless of its current sequence (inactive
  // sentinel or a stale active sequence left behind by a previous failed
  // compensation). Output infos are not aligned with table_ids and may be
  // shorter — table_ids whose row is missing simply have no entry in the
  // result, and the caller treats them as to-create.
  //
  // @param [in]  sql_proxy   ObMySQLProxy or ObMySQLTransaction
  // @param [in]  tenant_id   tenant for query
  // @param [in]  table_ids   table_ids to look up (no duplicates)
  // @param [in]  session_id  session whose rows to filter on
  // @param [out] infos       matched rows; may be shorter than table_ids
  // @return OB_SUCCESS on success
  static int batch_point_get_by_table_ids_and_session_id(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTableID> &table_ids,
    const uint64_t session_id,
    ObIArray<storage::ObSessionTabletInfo> &infos);
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
  // Update the sequence column of __all_tablet_to_global_temporary_table for the given tablets.
  // Used by the truncate-tablet-on-commit flow to mark a transaction-level GTT tablet as
  // inactive (sentinel value OB_GTT_V2_TRX_TABLET_INACTIVE_SEQUENCE) once its data store is drained,
  // and by the compensation flow to bump the sequence to the next-transaction value.
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] tablet_ids, tablet_ids whose sequence column will be overwritten
  // @param [in] new_sequence, the new sequence value to set
  // @return OB_SUCCESS if success
  static int batch_update_sequence(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t new_sequence);
  // Get ObSessionTabletInfo from __all_tablet_to_global_temporary_table by table_ids
  //
  // @param [in] sql_proxy, ObMySQLProxy or ObMySQLTransaction
  // @param [in] tenant_id, tenant for query
  // @param [in] table_ids, table_ids for getting
  // @param [out] infos, ObSessionTabletInfo for getting
  // @return OB_SUCCESS if success, result infos is sorted by tablet_id in ascending order
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
  static int inner_batch_update_sequence_by_sql(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t new_sequence,
    const int64_t start_idx,
    const int64_t end_idx);
  static int inner_batch_get_by_sql(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTableID> &table_ids,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<storage::ObSessionTabletInfo> &infos);
  static int inner_batch_get_by_table_ids_and_session_id_sql(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTableID> &table_ids,
    const uint64_t session_id,
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
