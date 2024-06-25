// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEABASE_STORAGE_HA_UTILS_H_
#define OCEABASE_STORAGE_HA_UTILS_H_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "ob_storage_ha_struct.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "ob_transfer_struct.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{
class ObBackfillTXCtx;
class ObTransferHandler;
class ObStorageHAUtils
{
public:
  static int get_ls_leader(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &leader_addr);
  static int check_tablet_replica_validity(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObAddr &addr, const common::ObTabletID &tablet_id, common::ObISQLClient &sql_client);
  static int report_ls_meta_table(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const storage::ObMigrationStatus &migration_status);
  static int get_server_version(uint64_t &server_version);
  static int check_server_version(const uint64_t server_version);
  static int check_ls_deleted(
      const share::ObLSID &ls_id,
      bool &is_deleted);

  // When the src_ls of the transfer does not exist, it is necessary to check whether the dest_ls can be rebuilt
  static int check_transfer_ls_can_rebuild(
      const share::SCN replay_scn,
      bool &need_rebuild);
  static int get_readable_scn_with_retry(share::SCN &readable_scn);
  static int64_t get_rpc_timeout();
  static int check_is_primary_tenant(const uint64_t tenant_id, bool &is_primary_tenant);
  static int check_disk_space();
  static int check_ls_is_leader(const uint64_t tenant_id, const share::ObLSID &ls_id, bool &is_leader);

  static int calc_tablet_sstable_macro_block_cnt(
      const ObTabletHandle &tablet_handle, int64_t &data_macro_block_count);
  static int check_tenant_will_be_deleted(
      bool &is_deleted);
  static int get_sstable_read_info(
      const ObTablet &tablet,
      const ObITable::TableType &table_type,
      const bool is_normal_cg_sstable,
      const storage::ObITableReadInfo *&index_read_info);

  static int check_replica_validity(const obrpc::ObFetchLSMetaInfoResp &ls_info);
  static int check_log_status(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      int32_t &result);

private:
  static int check_merge_error_(const uint64_t tenant_id, common::ObISQLClient &sql_client);
  static int fetch_src_tablet_meta_info_(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const common::ObAddr &src_addr, common::ObISQLClient &sql_client,
    share::SCN &compaction_scn);
  static int check_tablet_replica_checksum_(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const share::SCN &compaction_scn, common::ObISQLClient &sql_client);
  static int get_readable_scn_(share::SCN &readable_scn);
};

struct ObTransferUtils
{
public:
  static bool is_need_retry_error(const int err);
  static int block_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts_scn);
  static int kill_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts_scn);
  static int unblock_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts_scn);
  static int get_gts(const uint64_t tenant_id, share::SCN &gts);
  static void set_transfer_module();
  static void clear_transfer_module();
  static int get_need_check_member(
      const common::ObIArray<ObAddr> &total_member_addr_list,
      const common::ObIArray<ObAddr> &finished_member_addr_list,
      common::ObIArray<ObAddr> &member_addr_list);
  static int check_ls_replay_scn(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::SCN &check_scn,
      const int32_t group_id,
      const common::ObIArray<ObAddr> &member_addr_list,
      ObTimeoutCtx &timeout_ctx,
      common::ObIArray<ObAddr> &finished_addr_list);
  static void add_transfer_error_diagnose_in_replay(
      const share::ObTransferTaskID &task_id,
      const share::ObLSID &dest_ls_id,
      const int result_code,
      const bool clean_related_info,
      const share::ObStorageHADiagTaskType type,
      const share::ObStorageHACostItemName result_msg);
  static void add_transfer_error_diagnose_in_backfill(
      const share::ObLSID &dest_ls_id,
      const share::SCN &log_sync_scn,
      const int result_code,
      const common::ObTabletID &tablet_id,
      const share::ObStorageHACostItemName result_msg);
  static void set_transfer_related_info(
      const share::ObLSID &dest_ls_id,
      const share::ObTransferTaskID &task_id,
      const share::SCN &start_scn);
  static void reset_related_info(const share::ObLSID &dest_ls_id);

  static void add_transfer_perf_diagnose_in_backfill(
              const share::ObStorageHAPerfDiagParams &params,
              const share::SCN &log_sync_scn,
              const int result_code,
              const uint64_t timestamp,
              const int64_t start_ts,
              const bool is_report);

  static void process_backfill_perf_diag_info(
              const share::ObLSID &dest_ls_id,
              const common::ObTabletID &tablet_id,
              const share::ObStorageHACostItemType item_type,
              const share::ObStorageHACostItemName name,
              share::ObStorageHAPerfDiagParams &params);

  static void process_start_out_perf_diag_info(
              const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
              const share::ObStorageHACostItemType item_type,
              const share::ObStorageHACostItemName name,
              const int result,
              const uint64_t timestamp,
              const int64_t start_ts,
              const bool is_report);
  static void process_start_in_perf_diag_info(
              const ObTXStartTransferInInfo &tx_start_transfer_in_info,
              const share::ObStorageHACostItemType item_type,
              const share::ObStorageHACostItemName name,
              const int result,
              const uint64_t timestamp,
              const int64_t start_ts,
              const bool is_report);
  static void process_finish_out_perf_diag_info(
              const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
              const share::ObStorageHACostItemType item_type,
              const share::ObStorageHACostItemName name,
              const int result,
              const uint64_t timestamp,
              const int64_t start_ts,
              const bool is_report);
  static void process_finish_in_perf_diag_info(
              const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
              const share::ObStorageHACostItemType item_type,
              const share::ObStorageHACostItemName name,
              const int result,
              const uint64_t timestamp,
              const int64_t start_ts,
              const bool is_report);
private:
  static int get_ls_(
      ObLSHandle &ls_handle,
      const share::ObLSID &dest_ls_id,
      ObLS *&ls);
  static int get_transfer_handler_(
      ObLSHandle &ls_handle,
      const share::ObLSID &dest_ls_id,
      ObTransferHandler *&transfer_handler);
  static int get_ls_migrate_status_(
      ObLSHandle &ls_handle,
      const share::ObLSID &dest_ls_id,
      ObMigrationStatus &migration_status);

  static void construct_perf_diag_replay_params_(
              const share::ObLSID &dest_ls_id_,
              const share::ObTransferTaskID &task_id_,
              const share::ObStorageHADiagTaskType task_type_,
              const share::ObStorageHACostItemType item_type_,
              const share::ObStorageHACostItemName item_name,
              const int64_t tablet_count,
              share::ObStorageHAPerfDiagParams &params);

  static void add_transfer_perf_diagnose_in_replay_(
              const share::ObStorageHAPerfDiagParams &params,
              const int result,
              const uint64_t timestamp,
              const int64_t start_ts,
              const bool is_report);
  static void construct_perf_diag_backfill_params_(
              const share::ObLSID &dest_ls_id,
              const common::ObTabletID &tablet_id,
              const share::ObStorageHACostItemType item_type,
              const share::ObStorageHACostItemName item_name,
              share::ObStorageHAPerfDiagParams &params);
};

} // end namespace storage
} // end namespace oceanbase

#endif
