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

#ifndef OCEANBASE_STORAGE_OB_LS_TABLET_SERVICE
#define OCEANBASE_STORAGE_OB_LS_TABLET_SERVICE

#include "common/ob_tablet_id.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_iarray.h"
#include "lib/lock/ob_bucket_lock.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/palf/palf_callback.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_dml_common.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/high_availability/ob_tablet_ha_status.h"
#include "storage/tablelock/ob_lock_memtable_mgr.h"
#include "storage/tx_table/ob_tx_ctx_memtable_mgr.h"
#include "storage/tx_table/ob_tx_data_memtable_mgr.h"
#include "storage/tablelock/ob_lock_memtable_mgr.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tablet/ob_tablet_id_set.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/multi_data_source/mds_table_mgr.h"

namespace oceanbase
{
namespace observer
{
class ObIMetaReport;
}
namespace common
{
class ObRowStore;
}

namespace share
{
namespace schema
{
struct ColumnMap;
}
}

namespace blocksstable
{
class ObMigrationSSTableParam;
struct ObDatumRowkey;
}

namespace compaction
{
class ObTabletMergeCtx;
}

namespace storage
{
class ObLS;
struct ObMetaDiskAddr;
class ObRowReshape;
class ObDMLRunningCtx;
class ObTableHandleV2;
class ObTableScanIterator;
class ObSingleRowGetter;
class ObLSTabletIterator;
class ObHALSTabletIDIterator;
class ObHALSTabletIterator;
class ObLSTabletFastIter;
class ObTabletMapKey;
struct ObStorageLogParam;
struct ObTabletCreateSSTableParam;
struct ObUpdateTableStoreParam;
class ObTabletTxMultiSourceDataUnit;
struct ObMigrationTabletParam;
class ObTableScanRange;
class ObTabletCreateDeleteMdsUserData;


class ObLSTabletService : public logservice::ObIReplaySubHandler,
                          public logservice::ObIRoleChangeSubHandler,
                          public logservice::ObICheckpointSubHandler
{
public:
  ObLSTabletService();
  ObLSTabletService(const ObLSTabletService&) = delete;
  ObLSTabletService &operator=(const ObLSTabletService&) = delete;
  virtual ~ObLSTabletService();
public:
  int init(ObLS *ls);
  int stop();
  void destroy();
  int offline();
  int online();
  // TODO: delete it if apply sequence
  // set allocators frozen to reduce active tenant_memory in ObLS::offline_()
  int set_frozen_for_all_memtables();
public:
  class AllowToReadMgr final
  {
  public:
    AllowToReadMgr() : allow_to_read_(false) {}
    ~AllowToReadMgr() = default;
    void disable_to_read();
    void enable_to_read();
    void load_allow_to_read_info(bool &allow_to_read);
  private:
    bool allow_to_read_;
  };
private:
  // for replay
  virtual int replay(
      const void *buffer,
      const int64_t nbytes,
      const palf::LSN &lsn,
      const share::SCN &scn) override;

  // for role change
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;

  // for checkpoint
  virtual int flush(share::SCN &recycle_scn) override;
  virtual share::SCN get_rec_scn() override;

public:
  int prepare_for_safe_destroy();
  int safe_to_destroy(bool &is_safe);

  // tablet operation
  int create_ls_inner_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &frozen_timestamp,
      const ObCreateTabletSchema &create_tablet_schema,
      const share::SCN &create_scn);
  int remove_ls_inner_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);

  int create_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &data_tablet_id,
      const share::SCN &create_scn,
      const int64_t snapshot_version,
      const ObCreateTabletSchema &create_tablet_schema,
      const lib::Worker::CompatMode &compat_mode,
      const bool need_create_empty_major_sstable,
      ObTabletHandle &tablet_handle);
  int create_transfer_in_tablet(
      const share::ObLSID &ls_id,
      const ObMigrationTabletParam &tablet_meta,
      ObTabletHandle &tablet_handle);
  int rollback_remove_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_start_scn);

  int get_tablet(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle,
      const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US * 10,
      const ObMDSGetTabletMode mode = ObMDSGetTabletMode::READ_READABLE_COMMITED);
  int get_tablet_with_timeout(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle,
      const int64_t retry_timeout_us,
      const ObMDSGetTabletMode mode = ObMDSGetTabletMode::READ_READABLE_COMMITED,
      const share::SCN &snapshot = share::SCN::max_scn());

  int remove_tablets(const common::ObIArray<common::ObTabletID> &tablet_id_array);
  // Exactly deletion compared with input tablets
  int remove_tablet(const ObTabletHandle& tablet_handle);
  int get_ls_min_end_scn(
      share::SCN &min_end_scn_from_latest_tablets,
      share::SCN &min_end_scn_from_old_tablets);
  int get_tx_data_memtable_mgr(ObMemtableMgrHandle &mgr_handle);
  int get_tx_ctx_memtable_mgr(ObMemtableMgrHandle &mgr_handle);
  int get_lock_memtable_mgr(ObMemtableMgrHandle &mgr_handle);
  int get_mds_table_mgr(mds::MdsTableMgrHandle &mgr_handle);
  int64_t get_tablet_count() const;

  // update tablet
  int update_tablet_checkpoint(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &old_addr,
    const ObMetaDiskAddr &new_addr,
    const bool is_replay_old,
    ObTabletHandle &new_handle);
  int update_tablet_table_store(
      const common::ObTabletID &tablet_id,
      const ObUpdateTableStoreParam &param,
      ObTabletHandle &handle);
  int update_medium_compaction_info(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle);
  int update_tablet_table_store( // only for small sstables defragmentation
      const ObTabletHandle &old_tablet_handle,
      const ObIArray<storage::ObITable *> &tables);
  int update_tablet_report_status(
      const common::ObTabletID &tablet_id,
      const bool found_column_group_checksum_error = false);
  int update_tablet_snapshot_version(
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version);
  int build_new_tablet_from_mds_table(
      compaction::ObTabletMergeCtx &ctx,
      const common::ObTabletID &tablet_id,
      const ObTableHandleV2 &mds_mini_sstable_handle,
      const share::SCN &flush_scn,
      ObTabletHandle &handle);
  int update_tablet_release_memtable_for_offline(
      const common::ObTabletID &tablet_id,
      const SCN scn);
  int update_tablet_ddl_commit_scn(
      const common::ObTabletID &tablet_id,
      const SCN ddl_commit_scn);
  int update_tablet_restore_status(
      const common::ObTabletID &tablet_id,
      const ObTabletRestoreStatus::STATUS &restore_status,
      const bool need_reset_transfer_flag);
  int update_tablet_ha_data_status(
      const common::ObTabletID &tablet_id,
      const ObTabletDataStatus::STATUS &data_status);
  int update_tablet_ha_expected_status(
      const common::ObTabletID &tablet_id,
      const ObTabletExpectedStatus::STATUS &expected_status);
  // Get tablet handle but ignore empty shell. Return OB_TABLET_NOT_EXIST if it is empty shell.
  int ha_get_tablet(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle);
  int get_tablet_without_memtables(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &handle);
  int ha_get_tablet_without_memtables(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &handle);
  int update_tablet_to_empty_shell(const common::ObTabletID &tablet_id);
  int replay_create_tablet(
      const ObMetaDiskAddr &disk_addr,
      const char *buf,
      const int64_t buf_len,
      const ObTabletID &tablet_id,
      ObTabletTransferInfo &tablet_transfer_info);

  int create_memtable(
      const common::ObTabletID &tablet_id,
      const int64_t schema_version,
      const bool for_direct_load,
      const bool for_replay,
      const share::SCN clog_checkpoint_scn = share::SCN::min_scn());
  int get_read_tables(
      const common::ObTabletID tablet_id,
      const int64_t timeout_us,
      const int64_t snapshot_version,
      ObTabletTableIterator &iter,
      const bool allow_no_ready_read = false);
  int check_allow_to_read();
  int set_tablet_status(
      const common::ObTabletID &tablet_id,
      const ObTabletCreateDeleteMdsUserData &tablet_status,
      mds::MdsCtx &ctx);
  int replay_set_tablet_status(
      const common::ObTabletID &tablet_id,
      const share::SCN &scn,
      const ObTabletCreateDeleteMdsUserData &tablet_status,
      mds::MdsCtx &ctx);
  int set_ddl_info(
      const common::ObTabletID &tablet_id,
      const ObTabletBindingMdsUserData &ddl_info,
      mds::MdsCtx &ctx,
      const int64_t timeout_us);
  int replay_set_ddl_info(
      const common::ObTabletID &tablet_id,
      const share::SCN &scn,
      const ObTabletBindingMdsUserData &ddl_info,
      mds::MdsCtx &ctx);

  // DAS interface
  int table_scan(
      ObTabletHandle &tablet_handle,
      ObTableScanIterator &iter,
      ObTableScanParam &param);
  int table_rescan(
      ObTabletHandle &tablet_handle,
      ObTableScanParam &param,
      common::ObNewRowIterator *result);
  int insert_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int insert_row(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &duplicated_column_ids,
      const common::ObNewRow &row,
      const ObInsertFlag flag,
      int64_t &affected_rows,
      common::ObNewRowIterator *&duplicated_rows);
  int update_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      const ObIArray< uint64_t> &updated_column_ids,
      ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int put_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      ObNewRowIterator *row_iter,
      int64_t &affected_rows); // for htable, insert or update
  int delete_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int lock_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObLockFlag lock_flag,
      const bool is_sfu,
      ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int lock_row(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObNewRow &row,
      const ObLockFlag lock_flag,
      const bool is_sfu);
  int get_multi_ranges_cost(
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      const common::ObIArray<common::ObStoreRange> &ranges,
      int64_t &total_size);
  int split_multi_ranges(
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      const ObIArray<ObStoreRange> &ranges,
      const int64_t expected_task_count,
      common::ObIAllocator &allocator,
      ObArrayArray<ObStoreRange> &multi_range_split_array);
  int estimate_row_count(
      const ObTableScanParam &param,
      const ObTableScanRange &scan_range,
      const int64_t timeout_us,
      common::ObIArray<ObEstRowCountRecord> &est_records,
      int64_t &logical_row_count,
      int64_t &physical_row_count);
  int estimate_block_count_and_row_count(
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      int64_t &macro_block_count,
      int64_t &micro_block_count,
      int64_t &sstable_row_count,
      int64_t &memtable_row_count,
      common::ObIArray<int64_t> &cg_macro_cnt_arr,
      common::ObIArray<int64_t> &cg_micro_cnt_arr);

  // iterator
  int build_tablet_iter(ObLSTabletIterator &iter, const bool except_ls_inner_tablet = false);
  int build_tablet_iter(ObHALSTabletIDIterator &iter);
  int build_tablet_iter(ObHALSTabletIterator &iter);
  int build_tablet_iter(ObLSTabletFastIter &iter, const bool except_ls_inner_tablet = false);

  // migration section
  typedef common::ObFunction<int(const obrpc::ObCopyTabletInfo &tablet_info, const ObTabletHandle &tablet_handle)> HandleTabletMetaFunc;
  int ha_scan_all_tablets(const HandleTabletMetaFunc &handle_tablet_meta_f);
  int trim_rebuild_tablet(
      const ObTabletID &tablet_id,
      const bool is_rollback = false);
  int rebuild_create_tablet(
      const ObMigrationTabletParam &mig_tablet_param,
      const bool keep_old);
  int create_or_update_migration_tablet(
      const ObMigrationTabletParam &mig_tablet_param,
      const bool is_transfer);
  int build_ha_tablet_new_table_store(
      const ObTabletID &tablet_id,
      const ObBatchUpdateTableStoreParam &param);
  void enable_to_read();
  void disable_to_read();
  int get_all_tablet_ids(const bool except_ls_inner_tablet, common::ObIArray<ObTabletID> &tablet_id_array);

  int flush_mds_table(int64_t recycle_scn);

  // for transfer check tablet write stop
  int check_tablet_no_active_memtable(const ObIArray<ObTabletID> &tablet_list, bool &has);
protected:
  virtual int prepare_dml_running_ctx(
      const common::ObIArray<uint64_t> *column_ids,
      const common::ObIArray<uint64_t> *upd_col_ids,
      ObTabletHandle &tablet_handle, // TODO: tablet handles is IN param, should be const
      ObDMLRunningCtx &run_ctx);
private:
  typedef ObSEArray<int64_t, 8> UpdateIndexArray;
  class GetAllTabletIDOperator final
  {
  public:
    explicit GetAllTabletIDOperator(common::ObIArray<common::ObTabletID> &tablet_ids,
        const bool except_ls_inner_tablet = false)
    : except_ls_inner_tablet_(except_ls_inner_tablet), tablet_ids_(tablet_ids) {}
    ~GetAllTabletIDOperator() = default;
    int operator()(const common::ObTabletID &tablet_id);
  private:
    bool except_ls_inner_tablet_;
    common::ObIArray<common::ObTabletID> &tablet_ids_;
  };
  class DestroyMemtableAndMemberAndMdsTableOperator final
  {
  public:
    DestroyMemtableAndMemberAndMdsTableOperator(ObLSTabletService *tablet_svr)
      : tablet_svr_(tablet_svr) {}
    ~DestroyMemtableAndMemberAndMdsTableOperator() = default;
    int operator()(const common::ObTabletID &tablet_id);
    common::ObTabletID cur_tablet_id_;
    ObLSTabletService *tablet_svr_;
  };
  class SetMemtableFrozenOperator final
  {
  public:
    SetMemtableFrozenOperator(ObLSTabletService *tablet_svr)
      : tablet_svr_(tablet_svr) {}
    ~SetMemtableFrozenOperator() = default;
    int operator()(const common::ObTabletID &tablet_id);
    common::ObTabletID cur_tablet_id_;
    ObLSTabletService *tablet_svr_;
  };
  class ObUpdateDDLCommitSCN final : public ObITabletMetaModifier
  {
  public:
    explicit ObUpdateDDLCommitSCN(const share::SCN ddl_commit_scn) : ddl_commit_scn_(ddl_commit_scn) {}
    virtual ~ObUpdateDDLCommitSCN() = default;
    virtual int modify_tablet_meta(ObTabletMeta &meta) override;
  private:
    const share::SCN ddl_commit_scn_;
    DISALLOW_COPY_AND_ASSIGN(ObUpdateDDLCommitSCN);
  };
private:
  static int refresh_memtable_for_ckpt(
      const ObMetaDiskAddr &old_addr,
      const ObMetaDiskAddr &cur_addr,
      ObTabletHandle &new_tablet_handle);
  static int safe_update_cas_tablet(
      const ObTabletMapKey &key,
      const ObMetaDiskAddr &addr,
      const ObTabletHandle &old_handle,
      ObTabletHandle &new_handle,
      ObTimeGuard &time_guard);
  static int safe_update_cas_empty_shell(
      const ObTabletMapKey &key,
      const ObTabletHandle &old_handle,
      ObTabletHandle &new_handle,
      ObTimeGuard &time_guard);
  int safe_create_cas_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObMetaDiskAddr &addr,
      ObTabletHandle &tablet_handle,
      ObTimeGuard &time_guard);
  int safe_create_cas_empty_shell(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle,
      ObTimeGuard &time_guard);
  void report_tablet_to_rs(const common::ObTabletID &tablet_id);
  void report_tablet_to_rs(const common::ObIArray<common::ObTabletID> &tablet_id_array);

  int direct_get_tablet(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle);
  int inner_table_scan(
      ObTabletHandle &tablet_handle,
      ObTableScanIterator &iter,
      ObTableScanParam &param);
  int get_tablet_addr(const ObTabletMapKey &key, ObMetaDiskAddr &addr);
  int has_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      bool &b_exist);
  int create_inner_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &data_tablet_id,
      const share::SCN &create_scn,
      const int64_t snapshot_version,
      const ObCreateTabletSchema &create_tablet_schema,
      ObTabletHandle &tablet_handle);
  int refresh_tablet_addr(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObUpdateTabletPointerParam &param,
      ObTabletHandle &tablet_handle);
  int do_remove_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  int inner_remove_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  int rollback_remove_tablet_without_lock(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  int rollback_rebuild_tablet(const ObTabletID &tablet_id);
  int trim_old_tablets(const ObTabletID &tablet_id);
  int rebuild_tablet_with_old(
      const ObMigrationTabletParam &mig_tablet_param,
      ObTabletHandle &tablet_guard);
  int migrate_update_tablet(const ObMigrationTabletParam &mig_tablet_param);
  int migrate_create_tablet(
      const ObMigrationTabletParam &mig_tablet_param,
      ObTabletHandle &handle);
  int delete_all_tablets();
  int offline_build_tablet_without_memtable_();
  int offline_gc_tablet_for_create_or_transfer_in_abort_();
  int offline_destroy_memtable_and_mds_table_();
  int mock_duplicated_rows_(common::ObNewRowIterator *&duplicated_rows);
private:
  static int check_real_leader_for_4377_(const ObLSID ls_id);
  static int check_need_rollback_in_transfer_for_4377_(const transaction::ObTxDesc *tx_desc,
                                                       ObTabletHandle &tablet_handle);
  static int check_parts_tx_state_in_transfer_for_4377_(transaction::ObTxDesc *tx_desc);
  static int build_create_sstable_param_for_migration(
      const blocksstable::ObMigrationSSTableParam &migrate_sstable_param,
      ObTabletCreateSSTableParam &create_sstable_param);
  static int need_check_old_row_legitimacy(
      ObDMLRunningCtx &run_ctx,
      bool &need_check,
      bool &is_udf);
  static int construct_table_rows(
      const ObNewRow *rows,
      ObStoreRow *tbl_rows,
      int64_t row_count);
  static int check_old_row_legitimacy(
      ObTabletHandle &data_tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const common::ObNewRow &old_row);
  static int check_new_row_legitimacy(
      ObDMLRunningCtx &run_ctx,
      const common::ObNewRow &new_row);
  static int insert_rows_to_tablet(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const common::ObNewRow *const rows,
      const int64_t row_count,
      ObRowsInfo &rows_info,
      storage::ObStoreRow *tbl_rows,
      int64_t &afct_num,
      int64_t &dup_num);
  static int insert_tablet_rows(
      const int64_t row_count,
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObStoreRow *rows,
      ObRowsInfo &rows_info);
  static int insert_lob_col(
      ObDMLRunningCtx &run_ctx,
      const ObColDesc &column,
      ObObj &obj,
      ObLobAccessParam *del_param,
      ObLobCommon *lob_common);
  static int insert_lob_tablet_row(
      ObTabletHandle &data_tablet,
      ObDMLRunningCtx &run_ctx,
      ObStoreRow &row);
  static int insert_lob_tablet_rows(
      ObTabletHandle &data_tablet,
      ObDMLRunningCtx &run_ctx,
      ObStoreRow *rows,
      int64_t row_count);
  static int extract_rowkey(
      const ObRelativeTable &table,
      const common::ObStoreRowkey &rowkey,
      char *buffer,
      const int64_t buffer_len,
      const common::ObTimeZoneInfo *tz_info = nullptr);
  static int extract_rowkey(
      const ObRelativeTable &table,
      const blocksstable::ObDatumRowkey &rowkey,
      char *buffer,
      const int64_t buffer_len,
      const common::ObTimeZoneInfo *tz_info = nullptr);
  static int get_next_rows(
      ObNewRowIterator *row_iter,
      ObNewRow *&rows,
      int64_t &row_count);
  static int construct_update_idx(
      const int64_t schema_rowkey_cnt,
      const share::schema::ColumnMap *col_map,
      const common::ObIArray<uint64_t> &upd_col_ids,
      UpdateIndexArray &update_idx);
  static int check_rowkey_change(
      const ObIArray<uint64_t> &update_ids,
      const ObRelativeTable &relative_table,
      bool &rowkey_change,
      bool &delay_new);
  static int check_rowkey_value_change(
      const common::ObNewRow &old_row,
      const common::ObNewRow &new_row,
      const int64_t rowkey_len,
      bool &rowkey_change);
  static int process_delta_lob(
      ObDMLRunningCtx &run_ctx,
      const ObColDesc &column,
      ObObj &old_obj,
      ObLobLocatorV2 &delta_lob,
      ObObj &obj);
  static int register_ext_info_commit_cb(
      ObDMLRunningCtx &run_ctx,
      ObObj &col_data,
      ObObj &ext_info_data);
  static int set_lob_storage_params(
      ObDMLRunningCtx &run_ctx,
      const ObColDesc &column,
      ObLobAccessParam &lob_param);

  static int process_lob_row(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const ObIArray<int64_t> &update_idx,
      bool data_tbl_rowkey_change,
      ObStoreRow &old_sql_row,
      ObStoreRow &old_row,
      ObStoreRow &new_row);
  static int update_row_to_tablet(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const bool rowkey_change,
      const ObIArray<int64_t> &update_idx,
      const bool delay_new,
      const bool lob_update,
      ObStoreRow &old_tbl_row,
      ObStoreRow &new_tbl_row,
      ObRowStore *row_store,
      bool &duplicate);
  static int process_old_row(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const bool data_tbl_rowkey_change,
      const bool lob_update,
      ObStoreRow &tbl_row);
  static int process_new_row(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const common::ObIArray<int64_t> &update_idx,
      const ObStoreRow &old_tbl_row,
      const ObStoreRow &new_tbl_row,
      const bool rowkey_change);
  static int process_data_table_row(
      ObTabletHandle &data_tablet,
      ObDMLRunningCtx &run_ctx,
      const ObIArray<int64_t> &update_idx,
      const ObStoreRow &old_tbl_row,
      const ObStoreRow &new_tbl_row,
      const bool rowkey_change);
  static int check_new_row_nullable_value(
      const ObIArray<uint64_t> &column_ids,
      ObRelativeTable &data_table,
      const ObNewRow &new_row);
  static int check_new_row_nullable_value(
      const common::ObIArray<share::schema::ObColDesc> &col_descs,
      ObRelativeTable &relative_table,
      const common::ObNewRow &new_row);
  static int check_new_row_shadow_pk(
      const ObIArray<uint64_t> &column_ids,
      ObRelativeTable &data_table,
      const ObNewRow &new_row);
  static int check_row_locked_by_myself(
      ObTabletHandle &tablet_handle,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const blocksstable::ObDatumRowkey &rowkey,
      bool &locked);
  static int get_conflict_rows(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const ObInsertFlag flag,
      const common::ObIArray<uint64_t> &out_col_ids,
      const common::ObNewRow &row,
      common::ObNewRowIterator *&duplicated_rows);
  static int init_single_row_getter(
      ObSingleRowGetter &row_getter,
      ObDMLRunningCtx &run_ctx,
      const ObIArray<uint64_t> &out_col_ids,
      ObRelativeTable &relative_table,
      bool skip_read_lob = false);
  static int single_get_row(
      ObSingleRowGetter &row_getter,
      const blocksstable::ObDatumRowkey &rowkey,
      ObNewRowIterator *&duplicated_rows);
  static int convert_row_to_rowkey(
      ObSingleRowGetter &index_row_getter,
      ObStoreRowkey &rowkey);
  static int get_next_row_from_iter(
      ObNewRowIterator *row_iter,
      ObStoreRow &store_row,
      const bool need_copy_cells);
  static int insert_row_to_tablet(
      const bool check_exist,
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObStoreRow &tbl_row);
  static int process_old_row_lob_col(
      ObTabletHandle &data_tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObStoreRow &tbl_row);
  static int table_refresh_row(
      ObTabletHandle &data_tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObNewRow &row);
  static int delete_row_in_tablet(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const ObNewRow &row);
  static int delete_lob_col(
      ObDMLRunningCtx &run_ctx,
      const ObColDesc &column,
      ObObj &obj,
      const ObObj &sql_obj,
      ObLobCommon *&lob_common,
      ObLobAccessParam &lob_param);
  static int delete_lob_tablet_rows(
      ObDMLRunningCtx &run_ctx,
      ObTabletHandle &data_tablet,
      ObStoreRow &tbl_row,
      const ObNewRow &row);
  static int prepare_scan_table_param(
      ObTableScanParam &param,
      share::schema::ObMultiVersionSchemaService &schema_service);
  static void dump_diag_info_for_old_row_loss(
      ObRelativeTable &data_table,
      ObStoreCtx &store_ctx,
      const ObStoreRow &tbl_row);
  int set_allow_to_read_(ObLS *ls);
  // TODO(chenqingxiang.cqx): remove this
  int create_empty_shell_tablet(
      const ObMigrationTabletParam &param,
      ObTabletHandle &tablet_handle);
  int check_rollback_tablet_is_same_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_start_scn,
      bool &is_same);
private:
  int direct_insert_rows(const uint64_t table_id,
                         const int64_t px_task_id,
                         const int64_t ddl_task_id,
                         const common::ObTabletID &tablet_id,
                         const common::ObIArray<uint64_t> &column_ids,
                         common::ObNewRowIterator *row_iter,
                         int64_t &affected_rows);
  static int check_is_gencol_check_failed(const ObRelativeTable &data_table,
                                uint64_t error_col_id,
                                bool &is_virtual_gencol);

private:
  friend class ObLSTabletIterator;
  friend class ObTabletCreateMdsHelper;
  friend class ObLSTabletFastIter;

  ObLS *ls_;
  ObTxDataMemtableMgr tx_data_memtable_mgr_;
  ObTxCtxMemtableMgr tx_ctx_memtable_mgr_;
  ObLockMemtableMgr lock_memtable_mgr_;
  mds::ObMdsTableMgr mds_table_mgr_;
  ObTabletIDSet tablet_id_set_;
  common::ObBucketLock bucket_lock_; // for tablet update, not for dml
  AllowToReadMgr allow_to_read_mgr_;
  bool is_inited_;
  bool is_stopped_;
};

inline int64_t ObLSTabletService::get_tablet_count() const
{
  return tablet_id_set_.size();
}
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_LS_TABLET_SERVICE
