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
#include "storage/ob_relative_table.h"
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
#include "storage/blocksstable/ob_datum_row_iterator.h"

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
enum ObDmlFlag;
class ObDatumRowStore;
}

namespace compaction
{
struct ObMulSourceDataNotifyArg;
struct ObEncryptMetaCache;
class ObTabletMergeCtx;
}
namespace sql
{
class ObDASDMLIterator;
class ObDASUpdIterator;
}
namespace rootserver
{
struct ObTruncateTabletArg;
}

namespace storage
{
class ObLS;
struct ObMetaDiskAddr;
class ObRowReshape;
class ObDMLRunningCtx;
class ObTableHandleV2;
class ObTableScanIterator;
class ObRowGetter;
class ObLSTabletIterator;
class ObLSTabletAddrIterator;
class ObHALSTabletIDIterator;
class ObHALSTabletIterator;
class ObLSTabletFastIter;
class ObTabletMapKey;
struct ObStorageLogParam;
struct ObTabletCreateSSTableParam;
struct ObUpdateTableStoreParam;
struct ObMigrationTabletParam;
class ObTableScanRange;
class ObTabletCreateDeleteMdsUserData;
class ObBlockStatScanParam;
class ObBlockStatIterator;


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
      const share::SCN &clog_checkpoint_scn,
      const share::SCN &mds_checkpoint_scn,
      const storage::ObTabletMdsUserDataType &create_type,
      const bool micro_index_clustered,
      const bool has_cs_replica,
      const ObTabletID &split_src_tablet_id,
      const uint64_t data_format_version,
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
  int get_ls_migration_required_size(int64_t &required_size);
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
  int update_tablet_ddl_replay_status_for_cs_replica(
      const common::ObTabletID &tablet_id,
      const ObCSReplicaDDLReplayStatus &ddl_replay_status);
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
  int update_tablet_restore_status(
      const share::SCN &reorg_scn,
      const common::ObTabletID &tablet_id,
      const ObTabletRestoreStatus::STATUS &restore_status,
      const bool need_reset_transfer_flag,
      const bool need_to_set_split_data_complete);
  int update_tablet_ha_data_status(
      const share::SCN &reorg_scn,
      const common::ObTabletID &tablet_id,
      const ObTabletDataStatus::STATUS &data_status);
  int update_tablet_ha_expected_status(
      const share::SCN &reorg_scn,
      const common::ObTabletID &tablet_id,
      const ObTabletExpectedStatus::STATUS &expected_status);
#ifdef OB_BUILD_SHARED_STORAGE
  int update_tablet_ss_change_version(
    const share::SCN &reorg_scn,
    const common::ObTabletID &tablet_id,
    const share::SCN &ss_change_version,
    const bool &fully_applied);
  int get_pending_upload_tablet_id_arr(
    const SCN &ls_ss_checkpoint_scn,
    ObIArray<ObTabletID> &tablet_id_arr);
  int write_tablet_id_set_to_pending_free();

  // Create or update local tablet with shared tablet.
  int create_or_update_with_ss_tablet(
      const ObTablet &ss_tablet,
      const SCN &meta_version,
      const SCN &tx_data_table_filled_tx_scn);

  int advance_notify_ss_change_version(
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const share::SCN &change_version);
#endif
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
  int update_tablet_to_empty_shell(
      const uint64_t data_version,
      const common::ObTabletID &tablet_id);
  int replay_create_tablet(
      const ObUpdateTabletPointerParam &param,
      const char *buf,
      const int64_t buf_len,
      const ObTabletID &tablet_id,
      ObTabletTransferInfo &tablet_transfer_info);

  int create_memtable(const common::ObTabletID &tablet_id, CreateMemtableArg &arg);
  int get_read_tables(
      const common::ObTabletID tablet_id,
      const int64_t timeout_us,
      // snapshot_version_for_tablet refers to the version provided to the
      // multi-data source for obtaining multi-version tablet status. Generally,
      // it is the txn's read version provided to obtain the corresponding
      // tablet status. Sometimes(for example, during write), we also provide
      // INT64_MAX to get the latest tablet status.
      const int64_t snapshot_version_for_tablet,
      // snapshot_version_for_tables refers to the version provided to the
      // table_store to obtain the required tables (including memtables and
      // sstables) for the caller. The function use the snapshot version to
      // filter the unnecessary tables and confirm the OB_SNAPSHOT_DISCARDED
      const int64_t snapshot_version_for_tables,
      ObTabletTableIterator &iter,
      const bool allow_no_ready_read,
      const bool need_split_src_table,
      const bool need_split_dst_table);
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
  int set_ddl_complete(
      const common::ObTabletID &tablet_id,
      const ObTabletDDLCompleteMdsUserDataKey &key,
      const ObTabletDDLCompleteMdsUserData &ddl_complete,
      mds::MdsCtx &ctx,
      const int64_t timeout);
  int replay_set_ddl_complete(
      const common::ObTabletID &tablet_id,
      const share::SCN &scn,
      const ObTabletDDLCompleteMdsUserDataKey &key,
      const ObTabletDDLCompleteMdsUserData &ddl_data,
      mds::MdsCtx &ctx);
  int set_direct_load_auto_inc_seq(
      const ObTabletID &tablet_id,
      const ObDirectLoadAutoIncSeqData &data,
      mds::MdsCtx &ctx,
      const int64_t timeout_us);
  int replay_set_direct_load_auto_inc_seq(
      const ObTabletID &tablet_id,
      const ObDirectLoadAutoIncSeqData &data,
      mds::MdsCtx &ctx,
      const share::SCN &scn);
  int set_truncate_info(
      const rootserver::ObTruncateTabletArg &arg,
      mds::MdsCtx &ctx,
      const int64_t timeout_us);
  int replay_set_truncate_info(
      const share::SCN &scn,
      const rootserver::ObTruncateTabletArg &arg,
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
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows);
  int insert_rows_with_fetch_dup(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &duplicated_column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      const ObInsertFlag flag,
      int64_t &affected_rows,
      blocksstable::ObDatumRowIterator *&duplicated_rows);
  int update_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      const ObIArray< uint64_t> &updated_column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows);
  int put_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      ObDatumRowIterator *row_iter,
      int64_t &affected_rows); // for htable, insert or update
  int delete_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows);
  int lock_rows(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObLockFlag lock_flag,
      const bool is_sfu,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows);
  int lock_row(
      ObTabletHandle &tablet_handle,
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      blocksstable::ObDatumRow &row,
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
  int inner_estimate_block_count_and_row_count(
      ObTabletTableIterator &tablet_iter,
      int64_t &macro_block_count,
      int64_t &micro_block_count,
      int64_t &sstable_row_count,
      int64_t &memtable_row_count,
      common::ObIArray<int64_t> &cg_macro_cnt_arr,
      common::ObIArray<int64_t> &cg_micro_cnt_arr);
  int estimate_block_count_and_row_count(
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      int64_t &macro_block_count,
      int64_t &micro_block_count,
      int64_t &sstable_row_count,
      int64_t &memtable_row_count,
      common::ObIArray<int64_t> &cg_macro_cnt_arr,
      common::ObIArray<int64_t> &cg_micro_cnt_arr);
  int estimate_skip_index_sortedness(
      const uint64_t& table_id,
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &sample_count,
      common::ObIArray<double> &sortedness,
      common::ObIArray<uint64_t> &res_sample_counts);
  int scan_block_stat(
      const ObTabletHandle &tablet_handle,
      ObBlockStatScanParam &scan_param,
      ObBlockStatIterator &iter);

  // iterator
  int build_tablet_iter(ObLSTabletIterator &iter, const bool except_ls_inner_tablet = false);
  int build_tablet_iter(ObLSTabletAddrIterator &iter);
  int build_tablet_iter(ObHALSTabletIDIterator &iter);
  int build_tablet_iter(ObHALSTabletIterator &iter);
  int build_tablet_iter(ObLSTabletFastIter &iter, const bool except_ls_inner_tablet = false);
  // only used for tenant slog checkpoint
  int build_tablet_iter_with_lock_hold(ObLSTabletFastIter &iter);

  int is_tablet_exist(const common::ObTabletID &tablet_id, bool &is_exist);

  // migration section
  typedef common::ObFunction<int(const obrpc::ObCopyTabletInfo &tablet_info, const ObTabletHandle &tablet_handle)> HandleTabletMetaFunc;
  int ha_scan_all_tablets(
      const HandleTabletMetaFunc &handle_tablet_meta_f,
      const bool need_sorted_tablet_id);
  int rebuild_create_tablet(
      const ObMigrationTabletParam &mig_tablet_param);
  int create_or_update_migration_tablet(
      const ObMigrationTabletParam &mig_tablet_param,
      const bool is_transfer);
  int build_tablet_with_batch_tables(
      const ObTabletID &tablet_id,
      const ObBatchUpdateTableStoreParam &param);
  void enable_to_read();
  void disable_to_read();
  int get_all_tablet_ids(const bool except_ls_inner_tablet, common::ObIArray<ObTabletID> &tablet_id_array);

  int flush_mds_table(int64_t recycle_scn);

  // for transfer check tablet write stop
  int check_tablet_no_active_memtable(const ObIArray<ObTabletID> &tablet_list, bool &has);

  /// @brief: apply defragment tablet(only used for slog checkpoint)
  /// @param t3m: the target that tablet will be applied to.
  /// @param tablet_key: key of specified tablet
  /// @param old_addr: tablet's original address
  /// @param new_handle: handle of specified tablet
  /// @param tsms: used for write slog(if not null).
  int apply_defragment_tablet(
    ObTenantMetaMemMgr &t3m,
    const ObTabletMapKey &tablet_key,
    const ObMetaDiskAddr &old_addr,
    ObTabletHandle &new_handle,
    ObTenantStorageMetaService &tsms);

   /// @brief: handle empty shell when doing slog truncate(only used for slog checkpoint)
   /// @param t3m: used for empty shell cas
   /// @param tablet_key: key of specified empty shell
   /// @param old_addr: empty shell's original address
   /// @return: return OB_NOT_SUPPORTED in SS mode.
   int refresh_empty_shell_for_slog_ckpt(
    ObTenantMetaMemMgr &t3m,
    const ObTabletMapKey &tablet_key,
    const ObMetaDiskAddr &old_addr);

  int alloc_private_tablet_meta_version_with_lock(const ObTabletMapKey &key, int64_t &tablet_meta_version);
  int check_allow_tablet_macro_check(
      const ObTabletID &tablet_id,
      const int32_t private_transfer_epoch,
      bool &allow);
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
  class ObUpdateRestoreStatus final : public ObITabletMetaModifier
  {
  public:
    explicit ObUpdateRestoreStatus(
        const ObTabletRestoreStatus::STATUS &restore_status,
        const bool need_reset_transfer_flag,
        const bool need_to_set_split_data_complete)
      : restore_status_(restore_status),
        need_reset_transfer_flag_(need_reset_transfer_flag),
        need_to_set_split_data_complete_(need_to_set_split_data_complete) {}
    virtual ~ObUpdateRestoreStatus() = default;
    virtual int modify_tablet_meta(ObTabletMeta &meta) override;
  private:
    const ObTabletRestoreStatus::STATUS restore_status_;
    const bool need_reset_transfer_flag_;
    const bool need_to_set_split_data_complete_;
    DISALLOW_COPY_AND_ASSIGN(ObUpdateRestoreStatus);
  };
  class ObUpdateHAExpectedStatus final : public ObITabletMetaModifier
  {
  public:
    explicit ObUpdateHAExpectedStatus(const ObTabletExpectedStatus::STATUS &expected_status)
      : expected_status_(expected_status) {}
    virtual ~ObUpdateHAExpectedStatus() = default;
    virtual int modify_tablet_meta(ObTabletMeta &meta) override;
  private:
    const ObTabletExpectedStatus::STATUS expected_status_;
    DISALLOW_COPY_AND_ASSIGN(ObUpdateHAExpectedStatus);
  };
  class ObDmlSplitCtx final {
  public:
    ObDmlSplitCtx() : allocator_(), dst_tablet_handle_(), dst_relative_table_() {}
    ~ObDmlSplitCtx() = default;
    int prepare_write_dst(
        const ObTabletID &src_tablet_id,
        const ObTabletID &dst_tablet_id,
        ObStoreCtx &store_ctx,
        const ObRelativeTable &relative_table);
    int prepare_write_dst(
        ObTabletHandle &tablet_handle,
        const blocksstable::ObDatumRow *data_row_for_lob,
        ObStoreCtx &store_ctx,
        const ObRelativeTable &relative_table,
        const ObDatumRowkey &rowkey);
    int prepare_write_dst(
        ObTabletHandle &tablet_handle,
        const blocksstable::ObDatumRow *data_row_for_lob,
        ObStoreCtx &store_ctx,
        const ObRelativeTable &relative_table,
        const blocksstable::ObDatumRow &new_row);
    void reuse();
  public:
    ObArenaAllocator allocator_;
    ObTabletHandle dst_tablet_handle_;
    ObRelativeTable dst_relative_table_;
    DISALLOW_COPY_AND_ASSIGN(ObDmlSplitCtx);
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
      ObTimeGuard &time_guard,
      const share::SCN &ss_change_version = share::SCN::invalid_scn());
  int safe_update_cas_empty_shell(
      const uint64_t data_version,
      const ObTabletMapKey &key,
      const ObTabletHandle &old_handle,
      ObTabletHandle &new_handle,
      ObTimeGuard &time_guard);
  int safe_create_cas_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObMetaDiskAddr &addr,
      ObTabletHandle &tablet_handle,
      ObTimeGuard &time_guard,
      const share::SCN &ss_change_version = share::SCN::invalid_scn());
  int safe_create_cas_empty_shell(
      const uint64_t data_version,
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
      const uint64_t data_version,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &data_tablet_id,
      const share::SCN &create_scn,
      const int64_t snapshot_version,
      const ObCreateTabletSchema &create_tablet_schema,
      ObTabletHandle &tablet_handle);
  int inner_estimate_block_count_and_row_count(
      ObTabletTableIterator &tablet_iter,
      int64_t &macro_block_count,
      int64_t &micro_block_count,
      int64_t &sstable_row_count,
      int64_t &memtable_row_count);
  int estimate_block_count_and_row_count_for_split_extra(
      const ObTabletID &tablet_id,
      const int64_t split_cnt,
      const ObMDSGetTabletMode mode,
      const int64_t timeout_us,
      const int64_t snapshot_version_for_tables,
      int64_t &macro_block_count,
      int64_t &micro_block_count,
      int64_t &sstable_row_count,
      int64_t &memtable_row_count,
      common::ObIArray<int64_t> &cg_macro_cnt_arr,
      common::ObIArray<int64_t> &cg_micro_cnt_arr);

  int refresh_tablet_addr(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObUpdateTabletPointerParam &param,
      ObTabletHandle &tablet_handle);
  int do_remove_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  /// @brief: remove tablet if it exists and return its(only called by create_transfer_in_tablet)
  /// current meta version
  /// @param[out] need_check_gc_queue: true if tablet not exists; false by default.
  int do_remove_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      /*out*/ int64_t &tablet_meta_version,
      /*out*/ bool &need_check_gc_queue);
  int inner_remove_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  int rollback_remove_tablet_without_lock(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  int migrate_update_tablet(
      const uint64_t data_version,
      const ObMigrationTabletParam &mig_tablet_param);
  int migrate_create_tablet(
      const uint64_t data_version,
      const ObMigrationTabletParam &mig_tablet_param,
      ObTabletHandle &handle);
  // Create tablet with shared tablet.
  int update_with_ss_tablet(
      const uint64_t data_version,
      const ObTablet &ss_tablet,
      const SCN &meta_version,
      const SCN &tx_data_table_filled_tx_scn);
  int create_with_ss_tablet(
      const uint64_t data_version,
      const ObTablet &ss_tablet,
      const SCN &meta_version,
      const SCN &tx_data_table_filled_tx_scn,
      ObTabletHandle &handle);
  int delete_all_tablets();
  int offline_build_tablet_without_memtable_();
  int offline_gc_tablet_for_create_or_transfer_in_abort_();
  int offline_destroy_memtable_and_mds_table_();

  int inner_get_read_tables(
      const common::ObTabletID tablet_id,
      const int64_t timeout_us,
      const int64_t snapshot_version_for_tablet,
      const int64_t snapshot_version_for_tables,
      ObTabletTableIterator &iter,
      const bool allow_no_ready_read,
      const bool need_split_src_table,
      const bool need_split_dst_table,
      const ObMDSGetTabletMode mode);
  int inner_get_read_tables_for_split_src(
      const common::ObTabletID tablet_id,
      const int64_t timeout_us,
      const int64_t snapshot_version,
      ObTabletTableIterator &iter,
      const bool allow_no_ready_read);

  int mock_duplicated_rows_(blocksstable::ObDatumRowIterator *&duplicated_rows);

  int alloc_private_tablet_meta_version_without_lock(const ObTabletMapKey &key, int64_t &tablet_meta_version);
  int update_private_tablet_last_match_meta_version_without_lock(
    const common::ObTabletID &tablet_id,
    ObTimeGuard &time_guard);

#ifdef OB_BUILD_SHARED_STORAGE
  int register_all_sstables_upload_(ObTabletHandle &new_tablet_handle);
#endif
private:
  static int replay_deserialize_tablet(
      const ObTabletMapKey &key,
      const char *buf,
      const int64_t buf_len,
      const ObTabletHandle &tablet_handle,
      common::ObArenaAllocator &allocator,
      ObTabletPoolType &pool_type,
      ObUpdateTabletPointerParam &param);
  static int check_real_leader_for_4377_(const ObLSID ls_id);
  static int check_need_rollback_in_transfer_for_4377_(const transaction::ObTxDesc *tx_desc,
                                                       ObTabletHandle &tablet_handle);
  static int check_parts_tx_state_in_transfer_for_4377_(transaction::ObTxDesc *tx_desc);
  static int check_old_row_legitimacy(
      const blocksstable::ObStoreCmpFuncs &cmp_funcs,
      ObTabletHandle &data_tablet_handle,
      ObRelativeTable &data_table,
      ObStoreCtx &store_ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> *column_ids,
      const ObColDescIArray *col_descs_ptr,
      const bool is_need_check_old_row,
      const bool is_udf,
      const blocksstable::ObDmlFlag &dml_flag,
      const blocksstable::ObDatumRow &old_row);
  static int check_new_row_legitimacy(
      ObDMLRunningCtx &run_ctx,
      const int64_t row_count,
      const blocksstable::ObDatumRow *datum_rows);
  static int insert_rows_to_tablet(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObRowsInfo &rows_info);

  static int put_rows_to_tablet(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObRowsInfo &rows_info,
      int64_t &afct_num);
  static int insert_tablet_rows(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObRowsInfo &rows_info);
  static int put_tablet_rows(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObRowsInfo &rows_info);
  static int insert_vector_index_rows(
      ObTabletHandle &data_tablet,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow *rows,
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
      blocksstable::ObDatumRowIterator *row_iter,
      blocksstable::ObDatumRow *&rows,
      int64_t &row_count);
  static int construct_update_idx(
      const int64_t schema_rowkey_cnt,
      const share::schema::ColumnMap *col_map,
      const common::ObIArray<uint64_t> &upd_col_ids,
      UpdateIndexArray &update_idx);
  static int check_rowkey_change(
      const ObIArray<uint64_t> &update_ids,
      const ObRelativeTable &relative_table,
      bool &rowkey_change);
  static int cache_rows_to_row_store(
      const int64_t row_count,
      ObDatumRow *old_rows,
      ObDatumRow *new_rows,
      ObDatumRowStore &row_store);
  static int update_row_to_tablet(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const bool rowkey_change,
      const ObIArray<int64_t> &update_idx,
      const bool delay_new,
      const bool lob_update,
      ObDatumRow &old_row,
      ObDatumRow &new_row,
      ObDatumRowStore &row_store);
  static int update_rows_to_tablet(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const bool rowkey_change,
      const ObIArray<int64_t> &update_idx,
      const bool delay_new,
      const bool lob_update,
      ObDatumRow *tmp_rows,
      ObRowsInfo &old_rows_info,
      ObRowsInfo &new_rows_info,
      ObDatumRowStore &row_store);
  static int process_old_row(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const bool rowkey_change,
      const bool lob_update,
      ObDatumRow &datum_row);
  static int process_old_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const bool rowkey_change,
    const bool lob_update,
    ObDatumRow *tmp_rows,
    ObRowsInfo &old_rows_info);

  static int process_new_rows(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const common::ObIArray<int64_t> &update_idx,
      const bool rowkey_change,
      ObRowsInfo &old_rows_info,
      ObRowsInfo &new_rows_info);
  static int process_new_row(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const common::ObIArray<int64_t> &update_idx,
      const bool rowkey_change,
      const ObDatumRow &old_datum_row,
      ObDatumRow &new_datum_row);
  static int delay_process_new_rows(
      ObDMLRunningCtx &run_ctx,
      const common::ObIArray<int64_t> &update_idx,
      const bool rowkey_change,
      ObDatumRow &old_row,
      ObDatumRow &new_row,
      ObDatumRowStore &row_store);
  static int check_datum_row_nullable_value(
      const common::ObIArray<share::schema::ObColDesc> &col_descs,
      ObRelativeTable &relative_table,
      const blocksstable::ObDatumRow &datum_row);
  static int check_datum_row_shadow_pk(
      const ObIArray<uint64_t> &column_ids,
      ObRelativeTable &data_table,
      const blocksstable::ObDatumRow &datum_row,
      const blocksstable::ObStorageDatumUtils &rowkey_datum_utils);
  static int check_row_locked_by_myself(
      ObTabletHandle &tablet_handle,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const blocksstable::ObDatumRowkey &rowkey,
      bool &locked);
  static int get_conflict_rows(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObRowsInfo &rows_info);
  static int get_conflict_rows_by_project(
    ObRelativeTable &relative_table,
    const ObRowsInfo &rows_info);
  static int get_conflict_rows_by_multi_get(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObRowsInfo &rows_info);
  static int get_conflict_rows_by_single_get(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObRowsInfo &rows_info);

  static int single_get_conflict_row(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &data_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &out_col_ids,
    const ObDatumRowkey &datum_rowkey,
    blocksstable::ObDatumRowIterator *&dup_row_iter);
  static int init_row_getter(
      ObRowGetter &row_getter,
      ObStoreCtx &store_ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &out_col_ids,
      ObRelativeTable &relative_table,
      const bool is_multi_get,
      const bool skip_read_lob);

  static int process_old_rows_lob_col(
    ObTabletHandle &data_tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const int64_t row_count,
    blocksstable::ObDatumRow *old_rows);
  static int process_old_row_lob_col(
      ObTabletHandle &data_tablet_handle,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &datum_row);
  static int table_refresh_row(
      ObTabletHandle &data_tablet_handle,
      ObRelativeTable &data_table,
      ObStoreCtx &store_ctx,
      const ObDMLBaseParam &dml_param,
      const ObColDescIArray &col_descs,
      ObIAllocator &lob_allocator,
      blocksstable::ObDatumRow &datum_row);
  static int delete_rows_in_tablet(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow *tbl_rows,
      ObRowsInfo &rows_info);
  static int delete_lob_tablet_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow *rows,
    int64_t row_count);
  static int delete_lob_tablet_rows(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &datum_row);
  static int prepare_scan_table_param(
      ObTableScanParam &param,
      share::schema::ObMultiVersionSchemaService &schema_service);
  int set_allow_to_read_(ObLS *ls);
  // TODO(chenqingxiang.cqx): remove this
  int create_empty_shell_tablet(
      const uint64_t data_version,
      const ObMigrationTabletParam &param,
      ObTabletHandle &tablet_handle);
  int check_rollback_tablet_is_same_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_start_scn,
      bool &is_same);

  // for lob tablet dml
  static int check_rowkey_length(
      const ObDMLRunningCtx &run_ctx,
      const blocksstable::ObDatumRow &datum_row);
  static int process_lob_before_insert(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow *rows,
      int64_t row_count);
  static int process_lob_before_insert(
      ObTabletHandle &data_tablet,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &row,
      const int16_t row_idx);
  static int process_lob_after_insert(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow *rows,
      int64_t row_count);
  static int process_lob_before_update(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const ObIArray<int64_t> &update_idx,
      const bool rowkey_change,
      const int64_t row_count,
      blocksstable::ObDatumRow *old_datum_rows,
      blocksstable::ObDatumRow *new_datum_rows);
  static int process_lob_after_update(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const ObIArray<int64_t> &update_idx,
      const bool rowkey_change,
      const int64_t row_count,
      blocksstable::ObDatumRow *old_datum_rows,
      blocksstable::ObDatumRow *new_datum_rows);

private:
  static int get_storage_row(const blocksstable::ObDatumRow &sql_row,
                             const ObIArray<uint64_t> &column_ids,
                             const ObColDescIArray &column_descs,
                             ObRowGetter &row_getter,
                             ObRelativeTable &data_table,
                             ObStoreCtx &store_ctx,
                             const ObDMLBaseParam &dml_param,
                             blocksstable::ObDatumRow *&out_row,
                             bool use_fuse_row_cache = false);
  static int check_is_gencol_check_failed(const ObRelativeTable &data_table,
                                uint64_t error_col_id,
                                bool &is_virtual_gencol);
  static int lock_row_wrap(
      ObTabletHandle &tablet_handle,
      const blocksstable::ObDatumRow *data_row_for_lob,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const blocksstable::ObDatumRowkey &rowkey);
  static int lock_row_wrap(
      ObTabletHandle &tablet_handle,
      const blocksstable::ObDatumRow *data_row_for_lob,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      ObColDescArray &col_descs,
      blocksstable::ObDatumRow &row);
  static int update_row_wrap(
      ObTabletHandle &tablet_handle,
      const blocksstable::ObDatumRow *data_row_for_lob,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const ObIArray<share::schema::ObColDesc> &col_descs,
      const ObIArray<int64_t> &update_idx,
      const blocksstable::ObDatumRow &old_row,
      blocksstable::ObDatumRow &new_row,
      const ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr);
  static int update_rows_wrap(
      ObTabletHandle &tablet_handle,
      const blocksstable::ObDatumRow *data_row_for_lob,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const ObColDescIArray &col_descs,
      const ObIArray<int64_t> &update_idx,
      const blocksstable::ObDatumRow *old_rows,
      const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr,
      ObRowsInfo &rows_info);
  static int batch_calc_split_dst_rows(
      ObLS &ls,
      ObTabletHandle &tablet_handle,
      ObRelativeTable &relative_table,
      blocksstable::ObDatumRow *rows,
      const int64_t row_count,
      const int64_t abs_timeout_us,
      ObIArray<ObTabletID> &dst_tablet_ids,
      ObIArray<ObArray<int64_t>> &dst_row_ids);
  static int insert_rows_wrap(
      ObTabletHandle &tablet_handle,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const ObDMLBaseParam &dml_param,
      const bool check_exist,
      const ObColDescIArray &col_descs,
      ObRowsInfo &rows_info);
  static int insert_row_wrap(
      ObTabletHandle &tablet_handle,
      const blocksstable::ObDatumRow *data_row_for_lob,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const bool check_exists,
      const ObIArray<share::schema::ObColDesc> &col_descs,
      blocksstable::ObDatumRow &row,
      const ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr);
  static int check_row_locked_by_myself_wrap(
      ObTabletHandle &tablet_handle,
      const blocksstable::ObDatumRow *data_row_for_lob,
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const blocksstable::ObDatumRowkey &rowkey,
      bool &locked);
  static int table_refresh_row_wrap(
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow &row);
  static int check_old_row_legitimacy_wrap(
      const blocksstable::ObStoreCmpFuncs &cmp_funcs,
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      const int64_t row_count,
      const blocksstable::ObDatumRow *old_rows,
      int64_t &error_row_idx);

private:
  friend class ObLSTabletIterator;
  friend class ObLSTabletAddrIterator;
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
