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

#include "storage/meta_mem/ob_tablet_handle.h"
#include "common/ob_tablet_id.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_bucket_lock.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/palf/palf_callback.h"
#include "share/ob_rpc_struct.h"
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
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
namespace observer
{
class ObIMetaReport;
}
namespace obrpc
{
struct ObCreateTabletInfo;
struct ObBatchCreateTabletArg;
struct ObBatchRemoveTabletArg;
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

namespace transaction
{
struct ObMulSourceDataNotifyArg;
}

namespace storage
{
enum class ObDiskReportFileType : uint8_t;
class ObLS;
struct ObMetaDiskAddr;
class ObRowReshape;
class ObDMLRunningCtx;
class ObTableHandleV2;
class ObTableScanIterator;
class ObSingleRowGetter;
class ObLSTabletIterator;
class ObHALSTabletIDIterator;
class ObTabletMapKey;
struct ObStorageLogParam;
struct ObTabletCreateSSTableParam;
struct ObUpdateTableStoreParam;
class ObTabletTxMultiSourceDataUnit;
struct ObMigrationTabletParam;
class ObTableScanRange;


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
  int init(ObLS *ls, observer::ObIMetaReport *rs_reporter);
  int stop();
  void destroy();
  int offline();
  int online();
public:
  class AllowToReadMgr final
  {
  public:
    struct AllowToReadInfo final
    {
      AllowToReadInfo() { info_.seq_ = 0; info_.allow_to_read_ = 0; info_.reserved_ = 0; }
      ~AllowToReadInfo() = default;
      bool allow_to_read() const { return info_.allow_to_read_ == 1; }
      bool operator==(const AllowToReadInfo &other) const {
        return info_.seq_ == other.info_.seq_
            && info_.allow_to_read_ == other.info_.allow_to_read_
            && info_.reserved_ == other.info_.reserved_;
      }

      TO_STRING_KV(K_(info));
      static const int32_t RESERVED = 63;
      union InfoUnion
      {
        struct types::uint128_t v128_;
        struct
        {
          uint64_t seq_ : 64;
          uint8_t allow_to_read_ : 1;
          uint64_t reserved_ : RESERVED;
        };
        TO_STRING_KV(K_(seq), K_(allow_to_read), K_(reserved));
      };
      InfoUnion info_;
    } __attribute__((__aligned__(16)));
  public:
    AllowToReadMgr(): read_info_() {}
    ~AllowToReadMgr() = default;
    void disable_to_read();
    void enable_to_read();
    void load_allow_to_read_info(AllowToReadInfo &read_info);
    void check_read_info_same(const AllowToReadInfo &read_info, bool &is_same);
  private:
    AllowToReadInfo read_info_;
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
  int batch_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const share::SCN &create_scn,
      const bool is_replay_clog = false);
  int batch_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const bool is_replay_clog = false);

  int create_ls_inner_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t memstore_version,
      const share::SCN &frozen_timestamp,
      const share::schema::ObTableSchema &table_schema,
      const lib::Worker::CompatMode &compat_mode,
      const share::SCN &create_scn);
  int remove_ls_inner_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);

  // TODO: after this call back interfaces ready,
  // batch create/remove interfaces should be private
  int on_prepare_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_redo_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_commit_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_tx_end_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_abort_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_prepare_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_redo_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_commit_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_tx_end_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int on_abort_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);

  int get_tablet(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle,
      const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_TIMEOUT_US);
  int get_tablet_with_timeout(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle,
      const int64_t retry_timeout_us,
      const int64_t get_timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_TIMEOUT_US);
  int remove_tablets(const common::ObIArray<common::ObTabletID> &tablet_id_array);
  int get_ls_min_end_scn_in_old_tablets(share::SCN &end_scn);
  int get_tx_data_memtable_mgr(ObMemtableMgrHandle &mgr_handle);
  int get_tx_ctx_memtable_mgr(ObMemtableMgrHandle &mgr_handle);
  int get_lock_memtable_mgr(ObMemtableMgrHandle &mgr_handle);
  int get_bf_optimal_prefix(int64_t &prefix);
  int64_t get_tablet_count() const;

  // clog replay
  int replay_update_storage_schema(
      const share::SCN &scn,
      const char *buf,
      const int64_t buf_size,
      const int64_t pos);
  int replay_medium_compaction_clog(
      const share::SCN &scn,
      const char *buf,
      const int64_t buf_size,
      const int64_t pos);
  int replay_update_reserved_snapshot(
      const share::SCN &scn,
      const char *buf,
      const int64_t buf_size,
      const int64_t pos);

  // update tablet
  int update_tablet_table_store(
      const common::ObTabletID &tablet_id,
      const ObUpdateTableStoreParam &param,
      ObTabletHandle &handle);
  int update_medium_compaction_info(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle);
  int update_tablet_table_store( // only for small sstables defragmentation
      const ObTabletHandle &old_tablet_handle,
      const ObIArray<ObTableHandleV2> &table_handles);
  int update_tablet_report_status(const common::ObTabletID &tablet_id);
  int update_tablet_restore_status(
      const common::ObTabletID &tablet_id,
      const ObTabletRestoreStatus::STATUS &restore_status);
  int update_tablet_ha_data_status(
      const common::ObTabletID &tablet_id,
      const ObTabletDataStatus::STATUS &data_status);
  int update_tablet_ha_expected_status(
      const common::ObTabletID &tablet_id,
      const ObTabletExpectedStatus::STATUS &expected_status);
  int replay_create_tablet(
      const ObMetaDiskAddr &disk_addr,
      const char *buf,
      const int64_t buf_len,
      const ObTabletID &tablet_id);
  int do_remove_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);

  int create_memtable(
      const common::ObTabletID &tablet_id,
      const int64_t schema_version,
      const bool for_replay = false,
      const share::SCN clog_checkpoint_scn = share::SCN::min_scn());
  int get_read_tables(
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      ObTabletTableIterator &iter,
      const bool allow_no_ready_read = false);

  // DAS interface
  int table_scan(
      ObTableScanIterator &iter,
      ObTableScanParam &param);
  int table_rescan(
      ObTableScanParam &param,
      common::ObNewRowIterator *result);
  int insert_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int insert_row(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &duplicated_column_ids,
      const common::ObNewRow &row,
      const ObInsertFlag flag,
      int64_t &affected_rows,
      common::ObNewRowIterator *&duplicated_rows);
  int update_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      const ObIArray< uint64_t> &updated_column_ids,
      ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int put_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      ObNewRowIterator *row_iter,
      int64_t &affected_rows); // for htable, insert or update
  int delete_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const ObIArray<uint64_t> &column_ids,
      ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int lock_rows(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const int64_t abs_lock_timeout,
      const ObLockFlag lock_flag,
      const bool is_sfu,
      ObNewRowIterator *row_iter,
      int64_t &affected_rows);
  int lock_row(
      ObStoreCtx &ctx,
      const ObDMLBaseParam &dml_param,
      const int64_t abs_lock_timeout,
      const ObNewRow &row,
      const ObLockFlag lock_flag,
      const bool is_sfu);
  int get_multi_ranges_cost(
      const ObTabletID &tablet_id,
      const common::ObIArray<common::ObStoreRange> &ranges,
      int64_t &total_size);
  int split_multi_ranges(
      const ObTabletID &tablet_id,
      const ObIArray<ObStoreRange> &ranges,
      const int64_t expected_task_count,
      common::ObIAllocator &allocator,
      ObArrayArray<ObStoreRange> &multi_range_split_array);
  int estimate_row_count(
      const ObTableScanParam &param,
      const ObTableScanRange &scan_range,
      ObIArray<ObEstRowCountRecord> &est_records,
      int64_t &logical_row_count,
      int64_t &physical_row_count);
  int estimate_block_count(
      const common::ObTabletID &tablet_id,
      int64_t &macro_block_count,
      int64_t &micro_block_count);

  // iterator
  int build_tablet_iter(ObLSTabletIterator &iter);
  int build_tablet_iter(ObHALSTabletIDIterator &iter);

  // migration section
  int trim_rebuild_tablet(
      const ObTabletID &tablet_id,
      const bool is_rollback = false);
  int rebuild_create_tablet(
      const ObMigrationTabletParam &mig_tablet_param,
      const bool keep_old);
  int create_or_update_migration_tablet(
      const ObMigrationTabletParam &mig_tablet_param,
      const bool is_transfer);
  int create_migration_sstable(
      const blocksstable::ObMigrationSSTableParam &mig_sstable_param,
      const ObSSTableStatus status,
      ObTableHandleV2 &table_handle);
  int finish_copy_migration_sstable(const ObTabletID &tablet_id, const ObITable::TableKey &sstable_key);
  int build_ha_tablet_new_table_store(
      const ObTabletID &tablet_id,
      const ObBatchUpdateTableStoreParam &param);
  void enable_to_read();
  void disable_to_read();
  int get_all_tablet_ids(const bool except_ls_inner_tablet, common::ObIArray<ObTabletID> &tablet_id_array);

protected:
  virtual int prepare_dml_running_ctx(
      const common::ObIArray<uint64_t> *column_ids,
      const common::ObIArray<uint64_t> *upd_col_ids,
      ObTabletHandle &tablet_handle, // TODO: tablet handles is IN param, should be const
      ObDMLRunningCtx &run_ctx);
private:
  typedef ObSEArray<int64_t, 8> UpdateIndexArray;
  typedef common::hash::ObHashSet<common::ObTabletID, hash::NoPthreadDefendMode> NonLockedHashSet;
  struct DeleteTabletInfo
  {
  public:
    DeleteTabletInfo();
    ~DeleteTabletInfo() = default;
    DeleteTabletInfo(const DeleteTabletInfo &other);
    DeleteTabletInfo &operator=(const DeleteTabletInfo &other);

    void reset()
    {
      delete_data_tablet_ = false;
      old_data_tablet_handle_.reset();
      new_data_tablet_handle_.reset();
      delete_index_tablet_ids_.reset();
    }
  public:
    bool delete_data_tablet_;
    ObTabletHandle old_data_tablet_handle_;
    ObTabletHandle new_data_tablet_handle_;
    common::ObSArray<common::ObTabletID> delete_index_tablet_ids_;
  };
  struct HashMapTabletDeleteFunctor
  {
  public:
    HashMapTabletDeleteFunctor(ObLS *ls);
    ~HashMapTabletDeleteFunctor();
    void destroy();
  public:
    bool operator()(const common::ObTabletID &tablet_id, DeleteTabletInfo &info);
    common::ObIArray<ObStorageLogParam> &get_slog_params() { return slog_params_; }

    TO_STRING_KV(K_(slog_params));
  private:
    int handle_remove_data_tablet(
        ObTabletHandle &data_tablet_handle,
        const common::ObIArray<common::ObTabletID> &delete_index_tablet_ids);
  private:
    ObLS *ls_;
    common::ObSArray<ObStorageLogParam> slog_params_;
  };
  struct HashMapTabletGetFunctor
  {
  public:
    HashMapTabletGetFunctor(common::ObIArray<uint64_t> &tablet_id_hash_array) : tablet_id_hash_array_(tablet_id_hash_array) {}
  public:
    bool operator()(const common::ObTabletID &tablet_id, DeleteTabletInfo &info);
  private:
    common::ObIArray<uint64_t> &tablet_id_hash_array_;
  };
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
  class DestroyMemtableAndMemberOperator final
  {
  public:
    DestroyMemtableAndMemberOperator(ObLSTabletService *tablet_svr)
      : tablet_svr_(tablet_svr) {}
    ~DestroyMemtableAndMemberOperator() = default;
    int operator()(const common::ObTabletID &tablet_id);
    common::ObTabletID cur_tablet_id_;
    ObLSTabletService *tablet_svr_;
  };
private:
  void report_tablet_to_rs(const common::ObIArray<common::ObTabletID> &tablet_id_array);

  int handle_remove_tablets(
      const common::ObIArray<ObStorageLogParam> &slog_params,
      const common::ObLinearHashMap<common::ObTabletID, DeleteTabletInfo> &delete_tablet_infos);
  int check_and_get_tablet(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle,
      const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_TIMEOUT_US);
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
  int create_tablet(
      const share::ObLSID &ls_id,
      const obrpc::ObBatchCreateTabletArg &arg,
      const share::SCN &create_scn,
      const obrpc::ObCreateTabletInfo &info,
      common::ObIArray<ObTabletHandle> &tablet_handle_array,
      NonLockedHashSet &data_tablet_id_set);
  int do_create_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &data_tablet_id,
      const common::ObIArray<common::ObTabletID> &index_tablet_array,
      const share::SCN &create_scn,
      const share::SCN &snapshot_version,
      const share::schema::ObTableSchema &table_schema,
      const lib::Worker::CompatMode &compat_mode,
      const common::ObTabletID &lob_meta_tablet_id,
      const common::ObTabletID &lob_piece_tablet_id,
      ObTabletHandle &tablet_handle);
  int build_single_data_tablet(
      const share::ObLSID &ls_id,
      const obrpc::ObBatchCreateTabletArg &arg,
      const share::SCN &create_scn,
      const obrpc::ObCreateTabletInfo &info,
      common::ObIArray<ObTabletHandle> &tablet_handle_array);
  static int build_batch_create_tablet_arg(
      const obrpc::ObBatchCreateTabletArg &old_arg,
      const NonLockedHashSet &existed_tablet_id_set,
      obrpc::ObBatchCreateTabletArg &new_arg);
  int add_batch_tablets(
      const share::ObLSID &ls_id,
      const NonLockedHashSet &data_tablet_id_set,
      common::ObIArray<ObTabletHandle> &tablet_handle_array);
  int do_batch_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const share::SCN &create_scn,
      const bool is_replay_clog,
      common::ObTimeGuard &time_guard,
      NonLockedHashSet &data_tablet_id_set);
  int post_handle_batch_create_tablets(const obrpc::ObBatchCreateTabletArg &arg);
  int do_remove_lob_tablet(const ObTabletHandle &tablet);
  int set_disk_addr(const common::ObTabletID &tablet_id, const ObMetaDiskAddr &disk_addr);

  int refresh_tablet_addr(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMetaDiskAddr &new_addr,
      ObTabletHandle &tablet_handle);
  int update_tablet_object_and_addr(
      ObTabletHandle &new_tablet_handle,
      const ObMetaDiskAddr &new_addr);
  int create_lob_tablet(
      const ObMigrationTabletParam &mig_tablet_param);
  int create_lob_tablet(
    const ObTablet &data_tablet);
  int rollback_rebuild_tablet(const ObTabletID &tablet_id);
  int trim_old_tablets(const ObTabletID &tablet_id);
  int rebuild_tablet_with_old(
      const ObMigrationTabletParam &mig_tablet_param,
      ObMetaObjGuard<ObTablet> &tablet_guard);
  int migrate_update_tablet(const ObMigrationTabletParam &mig_tablet_param);
  int migrate_create_tablet(
      const ObMigrationTabletParam &mig_tablet_param,
      ObTabletHandle &handle);
  int delete_all_tablets();
  int choose_msd(
      const ObUpdateTableStoreParam &param,
      const ObTablet &old_tablet,
      const ObTabletTxMultiSourceDataUnit *&tx_data,
      const ObTabletBindingInfo *&binding_info,
      const share::ObTabletAutoincSeq *&auto_inc_seq);

  static int build_create_sstable_param_for_migration(
      const blocksstable::ObMigrationSSTableParam &migrate_sstable_param,
      ObTabletCreateSSTableParam &create_sstable_param);

  static int get_tablet_schema_index(
      const common::ObTabletID &tablet_id,
      const common::ObIArray<common::ObTabletID> &table_ids,
      int64_t &index);
  static int get_all_tablet_id_hash_array(
      const obrpc::ObBatchCreateTabletArg &arg,
      common::ObIArray<uint64_t> &all_tablet_id_hash_array);
  int parse_and_verify_delete_tablet_info(
      const obrpc::ObBatchRemoveTabletArg &arg,
      common::ObLinearHashMap<common::ObTabletID, DeleteTabletInfo> &delete_tablet_infos);
  int get_all_tablet_id_hash_array(
      common::ObLinearHashMap<common::ObTabletID, DeleteTabletInfo> &delete_tablet_infos,
      common::ObIArray<uint64_t> &all_tablet_id_hash_array);

  int verify_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      NonLockedHashSet &existed_tablet_id_set);
  int try_pin_tablet_if_needed(const ObTabletHandle &tablet_handle);
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
      ObTabletHandle &tablet_handle,
      ObDMLRunningCtx &run_ctx,
      ObStoreRow *rows,
      const int64_t row_count,
      ObRowsInfo &rows_info);
  static int insert_lob_col(
      ObDMLRunningCtx &run_ctx,
      const ObColDesc &column,
      ObObj &obj,
      ObLobAccessParam *del_param,
      ObLobCommon *lob_common);
  static int check_lob_tablet_valid(
      ObTabletHandle &data_tablet);
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

private:
  int direct_insert_rows(const uint64_t table_id,
                         const int64_t task_id,
                         const common::ObTabletID &tablet_id,
                         const bool is_heap_table,
                         common::ObNewRowIterator *row_iter,
                         int64_t &affected_rows);

private:
  friend class ObLSTabletIterator;

  ObLS *ls_;
  ObTxDataMemtableMgr tx_data_memtable_mgr_;
  ObTxCtxMemtableMgr tx_ctx_memtable_mgr_;
  ObLockMemtableMgr lock_memtable_mgr_;
  ObTabletIDSet tablet_id_set_;
  common::ObBucketLock bucket_lock_; // for tablet update, not for dml
  observer::ObIMetaReport *rs_reporter_;
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
