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

#ifndef OCEANBASE_STORAGE_TABLET_OB_TABLET
#define OCEANBASE_STORAGE_TABLET_OB_TABLET

#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "storage/ob_storage_schema.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_meta_pointer_map.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet_table_store_flag.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/meta_mem/ob_tablet_pointer.h"

namespace oceanbase
{
namespace common
{
class ObThreadCond;
}

namespace share
{
class ObLSID;
struct ObTabletAutoincInterval;

namespace schema
{
class ObTableSchema;
}
}

namespace logservice
{
class ObLogHandler;
}

namespace memtable
{
class ObIMemtable;
class ObIMultiSourceDataUnit;
}

namespace blocksstable
{
class ObSSTable;
}

namespace transaction
{
class ObTransID;
}

namespace storage
{
class ObStoreCtx;
class ObTableHandleV2;
class ObFreezer;
class ObTabletDDLInfo;
class ObTabletDDLKvMgr;
class ObDDLKVHandle;
class ObDDLKVsHandle;
class ObStorageSchema;
class ObTabletTableIterator;
class ObMetaDiskAddr;

class ObTablet
{
  friend class ObLSTabletService;
  friend class ObTabletCreateDeleteHelper;
  friend class ObTabletBindingHelper;
  friend class ObTabletPointer;
  friend class ObTabletStatusChecker;
public:
  typedef ObMetaPointerHandle<ObTabletMapKey, ObTablet> ObTabletPointerHandle;
  typedef common::ObFixedArray<share::schema::ObColDesc, common::ObIAllocator> ColDescArray;
public:
  ObTablet();
  ObTablet(const ObTablet&) = delete;
  ObTablet &operator=(const ObTablet&) = delete;
  ~ObTablet();
public:
  void reset();
  bool is_ls_inner_tablet() const;
  bool is_ls_tx_data_tablet() const;
  bool is_ls_tx_ctx_tablet() const;
  bool is_data_tablet() const;
  bool is_local_index_tablet() const;
  bool is_lob_meta_tablet() const;
  bool is_lob_piece_tablet() const;
  bool is_aux_tablet() const;
  void update_wash_score(const int64_t score);
  void inc_ref();
  int64_t dec_ref();
  int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
  int64_t get_wash_score() const { return ATOMIC_LOAD(&wash_score_); }
  int get_rec_log_ts(int64_t &rec_log_ts);
public:
  // first time create tablet
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &data_tablet_id,
      const common::ObTabletID &lob_meta_tablet_id,
      const common::ObTabletID &lob_piece_tablet_id,
      const int64_t create_scn,
      const int64_t snapshot_version,
      const share::schema::ObTableSchema &table_schema,
      const lib::Worker::CompatMode compat_mode,
      const ObTabletTableStoreFlag &store_flag,
      ObTableHandleV2 &table_handle,
      ObFreezer *freezer);
  // dump/merge build new multi version tablet
  int init(
      const ObUpdateTableStoreParam &param,
      const ObTablet &old_tablet,
      const ObTabletTxMultiSourceDataUnit &tx_data,
      const ObTabletBindingInfo &ddl_data,
      const share::ObTabletAutoincSeq &autoinc_seq);
  // transfer build new tablet
  int init(
      const ObMigrationTabletParam &param,
      const bool is_update,
      ObFreezer *freezer);
  //batch update table store with range cut
  int init(
      const ObBatchUpdateTableStoreParam &param,
      const ObTablet &old_tablet,
      const ObTabletTxMultiSourceDataUnit &tx_data,
      const ObTabletBindingInfo &ddl_data,
      const share::ObTabletAutoincSeq &autoinc_seq);

  bool is_valid() const;

  // serialize & deserialize
  int serialize(char *buf, const int64_t len, int64_t &pos);
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int load_deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int deserialize_post_work();
  int dec_macro_disk_ref();
  int inc_macro_disk_ref();
  int64_t get_serialize_size() const;
  ObMetaObjGuard<ObTablet> &get_next_tablet_guard() { return next_tablet_guard_; }
  void set_next_tablet_guard(const ObMetaObjGuard<ObTablet> &next_tablet_guard);
  void trim_tablet_list();

  // dml operation
  int insert_row(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const ObColDescIArray &col_descs,
      const ObStoreRow &row);
  int insert_row_without_rowkey_check(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const ObColDescIArray &col_descs,
      const storage::ObStoreRow &row);
  int update_row(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const ObColDescIArray &col_descs,
      const ObIArray<int64_t> &update_idx,
      const storage::ObStoreRow &old_row,
      const storage::ObStoreRow &new_row);
  int lock_row(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const common::ObNewRow &row);
  int lock_row(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const blocksstable::ObDatumRowkey &rowkey);

  // table operation
  int get_read_tables(
      const int64_t snapshot_version,
      ObTabletTableIterator &iter,
      const bool allow_no_ready_read);
  int get_read_major_sstable(
      const int64_t &major_snapshot_version,
      ObTabletTableIterator &iter);
  int get_all_sstables(common::ObIArray<ObITable *> &sstables) const;
  int get_sstables_size(int64_t &used_size) const;
  int get_memtables(common::ObIArray<storage::ObITable *> &memtables, const bool need_active = false) const;
  int check_need_remove_old_table(const int64_t multi_version_start, bool &need_remove) const;
  int update_upper_trans_version(ObLS &ls, bool &is_updated);

  // memtable operation
  ObIMemtableMgr *get_memtable_mgr() const { return memtable_mgr_; } // TODO(bowen.gbw): get memtable mgr from tablet pointer handle
  // get the active memtable for write or replay.
  int get_active_memtable(ObTableHandleV2 &handle) const;
  int release_memtables(const int64_t log_ts);
  // force release all memtables
  // just for rebuild or migrate retry.
  int release_memtables();
  int reset_storage_related_member();

  // multi-source data operation
  int check_tx_data(bool &is_valid) const;
  int get_tx_data(ObTabletTxMultiSourceDataUnit &tx_data, const bool check_valid = true) const;
  int get_ddl_data(ObTabletBindingInfo &ddl_data) const;
  int get_tablet_status(ObTabletStatus::Status &tablet_status);

  template<class T>
  int prepare_data(T &multi_source_data_unit, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  template<class T>
  int clear_unsynced_cnt_for_tx_end_if_need(
      T &multi_source_data_unit,
      const int64_t log_ts,
      const bool for_replay);
  template<class T>
  int set_multi_data_for_commit(T &multi_source_data_unit, const int64_t log_ts, const bool for_replay, const memtable::MemtableRefOp ref_op);

  template<class T>
  int save_multi_source_data_unit(
      const T *const msd,
      const int64_t memtable_log_ts,
      const bool for_replay,
      const memtable::MemtableRefOp ref_op = memtable::MemtableRefOp::NONE,
      const bool is_callback = false);

  template<class T>
  int set_log_ts(
      T *const multi_source_data_unit,
      const int64_t log_ts);

  // static help function
  static int deserialize_id(
      const char *buf,
      const int64_t len,
      share::ObLSID &ls_id,
      common::ObTabletID &tablet_id);
  static int64_t get_lock_wait_timeout(
      const int64_t abs_lock_timeout,
      const int64_t stmt_timeout);
  int rowkey_exists(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const common::ObNewRow &row,
      bool &exists);
  int rowkeys_exists(
      ObStoreCtx &store_ctx,
      ObRelativeTable &relative_table,
      ObRowsInfo &rows_info,
      bool &exists);
  static int prepare_memtable(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      memtable::ObMemtable *&write_memtable);

  // migration section
  // used for migration source generating create tablet rpc argument
  int build_migration_tablet_param(ObMigrationTabletParam &mig_tablet_param) const;
  int build_migration_sstable_param(
      const ObITable::TableKey &table_key,
      blocksstable::ObMigrationSSTableParam &mig_sstable_param) const;
  int get_ha_tables(
      ObTableStoreIterator &iter,
      bool &is_ready_for_read);
  int get_ha_sstable_size(int64_t &data_size);

  // for restore
  // check whether we have dumped a sstable or not.
  int check_has_sstable(bool &has_sstable) const;

  // ddl kv
  int get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle, bool try_create = false);
  int set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int start_ddl_if_need();
  int get_ddl_sstable_handles(ObTablesHandleArray &ddl_sstable_handles);
  int get_migration_sstable_size(int64_t &data_size);

  // other
  const ObTabletMeta &get_tablet_meta() const { return tablet_meta_; }
  const ObTabletTableStore &get_table_store() const { return table_store_; }
  int64_t get_clog_checkpoint_ts() const { return tablet_meta_.clog_checkpoint_ts_; }
  int64_t get_snapshot_version() const { return tablet_meta_.snapshot_version_; }
  int64_t get_multi_version_start() const { return tablet_meta_.multi_version_start_; }

  // deprecated later, DO NOT use it!
  ObTabletTableStore &get_table_store() { return table_store_; }

  const ObStorageSchema &get_storage_schema() const { return storage_schema_; }

  const ObTableReadInfo &get_full_read_info() const { return full_read_info_; }
  const ObTableReadInfo &get_index_read_info() const { return *full_read_info_.get_index_read_info(); }
  const ObTabletPointerHandle &get_pointer_handle() { return pointer_hdl_; }
  const compaction::ObMediumCompactionInfoList &get_medium_compaction_info_list() const { return medium_info_list_; }

  int get_meta_disk_addr(ObMetaDiskAddr &addr) const;

  int assign_pointer_handle(const ObTabletPointerHandle &ptr_hdl);

  int replay_update_storage_schema(
      const int64_t log_ts,
      const char *buf,
      const int64_t buf_size,
      int64_t &pos);
  int get_schema_version_from_storage_schema(int64_t &schema_version);
  int fetch_tablet_autoinc_seq_cache(
      const uint64_t cache_size,
      share::ObTabletAutoincInterval &result);

  int get_latest_autoinc_seq(share::ObTabletAutoincSeq &autoinc_seq) const;
  int update_tablet_autoinc_seq(
      const uint64_t autoinc_seq,
      const int64_t replay_log_ts);

  int get_kept_multi_version_start(
      int64_t &multi_version_start,
      int64_t &min_reserved_snapshot);

  int check_schema_version_elapsed(
      const int64_t schema_version,
      const bool need_wait_trans_end,
      int64_t &max_commit_version,
      transaction::ObTransID &pending_tx_id);
  int replay_schema_version_change_log(const int64_t schema_version);
  int get_tablet_report_info(
      const int64_t snapshot_version,
      common::ObIArray<int64_t> &column_checksums,
      int64_t &data_size,
      int64_t &required_size,
      const bool need_checksums = true);
  int set_tx_data(
      const ObTabletTxMultiSourceDataUnit &tx_data,
      const int64_t memtable_log_ts,
      const bool for_replay,
      const memtable::MemtableRefOp ref_op = memtable::MemtableRefOp::NONE,
      const bool is_callback = false);
  int set_tx_data_in_tablet_pointer();
  int set_memtable_clog_checkpoint_ts(
      const ObMigrationTabletParam *tablet_meta);
  int remove_memtables_from_data_checkpoint();

  TO_STRING_KV(KP(this), K_(wash_score), K_(ref_cnt), K_(tablet_meta), K_(table_store), K_(storage_schema));
private:
  int64_t get_self_size() const;
  int get_memtable_mgr(ObIMemtableMgr *&memtable_mgr) const;

  logservice::ObLogHandler *get_log_handler() const { return log_handler_; } // TODO(bowen.gbw): get log handler from tablet pointer handle
  common::ObThreadCond &get_cond();
  common::TCRWLock &get_rw_lock();

  int init_shared_params(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t max_saved_schema_version,
      ObFreezer *freezer);
  int build_read_info(common::ObIAllocator &allocator);
  int create_memtable(const int64_t schema_version, const int64_t clog_checkpoint_ts, const bool for_replay=false);
  int try_update_start_scn();
  int try_update_ddl_checkpoint_ts();
  int try_update_table_store_flag(const ObUpdateTableStoreParam &param);
  int try_update_storage_schema(
      const int64_t table_id,
      const int64_t schema_version,
      ObIAllocator &allocator,
      const int64_t timeout_ts);
  int get_max_schema_version(int64_t &schema_version);
  int inner_get_all_sstables(common::ObIArray<ObITable *> &sstables) const;
  int pre_transform_sstable_root_block(const ObTableReadInfo &index_read_info);
  int choose_and_save_storage_schema(
      common::ObIAllocator &allocator,
      const ObStorageSchema &tablet_schema,
      const ObStorageSchema &param_schema);
  int check_schema_version_for_bounded_staleness_read(
      const int64_t table_version_for_read,
      const int64_t data_max_schema_version,
      const uint64_t table_id);

  int do_rowkey_exists(
      ObStoreCtx &store_ctx,
      const int64_t table_id,
      const blocksstable::ObDatumRowkey &rowkey,
      const common::ObQueryFlag &query_flag,
      bool &exists);
  static int do_rowkeys_exist(
      ObTableStoreIterator &tables_iter,
      ObRowsInfo &rows_info,
      bool &exists);

  // used for freeze_tablet
  int inner_create_memtable(
      const int64_t clog_checkpoint_ts = 1,/*1 for first memtable, filled later*/
      const int64_t schema_version = 0/*0 for first memtable*/,
      const bool for_replay=false);

  int write_sync_tablet_seq_log(share::ObTabletAutoincSeq &autoinc_seq,
                                const uint64_t new_autoinc_seq,
                                int64_t &log_ts);
  int update_ddl_info(
      const int64_t schema_version,
      const int64_t log_ts,
      int64_t &schema_refreshed_ts);
  int write_tablet_schema_version_change_clog(
      const int64_t schema_version,
      int64_t &log_ts);
  int get_ddl_info(
      int64_t &refreshed_schema_version,
      int64_t &refreshed_schema_ts) const;
  int get_read_tables(
      const int64_t snapshot_version,
      ObTableStoreIterator &iter,
      const bool allow_no_ready_read);
  int get_read_major_sstable(
      const int64_t &major_snapshot_version,
      ObTableStoreIterator &iter);
  int allow_to_read_();

  // multi-source data
  int inner_get_tx_data(ObTabletTxMultiSourceDataUnit &tx_data, bool &exist_on_memtable) const;
  int set_tx_id(
      const transaction::ObTransID &tx_id,
      const bool for_replay);
  int set_tx_log_ts(
      const transaction::ObTransID &tx_id,
      const int64_t log_ts,
      const bool for_replay);
  int set_tablet_final_status(
      ObTabletTxMultiSourceDataUnit &tx_data,
      const int64_t memtable_log_ts,
      const bool for_replay,
      const memtable::MemtableRefOp ref_op);
  int set_tx_data(
      const ObTabletTxMultiSourceDataUnit &tx_data,
      const bool for_replay,
      const memtable::MemtableRefOp ref_op = memtable::MemtableRefOp::NONE,
      const bool is_callback = false);
  int get_msd_from_memtable(memtable::ObIMultiSourceDataUnit &msd) const;
  int set_tx_data_in_tablet_pointer(const ObTabletTxMultiSourceDataUnit &tx_data);
  int get_max_sync_storage_schema_version(int64_t &max_schema_version) const;
  int check_max_sync_schema_version() const;
  int check_sstable_column_checksum() const;

  template<class T>
  int dec_unsynced_cnt_for_if_need(
      T &multi_source_data_unit,
      const bool for_replay,
      const memtable::MemtableRefOp ref_op);
  template<class T>
  int inner_set_multi_data_for_commit(
      T &multi_source_data_unit,
      const int64_t log_ts,
      const bool for_replay,
      const memtable::MemtableRefOp ref_op);

private:
  static const int32_t TABLET_VERSION = 1;
private:
  int32_t version_;
  int32_t length_;
  volatile int64_t wash_score_ CACHE_ALIGNED;
  volatile int64_t ref_cnt_ CACHE_ALIGNED;
  ObTabletPointerHandle pointer_hdl_;
  ObTabletMeta tablet_meta_;
  ObTabletTableStore table_store_;
  ObStorageSchema storage_schema_;
  compaction::ObMediumCompactionInfoList medium_info_list_;

  // NOTICE: these two pointers: memtable_mgr_ and log_handler_,
  // are considered as cache for tablet.
  // we keep it on tablet because we cannot get them in ObTablet::deserialize
  // through ObTabletPointerHandle.
  // may be some day will fix this issue, then the pointers have no need to exist.

  ObIMemtableMgr *memtable_mgr_;
  logservice::ObLogHandler *log_handler_;

  mutable common::TCRWLock table_store_lock_; // protect table store memtable cache read and update

  ObTableReadInfo full_read_info_;
  common::ObIAllocator *allocator_;
  ObMetaObjGuard<ObTablet> next_tablet_guard_;

  //ATTENTION : Add a new variable need consider ObMigrationTabletParam
  // and tablet meta init interface for migration.
  // yuque : https://yuque.antfin.com/ob/ob-backup/zzwpuh

  bool is_inited_;
};

inline bool ObTablet::is_ls_inner_tablet() const
{
  return tablet_meta_.tablet_id_.is_ls_inner_tablet();
}

inline bool ObTablet::is_ls_tx_data_tablet() const
{
  return tablet_meta_.tablet_id_.is_ls_tx_data_tablet();
}

inline bool ObTablet::is_ls_tx_ctx_tablet() const
{
  return tablet_meta_.tablet_id_.is_ls_tx_ctx_tablet();
}

inline bool ObTablet::is_valid() const
{
  return pointer_hdl_.is_valid()
      && tablet_meta_.is_valid()
      && table_store_.is_valid()
      && storage_schema_.is_valid();
}

inline bool ObTablet::is_data_tablet() const
{
  return is_valid()
      && (tablet_meta_.tablet_id_ == tablet_meta_.data_tablet_id_);
}

inline bool ObTablet::is_local_index_tablet() const
{
  return is_valid()
      && (tablet_meta_.tablet_id_ != tablet_meta_.data_tablet_id_)
      && !(tablet_meta_.ddl_data_.lob_meta_tablet_id_.is_valid())
      && !(tablet_meta_.ddl_data_.lob_piece_tablet_id_.is_valid());
}

inline bool ObTablet::is_lob_meta_tablet() const
{
  return is_valid()
      && (tablet_meta_.tablet_id_ == tablet_meta_.ddl_data_.lob_meta_tablet_id_);
}

inline bool ObTablet::is_lob_piece_tablet() const
{
  return is_valid()
      && (tablet_meta_.tablet_id_ == tablet_meta_.ddl_data_.lob_piece_tablet_id_);
}

inline bool ObTablet::is_aux_tablet() const
{
  return is_valid()
      && (is_local_index_tablet() || is_lob_meta_tablet() || is_lob_piece_tablet());
}

inline void ObTablet::update_wash_score(const int64_t score)
{
  int64_t ret_v = 0;
  int64_t old_v = ATOMIC_LOAD(&wash_score_);
  if (score > old_v) {
    while (old_v != (ret_v = ATOMIC_CAS(&wash_score_, old_v, score))) {
      if (ret_v >= score) {
        break; // higher score may be updated by others, so just skip.
      } else {
        old_v = ret_v;
      }
    }
  }
}

inline void ObTablet::inc_ref()
{
  int64_t cnt = ATOMIC_AAF(&ref_cnt_, 1);
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  STORAGE_LOG(DEBUG, "tablet inc ref", KP(this), K(tablet_id), "ref_cnt", cnt, K(lbt()));
}

inline int64_t ObTablet::dec_ref()
{
  int64_t cnt = ATOMIC_SAF(&ref_cnt_, 1/* just sub 1 */);
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  STORAGE_LOG(DEBUG, "tablet dec ref", KP(this), K(tablet_id), "ref_cnt", cnt, K(lbt()));

  return cnt;
}

inline int64_t ObTablet::get_lock_wait_timeout(
    const int64_t abs_lock_timeout,
    const int64_t stmt_timeout)
{
  return (abs_lock_timeout < 0 ? stmt_timeout :
          (abs_lock_timeout > stmt_timeout ? stmt_timeout : abs_lock_timeout));
}

template<class T>
int ObTablet::prepare_data(T &multi_source_data_unit, const transaction::ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const int64_t log_ts = trans_flags.for_replay_ ? trans_flags.log_ts_ : INT64_MAX;
  TRANS_LOG(INFO, "prepare data when tx_end", K(multi_source_data_unit), K(tablet_meta_.tablet_id_));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!multi_source_data_unit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(multi_source_data_unit));
  } else if (OB_UNLIKELY(multi_source_data_unit.is_tx_end())) {
    TRANS_LOG(INFO, "skip for is_tx_end is true", K(multi_source_data_unit));
  } else if (FALSE_IT(multi_source_data_unit.set_tx_end(true))) {
  } else if (OB_FAIL(save_multi_source_data_unit(&multi_source_data_unit, log_ts, trans_flags.for_replay_/*for_replay*/, memtable::MemtableRefOp::INC_REF))) {
    TRANS_LOG(WARN, "failed to save multi_source_data", K(ret), K(multi_source_data_unit), K(log_ts));
  }

  return ret;
}

template<class T>
int ObTablet::set_multi_data_for_commit(
    T &multi_source_data_unit,
    const int64_t log_ts,
    const bool for_replay,
    const memtable::MemtableRefOp ref_op)
{
  int ret = OB_SUCCESS;

  bool is_callback = true;
  TRANS_LOG(INFO, "set_multi_data_for_commit", K(multi_source_data_unit), K(ref_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!multi_source_data_unit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(multi_source_data_unit));
  } else if (!multi_source_data_unit.is_tx_end()) {
    if (OB_FAIL(save_multi_source_data_unit(&multi_source_data_unit, log_ts, for_replay, ref_op, is_callback))) {
      TRANS_LOG(WARN, "failed to save tx data", K(ret), K(multi_source_data_unit), K(for_replay), K(ref_op));
    }
  } else if (OB_FAIL(inner_set_multi_data_for_commit(multi_source_data_unit, log_ts, for_replay, ref_op))) {
    TRANS_LOG(WARN, "failed to back_fill_log_ts_for_ddl_data", K(ret), K(multi_source_data_unit));
  }
  return ret;
}

template<class T>
int ObTablet::dec_unsynced_cnt_for_if_need(
    T &multi_source_data_unit,
    const bool for_replay,
    const memtable::MemtableRefOp ref_op)
{
  int ret = OB_SUCCESS;
  const int64_t log_ts = INT64_MAX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!multi_source_data_unit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(multi_source_data_unit));
  } else if (memtable::MultiSourceDataUnitType::TABLET_TX_DATA == multi_source_data_unit.type()) {
    ObTabletTxMultiSourceDataUnit tx_data;
    if (OB_FAIL(get_tx_data(tx_data))) {
      TRANS_LOG(WARN, "failed to get tx data", K(ret));
    } else if (OB_FAIL(save_multi_source_data_unit(&tx_data, log_ts, for_replay, ref_op, true/*is_callback*/))) {
      TRANS_LOG(WARN, "failed to save tx data", K(ret), K(tx_data), K(log_ts));
    }
  } else if (memtable::MultiSourceDataUnitType::TABLET_BINDING_INFO == multi_source_data_unit.type()) {
    ObTabletBindingInfo binding_info;
    if (OB_FAIL(get_ddl_data(binding_info))) {
      TRANS_LOG(WARN, "failed to get binding info", K(ret));
    } else if (OB_FAIL(save_multi_source_data_unit(&binding_info, log_ts, for_replay, ref_op, true/*is_callback*/))) {
      TRANS_LOG(WARN, "failed to binding info", K(ret), K(binding_info), K(log_ts));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "multi-data type not support", K(ret), K(multi_source_data_unit));
  }
  return ret;
}

template<class T>
int ObTablet::inner_set_multi_data_for_commit(
    T &multi_source_data_unit,
    const int64_t log_ts,
    const bool for_replay,
    const memtable::MemtableRefOp ref_op)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!multi_source_data_unit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(multi_source_data_unit));
  } else if (OB_FAIL(dec_unsynced_cnt_for_if_need(multi_source_data_unit, for_replay, ref_op))) {
    TRANS_LOG(WARN, "failed to dec_unsynced_cnt_for_ddl_data_if_need", K(ret), K(multi_source_data_unit), K(for_replay), K(ref_op));
  } else if (memtable::MultiSourceDataUnitType::TABLET_TX_DATA == multi_source_data_unit.type()) {
    ObTabletTxMultiSourceDataUnit tx_data;
    if (OB_FAIL(get_tx_data(tx_data))) {
      TRANS_LOG(WARN, "failed to get tx data", K(ret));
    } else if (OB_FAIL(tx_data.deep_copy(&multi_source_data_unit))) {
      TRANS_LOG(WARN, "fail to deep_copy", K(ret), K(multi_source_data_unit), K(tx_data), K(get_tablet_meta()));
    } else if (OB_FAIL(clear_unsynced_cnt_for_tx_end_if_need(tx_data, log_ts, for_replay))) {
      TRANS_LOG(WARN, "failed to save tx data", K(ret), K(tx_data), K(log_ts));
    }
  } else if (memtable::MultiSourceDataUnitType::TABLET_BINDING_INFO == multi_source_data_unit.type()) {
    ObTabletBindingInfo binding_info;
    if (OB_FAIL(get_ddl_data(binding_info))) {
      TRANS_LOG(WARN, "failed to get binding info", K(ret));

    } else if (OB_FAIL(binding_info.deep_copy(&multi_source_data_unit))) {
      TRANS_LOG(WARN, "fail to deep_copy", K(ret), K(multi_source_data_unit), K(binding_info), K(get_tablet_meta()));
    } else if (OB_FAIL(clear_unsynced_cnt_for_tx_end_if_need(binding_info, log_ts, for_replay))) {
      TRANS_LOG(WARN, "failed to save ddl data", K(ret), K(multi_source_data_unit), K(log_ts));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "multi-data type not support", K(ret), K(multi_source_data_unit));
  }

  return ret;
}


template<class T>
int ObTablet::clear_unsynced_cnt_for_tx_end_if_need(
    T &multi_source_data_unit,
    const int64_t log_ts,
    const bool for_replay)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!multi_source_data_unit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(multi_source_data_unit));
  } else if (OB_UNLIKELY(!multi_source_data_unit.is_tx_end())) {
  } else if (FALSE_IT(multi_source_data_unit.set_tx_end(false))) {
  } else if (OB_FAIL(save_multi_source_data_unit(&multi_source_data_unit, log_ts, for_replay, memtable::MemtableRefOp::DEC_REF, true/*is_callback*/))) {
    TRANS_LOG(WARN, "failed to save tx data", K(ret), K(multi_source_data_unit), K(log_ts));
  }
  return ret;
}

template<class T>
int ObTablet::save_multi_source_data_unit(
    const T *const msd,
    const int64_t memtable_log_ts,
    const bool for_replay,
    const memtable::MemtableRefOp ref_op,
    const bool is_callback)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret), K_(is_inited), K(tablet_id));
  } else if (OB_ISNULL(msd)) {
    ret = common::OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), KP(msd));
  } else if (OB_UNLIKELY(is_ls_inner_tablet())) {
    ret = common::OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ls inner tablet does not support multi source data", K(ret), K(tablet_id));
  } else if (is_callback) {
    ObTableHandleV2 handle;
    memtable::ObMemtable *memtable = nullptr;
    if (OB_FAIL(memtable_mgr_->get_memtable_for_multi_source_data_unit(handle, msd->type()))) {
      if (OB_ENTRY_NOT_EXIST == ret && for_replay) {
        TRANS_LOG(INFO, "clog_checkpoint_ts of ls is bigger than the commit_info log_ts of this multi-trans in replay, failed to get multi source data unit",
                  K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts));
        if (OB_FAIL(save_multi_source_data_unit(msd, memtable_log_ts, for_replay, ref_op, false))) {
          TRANS_LOG(WARN, "failed to save multi source data unit", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts), K(ref_op));
        }
      } else {
        TRANS_LOG(WARN, "failed to get multi source data unit", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts));
      }
    } else if (OB_FAIL(handle.get_data_memtable(memtable))) {
      TRANS_LOG(WARN, "fail to get memtable", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(memtable->save_multi_source_data_unit(msd, memtable_log_ts, for_replay, ref_op, is_callback))) {
      TRANS_LOG(WARN, "failed to save multi source data unit", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts), K(ref_op));
    }
  }
  // for tx_end(inf_ref), binding_info must be prepared after tablet_state is prepared
  else if (memtable::MemtableRefOp::INC_REF == ref_op
           && memtable::MultiSourceDataUnitType::TABLET_BINDING_INFO == msd->type()) {
    ObTableHandleV2 handle;
    memtable::ObMemtable *memtable = nullptr;
    if (OB_FAIL(memtable_mgr_->get_memtable_for_multi_source_data_unit(handle, memtable::MultiSourceDataUnitType::TABLET_TX_DATA))) {
      TRANS_LOG(WARN, "failed to get multi source data unit", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts));
    } else if (OB_FAIL(handle.get_data_memtable(memtable))) {
      TRANS_LOG(WARN, "[Freezer] fail to get memtable", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(memtable->save_multi_source_data_unit(msd, memtable_log_ts, for_replay, ref_op/*add_ref*/, is_callback/*false*/))) {
      TRANS_LOG(WARN, "failed to save multi source data unit", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts), K(ref_op));
    }
  } else {
    memtable::ObIMemtable *i_memtable = nullptr;
    ObStoreCtx mock_store_ctx;
    mock_store_ctx.log_ts_ = memtable_log_ts;
    mock_store_ctx.tablet_id_ = tablet_meta_.tablet_id_;

    ObStorageTableGuard guard(this, mock_store_ctx, true, for_replay, memtable_log_ts, true/*for_multi_source_data*/);
    if (OB_FAIL(guard.refresh_and_protect_memtable())) {
      TRANS_LOG(WARN, "failed to refresh table", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts), K(for_replay));
    } else if (OB_FAIL(guard.get_memtable_for_replay(i_memtable))) {
      if (OB_NO_NEED_UPDATE == ret) {
        TRANS_LOG(INFO, "no need to replay log", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts), K(for_replay));
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(WARN, "failed to get memtable", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts), K(for_replay));
      }
    } else {
      memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(i_memtable);
      const int64_t start = ObTimeUtility::current_time();
      while (OB_SUCC(ret) &&
             memtable->get_logging_blocked() &&
             ObLogTsRange::MAX_TS == memtable_log_ts) {
        if (ObTimeUtility::current_time() - start > 100 * 1000) {
          ret = OB_BLOCK_FROZEN;
          TRANS_LOG(WARN, "logging_block costs too much time", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts), K(ref_op), K(for_replay));
        }
        ob_usleep(100);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(memtable->save_multi_source_data_unit(msd, memtable_log_ts, for_replay, ref_op, is_callback))) {
        TRANS_LOG(WARN, "failed to save multi source data unit", K(ret), K(ls_id), K(tablet_id), K(memtable_log_ts), K(ref_op), K(for_replay));
      }
    }
  }
  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO, "succeed to save multi source data unit", K(ls_id), K(tablet_id), K(memtable_log_ts), K(for_replay), KPC(msd));
  } else if (for_replay) {
    if (OB_ALLOCATE_MEMORY_FAILED == ret || OB_MINOR_FREEZE_NOT_ALLOW == ret) {
      ret = OB_EAGAIN;
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLET_OB_TABLET
