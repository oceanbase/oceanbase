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

#ifndef OCEANBASE_STORAGE_DDL_OB_DIRECT_INSERT_SSTABLE_CTX_NEW_H
#define OCEANBASE_STORAGE_DDL_OB_DIRECT_INSERT_SSTABLE_CTX_NEW_H

#include "storage/meta_mem/ob_tablet_handle.h"
#include "lib/lock/ob_mutex.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_bucket_lock.h"
#include "common/ob_tablet_id.h"
#include "common/row/ob_row_iterator.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/scn.h"
#include "storage/ob_i_table.h"
#include "storage/ob_row_reshape.h"
#include "storage/blocksstable/ob_imacro_block_flush_callback.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "src/share/ob_ddl_common.h"

namespace oceanbase
{
namespace sql
{
class ObPxMultiPartSSTableInsertOp;
class ObExecContext;
class ObDDLCtrl;
}

namespace blocksstable
{
class ObIMacroBlockFlushCallback;
class ObMacroBlockWriter;
}

namespace share
{
struct ObTabletCacheInterval;
}

namespace storage
{
class ObTablet;
class ObLobMetaRowIterator;
class ObTabletDirectLoadMgrHandle;
class ObTabletDirectLoadMgr;
class ObTabletFullDirectLoadMgr;
class ObTabletIncDirectLoadMgr;
struct ObInsertMonitor;

class ObTenantDirectLoadMgr final
{
public:
  ObTenantDirectLoadMgr();
  ~ObTenantDirectLoadMgr();
  void destroy();
  static int mtl_init(
      ObTenantDirectLoadMgr *&tenant_direct_load_mgr);
  int init();

  int alloc_execution_context_id(int64_t &context_id);

  // create tablet direct lob manager for data tablet, and
  // create lob meta tablet manager inner on need.
  // Actually,
  // 1. lob meta direct load mgr will be created when creating data tablet direct load mgr.
  // 2. lob meta direct load mgr will be created by itself when it is recovered from checkpoint.
  // @param [in] param, to init or update tablet direct load mgr.
  // @param [in] checkpoint_scn, to decide when to create the lob meta tablet direct load mgr.
  int create_tablet_direct_load(
      const int64_t context_id,
      const int64_t execution_id,
      const ObTabletDirectLoadInsertParam &param,
      const share::SCN checkpoint_scn = share::SCN::min_scn(),
      const bool only_persisted_ddl_data = false);

  int replay_create_tablet_direct_load(
      const ObTabletHandle &tablet_handle,
      const int64_t execution_id,
      const ObTabletDirectLoadInsertParam &param);

  // to start the direct load, write start log in actually.
  // @param [in] is_full_direct_load.
  // @param [in] ls_id.
  // @param [in] tablet_id, the commit version for the full direct load,
  int open_tablet_direct_load(
      const bool is_full_direct_load,
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t context_id,
      share::SCN &start_scn,
      ObTabletDirectLoadMgrHandle &handle);

  // create sstable slice writer for direct load.
  // @param [in] slice_info.is_full_direct_load_.
  // @param [in] slice_info.is_lob_tablet_slice_, to decide create slice writer for data tablet or lob meta tablet.
  // @param [in] slice_info.tablet_id_, is always the data tablet id rather than lob meta tablet id.
  // @param [in] start_seq, start sequence of macro block, decide the logical id.
  // @param [out] slice_info.slice_id, to identify the created slice writer.
  int open_sstable_slice(
      const blocksstable::ObMacroDataSeq &start_seq,
      ObDirectLoadSliceInfo &slice_info);

  // fill data row into macro block directly for data tablet.
  int fill_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows,
      ObInsertMonitor *insert_monitor = NULL);

  // fill lob meta data into macro block directly.
  // @param [in] slice_info, contains is_full_direct_load, data_tablet_id, lob slice id.
  // @param [in] cs_type, collation type of the lob column.
  // @param [in] lob_id.
  // @param [out] datum, to fill the lob column in the data row.
  int fill_lob_sstable_slice(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      share::ObTabletCacheInterval &pk_interval,
      const ObArray<int64_t> &lob_column_idxs,
      const ObArray<common::ObObjMeta> &col_types,
      blocksstable::ObDatumRow &datum_row);
  // flush macro block, close and destroy slice writer.
  int close_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info,
      ObInsertMonitor *insert_monitor,
      blocksstable::ObMacroDataSeq &next_seq);

  // end direct load due to commit or abort.
  // @param [in] is_full_direct_load.
  // @param [in] tablet_id.
  // @param [in] need_commit, to decide whether to create sstable.
  //             need_commit = true when commit, and need_commit = false when abort.
  // @param [in] emergent_finish, to decide whether to create sstable immediately or later(batch create).
  // @param [in] task_id, table_id, execution_id, for ddl report checksum.
  int close_tablet_direct_load(
      const int64_t context_id,
      const bool is_full_direct_load,
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool need_commit,
      const bool emergent_finish = true,
      const int64_t task_id = 0,
      const int64_t table_id = common::OB_INVALID_ID,
      const int64_t execution_id = -1);

  // some utils functions below.
  // to get online stats result,
  // and to avoid empty result, the caller should set need_online_opt_stat_gather_ when create tablet manager.
  int get_online_stat_collect_result(
      const bool is_full_direct_load,
      const ObTabletID &tablet_id,
      const ObArray<ObOptColumnStat*> *&column_stat_array);
  // fetch hidden pk value, for ddl only.
  int get_tablet_cache_interval(
      const int64_t context_id,
      const ObTabletID &tablet_id,
      share::ObTabletCacheInterval &interval);
  int get_tablet_mgr(
      const ObTabletID &tablet_id,
      const bool is_full_direct_load,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle);
  int get_tablet_mgr_and_check_major(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool is_full_direct_load,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
      bool &is_major_sstable_exist);
  // for direct load rescan
  int calc_range(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t thread_cnt,
      const bool is_full_direct_load);
  int fill_column_group(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool is_full_direct_load,
      const int64_t thread_cnt,
      const int64_t thread_id);
  int cancel(
      const int64_t context_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool is_full_direct_load);
  int gc_tablet_direct_load();
  // remove tablet direct load mgr from hashmap,
  // for full direct load, it will be called when physical major generates,
  // for incremental direct load, it will be called in close_tablet_direct_load
  // @param [in] context_id to match ObTabletDirectLoadMgr, avoid an old task remove a new ObTabletDirectLoadMgr
  //             only take effect when !mgr_key.is_full_direct_load_ and context_id > 0
  int remove_tablet_direct_load(const ObTabletDirectLoadMgrKey &mgr_key, int64_t context_id = 0);
  ObIAllocator &get_allocator() { return allocator_; }
private:
  struct GetGcCandidateOp final {
  public:
    GetGcCandidateOp(ObIArray<std::pair<share::ObLSID, ObTabletDirectLoadMgrKey>> &candidate_mgrs)
      : candidate_mgrs_(candidate_mgrs) {}
    ~GetGcCandidateOp() {}
    int operator() (common::hash::HashMapPair<ObTabletDirectLoadMgrKey, ObTabletDirectLoadMgr *> &kv);
  private:
    DISALLOW_COPY_AND_ASSIGN(GetGcCandidateOp);
    ObIArray<std::pair<share::ObLSID, ObTabletDirectLoadMgrKey>> &candidate_mgrs_;
  };

  int try_create_tablet_direct_load_mgr(
      const int64_t context_id,
      const int64_t execution_id,
      const bool major_sstable_exist,
      ObIAllocator &allocator,
      const ObTabletDirectLoadMgrKey &mgr_key,
      const bool is_lob_tablet,
      ObTabletDirectLoadMgrHandle &handle);
  int get_tablet_mgr_no_lock(
      const ObTabletDirectLoadMgrKey &mgr_key,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle);
  int check_and_process_finished_tablet(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObIStoreRowIterator *row_iter = nullptr,
      const int64_t task_id = 0,
      const int64_t table_id = common::OB_INVALID_ID,
      const int64_t execution_id = -1);
  int get_tablet_exec_context_with_rlock(
      const ObTabletDirectLoadExecContextId &exec_id,
      ObTabletDirectLoadExecContext &exec_context);
  int remove_tablet_direct_load_nolock(const ObTabletDirectLoadMgrKey &mgr_key, int64_t context_id = 0);
  // to generate unique slice id for slice writer, putting here is just to
  // simplify the logic of the tablet_direct_load_mgr.
  int64_t generate_slice_id();
  int64_t generate_context_id();

private:
  typedef common::hash::ObHashMap<
    ObTabletDirectLoadMgrKey,
    ObTabletDirectLoadMgr *,
    common::hash::NoPthreadDefendMode> TABLET_MGR_MAP;
  typedef common::hash::ObHashMap<
    ObTabletDirectLoadExecContextId, // context_id
    ObTabletDirectLoadExecContext,
    common::hash::NoPthreadDefendMode> TABLET_EXEC_CONTEXT_MAP;
  bool is_inited_;
  common::ObBucketLock bucket_lock_; // to avoid concurrent execution on the TabletDirectLoadMgr.
  common::ObConcurrentFIFOAllocator allocator_;
  TABLET_MGR_MAP tablet_mgr_map_;
  TABLET_EXEC_CONTEXT_MAP tablet_exec_context_map_;
  int64_t slice_id_generator_;
  int64_t context_id_generator_;
  volatile int64_t last_gc_time_;
DISALLOW_COPY_AND_ASSIGN(ObTenantDirectLoadMgr);
};


struct ObTabletDirectLoadBuildCtx final
{
public:
  ObTabletDirectLoadBuildCtx();
  ~ObTabletDirectLoadBuildCtx();
  bool is_valid () const;
  static uint64_t get_slice_id_hash(const int64_t slice_id)
  {
    return common::murmurhash(&slice_id, sizeof(slice_id), 0L);
  }
  void reset_slice_ctx_on_demand();
  void cleanup_slice_writer(const int64_t context_id);
  TO_STRING_KV(K_(build_param), K_(is_task_end), K_(task_finish_count), K_(task_total_cnt), K_(sorted_slices_idx), K_(commit_scn), KPC(storage_schema_));
  struct AggregatedCGInfo final {
  public:
    AggregatedCGInfo()
      : start_idx_(0),
        last_idx_(0) {}
    ~AggregatedCGInfo() {}
    TO_STRING_KV(K_(start_idx), K_(last_idx));
  public:
    int64_t start_idx_;
    int64_t last_idx_;
  };
public:
  struct SliceKey
  {
  public:
    SliceKey() : context_id_(0), slice_id_(0) {}
    explicit SliceKey(const int64_t context_id, const int64_t slice_id): context_id_(context_id), slice_id_(slice_id) {}
    ~SliceKey() {}
    uint64_t hash() const { return murmurhash(&slice_id_, sizeof(slice_id_), 0); }
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS;}
    bool operator == (const SliceKey &other) const { return context_id_ == other.context_id_ && slice_id_ == other.slice_id_; }
    TO_STRING_KV(K_(context_id), K_(slice_id));
  public:
    int64_t context_id_;
    int64_t slice_id_;
  };
  typedef common::hash::ObHashMap<
    SliceKey,
    ObDirectLoadSliceWriter *> SLICE_MGR_MAP;
  common::ObConcurrentFIFOAllocator allocator_;
  common::ObConcurrentFIFOAllocator slice_writer_allocator_;
  ObTabletDirectLoadInsertParam build_param_;
  SLICE_MGR_MAP slice_mgr_map_; // key is <context_id, slice_id>, decided by upper caller.
  blocksstable::ObWholeDataStoreDesc data_block_desc_;
  blocksstable::ObSSTableIndexBuilder *index_builder_;
  common::ObArray<ObOptColumnStat*> column_stat_array_; // online column stat result.
  common::ObArray<ObDirectLoadSliceWriter *> sorted_slice_writers_;
  common::ObArray<AggregatedCGInfo> sorted_slices_idx_; //for cg_aggregation
  bool is_task_end_; // to avoid write commit log/freeze in memory index sstable again.
  int64_t task_finish_count_; // reach the parallel slice cnt, means the tablet data finished.
  int64_t task_total_cnt_; // parallelism of the PX.
  int64_t fill_column_group_finish_count_;
  share::SCN commit_scn_;
  ObArenaAllocator schema_allocator_;
  ObStorageSchema *storage_schema_;
};

class ObTabletDirectLoadMgr
{
public:
  ObTabletDirectLoadMgr();
  virtual ~ObTabletDirectLoadMgr();
  virtual bool is_valid();
  virtual int update(
      ObTabletDirectLoadMgr *lob_tablet_mgr,
      const ObTabletDirectLoadInsertParam &build_param);
  virtual int open(const int64_t current_execution_id, share::SCN &start_scn) = 0; // write start log.
  virtual int close(const int64_t current_execution_id, const share::SCN &start_scn) = 0; // end tablet.

  virtual int open_sstable_slice(
      const bool is_data_tablet_process_for_lob,
      const blocksstable::ObMacroDataSeq &start_seq,
      const int64_t context_id,
      const int64_t slice_id);
  virtual int fill_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info,
      const share::SCN &start_scn,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows,
      ObInsertMonitor *insert_monitor = NULL);
  virtual int fill_lob_sstable_slice(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      const share::SCN &start_scn,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObDatumRow &datum_row);
  virtual int fill_lob_sstable_slice(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      const share::SCN &start_scn,
      share::ObTabletCacheInterval &pk_interval,
      const ObArray<int64_t> &lob_column_idxs,
      const ObArray<common::ObObjMeta> &col_types,
      blocksstable::ObDatumRow &datum_row);
  // for delete lob in incremental direct load only
  virtual int fill_lob_meta_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      const share::SCN &start_scn,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows);
  virtual int close_sstable_slice(
      const bool is_data_tablet_process_for_lob,
      const ObDirectLoadSliceInfo &slice_info,
      const share::SCN &start_scn,
      const int64_t execution_id,
      ObInsertMonitor *insert_monitor,
      blocksstable::ObMacroDataSeq &next_seq);

  // for ref_cnt
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  int cancel();
  int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
  int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }

  // some utils.
  virtual int64_t get_context_id() const { return 0; }
  virtual share::SCN get_start_scn() = 0;
  virtual share::SCN get_commit_scn(const ObTabletMeta &tablet_meta) = 0;
  inline const ObITable::TableKey &get_table_key() const { return table_key_; }
  inline uint64_t get_data_format_version() const { return data_format_version_; }
  inline ObDirectLoadType get_direct_load_type() const { return direct_load_type_; }
  inline ObTabletDirectLoadBuildCtx &get_sqc_build_ctx() { return sqc_build_ctx_; }
  inline const share::ObLSID &get_ls_id() const { return ls_id_; }
  inline const ObTabletID &get_tablet_id() const { return tablet_id_; }
  inline ObTabletID get_lob_meta_tablet_id() {
    return lob_mgr_handle_.is_valid() ? lob_mgr_handle_.get_obj()->get_tablet_id() : ObTabletID();
  }
  inline int64_t get_ddl_task_id() const { return sqc_build_ctx_.build_param_.runtime_only_param_.task_id_; }
  // virtual int get_online_stat_collect_result();

  virtual int wrlock(const int64_t timeout_us, uint32_t &lock_tid);
  virtual int rdlock(const int64_t timeout_us, uint32_t &lock_tid);
  virtual void unlock(const uint32_t lock_tid);
  int prepare_index_builder_if_need(const ObTableSchema &table_schema);
  virtual int wait_notify(const ObDirectLoadSliceWriter *slice_writer, const share::SCN &start_scn);
  int fill_column_group(const int64_t thread_cnt, const int64_t thread_id);
  virtual int notify_all();
  virtual int calc_range(const int64_t thread_cnt);
  int calc_cg_range(ObArray<ObDirectLoadSliceWriter *> &sorted_slices, const int64_t thread_cnt);
  const ObIArray<ObColumnSchemaItem> &get_column_info() const { return column_items_; };
  int prepare_storage_schema(ObTabletHandle &tablet_handle);

  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(is_schema_item_ready), K_(ls_id), K_(tablet_id), K_(table_key), K_(data_format_version), K_(ref_cnt),
               K_(direct_load_type), K_(sqc_build_ctx), KPC(lob_mgr_handle_.get_obj()), K_(schema_item), K_(column_items), K_(lob_column_idxs));

private:
  int prepare_schema_item_on_demand(const uint64_t table_id,
                                    const int64_t parallel);
  void calc_cg_idx(const int64_t thread_cnt, const int64_t thread_id, int64_t &strat_idx, int64_t &end_idx);
  int fill_aggregated_column_group(
      const int64_t start_idx,
      const int64_t last_idx,
      const ObStorageSchema *storage_schema,
      ObCOSliceWriter *cur_writer,
      int64_t &fill_cg_finish_count,
      int64_t &fill_row_cnt);
// private:
  /* +++++ online column stat collect +++++ */
  // virtual int init_sql_statistics_if_needed();
  // int collect_obj(const blocksstable::ObDatumRow &datum_row);
  /* +++++ -------------------------- +++++ */
public:
  static const int64_t TRY_LOCK_TIMEOUT = 1 * 1000000; // 1s
  static const int64_t EACH_MACRO_MIN_ROW_CNT = 1000000; // 100w
protected:
  bool is_inited_;
  bool is_schema_item_ready_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObITable::TableKey table_key_;
  uint64_t data_format_version_;
  common::ObLatch lock_;
  int64_t ref_cnt_;
  ObDirectLoadType direct_load_type_;
  // sqc_build_ctx_ is just used for the observer node who receives the requests from the SQL Layer
  // to write the start log and the data redo log. And other observer nodes can not use it.
  ObTabletDirectLoadBuildCtx sqc_build_ctx_;
  // to handle the lob meta tablet, use it before the is_valid judgement.
  ObTabletDirectLoadMgrHandle lob_mgr_handle_;
  common::ObThreadCond cond_; // for fill column group
  // cache ObTableSchema for lob direct load performance
  ObArray<ObColumnSchemaItem> column_items_;
  ObArray<int64_t> lob_column_idxs_;
  ObArray<common::ObObjMeta> lob_col_types_;
  ObTableSchemaItem schema_item_;
  int64_t dir_id_;
};

class ObTabletFullDirectLoadMgr final : public ObTabletDirectLoadMgr
{
public:
  ObTabletFullDirectLoadMgr();
  ~ObTabletFullDirectLoadMgr();
  virtual int update(
      ObTabletDirectLoadMgr *lob_tablet_mgr,
      const ObTabletDirectLoadInsertParam &build_param);
  int open(const int64_t current_execution_id, share::SCN &start_scn) override; // start
  int close(const int64_t execution_id, const share::SCN &start_scn) override; // end, including write commit log, wait major sstable generates.

  int start_nolock(
      const ObITable::TableKey &table_key,
      const share::SCN &start_scn,
      const uint64_t data_format_version,
      const int64_t execution_id,
      const share::SCN &checkpoint_scn,
      ObDDLKvMgrHandle &ddl_kv_mgr_handle,
      ObDDLKvMgrHandle &lob_kv_mgr_handle);
  int start(
      ObTablet &tablet,
      const ObITable::TableKey &table_key,
      const share::SCN &start_scn,
      const uint64_t data_format_version,
      const int64_t execution_id,
      const share::SCN &checkpoint_scn);
  int start_with_checkpoint(
      ObTablet &tablet,
      const share::SCN &start_scn,
      const uint64_t data_format_version,
      const int64_t execution_id,
      const share::SCN &checkpoint_scn);
  int commit(
      ObTablet &tablet,
      const share::SCN &start_scn,
      const share::SCN &commit_scn,
      const uint64_t table_id,
      const int64_t ddl_task_id,
      const bool is_replay); // schedule build a major sstable
  int replay_commit(
      ObTablet &tablet,
      const share::SCN &start_scn,
      const share::SCN &commit_scn,
      const uint64_t table_id,
      const int64_t ddl_task_id);

  void set_commit_scn_nolock(const share::SCN &scn);
  int set_commit_scn(const share::SCN &scn);
  share::SCN get_start_scn() override;
  share::SCN get_commit_scn(const ObTabletMeta &tablet_meta) override;

  // check need schedule major compaction.
  int can_schedule_major_compaction_nolock(
      const ObTablet &tablet,
      bool &can_schedule);
  int prepare_ddl_merge_param(
      const ObTablet &tablet,
      ObDDLTableMergeDagParam &merge_param);
  int prepare_major_merge_param(ObTabletDDLParam &param);
  void cleanup_slice_writer(const int64_t context_id);
  INHERIT_TO_STRING_KV("ObTabletDirectLoadMgr", ObTabletDirectLoadMgr, K_(start_scn), K_(commit_scn), K_(execution_id));
private:
  bool is_started() { return start_scn_.is_valid_and_not_min(); }
  int schedule_merge_task(const share::SCN &start_scn, const share::SCN &commit_scn, const bool wait_major_generated, const bool is_replay); // try wait build major sstable
  int cleanup_unlock();
  int init_ddl_table_store(const share::SCN &start_scn, const int64_t snapshot_version, const share::SCN &ddl_checkpoint_scn);
  int update_major_sstable();

private:
  share::SCN start_scn_;
  share::SCN commit_scn_;
  int64_t execution_id_;
DISALLOW_COPY_AND_ASSIGN(ObTabletFullDirectLoadMgr);
};

class ObTabletIncDirectLoadMgr final : public ObTabletDirectLoadMgr
{
public:
  ObTabletIncDirectLoadMgr(int64_t context_id);
  ~ObTabletIncDirectLoadMgr();

  // called by creator only
  int update(ObTabletDirectLoadMgr *lob_tablet_mgr,
             const ObTabletDirectLoadInsertParam &build_param) override final;
  int open(const int64_t current_execution_id, share::SCN &start_scn) override final;
  int close(const int64_t current_execution_id, const share::SCN &start_scn) override final;

  int64_t get_context_id() const override { return context_id_; }
  share::SCN get_start_scn() override { return start_scn_; }
  // unused, for full direct load only
  share::SCN get_commit_scn(const ObTabletMeta &tablet_meta) override
  {
    UNUSED(tablet_meta);
    return share::SCN::invalid_scn();
  }

private:
  int start(const int64_t execution_id, const share::SCN &start_scn);
  int commit(const int64_t execution_id, const share::SCN &commit_scn);

private:
  int64_t context_id_;
  share::SCN start_scn_;
  bool is_closed_;
DISALLOW_COPY_AND_ASSIGN(ObTabletIncDirectLoadMgr);
};

}// namespace storage
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_OB_DIRECT_INSERT_SSTABLE_CTX_NEW_H
