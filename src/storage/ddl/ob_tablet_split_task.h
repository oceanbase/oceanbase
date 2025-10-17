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

#ifndef OCEANBASE_STORAGE_OB_TABLET_SPLIT_TASK_H
#define OCEANBASE_STORAGE_OB_TABLET_SPLIT_TASK_H

#include "share/ob_ddl_common.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/schema/ob_table_param.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_sstable_row_whole_scanner.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_tablet_split_util.h"
#include "storage/ob_micro_block_index_iterator.h"

namespace oceanbase
{
namespace storage
{

bool is_data_split_dag(const ObDagType::ObDagTypeEnum &dag_type);

struct ObSplitSSTableTaskKey final
{
public:
  ObSplitSSTableTaskKey()
    : src_sst_key_(), dest_tablet_id_() { }
  ObSplitSSTableTaskKey(const ObITable::TableKey &src_key, const ObTabletID &dst_tablet_id)
    : src_sst_key_(src_key), dest_tablet_id_(dst_tablet_id)
  { }
  ~ObSplitSSTableTaskKey() { reset(); }
  void reset() {
    src_sst_key_.reset();
    dest_tablet_id_.reset();
  }
  int64_t hash() const { return src_sst_key_.hash() + dest_tablet_id_.hash(); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator== (const ObSplitSSTableTaskKey &other) const {
    return src_sst_key_ == other.src_sst_key_ && dest_tablet_id_ == other.dest_tablet_id_;
  }
  bool is_valid() const { return src_sst_key_.is_valid() && dest_tablet_id_.is_valid(); }
  TO_STRING_KV(K_(src_sst_key), K_(dest_tablet_id));
public:
  ObITable::TableKey src_sst_key_;
  ObTabletID dest_tablet_id_;
};

typedef std::pair<common::ObTabletID, blocksstable::ObDatumRowkey> TabletBoundPair;

struct ObTabletSplitParam : public share::ObIDagInitParam
{
public:
  ObTabletSplitParam();
  virtual ~ObTabletSplitParam();
  bool is_valid() const;
  int init(const ObTabletSplitParam &param);
  int init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg);
  int init(const obrpc::ObTabletSplitArg &arg);
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(ls_id), K_(table_id), K_(schema_version),
               K_(task_id), K_(source_tablet_id), K_(dest_tablets_id), K_(compaction_scn), K_(user_parallelism),
               K_(compat_mode), K_(data_format_version), K_(consumer_group_id),
               K_(can_reuse_macro_block), K_(split_sstable_type), K_(parallel_datum_rowkey_list),
               K_(min_split_start_scn));
private:
  common::ObArenaAllocator rowkey_allocator_; // for DatumRowkey.
public:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t table_id_;
  int64_t schema_version_; // always the data table schema version
  int64_t task_id_; // ddl task id.
  ObTabletID source_tablet_id_;
  ObArray<ObTabletID> dest_tablets_id_;
  int64_t compaction_scn_;
  int64_t user_parallelism_;
  lib::Worker::CompatMode compat_mode_;
  int64_t data_format_version_;
  uint64_t consumer_group_id_;
  bool can_reuse_macro_block_;
  share::ObSplitSSTableType split_sstable_type_;
  common::ObSArray<blocksstable::ObDatumRowkey> parallel_datum_rowkey_list_;
  share::SCN min_split_start_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitParam);
};

struct ObTabletSplitCtx final
{
public:
  ObTabletSplitCtx();
  ~ObTabletSplitCtx();
  int init(const ObTabletSplitParam &param);
  bool is_valid() const;
  TO_STRING_KV(
    K_(is_inited), K_(is_split_finish_with_meta_flag), K_(data_split_ranges), K_(complement_data_ret),
    K_(skipped_split_major_keys), K_(split_point_major_macros), K_(split_point_minor_macros),
    K_(parallel_cnt_of_each_sstable), K_(split_scn), K_(row_inserted), K_(cg_row_inserted),
    K_(physical_row_count), K_(split_scn), K_(reorg_scn),
    K(ls_rebuild_seq_), K_(split_majors_count), K_(max_major_snapshot)
#ifdef OB_BUILD_SHARED_STORAGE
    , K_(is_data_split_executor)
#endif
    );

private:
  template <typename KEY, typename VALUE>
  struct GetMapItemKeyFn final
  {
  public:
    GetMapItemKeyFn() : map_keys_(), ret_code_(OB_SUCCESS) {}
    ~GetMapItemKeyFn() = default;
    int operator() (common::hash::HashMapPair<KEY, VALUE> &entry)
    {
      int ret = ret_code_; // for LOG_WARN
      if (OB_LIKELY(OB_SUCCESS == ret_code_) && OB_SUCCESS != (ret_code_ = map_keys_.push_back(entry.first))) {
        ret = ret_code_;
        LOG_WARN("push back map item key failed", K(ret_code_), K(entry.first));
      }
      return ret_code_;
    }
  public:
    ObArray<KEY> map_keys_;
    int ret_code_;
  };
public:
  // generate index tree.
  int prepare_index_builder(
      const ObTabletSplitParam &param);
  int get_clipped_storage_schema_on_demand(
      const ObTabletID &src_tablet_id,
      const ObSSTable &src_sstable,
      const ObStorageSchema &latest_schema,
      const bool try_create,
      const ObStorageSchema *&storage_schema);
  // get ith from majors or minors.
  int get_index_in_source_sstables(
      const ObSSTable &src_sstable,
      int64_t &sstable_index/*start from 0*/);
  int append_split_point_macros(
      const bool is_major_macros,
      const ObIArray<blocksstable::MacroBlockId> &additional_macros);
private:
  int get_split_majors_infos();
private:
  common::ObArenaAllocator range_allocator_; // for datum range.
  common::ObArenaAllocator allocator_;
  ObSpinLock lock_;
public:
  typedef common::hash::ObHashMap<
    ObSplitSSTableTaskKey, ObSSTableIndexBuilder*> INDEX_BUILDER_MAP;
  bool is_inited_;
  bool is_split_finish_with_meta_flag_;

  ObArray<ObDatumRange> data_split_ranges_;
  int complement_data_ret_;
  ObLSHandle ls_handle_;
  ObTabletHandle tablet_handle_; // is important, rowkey_read_info, source_tables rely on it.
  ObTableStoreIterator table_store_iterator_;
  // for rewrite macro block task.
  INDEX_BUILDER_MAP index_builder_map_; // map between source sstable and dest sstables.
  common::hash::ObHashMap<ObITable::TableKey/*source major sstable*/, ObStorageSchema*> clipped_schemas_map_;
  ObArray<ObITable::TableKey> skipped_split_major_keys_;
  int64_t row_inserted_;
  int64_t cg_row_inserted_; // unused
  int64_t physical_row_count_;
  // the split point macro blocks in reuse macro split scenario.
  common::ObArray<blocksstable::MacroBlockId> split_point_major_macros_;
  common::ObArray<blocksstable::MacroBlockId> split_point_minor_macros_;
  int64_t parallel_cnt_of_each_sstable_;
  share::SCN split_scn_;
  share::SCN reorg_scn_; // transfer_scn.
  int64_t ls_rebuild_seq_;
  int64_t split_majors_count_;
  int64_t max_major_snapshot_;
#ifdef OB_BUILD_SHARED_STORAGE
  ObSSDataSplitHelper ss_split_helper_;
  bool is_data_split_executor_;
#endif
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitCtx);
};

class ObIDataSplitDag : public share::ObIDag
{
public:
  explicit ObIDataSplitDag(const ObDagType::ObDagTypeEnum type) :
    ObIDag(type) { }
  virtual ~ObIDataSplitDag() { }
  virtual int report_replica_build_status() const = 0;
  virtual int get_complement_data_ret() const = 0;
  virtual void set_complement_data_ret(const int ret_code) = 0;
protected:
  int alloc_and_add_common_task(
    ObITask *last_task,
    const int64_t rebuild_seq,
    const ObLSID &ls_id,
    const ObTabletID &src_tablet_id,
    const ObIArray<ObTabletID> &dst_tablet_ids,
    const bool can_reuse_macro_block,
    const share::SCN &dest_reorg_scn,
    const share::SCN &split_start_scn);
};

class ObTabletSplitDag final: public ObIDataSplitDag
{
public:
  ObTabletSplitDag();
  virtual ~ObTabletSplitDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual uint64_t hash() const override;
  bool operator ==(const share::ObIDag &other) const;
  bool is_inited() const { return is_inited_; }
  ObTabletSplitCtx &get_context() { return context_; }
  void handle_init_failed_ret_code(int ret) { context_.complement_data_ret_ = ret; }
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return param_.compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
  virtual int report_replica_build_status() const override;
  virtual int get_complement_data_ret() const override {
    return context_.complement_data_ret_;
  }
  virtual void set_complement_data_ret(const int ret_code) override {
    context_.complement_data_ret_ = OB_SUCCESS == context_.complement_data_ret_ ?
      ret_code : context_.complement_data_ret_;
  }
  int calc_total_row_count();
private:
  bool is_inited_;
  ObTabletSplitParam param_;
  ObTabletSplitCtx context_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitDag);
};

class ObTabletSplitPrepareTask final : public share::ObITask
{
public:
  ObTabletSplitPrepareTask()
    : ObITask(TASK_TYPE_DDL_SPLIT_PREPARE), is_inited_(false), param_(nullptr), context_(nullptr)
    { }
  virtual ~ObTabletSplitPrepareTask() = default;
  int init(ObTabletSplitParam &param, ObTabletSplitCtx &ctx);
  virtual int process() override;
private:
  int prepare_context();
private:
  bool is_inited_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitPrepareTask);
};

class ObTabletSplitWriteTask final : public share::ObITask
{
public:
  ObTabletSplitWriteTask();
  virtual ~ObTabletSplitWriteTask();
  int init(const int64_t task_id,
    ObTabletSplitParam &param, ObTabletSplitCtx &ctx, storage::ObITable *table);
  virtual int process() override;
private:
  int generate_next_task(ObITask *&next_task);
  // prepare default_row, write_row.
  int prepare_context(const ObStorageSchema *&clipped_storage_schema);
  // fill nop for minor, and fill orig default value for major.
  int fill_tail_column_datums(
      const blocksstable::ObDatumRow &scan_row);
  int prepare_macro_seq_param(
      const int64_t dest_tablet_index, // index at param.dest_tablets_id_
      ObMacroSeqParam &macro_seq_param);
  int prepare_macro_block_writer(
      const ObStorageSchema &clipped_storage_schema,
      ObIArray<ObWholeDataStoreDesc> &data_desc_arr,
      ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr);
  // for reuse macro block task.
  int prepare_sorted_high_bound_pair(
      common::ObSArray<TabletBoundPair> &tablet_bound_arr);
  int process_reuse_macro_block_task(
      const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
      const ObStorageSchema &clipped_storage_schema);
  int process_rows_for_reuse_task(
      const ObStorageSchema &clipped_storage_schema,
      const ObIArray<TabletBoundPair> &tablet_bound_arr,
      const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
      const ObMacroBlockDesc &data_macro_desc,
      int64_t &dest_tablet_index);
  // for rewrite macro block task.
  int process_rewrite_macro_block_task(
      const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
      const ObStorageSchema &clipped_storage_schema);
  int process_rows_for_rewrite_task(
      const ObStorageSchema &clipped_storage_schema,
      const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
      const ObDatumRange &query_range);
  int check_and_cast_high_bound(
      const common::ObIArray<ObColDesc> &col_descs,
      common::ObSArray<TabletBoundPair> &bound_pairs);
private:
  static const int64_t MAP_BUCKET_NUM = 100;
  bool is_inited_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  ObSSTable *sstable_; // split source sstable.
  const ObITableReadInfo *rowkey_read_info_;
  blocksstable::ObDatumRow write_row_;
  blocksstable::ObDatumRow default_row_;

  // for rewrite macro block task.
  int64_t task_id_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitWriteTask);
};

class ObTabletSplitMergeTask final : public share::ObITask
{
public:
  ObTabletSplitMergeTask()
    : ObITask(TASK_TYPE_DDL_SPLIT_MERGE),
    allocator_("SplitSSTCreate", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    is_inited_(false), param_(nullptr), context_(nullptr)
    { }
  virtual ~ObTabletSplitMergeTask() = default;
  int init(ObTabletSplitParam &param, ObTabletSplitCtx &ctx);
  virtual int process() override;
  static int check_need_fill_empty_sstable(
      ObLSHandle &ls_handle,
      const bool is_minor_sstable,
      const ObITable::TableKey &table_key,
      const ObTabletID &dst_tablet_id,
      bool &need_fill_empty_sstable,
      SCN &end_scn);
  static int build_create_empty_sstable_param(
      const ObSSTableBasicMeta &meta,
      const ObITable::TableKey &table_key,
      const ObTabletID &dst_tablet_id,
      const SCN &end_scn,
      ObTabletCreateSSTableParam &create_sstable_param);
  static int update_table_store_with_batch_tables(
      const int64_t ls_rebuild_seq,
      const ObLSHandle &ls_handle,
      const ObTabletHandle &src_tablet_handle,
      const ObTabletID &dst_tablet_id,
      const ObTablesHandleArray &tables_handle,
      const compaction::ObMergeType &merge_type,
      const ObIArray<ObITable::TableKey> &skipped_split_major_keys,
      const int64_t op_id,
      const share::SCN &dest_reorg_scn);
private:
  int create_sstable(
      const share::ObSplitSSTableType &split_sstable_type);
  int build_create_sstable_param(
      const ObSSTable &src_table,
      const int64_t dest_tablet_index, // index at param.dest_tablets_id_
      ObSSTableIndexBuilder *index_builder,
      const ObIArray<blocksstable::MacroBlockId> &split_point_macros_id,
      ObTabletCreateSSTableParam &create_sstable_param);
  int check_and_create_mds_sstable(
      const ObTabletID &dest_tablet_id);
#ifdef OB_BUILD_SHARED_STORAGE
  int close_ss_index_builder(
      const ObSSTable &src_table, // source table.
      const int64_t dest_tablet_index, // index at param.dest_tablets_id_
      ObSSTableIndexBuilder *index_builder,
      ObSSTableMergeRes &res);
#endif
private:
  common::ObArenaAllocator allocator_; // for sstable created.
  bool is_inited_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitMergeTask);
};

#ifdef OB_BUILD_SHARED_STORAGE
struct ObMacroEndKey
{
public:
  ObMacroEndKey (const MacroBlockId &id, const ObDatumRowkey &end_key)
    : macro_id_(id), end_key_(end_key)
    {}
  ObMacroEndKey ()
    : macro_id_(), end_key_()
    {}
  void reset()
  {
    macro_id_.reset();
    end_key_.reset();
  }
  bool is_valid() const {
    return macro_id_.is_valid() && end_key_.is_valid();
  }
  int assign(const ObMacroEndKey &other);
  TO_STRING_KV(K_(macro_id), K(end_key_));
public:
  MacroBlockId macro_id_;
  ObDatumRowkey end_key_;
};

// download sstable, prewarm, and update local table store.
int check_test_block_downloading();
class ObSplitDownloadSSTableTask final : public share::ObITask
{
public:
  ObSplitDownloadSSTableTask()
    : ObITask(TASK_TYPE_DDL_SPLIT_DOWNLOAD_SSTABLE),
      is_inited_(false), ls_rebuild_seq_(-1), ls_id_(),
      source_tablet_id_(), dest_tablets_id_(), can_reuse_macro_block_(false),
      dest_reorg_scn_()
    { }
  virtual ~ObSplitDownloadSSTableTask() = default;
  int init(
      const int64_t ls_rebuild_seq,
      const ObLSID &ls_id,
      const ObTabletID &src_tablet_id,
      const ObIArray<ObTabletID> &dst_tablets_id,
      const bool can_reuse_macro_block,
      const share::SCN &dest_reorg_scn,
      const share::SCN &split_start_scn);
  static int prewarm_for_split(
      const ObTabletHandle &dest_tablet_handle,
      ObSSTable &sstable);
  static int is_split_dest_sstable(
      const ObSSTable &sstable,
      bool &is_split_dest_sstable);
  virtual int process() override;
private:
  static int prewarm_split_point_macro_if_need(
      const int64_t dest_tablet_id,
      const ObSSTable &dest_sstable,
      const ObIArray<MacroBlockId> &dest_macro_ids/*fist and last macro of dest sstable if any*/);
  static int iterate_macros_update_eff_id(
      const ObTabletID &dest_tablet_id,
      ObDualMacroMetaIterator &meta_iter,
      ObIArray<MacroBlockId> &dest_macro_ids,
      ObIAllocator &allocator);
  static int iterate_micros_update_eff_id(
      const ObTabletID &dest_tablet_id,
      ObMicroBlockIndexIterator &micro_iter);
private:
  int get_shared_tablet_versions_iter(
      const ObTabletID &dst_tablet_id,
      const share::SCN &end_version,
      ObSSMetaIterGuard<ObSSTabletIterator> &iter_guard,
      ObSSTabletIterator *&tablet_version_iter);
  int get_shared_tablet_for_split_major(
      const ObTabletID &dst_tablet_id,
      share::SCN &target_tablet_version);
  int get_shared_tablet_for_split_mds(
      const ObTabletID &dst_tablet_id,
      const share::SCN &target_major_tablet_version,
      share::SCN &target_tablet_version);
  int get_shared_tablet_for_split_minor(
      const ObTabletID &dst_tablet_id,
      const share::SCN &target_major_tablet_version,
      share::SCN &target_tablet_version);
  int get_specified_shared_tablet_versions(
      const ObTabletID &dst_tablet_id,
      ObIArray<share::SCN> &target_tablet_versions); // order by minor, mds, major.
  int prewarm(
      const ObTabletHandle &ss_tablet_handle,
      const ObTablesHandleArray &batch_sstables_handle);
  int collect_split_sstables(
      ObArenaAllocator &allocator,
      const share::ObSplitSSTableType &split_sstable_type,
      const ObTableStoreIterator &ss_table_store_iterator,
      ObTablesHandleArray &batch_sstables_handle);
  int download_sstables_and_update_local(
      ObLSHandle &new_ls_handle,
      const ObTabletHandle &local_source_tablet_hdl);

private:
  bool is_inited_;
  int64_t ls_rebuild_seq_;
  ObLSID ls_id_;
  ObTabletID source_tablet_id_;
  ObArray<ObTabletID> dest_tablets_id_;
  bool can_reuse_macro_block_;
  share::SCN dest_reorg_scn_;
  share::SCN split_start_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObSplitDownloadSSTableTask);
};
#endif

class ObSplitFinishTask final : public share::ObITask
{
public:
  ObSplitFinishTask()
    :ObITask(TASK_TYPE_DDL_SPLIT_FINISH),
    is_inited_(false)
  {}
  virtual ~ObSplitFinishTask() = default;
  int init();
  virtual int process() override;
private:
  bool is_inited_;
};

struct ObSplitScanParam final
{
public:
  ObSplitScanParam(
    const int64_t table_id,
    ObTablet &src_tablet,
    const ObDatumRange &query_range,
    const ObStorageSchema &storage_schema) :
    table_id_(table_id), src_tablet_(src_tablet), query_range_(&query_range),
    storage_schema_(&storage_schema)
  { }
  ~ObSplitScanParam() = default;
  bool is_valid() const {
    return table_id_ > 0 && src_tablet_.is_valid() && nullptr != query_range_
        && (nullptr != storage_schema_ && storage_schema_->is_valid());
  }
  TO_STRING_KV(K_(table_id), K_(src_tablet), KPC_(query_range), KPC_(storage_schema));
public:
  int64_t table_id_;
  ObTablet &src_tablet_; // split source tablet.
  const ObDatumRange *query_range_; // whole_range for sstable scan.
  const ObStorageSchema *storage_schema_;
};

class ObRowScan : public ObIStoreRowIterator
{
public:
  ObRowScan();
  virtual ~ObRowScan();
  // to scan the sstable with the specified query_range.
  int init(
      const ObSplitScanParam &param,
      ObSSTable &sstable);

  // to scan the specified whole macro block.
  int init(
      const ObSplitScanParam &param,
      const blocksstable::ObMacroBlockDesc &macro_desc,
      ObSSTable &sstable);

  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override;

  const ObITableReadInfo *get_rowkey_read_info() const { return rowkey_read_info_; }
  storage::ObTxTableGuards &get_tx_table_guards() { return ctx_.mvcc_acc_ctx_.get_tx_table_guards(); }

  TO_STRING_KV(K_(is_inited), K_(ctx), K_(access_ctx), KPC_(rowkey_read_info), K_(access_param));
private:
  int construct_access_param(
      const ObSplitScanParam &param);
  int construct_access_ctx(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id);
  int build_rowkey_read_info(
      const ObSplitScanParam &param);
private:
  bool is_inited_;
  ObSSTableRowWholeScanner *row_iter_;
  ObStoreCtx ctx_;
  ObTableAccessContext access_ctx_;
  ObRowkeyReadInfo *rowkey_read_info_; // with extra rowkey.
  ObTableAccessParam access_param_;
  common::ObArenaAllocator allocator_;
};

class ObSnapshotRowScan final : public ObIStoreRowIterator
{
public:
  ObSnapshotRowScan();
  virtual ~ObSnapshotRowScan();
  void reset();
  int init(
      const ObSplitScanParam &param,
      const ObIArray<share::schema::ObColDesc> &schema_store_col_descs,
      const int64_t schema_column_cnt,
      const int64_t schema_rowkey_cnt,
      const bool is_oracle_mode,
      const ObTabletHandle &tablet_handle,
      const int64_t snapshot_version);
  int construct_access_param(
      const uint64_t table_id,
      const common::ObTabletID &tablet_id,
      const ObITableReadInfo &read_info);
  int construct_range_ctx(ObQueryFlag &query_flag, const share::ObLSID &ls_id);
  int construct_multiple_scan_merge(const ObTabletTableIterator &table_iter, const ObDatumRange &range);
  int add_extra_rowkey(const ObDatumRow &row);
  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override;
  TO_STRING_KV(K_(is_inited));
private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  int64_t snapshot_version_;
  int64_t schema_rowkey_cnt_;
  ObDatumRange range_;
  ObTableReadInfo read_info_;
  ObDatumRow write_row_;
  ObArray<int32_t> out_cols_projector_;
  ObTableAccessParam access_param_;
  ObStoreCtx ctx_;
  ObTableAccessContext access_ctx_;
  ObGetTableParam get_table_param_;
  ObMultipleScanMerge *scan_merge_;
};

class ObUncommittedRowScan : public ObIStoreRowIterator
{
public:
  ObUncommittedRowScan();
  virtual ~ObUncommittedRowScan();
  int init(
      const ObSplitScanParam param,
      ObSSTable &src_sstable,
      const int64_t major_snapshot_version,
      const int64_t schema_column_cnt);
  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override;
private:
  int get_next_rowkey_rows();
  int row_queue_add(const ObDatumRow &row);
  void row_queue_reuse();
  int check_can_skip(const blocksstable::ObDatumRow &row, bool &can_skip);
private:
  ObRowScan row_scan_;
  bool row_scan_end_;
  const ObDatumRow *next_row_;
  int64_t major_snapshot_version_;
  int64_t trans_version_col_idx_;
  blocksstable::ObRowQueue row_queue_;
  ObArenaAllocator row_queue_allocator_;
  bool row_queue_has_unskippable_row_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_TABLET_SPLIT_TASK_H
