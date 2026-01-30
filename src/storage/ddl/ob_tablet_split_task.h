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
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/ddl/ob_tablet_split_sstable_helper.h"
#include "storage/ob_micro_block_index_iterator.h"
#include "storage/ddl/ob_tablet_split_iterator.h"

namespace oceanbase
{
namespace storage
{

bool is_data_split_dag(const ObDagType::ObDagTypeEnum &dag_type);
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
    K_(skipped_split_major_keys),
    K_(parallel_cnt_of_each_sstable), K_(split_scn), K_(row_inserted), K_(cg_row_inserted),
    K_(physical_row_count), K_(split_scn), K_(reorg_scn),
    K(ls_rebuild_seq_), K_(split_majors_count), K_(max_major_snapshot)
#ifdef OB_BUILD_SHARED_STORAGE
    , K_(is_data_split_executor)
#endif
    );

public:
  // generate index tree.
  int prepare_schema_and_result_array(
      const ObTabletSplitParam &param);
  // get ith from majors or minors.
  int get_index_in_source_sstables(
      const ObSSTable &src_sstable,
      int64_t &sstable_index/*start from 0*/) const;
  int generate_sstable(
      const int64_t dest_tablet_index,
      const ObTabletCreateSSTableParam &create_sstable_param);
  int generate_mds_sstable(
      const compaction::ObTabletMergeCtx &tablet_merge_ctx,
      const int64_t dest_tablet_index,
      int64_t index_tree_start_seq,
      ObMdsTableMiniMerger &mds_mini_merger);
  int get_result_tables_handle_array(
      const int64_t dest_tablet_index,
      const share::ObSplitSSTableType &split_sstable_type,
      ObTablesHandleArray &tables_handle_array,
      ObTablesHandleArray &cg_tables_handle_array/*to hold cgs' macro ref before updating table store*/);
  inline void set_complement_data_ret(const int ret_code) { ATOMIC_STORE(&complement_data_ret_, ret_code); }
  inline int get_complement_data_ret() const { return ATOMIC_LOAD(&complement_data_ret_); }
public:
  int alloc_and_init_helper(
      const ObSSTSplitHelperInitParam &param,
      ObSSTableSplitHelper *&helper);
  int get_sstable_helper(
      const ObITable::TableKey &table_key,
      ObSSTableSplitHelper *&helper);
  int free_helper(const ObITable::TableKey &table_key);
private:
  int get_split_majors_infos();
  int inner_organize_result_tables(
      const share::ObSplitSSTableType &split_sstable_type,
      const int64_t dest_tablet_index,
      ObTablesHandleArray &cg_tables_handle_array/*to hold cgs' macro ref*/);
private:
  common::ObConcurrentFIFOAllocator concurrent_allocator_;
  common::ObArenaAllocator arena_allocator_; // for tablet_handle, datum range, mds_storage_schema, and sstable_created.
  ObSpinLock lock_;
  common::ObBucketLock bucket_lock_;
  common::hash::ObHashMap<ObITable::TableKey, ObSSTableSplitHelper *> sstable_split_helpers_map_;
public:
  bool is_inited_;
  ObFixedArray<ObTablesHandleArray, common::ObIAllocator> result_tables_handle_array_;
  ObStorageSchema *mds_storage_schema_;
  bool is_split_finish_with_meta_flag_;

  ObArray<ObDatumRange> data_split_ranges_; // Rowkey ranges.
  int complement_data_ret_;
  ObLSHandle ls_handle_;
  ObTabletHandle tablet_handle_; // is important, rowkey_read_info, source_tables rely on it.
  ObTableStoreIterator table_store_iterator_;
  ObArray<ObITable::ObITable::TableKey> skipped_split_major_keys_;
  int64_t row_inserted_;
  int64_t cg_row_inserted_; // unused
  int64_t physical_row_count_;
  int64_t parallel_cnt_of_each_sstable_;
  share::SCN split_scn_;
  share::SCN reorg_scn_; // transfer_scn.
  int64_t ls_rebuild_seq_;
  int64_t split_majors_count_;
  int64_t max_major_snapshot_;
#ifdef OB_BUILD_SHARED_STORAGE
  ObSSDataSplitHelper ss_minor_split_helper_;
  ObSSDataSplitHelper ss_mds_split_helper_;
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
  virtual void report_build_stat(
    const char *event_name,
    const int result,
    const char *event_info = nullptr) const = 0;
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
  // TODO to remove ObTabletSplitWriteTask.
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
  virtual void report_build_stat(
    const char *event_name,
    const int result,
    const char *event_info = nullptr) const override;
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
    : ObITask(TASK_TYPE_DDL_SPLIT_PREPARE),
      is_inited_(false), param_(nullptr), context_(nullptr),
      tablet_merge_task_(nullptr)
    { }
  virtual ~ObTabletSplitPrepareTask() = default;
  int init(ObTabletSplitParam &param, ObTabletSplitCtx &ctx, ObITask &tablet_merge_task);
  virtual int process() override;
private:
  int prepare_context();
  int generate_next_tasks();
  int prepare_mds_mock_table_key(ObITable::TableKey &mock_mds_key);
private:
  bool is_inited_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  ObITask *tablet_merge_task_; // the last task for the dynamic tasks.
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitPrepareTask);
};

// process for each sstable.
class ObSSTableSplitPrepareTask final : public ObITask
{
public:
  ObSSTableSplitPrepareTask()
    : ObITask(TASK_TYPE_SSTABLE_SPLIT_PREPARE),
      is_inited_(false), param_(nullptr), context_(nullptr),
      tablet_merge_task_(nullptr), table_key_(), sstable_(nullptr)
    { }
  virtual ~ObSSTableSplitPrepareTask() = default;
  int init(
      ObTabletSplitParam &param,
      ObTabletSplitCtx &ctx,
      const ObITable::TableKey &table_key,
      storage::ObITable *table,
      ObITask *tablet_merge_task);
  virtual int process() override;
  TO_STRING_KV(K_(is_inited), K_(table_key), KPC(sstable_),
    KPC_(param), KPC_(context), KP_(tablet_merge_task));
private:
  int generate_common_tasks(
      const ObITable::TableKey &table_key);
  int generate_tasks_for_packed_sstable(
      const ObSSTableSplitHelper &helper);
  int generate_next_tasks();
private:
  bool is_inited_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  ObITask *tablet_merge_task_; // the last task for the dynamic tasks.
  ObITable::TableKey table_key_;
  ObSSTable *sstable_; // split source sstable, like row_store sstable or CO except CGs.
  DISALLOW_COPY_AND_ASSIGN(ObSSTableSplitPrepareTask);
};

class ObSSTableSplitWriteTask final : public ObITask
{
public:
  ObSSTableSplitWriteTask() :
    ObITask(TASK_TYPE_SSTABLE_SPLIT_WRITE),
    is_inited_(false), task_idx_(OB_INVALID_INDEX), param_(nullptr),
    context_(nullptr), table_key_()
  { }
  virtual ~ObSSTableSplitWriteTask() = default;
  int init(const int64_t task_id,
      ObTabletSplitParam &param,
      ObTabletSplitCtx &ctx,
      const ObITable::TableKey &table_key);
  virtual int process() override;
  TO_STRING_KV(K_(is_inited), K_(task_idx), K_(table_key),
    KPC(param_), KPC(context_));
private:
  int generate_next_task(
      ObITask *&next_task);
private:
  bool is_inited_;
  int64_t task_idx_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  ObITable::TableKey table_key_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableSplitWriteTask);
};

class ObSSTableSplitMergeTask final : public share::ObITask
{
public:
  ObSSTableSplitMergeTask() :
    ObITask(TASK_TYPE_SSTABLE_SPLIT_MERGE),
    is_inited_(false), param_(nullptr), context_(nullptr),
    table_key_()
  { }
  virtual ~ObSSTableSplitMergeTask() = default;
  int init(
      ObTabletSplitParam &param,
      ObTabletSplitCtx &ctx,
      const ObITable::TableKey &table_key);
  virtual int process() override;
  TO_STRING_KV(K_(is_inited), K_(table_key), KPC(param_),
    KPC(context_));
private:
  bool is_inited_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  ObITable::TableKey table_key_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableSplitMergeTask);
};

class ObTabletSplitMergeTask final : public share::ObITask
{
public:
  ObTabletSplitMergeTask()
    : ObITask(TASK_TYPE_DDL_SPLIT_MERGE),
    is_inited_(false), param_(nullptr), context_(nullptr)
    { }
  virtual ~ObTabletSplitMergeTask() = default;
  int init(ObTabletSplitParam &param, ObTabletSplitCtx &ctx);
  virtual int process() override;
  static int update_table_store_with_batch_tables(
      const int64_t ls_rebuild_seq,
      const ObLSHandle &ls_handle,
      const ObTabletHandle &src_tablet_handle,
      const ObTabletID &dst_tablet_id,
      const ObTablesHandleArray &tables_handle,
      const compaction::ObMergeType &merge_type,
      const ObIArray<ObITable::ObITable::TableKey> &skipped_split_major_keys,
      const int64_t op_id,
      const share::SCN &dest_reorg_scn);
private:
  int check_cg_sstables_checksum(
      const share::ObSplitSSTableType &split_sstable_type,
      const ObTablesHandleArray &batch_sstables_handle);
  int collect_and_update_sstable(
      const share::ObSplitSSTableType &split_sstable_type);
private:
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

}  // end namespace storage
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_TABLET_SPLIT_TASK_H
