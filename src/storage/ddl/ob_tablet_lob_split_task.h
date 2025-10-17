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

#ifndef STORAGE_AUTO_SPLIT_OB_TABLET_LOB_SPLIT_TASK_H_
#define STORAGE_AUTO_SPLIT_OB_TABLET_LOB_SPLIT_TASK_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_i_table.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/ddl/ob_complement_data_task.h"
#include "storage/ddl/ob_tablet_split_task.h"

namespace oceanbase
{

namespace storage
{

struct ObIStoreRowIteratorPtr
{
public:
  ObIStoreRowIteratorPtr() : iter_(nullptr) {}
  ObIStoreRowIteratorPtr(ObIStoreRowIterator *iter) : iter_(iter) {}
  ~ObIStoreRowIteratorPtr() = default;
  TO_STRING_KV(KP_(iter));
public:
  ObIStoreRowIterator *iter_;
};

struct ObLobIdItem final
{
public:
  ObLobIdItem() : id_(), tablet_idx_(-1) {}
  ~ObLobIdItem() {}
  int cmp(const ObLobIdItem& other) const
  {
    return MEMCMP(&id_, &other.id_, sizeof(ObLobId));
  }
  int64_t get_deep_copy_size() const
  {
    return 0;
  }
  int deep_copy(const ObLobIdItem &src, char *buf, int64_t len, int64_t &pos)
  {
    UNUSED(buf);
    UNUSED(len);
    UNUSED(pos);
    id_ = src.id_;
    tablet_idx_ = src.tablet_idx_;
    return OB_SUCCESS;
  }
  bool is_valid() const { return id_.is_valid() && tablet_idx_ >= 0; }
  void reset() {
    id_.reset();
    tablet_idx_ = -1L;
  }
  TO_STRING_KV(K_(id), K_(tablet_idx));
  NEED_SERIALIZE_AND_DESERIALIZE;
public:
  ObLobId id_;
  int64_t tablet_idx_;
};

class ObLobIdItemCompare
{
public:
  ObLobIdItemCompare(int &sort_ret) : result_code_(sort_ret) {}
  bool operator()(const ObLobIdItem *left, const ObLobIdItem *right);
public:
  int &result_code_;
};

typedef ObExternalSort<ObLobIdItem, ObLobIdItemCompare> ObLobIdMap;

struct ObLobSplitParam : public share::ObIDagInitParam
{
public:
  ObLobSplitParam() :
    rowkey_allocator_("LobSplitRowkey", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    tenant_id_(common::OB_INVALID_TENANT_ID), ls_id_(share::ObLSID::INVALID_LS_ID),
    ori_lob_meta_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    new_lob_tablet_ids_(), schema_version_(0),
    data_format_version_(0), parallelism_(0), compaction_scn_(),
    compat_mode_(lib::Worker::CompatMode::INVALID), task_id_(0),
    source_table_id_(OB_INVALID_ID), dest_schema_id_(OB_INVALID_ID), consumer_group_id_(0),
    lob_col_idxs_(), split_sstable_type_(share::ObSplitSSTableType::SPLIT_BOTH), parallel_datum_rowkey_list_(),
    min_split_start_scn_()
  {}
  virtual ~ObLobSplitParam();
  int init(const ObLobSplitParam &other);
  int init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg);
  int init(const obrpc::ObTabletSplitArg &arg);
  bool is_valid() const {
    return OB_INVALID_ID != tenant_id_ && ls_id_.is_valid() && ori_lob_meta_tablet_id_.is_valid()
           && new_lob_tablet_ids_.count() > 0 && schema_version_ > 0
           && data_format_version_ > 0 && parallelism_ > 0
           && compat_mode_ != lib::Worker::CompatMode::INVALID
           && task_id_ > 0 && OB_INVALID_ID != source_table_id_ && OB_INVALID_ID != dest_schema_id_
           && consumer_group_id_ >= 0 && lob_col_idxs_.count() > 0 && parallel_datum_rowkey_list_.count() > 0;
  }
  int assign(const ObLobSplitParam &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(ori_lob_meta_tablet_id),
    K_(new_lob_tablet_ids), K_(schema_version), K_(data_format_version),
    K_(parallelism), K_(compaction_scn), K_(compat_mode), K_(task_id),
    K_(source_table_id), K_(dest_schema_id), K_(lob_col_idxs), K_(consumer_group_id),
    K_(lob_col_idxs), K_(split_sstable_type), K_(parallel_datum_rowkey_list),
    K_(min_split_start_scn));
private:
  common::ObArenaAllocator rowkey_allocator_; // for DatumRowkey.
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTabletID ori_lob_meta_tablet_id_;
  ObArray<ObTabletID> new_lob_tablet_ids_;
  int64_t schema_version_;
  int64_t data_format_version_;
  int64_t parallelism_; // dop
  int64_t compaction_scn_;
  lib::Worker::CompatMode compat_mode_;
  int64_t task_id_;
  int64_t source_table_id_; // src_main_table_id
  int64_t dest_schema_id_; // src_lob_meta_table_id
  uint64_t consumer_group_id_;
  ObSArray<uint64_t> lob_col_idxs_;
  share::ObSplitSSTableType split_sstable_type_;
  common::ObSArray<blocksstable::ObDatumRowkey> parallel_datum_rowkey_list_; // calc by main table.
  share::SCN min_split_start_scn_;
};

struct ObLobSplitContext final
{
public:
  ObLobSplitContext() :
    range_allocator_("LobSplitRange", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    is_inited_(false), data_ret_(0), is_lob_piece_(false),
    ls_handle_(), main_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    allocator_(common::ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    main_tablet_handle_(), lob_meta_tablet_handle_(),
    m_allocator_(allocator_), new_main_tablet_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE, m_allocator_),
    new_lob_tablet_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE, m_allocator_),
    cmp_ret_(0), comparer_(cmp_ret_), total_map_(nullptr), sub_maps_(),
    skipped_split_major_keys_(), row_inserted_(0), cg_row_inserted_(0), physical_row_count_(0),
    split_scn_(), reorg_scn_(), ls_rebuild_seq_(-1),
    dst_major_snapshot_(-1)
#ifdef OB_BUILD_SHARED_STORAGE
    , ss_split_helper_(), is_data_split_executor_(false)
#endif
  {}
  ~ObLobSplitContext() { destroy(); }
  int init(const ObLobSplitParam& param);
  int init_maps(const ObLobSplitParam& param);
  inline bool is_valid() const { return is_inited_; }
  void destroy();
  TO_STRING_KV(
    K_(is_inited), K_(is_split_finish_with_meta_flag), K_(data_ret), K_(is_lob_piece),
    K_(ls_handle), K_(main_tablet_id), K_(main_tablet_handle),
    K_(lob_meta_tablet_handle), K_(new_main_tablet_ids),
    K_(new_lob_tablet_ids), KPC_(total_map), K_(sub_maps), K_(main_table_ranges),
    K_(skipped_split_major_keys), K_(row_inserted), K_(cg_row_inserted), K_(physical_row_count),
    K_(split_scn), K_(reorg_scn), K_(ls_rebuild_seq),
    K_(dst_major_snapshot)
#ifdef OB_BUILD_SHARED_STORAGE
    , K_(is_data_split_executor)
#endif
    );

private:
  int get_dst_lob_tablet_ids(const ObLobSplitParam& param);
private:
  common::ObArenaAllocator range_allocator_; // for datum range.
public:
  static const int64_t EACH_SORT_MEMORY_LIMIT = 8L * 1024L * 1024L; // 8MB
  bool is_inited_;
  bool is_split_finish_with_meta_flag_;
  int data_ret_;
  bool is_lob_piece_;
  ObLSHandle ls_handle_;
  ObTabletID main_tablet_id_;
  common::ObArenaAllocator allocator_;
  ObTabletHandle main_tablet_handle_;
  ObTabletHandle lob_meta_tablet_handle_;
  ObTableStoreIterator main_table_store_iterator_;
  ObTableStoreIterator lob_meta_table_store_iterator_;
  common::ModulePageAllocator m_allocator_;
  ObArray<ObTabletID> new_main_tablet_ids_;
  ObArray<ObTabletID> new_lob_tablet_ids_;
  int cmp_ret_;
  ObLobIdItemCompare comparer_;
  ObLobIdMap* total_map_;
  ObArray<ObLobIdMap*> sub_maps_;
  ObArray<ObDatumRange> main_table_ranges_;
  ObArray<ObITable::TableKey> skipped_split_major_keys_;
  int64_t row_inserted_;
  int64_t cg_row_inserted_; // unused
  int64_t physical_row_count_;
  share::SCN split_scn_;
  share::SCN reorg_scn_;
  int64_t ls_rebuild_seq_;
  int64_t dst_major_snapshot_;
#ifdef OB_BUILD_SHARED_STORAGE
  ObSSDataSplitHelper ss_split_helper_;
  bool is_data_split_executor_;
#endif
};


class ObTabletLobSplitDag final : public ObIDataSplitDag
{
public:
  ObTabletLobSplitDag();
  virtual ~ObTabletLobSplitDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual uint64_t hash() const override;
  bool operator ==(const share::ObIDag &other) const;
  bool is_inited() const { return is_inited_; }
  ObLobSplitParam &get_param() { return param_; }
  ObLobSplitContext &get_context() { return context_; }
  void handle_init_failed_ret_code(int ret) { context_.data_ret_ = ret; }
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return param_.compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual int create_first_task() override;
  virtual bool ignore_warning() override;
  virtual bool is_ha_dag() const override { return false; }
  // report lob tablet split status to RS.
  virtual int report_replica_build_status() const override;
  virtual int get_complement_data_ret() const override {
    return context_.data_ret_;
  }
  virtual void set_complement_data_ret(const int ret_code) override {
    context_.data_ret_ = OB_SUCCESS == context_.data_ret_ ?
      ret_code : context_.data_ret_;
  }
  int calc_total_row_count();
private:
  bool is_inited_;
  ObLobSplitParam param_;
  ObLobSplitContext context_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletLobSplitDag);
};


class ObTabletLobBuildMapTask : public share::ObITask
{
public:
  ObTabletLobBuildMapTask();
  virtual ~ObTabletLobBuildMapTask();
  int init(const int64_t task_id, ObLobSplitParam &param, ObLobSplitContext &ctx);
  virtual int process() override;
private:
  // int prepare_context();
  int generate_next_task(ObITask *&next_task);
  int build_sorted_map(ObIArray<ObRowScan*>& iters);
private:
  bool is_inited_;
  int64_t task_id_;
  int64_t rk_cnt_;
  ObLobSplitParam *param_;
  ObLobSplitContext *ctx_;
  common::ObArenaAllocator allocator_;
};

class ObTabletLobMergeMapTask : public share::ObITask
{
public:
  ObTabletLobMergeMapTask();
  virtual ~ObTabletLobMergeMapTask();
  int init(ObLobSplitParam &param, ObLobSplitContext &ctx);
  virtual int process() override;
private:
#ifdef OB_BUILD_SHARED_STORAGE
  int prepare_split_helper();
#endif
private:
  static const int64_t SORT_MEMORY_LIMIT = 32L * 1024L * 1024L;
  bool is_inited_;
  ObLobSplitParam *param_;
  ObLobSplitContext *ctx_;
};

struct ObTabletLobWriteSSTableCtx final
{
public:
  ObTabletLobWriteSSTableCtx();
  ~ObTabletLobWriteSSTableCtx();
  int init(
    const ObSSTable &org_sstable,
    const ObStorageSchema &storage_schema,
    const int64_t major_snapshot_version,
    const int64_t sstable_index);
  int assign(const ObTabletLobWriteSSTableCtx &other);
  int64_t get_version() const { return table_key_.is_major_sstable() ? dst_major_snapshot_version_ : table_key_.get_end_scn().get_val_for_tx(); }
  bool is_valid() const {
    return table_key_.is_valid() && data_seq_ > -1 && meta_.is_valid() && dst_major_snapshot_version_ >= 0
      && sstable_index_ >= 0; }
  TO_STRING_KV(K_(table_key), K_(data_seq), K_(merge_type),
    K_(meta), K_(dst_uncommitted_tx_id_arr), K_(dst_major_snapshot_version),
    K_(sstable_index));
public:
  ObITable::TableKey table_key_;
  int64_t data_seq_;
  compaction::ObMergeType merge_type_;
  ObSSTableBasicMeta meta_; // for major split, it's src lob tablet's last major sstable's meta with MODIFICATION
  ObArray<int64_t> dst_uncommitted_tx_id_arr_; // first uncommitted row's tx id for each split dst minor sstable
  int64_t dst_major_snapshot_version_;
  int64_t sstable_index_; // the index of the sstable in minors or major.
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletLobWriteSSTableCtx);
};

class ObTabletLobWriteDataTask : public share::ObITask
{
public:
  ObTabletLobWriteDataTask();
  virtual ~ObTabletLobWriteDataTask();
  int init(const int64_t task_id, ObLobSplitParam &param, ObLobSplitContext &ctx);
  virtual int process() override;
private:
  int generate_next_task(ObITask *&next_task);
  int prepare_sstable_writers_and_builders(const ObStorageSchema &storage_schema,
                                           ObArrayArray<ObWholeDataStoreDesc>& data_desc_arr,
                                           ObArrayArray<ObMacroBlockWriter*>& slice_writers,
                                           ObArrayArray<ObSSTableIndexBuilder*>& index_builders);
  int prepare_sstable_writer(const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                             const int64_t dest_tablet_index/*tablet index in ctx.new_lob_tablet_ids_*/,
                             const ObStorageSchema &storage_schema,
                             ObWholeDataStoreDesc &data_desc,
                             ObMacroBlockWriter *&slice_writer,
                             ObSSTableIndexBuilder *&index_builder);
  int prepare_macro_seq_param(
      const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
      const int64_t dest_tablet_index,
      ObMacroSeqParam &macro_seq_param);
  int prepare_sstable_macro_writer(const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                                   const int64_t dest_tablet_index,
                                   const ObStorageSchema &storage_schema,
                                   ObWholeDataStoreDesc &data_desc,
                                   ObSSTableIndexBuilder *index_builder,
                                   ObMacroBlockWriter *&slice_writer);
  int prepare_sstable_index_builder(const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                                    const ObTabletID &new_tablet_id,
                                    const ObStorageSchema &storage_schema,
                                    ObWholeDataStoreDesc &data_desc,
                                    ObSSTableIndexBuilder *&index_builder);
  int dispatch_rows(ObIArray<ObIStoreRowIteratorPtr>& iters,
                    ObArrayArray<ObMacroBlockWriter*>& slice_writers);
  int create_sstables(ObArrayArray<ObSSTableIndexBuilder*>& index_builders,
                      const share::ObSplitSSTableType split_sstable_type);
  int create_empty_sstable(const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                           const ObTabletID &new_tablet_id,
                           const share::SCN &end_scn,
                           ObTableHandleV2 &new_table_handle);
  int create_sstable(ObSSTableIndexBuilder *sstable_index_builder,
                     const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                     const int64_t tablet_idx,
                     ObTableHandleV2 &new_table_handle);
  void release_slice_writer(ObMacroBlockWriter *&slice_writer);
  void release_index_builder(ObSSTableIndexBuilder *&index_builder);
  void release_sstable_writers_and_builders(ObArrayArray<ObMacroBlockWriter*>& slice_writers,
                                            ObArrayArray<ObSSTableIndexBuilder*>& index_builders);
  int check_and_create_mds_sstable(
      const ObTabletID &dest_tablet_id);
#ifdef OB_BUILD_SHARED_STORAGE
  int close_ss_index_builder(
      const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
      const int64_t dest_tablet_index, // index in ctx.dest_tablets_id.
      ObSSTableIndexBuilder *index_builder,
      ObSSTableMergeRes &res);
#endif
private:
  common::ObArenaAllocator allocator_;
  bool is_inited_;
  int64_t task_id_;
  int64_t rk_cnt_;
  ObArray<ObTabletLobWriteSSTableCtx> write_sstable_ctx_array_;
  ObLobSplitParam *param_;
  ObLobSplitContext *ctx_;
};

class ObTabletLobSplitUtil final
{
public:
  static int open_rowscan_iters(const share::ObSplitSSTableType &split_sstable_type,
                                ObIAllocator &allocator,
                                int64_t table_id,
                                const ObTabletHandle &tablet_handle,
                                const ObTableStoreIterator &table_store_iterator,
                                const ObDatumRange &query_range,
                                const ObStorageSchema &main_table_storage_schema,
                                ObIArray<ObRowScan*> &iters);
  static int open_uncommitted_scan_iters(ObLobSplitParam *param,
                                         ObLobSplitContext *ctx,
                                         int64_t table_id,
                                         const ObTabletHandle &tablet_handle,
                                         const ObTableStoreIterator &table_iter,
                                         const ObDatumRange &query_range,
                                         const int64_t major_snapshot_version,
                                         ObIArray<ObIStoreRowIteratorPtr> &iters,
                                         ObIArray<ObTabletLobWriteSSTableCtx> &write_sstable_ctx_array);
  static int open_snapshot_scan_iters(ObLobSplitParam *param,
                                      ObLobSplitContext *ctx,
                                      int64_t table_id,
                                      const ObTabletHandle &tablet_handle,
                                      const ObTableStoreIterator &table_iter,
                                      const ObDatumRange &query_range,
                                      const int64_t major_snapshot_version,
                                      ObIArray<ObIStoreRowIteratorPtr> &iters,
                                      ObIArray<ObTabletLobWriteSSTableCtx> &write_sstable_ctx_array);
  static int generate_col_param(const ObMergeSchema *schema,
                                int64_t& rk_cnt);

  static int process_write_split_start_log_request(
      const ObTabletSplitArg &arg,
      share::SCN &scn);
  static int process_tablet_split_request(
      const bool is_lob_tablet,
      const bool is_start_request,
      const void *request_arg,
      void *request_res);
  static int write_split_log(
      const bool is_lob_tablet,
      const bool is_start_request,
      const share::ObLSID &ls_id,
      const share::ObIDagInitParam *input_param,
      share::SCN &scn);
};

class ObSingleRowIterWrapper: public ObIStoreRowIterator
{
public:
  ObSingleRowIterWrapper() : row_(nullptr), iter_end_(false) {}
  ObSingleRowIterWrapper(const blocksstable::ObDatumRow *row) : row_(row), iter_end_(false) {}
  virtual ~ObSingleRowIterWrapper() {}

  void set_row(const blocksstable::ObDatumRow *row) { row_ = row; }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row);
  virtual void reset() { iter_end_ = false; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSingleRowIterWrapper);
private:
  // data members
  const blocksstable::ObDatumRow *row_;
  bool iter_end_;
};

inline int ObSingleRowIterWrapper::get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif
