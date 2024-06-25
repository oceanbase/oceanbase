//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_BASIC_TABLET_MERGE_CTX_H_
#define OB_STORAGE_COMPACTION_BASIC_TABLET_MERGE_CTX_H_
#include "storage/compaction/ob_tablet_merge_info.h"
#include "storage/compaction/ob_partition_parallel_merge_ctx.h"
namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}
namespace compaction
{
struct ObStaticMergeParam final
{
  ObStaticMergeParam(ObTabletMergeDagParam &dag_param);
  ~ObStaticMergeParam() { reset(); }
  bool is_valid() const;
  void reset();
  int init_static_info(
    const int64_t concurrent_cnt,
    ObTabletHandle &tablet_handle);
  int init_co_major_merge_params();
  int init_sstable_logic_seq();
  int get_basic_info_from_result(const ObGetMergeTablesResult &get_merge_table_result);
  int64_t get_compaction_scn() const {
    return is_multi_version_merge(get_merge_type()) ?
      scn_range_.end_scn_.get_val_for_tx() : version_range_.snapshot_version_;
  }
  OB_INLINE ObMergeType get_merge_type() const { return dag_param_.merge_type_; }
  OB_INLINE const ObLSID &get_ls_id() const { return dag_param_.ls_id_; }
  OB_INLINE const ObTabletID &get_tablet_id() const { return dag_param_.tablet_id_; }

  int cal_minor_merge_param(const bool has_compaction_filter);
  int cal_major_merge_param();
  bool is_build_row_store_from_rowkey_cg() const;
  bool is_build_row_store() const;
  OB_INLINE void set_full_merge_and_level(bool is_full_merge)
  {
    progressive_merge_num_ = 0;
    is_full_merge_ = is_full_merge;
    merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  }
private:
  int init_multi_version_column_descs();

public:
  TO_STRING_KV(K_(dag_param), K_(scn_range), K_(version_range),
      K_(is_full_merge), K_(concurrent_cnt), K_(merge_level), K_(major_sstable_status),
      "merge_reason", ObAdaptiveMergePolicy::merge_reason_to_str(merge_reason_),
      "co_major_merge_type_", ObCOMajorMergePolicy::co_major_merge_type_to_str(co_major_merge_type_),
      K_(sstable_logic_seq), K_(tables_handle), K_(is_rebuild_column_store), K_(is_schema_changed), K_(is_tenant_major_merge),
      K_(read_base_version), K_(merge_scn), K_(need_parallel_minor_merge),
      K_(progressive_merge_round), K_(progressive_merge_step), K_(progressive_merge_num),
      K_(schema_version), KP_(schema), K_(multi_version_column_descs), K_(ls_handle), K_(snapshot_info), KP_(report), K_(is_backfill));

  ObTabletMergeDagParam &dag_param_;
  bool is_full_merge_; // full merge or increment merge
  bool is_rebuild_column_store_;
  bool is_schema_changed_;
  bool need_parallel_minor_merge_;
  bool is_tenant_major_merge_;
  ObMergeLevel merge_level_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason_;
  ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type_;
  ObCOMajorSSTableStatus major_sstable_status_; // The type of major sstable, may mismatch with table schema (such as no normal col cg)
  int16_t sstable_logic_seq_;
  storage::ObLSHandle ls_handle_;
  storage::ObTablesHandleArray tables_handle_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_num_;
  int64_t progressive_merge_step_;
  int64_t concurrent_cnt_;
  int64_t data_version_; // for major, get from medium_info
  int64_t ls_rebuild_seq_;
  int64_t read_base_version_; // use for major merge
  int64_t create_snapshot_version_;
  int64_t start_time_;
  share::SCN merge_scn_;
  ObVersionRange version_range_;
  share::ObScnRange scn_range_;
  const ObRowkeyReadInfo *rowkey_read_info_;
  int64_t schema_version_;
  const ObStorageSchema *schema_;
  observer::ObIMetaReport *report_;
  ObStorageSnapshotInfo snapshot_info_;
  int64_t tx_id_;
  common::ObSEArray<share::schema::ObColDesc, 2 * OB_ROW_DEFAULT_COLUMNS_COUNT> multi_version_column_descs_;
  bool is_backfill_;
  DISALLOW_COPY_AND_ASSIGN(ObStaticMergeParam);
};

struct ObBasicTabletMergeCtx;
struct ObCtxMergeInfoCollector final
{
  void prepare(ObBasicTabletMergeCtx &ctx);
  void finish(ObTabletMergeInfo &merge_info);
  void destroy(ObCompactionMemoryContext &merge_ctx);
  TO_STRING_KV(KP_(merge_progress), K_(time_guard), K_(error_location));
public:
  ObPartitionMergeProgress *merge_progress_;
  ObICompactionFilter *compaction_filter_;
  // for row_store, record all event
  // for columnar_store, record event in prepare/finish stage
  // time_guard on exeDag will record event in execute stage
  ObStorageCompactionTimeGuard time_guard_;
  ObTransNodeDMLStat tnode_stat_; // collect trans node dml stat on memtable, only worked in mini compaction.
  share::ObDiagnoseLocation error_location_;
};

struct ObBasicTabletMergeCtx
{
public:
  ObBasicTabletMergeCtx(
    ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator);
  virtual ~ObBasicTabletMergeCtx() { destroy(); }
  bool is_valid() const;
  bool is_schema_valid() const
  {
    return nullptr != get_schema() && get_schema()->is_valid();
  }
  void destroy();
  /* PREPARE SECTION */
  virtual int build_ctx(bool &finish_flag);
  int build_ctx_after_init();
  int prepare_merge_progress(
    ObPartitionMergeProgress *&progress,
    ObTabletMergeDag *merge_dag = nullptr,
    const uint32_t start_cg_idx = 0,
    const uint32_t end_cg_idx = 0);
  int build_index_tree(
    ObTabletMergeInfo &merge_info,
    const ObITableReadInfo *index_read_info,
    const storage::ObStorageColumnGroupSchema *cg_schema = nullptr,
    const uint16_t table_cg_idx = 0);
  virtual int get_ls_and_tablet();
  void init_time_guard(const int64_t time) {
    info_collector_.time_guard_.set_last_click_ts(time);
  }
  /* EXECUTE SECTION */
  void time_guard_click(const uint16_t event)
  {
    info_collector_.time_guard_.click(event);
  }
  int get_merge_range(int64_t parallel_idx, blocksstable::ObDatumRange &merge_range);
  bool has_filter() const { return nullptr != info_collector_.compaction_filter_; }
  OB_INLINE int filter(const blocksstable::ObDatumRow &row, ObICompactionFilter::ObFilterRet &filter_ret)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(info_collector_.compaction_filter_)) {
      ret = info_collector_.compaction_filter_->filter(row, filter_ret);
    }
    return ret;
  }
  /* FINISH SECTION */
  virtual int update_tablet(
    const blocksstable::ObSSTable &sstable,
    ObTabletHandle &new_tablet_handle);
  int try_set_upper_trans_version(blocksstable::ObSSTable &sstable);
  int update_tablet_after_merge();
  ObITable::TableType get_merged_table_type(
    const ObStorageColumnGroupSchema *cg_schema,
    const bool is_main_table) const;

  OB_INLINE void collect_tnode_dml_stat(const ObTransNodeDMLStat tnode_stat);
  void add_sstable_merge_info(ObSSTableMergeInfo &merge_info,
                              const share::ObDagId &dag_id,
                              const int64_t hash,
                              const ObCompactionTimeGuard &time_guard,
                              const blocksstable::ObSSTable *sstable = nullptr,
                              const ObStorageSnapshotInfo *snapshot_info = nullptr,
                              const int64_t start_cg_idx = 0,
                              const int64_t end_cg_idx = 0);
  int generate_participant_table_info(PartTableInfo &info) const;
  int generate_macro_id_list(char *buf, const int64_t buf_len, const blocksstable::ObSSTable *&sstable) const;
  /* GET FUNC */
  #define CTX_DEFINE_FUNC(var_type, param, var_name) \
    OB_INLINE var_type get_##var_name() const { return param. var_name##_; }
  #define DAG_PARAM_FUNC(var_type, var_name) \
    CTX_DEFINE_FUNC(var_type, get_dag_param(), var_name)
  #define STATIC_PARAM_FUNC(var_type, var_name) \
    CTX_DEFINE_FUNC(var_type, static_param_, var_name)
  DAG_PARAM_FUNC(ObMergeType, merge_type);
  DAG_PARAM_FUNC(const ObLSID &, ls_id);
  DAG_PARAM_FUNC(const ObTabletID &, tablet_id);
  DAG_PARAM_FUNC(int64_t, merge_version);
  DAG_PARAM_FUNC(int64_t, transfer_seq);
  STATIC_PARAM_FUNC(bool, is_tenant_major_merge);
  STATIC_PARAM_FUNC(bool, is_full_merge);
  STATIC_PARAM_FUNC(bool, need_parallel_minor_merge);
  STATIC_PARAM_FUNC(int64_t, read_base_version);
  STATIC_PARAM_FUNC(int64_t, ls_rebuild_seq);
  STATIC_PARAM_FUNC(const storage::ObTablesHandleArray &, tables_handle);
  STATIC_PARAM_FUNC(const ObTabletMergeDagParam &, dag_param);
  STATIC_PARAM_FUNC(const SCN &, merge_scn);
  OB_INLINE int64_t get_concurrent_cnt() const { return parallel_merge_ctx_.get_concurrent_cnt(); }
  OB_INLINE ObMergeType get_inner_table_merge_type() const { return get_is_tenant_major_merge() ? MAJOR_MERGE : get_merge_type(); }
  OB_INLINE const ObStorageSchema *get_schema() const { return static_param_.schema_; }
  OB_INLINE ObLS *get_ls() const { return static_param_.ls_handle_.get_ls(); }
  ObTablet *get_tablet() const { return tablet_handle_.get_obj(); }
  OB_INLINE int64_t get_snapshot() const { return static_param_.version_range_.snapshot_version_; }
  int get_storage_schema();
  int update_storage_schema_by_memtable(
    const ObStorageSchema &schema_on_tablet,
    const ObTablesHandleArray &merge_tables_handle);
  static bool need_swap_tablet(ObProtectedMemtableMgrHandle &memtable_mgr_handle, const int64_t row_count, const int64_t macro_count);
  VIRTUAL_TO_STRING_KV(K_(static_param), K_(static_desc), K_(parallel_merge_ctx), K_(tablet_handle),
    K_(info_collector), KP_(merge_dag));
protected:
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result);
  virtual int try_swap_tablet(ObGetMergeTablesResult &get_merge_table_result)
  {
    UNUSED(get_merge_table_result);
    return OB_SUCCESS;
  }
  virtual int prepare_schema();
  virtual void free_schema();
  virtual int cal_merge_param() { return static_param_.cal_minor_merge_param(false/*has_compaction_filter*/); }
  int init_parallel_merge_ctx();
  int init_static_param_and_desc();
  int init_read_info();
  virtual int init_tablet_merge_info(const bool need_check = true) = 0;
  virtual int prepare_index_tree() = 0;
  void build_update_table_store_param(
    const blocksstable::ObSSTable *sstable,
    ObUpdateTableStoreParam &param);
  virtual void update_and_analyze_progress() {}
  virtual int create_sstable(const blocksstable::ObSSTable *&new_sstable) = 0;
  OB_INLINE int get_schema_info_from_tables(
    const ObTablesHandleArray &merge_tables_handle,
    const int64_t column_cnt_in_schema,
    int64_t &max_column_cnt_in_memtable,
    int64_t &max_schema_version_in_memtable);
  virtual int update_tablet_directly(ObGetMergeTablesResult &get_merge_table_result)
  {
    UNUSED(get_merge_table_result);
    return OB_NOT_SUPPORTED;
  }
  void after_update_tablet_for_major();
  virtual int collect_running_info() = 0;
  int swap_tablet();
  int get_medium_compaction_info(); // for major
  int swap_tablet(ObGetMergeTablesResult &get_merge_table_result); // for major
  int get_meta_compaction_info(); // for meta major
  static const int64_t LARGE_VOLUME_DATA_ROW_COUNT_THREASHOLD = 1000L * 1000L; // 100w
  static const int64_t LARGE_VOLUME_DATA_MACRO_COUNT_THREASHOLD = 300L;
public:
  ObCompactionMemoryContext mem_ctx_;
  ObStaticMergeParam static_param_;
  blocksstable::ObStaticDataStoreDesc static_desc_;
  ObTabletHandle tablet_handle_;
  ObParallelMergeCtx parallel_merge_ctx_;
  ObTabletMergeDag *merge_dag_;
  ObCtxMergeInfoCollector info_collector_;
  ObRowkeyReadInfo read_info_; // moved from ObCOMerger, avoid constructing each time
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_BASIC_TABLET_MERGE_CTX_H_
