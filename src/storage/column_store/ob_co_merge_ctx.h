//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_CO_MERGE_CTX_H_
#define OB_STORAGE_CO_MERGE_CTX_H_
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/blocksstable/ob_major_checksum_info.h"
#include "storage/column_store/ob_co_merge_log.h"
namespace oceanbase
{
namespace compaction
{

class ObCOMergeDagNet;

// TODO(@jingshui)
class ObCOMergeTwoStageCtx
{
public:
  ObCOMergeTwoStageCtx()
    : mgr_count_(0),
      file_block_size_(ObCOMergeLogFile::BLOCK_SIZE),
      mem_ctx_(nullptr),
      mgr_array_(nullptr)
  {}
  virtual ~ObCOMergeTwoStageCtx() { reset(); }
  int init(ObCompactionMemoryContext &mem_ctx, const int64_t concurrent_cnt);
  int init_mgr(const int64_t task_id, ObBasicTabletMergeCtx &ctx);
  int destroy_mgr(const int64_t task_id);
  int get_mgr(const int64_t idx, ObCOMergeLogFileMgr *&mgr);
  int close_files(const int64_t task_id, const int64_t start_cg_idx, const int64_t end_cg_idx);
  // for unittest now
  OB_INLINE void set_file_block_size(const int64_t block_size) { file_block_size_ = block_size; }
  void reset();
  bool is_valid();

private:
  int64_t mgr_count_;
  int64_t file_block_size_;
  ObCompactionMemoryContext *mem_ctx_;
  ObCOMergeLogFileMgr **mgr_array_; // each range a mgr // build/replay two stages share a mgr
};

struct ObCOTabletMergeCtx : public ObBasicTabletMergeCtx
{
  enum MergeLogStorage : uint8_t
  {
    NORMAL = 0,
    ROW_TMP_FILE,
    COLUMN_TMP_FILE
  };

  ObCOTabletMergeCtx(
    ObIDagNet &dag_net,
    ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator);
  ~ObCOTabletMergeCtx();
  void destroy();
  /* override from ObBasicTabletMergeCtx */
  virtual int build_ctx(bool &finish_flag) override;
  virtual int prepare_schema() override;
  virtual int init_tablet_merge_info() override;
  virtual int cal_merge_param() override;
  virtual int prepare_compaction_filter() override
  { return alloc_mds_info_compaction_filter(); }
  virtual int prepare_index_tree() override { return OB_SUCCESS; }
  virtual int collect_running_info() override;
  virtual int check_merge_ctx_valid() override;
  virtual int try_swap_tablet(ObGetMergeTablesResult &get_merge_table_result) override
  { return ObBasicTabletMergeCtx::swap_tablet(get_merge_table_result); }

  /* PREPARE SECTION */
  int prepare_row_store_cg_schema();
  int construct_column_param(
      const uint64_t column_id,
      const ObStorageColumnSchema *column_schema,
      ObColumnParam &column_param);
  int mock_row_store_table_read_info();
  int prepare_mocked_row_store_cg_schema();
  bool should_mock_row_store_cg_schema();
  int prepare_cs_replica_param(const ObMediumCompactionInfo *medium_info);
  int handle_alter_cg_delayed_in_cs_replica();

  int prepare_index_builder(const int64_t start_cg_idx, const int64_t end_cg_idx, const bool reuse_merge_info = true);
  int inner_loop_prepare_index_tree(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx);
  // only used for ObCOMergeExeDag
  bool is_cg_merge_infos_valid(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const bool check_info_ready) const;

  int init_merge_flag();
  int prepare_two_stage_ctx();
  // init merge log mgr when you really need it, cause init merge log mgr will open tmp file
  int init_merge_log_mgr(const int64_t task_id);
  int destroy_merge_log_mgr(const int64_t task_id);
  int check_prefer_reuse_macro_block();

  /* EXECUTE SECTION */
  int check_need_schedule_minor(bool &schedule_minor) const;
  int schedule_minor_errsim(bool &schedule_minor) const;
  void update_execute_time(const int64_t cost_time);
  int do_replay_finish(const int64_t idx, const int64_t start_cg_idx, const int64_t end_cg_idx);

  /* FINISH SECTION */
  int create_sstable(const blocksstable::ObSSTable *&new_sstable);
  int create_cg_sstable(const int64_t cg_idx);
  int inner_add_cg_sstables(const ObSSTable *&new_sstable);
  int validate_column_checksums(
      int64_t *all_column_cksums,
      const int64_t all_column_cnt,
      const common::ObIArray<ObStorageColumnGroupSchema> &cg_schemas);
  int check_convert_co_checksum(const ObSSTable *new_sstable);
  int push_table_handle(ObTableHandleV2 &table_handle, int64_t &count);
  int collect_cg_running_info(const int64_t cg_idx);

  /* UTILITY FUNC */
  const ObITableReadInfo *get_full_read_info() const
  {
    const ObITableReadInfo *ret_info = NULL;
    if (need_mock_co_read_info()) {
      ret_info = &mocked_row_store_table_read_info_;
    } else {
      ret_info = &read_info_;
    }
    return ret_info;
  }
  void destroy_merge_info_array(
      const uint32_t start_cg_idx,
      const uint32_t end_cg_idx,
      const bool release_mem_flag);
  void destroy_merge_info(const uint32_t cg_idx, const bool release_mem_flag);

  OB_INLINE bool is_build_row_store_from_rowkey_cg() const { return static_param_.co_static_param_.is_build_row_store_from_rowkey_cg_; }
  OB_INLINE bool is_build_redundant_row_store_from_rowkey_cg() const { return static_param_.co_static_param_.is_build_redundent_row_store_from_rowkey_cg_; }
  OB_INLINE bool is_build_row_store() const { return static_param_.is_build_row_store(); }
  OB_INLINE bool is_rebuild_column_store_with_rowkey_cg() const
  {
    return static_param_.co_static_param_.is_rebuild_column_store_ &&
           static_param_.co_static_param_.contain_rowkey_base_co_sstable_;
  }
  OB_INLINE bool contain_rowkey_base_co_sstable() const { return static_param_.co_static_param_.contain_rowkey_base_co_sstable_; }
  OB_INLINE bool need_mock_co_read_info() const
  {
    return is_build_row_store_from_rowkey_cg() ||
           is_rebuild_column_store_with_rowkey_cg() ||
           is_build_redundant_row_store_from_rowkey_cg();
  }
  OB_INLINE bool is_using_column_tmp_file() { return MergeLogStorage::COLUMN_TMP_FILE == merge_log_storage_; }
  OB_INLINE bool is_using_row_tmp_file() { return MergeLogStorage::ROW_TMP_FILE == merge_log_storage_; }
  OB_INLINE bool is_using_tmp_file()
  {
    return MergeLogStorage::COLUMN_TMP_FILE == merge_log_storage_ ||
           MergeLogStorage::ROW_TMP_FILE == merge_log_storage_;
  }
  int get_cg_schema_for_merge(const int64_t idx, const ObStorageColumnGroupSchema *&cg_schema_ptr);
  int get_merge_log_mgr(const int64_t idx, ObCOMergeLogFileMgr *&mgr);

  INHERIT_TO_STRING_KV("ObCOTabletMergeCtx", ObBasicTabletMergeCtx,
      K_(merge_flag), K_(array_count), K_(retry_cnt), K_(prefer_reuse_macro_block), K_(base_rowkey_cg_idx));

  /* STATIC CONST */
  static const int64_t SCHEDULE_MINOR_CG_CNT_THREASHOLD = 20;
  static const int64_t SCHEDULE_MINOR_TABLE_CNT_THREASHOLD = 3;
  static const int64_t SCHEDULE_MINOR_ROW_CNT_THREASHOLD = 100 * 1000L;
  // every DAG_NET_ERROR_COUNT_THREASHOLD failure we hope finished exe_dag counts grow EXE_DAG_FINISH_GROWTH_RATIO
  static const int64_t DAG_NET_ERROR_COUNT_THREASHOLD = 10;
  static constexpr double EXE_DAG_FINISH_GROWTH_RATIO = 0.1;
  // when finished exe_dag counts reach EXE_DAG_FINISH_UP_RATIO, we allow up to DAG_NET_ERROR_COUNT_UP_THREASHOLD dag failures
  static constexpr double EXE_DAG_FINISH_UP_RATIO = 0.6;
  static const int64_t DAG_NET_ERROR_COUNT_UP_THREASHOLD = 2000;
  static const int64_t PREFER_REUSE_MACRO_BLOCKS_RAITO = 2;

  /* MEMBER VARIABLE */
  union {
    struct {
      // whether all/rowkey cg replay directly
      uint16_t need_replay_base_directly_ : 1;
      // whether use tmp file to save merge log
      // 0: no tmp file
      // 1: row store tmp file
      // 2: column store tmp file
      uint16_t merge_log_storage_ : 2;
      uint16_t reserved_ : 13;
    };
    uint16_t merge_flag_;
  };
  int64_t array_count_; // equal to cg count
  int64_t start_schedule_cg_idx_;
  int64_t base_rowkey_cg_idx_;
  int64_t retry_cnt_;
  bool prefer_reuse_macro_block_;
  ObIDagNet &dag_net_;
  ObTabletMergeInfo **cg_merge_info_array_;
  ObITable **merged_sstable_array_;
  lib::ObMutex ctx_lock_;
  // store merged cg major sstables for random order, just hold table ref
  storage::ObTablesHandleArray merged_cg_tables_handle_;
  ObStorageColumnGroupSchema mocked_row_store_cg_;
/*
  if schema is pure col(EACH CG) but need to output row_store,
  OLD_MAJOR have row_store as CO, could use read_info from tablet to read full row
  OLD_MAJOR is pure col, need mock one row_store read_info to read row from pure_col(use query interface)
*/
  ObTableReadInfo mocked_row_store_table_read_info_; // read info for merge from col store to row store
  ObSSTableMergeHistory dag_net_merge_history_; // record info for dag net
  ObCOMergeTwoStageCtx *two_stage_ctx_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_CO_MERGE_CTX_H_
