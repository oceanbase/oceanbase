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
namespace oceanbase
{
namespace compaction
{

class ObCOMergeDagNet;
struct ObCOMergeExeStat{
  ObCOMergeExeStat()
    : error_count_(0),
      period_error_count_(0),
      period_finish_count_(0),
      finish_count_(0)
  {}
  ~ObCOMergeExeStat() {}
  TO_STRING_KV(K_(error_count), K_(period_error_count), K_(period_finish_count), K_(finish_count));
  int16_t error_count_;
  int16_t period_error_count_;
  int32_t period_finish_count_;
  int64_t finish_count_;
};

struct ObCOTabletMergeCtx : public ObBasicTabletMergeCtx
{
  enum CGScheduleStatus : uint8_t
  {
    CG_SCHE_STATUS_IDLE = 0,
    CG_SCHE_STATUS_FINISHED,
    CG_SCHE_STATUS_CREATED,
    CG_SCHE_STATUS_SCHEDULED,
    CG_SCHE_STATUS_FAILED,
    CG_SCHE_STATUS_MAX,
  };
  static bool is_cg_could_schedule(const CGScheduleStatus status)
  {
    return CG_SCHE_STATUS_IDLE == status || CG_SCHE_STATUS_FAILED == status;
  }
  bool is_co_dag_net_failed();

  ObCOTabletMergeCtx(
    ObCOMergeDagNet &dag_net,
    ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator);
  ~ObCOTabletMergeCtx();
  void destroy();
  virtual int prepare_schema() override;
  virtual int cal_merge_param() override { return static_param_.cal_major_merge_param(); }
  virtual int init_tablet_merge_info(const bool need_check = true) override;
  virtual int prepare_index_tree() override { return OB_SUCCESS; }
  virtual int collect_running_info() override;
  virtual int build_ctx(bool &finish_flag) override;
  OB_INLINE bool all_cg_finish() const // locked by ObCODagNet ctx_lock_
  {
    return exe_stat_.finish_count_ == array_count_;
  }
  OB_INLINE void one_batch_finish(const int64_t cg_cnt)
  {
    exe_stat_.finish_count_ += cg_cnt;
    exe_stat_.period_finish_count_ += cg_cnt;
  }
  OB_INLINE void one_batch_fail()
  {
    ++exe_stat_.error_count_;
    ++exe_stat_.period_error_count_;
  }
  int create_sstable(const blocksstable::ObSSTable *&new_sstable);
  int prepare_index_builder(
      const uint32_t start_cg_idx,
      const uint32_t end_cg_idx,
      const bool is_retry_create = false);
  int create_sstables(const uint32_t start_cg_idx, const uint32_t end_cg_idx);
  int inner_add_cg_sstables(const ObSSTable *&new_sstable);
  int validate_column_checksums(
      int64_t *all_column_cksums,
      const int64_t all_column_cnt,
      const common::ObIArray<ObStorageColumnGroupSchema> &cg_schemas);
  int push_table_handle(ObTableHandleV2 &table_handle, int64_t &count);
  int revert_pushed_table_handle(
    const int64_t start_cg_idx,
    const int64_t right_border_cg_idx,
    const int64_t exist_cg_tables_cnt);
  void destroy_merge_info_array(
      const uint32_t start_cg_idx,
      const uint32_t end_cg_idx,
      const bool release_mem_flag = true);
  int check_need_schedule_minor(bool &schedule_minor) const;
  int collect_running_info(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const int64_t hash,
    const share::ObDagId &dag_id,
    const ObCompactionTimeGuard &time_guard);
  int schedule_minor_errsim(bool &schedule_minor) const;
  // only used for ObCOMergeBatchExeDag
  bool is_cg_merge_infos_valid(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const bool check_info_ready) const;
  int init_major_sstable_status();
  int construct_column_param(
      const uint64_t column_id,
      const ObStorageColumnSchema *column_schema,
      ObColumnParam &column_param);
  int init_table_read_info();
  int inner_loop_prepare_index_tree(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx);
  virtual int try_swap_tablet(ObGetMergeTablesResult &get_merge_table_result) override
  { return ObBasicTabletMergeCtx::swap_tablet(get_merge_table_result); }
  int prepare_mocked_row_store_cg_schema();
  bool should_mock_row_store_cg_schema();
  OB_INLINE bool is_build_row_store_from_rowkey_cg() const { return static_param_.is_build_row_store_from_rowkey_cg(); }
  OB_INLINE bool is_build_row_store() const { return static_param_.is_build_row_store(); }
  int get_cg_schema_for_merge(const int64_t idx, const ObStorageColumnGroupSchema *&cg_schema_ptr);
  INHERIT_TO_STRING_KV("ObCOTabletMergeCtx", ObBasicTabletMergeCtx,
      K_(array_count), K_(exe_stat));

  static const int64_t DEFAULT_CG_MERGE_BATCH_SIZE = 10;
  static const int64_t SCHEDULE_MINOR_CG_CNT_THREASHOLD = 20;
  static const int64_t SCHEDULE_MINOR_TABLE_CNT_THREASHOLD = 3;
  static const int64_t SCHEDULE_MINOR_ROW_CNT_THREASHOLD = 100 * 1000L;
  // every DAG_NET_ERROR_COUNT_THREASHOLD failure we hope finished exe_dag counts grow EXE_DAG_FINISH_GROWTH_RATIO
  static const int64_t DAG_NET_ERROR_COUNT_THREASHOLD = 10;
  static constexpr double EXE_DAG_FINISH_GROWTH_RATIO = 0.1;
  // when finished exe_dag counts reach EXE_DAG_FINISH_UP_RATIO, we allow up to DAG_NET_ERROR_COUNT_UP_THREASHOLD dag failures
  static constexpr double EXE_DAG_FINISH_UP_RATIO = 0.6;
  static const int64_t DAG_NET_ERROR_COUNT_UP_THREASHOLD = 2000;
  int64_t array_count_; // equal to cg count
  ObCOMergeExeStat exe_stat_;
  ObCOMergeDagNet &dag_net_;
  ObTabletMergeInfo **cg_merge_info_array_;
  ObITable **merged_sstable_array_;
  CGScheduleStatus *cg_schedule_status_array_;
  lib::ObMutex cg_tables_handle_lock_;
  // store merged cg major sstables for random order, just hold table ref
  storage::ObTablesHandleArray merged_cg_tables_handle_;
  ObStorageColumnGroupSchema mocked_row_store_cg_;
  ObTableReadInfo table_read_info_; // read info for merge from col store to row store
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_CO_MERGE_CTX_H_
