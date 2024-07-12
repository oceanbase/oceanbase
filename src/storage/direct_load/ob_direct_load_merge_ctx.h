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
#pragma once

#include "lib/allocator/ob_allocator.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/schema/ob_table_param.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_struct.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_trans_param.h"
#include "storage/direct_load/ob_direct_load_fast_heap_table.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace common {
class ObOptOSGColumnStat;
class ObOptTableStat;
} // namespace common
namespace storage
{
class ObDirectLoadInsertTableContext;
class ObDirectLoadPartitionMergeTask;
class ObDirectLoadPartitionRescanTask;
class ObDirectLoadPartitionDelLobTask;
class ObDirectLoadTabletMergeCtx;
class ObIDirectLoadPartitionTable;
class ObDirectLoadSSTable;
class ObDirectLoadMultipleSSTable;
class ObDirectLoadMultipleHeapTable;
class ObDirectLoadMultipleMergeRangeSplitter;
class ObDirectLoadDMLRowHandler;
class ObIDirectLoadPartitionTableBuilder;
class ObDirectLoadTmpFileManager;

struct ObDirectLoadMergeParam
{
public:
  ObDirectLoadMergeParam();
  ~ObDirectLoadMergeParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_id),
               K_(lob_meta_table_id),
               K_(target_table_id),
               K_(rowkey_column_num),
               K_(store_column_count),
               K_(fill_cg_thread_cnt),
               KP_(lob_column_idxs),
               K_(table_data_desc),
               KP_(datum_utils),
               KP_(lob_column_idxs),
               KP_(col_descs),
               K_(lob_id_table_data_desc),
               KP_(lob_meta_datum_utils),
               KP_(lob_meta_col_descs),
               K_(is_heap_table),
               K_(is_fast_heap_table),
               K_(is_incremental),
               "insert_mode", ObDirectLoadInsertMode::get_type_string(insert_mode_),
               KP_(insert_table_ctx),
               KP_(dml_row_handler),
               KP_(file_mgr),
               K_(trans_param));
public:
  uint64_t table_id_;
  uint64_t lob_meta_table_id_;
  uint64_t target_table_id_;
  int64_t rowkey_column_num_;
  int64_t store_column_count_;
  int64_t fill_cg_thread_cnt_;
  const common::ObArray<int64_t> *lob_column_idxs_;
  storage::ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  storage::ObDirectLoadTableDataDesc lob_id_table_data_desc_;
  const blocksstable::ObStorageDatumUtils *lob_meta_datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *lob_meta_col_descs_;
  bool is_heap_table_;
  bool is_fast_heap_table_;
  bool is_incremental_;
  ObDirectLoadInsertMode::Type insert_mode_;
  ObDirectLoadInsertTableContext *insert_table_ctx_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  ObDirectLoadTmpFileManager *file_mgr_;
  ObDirectLoadTransParam trans_param_;
};

class ObDirectLoadMergeCtx
{
private:
  typedef common::hash::ObHashMap<int64_t, ObIDirectLoadPartitionTableBuilder *> TABLE_BUILDER_MAP;
public:
  ObDirectLoadMergeCtx();
  ~ObDirectLoadMergeCtx();
  int init(observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &param,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids);
  int get_table_builder(ObIDirectLoadPartitionTableBuilder *&table_builder);
  int close_table_builder();
  TABLE_BUILDER_MAP& get_table_builder_map() { return table_builder_map_; }
  const common::ObIArray<ObDirectLoadTabletMergeCtx *> &get_tablet_merge_ctxs() const
  {
    return tablet_merge_ctx_array_;
  }
private:
  int create_all_tablet_ctxs(const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids);
private:
  common::ObArenaAllocator allocator_;
  observer::ObTableLoadTableCtx *ctx_;
  ObDirectLoadMergeParam param_;
  common::ObArray<ObDirectLoadTabletMergeCtx *> tablet_merge_ctx_array_;
  TABLE_BUILDER_MAP table_builder_map_;
  lib::ObMutex mutex_;
  bool is_inited_;
};

class ObDirectLoadTabletMergeCtx
{
public:
  ObDirectLoadTabletMergeCtx();
  ~ObDirectLoadTabletMergeCtx();
  int init(ObDirectLoadMergeCtx *merge_ctx,
           observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &param,
           const table::ObTableLoadLSIdAndPartitionId &ls_partition_id);
  int build_merge_task(const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                       const common::ObIArray<share::schema::ObColDesc> &col_descs,
                       int64_t max_parallel_degree, bool is_multiple_mode);
  int build_merge_task_for_multiple_pk_table(
    const common::ObIArray<ObDirectLoadMultipleSSTable *> &multiple_sstable_array,
    ObDirectLoadMultipleMergeRangeSplitter &range_splitter,
    int64_t max_parallel_degree);
  int build_aggregate_merge_task_for_multiple_heap_table(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array);
  int build_rescan_task(int64_t thread_count);
  int build_del_lob_task(
    const common::ObIArray<ObDirectLoadMultipleSSTable *> &multiple_sstable_array,
    ObDirectLoadMultipleMergeRangeSplitter &range_splitter,
    const int64_t max_parallel_degree);
  int inc_finish_count(bool &is_ready);
  int inc_rescan_finish_count(bool &is_ready);
  int inc_del_lob_finish_count(bool &is_ready);
  int get_table_builder(ObIDirectLoadPartitionTableBuilder *&table_builder);
  bool merge_with_origin_data()
  {
    return !param_.is_incremental_ && param_.insert_mode_ == ObDirectLoadInsertMode::NORMAL;
  }
  bool merge_with_conflict_check()
  {
    return param_.is_incremental_ &&
           (param_.insert_mode_ == ObDirectLoadInsertMode::NORMAL ||
           (param_.insert_mode_ == ObDirectLoadInsertMode::INC_REPLACE && param_.lob_column_idxs_->count() > 0));
  }
  const ObDirectLoadMergeParam &get_param() const { return param_; }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  const common::ObIArray<ObDirectLoadPartitionMergeTask *> &get_tasks() const
  {
    return task_array_;
  }
  const common::ObIArray<ObDirectLoadPartitionRescanTask *> &get_rescan_tasks() const
  {
    return rescan_task_array_;
  }
  const common::ObIArray<ObDirectLoadPartitionDelLobTask *> &get_del_lob_tasks() const
  {
    return del_lob_task_array_;
  }
  TO_STRING_KV(K_(param), K_(tablet_id));
private:
  int init_sstable_array(const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array);
  int init_multiple_sstable_array(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array);
  int init_multiple_heap_table_array(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array);
  int build_empty_data_merge_task(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                  int64_t max_parallel_degree);
  int build_pk_table_multiple_merge_task(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    int64_t max_parallel_degree);
  int build_heap_table_multiple_merge_task(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    int64_t max_parallel_degree);
  int get_autoincrement_value(uint64_t count, share::ObTabletCacheInterval &interval);

private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadMergeCtx *merge_ctx_;
  observer::ObTableLoadTableCtx *ctx_;
  ObDirectLoadMergeParam param_;
  common::ObTabletID tablet_id_;
  ObDirectLoadOriginTable origin_table_;
  ObDirectLoadOriginTable lob_meta_origin_table_;
  common::ObArray<ObDirectLoadSSTable *> sstable_array_;
  common::ObArray<ObDirectLoadMultipleSSTable *> multiple_sstable_array_;
  common::ObArray<ObDirectLoadMultipleHeapTable *> multiple_heap_table_array_;
  common::ObArray<ObDirectLoadMultipleSSTable *> del_lob_multiple_sstable_array_;
  common::ObArray<blocksstable::ObDatumRange> range_array_;
  common::ObArray<blocksstable::ObDatumRange> del_lob_range_array_;
  common::ObArray<ObDirectLoadPartitionMergeTask *> task_array_;
  common::ObArray<ObDirectLoadPartitionRescanTask *> rescan_task_array_;
  common::ObArray<ObDirectLoadPartitionDelLobTask *> del_lob_task_array_;
  int64_t task_finish_count_ CACHE_ALIGNED;
  int64_t rescan_task_finish_count_ CACHE_ALIGNED;
  int64_t del_lob_task_finish_count_ CACHE_ALIGNED;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
