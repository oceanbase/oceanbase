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
#include "share/stat/ob_stat_define.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_osg_column_stat.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_dml_stat.h"
#include "share/table/ob_table_load_sql_statistics.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_fast_heap_table.h"

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
class ObDirectLoadTabletMergeCtx;
class ObIDirectLoadPartitionTable;
class ObDirectLoadSSTable;
class ObDirectLoadMultipleSSTable;
class ObDirectLoadMultipleHeapTable;
class ObDirectLoadMultipleMergeRangeSplitter;
class ObDirectLoadDMLRowHandler;

struct ObDirectLoadMergeParam
{
public:
  ObDirectLoadMergeParam();
  ~ObDirectLoadMergeParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_id), K_(target_table_id), K_(rowkey_column_num), K_(store_column_count),
               K_(snapshot_version), K_(table_data_desc), KP_(datum_utils), KP_(col_descs),
               KP_(cmp_funcs), K_(is_heap_table), K_(is_fast_heap_table),
               K_(online_opt_stat_gather), KP_(insert_table_ctx), KP_(dml_row_handler));
public:
  uint64_t table_id_;
  uint64_t target_table_id_;
  int64_t rowkey_column_num_;
  int64_t store_column_count_;
  int64_t snapshot_version_;
  storage::ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStoreCmpFuncs *cmp_funcs_;
  bool is_heap_table_;
  bool is_fast_heap_table_;
  bool online_opt_stat_gather_;
  ObDirectLoadInsertTableContext *insert_table_ctx_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
};

class ObDirectLoadMergeCtx
{
public:
  ObDirectLoadMergeCtx();
  ~ObDirectLoadMergeCtx();
  int init(const ObDirectLoadMergeParam &param,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids);
  const common::ObIArray<ObDirectLoadTabletMergeCtx *> &get_tablet_merge_ctxs() const
  {
    return tablet_merge_ctx_array_;
  }
private:
  int create_all_tablet_ctxs(const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
                             const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids);
private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadMergeParam param_;
  common::ObSEArray<ObDirectLoadTabletMergeCtx *, 64> tablet_merge_ctx_array_;
  bool is_inited_;
};

class ObDirectLoadTabletMergeCtx
{
public:
  ObDirectLoadTabletMergeCtx();
  ~ObDirectLoadTabletMergeCtx();
  int init(const ObDirectLoadMergeParam &param, const table::ObTableLoadLSIdAndPartitionId &ls_partition_id,
      const table::ObTableLoadLSIdAndPartitionId &target_ls_partition_id);
  int build_merge_task(const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                       const common::ObIArray<share::schema::ObColDesc> &col_descs,
                       int64_t max_parallel_degree, bool is_multiple_mode);
  int build_merge_task_for_multiple_pk_table(
    const common::ObIArray<ObDirectLoadMultipleSSTable *> &multiple_sstable_array,
    ObDirectLoadMultipleMergeRangeSplitter &range_splitter,
    int64_t max_parallel_degree);
  int build_aggregate_merge_task_for_multiple_heap_table(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array);
  int inc_finish_count(bool &is_ready);
  int collect_sql_statistics(
    const common::ObIArray<ObDirectLoadFastHeapTable *> &fast_heap_table_array, table::ObTableLoadSqlStatistics &sql_statistics);
  int collect_dml_stat(const common::ObIArray<ObDirectLoadFastHeapTable *> &fast_heap_table_array,
                       table::ObTableLoadDmlStat &dml_stats);
  const ObDirectLoadMergeParam &get_param() const { return param_; }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  const common::ObTabletID &get_target_tablet_id() const { return target_tablet_id_; }
  const common::ObIArray<ObDirectLoadPartitionMergeTask *> &get_tasks() const
  {
    return task_array_;
  }
  TO_STRING_KV(K_(param), K_(target_partition_id), K_(tablet_id), K_(target_tablet_id));
private:
  int init_sstable_array(const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array);
  int init_multiple_sstable_array(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array);
  int init_multiple_heap_table_array(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array);
  int build_empty_data_merge_task(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                  int64_t max_parallel_degree);
  int build_pk_table_merge_task(const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                                const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                int64_t max_parallel_degree);
  int build_pk_table_multiple_merge_task(
    const common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    int64_t max_parallel_degree);
  int build_heap_table_merge_task(
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
  ObDirectLoadMergeParam param_;
  uint64_t target_partition_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID target_tablet_id_;
  ObDirectLoadOriginTable origin_table_;
  common::ObSEArray<ObDirectLoadSSTable *, 64> sstable_array_;
  common::ObSEArray<ObDirectLoadMultipleSSTable *, 64> multiple_sstable_array_;
  common::ObSEArray<ObDirectLoadMultipleHeapTable *, 64> multiple_heap_table_array_;
  common::ObSEArray<blocksstable::ObDatumRange, 64> range_array_;
  common::ObSEArray<ObDirectLoadPartitionMergeTask *, 64> task_array_;
  int64_t task_finish_count_ CACHE_ALIGNED;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
