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

#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_i_table.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadStoreTableCtx;
class ObTableLoadMergeCompactTableOp;
class ObTableLoadTask;

struct ObTableLoadParallelCompactTabletCtx
{
public:
  ObTableLoadParallelCompactTabletCtx();
  ~ObTableLoadParallelCompactTabletCtx();
  int set_parallel_merge_param(int64_t merge_sstable_count, int64_t range_count);
  int finish_range_merge(int64_t range_idx, const ObDirectLoadTableHandle &range_sstable,
                         bool &all_range_finish);
  int apply_merged_sstable(const ObDirectLoadTableHandle &merge_sstable);
  TO_STRING_KV(K_(tablet_id), K_(sstables), K_(merge_sstable_count), K_(range_count),
               K_(range_sstable_count), K_(ranges), K_(range_sstables));

public:
  common::ObTabletID tablet_id_;
  common::ObArenaAllocator allocator_; // for alloc sstables
  ObDirectLoadTableHandleArray sstables_;
  int64_t merge_sstable_count_;
  int64_t range_count_;
  int64_t range_sstable_count_;
  lib::ObMutex mutex_; // for alloc range sstable
  common::ObArenaAllocator range_allocator_; // for alloc range and range sstable
  common::ObArray<ObDirectLoadMultipleDatumRange> ranges_;
  common::ObArray<ObDirectLoadTableHandle> range_sstables_;
};

class ObTableLoadParallelTableCompactor
{
  class SplitRangeTaskProcessor;
  class MergeRangeTaskProcessor;
  class CompactSSTableTaskProcessor;
  class ParallelMergeTaskCallback;

  class SSTableCompare
  {
  public:
    SSTableCompare();
    ~SSTableCompare();
    bool operator()(const ObDirectLoadTableHandle lhs, const ObDirectLoadTableHandle rhs);
    int get_error_code() const { return result_code_; }
    int result_code_;
  };
  typedef common::hash::ObHashMap<common::ObTabletID, ObTableLoadParallelCompactTabletCtx *>
    TabletCtxMap;

public:
  ObTableLoadParallelTableCompactor();
  ~ObTableLoadParallelTableCompactor();
  int init(ObTableLoadMergeCompactTableOp *op);
  int start();
  void stop();

private:
  int construct_compactors();
  int start_merge();
  int schedule_merge_unlock();
  int construct_split_range_task(ObTableLoadParallelCompactTabletCtx *tablet_ctx);
  int construct_merge_range_task(ObTableLoadParallelCompactTabletCtx *tablet_ctx,
                                 int64_t range_idx);
  int construct_compact_sstable_task(ObTableLoadParallelCompactTabletCtx *tablet_ctx);
  int handle_tablet_split_range_finish(ObTableLoadParallelCompactTabletCtx *tablet_ctx);
  int handle_tablet_range_merge_finish(ObTableLoadParallelCompactTabletCtx *tablet_ctx);
  int handle_tablet_compact_sstable_finish(ObTableLoadParallelCompactTabletCtx *tablet_ctx);
  int handle_task_finish(int64_t thread_idx, int ret_code);
  int handle_parallel_compact_success();

private:
  int add_light_task(ObTableLoadTask *task);
  int add_heavy_task(ObTableLoadTask *task);
  int64_t get_task_count() const;
  int add_idle_thread(int64_t thread_idx);
  int64_t get_idle_thread_count() const;

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadStoreTableCtx *store_table_ctx_;
  ObTableLoadMergeCompactTableOp *op_;
  int64_t thread_count_;
  ObDirectLoadTableDataDesc table_data_desc_;
  common::ObArenaAllocator allocator_;
  TabletCtxMap tablet_ctx_map_;
  mutable lib::ObMutex mutex_;
  ObArray<ObTableLoadTask *> light_task_list_;
  ObArray<ObTableLoadTask *> heavy_task_list_;
  ObArray<int64_t> idle_thread_list_;
  bool has_error_;
  volatile bool is_stop_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
