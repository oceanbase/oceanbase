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
#include "lib/container/ob_vector.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadTask;

class ObTableLoadParallelMergeSSTableCompare
{
public:
  ObTableLoadParallelMergeSSTableCompare();
  ~ObTableLoadParallelMergeSSTableCompare();
  bool operator()(const storage::ObDirectLoadMultipleSSTable *lhs,
                  const storage::ObDirectLoadMultipleSSTable *rhs);
  int get_error_code() const { return result_code_; }
  int result_code_;
};

struct ObTableLoadParallelMergeTabletCtx
{
public:
  ObTableLoadParallelMergeTabletCtx();
  ~ObTableLoadParallelMergeTabletCtx();
  int set_parallel_merge_param(int64_t merge_sstable_count, int64_t range_count);
  int finish_range_merge(int64_t range_idx, storage::ObDirectLoadMultipleSSTable *range_sstable,
                         bool &all_range_finish);
  int apply_merged_sstable(storage::ObDirectLoadMultipleSSTable *merged_sstable);
  TO_STRING_KV(K_(tablet_id), K_(sstables), K_(merge_sstable_count), K_(range_count),
               K_(range_sstable_count), K_(ranges), K_(range_sstables));
public:
  common::ObTabletID tablet_id_;
  common::ObArenaAllocator allocator_; // for alloc sstables
  common::ObVector<storage::ObDirectLoadMultipleSSTable *> sstables_;
  int64_t merge_sstable_count_;
  int64_t range_count_;
  int64_t range_sstable_count_;
  lib::ObMutex mutex_; // for alloc range sstable
  common::ObArenaAllocator range_allocator_; // for alloc range and range sstable
  common::ObArray<ObDirectLoadMultipleDatumRange> ranges_;
  common::ObArray<storage::ObDirectLoadMultipleSSTable *> range_sstables_;
};

class ObTableLoadParallelMergeCb
{
public:
  virtual ~ObTableLoadParallelMergeCb() = default;
  virtual int on_success() = 0;
};

class ObTableLoadParallelMergeCtx
{
  class SplitRangeTaskProcessor;
  class MergeRangeTaskProcessor;
  class CompactSSTableTaskProcessor;
  class ParallelMergeTaskCallback;
public:
  typedef common::hash::ObHashMap<common::ObTabletID, ObTableLoadParallelMergeTabletCtx *>
    TabletCtxMap;
  typedef TabletCtxMap::const_iterator TabletCtxIterator;
  ObTableLoadParallelMergeCtx();
  ~ObTableLoadParallelMergeCtx();
  int init(ObTableLoadStoreCtx *store_ctx);
  int add_tablet_sstable(storage::ObDirectLoadMultipleSSTable *sstable);
  int start(ObTableLoadParallelMergeCb *cb);
  void stop();
  const TabletCtxMap &get_tablet_ctx_map() const { return tablet_ctx_map_; }
private:
  int start_merge();
  int schedule_merge_unlock();
  int construct_split_range_task(ObTableLoadParallelMergeTabletCtx *tablet_ctx);
  int construct_merge_range_task(ObTableLoadParallelMergeTabletCtx *tablet_ctx, int64_t range_idx);
  int construct_compact_sstable_task(ObTableLoadParallelMergeTabletCtx *tablet_ctx);
  int handle_tablet_split_range_finish(ObTableLoadParallelMergeTabletCtx *tablet_ctx);
  int handle_tablet_range_merge_finish(ObTableLoadParallelMergeTabletCtx *tablet_ctx);
  int handle_tablet_compact_sstable_finish(ObTableLoadParallelMergeTabletCtx *tablet_ctx);
  int handle_task_finish(int64_t thread_idx, int ret_code);
private:
  int add_light_task(ObTableLoadTask *task);
  int add_heavy_task(ObTableLoadTask *task);
  int64_t get_task_count() const;
  int add_idle_thread(int64_t thread_idx);
  int64_t get_idle_thread_count() const;
private:
  ObTableLoadStoreCtx *store_ctx_;
  int64_t thread_count_;
  ObTableLoadParallelMergeCb *cb_;
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
