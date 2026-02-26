/**
 * Copyright (c) 2025 OceanBase
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
class ObTableLoadTableOpCtx;

struct ObTableLoadDagParallelCompactTabletCtx
{
public:
  ObTableLoadDagParallelCompactTabletCtx();
  ~ObTableLoadDagParallelCompactTabletCtx();
  int set_parallel_merge_param(int64_t merge_sstable_count, int64_t range_count);
  int finish_range_merge(int64_t range_idx, const ObDirectLoadTableHandle &range_sstable,
                         bool &all_range_finish);
  TO_STRING_KV(K_(tablet_id), K_(sstables), K_(merge_sstable_count), K_(range_count),
               K_(range_sstable_count), K_(ranges), K_(range_sstables));

public:
  common::ObTabletID tablet_id_;
  ObDirectLoadTableHandleArray sstables_;
  int64_t merge_sstable_count_;
  int64_t range_count_;
  int64_t range_sstable_count_;
  lib::ObMutex mutex_; // for alloc range sstable
  common::ObArenaAllocator range_allocator_; // for alloc range and range sstable
  common::ObArray<ObDirectLoadMultipleDatumRange> ranges_;
  common::ObArray<ObDirectLoadTableHandle> range_sstables_;
  common::ObArray<ObDirectLoadTableHandle> old_sstables_;
};

class ObTableLoadDagSSTableCompare
{
public:
  ObTableLoadDagSSTableCompare();
  ~ObTableLoadDagSSTableCompare();
  bool operator()(const ObDirectLoadTableHandle lhs, const ObDirectLoadTableHandle rhs);
  int get_error_code() const { return result_code_; }
  int result_code_;
};

class ObTableLoadDagParallelSSTableCompactor
{
public:
  typedef common::hash::ObHashMap<common::ObTabletID, ObTableLoadDagParallelCompactTabletCtx *>
    TabletCtxMap;
  ObTableLoadDagParallelSSTableCompactor();
  ~ObTableLoadDagParallelSSTableCompactor();
  int init(ObTableLoadStoreCtx *store_ctx, ObTableLoadTableOpCtx *op_ctx);
  int prepare_compact();
  int close();
  TabletCtxMap &get_tablet_ctx_map() { return tablet_ctx_map_; }

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadTableOpCtx *op_ctx_;
  common::ObArenaAllocator allocator_;
  TabletCtxMap tablet_ctx_map_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
