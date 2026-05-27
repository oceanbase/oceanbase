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
 *
 * Unified cache for update-split-merge flow.
 * Owns per-transaction merge_map + references instance-level storager.
 * Sorter only calls put / get_and_merge / handle_unmatched.
 */

#ifndef OCEANBASE_LIBOBCDC_UPDATE_SPLIT_MERGE_CACHE_H_
#define OCEANBASE_LIBOBCDC_UPDATE_SPLIT_MERGE_CACHE_H_

#include "lib/hash/ob_hashmap.h"
#include "ob_log_trans_ctx.h"
#include "ob_log_binlog_record.h"
#include "ob_log_trans_stat_mgr.h"
#include "ob_cdc_update_split_merge_storager.h"

namespace oceanbase
{
namespace libobcdc
{

class IObLogResourceCollector;

struct MergeCacheEntry
{
  // IN_STMT:   cache directly references the DmlStmtTask (T1, no offload).
  // OFFLOADED: payload has been serialized and handed to the storager (T2, RocksDB).
  //            delete_stmt_ is nullptr in this state; offload_key_ carries the key
  //            needed to reach into the storager at merge / cleanup time.
  enum State { IN_STMT = 0, OFFLOADED = 1 };
  State state_;
  DmlStmtTask *delete_stmt_;
  UpdateSplitMergeKey offload_key_;
  MergeCacheEntry() : state_(IN_STMT), delete_stmt_(nullptr), offload_key_() {}
  TO_STRING_KV(K_(state), KP_(delete_stmt), K_(offload_key));
};

typedef common::hash::ObHashMap<int64_t, MergeCacheEntry> UpdateSplitMergeMap;

class ObCDCUpdateSplitMergeCache
{
public:
  ObCDCUpdateSplitMergeCache();
  ~ObCDCUpdateSplitMergeCache();

  int init(const transaction::ObTransID &trans_id,
           ObCDCUpdateSplitMergeStorager &storager,
           IObLogResourceCollector &resource_collector,
           UpdateSplitMergeStatInfo &stat);
  void destroy();

  int put(const int64_t trace_id, DmlStmtTask *del_stmt);

  int get_and_merge(const int64_t trace_id,
                    DmlStmtTask *ins_stmt,
                    ObLogBR *&output_br);

  int handle_unmatched();

  int64_t size() const { return merge_map_.size(); }

private:
  // Decide whether to offload the DELETE payload from the original DmlStmtTask
  // to the storager (RocksDB). "Offload" here means "release the DmlStmtTask
  // back to its PartTransTask allocator"; it is orthogonal to where the bytes
  // physically land (the storager writes them to RocksDB, whose memtable
  // already acts as an implicit in-memory buffer).
  bool should_offload_to_storager_(const int64_t trace_id) const;

  int merge_in_stmt_(MergeCacheEntry &entry,
                     DmlStmtTask *ins_stmt,
                     ObLogBR *&output_br);

  int merge_offloaded_(const MergeCacheEntry &entry,
                       const int64_t trace_id,
                       DmlStmtTask *ins_stmt,
                       ObLogBR *&output_br);

private:
  bool is_inited_;
  transaction::ObTransID trans_id_;
  UpdateSplitMergeMap merge_map_;
  ObCDCUpdateSplitMergeStorager *storager_;
  IObLogResourceCollector *resource_collector_;
  UpdateSplitMergeStatInfo *stat_;

  DISALLOW_COPY_AND_ASSIGN(ObCDCUpdateSplitMergeCache);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
