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

#include "lib/lock/ob_mutex.h"
#include "storage/blocksstable/ob_sstable.h"
#include "observer/table_load/ob_table_load_table_compactor.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "storage/direct_load/ob_direct_load_merge_task_iterator.h"
#include "storage/direct_load/ob_direct_load_partition_merge_task.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadParam;
class ObTableLoadStoreCtx;

class ObTableLoadMerger
{
  class MergeTaskProcessor;
  class MergeTaskCallback;
public:
  ObTableLoadMerger(ObTableLoadStoreCtx *store_ctx);
  ~ObTableLoadMerger();
  int init();
  int start();
  void stop();
  int handle_table_compact_success();
  int collect_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics);
  int collect_dml_stat(table::ObTableLoadDmlStat &dml_stats);
private:
  int build_merge_ctx();
  int start_merge();
  int get_next_merge_task(storage::ObDirectLoadPartitionMergeTask *&merge_task);
  void handle_merge_task_finish(storage::ObDirectLoadPartitionMergeTask *&merge_task);
  int handle_merge_thread_finish(int ret_code);
private:
  ObTableLoadStoreCtx * const store_ctx_;
  const ObTableLoadParam &param_;
  ObTableLoadTableCompactCtx table_compact_ctx_;
  storage::ObDirectLoadMergeCtx merge_ctx_;
  mutable lib::ObMutex mutex_;
  ObDirectLoadMergeTaskIterator merge_task_iter_;
  common::ObDList<storage::ObDirectLoadPartitionMergeTask> merging_list_;
  int64_t running_thread_count_ CACHE_ALIGNED;
  volatile bool has_error_;
  volatile bool is_stop_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
