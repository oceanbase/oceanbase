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

#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/ob_table_load_table_compactor.h"
#include "storage/direct_load/ob_direct_load_i_table.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadParam;

class ObTableLoadGeneralTableCompactor : public ObTableLoadTableCompactor
{
  class CompactTaskProcessor;
  class CompactTaskCallback;
public:
  ObTableLoadGeneralTableCompactor();
  virtual ~ObTableLoadGeneralTableCompactor();
  void reset();
  int start() override;
  void stop() override;
private:
  int inner_init() override;
private:
  class CompactorTask : public common::ObDLinkBase<CompactorTask>
  {
  public:
    CompactorTask(storage::ObIDirectLoadTabletTableCompactor *table_compactor);
    ~CompactorTask();
    int add_table(storage::ObIDirectLoadPartitionTable *table);
    int process();
    void stop();
  private:
    storage::ObIDirectLoadTabletTableCompactor *table_compactor_;
  };
  typedef common::hash::ObHashMap<common::ObTabletID, CompactorTask *> CompactorTaskMap;
  class CompactorTaskIter
  {
  public:
    CompactorTaskIter();
    ~CompactorTaskIter();
    void reset();
    int add(CompactorTask *compactor_task);
    int get_next_compactor_task(CompactorTask *&compactor_task);
  private:
    common::ObSEArray<CompactorTask *, 64> compactor_task_array_;
    int64_t pos_;
  };
  int add_tablet_table(int32_t session_id, CompactorTaskMap &compactor_task_map,
                       storage::ObIDirectLoadPartitionTable *table);
  int create_tablet_table_compactor(int32_t session_id, const common::ObTabletID &tablet_id,
                                    storage::ObIDirectLoadTabletTableCompactor *&table_compactor);
  int create_tablet_compactor_task(int32_t session_id, const common::ObTabletID &tablet_id,
                                   CompactorTask *&compactor_task);
private:
  int construct_compactors();
  int start_compact();
  int get_next_compactor_task(CompactorTask *&compactor_task);
  void handle_compactor_task_finish(CompactorTask *compactor_task);
  int handle_compact_thread_finish(int ret_code);
  int build_result();
private:
  ObTableLoadStoreCtx *store_ctx_;
  const ObTableLoadParam *param_;
  common::ObArenaAllocator allocator_;
  common::ObSEArray<storage::ObIDirectLoadTabletTableCompactor *, 64> all_compactor_array_;
  mutable lib::ObMutex mutex_;
  CompactorTaskIter compactor_task_iter_;
  common::ObDList<CompactorTask> compacting_list_;
  int64_t running_thread_count_ CACHE_ALIGNED;
  volatile bool has_error_;
  volatile bool is_stop_;
};

} // namespace observer
} // namespace oceanbase
