/**
 * Copyright (c) 2024 OceanBase
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

#include "lib/container/ob_iarray.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_latch.h"
#include "rootserver/mview/ob_mview_pending_task_define.h"

namespace oceanbase
{
namespace rootserver
{

class ObMViewPendingTaskQueue
{
public:
  typedef common::hash::ObHashMap<ObMViewPendingTaskKey,
                                  ObMViewPendingTask *,
                                  common::hash::NoPthreadDefendMode> PendingTaskMap;
  typedef common::hash::ObHashMap<ObMViewRefreshKey,
                                  ObMViewPendingRefreshCtx *,
                                  common::hash::NoPthreadDefendMode> RefreshCtxMap;

public:
  ObMViewPendingTaskQueue();
  ~ObMViewPendingTaskQueue();
  DISALLOW_COPY_AND_ASSIGN(ObMViewPendingTaskQueue);

  int init(uint64_t tenant_id);
  int destroy();

  int push_tasks(const common::ObIArray<ObMViewPendingTask *> &tasks,
                 const common::ObCurTraceId::TraceId *trace_id = nullptr);
  int peek_task(ObMViewPendingTask &task);

  int set_task_running(uint64_t tenant_id,
                       int64_t refresh_id,
                       uint64_t mview_id,
                       const common::ObAddr *svr_addr = nullptr);
  int set_task_success(uint64_t tenant_id,
                       int64_t refresh_id,
                       uint64_t mview_id,
                       bool &refresh_finished);
  int set_task_retry_wait(uint64_t tenant_id,
                          int64_t refresh_id,
                          uint64_t mview_id,
                          bool &refresh_finished);
  int set_task_failed(uint64_t tenant_id,
                      int64_t refresh_id,
                      uint64_t mview_id,
                      int error_ret,
                      bool &refresh_finished);
  int set_task_cancelled(uint64_t tenant_id,
                         int64_t refresh_id,
                         uint64_t mview_id,
                         bool &refresh_finished);
  int recycle_refresh(uint64_t tenant_id,
                      int64_t refresh_id);
  int cancel_all_pending_tasks(uint64_t tenant_id,
                               int64_t refresh_id,
                               bool &refresh_finished);
  int get_refresh_status(uint64_t tenant_id,
                         int64_t refresh_id,
                         ObMViewTaskStatus &status) const;
  int64_t get_total_running_cnt() const;
  int set_task_pending(uint64_t tenant_id,
                       int64_t refresh_id,
                       uint64_t mview_id);
  int get_task_status(uint64_t tenant_id, int64_t refresh_id,
                      uint64_t mview_id, int64_t &status) const;
  int get_task_retry_count(uint64_t tenant_id, int64_t refresh_id,
                            uint64_t mview_id, int64_t &retry_count) const;
  int get_refresh_trace_id(uint64_t tenant_id,
                           int64_t refresh_id,
                           common::ObCurTraceId::TraceId &trace_id) const;
  // Snapshot every RUNNING task into `infos` under the read lock and release
  // the lock before returning. The caller drives the per-row callback outside
  // the lock to avoid blocking writers (state changes) for the full dump.
  int collect_running_jobs(common::ObIArray<ObMViewPendingRunningJobInfo> &infos) const;
  // Patch in-memory session_id for tasks that are still RUNNING and whose
  // current session_id_ is 0. Tasks not found or in non-RUNNING state, and
  // entries with session_id == 0 in the input, are silently skipped.
  int patch_session_ids_for_running_tasks(
      const common::ObIArray<ObMViewPendingTaskSessionIdEntry> &session_id_entries);
  int clear();
  // FOR UNIT TESTS ONLY: copy the refresh ctx for (tenant_id, refresh_id).
  int get_refresh_ctx_for_test(uint64_t tenant_id,
                               int64_t refresh_id,
                               ObMViewPendingRefreshCtx &ctx) const;

  static bool is_task_status_non_terminal(const int64_t status)
  { return MV_TASK_PENDING == status || MV_TASK_RUNNING == status || MV_TASK_RETRY_WAIT == status;  }
private:
  int free_task(ObMViewPendingTask *task);
  int alloc_refresh_ctx(uint64_t tenant_id,
                        int64_t refresh_id,
                        const common::ObCurTraceId::TraceId *trace_id,
                        ObMViewPendingRefreshCtx *&ctx);
  int free_refresh_ctx(ObMViewPendingRefreshCtx *ctx);
  int get_refresh_ctx(uint64_t tenant_id,
                      int64_t refresh_id,
                      ObMViewPendingRefreshCtx *&ctx) const;
  int refresh_finished_after_status_change(ObMViewPendingRefreshCtx &ctx,
                                           bool &refresh_finished) const;
  int set_task_status(uint64_t tenant_id,
                      int64_t refresh_id,
                      uint64_t mview_id,
                      const int64_t from_status,
                      const int64_t to_status,
                      int error_ret,
                      bool &refresh_finished,
                      bool ignore_status_check = false,
                      const common::ObAddr *svr_addr = nullptr);
  int inner_set_task_status(ObMViewPendingRefreshCtx &ctx,
                            uint64_t tenant_id,
                            int64_t refresh_id,
                            uint64_t mview_id,
                            const int64_t from_status,
                            const int64_t to_status,
                            bool ignore_status_check,
                            const common::ObAddr *svr_addr = nullptr);
  int inner_recursive_cancel_dept_tasks(ObMViewPendingRefreshCtx &ctx,
                                        uint64_t tenant_id,
                                        int64_t refresh_id,
                                        uint64_t mview_id);
  int inner_recursive_recycle_tasks(uint64_t tenant_id,
                                    int64_t refresh_id,
                                    uint64_t mview_id,
                                    ObIArray<ObMViewPendingTask *> &erased_tasks);
  int accum_ctx_stats_for_task(const ObMViewPendingTask &task,
                                ObMViewPendingRefreshCtx &ctx);
  int check_task_deps_satisfied(const ObMViewPendingTask &task,
                                bool &satisfied) const;
  int check_prev_task_satisfied(const ObMViewPendingTask &task,
                                bool &satisfied) const;

private:
  PendingTaskMap pending_map_;
  RefreshCtxMap refresh_map_;
  common::ObDList<ObMViewPendingTask> task_list_;
  mutable common::ObLatch rw_lock_;
  common::ObFIFOAllocator allocator_;
  int64_t total_running_cnt_;
  int64_t task_count_;
  bool is_inited_;
};

} // namespace rootserver
} // namespace oceanbase
