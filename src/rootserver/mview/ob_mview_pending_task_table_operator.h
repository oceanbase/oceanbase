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

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/net/ob_addr.h"
#include "rootserver/mview/ob_mview_pending_task_queue.h"
#include "share/scn.h"

namespace oceanbase
{
namespace rootserver
{

class ObMViewPendingTaskTableOperator
{
public:
  ObMViewPendingTaskTableOperator();
  ~ObMViewPendingTaskTableOperator();
  DISALLOW_COPY_AND_ASSIGN(ObMViewPendingTaskTableOperator);

  int init(common::ObISQLClient *sql_proxy);
  void destroy();

  int insert_tasks(const common::ObIArray<ObMViewPendingTask *> &tasks);
  int update_task_to_running(uint64_t tenant_id,
                             int64_t refresh_id,
                             uint64_t mview_id,
                             const common::ObAddr &svr_addr);
  static int update_task_session_id(common::ObISQLClient *sql_proxy,
                                     uint64_t tenant_id,
                                     int64_t refresh_id,
                                     uint64_t mview_id,
                                     uint32_t session_id);
  int update_task_to_success(uint64_t tenant_id,
                             int64_t refresh_id,
                             uint64_t mview_id);
  int update_task_to_retry_wait(uint64_t tenant_id,
                                int64_t refresh_id,
                                uint64_t mview_id,
                                int64_t next_retry_ts);
  int update_task_to_pending(uint64_t tenant_id,
                             int64_t refresh_id,
                             uint64_t mview_id);
  int update_task_running_to_pending(uint64_t tenant_id,
                                     int64_t refresh_id,
                                     uint64_t mview_id);
  // Fetch single-row status + next_retry_ts. Returns OB_ENTRY_NOT_EXIST when the row is absent
  // (e.g. recycle_refresh already deleted the refresh group).
  int get_task_sync_info(uint64_t tenant_id,
                         int64_t refresh_id,
                         uint64_t mview_id,
                         ObMViewTaskStatus &status,
                         int64_t &next_retry_ts,
                         uint64_t &target_data_sync_scn);
  int update_task_to_failed(uint64_t tenant_id, int64_t refresh_id, uint64_t failed_mview_id);
  // Batch cancel all PENDING/RETRY_WAIT tasks for a refresh. Idempotent.
  int batch_cancel_pending_tasks(uint64_t tenant_id, int64_t refresh_id);
  int delete_tasks_by_refresh_id(uint64_t tenant_id,
                                 int64_t refresh_id);
  int delete_terminal_tasks(uint64_t tenant_id);
  // Allocate tasks (and their dep arrays) directly from `alloc` so callers can push
  // pointers straight into the pending-task queue without an intermediate deep copy.
  int load_tasks_batch(common::ObIAllocator &alloc,
                       common::ObIArray<ObMViewPendingTask *> &tasks,
                       uint64_t last_scn,
                       int64_t last_refresh_id,
                       int64_t refresh_limit);
  int get_min_pending_task_snapshot(share::SCN &scn);
  // Batch-fetch session_id for the given (refresh_id, mview_id) keys that are
  // currently RUNNING in the inner table.
  int batch_get_running_session_ids(uint64_t tenant_id,
                                    common::ObIArray<ObMViewPendingTaskSessionIdEntry> &session_id_entries) const;
  int get_running_session_infos(uint64_t tenant_id,
                                int64_t refresh_id,
                                common::ObIArray<uint32_t> &out_session_ids,
                                common::ObIArray<common::ObAddr> &out_addrs,
                                bool &need_retry) const;
  // Fetch active (PENDING / RUNNING / RETRY_WAIT) refresh_ids for a given mview.
  // Used by DDL (drop materialized view / drop database) to enumerate refreshes
  // it must kill before taking the OBJ_TYPE_MATERIALIZED_VIEW lock.
  int get_active_refresh_ids_by_mview(uint64_t tenant_id,
                                      uint64_t mview_id,
                                      common::ObIArray<int64_t> &refresh_ids) const;

private:
  int update_task_status(uint64_t tenant_id,
                         int64_t refresh_id,
                         uint64_t mview_id,
                         int64_t old_status,
                         int64_t new_status);
  static int execute_trans_write(common::ObISQLClient *sql_proxy,
                                  uint64_t tenant_id,
                                  const common::ObSqlString &sql,
                                  int64_t &affected_rows);
  static int build_load_tasks_sql(common::ObSqlString &sql,
                                   uint64_t last_scn,
                                   int64_t last_refresh_id,
                                   int64_t refresh_limit);
  static int extract_task_from_result(common::sqlclient::ObMySQLResult &result,
                                      uint64_t actual_tenant_id,
                                      ObMViewPendingTask &task);
  static int fill_task_dep_ids(common::ObIAllocator &alloc,
                                ObMViewPendingTask &task,
                                const common::ObIArray<uint64_t> &flat_deps,
                                int64_t dep_range_start,
                                int64_t dep_range_end,
                                const common::ObIArray<ObMViewPendingTask *> &tasks,
                                int64_t group_start,
                                int64_t group_end);
  static int assign_batch_dep_ids(common::ObIAllocator &alloc,
                                   common::ObIArray<ObMViewPendingTask *> &tasks,
                                   const common::ObIArray<uint64_t> &flat_deps,
                                   const common::ObIArray<int64_t> &dep_start);
  static int build_batch_session_ids_sql(
      const common::ObIArray<ObMViewPendingTaskSessionIdEntry> &session_id_entries,
      common::ObSqlString &sql);
  static int fill_session_ids_from_result(
      common::sqlclient::ObMySQLResult &result,
      common::ObIArray<ObMViewPendingTaskSessionIdEntry> &session_id_entries);

  common::ObISQLClient *sql_proxy_;
  bool is_inited_;
};

} // namespace rootserver
} // namespace oceanbase
