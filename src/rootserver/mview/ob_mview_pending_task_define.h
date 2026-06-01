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

#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/net/ob_addr.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace rootserver
{

enum ObMViewTaskStatus
{
  MV_TASK_PENDING = 0,
  MV_TASK_RUNNING,
  MV_TASK_RETRY_WAIT,
  MV_TASK_SUCCESS,
  MV_TASK_FAILED,
  MV_TASK_CANCELLED,
};

class ObMViewPendingTaskKey
{
public:
  ObMViewPendingTaskKey();
  ObMViewPendingTaskKey(const uint64_t tenant_id,
                        const int64_t refresh_id,
                        const uint64_t mview_id);
  ~ObMViewPendingTaskKey();

  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const ObMViewPendingTaskKey &other) const;
  TO_STRING_KV(K_(tenant_id), K_(refresh_id), K_(mview_id));

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t mview_id_;
};

struct ObMViewPendingTaskSessionIdEntry
{
  ObMViewPendingTaskSessionIdEntry();
  ObMViewPendingTaskSessionIdEntry(const ObMViewPendingTaskKey &key,
                                   const int64_t info_idx);
  TO_STRING_KV(K_(key), K_(info_idx), K_(session_id));

  ObMViewPendingTaskKey key_;
  int64_t info_idx_;
  uint32_t session_id_;
};

class ObMViewRefreshKey
{
public:
  ObMViewRefreshKey();
  ObMViewRefreshKey(const uint64_t tenant_id, const int64_t refresh_id);
  ~ObMViewRefreshKey();

  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const ObMViewRefreshKey &other) const;
  TO_STRING_KV(K_(tenant_id), K_(refresh_id));

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
};

class ObMViewPendingRunningJobInfo
{
public:
  ObMViewPendingRunningJobInfo();
  ~ObMViewPendingRunningJobInfo();
  TO_STRING_KV(K_(tenant_id),
               K_(refresh_id),
               K_(mview_id),
               K_(target_data_sync_scn),
               K_(refresh_method),
               K_(refresh_parallel),
               K_(gmt_create),
               K_(gmt_modified),
               K_(session_id));

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t mview_id_;
  uint64_t target_data_sync_scn_;
  share::schema::ObMVRefreshMethod refresh_method_;
  int64_t refresh_parallel_;
  int64_t gmt_create_;
  int64_t gmt_modified_;
  // 0 means session_id is not yet known on this server (executor sets it on
  // disk; manager-side memory is patched lazily on foreach_running_job).
  uint32_t session_id_;
};

class ObMViewPendingTask : public common::ObDLinkBase<ObMViewPendingTask>
{
public:
  enum
  {
    ROOT_TASK_FLAG = 1,
    NESTED_REFRESH_FLAG = 2
  };

public:
  ObMViewPendingTask();
  ~ObMViewPendingTask();

  bool is_valid() const;
  bool is_root_task() const;
  bool is_nested_refresh() const;
  int assign(const ObMViewPendingTask &other);
  static int deep_copy(common::ObIAllocator &alloc,
                        const ObMViewPendingTask &src,
                        ObMViewPendingTask *&dst);
  TO_STRING_KV(K_(tenant_id),
               K_(refresh_id),
               K_(mview_id),
               K_(seq),
               K_(target_data_sync_scn),
               K_(refresh_method),
               K_(refresh_parallel),
               K_(status),
               K_(skip_cnt),
               K_(retry_count),
               K_(next_retry_ts),
               K_(flags),
               K_(dep_mview_id_cnt),
               KP_(dep_mview_ids),
               K_(gmt_create),
               K_(gmt_modified),
               K_(svr_addr),
               K_(session_id));

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t mview_id_;
  int64_t seq_;
  uint64_t target_data_sync_scn_;
  share::schema::ObMVRefreshMethod refresh_method_;
  int64_t refresh_parallel_;
  int64_t status_;
  int64_t skip_cnt_;
  int64_t retry_count_;
  int64_t next_retry_ts_;
  int64_t flags_;
  int64_t dep_mview_id_cnt_;
  uint64_t *dep_mview_ids_;
  int64_t gmt_create_;
  int64_t gmt_modified_;
  common::ObAddr svr_addr_;
  // Set by the executor (on another server) to the disk row only; manager-side
  // in-memory value is 0 until lazy-patched by foreach_running_job. Reset to 0
  // on every transition into MV_TASK_RUNNING so stale values from a previous
  // run don't leak after retry.
  uint32_t session_id_;
};

class ObMViewPendingRefreshCtx
{
public:
  ObMViewPendingRefreshCtx();
  ~ObMViewPendingRefreshCtx();

  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id),
               K_(refresh_id),
               K_(root_mview_id),
               K_(unfinished_task_cnt),
               K_(running_task_cnt),
               K_(has_terminal_failure),
               K_(root_task_succeeded),
               K_(cancelled),
               K_(trace_id));
  bool is_task_finished() const { return root_mview_id_ != OB_INVALID_ID && unfinished_task_cnt_ <= 0; }

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t root_mview_id_;
  int64_t unfinished_task_cnt_;
  int64_t running_task_cnt_;
  bool has_terminal_failure_;
  bool root_task_succeeded_;
  bool cancelled_;
  common::ObCurTraceId::TraceId trace_id_;
};

} // namespace rootserver
} // namespace oceanbase
