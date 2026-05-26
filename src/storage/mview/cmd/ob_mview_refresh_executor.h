/**
 * Copyright (c) 2023 OceanBase
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

#include "lib/container/ob_array.h"
#include "share/schema/ob_dependency_info.h"
#include "share/schema/ob_mlog_info.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_schema_checker.h"
#include "storage/mview/ob_mview_refresh_stats_utils.h"
#include "share/schema/ob_mview_info.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{
struct ObMViewRefreshParam;

struct ObMViewRefreshArg
{
public:
  ObMViewRefreshArg()
    : push_deferred_rpc_(false),
      refresh_after_errors_(false),
      purge_option_(0),
      heap_size_(0),
      atomic_refresh_(false),
      nested_(false),
      out_of_place_(false),
      skip_ext_data_(false),
      refresh_parallel_(0)
  {
  }
  bool is_valid() const { return !list_.empty(); }
  TO_STRING_KV(K_(list), K_(method), K_(nested), K_(refresh_parallel));
  void operator ()(const ObMViewRefreshArg &other);
public:
  ObString list_;
  ObString method_;
  ObString rollback_seg_;
  bool push_deferred_rpc_;
  bool refresh_after_errors_;
  int64_t purge_option_;
  int64_t heap_size_;
  bool atomic_refresh_;
  bool nested_;
  bool out_of_place_;
  bool skip_ext_data_;
  int64_t refresh_parallel_;
};

class ObMViewRefreshExecutor
{
public:
  ObMViewRefreshExecutor();
  ~ObMViewRefreshExecutor();
  DISABLE_COPY_ASSIGN(ObMViewRefreshExecutor);

  int execute(sql::ObExecContext &ctx, const ObMViewRefreshArg &arg);
private:
  int write_run_start(const ObMViewRefreshArg &refresh_arg,
                      const uint64_t mview_id,
                      const int64_t start_time);
  int do_refresh(sql::ObExecContext &ctx,
                 const uint64_t tenant_id,
                 const uint64_t refresh_id,
                 const uint64_t mview_id,
                 const share::schema::ObMVRefreshMethod refresh_method_arg,
                 const int64_t refresh_parallel,
                 const bool nested_consistent_refresh);
  int do_nested_refresh(const uint64_t target_mview_id, const ObIArray<uint64_t> &nested_mview_ids);
  int sync_check_nested_mview_mds(const uint64_t mview_id,
                                  const uint64_t refresh_id,
                                  const share::SCN &target_data_sync_scn);
  int register_nested_mview_mds(const uint64_t mview_id,
                                const uint64_t refresh_id,
                                const ObIArray<uint64_t> &nest_mview_ids,
                                const share::SCN &target_data_sync_scn,
                                common::ObMySQLTransaction &trans);
  int register_nested_mview_mds_and_check(const uint64_t mview_id,
                                          const uint64_t refresh_id,
                                          const ObIArray<uint64_t> &nest_mview_ids,
                                          const share::SCN &target_data_sync_scn,
                                          ObMySQLTransaction &trans);
  int scheduler_nested_mviews_sync_refresh(const uint64_t target_mview_id,
                                           const uint64_t refresh_id,
                                           const share::SCN &target_data_sync_scn,
                                           const ObIArray<uint64_t> &nested_mview_ids,
                                           ObMySQLTransaction &trans);
private:
  sql::ObExecContext *ctx_;
  const ObMViewRefreshArg *arg_;
  sql::ObSQLSessionInfo *session_info_;

  uint64_t tenant_id_;
  int64_t refresh_id_;
  share::SCN target_data_sync_scn_;
  share::schema::ObMVRefreshMethod refresh_method_;
};

} // namespace storage
} // namespace oceanbase
