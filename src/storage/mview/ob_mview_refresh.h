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

#include "share/ob_table_range.h"
#include "share/schema/ob_dependency_info.h"
#include "share/schema/ob_mlog_info.h"
#include "share/schema/ob_mview_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/mview/ob_mview_refresh_stats_utils.h"
#include "storage/mview/ob_mview_transaction.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObMVDepInfo;
} // namespace sql
namespace transaction
{
namespace tablelock
{
struct ObTableLockHolderInfo;
} // namespace tablelock
} // namespace transaction
namespace storage
{

struct ObMViewRefreshParam
{
public:
  ObMViewRefreshParam(const uint64_t tenant_id, const uint64_t mview_id,
                      const int64_t refresh_id, const share::schema::ObMVRefreshMethod refresh_method,
                      const int64_t parallel)
    : tenant_id_(tenant_id),
      mview_id_(mview_id),
      refresh_id_(refresh_id),
      refresh_method_(refresh_method),
      retry_id_(0),
      parallel_(parallel),
      is_consistent_refresh_(false),
      target_data_sync_scn_()
  {}

  TO_STRING_KV(K_(tenant_id), K_(mview_id), K_(refresh_id), K_(refresh_method), K_(retry_id),
          K_(parallel), K_(is_consistent_refresh), K_(target_data_sync_scn));

public:
  const uint64_t tenant_id_;
  const uint64_t mview_id_;
  const int64_t refresh_id_;
  const share::schema::ObMVRefreshMethod refresh_method_;
  int64_t retry_id_;
  const int64_t parallel_;
  bool is_consistent_refresh_;
  share::SCN target_data_sync_scn_;
};

class ObMViewRefresher
{
public:
  ObMViewRefresher(sql::ObExecContext &ctx, const ObMViewRefreshParam &param);
  ~ObMViewRefresher();
  DISABLE_COPY_ASSIGN(ObMViewRefresher);

  // normal
  int refresh();

  // static
  static int collect_based_schema_object_infos(const uint64_t tenant_id,
                                               const uint64_t data_version,
                                               share::schema::ObSchemaGetterGuard &schema_guard,
                                               const ObIArray<share::schema::ObDependencyInfo> &dependency_infos,
                                               ObIArray<share::schema::ObBasedSchemaObjectInfo> &based_schema_object_infos,
                                               uint64_t &direct_dep_cnt);
  // calc parallelism for initial complete refresh issued by CREATE MV,
  // priority: ddl_parallel_hint > refresh_dop > global mview_refresh_dop > 1
  static int calc_create_mv_refresh_parallelism(share::schema::ObSchemaGetterGuard &schema_guard,
                                                const uint64_t tenant_id,
                                                const int64_t ddl_parallel_hint,
                                                const int64_t refresh_dop,
                                                int64_t &refresh_parallelism);

  TO_STRING_KV(K_(param));

private:
  // normal
  int prepare_for_refresh(ObIAllocator &allocator,
                          share::schema::ObMVRefreshType &refresh_type,
                          ObIArray<ObString> &fast_refresh_sqls);
  int complete_refresh();
  int fast_refresh(const ObIArray<ObString> &refresh_sqls);
  int do_fast_refresh(const ObIArray<ObString> &refresh_sqls);
  int prepare_plan_capture_info(sql::ObSQLSessionInfo *exec_session_info,
                                 const ObString &fast_refresh_sql,
                                 int exec_ret,
                                 int64_t execution_time,
                                 ObMViewStmtPlanCaptureInfo &capture_info);
  int purge_mlog(const ObIArray<share::schema::ObMLogInfo> &mlog_infos,
                 const ObIArray<share::schema::ObDependencyInfo> &dependency_infos);
  int calc_mv_refresh_parallelism(const int64_t refresh_param_dop,
                                  const int64_t mview_info_dop,
                                  int64_t &refresh_parallelism);
  // Env state saved by setup_refresh_env_ and restored by teardown_refresh_env_.
  struct RefreshEnvState
  {
    bool has_updated_dml_dop_;
    uint64_t orig_dml_dop_;
    bool has_enabled_plan_monitor_;
    RefreshEnvState()
      : has_updated_dml_dop_(false), orig_dml_dop_(0), has_enabled_plan_monitor_(false)
    {}
    TO_STRING_KV(K_(has_updated_dml_dop), K_(orig_dml_dop), K_(has_enabled_plan_monitor));
  };

  int setup_refresh_env_(sql::ObSQLSessionInfo *session,
                         bool is_inner_session,
                         int64_t parallelism,
                         RefreshEnvState &env_state);
  int teardown_refresh_env_(sql::ObSQLSessionInfo *session,
                            const RefreshEnvState &env_state);

  // const
  int lock_mview_for_refresh(ObMViewTransaction &trans) const;
  int fetch_mview_info(ObMViewTransaction &trans,
                       share::schema::ObMViewInfo &mview_info) const;
  int fetch_mview_refresh_stats_collection_level(ObMViewTransaction &trans,
                                                 const uint64_t tenant_id,
                                                 const uint64_t mview_id,
                                                 share::schema::ObMVRefreshStatsCollectionLevel &collection_level) const;
  int get_and_check_refresh_scn(ObMViewTransaction &trans,
                                share::ObScnRange &mview_refresh_scn_range,
                                share::ObScnRange &base_table_scn_range) const;
  int fetch_based_infos(ObMViewTransaction &trans,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        const ObIArray<uint64_t> &tables_need_mlog,
                        ObIArray<share::schema::ObMLogInfo> &mlog_infos) const;
  int collect_and_check_detail_table_changes(ObMViewTransaction &trans,
                                             share::schema::ObSchemaGetterGuard &schema_guard,
                                             const ObIArray<uint64_t> &tables_need_mlog,
                                             ObIArray<uint64_t> &tables_without_delete,
                                             ObIArray<uint64_t> &tables_without_insert,
                                             bool &reached_complete_refresh_threshold) const;
  bool reached_detail_complete_refresh_threshold(const ObMViewDetailTableChangeStats &data) const;
  static bool has_valid_exclusive_holder_(const ObIArray<transaction::tablelock::ObTableLockHolderInfo> &holder_info);

private:
  sql::ObExecContext &ctx_;
  const ObMViewRefreshParam &param_;
  ObMViewTransaction trans_;

  share::schema::ObMVRefreshStatsCollectionLevel collection_level_;
  double complete_refresh_ratio_threshold_;
  bool enable_adaptive_refresh_step_;

  int64_t refresh_parallel_;
  share::schema::ObMViewInfo mview_info_;
  ObSEArray<share::schema::ObMLogInfo, 4> mlog_infos_;
  ObSEArray<share::schema::ObDependencyInfo, 4> dependency_infos_;
  share::ObScnRange base_table_scn_range_;
  share::ObScnRange mview_refresh_scn_range_;
};

} // namespace storage
} // namespace oceanbase
