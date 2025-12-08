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
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_schema_checker.h"
#include "storage/mview/ob_mview_refresh_stats_collect.h"
#include "src/share/schema/ob_mview_info.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "src/storage/mview/ob_mview_transaction.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObDatabaseSchema;
}
}
namespace storage
{
struct ObMViewRefreshArg
{
public:
  ObMViewRefreshArg()
    : push_deferred_rpc_(false),
      refresh_after_errors_(false),
      purge_option_(0),
      parallelism_(0),
      heap_size_(0),
      atomic_refresh_(false),
      nested_(false),
      out_of_place_(false),
      skip_ext_data_(false),
      refresh_parallel_(0),
      nested_consistent_refresh_(false)
  {
  }
  bool is_valid() const { return !list_.empty(); }
  TO_STRING_KV(K_(list), K_(method), K_(nested), K_(nested_consistent_refresh));
  void operator ()(const ObMViewRefreshArg &other);
public:
  ObString list_;
  ObString method_;
  ObString rollback_seg_;
  bool push_deferred_rpc_;
  bool refresh_after_errors_;
  int64_t purge_option_;
  int64_t parallelism_;
  int64_t heap_size_;
  bool atomic_refresh_;
  bool nested_;
  bool out_of_place_;
  bool skip_ext_data_;
  int64_t refresh_parallel_;
  bool nested_consistent_refresh_;
};

class ObMViewRefreshExecutor
{
public:
  ObMViewRefreshExecutor();
  ~ObMViewRefreshExecutor();
  DISABLE_COPY_ASSIGN(ObMViewRefreshExecutor);

  int execute(sql::ObExecContext &ctx, const ObMViewRefreshArg &arg);
  share::schema::ObMViewInfo &get_first_mview_info() {
    return mview_infos_.at(0);
  }
private:
  int resolve_arg(const ObMViewRefreshArg &arg);
  int do_refresh();
  int do_nested_refresh_();
  int get_and_check_mview_database_schema(share::schema::ObSchemaGetterGuard *schema_guard,
                                          const uint64_t mview_id,
                                          const share::schema::ObDatabaseSchema *&database_schema);
  int sync_check_nested_mview_mds(const uint64_t mview_id,
                                  const uint64_t refresh_id,
                                  const share::SCN &target_data_sync_scn);
  int register_nested_mview_mds_(const uint64_t mview_id,
                                 const uint64_t refresh_id,
                                 const ObIArray<uint64_t> &nest_mview_ids,
                                 const share::SCN &target_data_sync_scn,
                                 ObMySQLTransaction &trans);
  int register_nested_mview_mds_and_check(const uint64_t mview_id,
                                          const uint64_t refresh_id,
                                          const ObIArray<uint64_t> &nest_mview_ids,
                                          const share::SCN &target_data_sync_scn,
                                          ObMySQLTransaction &trans);
  int scheduler_nested_mviews_sync_refresh_(const uint64_t target_mview_id,
                                            const uint64_t refresh_id,
                                            const share::SCN &target_data_sync_scn,
                                            const ObIArray<uint64_t> &nested_mview_ids,
                                            rootserver::MViewDeps &target_mview_deps,
                                            rootserver::MViewDeps &mview_reverse_deps,
                                            ObMySQLTransaction &trans);
  int scheduler_nested_mviews_refresh_(const ObIArray<uint64_t> &nested_mview_ids);
  int check_register_new_mview_list_(const uint64_t target_mview_id,
                                     const uint64_t mview_id,
                                     const uint64_t refresh_id,
                                     const share::SCN &target_data_sync_scn,
                                     const rootserver::MViewDeps &target_mview_deps,
                                     const rootserver::MViewDeps &mview_reverse_deps,
                                     ObMySQLTransaction &trans,
                                     hash::ObHashSet<uint64_t> &mv_sets);
  int generate_database_table_name_(const ObTableSchema *table_schema,
                                    ObSqlString &table_name);
  int set_session_vars_(const uint64_t mview_id,
                        ObMViewTransaction &trans);
private:
  sql::ObExecContext *ctx_;
  const ObMViewRefreshArg *arg_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObSchemaChecker schema_checker_;

  uint64_t tenant_id_;
  ObArray<uint64_t> mview_ids_;
  ObArray<share::schema::ObMVRefreshMethod> refresh_methods_;
  ObArray<share::schema::ObMViewInfo> mview_infos_;
};

} // namespace storage
} // namespace oceanbase
