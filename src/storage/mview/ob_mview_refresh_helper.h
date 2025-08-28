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

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/number/ob_number_v2.h"
#include "lib/ob_define.h"
#include "share/ob_table_range.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace common
{
class ObSqlString;
} // namespace common
namespace transaction
{
namespace tablelock
{
class ObLockObjRequest;
} // namespace tablelock
} // namespace transaction
namespace storage
{
class ObMViewTransaction;

class ObMViewRefreshHelper
{
public:
  static int get_current_scn(share::SCN &current_scn);

  static int lock_mview(ObMViewTransaction &trans, const uint64_t tenant_id,
                        const uint64_t mview_id, const bool try_lock = false);

  static int generate_purge_mlog_sql(share::schema::ObSchemaGetterGuard &schema_guard,
                                     const uint64_t tenant_id, const uint64_t mlog_id,
                                     const share::SCN &purge_scn, const int64_t purge_log_parallel,
                                     ObSqlString &sql_string);

  static int get_table_row_num(ObMViewTransaction &trans, const uint64_t tenant_id,
                               const uint64_t table_id, const share::SCN &scn, int64_t &num_rows);

  static int get_mlog_dml_row_num(ObMViewTransaction &trans, const uint64_t tenant_id,
                                  const uint64_t table_id, const share::ObScnRange &scn_range,
                                  int64_t &num_rows_ins, int64_t &num_rows_upd,
                                  int64_t &num_rows_del);
  static int sync_post_nested_mview_rpc(obrpc::ObCheckNestedMViewMdsArg &arg,
                                        obrpc::ObCheckNestedMViewMdsRes &res);
  static int check_dep_mviews_satisfy_target_scn(const uint64_t tenant_id,
                                                 const share::SCN &target_data_sync_scn,
                                                 const share::SCN &read_snapshot,
                                                 const ObIArray<uint64_t> &dep_mview_ids,
                                                 common::ObISQLClient &sql_proxy,
                                                 bool &satisfy,
                                                 bool oracle_mode = false);
  static int get_dep_mviews_from_dep_info(const uint64_t tenant_id,
                                          const ObIArray<share::schema::ObDependencyInfo> &dependency_infos,
                                          ObSchemaGetterGuard &schema_guard,
                                          ObIArray<uint64_t> &dep_mview_ids);
  static int collect_deps_and_check_satisfy(const uint64_t tenant_id,
                                            const uint64_t mview_id,
                                            const uint64_t target_data_sync_ts,
                                            const uint64_t snapshot_version,
                                            common::ObISQLClient &sql_proxy,
                                            ObSchemaGetterGuard &schema_guard,
                                            bool oracle_mode = false);
  
  static int replace_all_snapshot_zero(const std::string &input,
                                       const uint64_t snapshot_version,
                                       std::string &output,
                                       const bool oracle_mode);
  static int sync_get_min_target_data_sync_scn(const uint64_t tenant_id,
                                               const uint64_t mview_id,
                                               share::SCN &min_target_scn);
};

} // namespace storage
} // namespace oceanbase
