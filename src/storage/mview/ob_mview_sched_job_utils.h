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

#include "lib/ob_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMVRefreshInfo;
class ObSchemaGetterGuard;
class ObUserInfo;
}
}
namespace common
{
class ObIAllocator;
class ObISQLClient;
class ObObj;
class ObString;
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace dbms_scheduler
{
class ObDBMSSchedJobInfo;
}
namespace storage
{
class ObMViewSchedJobUtils
{
public:
  ObMViewSchedJobUtils() {}
  virtual ~ObMViewSchedJobUtils() {}

  static int generate_job_id(const uint64_t tenant_id,
                             int64_t &job_id);
  static int generate_job_name(common::ObIAllocator &allocator,
                               const int64_t job_id,
                               const common::ObString &name_prefix,
                               common::ObString &job_name);
  static int generate_job_action(common::ObIAllocator &allocator,
                                 const common::ObString &job_action_func,
                                 const common::ObString &db_name,
                                 const common::ObString &table_name,
                                 common::ObString &job_action);
  static int add_scheduler_job(common::ObISQLClient &sql_client,
                               const uint64_t tenant_id,
                               const int64_t job_id,
                               const common::ObString &job_name,
                               const common::ObString &job_action,
                               const common::ObObj &start_date,
                               const common::ObString &repeat_interval,
                               const common::ObString &exec_env);

  static int add_mview_info_and_refresh_job(common::ObISQLClient &sql_client,
                                            const uint64_t tenant_id,
                                            const uint64_t mview_id,
                                            const common::ObString &db_name,
                                            const common::ObString &table_name,
                                            const share::schema::ObMVRefreshInfo *refresh_info,
                                            const int64_t schema_version);

  static int disable_mview_refresh_job(common::ObISQLClient &sql_client,
                                       const uint64_t tenant_id,
                                       const uint64_t table_id);

  static int disable_mlog_purge_job(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const uint64_t table_id);

  static int calc_date_expression_from_str(sql::ObSQLSessionInfo &session,
                                           common::ObIAllocator &allocator,
                                           const uint64_t tenant_id,
                                           const ObString &date_str,
                                           common::ObObj &date_obj);
  static int convert_session_date_to_utc(sql::ObSQLSessionInfo *session_info,
                                         const common::ObObj &in_obj,
                                         common::ObObj &out_obj);
  static int calc_date_expression(dbms_scheduler::ObDBMSSchedJobInfo &job_info,
                                  int64_t &next_date_utc_ts);

public:
  static constexpr int64_t JOB_ID_OFFSET = 1000000L;
};
} // namespace storage
} // namespace oceanbase
