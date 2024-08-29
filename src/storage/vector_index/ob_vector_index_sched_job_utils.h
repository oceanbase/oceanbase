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
#include "storage/mview/ob_mview_sched_job_utils.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObSchemaGetterGuard;
}
} // namespace share
namespace common {
class ObIAllocator;
class ObISQLClient;
class ObObj;
class ObString;
} // namespace common
namespace sql {
class ObResolverParams;
class ObSQLSessionInfo;
} // namespace sql
namespace dbms_scheduler {
class ObDBMSSchedJobInfo;
}
namespace storage {
class ObVectorIndexSchedJobUtils : public ObMViewSchedJobUtils {
public:
  static constexpr char *VETCOR_INDEX_REFRESH_JOB_PREFIX =
      const_cast<char *>("VECTOR_INDEX_REFRESH$J_");
  static constexpr int64_t DEFAULT_REFRESH_INTERVAL_TS =
      10 * 60 * 1000000; // 10min
  static constexpr int64_t DEFAULT_REFRESH_TRIGGER_THRESHOLD = 10000;
  ObVectorIndexSchedJobUtils() : ObMViewSchedJobUtils() {}
  virtual ~ObVectorIndexSchedJobUtils() {}

  static int add_scheduler_job(common::ObISQLClient &sql_client,
                               const uint64_t tenant_id, const int64_t job_id,
                               const common::ObString &job_name,
                               const common::ObString &job_action,
                               const common::ObObj &start_date,
                               const int64_t repeat_interval_ts,
                               const common::ObString &exec_env);

  static int add_vector_index_refresh_job(common::ObISQLClient &sql_client,
                                          const uint64_t tenant_id,
                                          const common::ObString &vec_id_index_tb_name,
                                          const common::ObString &db_name,
                                          const common::ObString &table_name,
                                          const common::ObString &index_name,
                                          const common::ObString &exec_env);

  static int remove_vector_index_refresh_job(common::ObISQLClient &sql_client,
                                             const uint64_t tenant_id,
                                             const common::ObString &vec_id_index_tb_name);
};

} // namespace storage
} // namespace oceanbase