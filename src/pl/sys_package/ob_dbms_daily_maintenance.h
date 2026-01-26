
/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DAILY_MAINTENANCE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DAILY_MAINTENANCE_H_
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSDailyMaintenance
{
public:
  static int trigger_window_compaction_proc(sql::ObExecContext &ctx,
                                            sql::ParamStore &params,
                                            common::ObObj &result);
  static int set_thread_count(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);
private:
  static constexpr int64_t USES_PER_SEC = 1000 * 1000L;
  static constexpr int64_t DEFAULT_RETRY_TIMEOUT_US = 24 * 60 * 60 * USES_PER_SEC; // 24h
  static constexpr int64_t BASE_RETRY_SLEEP_DURATION_US = 5 * USES_PER_SEC; // 5s
  static constexpr int64_t MAX_RETRY_SLEEP_DURATION_US = 120 * USES_PER_SEC; // 120s
private:
  static int check_supported(const uint64_t tenant_id);
};

} // end of pl
} // end of oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DAILY_MAINTENANCE_H_