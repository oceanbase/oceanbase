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

#pragma once

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_sql_string.h"
#include "share/diagnosis/ob_runtime_metrics.h"

namespace oceanbase
{
namespace common
{
template<typename MetricType>
class ObOpProfile;
typedef ObOpProfile<ObMetric> ObProfile;

namespace sqlclient
{
class ObMySQLResult;
} // namespace sqlclient

struct ObProfileItem
{
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    return 0;
  };
  int64_t op_id_{0};
  int64_t plan_depth_{0};
  int64_t db_time_{0};
  ObProfile *profile_{nullptr};
  char *raw_profile_{nullptr};
  int64_t raw_profile_len_{0};
};

class ObProfileUtil
{
public:
  // session_tenant_id used to launch inner sql
  // param_tenant_id used to fetch sql plan monitor in where condition
  static int get_profile_by_id(ObIAllocator *alloc, int64_t session_tenant_id,
                               const ObString &trace_id, const ObString &svr_ip, int64_t svr_port,
                               int64_t param_tenant_id, bool fetch_all_op, int64_t op_id,
                               ObIArray<ObProfileItem> &profile_items);

private:
  static int inner_get_profile(ObIAllocator *alloc, int64_t tenant_id, const ObSqlString &sql,
                               ObIArray<ObProfileItem> &profile_items);

  static int read_profile_from_result(ObIAllocator *alloc,
                                      const common::sqlclient::ObMySQLResult &mysql_result,
                                      ObProfileItem &profile_item);
};

} // namespace common
} // namespace oceanbase
