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

#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{
class ObMViewStatsPurgeRefreshStatsExecutor
{
public:
  ObMViewStatsPurgeRefreshStatsExecutor();
  ~ObMViewStatsPurgeRefreshStatsExecutor();
  DISABLE_COPY_ASSIGN(ObMViewStatsPurgeRefreshStatsExecutor);

  int execute(sql::ObExecContext &ctx, int64_t retention_period = INT64_MAX);

private:
  sql::ObExecContext *ctx_;
  sql::ObSQLSessionInfo *session_info_;

  uint64_t tenant_id_;
  int64_t retention_period_;
};

} // namespace storage
} // namespace oceanbase
