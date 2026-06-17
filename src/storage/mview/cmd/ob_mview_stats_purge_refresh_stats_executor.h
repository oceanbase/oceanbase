/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
