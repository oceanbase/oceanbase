/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "share/schema/ob_mview_refresh_stats.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{
struct ObMViewStatsPurgeRefreshStatsArg
{
public:
  ObMViewStatsPurgeRefreshStatsArg() : retention_period_(INT64_MAX) {}
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(mv_list), K_(retention_period));

public:
  ObString mv_list_;
  int64_t retention_period_;
};

class ObMViewStatsPurgeRefreshStatsExecutor
{
public:
  ObMViewStatsPurgeRefreshStatsExecutor();
  ~ObMViewStatsPurgeRefreshStatsExecutor();
  DISABLE_COPY_ASSIGN(ObMViewStatsPurgeRefreshStatsExecutor);

  int execute(sql::ObExecContext &ctx, const ObMViewStatsPurgeRefreshStatsArg &arg);

private:
  int resolve_arg(const ObMViewStatsPurgeRefreshStatsArg &arg);
  int purge_refresh_stats(const share::schema::ObMViewRefreshStats::FilterParam &filter_param);

private:
  static const int64_t PURGE_BATCH_COUNT = 1000;
  enum class OpType
  {
    PURGE_ALL_REFRESH_STATS = 0,
    PURGE_SPECIFY_REFRESH_STATS = 1,
    MAX
  };

private:
  sql::ObExecContext *ctx_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObSchemaChecker schema_checker_;

  uint64_t tenant_id_;
  OpType op_type_;
  ObArray<uint64_t> mview_ids_;
  int64_t retention_period_;
};

} // namespace storage
} // namespace oceanbase
