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
class ObSQLSessionInfo;
} // namespace sql
namespace storage
{
struct ObMViewStatsSetSystemDefaultArg
{
public:
  ObMViewStatsSetSystemDefaultArg() : retention_period_(INT64_MAX) {}
  bool is_valid() const { return !parameter_name_.empty(); }
  TO_STRING_KV(K_(parameter_name), K_(collection_level), K_(retention_period));

public:
  ObString parameter_name_;
  ObString collection_level_;
  int64_t retention_period_;
};

class ObMViewStatsSetSystemDefaultExecutor
{
public:
  ObMViewStatsSetSystemDefaultExecutor();
  ~ObMViewStatsSetSystemDefaultExecutor();
  DISABLE_COPY_ASSIGN(ObMViewStatsSetSystemDefaultExecutor);

  int execute(sql::ObExecContext &ctx, const ObMViewStatsSetSystemDefaultArg &arg);

private:
  int resolve_arg(const ObMViewStatsSetSystemDefaultArg &arg);

private:
  enum class OpType
  {
    SET_COLLECTION_LEVEL = 0,
    SET_RETENTION_PERIOD = 1,
    MAX
  };

private:
  sql::ObExecContext *ctx_;
  sql::ObSQLSessionInfo *session_info_;

  uint64_t tenant_id_;
  OpType op_type_;
  share::schema::ObMVRefreshStatsCollectionLevel collection_level_;
  int64_t retention_period_;
};

} // namespace storage
} // namespace oceanbase
