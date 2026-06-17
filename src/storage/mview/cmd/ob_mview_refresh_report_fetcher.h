/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{
struct MViewReportContext;

class ObMViewRefreshReportFetcher
{
public:
  static int fetch_all(sql::ObExecContext &ctx,
                       uint64_t conn_tenant_id,
                       uint64_t target_tenant_id,
                       int64_t refresh_id,
                       MViewReportContext &context);
};

} // namespace storage
} // namespace oceanbase
