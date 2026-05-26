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
