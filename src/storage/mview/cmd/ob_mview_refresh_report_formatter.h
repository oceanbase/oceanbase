/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/string/ob_sql_string.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{
class ObTimeZoneInfo;
} // namespace common
namespace storage
{
struct MViewRefreshReport;

int ob_mview_refresh_report_format_text_report(const MViewRefreshReport &report,
                                               const common::ObTimeZoneInfo *sys_tz_info,
                                               common::ObSqlString &report_text);
int ob_mview_refresh_report_format_json_report(const MViewRefreshReport &report,
                                               const common::ObTimeZoneInfo *sys_tz_info,
                                               common::ObSqlString &report_text);

} // namespace storage
} // namespace oceanbase
