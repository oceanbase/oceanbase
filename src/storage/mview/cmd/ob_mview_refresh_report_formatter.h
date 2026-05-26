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
