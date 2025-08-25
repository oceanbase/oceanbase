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

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "pl/ob_pl_type.h"
#include "share/diagnosis/ob_profile_util.h"

namespace oceanbase
{
namespace common
{
class ObObj;
class ObProfileItem;

} // namespace common
namespace sql
{
class ObExecContext;
typedef common::ParamStore ParamStore;
} // namespace sql

namespace pl
{
enum class ProfileDisplayType
{
  AGGREGATED = 0,
  ORIGINAL,
};

struct ProfileText
{
  char *buf_{nullptr};
  int64_t buf_len_{0};
  int64_t pos_{0};
  ProfileDisplayType type_{ProfileDisplayType::AGGREGATED};
  metric::Level display_level_{metric::Level::STANDARD};
};

class ObDbmsXprofile
{
public:
  static int display_profile(sql::ObExecContext &ctx, sql::ParamStore &params,
                             common::ObObj &result);

private:
  static int set_display_type(const ObString &format, ProfileDisplayType &type);
  static int set_display_level(int64_t level, metric::Level &display_level);

  static int format_profile_result(sql::ObExecContext &ctx, ObIArray<ObProfileItem> &profile_items,
                                   ProfileText &profile_text);

  static int flatten_op_profile(const ObIArray<ObProfileItem> &profile_items,
                                ProfileText &profile_text);

  static int aggregate_op_profile(const ObIArray<ObProfileItem> &profile_items,
                                  ProfileText &profile_text);

  static int format_summary_info(const ObIArray<ObProfileItem> &profile_items,
                                 ProfileText &profile_text);

  static int format_agg_profiles(const ObIArray<ObProfileItem> &profile_items,
                                 ProfileText &profile_text);

  static int format_one_agg_profile(const ObIArray<ObProfileItem> &profile_items, int64_t start_idx,
                                    int end_idx, ProfileText &profile_text);

  static int set_display_result(sql::ObExecContext &ctx, ProfileText &profile_text,
                                common::ObObj &result);

  static int set_display_result_for_oracle(sql::ObExecContext &ctx, ProfileText &profile_text,
                                           common::ObObj &result);

  static int profile_text_to_strings(ProfileText &profile_text,
                                     ObIArray<common::ObString> &profile_strs);

  static int set_display_result_for_mysql(sql::ObExecContext &ctx, ProfileText &profile_text,
                                          common::ObObj &result);
};

} // namespace pl
} // namespace oceanbase
