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
struct ObProfileItem;
struct ObMergedProfileItem;

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
  AGGREGATED = 0,    // Display aggregated profile with detail info, with raw json format
  ORIGINAL,          // Display original profile without aggregation, with raw json format
  AGGREGATED_PRETTY, // Display aggregated profile with pretty format
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
                                   const ObString &trace_id, ProfileText &profile_text);

  static int flatten_op_profile(const ObIArray<ObProfileItem> &profile_items,
                                ProfileText &profile_text);

  static int aggregate_op_profile(sql::ObExecContext &ctx,
                                  const ObIArray<ObProfileItem> &profile_items,
                                  const ObString &trace_id, ProfileText &profile_text);

  static int format_summary_info(const ObMergedProfileItem *compile_profile,
                                 const ObIArray<ObMergedProfileItem> &merged_items,
                                 int64_t execution_count, ProfileText &profile_text);

  static int format_agg_profiles(const ObIArray<ObMergedProfileItem> &merged_items,
                                 ProfileText &profile_text);

  static int set_display_result(sql::ObExecContext &ctx, ProfileText &profile_text,
                                common::ObObj &result);

  static int set_display_result_for_oracle(sql::ObExecContext &ctx, ProfileText &profile_text,
                                           common::ObObj &result);

  static int profile_text_to_strings(ProfileText &profile_text,
                                     ObIArray<common::ObString> &profile_strs);

  static int set_display_result_for_mysql(sql::ObExecContext &ctx, ProfileText &profile_text,
                                          common::ObObj &result);
};

// for pretry print
class ProfilePrefixHelper
{
public:
  struct PrefixInfo
  {
    TO_STRING_KV(K(op_id_));
    int64_t plan_depth_;
    int64_t op_id_{-1};
    int64_t parent_op_id_{-1};
    int64_t last_child_op_id_{-1};
    ObString profile_prefix_;
    ObString metric_prefix_;
  };

  struct Ancestor
  {
    TO_STRING_KV(K(op_id_));
    int64_t op_id_{-1};
    bool last_child_processed_{false};
  };

  ProfilePrefixHelper(ObIAllocator &allocator)
      : allocator_(allocator), prefix_infos_(), ancestors_stack_() {}
  int prepare_pretty_prefix(const ObIArray<ObMergedProfileItem> &merged_items);
  const ObIArray<PrefixInfo> &get_prefixs() const { return prefix_infos_; }
private:
  int append_profile_prefix(PrefixInfo &current_profile, int64_t current_depth);
  int append_metric_prefix(PrefixInfo &current_profile, int64_t current_depth);
private:
  ObIAllocator &allocator_;
  ObSEArray<PrefixInfo, 4> prefix_infos_;
  // each element reference to an ancestor of current operator
  ObSEArray<Ancestor, 4> ancestors_stack_;
};


} // namespace pl
} // namespace oceanbase
