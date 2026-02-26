/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DUPLICATE_SCOPE_DEFINE_H_
#define OB_DUPLICATE_SCOPE_DEFINE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{
/* please modify ObReplicateScope and replicate_scope_strings
 * when add items to ObDuplicateScope
 */
enum class ObDuplicateScope : int64_t
{
  DUPLICATE_SCOPE_NONE = 0,
  //DUPLICATE_SCOPE_ZONE,
  //DUPLICATE_SCOPE_REGION,
  DUPLICATE_SCOPE_CLUSTER,
  DUPLICATE_SCOPE_MAX,
};

const char *const duplicate_scope_strings[] =
{
  "none",
  //"zone",
  //"region",
  "cluster"
};

enum class ObDuplicateReadConsistency : int64_t
{
  STRONG = 0,
  WEAK,
  MAX,
};

const char *const duplicate_read_consistency_strings[] =
{
  "STRONG",
  "WEAK"
};

class ObDuplicateScopeChecker
{
public:
  static bool is_valid_replicate_scope(ObDuplicateScope duplicate_scope) {
    return duplicate_scope < ObDuplicateScope::DUPLICATE_SCOPE_MAX
           && duplicate_scope >= ObDuplicateScope::DUPLICATE_SCOPE_NONE;
  }
  static int convert_duplicate_scope_string(
      const common::ObString &duplicate_scope_str,
      ObDuplicateScope &duplicate_scope) {
    int ret = common::OB_SUCCESS;
    bool found = false;
    const int64_t max = static_cast<int64_t>(ObDuplicateScope::DUPLICATE_SCOPE_MAX);
    for (int64_t i = 0; !found && i < max; ++i) {
      if (0 == common::ObString::make_string(duplicate_scope_strings[i]).case_compare(duplicate_scope_str)) {
        found = true;
        duplicate_scope = static_cast<ObDuplicateScope>(i);
      }
    }
    if (!found) {
      ret = common::OB_INVALID_ARGUMENT;
    }
    return ret;
  }
  static int convert_duplicate_scope_string(
      const char *const duplicate_scope_str,
      ObDuplicateScope &duplicate_scope) {
    int ret = common::OB_SUCCESS;
    const common::ObString this_string = common::ObString::make_string(duplicate_scope_str);
    ret = convert_duplicate_scope_string(this_string, duplicate_scope);
    return ret;
  }
  static bool is_valid_duplicate_scope(const ObDuplicateScope duplicate_scope) {
    return duplicate_scope == ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER;
  }
};

class ObDuplicateReadConsistencyChecker
{
public:
  static bool is_valid_duplicate_read_consistency(ObDuplicateReadConsistency read_consistency) {
    return read_consistency < ObDuplicateReadConsistency::MAX
           && read_consistency >= ObDuplicateReadConsistency::STRONG;
  }
  static int convert_duplicate_read_consistency_string(
      const common::ObString &duplicate_read_consistency_str,
      ObDuplicateReadConsistency &duplicate_read_consistency) {
    int ret = OB_SUCCESS;
    duplicate_read_consistency = ObDuplicateReadConsistency::MAX;
    bool found = false;
    for (int64_t idx = static_cast<int64_t>(ObDuplicateReadConsistency::STRONG);
         idx < static_cast<int64_t>(ObDuplicateReadConsistency::MAX) && !found;
         ++idx) {
      if (0 == common::ObString::make_string(duplicate_read_consistency_strings[idx]).case_compare(duplicate_read_consistency_str)) {
        found = true;
        duplicate_read_consistency = static_cast<ObDuplicateReadConsistency>(idx);
      }
    }
    if (!found) {
      ret = OB_INVALID_ARGUMENT;
    }
    return ret;
  }
  static const char* get_duplicate_read_consistency_str(const ObDuplicateReadConsistency &duplicate_read_consistency) {
    switch (duplicate_read_consistency) {
      case ObDuplicateReadConsistency::STRONG:
      case ObDuplicateReadConsistency::WEAK:
        return duplicate_read_consistency_strings[static_cast<uint64_t>(duplicate_read_consistency)];
      default:
        return "UNKNOWN";
    }
  }
};

}  // share
}  // oceanbase

#endif /* OB_DUPLICATE_SCOPE_DEFINE_H_ */
