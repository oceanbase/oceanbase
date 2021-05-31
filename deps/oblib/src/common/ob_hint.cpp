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

#include "common/ob_hint.h"

namespace oceanbase {
namespace common {

void ObTableScanHint::reset()
{
  max_parallel_count_ = OB_DEFAULT_MAX_PARALLEL_COUNT;
  timeout_us_ = OB_DEFAULT_STMT_TIMEOUT;
  enable_parallel_ = true;
  frozen_version_ = -1;
  read_consistency_ = INVALID_CONSISTENCY;
  force_refresh_lc_ = false;
}

int64_t ObTableScanHint::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(max_parallel_count),
      K_(enable_parallel),
      N_TIMEOUT,
      timeout_us_,
      N_FROZEN_VERSION,
      frozen_version_,
      N_CONSISTENCY_LEVEL,
      get_consistency_level_str(read_consistency_),
      K_(force_refresh_lc));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObTableScanHint, max_parallel_count_, enable_parallel_, timeout_us_, frozen_version_,
    read_consistency_, force_refresh_lc_);

int64_t ObTableScanHint::hint_to_string(char* buf, const int64_t buf_len, int64_t& pos) const
{
  bool has_pre = false;
  if (OB_DEFAULT_MAX_PARALLEL_COUNT != max_parallel_count_) {
    BUF_PRINTF(has_pre ? ", " : "");
    J_KV(K_(max_parallel_count));
    has_pre = true;
  }
  if (!enable_parallel_) {
    BUF_PRINTF(has_pre ? ", " : "");
    J_KV(K_(enable_parallel));
    has_pre = true;
  }
  if (OB_DEFAULT_STMT_TIMEOUT != timeout_us_) {
    BUF_PRINTF(has_pre ? ", " : "");
    J_KV(K_(timeout_us));
    has_pre = true;
  }
  if (-1 != frozen_version_) {
    BUF_PRINTF(has_pre ? ", " : "");
    J_KV(K_(frozen_version));
    has_pre = true;
  }
  if (INVALID_CONSISTENCY != read_consistency_) {
    BUF_PRINTF(has_pre ? ", " : "");
    J_KV(K_(read_consistency));
    has_pre = true;
  }
  if (force_refresh_lc_) {
    BUF_PRINTF(has_pre ? ", " : "");
    J_KV(K_(force_refresh_lc));
    has_pre = true;
  }
  return pos;
}

const char* get_consistency_level_str(ObConsistencyLevel level)
{
  const char* ret = "INVALID_CONSISTENCY";
  switch (level) {
    case FROZEN:
      ret = "FROZEN";
      break;
    case WEAK:
      ret = "WEAK";
      break;
    case STRONG:
      ret = "STRONG";
      break;
    default:
      break;
  }
  return ret;
}

ObConsistencyLevel get_consistency_level_by_str(const ObString& level)
{
  static const char* consistency_level_strs[] = {"FROZEN", "WEAK", "STRONG"};
  ObConsistencyLevel ret = INVALID_CONSISTENCY;
  for (int32_t i = 0; i < ARRAYSIZEOF(consistency_level_strs); ++i) {
    if (static_cast<int32_t>(strlen(consistency_level_strs[i])) == level.length() &&
        0 == strncasecmp(consistency_level_strs[i], level.ptr(), level.length())) {
      ret = static_cast<ObConsistencyLevel>(i);
      break;
    }
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
