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

#include "ob_cdc_util.h"

namespace oceanbase
{
namespace cdc
{
int ObExtRpcQit::init(const int64_t deadline)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(deadline <= BASE_DEADLINE)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid deadline", K(ret), K(deadline));
  } else {
    deadline_ = deadline;
  }
  return ret;
}

bool ObExtRpcQit::should_hurry_quit() const
{
  bool bool_ret = false;

  if (OB_LIKELY(common::OB_INVALID_TIMESTAMP != deadline_)) {
    int64_t now = ObTimeUtility::current_time();
    bool_ret = (now > deadline_ - RESERVED_INTERVAL);
  }

  return bool_ret;
}
} // namespace cdc
} // namespace oceanbase

