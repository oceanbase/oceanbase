/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

