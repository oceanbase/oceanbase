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

#define USING_LOG_PREFIX SHARE
#include "share/ob_share_util.h"
#include "common/ob_timeout_ctx.h"
#include "share/ob_worker.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log_module.h"
namespace oceanbase {
using namespace common;
namespace share {
int ObShareUtil::set_default_timeout_ctx(ObTimeoutCtx &ctx, const int64_t default_timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout_ts = OB_INVALID_TIMESTAMP;
  int64_t ctx_timeout_ts = ctx.get_abs_timeout();
  int64_t worker_timeout_ts = THIS_WORKER.get_timeout_ts();
  if (0 < ctx_timeout_ts) {
    // ctx has already been set, use it
    abs_timeout_ts = ctx_timeout_ts;
  } else if (INT64_MAX == worker_timeout_ts) {
    // if worker's timeout_ts has not been set，change to default_timeout
    abs_timeout_ts = ObTimeUtility::current_time() + default_timeout;
  } else if (0 < worker_timeout_ts) {
    // use worker's timeout if only it is valid
    abs_timeout_ts = worker_timeout_ts;
  } else {
    // worker's timeout_ts is invalid, set to default timeout
    abs_timeout_ts = ObTimeUtility::current_time() + default_timeout;
  }
  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_ts))) {
    LOG_WARN(
        "set timeout failed", KR(ret), K(abs_timeout_ts), K(ctx_timeout_ts), K(worker_timeout_ts), K(default_timeout));
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout", KR(ret), K(abs_timeout_ts), K(ctx_timeout_ts), K(worker_timeout_ts), K(default_timeout));
  } else {
    LOG_DEBUG("set_default_timeout_ctx success",
        K(abs_timeout_ts),
        K(ctx_timeout_ts),
        K(worker_timeout_ts),
        K(default_timeout));
  }
  return ret;
}
}  // end namespace share
}  // end namespace oceanbase