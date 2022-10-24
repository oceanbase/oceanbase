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

#ifndef OCEANBASE_UNITEST_LOGSERVICE_MOCK_OB_I_LOG_CTX_
#define OCEANBASE_UNITEST_LOGSERVICE_MOCK_OB_I_LOG_CTX_
#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/ob_log_handler.h"

namespace oceanbase
{
namespace logservice
{
class MockLogCtx : public AppendCb, public ObLink
{
public:
  MockLogCtx() {}
  virtual ~MockLogCtx() {}
public:
  void set_ctx(const int64_t ctx)
  {
    ctx_ = ctx;
  }
  int on_success()
  {
    int ret = OB_SUCCESS;
    usleep(15 * 1000 * 1000);
    REPLAY_LOG(TRACE, "log ctx on success", K(ret), K(ctx_));
    return ret;
  }
  int on_failure()
  {
    return OB_SUCCESS;
  }
  int64_t get_execute_hint()
  {
    return 0;
  }
private:
  int64_t ctx_;
};
} // end namespace logservice
} // end namespace oceanbase
#endif // OCEANBASE_UNITEST_LOGSERVICE_MOCK_OB_I_LOG_CTX_
