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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_flashback_standby_log_executor.h"

#include "sql/engine/ob_exec_context.h"
#include "rootserver/standby/ob_flashback_standby_log_command.h"

namespace oceanbase
{
namespace sql
{
int ObFlashbackStandbyLogExecutor::execute(ObExecContext& ctx, ObFlashbackStandbyLogStmt& stmt)
{
  int ret = OB_SUCCESS;
  const ObFlashbackStandbyLogArg &arg = stmt.get_arg();
  rootserver::ObFlashbackStandbyLogCommand command;
  //left 200ms to return result
  const int64_t remain_timeout_interval_us = THIS_WORKER.get_timeout_remain();
  const int64_t execute_timeout_interval_us = remain_timeout_interval_us - 200 * 1000;
  const int64_t original_timeout_abs_us = THIS_WORKER.get_timeout_ts();
  if (0 < execute_timeout_interval_us) {
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + execute_timeout_interval_us);
  }

  if (OB_FAIL(command.execute(arg))) {
    LOG_WARN("fail to execute command", KR(ret), K(arg));
  }

  //set timeout back
  if (0 < execute_timeout_interval_us) {
    THIS_WORKER.set_timeout_ts(original_timeout_abs_us);
  }
  return ret;
}
} // sql
} // oceanbase