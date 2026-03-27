/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_OB_CMD_EXECUTOR_
#define OCEANBASE_SQL_EXECUTOR_OB_CMD_EXECUTOR_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObICmd;
class ObExecContext;
class ObCmdExecutor
{
public:
  static int execute(ObExecContext &ctx, ObICmd &cmd);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObCmdExecutor);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_CMD_EXECUTOR_ */
//// end of header file
