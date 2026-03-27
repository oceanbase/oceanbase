/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_LOB_CHECK_JOB_SCHEDULER_H_
#define OCEANBASE_SHARE_OB_LOB_CHECK_JOB_SCHEDULER_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace share
{
static const char *LOB_CHECK_JOB_NAME = "lob_check_job";

class ObLobCheckJobScheduler
{
public:
  // create LOB consistency check job when tenant is created
  static int create_lob_check_job(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

private:
  static int create_job_(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);
};

} // end of share
} // end of oceanbase

#endif // OCEANBASE_SHARE_OB_LOB_CHECK_JOB_SCHEDULER_H_
