/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
