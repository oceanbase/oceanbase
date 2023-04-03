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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_TASK_RUNNER_NOTIFIER_
#define OCEANBASE_SQL_EXECUTOR_OB_TASK_RUNNER_NOTIFIER_

#include "share/ob_define.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace sql
{
class ObTaskRunnerNotifier
{
public:
  ObTaskRunnerNotifier(ObSQLSessionInfo *session, ObSQLSessionMgr *mgr)
      : session_(session), mgr_(mgr)
  {
  }

  virtual ~ObTaskRunnerNotifier() {}

  virtual int kill()
  {
    int ret = OB_SUCCESS;
    if (NULL != mgr_ && NULL != session_) {
      if (OB_FAIL(mgr_->kill_query(*session_))) {
        SQL_LOG(WARN, "kill query failed", K(ret), K(session_->get_sessid()));
      }
    }
    return ret;
  }

private:
  ObSQLSessionInfo *session_;
  ObSQLSessionMgr *mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTaskRunnerNotifier);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_TASK_RUNNER_NOTIFIER_ */
