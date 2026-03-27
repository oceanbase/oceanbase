/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
        SQL_LOG(WARN, "kill query failed", K(ret), K(session_->get_server_sid()));
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
