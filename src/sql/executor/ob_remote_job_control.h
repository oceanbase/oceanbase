/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_OB_REMOTE_JOB_CONTROL_
#define OCEANBASE_SQL_EXECUTOR_OB_REMOTE_JOB_CONTROL_

#include "sql/executor/ob_job_control.h"

namespace oceanbase
{
namespace sql
{
class ObRemoteJobControl : public ObJobControl
{
public:
  explicit ObRemoteJobControl();
  virtual ~ObRemoteJobControl();

  virtual int get_ready_jobs(common::ObIArray<ObJob*> &jobs, 
      bool serial_schedule = false) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteJobControl);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_REMOTE_JOB_CONTROL_ */
