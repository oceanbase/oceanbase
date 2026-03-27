/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_ASYNC_PLAN_DRIVER_
#define OCEANBASE_OBSERVER_MYSQL_ASYNC_PLAN_DRIVER_

#include "observer/mysql/ob_query_driver.h"

namespace oceanbase
{

namespace sql
{
struct ObSqlCtx;
class ObSQLSessionInfo;
}


namespace observer
{

class ObIMPPacketSender;
class ObMySQLResultSet;
class ObQueryRetryCtrl;
class ObSqlEndTransCb;
class ObAsyncPlanDriver : public ObQueryDriver
{
public:
  ObAsyncPlanDriver(const ObGlobalContext &gctx,
                    const sql::ObSqlCtx &ctx,
                    sql::ObSQLSessionInfo &session,
                    ObQueryRetryCtrl &retry_ctrl,
                    ObIMPPacketSender &sender,
                    bool is_prexecute = false);
  virtual ~ObAsyncPlanDriver();

  virtual int response_result(ObMySQLResultSet &result);

private:
  /* functions */
  /* variables */
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObAsyncPlanDriver);
};


}
}
#endif /* OCEANBASE_OBSERVER_MYSQL_ASYNC_PLAN_DRIVER_ */
//// end of header file

