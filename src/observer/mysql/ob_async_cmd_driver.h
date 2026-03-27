/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_ASYNC_CMD_DRIVER_
#define OCEANBASE_OBSERVER_MYSQL_ASYNC_CMD_DRIVER_

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
class ObAsyncCmdDriver : public ObQueryDriver
{
public:
  ObAsyncCmdDriver(const ObGlobalContext &gctx,
                  const sql::ObSqlCtx &ctx,
                  sql::ObSQLSessionInfo &session,
                  ObQueryRetryCtrl &retry_ctrl,
                  ObIMPPacketSender &sender,
                  bool is_prexecute = false);
  virtual ~ObAsyncCmdDriver();

  virtual int response_result(ObMySQLResultSet &result);
  int flush_buffer(bool is_last);

private:
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObAsyncCmdDriver);
};


}
}
#endif /* OCEANBASE_OBSERVER_MYSQL_ASYNC_CMD_DRIVER_ */
//
