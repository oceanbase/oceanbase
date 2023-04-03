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
struct ObGlobalContext;
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

private:
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObAsyncCmdDriver);
};


}
}
#endif /* OCEANBASE_OBSERVER_MYSQL_ASYNC_CMD_DRIVER_ */
//
