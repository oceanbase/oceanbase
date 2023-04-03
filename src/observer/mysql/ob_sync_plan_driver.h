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

#ifndef OCEANBASE_OBSERVER_MYSQL_SYNC_PLAN_DRIVER_
#define OCEANBASE_OBSERVER_MYSQL_SYNC_PLAN_DRIVER_

#include "observer/mysql/ob_query_driver.h"
#include "common/object/ob_object.h"

namespace oceanbase
{

namespace sql
{
struct ObSqlCtx;
class ObSQLSessionInfo;
class ObPhysicalPlan;
class ObExecContext;
}

namespace observer
{

class ObIMPPacketSender;
struct ObGlobalContext;
class ObMySQLResultSet;
class ObQueryRetryCtrl;
class ObSyncPlanDriver : public ObQueryDriver
{
public:
  ObSyncPlanDriver(const ObGlobalContext &gctx,
                   const sql::ObSqlCtx &ctx,
                   sql::ObSQLSessionInfo &session,
                   ObQueryRetryCtrl &retry_ctrl,
                   ObIMPPacketSender &sender,
                   bool is_prexecute = false,
                   int32_t iteration_count = common::OB_INVALID_COUNT);
  virtual ~ObSyncPlanDriver();

  virtual int response_result(ObMySQLResultSet &result);
protected:
  int enter_query_admission(sql::ObSQLSessionInfo &session,
                            sql::ObExecContext &exec_ctx,
                            sql::ObPhysicalPlan &plan,
                            int64_t &worker_count);
  void exit_query_admission(int64_t worker_count);

  /* disallow copy & assign */
  int32_t iteration_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSyncPlanDriver);
};
}
}
#endif /* OCEANBASE_OBSERVER_MYSQL_SYNC_PLAN_DRIVER_ */
//// end of header file
