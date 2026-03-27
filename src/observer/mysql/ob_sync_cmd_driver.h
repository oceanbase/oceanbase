/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_SYNC_CMD_DRIVER_
#define OCEANBASE_OBSERVER_MYSQL_SYNC_CMD_DRIVER_

#include "observer/mysql/ob_query_driver.h"
#include "rpc/obmysql/packet/ompk_eof.h"

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
class ObSyncCmdDriver : public ObQueryDriver
{
public:
  ObSyncCmdDriver(const ObGlobalContext &gctx,
                  const sql::ObSqlCtx &ctx,
                  sql::ObSQLSessionInfo &session,
                  ObQueryRetryCtrl &retry_ctrl,
                  ObIMPPacketSender &sender,
                  bool is_prexecute = false);
  virtual ~ObSyncCmdDriver();

  virtual int response_query_result(sql::ObResultSet &result,
                                    bool is_ps_protocol,
                                    bool has_more_result,
                                    bool &can_retry,
                                    int64_t fetch_limit  = common::OB_INVALID_COUNT);
  virtual int response_result(ObMySQLResultSet &result);

private:
  /* functions */
  int process_schema_version_changes(const ObMySQLResultSet &result);
  int check_and_refresh_schema(uint64_t tenant_id);
  int response_query_result(ObMySQLResultSet &result);
  void free_output_row(ObMySQLResultSet &result);
  /* variables */
  /* const */
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObSyncCmdDriver);
};


}
}
#endif /* OCEANBASE_OBSERVER_MYSQL_SYNC_CMD_DRIVER_ */
//// end of header file

