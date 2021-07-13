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

#ifndef OCEANBASE_OBSERVER_MYSQL_SYNC_CMD_DRIVER_
#define OCEANBASE_OBSERVER_MYSQL_SYNC_CMD_DRIVER_

#include "observer/mysql/ob_query_driver.h"

namespace oceanbase {

namespace sql {
class ObSqlCtx;
class ObSQLSessionInfo;
}  // namespace sql

namespace observer {

class ObIMPPacketSender;
class ObGlobalContext;
class ObMySQLResultSet;
class ObQueryRetryCtrl;
class ObSyncCmdDriver : public ObQueryDriver {
public:
  ObSyncCmdDriver(const ObGlobalContext& gctx, const sql::ObSqlCtx& ctx, sql::ObSQLSessionInfo& session,
      ObQueryRetryCtrl& retry_ctrl, ObIMPPacketSender& sender);
  virtual ~ObSyncCmdDriver();

  void send_eof_packet();
  virtual int response_result(ObMySQLResultSet& result);

private:
  /* functions */
  int process_schema_version_changes(const ObMySQLResultSet& result);
  int check_and_refresh_schema(uint64_t tenant_id);
  int response_query_result(ObMySQLResultSet& result);
  /* variables */
  /* const */
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObSyncCmdDriver);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_MYSQL_SYNC_CMD_DRIVER_ */
//// end of header file
