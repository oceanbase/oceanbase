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

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_KILL_EXECUTOR_H__
#define OCEANBASE_SQL_ENGINE_CMD_OB_KILL_EXECUTOR_H__
#include "share/ob_srv_rpc_proxy.h"
namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}
namespace common
{
class ObSqlString;
class ObAddr;
}
namespace sql
{
class ObExecContext;
class ObKillStmt;
class ObSQLSessionInfo;
class ObSQLSessionMgr;
class ObKillSessionArg;

class ObKillSession
{
public:
  ObKillSession() {}
  virtual ~ObKillSession() {}
protected:
  int kill_session(const ObKillSessionArg &arg, ObSQLSessionMgr &sess_mgr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObKillSession);
};

class ObKillExecutor : public ObKillSession
{
public:
  ObKillExecutor() {}
  virtual ~ObKillExecutor() {}
  int execute(ObExecContext &ctx, ObKillStmt &stmt);
private:
  int kill_client_session(const ObKillSessionArg &arg, ObSQLSessionMgr &sess_mgr,
                          ObExecContext &ctx);
  int get_client_session_create_time_and_auth(const ObKillSessionArg &arg, ObExecContext &ctx,
                          common::ObAddr &cs_addr, int64_t &create_time);
  int get_remote_session_location(const ObKillSessionArg &arg, ObExecContext &ctx, common::ObAddr &addr, bool is_client_session = false);
  int generate_read_sql(uint32_t sess_id, common::ObSqlString &sql);
  int generate_read_sql_from_session_info(uint32_t sess_id, common::ObSqlString &sql);
  int kill_remote_session(ObExecContext &ctx, const common::ObAddr &addr, const ObKillSessionArg &arg);

  DISALLOW_COPY_AND_ASSIGN(ObKillExecutor);
};

class ObRpcKillSessionP : public obrpc::ObRpcProcessor<
     obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_KILL_SESSION> >, public ObKillSession
{
public:
  explicit ObRpcKillSessionP(const observer::ObGlobalContext &gctx) : gctx_(gctx)
  {}
  ~ObRpcKillSessionP() {}
protected:
  int process();
private:
  const observer::ObGlobalContext &gctx_;
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_CMD_OB_KILL_EXECUTOR_H__ */
//// end of header file
