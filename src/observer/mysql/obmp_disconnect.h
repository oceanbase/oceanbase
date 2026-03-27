/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OBMP_DISCONNECT_H_
#define _OBMP_DISCONNECT_H_

#include "rpc/frame/ob_req_processor.h"
#include "sql/session/ob_sql_session_mgr.h"
namespace oceanbase
{
namespace sql
{
class ObFreeSessionCtx;
}
namespace observer
{

// Before coming into this class, all information about this
// connection maybe invalid.
class ObMPDisconnect
    : public rpc::frame::ObReqProcessor
{
public:
  explicit ObMPDisconnect(const sql::ObFreeSessionCtx &ctx);
  virtual ~ObMPDisconnect();

protected:
  int run();
private:
  int kill_unfinished_session(uint32_t sessid);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPDisconnect);
  sql::ObFreeSessionCtx ctx_;
}; // end of class ObMPDisconnect

} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OBMP_DISCONNECT_H_ */
