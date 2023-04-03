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
