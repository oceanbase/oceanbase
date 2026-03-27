/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEABASE_OBSERVER_OB_SRV_TASK_H_
#define _OCEABASE_OBSERVER_OB_SRV_TASK_H_

#include "rpc/ob_request.h"
#include "observer/mysql/obmp_disconnect.h"

namespace oceanbase
{

namespace common
{
class ObDiagnosticInfo;
}

namespace sql
{
class ObFreeSessionCtx;
}
namespace observer
{

class ObSrvTask
    : public rpc::ObRequest
{
public:
  ObSrvTask()
      : ObRequest(ObRequest::OB_TASK)
  {}

  virtual rpc::frame::ObReqProcessor &get_processor() = 0;
}; // end of class ObSrvTask

class ObDisconnectTask
    : public ObSrvTask
{
public:
  ObDisconnectTask(const sql::ObFreeSessionCtx &ctx)
       : proc_(ctx)
  {
  }

  virtual rpc::frame::ObReqProcessor &get_processor()
  {
    return proc_;
  }

private:
  ObMPDisconnect proc_;
}; // end of class ObDisconnectTAsk


} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_SRV_TASK_H_ */
