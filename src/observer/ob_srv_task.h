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

#ifndef _OCEABASE_OBSERVER_OB_SRV_TASK_H_
#define _OCEABASE_OBSERVER_OB_SRV_TASK_H_

#include "rpc/ob_request.h"
#include "observer/mysql/obmp_disconnect.h"

namespace oceanbase
{
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
