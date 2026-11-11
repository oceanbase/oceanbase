/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_STANDBY_RPC_PROCESSOR_
#define OCEANBASE_LOGSERVICE_OB_LOG_STANDBY_RPC_PROCESSOR_

#include "rpc/obrpc/ob_rpc_processor.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"
#include "logservice/transportservice/ob_log_transport_rpc_proxy.h"
#include "lib/function/ob_function.h"

namespace oceanbase
{
namespace logservice
{

void fill_current_rpc_semi_sync_early_ack(const ObLogTransportReq &req,
                                          palf::LSN &ack_lsn,
                                          share::SCN &ack_scn);

// 备库日志传输RPC处理
class ObLogStandbyTransportP : public obrpc::ObRpcProcessor<obrpc::ObLogTransportRpcProxy::ObRpc<obrpc::OB_LOG_TRANSPORT_REQ> >
{
public:
  ObLogStandbyTransportP() {}
  virtual ~ObLogStandbyTransportP() {}

  int process();
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_STANDBY_RPC_PROCESSOR_
