/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RESP_PROCESSOR_
#define OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RESP_PROCESSOR_

#include "rpc/obrpc/ob_rpc_processor.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"
#include "logservice/transportservice/ob_log_transport_rpc_proxy.h"

namespace oceanbase
{
namespace logservice
{

// 主库接收备库ACK的RPC处理
class ObLogTransportRespP : public obrpc::ObRpcProcessor<obrpc::ObLogTransportRpcProxy::ObRpc<obrpc::OB_LOG_SYNC_STANDBY_INFO> >
{
public:
  ObLogTransportRespP() {}
  virtual ~ObLogTransportRespP();

  int process();

private:

};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RESP_PROCESSOR_
