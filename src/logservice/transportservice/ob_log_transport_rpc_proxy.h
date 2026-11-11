/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RPC_PROXY_
#define OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RPC_PROXY_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"
#include "share/ob_define.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
namespace logservice
{
struct ObLogTransportReq;
struct ObLogSyncStandbyInfo;
};
namespace obrpc
{

class ObLogTransportRpcProxy : public ObRpcProxy
{
public:
  DEFINE_TO(ObLogTransportRpcProxy);
  RPC_AP(PR1 post_log_transport_req, OB_LOG_TRANSPORT_REQ,
         (logservice::ObLogTransportReq), logservice::ObLogSyncStandbyInfo);
  RPC_AP(PR1 post_log_transport_resp, OB_LOG_SYNC_STANDBY_INFO,
         (logservice::ObLogSyncStandbyInfo), logservice::ObLogTransportReq);
};

} // namespace obrpc
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RPC_PROXY_