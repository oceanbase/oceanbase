/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIECT, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the Mulan PubL v2 for more details.
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