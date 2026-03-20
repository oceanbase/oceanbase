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
