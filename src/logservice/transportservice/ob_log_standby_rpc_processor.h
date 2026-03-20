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

// 备库日志传输RPC处理
class ObLogStandbyTransportP : public obrpc::ObRpcProcessor<obrpc::ObLogTransportRpcProxy::ObRpc<obrpc::OB_LOG_TRANSPORT_REQ> >
{
public:
  ObLogStandbyTransportP() : filter_(nullptr) {}
  virtual ~ObLogStandbyTransportP() { filter_ = nullptr; }

  int process();

  void set_filter(void *filter) { filter_ = reinterpret_cast<ObFunction<bool(const ObAddr &src)> *>(filter); }

private:
  ObFunction<bool(const ObAddr &src)> *filter_;

};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_STANDBY_RPC_PROCESSOR_
