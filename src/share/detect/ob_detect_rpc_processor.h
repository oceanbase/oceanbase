/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/detect/ob_detect_rpc_proxy.h"

namespace oceanbase {
namespace obrpc {

class ObDetectRpcP
		: public obrpc::ObRpcProcessor<obrpc::ObDetectRpcProxy::ObRpc<obrpc::OB_DETECT_RPC_CALL>>
{
public:
  ObDetectRpcP() {}
  virtual ~ObDetectRpcP() = default;
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDetectRpcP);
};

} // end namespace obrpc
} // end namespace oceanbase
