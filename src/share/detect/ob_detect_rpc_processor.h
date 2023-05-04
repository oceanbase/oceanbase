/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
