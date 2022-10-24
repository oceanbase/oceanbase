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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_TRANSLATOR_
#define OCEANBASE_RPC_OBRPC_OB_RPC_TRANSLATOR_

#include "rpc/frame/ob_req_translator.h"
#include "rpc/obrpc/ob_rpc_session_handler.h"
#include "ob_rpc_packet.h"

namespace oceanbase
{
namespace common
{
class ObDataBuffer;
} // end of namespace common
namespace rpc
{
class ObRequest;
namespace frame
{
class ObReqProcessor;
} // end of namespace frame
} // end of namespace rpc


namespace obrpc
{

class ObRpcStreamCond;

class ObRpcTranslator
    : public rpc::frame::ObReqTranslator
{
public:
  int th_init();
  int th_destroy();

  inline ObRpcSessionHandler &get_session_handler();

protected:
  rpc::frame::ObReqProcessor* get_processor(rpc::ObRequest &req) = 0;

protected:
  ObRpcSessionHandler session_handler_;
}; // end of class ObRpcTranslator

//////////////////////////////////////////////////////////////
//  inline functions definition
///
ObRpcSessionHandler &ObRpcTranslator::get_session_handler()
{
  return session_handler_;
}

} // end of namespace obrpc
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_TRANSLATOR_
