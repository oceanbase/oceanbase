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

#ifndef _OCEABASE_RPC_OB_PACKET_H_
#define _OCEABASE_RPC_OB_PACKET_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace rpc
{

// This is the base class for oceanbase packets. Contruct directly is
// not allowed, so that the its destructor is unaccessible.
class ObPacket
{
public:
  ObPacket() {}
  virtual ~ObPacket() {}

  DECLARE_VIRTUAL_TO_STRING = 0;
}; // end of class ObPacket


enum class ConnectionPhaseEnum
{
  CPE_CONNECTED = 0,//server will send handshake pkt
  CPE_SSL_CONNECT,  //server will do ssl connect
  CPE_AUTHED        //server will do auth check and send ok pkt
};


} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_OB_PACKET_H_ */
