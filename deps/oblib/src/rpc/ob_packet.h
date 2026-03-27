/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  CPE_AUTHED,       //server will do auth check and send ok pkt
  CPE_AUTH_SWITCH,  //server will do auth switch check and send ok pkt
};


} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_OB_PACKET_H_ */
