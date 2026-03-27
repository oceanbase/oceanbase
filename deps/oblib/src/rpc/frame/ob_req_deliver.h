/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEABASE_RPC_FRAME_OB_REQ_DELIVER_H_
#define _OCEABASE_RPC_FRAME_OB_REQ_DELIVER_H_

#include "rpc/frame/ob_req_queue_thread.h"
#include "rpc/frame/ob_req_qhandler.h"
#include "rpc/frame/ob_req_translator.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace rpc
{
class ObRequest;
namespace frame
{

// This class plays a deliver role who'll deliver each packet received
// from the upper to responding packet queue. The deliver rules is
// defined by those macros named
class ObReqDeliver
{
public:
  virtual ~ObReqDeliver() {}

  virtual int init() = 0;
  // deliver a ObPacket to a responding queue
  virtual int deliver(rpc::ObRequest &req) = 0;
  virtual void stop() = 0;
  virtual int lock_tenant_list()
  {
    return OB_SUCCESS;
  }
  virtual int unlock_tenant_list()
  {
    return OB_SUCCESS;
  }

}; // end of class ObPktDeliver

class ObReqQDeliver
    : public ObReqDeliver
{
public:
  explicit ObReqQDeliver(ObiReqQHandler &qhandler);
  ObiReqQHandler &get_qhandler() { return qhandler_; }

protected:
  ObiReqQHandler &qhandler_;
}; // end of class ObReqQDeliver

} // end of namespace frame
} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OB_REQ_DELIVER_H_ */
