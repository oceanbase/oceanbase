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

#ifndef _OCEABASE_RPC_FRAME_OB_REQ_DELIVER_H_
#define _OCEABASE_RPC_FRAME_OB_REQ_DELIVER_H_

#include "rpc/frame/ob_req_queue_thread.h"
#include "rpc/frame/ob_req_qhandler.h"
#include "rpc/frame/ob_req_translator.h"

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
