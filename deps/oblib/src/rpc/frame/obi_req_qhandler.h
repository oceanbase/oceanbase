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

#ifndef _OCEABASE_RPC_FRAME_OBI_REQ_QHANDLER_H_
#define _OCEABASE_RPC_FRAME_OBI_REQ_QHANDLER_H_

namespace oceanbase
{
namespace obsys
{
class CThread;
}
namespace rpc
{
class ObRequest;
namespace frame
{

class ObPacketQueueSessionHandler;
class ObiReqQHandler
{
public:
  virtual ~ObiReqQHandler() {}

  virtual int onThreadCreated(obsys::CThread *) = 0;
  virtual int onThreadDestroy(obsys::CThread *) = 0;

  virtual bool handlePacketQueue(ObRequest *req, void *args) = 0;
};

} // end of namespace frame
} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OBI_REQ_QHANDLER_H_ */
