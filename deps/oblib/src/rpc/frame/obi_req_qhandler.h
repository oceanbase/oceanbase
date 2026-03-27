/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
