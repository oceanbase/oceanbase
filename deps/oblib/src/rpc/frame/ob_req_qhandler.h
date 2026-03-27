/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEABASE_RPC_FRAME_OB_REQ_QHANDLER_H_
#define _OCEABASE_RPC_FRAME_OB_REQ_QHANDLER_H_

#include "rpc/frame/obi_req_qhandler.h"

namespace oceanbase
{
namespace rpc
{
class ObRequest;
namespace frame
{

class ObReqTranslator;
class ObReqQHandler
    : public ObiReqQHandler
{
public:
  explicit ObReqQHandler(ObReqTranslator &translator_);
  virtual ~ObReqQHandler();

  int init();

  // invoke when queue thread created.
  int onThreadCreated(obsys::CThread *);
  int onThreadDestroy(obsys::CThread *);

  bool handlePacketQueue(ObRequest *req, void *args);

private:
  ObReqTranslator &translator_;
}; // end of class ObReqQHandler

} // end of namespace frame
} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OB_REQ_QHANDLER_H_ */
