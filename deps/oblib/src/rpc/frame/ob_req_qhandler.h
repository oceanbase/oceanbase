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
