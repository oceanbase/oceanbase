/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEABASE_RPC_FRAME_OB_REQ_TRANSLATOR_H_
#define _OCEABASE_RPC_FRAME_OB_REQ_TRANSLATOR_H_

#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace rpc
{
class ObRequest;
namespace frame
{
class ObReqProcessor;

class ObReqTranslator
{
public:
  ObReqTranslator() {}
  virtual ~ObReqTranslator() {}

  int init();

  // Be aware those two functions would be invoked for each thread.
  virtual int th_init();
  virtual int th_destroy();

  virtual int translate(ObRequest &req, ObReqProcessor *&processor);
  virtual int release(ObReqProcessor *processor);

protected:
  virtual ObReqProcessor* get_processor(ObRequest &req) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObReqTranslator);
}; // end of class ObReqTranslator

} // end of namespace frame
} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OB_REQ_TRANSLATOR_H_ */
