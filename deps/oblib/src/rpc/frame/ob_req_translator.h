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
