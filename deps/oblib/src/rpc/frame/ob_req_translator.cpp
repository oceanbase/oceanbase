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

#define USING_LOG_PREFIX RPC
#include "rpc/frame/ob_req_translator.h"

#include "rpc/ob_request.h"
#include "rpc/frame/ob_req_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;

int ObReqTranslator::init()
{
  return OB_SUCCESS;
}

int ObReqTranslator::th_init()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObReqTranslator::th_destroy()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObReqTranslator::translate(ObRequest &req, ObReqProcessor *&processor)
{
  int ret = OB_SUCCESS;
  processor = get_processor(req);
  if (NULL == processor) {
    RPC_LOG(WARN, "can't translate packet", K(req), K(ret));
    ret = OB_NOT_SUPPORTED;
  } else {
    //processor->reuse();
    processor->set_ob_request(req);
  }
  return ret;
}

int ObReqTranslator::release(ObReqProcessor *processor)
{
  int ret = OB_SUCCESS;
  UNUSED(processor);
  return ret;
}
