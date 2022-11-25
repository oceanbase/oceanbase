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

#define USING_LOG_PREFIX RPC_FRAME

#include "rpc/frame/ob_req_qhandler.h"

#include "rpc/ob_request.h"
#include "rpc/frame/ob_req_translator.h"
#include "rpc/frame/ob_req_processor.h"

using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::common;

ObReqQHandler::ObReqQHandler(ObReqTranslator &translator)
    : translator_(translator)
{
  // empty
}

ObReqQHandler::~ObReqQHandler()
{
  // empty
}

int ObReqQHandler::init()
{
  return translator_.init();
}

int ObReqQHandler::onThreadCreated(obsys::CThread *th)
{
  UNUSED(th);
  LOG_INFO("new task thread create", K(&translator_));
  return translator_.th_init();
}

int ObReqQHandler::onThreadDestroy(obsys::CThread *th)
{
  UNUSED(th);
  return translator_.th_destroy();
}

bool ObReqQHandler::handlePacketQueue(ObRequest *req, void */* arg */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(req), K(ret));
  }

  ObReqProcessor *processor = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(translator_.translate(*req, processor))) {
      LOG_WARN("translate reqeust fail", K(*req), K(ret));
      processor = NULL;
    }
  }

  // We just test processor is created correctly, but ignore the
  // returning code before.
  if (NULL == processor) {
    if (NULL != req) {
      on_translate_fail(req, ret);
    }
  } else {
    req->set_trace_point(ObRequest::OB_EASY_REQUEST_QHANDLER_PROCESSOR_RUN);
    if (OB_FAIL(processor->run())) {
      LOG_WARN("process rpc fail", K(ret));
    }
    translator_.release(processor);
  }

  return OB_SUCC(ret);
}
