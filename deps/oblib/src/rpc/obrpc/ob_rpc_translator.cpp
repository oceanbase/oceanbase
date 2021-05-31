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

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_rpc_translator.h"

#include "rpc/ob_request.h"
#include "rpc/frame/ob_req_processor.h"
#include "rpc/obrpc/ob_rpc_stream_cond.h"
#include "rpc/obrpc/ob_rpc_packet.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;

int ObRpcTranslator::th_init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObReqTranslator::th_init())) {
    LOG_WARN("init req translator for thread fail", K(ret));
  } else {
    LOG_INFO("Init thread local success");
  }

  return ret;
}

int ObRpcTranslator::th_destroy()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObReqTranslator::th_destroy())) {
    LOG_ERROR("destroy req translator fail", K(ret));
  }

  return ret;
}
