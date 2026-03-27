/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_rpc_translator.h"


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
