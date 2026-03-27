/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "ob_table_load_rpc_processor.h"
#include "observer/table_load/ob_table_load_service.h"

namespace oceanbase
{
namespace observer
{

int ObDirectLoadControlP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::direct_load_control(arg_, result_, allocator_))) {
    LOG_WARN("fail to direct load control", KR(ret), K(arg_));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
