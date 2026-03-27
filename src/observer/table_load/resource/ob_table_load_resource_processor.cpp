/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/resource/ob_table_load_resource_processor.h"

namespace oceanbase
{
namespace observer
{

int ObDirectLoadResourceP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::direct_load_resource(arg_, result_, allocator_))) {
    LOG_WARN("fail to direct load resource", KR(ret), K(arg_));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
