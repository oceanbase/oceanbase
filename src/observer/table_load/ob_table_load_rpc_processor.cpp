/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
