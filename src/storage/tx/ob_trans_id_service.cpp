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

#include "ob_trans_id_service.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{

namespace transaction
{

int ObTransIDService::mtl_init(ObTransIDService *&trans_id_service)
{
  return trans_id_service->init();
}

int ObTransIDService::init()
{
  self_ = GCTX.self_addr();
  service_type_ = ServiceType::TransIDService;
  pre_allocated_range_ = TRANS_ID_PREALLOCATED_RANGE;
  return OB_SUCCESS;
}

int ObTransIDService::handle_request(const ObGtiRequest &request, obrpc::ObGtiRpcResult &result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(request));
  } else {
    TRANS_LOG(DEBUG, "handle gti request", K(request));
    const uint64_t tenant_id = request.get_tenant_id();
    const int64_t range = request.get_range();
    int64_t start_id = 0;
    int64_t end_id = 0;
    if (OB_FAIL(get_number(range, 0, start_id, end_id))) {
      TRANS_LOG(WARN, "get trans id failed", KR(ret));
    }
    if (OB_FAIL(result.init(tenant_id, ret, start_id, end_id))) {
      TRANS_LOG(WARN, "gti result init failed", KR(ret), K(request));
    }
  }
  //todo zhaoxing:ObTransStatistic
  return ret;
}

}
}