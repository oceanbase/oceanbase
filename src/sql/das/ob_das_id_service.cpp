/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_id_service.h"
namespace oceanbase
{
namespace sql
{
int ObDASIDService::mtl_init(ObDASIDService *&das_id_service)
{
  return das_id_service->init();
}

int ObDASIDService::init()
{
  self_ = GCTX.self_addr();
  service_type_ = ServiceType::DASIDService;
  pre_allocated_range_ = DAS_ID_PREALLOCATED_RANGE;
  return OB_SUCCESS;
}

int ObDASIDService::handle_request(const ObDASIDRequest &request, obrpc::ObDASIDRpcResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(request));
  } else {
    const uint64_t tenant_id = request.get_tenant_id();
    const int64_t range = request.get_range();
    int64_t start_id = 0;
    int64_t end_id = 0;
    if (is_user_tenant(MTL_ID())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get das id from user's service", KR(ret), K(request));
    } else if (OB_FAIL(get_number(range, 0, start_id, end_id))) {
      LOG_WARN("get das id failed", KR(ret));
    }
    // overwrite ret
    if (OB_FAIL(result.init(tenant_id, ret, start_id, end_id))) {
      LOG_WARN("das id result init failed", KR(ret), K(request));
    }
  }
  // overwrite ret
  return OB_SUCCESS;
}
} // namespace sql
} // namespace oceanbase
