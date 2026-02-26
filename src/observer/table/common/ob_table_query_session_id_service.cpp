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

#define USING_LOG_PREFIX SERVER
#include "ob_table_query_session_id_service.h"

namespace oceanbase
{
namespace observer
{
int ObTableSessIDService::mtl_init(ObTableSessIDService *&table_sess_id_service)
{
  return table_sess_id_service->init();
}

int ObTableSessIDService::init()
{
  self_ = GCTX.self_addr();
  service_type_ = ServiceType::TableSessIDService;
  limited_id_ = MAX_SESSION_ID;
  pre_allocated_range_ = OB_TABLE_QUERY_SESSION_RANGE;
  return OB_SUCCESS;
}

int ObTableSessIDService::handle_request(const ObTableSessIDRequest &request, obrpc::ObTableSessIDRpcResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(request));
  } else {
    LOG_DEBUG("handle table session id request", K(ret), K(request));
    const uint64_t tenant_id = request.get_tenant_id();
    const int64_t range = request.get_range();
    int64_t start_id = 0;
    int64_t end_id = 0;
    if (OB_FAIL(get_number(range, 0, start_id, end_id))) {
      LOG_WARN("get session id failed", K(ret));
    }
    if (OB_FAIL(result.init(tenant_id, ret, start_id, end_id))) {
      LOG_WARN("session id result init failed", K(ret), K(tenant_id), K(request));
    }
  }
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase