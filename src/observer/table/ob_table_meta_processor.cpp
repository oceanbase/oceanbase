/**
 * Copyright (c) 2025 OceanBase
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

#include "ob_table_meta_processor.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;

ObTableMetaP::ObTableMetaP(const ObGlobalContext &gctx):
  ParentType(gctx)
{
}


int ObTableMetaP::try_process()
{
  int ret = OB_SUCCESS;
  exec_ctx_.set_timeout_ts(get_timeout_ts());
  exec_ctx_.get_sess_guard().get_sess_info().set_query_start_time(ObTimeUtility::current_time());
  ObTableMetaRequest &request = arg_;
  ObTableMetaResponse &result = result_;
  ObTableMetaHandlerGuard handler_guard(allocator_);
  ObITableMetaHandler *handler = nullptr;
  uint64_t tenant_id = MTL_ID();
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get tenant data version", K(ret), K(tenant_id));
  } else if (!((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0)
               || (data_version >= DATA_VERSION_4_4_1_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("hbase admin interface is not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "hbase admin interface");
  } else if (OB_FAIL(handler_guard.get_handler(request.meta_type_, handler))) {
    LOG_WARN("failed to get table meta handler", K(ret), K(request.meta_type_));
  } else if (OB_ISNULL(handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table meta handler", K(ret));
  } else if (OB_FAIL(handler->pre_check())) {
    LOG_WARN("failed to pre_check", K(ret));
  } else if (OB_FAIL(handler->parse(request))) {
    LOG_WARN("failed to parse table meta request", K(ret), K(request));
  } else if (OB_FAIL(handler->handle(exec_ctx_, result))) {
    LOG_WARN("failed to handle table meta request", K(ret), K(request));
  }
  return ret;
}
