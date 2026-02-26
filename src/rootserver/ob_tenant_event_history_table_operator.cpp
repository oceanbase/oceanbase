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
#define USING_LOG_PREFIX RS

#include "ob_tenant_event_history_table_operator.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;
int ObTenantEventHistoryTableOperator::init(common::ObMySQLProxy &proxy,
                                        const common::ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObEventHistoryTableOperator::init(proxy))) {
    LOG_WARN("fail to init event history table operator", KR(ret));
  } else {
    const bool is_rs_event = false;
    const bool is_server_event = false;
    set_addr(self_addr, is_rs_event, is_server_event);
    set_event_table(share::OB_ALL_TENANT_EVENT_HISTORY_TNAME);
  }
  return ret;
}
ObTenantEventHistoryTableOperator &ObTenantEventHistoryTableOperator::get_instance()
{
  static ObTenantEventHistoryTableOperator instance;
  return instance;
}
int ObTenantEventHistoryTableOperator::async_delete()
{
  return OB_NOT_SUPPORTED;
}
}//end namespace rootserver
}//end namespace oceanbase