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

#include "ob_rs_event_history_table_operator.h"
#include "share/config/ob_server_config.h"
namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;

int ObRsEventHistoryTableOperator::init(common::ObMySQLProxy &proxy,
                                        const common::ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  const bool is_rs_event = true;
  const bool is_server_event = false;
  set_addr(self_addr, is_rs_event, is_server_event);
  if (OB_FAIL(ObEventHistoryTableOperator::init(proxy))) {
  } else {
    set_event_table(share::OB_ALL_ROOTSERVICE_EVENT_HISTORY_TNAME);
  }
  return ret;
}

ObRsEventHistoryTableOperator &ObRsEventHistoryTableOperator::get_instance()
{
  static ObRsEventHistoryTableOperator instance;
  return instance;
}

int ObRsEventHistoryTableOperator::async_delete()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", KR(ret));
  } else if (OB_FAIL(default_async_delete())) {
    SHARE_LOG(WARN, "failed to default async delete", KR(ret));
  }
  return ret;
}
}//end namespace rootserver
}//end namespace oceanbase
