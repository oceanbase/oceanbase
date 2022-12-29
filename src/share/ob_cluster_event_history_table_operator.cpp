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

#include "ob_cluster_event_history_table_operator.h"
#include "share/config/ob_server_config.h"
namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;

int ObAllClusterEventHistoryTableOperator::init(common::ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObEventHistoryTableOperator::init(proxy))) {
    SHARE_LOG(WARN, "fail to init event history table operator", KR(ret));
  } else {
    set_event_table(share::OB_ALL_CLUSTER_EVENT_HISTORY_TNAME);
  }
  return ret;
}

ObAllClusterEventHistoryTableOperator &ObAllClusterEventHistoryTableOperator::get_instance()
{
  static ObAllClusterEventHistoryTableOperator instance;
  return instance;
}

int ObAllClusterEventHistoryTableOperator::async_delete()
{
  return OB_NOT_SUPPORTED;
}
} // end namespace observer
} // end namespace oceanbase
