/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_cluster_event_history_table_operator.h"
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
