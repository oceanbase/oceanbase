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

#ifndef _OB_CLUSTER_EVENT_HISTORY_TABLE_OPERATOR_H
#define _OB_CLUSTER_EVENT_HISTORY_TABLE_OPERATOR_H
#include "share/ob_event_history_table_operator.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace observer
{
class ObAllClusterEventHistoryTableOperator: public share::ObEventHistoryTableOperator
{
public:
  virtual ~ObAllClusterEventHistoryTableOperator() {}

  int init(common::ObMySQLProxy &proxy);

  virtual int async_delete() override;

  static ObAllClusterEventHistoryTableOperator &get_instance();
private:
  ObAllClusterEventHistoryTableOperator() {}
  DISALLOW_COPY_AND_ASSIGN(ObAllClusterEventHistoryTableOperator);
};

} // end namespace observer
} // end namespace oceanbase

#define CLUSTER_EVENT_INSTANCE (::oceanbase::observer::ObAllClusterEventHistoryTableOperator::get_instance())

#define CLUSTER_EVENT_SYNC_ADD(args...) \
  if (OB_SUCC(ret)) { \
    uint64_t data_version = 0; \
    if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) { \
      SHARE_LOG(WARN, "fail to get data version", KR(ret), "tenant_id", OB_SYS_TENANT_ID); \
    } else if (data_version < DATA_VERSION_4_1_0_0) { \
      /* inner table is not ready, just skip */ \
    } else if (OB_FAIL(CLUSTER_EVENT_INSTANCE.sync_add_event(args))) { \
      SHARE_LOG(WARN, "fail to sync add event", KR(ret)); \
    } \
  }

#endif /* _OB_CLUSTER_EVENT_HISTORY_TABLE_OPERATOR_H */
