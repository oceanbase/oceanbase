/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_EVENT_HISTORY_TABLE_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_TENANT_EVENT_HISTORY_TABLE_OPERATOR_H_
#include "share/ob_event_history_table_operator.h"

namespace oceanbase
{
namespace rootserver
{
class ObTenantEventHistoryTableOperator : public share::ObEventHistoryTableOperator
{
public:
  virtual ~ObTenantEventHistoryTableOperator() {}
  int init(common::ObMySQLProxy &proxy, const common::ObAddr &self_addr);
  virtual int async_delete() override;
  static ObTenantEventHistoryTableOperator &get_instance();
private:
  ObTenantEventHistoryTableOperator() {}
  DISALLOW_COPY_AND_ASSIGN(ObTenantEventHistoryTableOperator);
};
} //end namespace rootserver
} //end namespace oceanbase
#define TENANT_EVENT_INSTANCE (::oceanbase::rootserver::ObTenantEventHistoryTableOperator::get_instance())
#define TENANT_EVENT_ADD(args...)                                         \
  TENANT_EVENT_INSTANCE.async_add_tenant_event(args)
#endif // OCEANBASE_ROOTSERVER_OB_TENANT_EVENT_HISTORY_TABLE_OPERATOR_H_