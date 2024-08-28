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