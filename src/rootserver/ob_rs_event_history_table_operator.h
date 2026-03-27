/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_RS_EVENT_HISTORY_TABLE_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_RS_EVENT_HISTORY_TABLE_OPERATOR_H_
#include "share/ob_event_history_table_operator.h"

namespace oceanbase
{
namespace rootserver
{
class ObRsEventHistoryTableOperator : public share::ObEventHistoryTableOperator
{
public:
  virtual ~ObRsEventHistoryTableOperator() {}

  int init(common::ObMySQLProxy &proxy, const common::ObAddr &self_addr);

  virtual int async_delete() override;

  static ObRsEventHistoryTableOperator &get_instance();
private:
  ObRsEventHistoryTableOperator() {}
  DISALLOW_COPY_AND_ASSIGN(ObRsEventHistoryTableOperator);
};

} //end namespace rootserver
} //end namespace oceanbase

#define ROOTSERVICE_EVENT_INSTANCE (::oceanbase::rootserver::ObRsEventHistoryTableOperator::get_instance())
#define ROOTSERVICE_EVENT_ADD(args...)                                         \
  ROOTSERVICE_EVENT_INSTANCE.add_event<false>(args)
#define ROOTSERVICE_EVENT_ADD_TRUNCATE(args...)                                \
  ROOTSERVICE_EVENT_INSTANCE.add_event<true>(args)
#endif // OCEANBASE_ROOTSERVER_OB_RS_EVENT_HISTORY_TABLE_OPERATOR_H_
