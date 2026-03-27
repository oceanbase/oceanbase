/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SERVER_EVENT_HISTORY_TABLE_OPERATOR_H
#define _OB_SERVER_EVENT_HISTORY_TABLE_OPERATOR_H 1
#include "share/ob_event_history_table_operator.h"

namespace oceanbase
{
namespace observer
{
class ObAllServerEventHistoryTableOperator: public share::ObEventHistoryTableOperator
{
public:
  virtual ~ObAllServerEventHistoryTableOperator() {}

  int init(common::ObMySQLProxy &proxy, const common::ObAddr &self_addr);

  virtual int async_delete() override;

  static ObAllServerEventHistoryTableOperator &get_instance();
private:
  ObAllServerEventHistoryTableOperator() {}
  DISALLOW_COPY_AND_ASSIGN(ObAllServerEventHistoryTableOperator);
};

} // end namespace observer
} // end namespace oceanbase

#define SERVER_EVENT_INSTANCE (::oceanbase::observer::ObAllServerEventHistoryTableOperator::get_instance())
#define SERVER_EVENT_ADD(args...)                                         \
  SERVER_EVENT_INSTANCE.add_event<false>(args)
#define SERVER_EVENT_SYNC_ADD(args...)                                         \
  SERVER_EVENT_INSTANCE.sync_add_event(args)
#define SERVER_EVENT_ADD_WITH_RETRY(args...)                                         \
  SERVER_EVENT_INSTANCE.add_event_with_retry(args)

#endif /* _OB_SERVER_EVENT_HISTORY_TABLE_OPERATOR_H */
