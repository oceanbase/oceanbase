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
  SERVER_EVENT_INSTANCE.add_event(args)
#define SERVER_EVENT_SYNC_ADD(args...)                                         \
  SERVER_EVENT_INSTANCE.sync_add_event(args)
#define SERVER_EVENT_ADD_WITH_RETRY(args...)                                         \
  SERVER_EVENT_INSTANCE.add_event_with_retry(args)

#endif /* _OB_SERVER_EVENT_HISTORY_TABLE_OPERATOR_H */
