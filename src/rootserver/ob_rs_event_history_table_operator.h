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
  ROOTSERVICE_EVENT_INSTANCE.add_event(args)

#endif // OCEANBASE_ROOTSERVER_OB_RS_EVENT_HISTORY_TABLE_OPERATOR_H_
