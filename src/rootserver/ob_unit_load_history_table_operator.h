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

#ifndef OCEANBASE_ROOTSERVER_OB_UNIT_LOAD_HISTORY_TABLE_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_UNIT_LOAD_HISTORY_TABLE_OPERATOR_H_
#include "share/ob_event_history_table_operator.h"
#include "rootserver/ob_balance_info.h"

namespace oceanbase {
namespace rootserver {
class ObUnitLoadHistoryTableOperator : public share::ObEventHistoryTableOperator {
public:
  virtual ~ObUnitLoadHistoryTableOperator()
  {}

  int init(common::ObMySQLProxy& proxy, const common::ObAddr& self_addr);

  virtual int async_delete() override;
  static ObUnitLoadHistoryTableOperator& get_instance();

  int add_load(int64_t tenant_id, ObResourceWeight& weight, UnitStat& us);

private:
  ObUnitLoadHistoryTableOperator()
  {}
  DISALLOW_COPY_AND_ASSIGN(ObUnitLoadHistoryTableOperator);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#define UNIT_LOAD_INSTANCE (::oceanbase::rootserver::ObUnitLoadHistoryTableOperator::get_instance())
#define UNIT_LOAD_ADD(args...) UNIT_LOAD_INSTANCE.add_load(args)

#endif  // OCEANBASE_ROOTSERVER_OB_UNIT_LOAD_HISTORY_TABLE_OPERATOR_H_
