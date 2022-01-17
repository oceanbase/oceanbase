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

#ifndef OCEANBASE_SHARE_OB_UNIT_STAT_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_UNIT_STAT_TABLE_OPERATOR_H_

#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_unit_stat.h"
#include "share/ob_check_stop_provider.h"
#include "share/partition_table/ob_partition_table_operator.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
class ObUnitStatTableOperator {
public:
  ObUnitStatTableOperator();
  virtual ~ObUnitStatTableOperator();

  int init(share::ObPartitionTableOperator& pt_operator, share::schema::ObMultiVersionSchemaService& schema_service,
      share::ObCheckStopProvider& check_stop_provider);
  int get_unit_stat(uint64_t tenant_id, uint64_t unit_id, ObUnitStat& unit_stat) const;

  int get_unit_stat(uint64_t tenant_id, share::ObUnitStatMap& unit_stat_map) const;

private:
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }

private:
  bool inited_;
  share::ObPartitionTableOperator* pt_operator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObCheckStopProvider* check_stop_provider_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObUnitStatTableOperator);
};
}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_UNIT_STAT_TABLE_OPERATOR_H_
