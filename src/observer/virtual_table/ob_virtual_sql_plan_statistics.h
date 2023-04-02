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

#ifndef OCEANBASE_OBSERVER_OB_VIRTUAL_SQL_PLAN_STATISTICS_H
#define OCEANBASE_OBSERVER_OB_VIRTUAL_SQL_PLAN_STATISTICS_H 1

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"
namespace oceanbase
{
namespace observer
{
class ObVirtualSqlPlanStatistics : public common::ObVirtualTableScannerIterator
{
public:
  ObVirtualSqlPlanStatistics();
  virtual ~ObVirtualSqlPlanStatistics();
  int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }
private:

int get_all_tenant_id();
int get_row_from_specified_tenant(uint64_t tenant_id, bool &is_end);
int fill_cells(const sql::ObOperatorStat &pstat);
private:
  enum COLUMN_ID
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    PLAN_ID,
    OPERATION_ID,
    EXECUTIONS,
    OUTPUT_ROWS,
    INPUT_ROWS,
    RESCAN_TIMES,
    BUFFER_GETS,
    DISK_READS,
    DISK_WRITES,
    ELAPSED_TIME,
    EXTEND_INFO1,
    EXTEND_INFO2
  };
  common::ObSEArray<uint64_t, 16> tenant_id_array_;
  common::ObSEArray<sql::ObOperatorStat, 128> operator_stat_array_;
  int64_t tenant_id_;
  int64_t tenant_id_array_idx_;
  int64_t operator_stat_array_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualSqlPlanStatistics);
};

} //end namespace observer
} //end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_VIRTUAL_SQL_PLAN_STATISTICS_H */


