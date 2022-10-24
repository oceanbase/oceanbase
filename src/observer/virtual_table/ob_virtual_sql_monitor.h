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

#ifndef OCEANBASE_OBSERVER_OB_VIRTUAL_SQL_MONITOR_H
#define OCEANBASE_OBSERVER_OB_VIRTUAL_SQL_MONITOR_H
#include "share/ob_virtual_table_projector.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"
#include "observer/mysql/ob_ra_queue.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObMultiVersionSchemaService;
}
}
namespace sql
{
class ObMonitorInfoManager;
class ObPhyPlanMonitorInfo;
}
namespace observer
{
class ObVirtualSqlMonitor : public common::ObVirtualTableProjector
{
public:
  ObVirtualSqlMonitor();
  virtual ~ObVirtualSqlMonitor();
  int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  int set_addr(const common::ObAddr &addr);
  void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }
private:
enum COLUMN_ID
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    REQUEST_ID,
    JOB_ID,
    TASK_ID,
    PLAN_ID,
    SCHEDULER_IP,
    SCHEDULER_PORT,
    MONITOR_INFO,
    EXTEND_INFO,
    SQL_EXEC_START,
  };

  DISALLOW_COPY_AND_ASSIGN(ObVirtualSqlMonitor);
  int get_next_monitor_info();
  static const int64_t OB_MAX_INFO_LENGTH = 1024;
private:
  sql::ObMonitorInfoManager *monitor_manager_;
  int64_t start_id_;
  int64_t end_id_;
  common::ObRaQueue::Ref ref_;
  sql::ObPhyPlanMonitorInfo *plan_info_;
  int64_t tenant_id_;
  int64_t request_id_;
  int64_t plan_id_;
  char scheduler_ipstr_[common::OB_IP_STR_BUFF];
  int32_t scheduler_port_;
  common::ObString ipstr_;
  int32_t port_;
  char info_buf_[OB_MAX_INFO_LENGTH];
  int64_t execution_time_;
};
} //namespace observer
} //namespace oceanbase
#endif
