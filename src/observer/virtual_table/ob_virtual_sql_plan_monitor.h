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

#ifndef OCEANBASE_OBSERVER_OB_VIRTUAL_SQL_PLAN_MONITOR_H
#define OCEANBASE_OBSERVER_OB_VIRTUAL_SQL_PLAN_MONITOR_H
#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/net/ob_addr.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"

namespace oceanbase
{
namespace sql
{
class ObMonitorNode;
}
namespace common
{
class ObIAllocator;
}
namespace share
{
class ObTenantSpaceFetcher;
}

namespace observer
{
class ObVirtualSqlPlanMonitor : public common::ObVirtualTableScannerIterator
{
public:
  ObVirtualSqlPlanMonitor();
  virtual ~ObVirtualSqlPlanMonitor();
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  void set_addr(const common::ObAddr &addr) { addr_ = addr; }
  virtual int set_ip(const common::ObAddr &addr);
  virtual void reset();
  int check_ip_and_port(bool &is_valid);
  void use_index_scan() { is_use_index_ = true; }
  bool is_index_scan() const { return is_use_index_; }
private:
  int convert_node_to_row(sql::ObMonitorNode &node, ObNewRow *&row);
  int extract_tenant_ids();
  int extract_request_ids(const uint64_t tenant_id,
                          int64_t &start_id,
                          int64_t &end_id,
                          bool &is_valid);
  int switch_tenant_monitor_node_list();
  int report_rt_monitor_node(common::ObNewRow *&row);
  inline void reset_rt_node_info()
  {
    need_rt_node_ = false;
    rt_nodes_.reset();
    rt_node_idx_ = 0;
    rt_start_idx_ = INT_MAX;
    rt_end_idx_ = INT_MIN;
  }
private:
  enum COLUMN_ID
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    REQUEST_ID,
    TRACE_ID,
    FIRST_REFRESH_TIME,
    LAST_REFRESH_TIME,
    FIRST_CHANGE_TIME,
    LAST_CHANGE_TIME,
    OTHERSTAT_1_ID,
    OTHERSTAT_1_VALUE,
    OTHERSTAT_2_ID,
    OTHERSTAT_2_VALUE,
    OTHERSTAT_3_ID,
    OTHERSTAT_3_VALUE,
    OTHERSTAT_4_ID,
    OTHERSTAT_4_VALUE,
    OTHERSTAT_5_ID,
    OTHERSTAT_5_VALUE,
    OTHERSTAT_6_ID,
    OTHERSTAT_6_VALUE,
    OTHERSTAT_7_ID,
    OTHERSTAT_7_VALUE,
    OTHERSTAT_8_ID,
    OTHERSTAT_8_VALUE,
    OTHERSTAT_9_ID,
    OTHERSTAT_9_VALUE,
    OTHERSTAT_10_ID,
    OTHERSTAT_10_VALUE,
    THREAD_ID,
    PLAN_OPERATION,
    STARTS,
    OUTPUT_ROWS,
    PLAN_LINE_ID,
    PLAN_DEPTH,
    OUTPUT_BATCHES, // for batch
    SKIPPED_ROWS_COUNT, // for batch
    DB_TIME,
    USER_IO_WAIT_TIME,
    WORKAREA_MEM,
    WORKAREA_MAX_MEM,
    WORKAREA_TEMPSEG,
    WORKAREA_MAX_TEMPSEG
  };
  DISALLOW_COPY_AND_ASSIGN(ObVirtualSqlPlanMonitor);

  const static int64_t PRI_KEY_IP_IDX        = 0;
  const static int64_t PRI_KEY_PORT_IDX      = 1;
  const static int64_t PRI_KEY_TENANT_ID_IDX = 2;
  const static int64_t PRI_KEY_REQ_ID_IDX    = 3;

  const static int64_t IDX_KEY_TENANT_ID_IDX = 0;
  const static int64_t IDX_KEY_REQ_ID_IDX    = 1;
  const static int64_t IDX_KEY_IP_IDX        = 2;
  const static int64_t IDX_KEY_PORT_IDX      = 3;


  sql::ObPlanMonitorNodeList *cur_mysql_req_mgr_;
  int64_t start_id_;
  int64_t end_id_;
  int64_t cur_id_;
  common::ObRaQueue::Ref ref_;
  common::ObAddr addr_;
  common::ObString ipstr_;
  int32_t port_;
  char server_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char trace_id_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  bool is_first_get_;
  bool is_use_index_;
  common::ObSEArray<uint64_t, 16> tenant_id_array_;
  int64_t tenant_id_array_idx_;
  share::ObTenantSpaceFetcher *with_tenant_ctx_;
  bool need_rt_node_;
  common::ObArray<sql::ObMonitorNode> rt_nodes_;
  int64_t rt_node_idx_;
  int64_t rt_start_idx_;
  int64_t rt_end_idx_;
};

} //namespace observer
} //namespace oceanbase
#endif
