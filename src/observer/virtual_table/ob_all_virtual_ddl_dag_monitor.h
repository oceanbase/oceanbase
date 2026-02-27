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

#ifndef OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_DDL_DAG_MONITOR_H
#define OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_DDL_DAG_MONITOR_H

#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/net/ob_addr.h"
#include "storage/ddl/ob_ddl_dag_monitor_mgr.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualDDLDagMonitor : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    DAG_ID,
    DAG_INFO,
    TASK_ID,
    TASK_INFO,
    FORMAT_VERSION,
    TRACE_ID,
    CREATE_TIME,
    FINISH_TIME,
    MESSAGE
  };

  ObAllVirtualDDLDagMonitor();
  virtual ~ObAllVirtualDDLDagMonitor();
  int init(const common::ObAddr &addr);
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;

private:
  int fill_cells(storage::ObDDLDagMonitorNode *node, storage::ObDDLDagMonitorInfo *task_info);
  int advance_to_next_tenant();

private:
  common::ObAddr addr_;
  bool is_inited_;
  common::ObSEArray<uint64_t, 8> tenant_ids_;
  int64_t tenant_idx_;
  common::ObSEArray<storage::ObDDLDagMonitorNode *, 64> nodes_;
  int64_t node_idx_;
  common::ObSEArray<storage::ObDDLDagMonitorInfo *, 64> task_infos_;
  int64_t info_idx_;
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  storage::ObDDLDagMonitorEntry entry_;
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_DDL_DAG_MONITOR_H
