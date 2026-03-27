/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SYS_TASK_STATUS_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SYS_TASK_STATUS_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "share/scheduler/ob_sys_task_stat.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualSysTaskStatus: public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSysTaskStatus();
  virtual ~ObAllVirtualSysTaskStatus();

  int init (share::ObSysTaskStatMgr &status_mgr);
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

private:
  share::ObSysStatMgrIter iter_;
  char task_id_[common::OB_TRACE_STAT_BUFFER_SIZE];
  char svr_ip_[common::MAX_IP_ADDR_LENGTH];
  char comment_[common::OB_MAX_TASK_COMMENT_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSysTaskStatus);

};

} // observer
} // oceanbase


#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SYS_TASK_STATUS_H_ */
