/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_PX_WORKER_STAT_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_PX_WORKER_STAT_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/net/ob_addr.h"
#include "sql/engine/px/ob_px_worker_stat.h"
namespace oceanbase
{
namespace sql
{
  class ObPxWorkerStatList;
  class ObPxWorkerStat;
}  
namespace observer
{

class ObAllPxWorkerStatTable : public common::ObVirtualTableScannerIterator
{
public:
  ObAllPxWorkerStatTable();
  virtual ~ObAllPxWorkerStatTable();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  inline void set_addr(common::ObAddr &addr) { addr_ = &addr; }
private:
  common::ObAddr *addr_;
  bool start_to_read_;
  common::ObArray<sql::ObPxWorkerStat> stat_array_;
  int64_t index_;
  char trace_id_[128];
  enum INSPECT_COLUMN
  {
    SESSION_ID = common::OB_APP_MIN_COLUMN_ID,
    TENANT_ID,
    SVR_IP,
    SVR_PORT,
    TRACE_ID,
    QC_ID,
    SQC_ID,
    WORKER_ID,
    DFO_ID,
    START_TIME,
    THREAD_ID,
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllPxWorkerStatTable);
};

} // namespace observer
} // namespace oceanbase
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_PX_WORKER_STAT_TABLE_

