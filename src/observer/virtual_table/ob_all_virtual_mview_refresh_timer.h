/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MVIEW_REFRESH_TIMER_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MVIEW_REFRESH_TIMER_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace rootserver
{
struct ObMViewSchedulerTimerSnapshot;
}
namespace observer
{

class ObAllVirtualMViewRefreshTimer : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualMViewRefreshTimer();
  virtual ~ObAllVirtualMViewRefreshTimer() override;
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;

private:
  enum ColumnId
  {
    GMT_MODIFIED = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    TENANT_ID,
    GROUP_ID,
    MVIEW_ID,
    NEXT_TRIGGER_TIME,
    GROUP_INTERVAL,
    EXPECTED_INTERVAL,
    SCHEDULE_INTERVAL,
    LAST_START_TIME,
  };

  int fill_scanner();
#ifdef OB_BUILD_MV_CLOSE_MODULES
  int fill_row(const uint64_t tenant_id,
               const rootserver::ObMViewSchedulerTimerSnapshot &snapshot);
#endif

private:
  bool start_to_read_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMViewRefreshTimer);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MVIEW_REFRESH_TIMER_
