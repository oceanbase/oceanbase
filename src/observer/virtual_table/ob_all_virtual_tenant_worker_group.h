/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_WORKER_GROUP_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_WORKER_GROUP_H_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualTenantWorkerGroup : public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    GROUP_ID,
    WORKERS_SIZE,
    REQ_QUEUE_SIZE,
    RECV_REQ_CNT,
    DELETED,
    TOKEN_CHANGE_TS,
    THROTTLED_TIME_US,
    IDLE_CNT,
    LAST_NOT_EMPTY_TS
  };

public:
  ObAllVirtualTenantWorkerGroup();
  virtual ~ObAllVirtualTenantWorkerGroup() override;
  virtual int inner_open() override;
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

private:
  bool start_to_read_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantWorkerGroup);
};

}
}
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_WORKER_GROUP_H_ */
