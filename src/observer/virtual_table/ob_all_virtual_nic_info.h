/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_NIC_INFO_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_NIC_INFO_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualNicInfo : public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    DEVNAME,
    SPEED_MBPS
  };

public:
  ObAllVirtualNicInfo();
  virtual ~ObAllVirtualNicInfo();
  virtual void reset() override;
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  bool is_end_;
  char svr_ip_[common::MAX_IP_ADDR_LENGTH];
  char devname_[common::MAX_IFNAME_LENGTH];
  int32_t svr_port_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualNicInfo);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_NIC_INFO_
