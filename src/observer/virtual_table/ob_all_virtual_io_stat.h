/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_IO_STAT_H_
#define OB_ALL_VIRTUAL_IO_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualIOStat : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualIOStat();
  virtual ~ObAllVirtualIOStat();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  enum IOStatColumn
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    DISK_TYPE,
    SYS_IO_UP_LIMIT_IN_MB,
    SYS_IO_BAND_IN_MB,
    SYS_IO_LOW_WATERMARK_IN_MB,
    SYS_IO_HIGH_WATERMARK_IN_MB,
    IO_BENCH_RESULT,
  };

private:
  char svr_ip_[common::OB_IP_STR_BUFF];
  char disk_type_[common::OB_MAX_DISK_TYPE_LENGTH];
  char io_bench_result_[common::OB_MAX_IO_BENCH_RESULT_LENGTH];

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualIOStat);
};


} // namespace observer
} // namespace oceanbase


#endif
