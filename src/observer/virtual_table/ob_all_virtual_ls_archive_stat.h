/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_LS_ARCHIVE_STAT_H_
#define OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_LS_ARCHIVE_STAT_H_
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace archive
{
struct LSArchiveStat;
}
namespace observer
{
class ObAllVirtualLSArchiveStat : public common::ObVirtualTableScannerIterator
{
public:
  explicit ObAllVirtualLSArchiveStat(omt::ObMultiTenant *omt) : omt_(omt) {}
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int insert_stat_(archive::LSArchiveStat &ls_stat);
private:
  char ip_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  omt::ObMultiTenant *omt_;
};
} // namespace observer
} // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_LS_ARCHIVE_STAT_H_ */
