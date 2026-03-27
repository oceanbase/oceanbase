/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_APPLY_STAT_H_
#define OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_APPLY_STAT_H_

#include "common/row/ob_row.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "logservice/applyservice/ob_log_apply_service.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualApplyStat : public common::ObVirtualTableScannerIterator
{
public:
  explicit ObAllVirtualApplyStat(omt::ObMultiTenant *omt) : omt_(omt) {}
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int insert_stat_(logservice::LSApplyStat &apply_stat);
private:
  static const int64_t VARCHAR_32 = 32;
  char role_str_[VARCHAR_32] = {'\0'};
  char ip_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  omt::ObMultiTenant *omt_;
};
} // namespace observer
} // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_APPLY_STAT_H_ */