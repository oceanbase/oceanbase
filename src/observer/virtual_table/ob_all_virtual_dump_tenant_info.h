/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_DUMP_TENANT_INFO_H_
#define OB_ALL_VIRTUAL_DUMP_TENANT_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualDumpTenantInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDumpTenantInfo();
  virtual ~ObAllVirtualDumpTenantInfo();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDumpTenantInfo);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif /* OB_ALL_VIRTUAL_DUMP_TENANT_INFO_H_ */
