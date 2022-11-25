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
