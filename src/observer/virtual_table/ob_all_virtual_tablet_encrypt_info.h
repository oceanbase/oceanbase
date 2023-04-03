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

#ifndef OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_H_
#define OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_H_
#include "common/row/ob_row.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualTabletEncryptInfo : public common::ObVirtualTableScannerIterator,
                                      public omt::ObMultiTenantOperator
{
public:

  ObAllVirtualTabletEncryptInfo() {}
  virtual ~ObAllVirtualTabletEncryptInfo() {}
  int init(common::ObIAllocator *allocator, common::ObAddr &addr) { return OB_SUCCESS; }
  int inner_get_next_row(common::ObNewRow *&row) override { return OB_ITER_END; }
  int process_curr_tenant(common::ObNewRow *&row) override { return OB_ITER_END; }
  void release_last_tenant() override {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletEncryptInfo);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_H_ */
