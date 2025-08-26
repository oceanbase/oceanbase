//Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_STORAGE_COMPACTION_INFO_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_STORAGE_COMPACTION_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
namespace oceanbase
{
namespace observer
{
class ObAllVirtualSharedStorageCompactionInfo
    : public common::ObVirtualTableScannerIterator,
      public omt::ObMultiTenantOperator {
public:
  ObAllVirtualSharedStorageCompactionInfo() {}
  virtual ~ObAllVirtualSharedStorageCompactionInfo() {}
  int init(ObIAllocator *allocator, common::ObAddr addr)
  { return OB_SUCCESS; }
  int inner_get_next_row(common::ObNewRow *&row) { return OB_ITER_END; }
  int process_curr_tenant(common::ObNewRow *&row) { return OB_ITER_END; }
  virtual void release_last_tenant() override {}
};
} // observer
} // oceanbase
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_STORAGE_COMPACTION_INFO_H_ */