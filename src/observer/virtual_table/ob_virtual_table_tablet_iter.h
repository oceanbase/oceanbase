//Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_TABLET_ITER_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_TABLET_ITER_H_

#include "common/row/ob_row.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace storage
{
class ObTenantTabletIterator;
}
namespace observer
{
class ObVirtualTableTabletIter : public common::ObVirtualTableScannerIterator,
                                         public omt::ObMultiTenantOperator
{
public:
  ObVirtualTableTabletIter();
  virtual ~ObVirtualTableTabletIter();
  int init(common::ObIAllocator *allocator, common::ObAddr &addr);
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int get_next_tablet();
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) = 0;
  virtual void release_last_tenant() override;
protected:
  common::ObAddr addr_;
  storage::ObTenantTabletIterator *tablet_iter_;
  common::ObArenaAllocator tablet_allocator_;
  ObTabletHandle tablet_handle_;
  int64_t ls_id_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  void *iter_buf_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObVirtualTableTabletIter);
};

}
}
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_TABLET_ITER_H_ */
