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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MGR_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MGR_H_

#include "common/row/ob_row.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_i_table.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace storage
{
class ObTenantTabletIterator;
}
namespace observer
{
class ObAllVirtualTableMgr : public common::ObVirtualTableScannerIterator,
                             public omt::ObMultiTenantOperator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    TABLE_TYPE,
    TABLET_ID,
    START_LOG_SCN,
    END_LOG_SCN,
    UPPER_TRANS_VERSION,
    SIZE,
    DATA_BLOCK_CNT,
    INDEX_BLOCK_CNT,
    LINKED_BLOCK_CNT,
    REF,
    IS_ACTIVE,
    CONTAIN_UNCOMMITTED_ROW,
    NESTED_OFFSET,
    NESTED_SIZE,
    CG_IDX,
    DATA_CHECKSUM,
    TABLE_FLAG
  };
public:
  ObAllVirtualTableMgr();
  virtual ~ObAllVirtualTableMgr();
  int init(common::ObIAllocator *allocator);
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) { addr_ = addr; }
private:
  // 过滤得到需要处理的租户
  virtual bool is_need_process(uint64_t tenant_id) override;
  // 处理当前迭代的租户
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  // 释放上一个租户的资源
  virtual void release_last_tenant() override;

  int get_next_tablet();
  int get_next_table(storage::ObITable *&table);
private:
  common::ObAddr addr_;
  storage::ObTenantTabletIterator *tablet_iter_;
  common::ObArenaAllocator tablet_allocator_;
  ObTabletHandle tablet_handle_;
  int64_t ls_id_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  storage::ObTableStoreIterator table_store_iter_;
  void *iter_buf_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTableMgr);
};

}
}
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MGR_H_ */
