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

#ifndef OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_H_
#define OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "share/ob_define.h"
namespace oceanbase {
namespace common {
class ObTenantManager;
class ObAddr;
}  // namespace common
namespace observer {
class ObGVTenantMemstoreInfo : public common::ObVirtualTableScannerIterator {
  enum MEMORY_INFO_COLUMN {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SERVER_IP,
    SERVER_PORT,
    ACTIVE_MEMSTORE_USED,
    TOTAL_MEMSTORE_USED,
    MAJOR_FREEZE_TRIGGER,
    MEMSTORE_LIMIT,
    FREEZE_CNT
  };

public:
  ObGVTenantMemstoreInfo();
  virtual ~ObGVTenantMemstoreInfo();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_tenant_mgr(common::ObTenantManager* tenant_mgr)
  {
    tenant_mgr_ = tenant_mgr;
  }
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = addr;
  }

private:
  common::ObTenantManager* tenant_mgr_;
  uint64_t current_pos_;
  common::ObAddr addr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGVTenantMemstoreInfo);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_H */
