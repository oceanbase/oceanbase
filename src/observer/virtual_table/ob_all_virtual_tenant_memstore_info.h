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
namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace observer
{
class ObAllVirtualTenantMemstoreInfo : public common::ObVirtualTableScannerIterator
{
  enum MEMORY_INFO_COLUMN {
    SERVER_IP = common::OB_APP_MIN_COLUMN_ID,
    SERVER_PORT,
    TENANT_ID,
    ACTIVE_SPAN,
    FREEZE_TRIGGER,
    FREEZE_CNT,
    MEMSTORE_USED,
    MEMSTORE_LIMIT
  };
public:
  ObAllVirtualTenantMemstoreInfo();
  virtual ~ObAllVirtualTenantMemstoreInfo();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) { addr_ = addr; }
private:
  uint64_t current_pos_;
  common::ObAddr addr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantMemstoreInfo);
};

}
}
#endif /* OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_H */
