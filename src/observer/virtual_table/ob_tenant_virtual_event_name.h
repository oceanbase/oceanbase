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

#ifndef OB_TENANT_VIRTUAL_EVENT_NAME_H_
#define OB_TENANT_VIRTUAL_EVENT_NAME_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/stat/ob_session_stat.h"

namespace oceanbase
{
namespace observer
{

class ObTenantVirtualEventName : public common::ObVirtualTableScannerIterator
{
public:
  ObTenantVirtualEventName();
  virtual ~ObTenantVirtualEventName();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  virtual void reset();

private:
  enum SYS_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    EVENT_ID,
    EVENT_NO,
    NAME,
    DISPLAY_NAME,
    PARAMETER1,
    PARAMETER2,
    PARAMETER3,
    WAIT_CLASS_ID,
    WAIT_CLASS_NO,
    WAIT_CLASS
  };
  int32_t event_iter_;
  uint64_t tenant_id_;
  common::ObObj cells_[common::OB_ROW_MAX_COLUMNS_COUNT];
  DISALLOW_COPY_AND_ASSIGN(ObTenantVirtualEventName);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_TENANT_VIRTUAL_EVENT_NAME_H_ */
