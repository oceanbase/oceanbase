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

#ifndef OB_ALL_VIRTUAL_LONG_OPS_STATUS_H_
#define OB_ALL_VIRTUAL_LONG_OPS_STATUS_H_

#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/longops_mgr/ob_longops_mgr.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualLongOpsStatus : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualLongOpsStatus();
  virtual ~ObAllVirtualLongOpsStatus();
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }
private:

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLongOpsStatus);
  common::ObAddr addr_;
  share::ObLongopsValue longops_value_;
  share::ObLongopsIterator longops_iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char trace_id_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OB_ALL_VIRTUAL_LONG_OPS_STATUS_H_
