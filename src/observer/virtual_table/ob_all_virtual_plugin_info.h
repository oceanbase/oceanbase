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

#pragma once

#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace plugin {
class ObPluginEntryHandle;
}
namespace observer {

class ObAllVirtualPluginInfo final : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualPluginInfo();
  virtual ~ObAllVirtualPluginInfo();

public:
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  virtual int inner_open() override;
  virtual int inner_close() override;
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }

private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];

  ObArray<plugin::ObPluginEntryHandle *> plugin_entries_;

  int64_t iter_index_ = -1;

  TO_STRING_KV(K(addr_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPluginInfo);
};

} // namespace observer
} // namespace oceanbase
