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

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_long_ops_monitor.h"

namespace oceanbase {
namespace observer {

class ObAllVirtualLongOpsStatus : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualLongOpsStatus();
  virtual ~ObAllVirtualLongOpsStatus() = default;
  int init();
  virtual int inner_get_next_row(common::ObNewRow*& row) override;

private:
  int convert_stat_to_row(const storage::ObILongOpsStat& stat, common::ObNewRow*& row);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLongOpsStatus);
  bool is_inited_;
  storage::ObLongOpsMonitorIterator iter_;
  char ip_[common::OB_MAX_SERVER_ADDR_SIZE];
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OB_ALL_VIRTUAL_LONG_OPS_STATUS_H_
