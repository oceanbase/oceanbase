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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PX_TARGET_MONITOR_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PX_TARGET_MONITOR_H_


#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "sql/engine/px/ob_px_target_mgr.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualPxTargetMonitor: public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualPxTargetMonitor();
  virtual ~ObAllVirtualPxTargetMonitor() {}
public:
  int init();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual int inner_close();
private:
  int prepare_start_to_read();
  int get_next_target_info(ObPxTargetInfo &target_info);
private:
  enum TARGET_MONITOR_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    IS_LEADER,
    VERSION,
    PEER_IP,
    PEER_PORT,
    PEER_TARGET,
    PEER_TARGET_USED,
    LOCAL_TARGET_USED,
    LOCAL_PARALLEL_SESSION_COUNT
  };
  common::ObSEArray<uint64_t, 4> tenand_array_;
  uint64_t tenant_idx_;
  common::ObSEArray<ObPxTargetInfo, 10> target_info_array_;
  uint64_t target_usage_idx_;
  char svr_ip_buff_[common::OB_IP_PORT_STR_BUFF];
  char peer_ip_buff_[common::OB_IP_PORT_STR_BUFF];
}; //class ObAllVirtualPxTargetMonitor
}//namespace observer
}//namespace oceanbase
#endif //OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PX_TARGET_MONITOR_H_
