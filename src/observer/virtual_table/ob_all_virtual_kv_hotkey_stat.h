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

#ifndef OB_ALL_VIRTUAL_KV_HOTKEY_STAT_H_
#define OB_ALL_VIRTUAL_KV_HOTKEY_STAT_H_
#include "lib/list/ob_dlist.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/table/ob_table_virtual_table_mgr.h"

namespace oceanbase {
namespace observer {

class ObAllVirtualKvHotKeyStat : public ObVirtualTableScannerIterator
{
  enum HOTKEY_COLUMN {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    DATABASE_ID,
    PARTITION_ID,
    TABLE_ID,
    SVR_IP,
    SVR_PORT,
    HOTKEY,
    HOTKEY_TYPE,
    HOTKEY_FREQ,
    THROTTLE_PERCENT
  };
public:
  ObAllVirtualKvHotKeyStat();
  virtual ~ObAllVirtualKvHotKeyStat();

  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;
  virtual int inner_close() override;

private:
  int init();
  int fill_cells();
private:
  bool inited_;
  int64_t idx;
  int32_t svr_port_;
  char svr_ip_[common::OB_IP_STR_BUFF];
  ObArenaAllocator arena_allocator_;
  ObTableVirtualTableMgr virtual_tbl_mgr_;
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
