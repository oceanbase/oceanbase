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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_STAT_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "storage/blocksstable/ob_store_file_system.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualRaidStat : public common::ObVirtualTableScannerIterator {
  enum COLUMN_ID_LIST {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    DISK_INDEX,
    INSTALL_SEQ,
    DATA_NUM,
    PARITY_NUM,
    CREATE_TS,
    FINISH_TS,
    ALIAS_NAME,
    STATUS,
    PERCENT,
  };

public:
  ObAllVirtualRaidStat();
  virtual ~ObAllVirtualRaidStat();
  int init(const common::ObAddr& addr);

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  blocksstable::ObDiskStats disk_stats_;
  int64_t cur_idx_;
  common::ObAddr addr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualRaidStat);
};

}  // namespace observer
}  // namespace oceanbase

#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_STAT_H_ */
