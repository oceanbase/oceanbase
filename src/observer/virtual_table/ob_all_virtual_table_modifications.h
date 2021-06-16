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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MODIFICATIONS_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MODIFICATIONS_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "storage/ob_sstable_merge_info_mgr.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualTableModifications : public common::ObVirtualTableScannerIterator {
  enum COLUMN_ID_LIST {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLE_ID,
    PARTITION_ID,
    INSERT_ROW_COUNT,
    UPDATE_ROW_COUNT,
    DELETE_ROW_COUNT,
    MAX_SNAPSHOT_VERSION
  };

public:
  ObAllVirtualTableModifications();
  virtual ~ObAllVirtualTableModifications();
  int init();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = addr;
  }

private:
  static const int64_t DEFAULT_TABLE_INFO_CNT = 2048;
  int get_next_table_info(storage::ObTableModificationInfo*& info);

private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  common::ObSEArray<storage::ObTableModificationInfo, DEFAULT_TABLE_INFO_CNT> infos_;
  int64_t info_idx_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTableModifications);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MODIFICATIONS_H_ */
