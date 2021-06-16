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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MGR_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MGR_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "storage/ob_i_table.h"
#include "storage/ob_pg_mgr.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualTableMgr : public common::ObVirtualTableScannerIterator {
  enum COLUMN_ID_LIST {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLE_TYPE,
    TABLE_ID,
    PARTITION_ID,
    INDEX_ID,
    BASE_VERSION,
    MULTI_VERSION_START,
    SNAPSHOT_VERSION,
    MAX_MERGED_VERSION,
    UPPER_TRANS_VERSION,
    START_LOG_TS,
    END_LOG_TS,
    MAX_LOG_TS,
    OB_VERSION,
    LOGICAL_DATA_VERSION,
    SIZE,
    COMPACT_ROW,
    IS_ACTIVE,
    TIMESTAMP,
    REF,
    WRITE_REF,
    TRX_COUNT,
    PENDING_CB_COUNT,
    CONTAIN_UNCOMMITTED_ROW
  };

public:
  ObAllVirtualTableMgr();
  virtual ~ObAllVirtualTableMgr();
  int init();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = addr;
  }

private:
  int get_next_table(storage::ObITable*& table);

private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  storage::ObIPartitionGroupIterator* pg_iter_;
  storage::ObTablesHandle pg_tables_handle_;
  common::ObArray<storage::ObSSTable*> sstables_;
  storage::ObTablesHandle memtable_handle_;
  int64_t table_idx_;
  int64_t memtable_idx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTableMgr);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLE_MGR_H_ */
