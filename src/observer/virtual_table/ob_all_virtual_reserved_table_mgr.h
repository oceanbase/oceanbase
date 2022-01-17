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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_RESERVED_TABLE_MGR_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_RESERVED_TABLE_MGR_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "storage/ob_i_table.h"
#include "storage/ob_reserved_data_mgr.h"

namespace oceanbase {
namespace storage {
class ObIPartitionGroupIterator;
}
namespace observer {
class ObAllVirtualReservedTableMgr : public common::ObVirtualTableScannerIterator {
  enum COLUMN_ID_LIST {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLE_ID,
    TABLE_TYPE,
    PARTITION_ID,
    INDEX_ID,
    BASE_VERSION,
    MULTI_VERSION_START,
    SNAPSHOT_VERSION,
    OB_VERSION,
    SIZE,
    REF,
    RESERVE_TYPE,
    RESERVE_POINT_VERSION,
  };

public:
  ObAllVirtualReservedTableMgr();
  virtual ~ObAllVirtualReservedTableMgr();
  int init();
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;

private:
  const char* get_reserve_type_name_(storage::ObRecoveryPointType type);
  int get_point_next_table_(storage::ObITable*& table);
  int get_mgr_next_point_();
  int get_mgr_next_table_(storage::ObITable*& table);
  int get_next_table_(storage::ObITable*& table);

private:
  bool is_inited_;
  char svr_ip_[common::OB_MAX_SERVER_ADDR_SIZE];
  int32_t svr_port_;
  storage::ObTablesHandle tables_handle_;
  int64_t table_idx_;
  int64_t snapshot_version_;
  storage::ObRecoveryPointInfo* point_info_;
  storage::ObRecoveryDataMgr* pg_recovery_mgr_;
  storage::ObRecoveryPointIterator point_iter_;
  storage::ObIPartitionGroupIterator* partition_itertor_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualReservedTableMgr);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_RESERVED_TABLE_MGR_H_ */
