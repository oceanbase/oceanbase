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

#ifndef OB_ALL_VIRTUAL_PARTITION_AUDIT_H
#define OB_ALL_VIRTUAL_PARTITION_AUDIT_H

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"

namespace oceanbase {
namespace storage {
class ObIPartitionGroupIterator;
}
namespace observer {

class ObAllVirtualPartitionAudit : public ObVirtualTableScannerIterator {
public:
  ObAllVirtualPartitionAudit();
  virtual ~ObAllVirtualPartitionAudit();
  int init();
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;

private:
  enum COLUMNS {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLE_ID,
    PARTITION_ID,
    PARTITION_STATUS,
    BASE_ROW_COUNT,
    INSERT_ROW_COUNT,
    DELETE_ROW_COUNT,
    UPDATE_ROW_COUNT,
    QUERY_ROW_COUNT,
    INSERT_SQL_COUNT,
    DELETE_SQL_COUNT,
    UPDATE_SQL_COUNT,
    QUERY_SQL_COUNT,
    TRANS_COUNT,
    SQL_COUNT,
    ROLLBACK_INSERT_ROW_COUNT,
    ROLLBACK_DELETE_ROW_COUNT,
    ROLLBACK_UPDATE_ROW_COUNT,
    ROLLBACK_INSERT_SQL_COUNT,
    ROLLBACK_DELETE_SQL_COUNT,
    ROLLBACK_UPDATE_SQL_COUNT,
    ROLLBACK_TRANS_COUNT,
    ROLLBACK_SQL_COUNT,
  };
  bool is_inited_;
  char svr_ip_[common::OB_IP_STR_BUFF];
  int32_t svr_port_;
  storage::ObIPartitionGroupIterator* partition_itertor_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPartitionAudit);
};

}  // namespace observer
}  // namespace oceanbase

#endif /* !OB_ALL_VIRTUAL_PARTITION_AUDIT_H */
