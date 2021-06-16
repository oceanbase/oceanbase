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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PARTITION_ITEM__
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PARTITION_ITEM__

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_part_mgr_util.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObSimpleTableSchemaV2;
class ObTablePartItemIterator;
struct ObPartitionItem;
}  // namespace schema
}  // namespace share
namespace observer {
class ObAllVirtualPartitionItem : public common::ObVirtualTableScannerIterator {
  enum PARTITION_STAT {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    TABLE_ID,
    PARTITION_ID,
    TENANT_NAME,
    TABLE_NAME,

    PARTITION_LEVEL,
    PARTITION_NUM,
    PARTITION_IDX,

    PART_FUNC_TYPE,
    PART_NUM,
    PART_NAME,
    PART_IDX,
    PART_ID,
    PART_HIGH_BOUND,

    SUBPART_FUNC_TYPE,
    SUBPART_NUM,
    SUBPART_NAME,
    SUBPART_IDX,
    SUBPART_ID,
    SUBPART_HIGH_BOUND
  };
  static const int32_t COLUMN_COUNT = 20;
  static const int64_t BOUND_BUF_LENGTH = 100;

public:
  ObAllVirtualPartitionItem();
  virtual ~ObAllVirtualPartitionItem();

  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPartitionItem);
  int fill_row_cells(const share::schema::ObSimpleTableSchemaV2& table_schema, share::schema::ObPartitionItem& item,
      const char* tenant_name);

  int64_t last_tenant_idx_;
  int64_t last_table_idx_;
  share::schema::ObTablePartItemIterator part_iter_;
  share::schema::ObPartitionItem last_item_;
  bool has_more_;
  char part_high_bound_buf_[BOUND_BUF_LENGTH];
  char subpart_high_bound_buf_[BOUND_BUF_LENGTH];
};
}  // namespace observer
}  // namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_PARTITION_ITEM_
