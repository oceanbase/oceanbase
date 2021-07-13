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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_SUB_PARTITOIN_
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_SUB_PARTITOIN_

#include "ob_all_virtual_proxy_base.h"
#include "share/schema/ob_part_mgr_util.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObSubPartition;
class ObPartIteratorV2;
class ObSubPartIteratorV2;
}  // namespace schema
}  // namespace share
namespace observer {
class ObAllVirtualProxySubPartition : public ObAllVirtualProxyBaseIterator {
  enum ALL_VIRTUAL_PROXY_SUB_PARTITOIN_TABLE_COLUMNS {
    TABLE_ID = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    PART_ID,
    SUB_PART_ID,

    TENANT_ID,
    PART_NAME,
    STATUS,
    LOW_BOUND_VAL,
    LOW_BOUND_VAL_BIN,
    HIGH_BOUND_VAL,
    HIGH_BOUND_VAL_BIN,

    SPARE1,
    SPARE2,
    SPARE3,
    SPARE4,
    SPARE5,
    SPARE6,
  };

  enum ALL_VIRTUAL_PROXY_SUB_PARTITOIN_ROWKEY_IDX {
    TABLE_ID_IDX = 0,
    PART_ID_IDX,
    SUB_PART_ID_IDX,
    ROW_KEY_COUNT,
  };

public:
  ObAllVirtualProxySubPartition();
  virtual ~ObAllVirtualProxySubPartition();

  virtual int inner_open();
  virtual int inner_get_next_row();

  int fill_cells(const share::schema::ObSubPartition& table_schema);

private:
  share::schema::ObPartIteratorV2 part_iter_;
  share::schema::ObSubPartIteratorV2 subpart_iter_;
  share::schema::ObPartitionFuncType part_func_type_;
  bool is_sub_part_template_;
  const share::schema::ObTableSchema* table_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualProxySubPartition);
};

}  // end of namespace observer
}  // end of namespace oceanbase
#endif /* OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_SUB_PARTITOIN_ */
