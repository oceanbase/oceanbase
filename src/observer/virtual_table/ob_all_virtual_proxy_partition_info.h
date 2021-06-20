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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_PARTITION_INFO_
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_PARTITION_INFO_

#include "ob_all_virtual_proxy_base.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share
namespace sql {
class ObRawExpr;
}
namespace observer {
class ObAllVirtualProxyPartitionInfo : public ObAllVirtualProxyBaseIterator {
  enum ALL_VIRTUAL_PROXY_PARTITION_INFO_TABLE_COLUMNS {
    TABLE_ID = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    TENANT_ID,

    PART_LEVEL,
    ALL_PART_NUM,
    TEMPLATE_NUM,
    PART_ID_RULE_VER,

    PART_TYPE,
    PART_NUM,
    IS_COLUMN_TYPE,
    PART_SPACE,
    PART_EXPR,
    PART_EXPR_BIN,
    PART_RANGE_TYPE,
    PART_INTERVAL,
    PART_INTERVAL_BIN,
    INTERVAL_START,
    INTERVAL_START_BIN,

    SUB_PART_TYPE,
    SUB_PART_NUM,
    IS_SUB_COLUMN_TYPE,
    SUB_PART_SPACE,  // not used yet, for reserved
    SUB_PART_EXPR,
    SUB_PART_EXPR_BIN,
    SUB_PART_RANGE_TYPE,
    DEF_SUB_PART_INTERVAL,
    DEF_SUB_PART_INTERVAL_BIN,
    DEF_SUB_INTERVAL_START,
    DEF_SUB_INTERVAL_START_BIN,

    PART_KEY_NUM,
    PART_KEY_NAME,
    PART_KEY_TYPE,
    PART_KEY_IDX,  // used for calc insert stmt
    PART_KEY_LEVEL,
    PART_KEY_EXTRA,  // reserved for other info

    SPARE1,
    SPARE2,
    SPARE3,
    SPARE4,
    SPARE5,
    SPARE6,
  };

public:
  ObAllVirtualProxyPartitionInfo();
  virtual ~ObAllVirtualProxyPartitionInfo();

  virtual int inner_open();
  int inner_get_next_row();

  int fill_cells(const share::schema::ObTableSchema& table_schema);

private:
  int gen_proxy_part_pruning_str(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObColumnSchemaV2* column_schema, common::ObString& proxy_check_partition_str);

  int build_check_str_to_raw_expr(const common::ObString& check_expr_str,
      const share::schema::ObTableSchema& table_schema, sql::ObRawExpr*& check_expr);

private:
  int64_t next_table_idx_;
  int64_t next_part_key_idx_;
  common::ObSEArray<const share::schema::ObTableSchema*, 1> table_schemas_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualProxyPartitionInfo);
};

}  // end of namespace observer
}  // end of namespace oceanbase
#endif /* OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_PARTITION_INFO_ */
