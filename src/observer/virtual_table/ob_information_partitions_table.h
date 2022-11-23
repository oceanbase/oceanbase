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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_PARTITIONS_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_PARTITIONS_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace common
{
class ObObj;
}
namespace share
{
namespace schema
{
class ObSimpleTableSchemaV2;
class ObDatabaseSchema;
}
}

namespace observer
{
class ObInfoSchemaPartitionsTable : public common::ObVirtualTableScannerIterator
{
public:
  ObInfoSchemaPartitionsTable();
  virtual ~ObInfoSchemaPartitionsTable();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

  inline void set_tenant_id(uint64_t tenant_id);

private:
  int add_partitions(const share::schema::ObDatabaseSchema &database_schema,
                     common::ObObj *cells,
                     const int64_t col_count);
  int add_partitions(const share::schema::ObSimpleTableSchemaV2 &table_schema,
                     const common::ObString &database_name,
                     common::ObObj *cells,
                     const int64_t col_count);
  int gen_high_bound_val_str(
      const bool is_oracle_mode,
      const share::schema::ObBasePartition *part,
      common::ObString &val_str);
  int gen_list_bound_val_str(
      const bool is_oracle_mode,
      const share::schema::ObBasePartition *part,
      common::ObString &val_str);
  uint64_t tenant_id_;
private:
  enum PARTITION_COLUMN
  {
    TABLE_CATALOG = common::OB_APP_MIN_COLUMN_ID,
    TABLE_SCHEMA,
    TABLE_NAME,
    PARTITION_NAME,
    SUBPARTITION_NAME,
    PARTITION_ORDINAL_POSITION,
    SUBPARTITION_ORDINAL_POSITION,
    PARTITION_METHOD,
    SUBPARTITION_METHOD,
    PARTITION_EXPRESSION,
    SUBPARTITION_EXPRESSION,
    PARTITION_DESCRIPTION,
    TABLE_ROWS,
    AVG_ROW_LENGTH,
    DATA_LENGTH,
    MAX_DATA_LENGTH,
    INDEX_LENGTH,
    DATA_FREE,
    CREATE_TIME,
    UPDATE_TIME,
    CHECK_TIME,
    CHECKSUM,
    PARTITION_COMMENT,
    NODEGROUP,
    TABLESPACE_NAME,
    MAX_PARTITIONS_COLUMN
  };
  static const int64_t PARTITION_COLUMN_COUNT = 25;
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaPartitionsTable);
};

inline void ObInfoSchemaPartitionsTable::set_tenant_id(uint64_t tenant_id)
{
  tenant_id_ = tenant_id;
}

}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_PARTITIONS_TABLE */
