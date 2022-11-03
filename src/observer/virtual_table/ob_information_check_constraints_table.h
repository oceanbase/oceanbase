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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_CHECK_CONSTRAINTS_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_CHECK_CONSTRAINTS_
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase {
namespace observer {
class ObInfoSchemaCheckConstraintsTable : public common::ObVirtualTableScannerIterator {
public:
  ObInfoSchemaCheckConstraintsTable();
  virtual ~ObInfoSchemaCheckConstraintsTable();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

  inline void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }

private:
  int add_check_constraints(
      const share::schema::ObDatabaseSchema& database_schema, common::ObObj* cells, const int64_t col_count);
  int add_check_constraints(const share::schema::ObTableSchema& table_schema, const common::ObString& database_name,
      common::ObObj* cells, const int64_t col_count);

  uint64_t tenant_id_;

private:
  enum TABLE_CONSTRAINTS_COLUMN {
    CONSTRAINT_CATALOG = common::OB_APP_MIN_COLUMN_ID,
    CONSTRAINT_SCHEMA,
    CONSTRAINT_NAME,
    CHECK_CLAUSE,
    MAX_CHECK_CONSTRAINTS_COLUMN
  };
  static const int64_t CHECK_CONSTRAINTS_COLUMN_COUNT = 4;
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaCheckConstraintsTable);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_CHECK_CONSTRAINTS_ */
