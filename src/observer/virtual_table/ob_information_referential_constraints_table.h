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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_REFERENTIAL_CONSTRAINTS_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_REFERENTIAL_CONSTRAINTS_
#include "share/ob_virtual_table_scanner_iterator.h"

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
class ObTableSchema;
class ObDatabaseSchema;

}
}

namespace observer
{

class ObInfoSchemaReferentialConstraintsTable : public common::ObVirtualTableScannerIterator
{
public:
  ObInfoSchemaReferentialConstraintsTable();
  virtual ~ObInfoSchemaReferentialConstraintsTable();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

  inline void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }

private:
  int add_fk_constraints_in_db(
      const share::schema::ObDatabaseSchema &database_schema,
      common::ObObj *cells,
      const int64_t col_count);
  int add_fk_constraints_in_table(
      const share::schema::ObTableSchema &table_schema,
      const common::ObString &database_name,
      common::ObObj *cells,
      const int64_t col_count);
  uint64_t tenant_id_;

private:
  enum REFERENTIAL_CONSTRAINTS_COLUMN_COUNT_COLUMN
  {
    CONSTRAINT_CATALOG = common::OB_APP_MIN_COLUMN_ID,
    CONSTRAINT_SCHEMA,
    CONSTRAINT_NAME,
    UNIQUE_CONSTRAINT_CATALOG,
    UNIQUE_CONSTRAINT_SCHEMA,
    UNIQUE_CONSTRAINT_NAME,
    MATCH_OPTION,
    UPDATE_RULE,
    DELETE_RULE,
    TABLE_NAME,
    REFERENCED_TABLE_NAME,
    MAX_REFERENTIAL_CONSTRAINTS_COLUMN
  };
  static const int64_t REFERENTIAL_CONSTRAINTS_COLUMN_COUNT = 11;
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaReferentialConstraintsTable);
};


}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_REFERENTIAL_CONSTRAINTS_ */
