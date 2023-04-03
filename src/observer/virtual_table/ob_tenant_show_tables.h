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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_TABLES_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_TABLES_

#include "share/ob_virtual_table_iterator.h"
#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
using oceanbase::common::OB_APP_MIN_COLUMN_ID;
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace schema
{
class ObSimpleTableSchemaV2;
}
}
namespace observer
{
class ObTenantShowTables : public common::ObVirtualTableIterator
{
  enum TENANT_ALL_TABLES_COLUMN
  {
    DATABASE_ID = OB_APP_MIN_COLUMN_ID,
    TABLE_NAME = OB_APP_MIN_COLUMN_ID + 1,
    TABLE_TYPE = OB_APP_MIN_COLUMN_ID + 2,
  };
public:
  ObTenantShowTables();
  virtual ~ObTenantShowTables();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
private:
  int inner_get_next_row();
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObSEArray<const share::schema::ObSimpleTableSchemaV2 *, 128> table_schemas_;
  int64_t table_schema_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantShowTables);
};

}// observer
}// oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_TABLES_ */
