/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_TRIGGERS_TABLE_H_
#define OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_TRIGGERS_TABLE_H_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace sql
{
  class ObSQLSessionInfo;
}
namespace observer
{
class ObInfoSchemaTriggersTable : public common::ObVirtualTableScannerIterator
{
private:
  enum MySQLTriggersTableColumns {
    TRIGGER_CATALOG = 16,
    TRIGGER_SCHEMA,
    TRIGGER_NAME,
    EVENT_MANIPULATION,
    EVENT_OBJECT_CATALOG,
    EVENT_OBJECT_SCHEMA,
    EVENT_OBJECT_TABLE,
    ACTION_ORDER,
    ACTION_CONDITION,
    ACTION_STATEMENT,
    ACTION_ORIENTATION,
    ACTION_TIMING,
    ACTION_REFERENCE_OLD_TABLE,
    ACTION_REFERENCE_NEW_TABLE,
    ACTION_REFERENCE_OLD_ROW,
    ACTION_REFERENCE_NEW_ROW,
    CREATED,
    SQL_MODE,
    DEFINER,
    CHARACTER_SET_CLIENT,
    COLLATION_CONNECTION,
    DATABASE_COLLATION,
  };
public:
  ObInfoSchemaTriggersTable();
  virtual ~ObInfoSchemaTriggersTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

private:
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaTriggersTable);
};
}
}

#endif // OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_TRIGGERS_TABLE_H_
