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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_DB_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_DB_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObUserInfo;
}
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObMySQLDBTable : public common::ObVirtualTableScannerIterator
{
private:
  enum MySQLDBTableColumns {
    HOST = 16,
    DB,
    USER,
    SELECT_PRIV,
    INSERT_PRIV,
    UPDATE_PRIV,
    DELETE_PRIV,
    CREATE_PRIV,
    DROP_PRIV,
    GRANT_PRIV,
    REFERENCES_PRIV,
    INDEX_PRIV,
    ALTER_PRIV,
    CREATE_TMP_TABLE_PRIV,
    LOCK_TABLES_PRIV,
    CREATE_VIEW_PRIV,
    SHOW_VIEW_PRIV,
    CREATE_ROUTINE_PRIV,
    ALTER_ROUTINE_PRIV,
    EXECUTE_PRIV,
    EVENT_PRIV,
    TRIGGER_PRIV,
  };

public:
  ObMySQLDBTable();
  virtual ~ObMySQLDBTable();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

private:
  int get_user_info(const uint64_t tenant_id,
                    const uint64_t user_id,
                    const share::schema::ObUserInfo *&user_info);
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLDBTable);
};
}
}
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_DB_TABLE_
