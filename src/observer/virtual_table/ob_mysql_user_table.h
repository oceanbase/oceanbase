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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_USER_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_USER_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObMySQLUserTable : public common::ObVirtualTableScannerIterator
{
private:
  enum MySQLUserTableColumns {
    HOST = 16,
    USER_NAME,
    PASSWD,
    SELECT_PRIV,
    INSERT_PRIV,
    UPDATE_PRIV,
    DELETE_PRIV,
    CREATE_PRIV,
    DROP_PRIV,
    RELOAD_PRIV,
    SHUTDOWN_PRIV,
    PROCESS_PRIV,
    FILE_PRIV,
    GRANT_PRIV,
    REFERENCES_PRIV,
    INDEX_PRIV,
    ALTER_PRIV,
    SHOW_DB_PRIV,
    SUPER_PRIV,
    CREATE_TMP_TABLE_PRIV,
    LOCK_TABLES_PRIV,
    EXECUTE_PRIV,
    REPL_SLAVE_PRIV,
    REPL_CLIENT_PRIV,
    CREATE_VIEW_PRIV,
    SHOW_VIEW_PRIV,
    CREATE_ROUTINE_PRIV,
    ALTER_ROUTINE_PRIV,
    CREATE_USER_PRIV,
    EVENT_PRIV,
    TRIGGER_PRIV,
    CREATE_TABLESPACE_PRIV,
    SSL_TYPE,
    SSL_CIPHER,
    X509_ISSUER,
    X509_SUBJECT,
    MAX_QUESTIONS,
    MAX_UPDATES,
    MAX_CONNECTIONS,
    MAX_USER_CONNECTIONS,
    PLUGIN,
    AUTHENTICATION_STRING,
    PASSWORD_EXPIRED,
    ACCOUNT_LOCKED,
    DROP_DATABASE_LINK_PRIV,
    CREATE_DATABASE_LINK_PRIV,
  };
public:
  ObMySQLUserTable();
  virtual ~ObMySQLUserTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

private:
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLUserTable);
};
}
}
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_USER_TABLE_
