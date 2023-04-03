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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_USER_PRIVILEGES_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_USER_PRIVILEGES_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_priv_type.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace share
{
namespace schema
{
class ObUserInfo;
}
}
namespace observer
{
class ObInfoSchemaUserPrivilegesTable : public common::ObVirtualTableScannerIterator
{
public:
  class StaticInit {
  public:
    StaticInit();
  };
  friend class ObInfoSchemaUserPrivilegesTable::StaticInit;

  ObInfoSchemaUserPrivilegesTable();
  virtual ~ObInfoSchemaUserPrivilegesTable();

  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);

  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_user_id(uint64_t user_id) { user_id_ = user_id; }

private:
  enum USER_PRIVS_COLUMN
  {
    GRANTEE = 16,
    TABLE_CATALOG,
    PRIVILEGE_TYPE,
    IS_GRANTABLE,
    MAX_USER_PRIVS_COLUMN
  };
  enum {
    MAX_COL_COUNT = 4,
    USERNAME_AUX_LEN = 6// "''@''" + '\0'
  };

  int get_user_infos(const uint64_t tenant_id,
                     const uint64_t user_id,
                     common::ObArray<const share::schema::ObUserInfo *> &user_infos);
  int fill_row_with_user_info(const share::schema::ObUserInfo &user_info);

  static const char *priv_type_strs[OB_PRIV_MAX_SHIFT + 1];
  uint64_t tenant_id_;
  uint64_t user_id_;

  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaUserPrivilegesTable);
};
}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLES_SHOW_ */
