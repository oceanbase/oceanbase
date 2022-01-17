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

#ifndef _OB_RS_RESTORE_SQL_MODIFIER_IMPL_H_
#define _OB_RS_RESTORE_SQL_MODIFIER_IMPL_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/string/ob_string.h"
#include "observer/ob_restore_sql_modifier.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase {
namespace sql {
class ObResultSet;
class ObCreateTenantStmt;
class ObCreateDatabaseStmt;
class ObCreateTableStmt;
class ObCreateIndexStmt;
class ObCreateTablegroupStmt;
// class ObCreateResourcePoolStmt;
}  // namespace sql

namespace share {
class ObRestoreArgs;
}

namespace rootserver {
class ObServerManager;
class ObRestoreSQLModifierImpl : public observer::ObRestoreSQLModifier {
public:
  ObRestoreSQLModifierImpl(share::ObRestoreArgs& restore_args, common::hash::ObHashSet<uint64_t>& dropped_index_ids);
  virtual ~ObRestoreSQLModifierImpl();
  int modify(sql::ObResultSet& rs);
  void set_tenant_name(const common::ObString& name)
  {
    tenant_name_ = name;
  }

private:
  /* functions */
  int handle_create_tenant(sql::ObCreateTenantStmt* stmt);
  int handle_create_database(sql::ObCreateDatabaseStmt* stmt);
  int handle_create_table(sql::ObCreateTableStmt* stmt);
  int handle_create_index(sql::ObCreateIndexStmt* stmt);
  int handle_create_tablegroup(sql::ObCreateTablegroupStmt* stmt);
  /*
  int handle_create_resource_pool(sql::ObCreateResourcePoolStmt *stmt);
  int handle_replace_unit_num(sql::ObCreateResourcePoolStmt *stmt);
  int handle_replace_zone_name(sql::ObCreateResourcePoolStmt *stmt);
  */

  /* variables */
  common::ObString tenant_name_;
  share::ObRestoreArgs& restore_args_;
  common::hash::ObHashSet<uint64_t>& dropped_index_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreSQLModifierImpl);
};
}  // namespace rootserver

}  // namespace oceanbase
#endif /* _OB_RS_RESTORE_SQL_MODIFIER_IMPL_H_ */
//// end of header file
