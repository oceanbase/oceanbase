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

#ifndef OB_ALTER_USER_PRIMARY_ZONE_STMT_H_
#define OB_ALTER_USER_PRIMARY_ZONE_STMT_H_

#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObAlterUserPrimaryZoneStmt: public ObDDLStmt
{
public:
  ObAlterUserPrimaryZoneStmt();
  explicit ObAlterUserPrimaryZoneStmt(common::ObIAllocator *name_pool);
  virtual ~ObAlterUserPrimaryZoneStmt();
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
  void set_tenant_id(int64_t tenant_id) { arg_.database_schema_.set_tenant_id(tenant_id); } 
  int set_database_name(const ObString &database_name) 
      { return arg_.database_schema_.set_database_name(database_name); }
  int set_primary_zone(const ObString &primary_zone) 
      { return OB_SUCCESS; } // not supported
  int add_primary_zone_option() 
      { return arg_.alter_option_bitset_.add_member(obrpc::ObAlterDatabaseArg::PRIMARY_ZONE); }
  TO_STRING_KV(K_(stmt_type), K_(arg));
public:
  // data members
  obrpc::ObAlterDatabaseArg arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterUserPrimaryZoneStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OB_ALTER_USER_PROFILE_STMT_H_
