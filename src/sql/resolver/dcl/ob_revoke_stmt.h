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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_REVOKE_STMT_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_REVOKE_STMT_
#include "lib/string/ob_strings.h"
#include "share/schema/ob_priv_type.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{

class ObRevokeStmt: public ObDDLStmt
{
public:
  explicit ObRevokeStmt(common::ObIAllocator *name_pool);
  ObRevokeStmt();
  virtual ~ObRevokeStmt();

  int add_user(const uint64_t user_id);
  int add_role(const uint64_t role_id);
  int add_role_ora(const uint64_t role_id);
  void add_priv(const ObPrivType priv);
  void set_grant_level(share::schema::ObPrivLevel grant_level);
  void set_priv_set(ObPrivSet priv_set);
  int set_database_name(const common::ObString &database_name);
  int set_table_name(const common::ObString &table_name);
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_revoke_all(bool revoke_all) { revoke_all_ = revoke_all; }
  int set_priv_array(const share::ObRawPrivArray &array_in);
  int set_obj_priv_array(const share::ObRawObjPrivArray &array_in);
  void set_obj_id(uint64_t obj_id) { obj_id_ = obj_id; }
  void set_grantor_id(uint64_t grantor_id) { grantor_id_ = grantor_id; }
  void set_revoke_all_ora(bool flag) { revoke_all_ora_ = flag; }
  int add_grantee(const common::ObString &grantee);

  const common::ObIArray<uint64_t>& get_users() const;
  const common::ObIArray<uint64_t>& get_roles() const;
  const common::ObString& get_database_name() const;
  const common::ObString& get_table_name() const;
  share::schema::ObPrivLevel get_grant_level() const {return grant_level_;}
  share::schema::ObObjectType get_object_type() const {return object_type_;}
  void set_object_type(share::schema::ObObjectType object_type) { object_type_ = object_type; }
  const share::ObRawPrivArray& get_priv_array() const {return sys_priv_array_;}
  const share::ObRawObjPrivArray& get_obj_priv_array() const {return obj_priv_array_;}
  uint64_t get_obj_id() const { return obj_id_; }
  uint64_t get_grantor_id() const { return grantor_id_; }
  bool get_revoke_all_ora() const { return revoke_all_ora_; }

  ObPrivSet get_priv_set() const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  bool get_revoke_all() const { return revoke_all_; }
  const common::ObStrings& get_grantees() const { return grantees_; }
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() 
  { 
    return share::schema::OB_PRIV_USER_LEVEL == grant_level_ ? static_cast<obrpc::ObDDLArg &>(user_arg_) 
        : (share::schema::OB_PRIV_DB_LEVEL == grant_level_ ? static_cast<obrpc::ObDDLArg &>(db_arg_)
        : (share::schema::OB_PRIV_TABLE_LEVEL == grant_level_ ?  static_cast<obrpc::ObDDLArg &>(table_arg_)
        : static_cast<obrpc::ObDDLArg &>(syspriv_arg_))); 
  }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  ObPrivSet priv_set_;
  share::schema::ObPrivLevel grant_level_;
  common::ObString database_;
  common::ObString table_;
  uint64_t tenant_id_;
  common::ObArray<uint64_t, common::ModulePageAllocator, true> users_;
  bool revoke_all_;
  common::ObStrings grantees_;
  obrpc::ObRevokeUserArg user_arg_;
  obrpc::ObRevokeDBArg db_arg_;
  obrpc::ObRevokeTableArg table_arg_;
  obrpc::ObRevokeSysPrivArg syspriv_arg_;
  common::hash::ObPlacementHashSet<uint64_t, common::MAX_ENABLED_ROLES> role_id_set_;
  share::schema::ObObjectType object_type_;
  share::ObRawPrivArray sys_priv_array_;
  share::ObRawObjPrivArray obj_priv_array_;
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t grantor_id_;
  bool revoke_all_ora_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRevokeStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_DCL_OB_REVOKE_STMT_
