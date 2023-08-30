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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_GRANT_STMT_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_GRANT_STMT_
#include "lib/string/ob_strings.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "share/ob_priv_common.h"

namespace oceanbase
{

namespace sql
{

class ObGrantStmt: public ObDDLStmt
{
public:
  explicit ObGrantStmt(common::ObIAllocator *name_pool);
  ObGrantStmt();
  virtual ~ObGrantStmt();
  int add_user(const common::ObString &user_name, const common::ObString &host_name,
               const common::ObString &pwd, const common::ObString &need_enc);
  int add_role(const common::ObString &role);
  int add_user(const common::ObString &user_name, const common::ObString &host_name);
  void add_priv(const ObPrivType priv);
  void set_grant_level(share::schema::ObPrivLevel grant_level);
  void set_priv_set(ObPrivSet priv_set);
  int set_database_name(const common::ObString &database_name);
  int set_table_name(const common::ObString &table_name);
  void set_masked_sql(const common::ObString &masked_sql) { masked_sql_ = masked_sql; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_need_create_user(bool need_create_user) { need_create_user_ = need_create_user; }
  void set_need_create_user_priv(bool need_create_user_priv)
  { need_create_user_priv_ = need_create_user_priv; }
  int set_priv_array(const share::ObRawPrivArray &array_in);
  int set_obj_priv_array(const share::ObRawObjPrivArray &array_in);
  void set_option(uint64_t option) { option_ = option; }
  void set_grantor_id(uint64_t grantor_id) { grantor_id_ = grantor_id; }
  bool get_need_create_user() const { return need_create_user_; }
  bool need_create_user_priv() const { return need_create_user_priv_; }
  const common::ObStrings& get_users() const { return users_; }
  const common::ObSArray<common::ObString>& get_role_names() const { return grant_arg_.roles_; };
  const common::ObString& get_database_name() const { return database_; }
  const common::ObString& get_table_name() const { return table_; }
  const common::ObString &get_masked_sql() const { return masked_sql_; }
  share::schema::ObPrivLevel get_grant_level() const {return grant_level_;}
  share::schema::ObObjectType get_object_type() const { return object_type_;}
  const ObSEArray<uint64_t, 4> get_ins_col_ids() const { return ins_col_ids_; }
  const ObSEArray<uint64_t, 4> get_upd_col_ids() const { return upd_col_ids_; }
  const ObSEArray<uint64_t, 4> get_ref_col_ids() const { return ref_col_ids_; }
  void set_object_type(share::schema::ObObjectType object_type) { object_type_ = object_type; }
  uint64_t get_object_id() const { return object_id_;}
  void set_object_id(uint64_t object_id) { object_id_ = object_id; }
  int set_ins_col_ids(ObSEArray<uint64_t, 4> &col_ids) { return ins_col_ids_.assign(col_ids); }
  int set_upd_col_ids(ObSEArray<uint64_t, 4> &col_ids) { return upd_col_ids_.assign(col_ids); }
  int set_ref_col_ids(ObSEArray<uint64_t, 4> &col_ids) { return ref_col_ids_.assign(col_ids); }
  void set_ref_query(ObSelectStmt* ref_query) { ref_query_ = ref_query; }
  int add_grantee(const common::ObString &grantee);
  void set_grant_all_tab_priv(bool is_grant_all) { is_grant_all_tab_priv_ = is_grant_all; }

  const share::ObRawPrivArray& get_priv_array() const {return sys_priv_array_;}
  const share::ObRawObjPrivArray& get_obj_priv_array() const {return obj_priv_array_;}

  uint64_t get_option() const { return option_; }; 
  uint64_t get_grantor_id() const { return grantor_id_; }
  ObSelectStmt* get_ref_query() const { return ref_query_; }
  ObPrivSet get_priv_set() const { return priv_set_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const common::ObStrings& get_grantees() const { return grantees_; }
  bool is_grant_all_tab_priv() const { return is_grant_all_tab_priv_; }

  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return grant_arg_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  ObPrivSet priv_set_;
  share::schema::ObPrivLevel grant_level_;
  common::ObString database_;
  common::ObString table_;
  uint64_t tenant_id_;
  common::ObStrings grantees_;
  common::ObStrings users_;//user1, host1, pwd1, nec1; user2, host2, pwd2, nec2;..
  common::ObString masked_sql_;
  bool need_create_user_;
  bool need_create_user_priv_; // grant user identified by pwd
  obrpc::ObGrantArg grant_arg_; // 用于返回exec_tenant_id_
  common::hash::ObPlacementHashSet<common::ObString, common::MAX_ENABLED_ROLES> user_name_set_;
  common::hash::ObPlacementHashSet<common::ObString, common::MAX_ENABLED_ROLES> role_name_set_;
  share::schema::ObObjectType object_type_;
  uint64_t object_id_;
  uint64_t grantor_id_;
  uint64_t option_;
  share::ObRawPrivArray sys_priv_array_;
  share::ObRawObjPrivArray obj_priv_array_;
  ObSEArray<uint64_t, 4> ins_col_ids_;
  ObSEArray<uint64_t, 4> upd_col_ids_;
  ObSEArray<uint64_t, 4> ref_col_ids_;
  ObSelectStmt *ref_query_; // 用于grant 视图时，对视图依赖的table,view等做递归权限check.
  bool is_grant_all_tab_priv_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGrantStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_DCL_OB_GRANT_STMT_
