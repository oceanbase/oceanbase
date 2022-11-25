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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_CREATE_USE_STMT_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_CREATE_USE_STMT_

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/string/ob_strings.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObCreateUserStmt: public ObDDLStmt
{
public:
  explicit ObCreateUserStmt(common::ObIAllocator *name_pool);
  ObCreateUserStmt();
  virtual ~ObCreateUserStmt();

  void set_if_not_exists(const bool if_not_exists) { if_not_exist_ = if_not_exists; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  int add_user(const common::ObString &user_name,
               const common::ObString &host_name,
               const common::ObString &password,
               const common::ObString &need_enc);
  int add_ssl_info(const common::ObString &ssl_type,
                   const common::ObString &ssl_cipher,
                   const common::ObString &x509_issuer,
                   const common::ObString &x509_subject);
  void set_masked_sql(const common::ObString &masked_sql) { masked_sql_ = masked_sql; }
  uint64_t get_tenant_id() { return tenant_id_; }
  bool get_if_not_exists() const { return if_not_exist_; }
  const common::ObStrings &get_users() const { return users_; }
  const common::ObString &get_masked_sql() const { return masked_sql_; }
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_user_arg_; }
  void set_profile_id(const uint64_t profile_id) { profile_id_ = profile_id; }
  uint64_t get_profile_id() const { return profile_id_; }
  common::ObString &get_primary_zone() { return create_user_arg_.primary_zone_;}
  int set_primary_zone(const ObString &primary_zone) 
  { 
    create_user_arg_.primary_zone_ = primary_zone; 
    return OB_SUCCESS;
  }
  uint64_t get_max_connections_per_hour() { return max_connections_per_hour_; }
  void set_max_connections_per_hour(uint64_t val) { max_connections_per_hour_ = val; }
  uint64_t get_max_user_connections() { return max_user_connections_; }
  void set_max_user_connections(uint64_t val) { max_user_connections_ = val; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  uint64_t tenant_id_;
  common::ObStrings users_; // (user1, host1, pass1, need_enc1;
                            //  user2, host2, pass2, need_enc2,
                            //  ...,
                            //  ssl_type, ssl_cipher, x509_issuer, x509_subject)
  common::ObString masked_sql_;
  bool if_not_exist_;
  uint64_t profile_id_; //only used in oracle mode
  obrpc::ObCreateUserArg create_user_arg_; // 用于返回exec_tenant_id_
  uint64_t max_connections_per_hour_;
  uint64_t max_user_connections_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateUserStmt);
};
} // end namespace sql
} // end namespace oceanbase
#endif //OCEANBASE_SQL_RESOLVER_DCL_OB_CREATE_USER_STMT_
