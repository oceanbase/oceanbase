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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_SET_PASSWORD_STMT_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_SET_PASSWORD_STMT_
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/string/ob_strings.h"
namespace oceanbase
{
namespace sql
{
class ObSetPasswordStmt: public ObDDLStmt
{
public:
  explicit ObSetPasswordStmt(common::ObIAllocator *name_pool);
  ObSetPasswordStmt();
  virtual ~ObSetPasswordStmt();
  int set_user_password(const common::ObString &user_name, const common::ObString &host_name,
                        const common::ObString &password);
  int add_ssl_info(const common::ObString &ssl_type,
                   const common::ObString &ssl_cipher,
                   const common::ObString &x509_issuer,
                   const common::ObString &x509_subject);
  void set_masked_sql(const common::ObString &masked_sql) { masked_sql_ = masked_sql; }
  const common::ObStrings *get_user_password() const { return &user_pwd_; }
  const common::ObString &get_masked_sql() const { return masked_sql_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  bool get_need_enc() const { return need_enc_; }
  void set_need_enc(bool need_enc) { need_enc_ = need_enc; }
  bool get_for_current_user() const { return for_current_user_; }
  void set_for_current_user(bool for_current_user) { for_current_user_ = for_current_user; }
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObSetPasswdArg &get_ddl_arg() { return set_password_arg_; }
  void set_modify_max_connections(bool value) { modify_max_connections_ = value; }
  bool get_modify_max_connections() { return modify_max_connections_; }
  void set_max_connections_per_hour(uint64_t value) { max_connections_per_hour_ = value; }
  uint64_t get_max_connections_per_hour() { return max_connections_per_hour_; }
  void set_max_user_connections(uint64_t value) { max_user_connections_ = value; }
  uint64_t get_max_user_connections() { return max_user_connections_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  common::ObStrings user_pwd_;//username1, hostname1, passwd1;
                              //username2, hostname2, passwd2...
                              //ssl_type, ssl_cipher, x509_issuer, x509_subject
  common::ObString masked_sql_;
  uint64_t tenant_id_;
  bool need_enc_;
  bool for_current_user_;
  obrpc::ObSetPasswdArg set_password_arg_; // 用于返回exec_tenant_id_
  bool modify_max_connections_;
  uint64_t max_connections_per_hour_;
  uint64_t max_user_connections_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetPasswordStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_DCL_OB_SET_PASSWORD_STMT_
