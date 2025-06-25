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

#ifndef OCEANBASE_SQL_OB_CREATE_CCL_RULE_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_CCL_RULE_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCreateCCLRuleStmt : public ObDDLStmt
{
public:
  ObCreateCCLRuleStmt();
  explicit ObCreateCCLRuleStmt(common::ObIAllocator *name_pool);
  virtual ~ObCreateCCLRuleStmt();
  inline void set_if_not_exists(bool if_not_exists) {
    create_ccl_rule_arg_.if_not_exist_ = if_not_exists;
  }
  inline void set_tenant_id(uint64_t tenant_id) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_tenant_id(tenant_id);
  }
  inline void set_ccl_rule_name(const common::ObString &ccl_rule_name) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_ccl_rule_name(ccl_rule_name);
  }
  inline void set_affect_all_databases(bool affect_all_databases) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_affect_for_all_databases(
        affect_all_databases);
  }
  inline void set_affect_all_tables(bool affect_all_tables) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_affect_for_all_tables(
        affect_all_tables);
  }
  inline void set_affect_database(const common::ObString &affect_database) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_affect_database(affect_database);
  }
  inline void set_affect_table(const common::ObString &affect_table) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_affect_table(affect_table);
  }
  inline void set_affect_user(common::ObString &user_name) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_affect_user_name(user_name);
  }
  inline void set_affect_hostname(common::ObString &host_name) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_affect_host(host_name);
  }
  inline void set_affect_scope(ObCCLAffectScope affect_scope) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_affect_scope(affect_scope);
  }
  inline void set_affect_dml(ObCCLAffectDMLType affect_dml) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_affect_dml(affect_dml);
  }
  inline void set_max_concurrency(uint64_t max_concurrency) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_max_concurrency(max_concurrency);
  }
  inline void set_filter_keywords(common::ObString &ccl_keyword) {
    create_ccl_rule_arg_.ccl_rule_schema_.set_ccl_keywords(ccl_keyword);
  }

  const obrpc::ObCreateCCLRuleArg &get_create_ccl_rule_arg() const { return create_ccl_rule_arg_; }
  virtual bool cause_implicit_commit() const
  {
    return true;
  }
  virtual obrpc::ObDDLArg &get_ddl_arg()
  {
    return create_ccl_rule_arg_;
  }

  TO_STRING_KV(K_(create_ccl_rule_arg));

private:
  obrpc::ObCreateCCLRuleArg create_ccl_rule_arg_;
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_CREATE_CCL_RULE_STMT_H_
