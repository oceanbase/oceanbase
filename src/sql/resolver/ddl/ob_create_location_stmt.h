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

 #ifndef OCEANBASE_SQL_OB_CREATE_LOCATION_STMT_H_
 #define OCEANBASE_SQL_OB_CREATE_LOCATION_STMT_H_

 #include "share/ob_rpc_struct.h"
 #include "sql/resolver/ddl/ob_ddl_stmt.h"

 namespace oceanbase
 {
 namespace sql
 {
 class ObCreateLocationStmt : public ObDDLStmt
 {
 public:
   ObCreateLocationStmt();
   explicit ObCreateLocationStmt(common::ObIAllocator *name_pool);
   virtual ~ObCreateLocationStmt();

   virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
   virtual bool cause_implicit_commit() const { return true; }

   obrpc::ObCreateLocationArg &get_create_location_arg() { return arg_; }

   void set_tenant_id(const uint64_t tenant_id) { arg_.schema_.set_tenant_id(tenant_id); }
   void set_user_id(const uint64_t user_id) { arg_.user_id_ = user_id; }
   void set_or_replace(bool or_replace) { arg_.or_replace_ = or_replace; }
   int set_location_name(const common::ObString &name) { return arg_.schema_.set_location_name(name); }
   int set_location_url(const common::ObString &url) { return arg_.schema_.set_location_url(url); }
   int set_location_access_info(const common::ObString &access_info) { return arg_.schema_.set_location_access_info(access_info); }
   void set_masked_sql(const common::ObString &masked_sql) { masked_sql_ = masked_sql; }
   const common::ObString& get_masked_sql() { return masked_sql_; }
   bool is_or_replace() const {return arg_.or_replace_;}

   TO_STRING_KV(K_(arg));
 private:
   obrpc::ObCreateLocationArg arg_;
   common::ObString masked_sql_;
 private:
   DISALLOW_COPY_AND_ASSIGN(ObCreateLocationStmt);
 };
 } // namespace sql
 } // namespace oceanbase

 #endif // OCEANBASE_SQL_OB_CREATE_LOCATION_STMT_H_
