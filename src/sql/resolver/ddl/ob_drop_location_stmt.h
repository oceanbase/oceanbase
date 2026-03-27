/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_SQL_OB_DROP_LOCATION_STMT_H_
 #define OCEANBASE_SQL_OB_DROP_LOCATION_STMT_H_

 #include "share/ob_rpc_struct.h"
 #include "sql/resolver/ddl/ob_ddl_stmt.h"

 namespace oceanbase
 {
 namespace sql
 {
 class ObDropLocationStmt : public ObDDLStmt
 {
 public:
   ObDropLocationStmt();
   explicit ObDropLocationStmt(common::ObIAllocator *name_pool);
   virtual ~ObDropLocationStmt();

   virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
   virtual bool cause_implicit_commit() const { return true; }

   obrpc::ObDropLocationArg &get_drop_location_arg() { return arg_; }

   void set_tenant_id(const uint64_t id) { arg_.tenant_id_ = id; }
   void set_location_name(const common::ObString &name) { arg_.location_name_ = name; }

   TO_STRING_KV(K_(arg));
 private:
   obrpc::ObDropLocationArg arg_;
 private:
   DISALLOW_COPY_AND_ASSIGN(ObDropLocationStmt);
 };
 } // namespace sql
 } // namespace oceanbase

 #endif // OCEANBASE_SQL_OB_DROP_LOCATION_STMT_H_
