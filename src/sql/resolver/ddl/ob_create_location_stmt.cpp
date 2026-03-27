/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #include "sql/resolver/ddl/ob_create_location_stmt.h"

 using namespace oceanbase::common;
 using namespace oceanbase::share::schema;

 namespace oceanbase
 {
 namespace sql
 {
 ObCreateLocationStmt::ObCreateLocationStmt()
   : ObDDLStmt(stmt::T_CREATE_LOCATION),
     arg_()
 {
 }

 ObCreateLocationStmt::ObCreateLocationStmt(common::ObIAllocator *name_pool)
   : ObDDLStmt(name_pool, stmt::T_CREATE_LOCATION),
     arg_()
 {
 }

 ObCreateLocationStmt::~ObCreateLocationStmt()
 {
 }
 } // namespace sql
 } // namespace oceanbase