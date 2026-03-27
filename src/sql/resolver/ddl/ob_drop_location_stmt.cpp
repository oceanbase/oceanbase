/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #include "sql/resolver/ddl/ob_drop_location_stmt.h"

 using namespace oceanbase::common;
 using namespace oceanbase::share::schema;

 namespace oceanbase
 {
 namespace sql
 {
 ObDropLocationStmt::ObDropLocationStmt()
   : ObDDLStmt(stmt::T_DROP_LOCATION),
     arg_()
 {
 }

 ObDropLocationStmt::ObDropLocationStmt(common::ObIAllocator *name_pool)
   : ObDDLStmt(name_pool, stmt::T_DROP_LOCATION),
     arg_()
 {
 }

 ObDropLocationStmt::~ObDropLocationStmt()
 {
 }
 } // namespace sql
 } // namespace oceanbase