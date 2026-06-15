/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEMA_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEMA_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSSchema
{
public:
  ObDBMSSchema() {}
  virtual ~ObDBMSSchema() {}

public:
  // DBMS_SCHEMA.RECYCLE_SCHEMA_HISTORY()
  static int recycle_schema_history(ObPLExecCtx &pl_ctx, sql::ParamStore &params, common::ObObj &result);

  // Tenant entry for inspection (placeholder, implementation to be filled later)
  static int run_inspection(ObPLExecCtx &pl_ctx, sql::ParamStore &params, common::ObObj &result);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEMA_H_
