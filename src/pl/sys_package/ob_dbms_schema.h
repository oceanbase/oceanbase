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
