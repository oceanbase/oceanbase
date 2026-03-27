/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_EXTERNAL_TABLE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_EXTERNAL_TABLE_H_
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSExternalTable
{
public:
  static int auto_refresh_external_table(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
};

} // end of pl
} // end of oceanbase
#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_EXTERNAL_TABLE_H_ */
