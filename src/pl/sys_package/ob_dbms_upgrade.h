/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_UPGRADE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_UPGRADE_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSUpgrade
{
public:
  ObDBMSUpgrade() {}
  virtual ~ObDBMSUpgrade() {}
public:
  static int upgrade_single(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int upgrade_all(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_job_action(ObSqlString &job_action, ObSqlString &query_sql);
  static int flush_dll_ncomp(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_UPGRADE_H_ */
