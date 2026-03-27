/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ROUTINE_LOAD_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ROUTINE_LOAD_H_

#include "sql/engine/ob_exec_context.h"
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSRoutineLoadMysql
{
public:
  ObDBMSRoutineLoadMysql() {}
  virtual ~ObDBMSRoutineLoadMysql() {}

  static int consume_kafka(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

};

} // namespace pl
} // namespace oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ROUTINE_LOAD_H_ */