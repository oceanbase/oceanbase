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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEDULER_MYSQL_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEDULER_MYSQL_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSSchedulerMysql
{
public:
  ObDBMSSchedulerMysql() {}
  virtual ~ObDBMSSchedulerMysql() {}

public:

#define DECLARE_FUNC(func) \
  static int func(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(create_job);
  DECLARE_FUNC(disable);
  DECLARE_FUNC(enable);
  DECLARE_FUNC(set_attribute);

#undef DECLARE_FUNC

private:
  static int execute_sql(sql::ObExecContext &ctx, ObSqlString &sql, int64_t &affected_rows);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SCHEDULER_MYSQL_H_ */