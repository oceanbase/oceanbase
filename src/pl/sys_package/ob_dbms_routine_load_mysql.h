/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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