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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PREPROCESSOR_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PREPROCESSOR_H_

#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSPreprocessor
{
public:
  ObDBMSPreprocessor() {}
  virtual ~ObDBMSPreprocessor() {}

#define DECLARE_FUNC(func) \
  static int func( \
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(kkxccg1);
  DECLARE_FUNC(kkxccg2);
  DECLARE_FUNC(kkxccg3);

#undef DECLARE_FUNC

private:
  static int preprocessor(
    sql::ObExecContext &ctx, sql::ObSQLSessionInfo &session, ObString &source, ObString &result);
  static int construct_result(
    sql::ObExecContext &ctx, ObString &source, ObObj &result);
};

} // pl
} // oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PREPROCESSOR_H_