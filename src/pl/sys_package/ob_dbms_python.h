/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PYTHON_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PYTHON_H_


#include "sql/ob_sql_define.h"


namespace oceanbase
{

namespace common
{

class ObObj;

} // namespace common

namespace pl
{

class ObPLExecCtx;

class ObDBMSPython
{
public:
  static int loadpython_mysql(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int droppython_mysql(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PYTHON_H_
