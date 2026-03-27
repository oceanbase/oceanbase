/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_JAVA_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_JAVA_H_


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

class ObDBMSJava
{
public:
  static int loadjava_mysql(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int dropjava_mysql(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_JAVA_H_
