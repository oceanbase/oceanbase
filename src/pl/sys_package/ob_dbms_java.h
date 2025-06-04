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
