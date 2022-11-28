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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SYS_ERROR_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SYS_ERROR_H_

#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSSysError
{
public:
  ObDBMSSysError() {}
  virtual ~ObDBMSSysError() {}

#define DECLARE_FUNC(func) \
  static int func( \
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(ere0);
  DECLARE_FUNC(ere1);
  DECLARE_FUNC(ere2);
  DECLARE_FUNC(ere3);
  DECLARE_FUNC(ere4);
  DECLARE_FUNC(ere5);
  DECLARE_FUNC(ere6);
  DECLARE_FUNC(ere7);
  DECLARE_FUNC(ere8);

#undef DECLARE_FUNC

};

} // pl
} // oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SYS_ERROR_H_