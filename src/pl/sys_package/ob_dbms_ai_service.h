/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_AI_SERVICE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_AI_SERVICE_H_


#include "sql/ob_sql_define.h"
#include "lib/json_type/ob_json_base.h"
#include "sql/privilege_check/ob_ai_model_priv_util.h"

namespace oceanbase
{

namespace common
{

class ObObj;

} // namespace common

namespace pl
{

class ObPLExecCtx;

class ObDBMSAiService
{
public:
  static int create_ai_model(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int drop_ai_model(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

  static int create_ai_model_endpoint(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int alter_ai_model_endpoint(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int drop_ai_model_endpoint(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
private:
  static int precheck_version_and_param_count_(int expect_param_count, sql::ParamStore &params);
  static int get_json_base_(ObArenaAllocator &allocator, sql::ParamStore &params, common::ObIJsonBase *&j_base);
  static int check_ai_model_privilege_(ObPLExecCtx &ctx, ObPrivSet required_priv);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_AI_SERVICE_H_
