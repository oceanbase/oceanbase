/*
 * Copyright (c) 2021 OceanBase Technology Co.,Ltd.
 * OceanBase is licensed under Mulan PubL v1.
 * You can use this software according to the terms and conditions of the Mulan PubL v1.
 * You may obtain a copy of Mulan PubL v1 at:
 *          http://license.coscl.org.cn/MulanPubL-1.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v1 for more details.
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_JSON_ARRAY_TYPE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_JSON_ARRAY_TYPE_H_

#include "lib/json_type/ob_json_base.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "pl/ob_pl_user_type.h"
#include "pl/sys_package/ob_json_pl_utils.h"
namespace oceanbase
{
namespace pl
{
class ObPlJsonArray {
public:
  static int constructor(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int parse(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_type(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int clone(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int set_on_error(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
private:
  static int get_array_value(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              ObJsonNode *&json_val,
                              ObPlJsonNode *&pl_json_node,
                              int& error_behavior,
                              int expect_param_nums = 2);
};

} // end pl
} // end oceanbase
#endif