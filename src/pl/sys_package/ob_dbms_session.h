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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SESSION_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SESSION_H_

#include "sql/engine/ob_exec_context.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSSession
{
public:
  static int clear_all_context(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int clear_context(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int clear_identifier(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int set_context(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int set_identifier(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int reset_package(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
private:
  static int check_argument(const ObObj &input_param, bool allow_null,
                            bool need_case_up, int32_t param_idx,
                            int64_t max_len, ObString &output_param);
  static int check_client_id(const ObObj &input_param,
                             int64_t max_len,
                             ObString &output_param);
  static void try_caseup(ObCollationType cs_type, ObString &str_val);
  
  static int check_privileges(ObPLContext *pl_ctx,
                              const ObString &package_name,
                              const ObString &schema_name);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SESSION_H_ */