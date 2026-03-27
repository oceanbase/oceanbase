/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  static int is_role_enabled(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int session_is_role_enabled(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int current_is_role_enabled(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
private:
  static int check_argument(const ObObj &input_param, bool allow_null,
                            bool need_case_up, int32_t param_idx,
                            int64_t max_len, ObString &output_param,
                            ObIAllocator &alloc);
  static int check_client_id(const ObObj &input_param,
                             int64_t max_len,
                             ObString &output_param,
                             ObIAllocator &alloc);
  static int try_caseup(ObCollationType cs_type, ObString &str_val, ObIAllocator &alloc);
  
  static int check_privileges(ObPLContext *pl_ctx,
                              const ObString &package_name,
                              const ObString &schema_name);
  static int find_top_definer(ObPLContext *pl_ctx,
                              ObSchemaGetterGuard &schema_guard,
                              ObPLExecState *&exec_state);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_SESSION_H_ */