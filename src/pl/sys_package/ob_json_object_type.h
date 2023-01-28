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


#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_JSON_OBJECT_TYPE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_JSON_OBJECT_TYPE_H_

#include "lib/json_type/ob_json_base.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "pl/ob_pl_user_type.h"
#include "pl/sys_package/ob_json_pl_utils.h"

namespace oceanbase
{
namespace pl
{
class ObPlJsonObject {
public:

  static const int64_t PL_MAX_JSN_KEY_LENGTH = 256;

  static int constructor(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int parse(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_object(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_element(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_string(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_number(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_date(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_timestamp(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_boolean(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_clob(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_blob(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_clob_proc(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_blob_proc(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_type(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put_varchar(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put_clob(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put_blob(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put_json(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put_boolean(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put_null(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int exist(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int remove(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int rename_key(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int clone(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int set_on_error(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

private:
  static int get_internal(sql::ObExecContext &ctx,
                          sql::ParamStore &params,
                          common::ObObj &result,
                          bool is_res_obj);

  static int get_object_value(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              ObJsonNode *&json_val,
                              int& error_behavior,
                              int expect_param_nums = 2);
  static int get_lob_proc(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result, bool is_clob);
  static int put_inner(sql::ObExecContext &ctx,
                       sql::ParamStore &params,
                       common::ObObj &result,
                       ObPlJsonUtil::PL_JSN_STRING_TYPE type);
  static int get_val_string(sql::ObExecContext &ctx,
                            sql::ParamStore &params,
                            common::ObObj &result,
                            ObPlJsonUtil::PL_JSN_STRING_TYPE str_type = ObPlJsonUtil::PL_DEFAULT_STR_TYPE);

  static int get_json_val(sql::ObExecContext &ctx, ObObj &data, ObIJsonBase*& json_val, ObPlJsonUtil::PL_JSN_STRING_TYPE type);
};

} // end pl
} // end oceanbase

#endif
