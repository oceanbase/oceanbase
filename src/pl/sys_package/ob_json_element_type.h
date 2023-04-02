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


#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_JSON_ELEMENT_TYPE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_JSON_ELEMENT_TYPE_H_

#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "pl/ob_pl_user_type.h"
#include "pl/sys_package/ob_json_pl_utils.h"

namespace oceanbase
{
namespace pl
{

class ObPlJsonElement {
public:
  static int parse(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_object(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_array(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_scalar(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_string(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_number(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_boolean(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_true(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_false(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_null(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_date(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int is_timestamp(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_string(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_number(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_date(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_timestamp(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_boolean(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_clob(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_clob_proc(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_blob(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int to_blob_proc(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_size(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int set_on_error(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
private:
  // static int to_ora_date_str(ObString str, ObJsonBuffer &j_buf);
  static int to_str(sql::ObExecContext &ctx, sql::ParamStore &params,
                    common::ObObj &result, ObPlJsonUtil::PL_JSN_STRING_TYPE type = ObPlJsonUtil::PL_DEFAULT_STR_TYPE);
  static int to_lob_proc(sql::ObExecContext &ctx, sql::ParamStore &params,
                         common::ObObj &result, ObPlJsonUtil::PL_JSN_STRING_TYPE type = ObPlJsonUtil::PL_DEFAULT_STR_TYPE);

};
} // end pl
} // end oceanbase

#endif
