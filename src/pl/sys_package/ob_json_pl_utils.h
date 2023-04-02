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


#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_JSON_PL_UTILS_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_JSON_PL_UTILS_H_

#include "lib/json_type/ob_json_tree.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace pl
{

class ObPlJsonUtil {
  enum {
    JSN_PL_ELEMENT_TYPE_ID = 300023,
    JSN_PL_OBJECT_TYPE_ID = 300024
  };

public:
  enum PL_JSN_STRING_TYPE {
    PL_DEFAULT_STR_TYPE,
    PL_JSN_CLOB_TYPE,
    PL_JSN_BLOB_TYPE,
    PL_JSN_JSON_TYPE,
  } ;

  static int parse(sql::ObExecContext &ctx, sql::ParamStore &params,
                   common::ObObj &result, ObJsonNodeType expect_type = ObJsonNodeType::J_ERROR);
  static int get_jsontree(sql::ObExecContext &ctx, ObObj &obj, ObJsonNode *&json_doc, int32_t &err_behavior);
  static int get_jsontype(sql::ObExecContext &ctx,
                          sql::ParamStore &params,
                          ObJsonNodeType &json_type,
                          int &error_behavior);
  static int check_json_type(sql::ObExecContext &ctx, sql::ParamStore &params,
                             ObJsonNodeType json_type, common::ObObj &result);
  static int make_jsontype(sql::ObExecContext &ctx, const ObString &str,
                           ObJsonInType in_type, ObJsonNodeType expect_type,
                           ObPLJsonBaseType *&jsontype);
  static int transform_JsonBase_2_PLJsonType(sql::ObExecContext &ctx,
                                             ObJsonNode* json_val,
                                             ObPLJsonBaseType *&jsontype);
  static int print_decimal(number::ObNumber &num, ObScale scale, ObJsonBuffer &j_buf);
  static int get_json_object(sql::ObExecContext &ctx, ObJsonNode*& json_val);
  static int get_json_boolean(sql::ObExecContext &ctx, ObObj &data, ObJsonNode*& json_val);
  static int get_json_null(sql::ObExecContext &ctx, ObJsonNode*& json_val);
  static int set_on_error(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int check_on_error(ObObj &obj, int& err_val);
  static bool proc_on_error(common::ObObj &result, int error_behavior, int &ret);
  static bool is_pl_jsontype(int64_t id) {
    return (id == JSN_PL_ELEMENT_TYPE_ID || id == JSN_PL_OBJECT_TYPE_ID);
  }

  static bool is_hex(const uint8_t input);
  static bool is_hex_string(const ObString& hex_str);
  static bool is_pl_json_element_type(int64_t id) { return (id == JSN_PL_ELEMENT_TYPE_ID); }
  static bool is_pl_json_object_type(int64_t id) { return (id == JSN_PL_OBJECT_TYPE_ID); }

  static int str_to_lob_storage_obj(ObIAllocator &allocator, const ObString& input, common::ObObj& output);

  static int get_lob_inner(ObIAllocator& allocator, const ObString& val_str, ObPlJsonUtil::PL_JSN_STRING_TYPE type, ObIJsonBase*& j_base);
};

} // end pl
} // end oceanbase

#endif
