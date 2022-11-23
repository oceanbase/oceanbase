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

#ifndef OB_UDF_UTIL_H_
#define OB_UDF_UTIL_H_

#include <dlfcn.h>
#include "share/schema/ob_udf.h"
#include "sql/engine/user_defined_function/ob_user_defined_function.h"
#include "sql/engine/user_defined_function/ob_udf_registration_types.h"
#include "sql/engine/expr/ob_expr.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "sql/engine/expr/ob_expr_res_type.h"

namespace oceanbase
{
namespace common
{
class ObExprCtx;
class ObExprTypeCtx;
}
namespace sql
{

#define GET_AGG_UDF_ID(GROUP_ID, COL_COUNT, AGG_COUNT, AGG_IDX) \
    ((GROUP_ID)*((COL_COUNT)+(AGG_COUNT)) + AGG_IDX)

class ObPhysicalPlanCtx;
class obUdfConstArgs;
class ObUdfFunction;
class ObUdfUtil
{
public :
  /*
   * for example, if the defined func named 'my_udf_add', we wanna load the auxiliary function.
   * then we use 'my_udf_add_init' 'my_udf_add_deinit' 'my_udf_add_clear' or 'my_udf_add_add' to load
   * these functions (actually the max_expand_length is '_deinit\0').
   * */
  static const int UDF_MAX_EXPAND_LENGTH = 10;
  /* from mysql */
  static const int UDF_MAX_MBWIDTH = 3; /* Max multibyte sequence */
  static const int UDF_MAX_FIELD_CHARLENGTH = 255;
  static const int UDF_MAX_FIELD_WIDTH = (UDF_MAX_FIELD_CHARLENGTH * UDF_MAX_MBWIDTH + 1); /* Max column width +1 */
  static const int UDF_DECIMAL_BUFF_LENGTH = 9;   /** maximum length of buffer in our big digits (uint32). */
  static const int UDF_DECIMAL_MAX_POSSIBLE_PRECISION = (UDF_DECIMAL_BUFF_LENGTH * 9); /* the number of digits that my_decimal can possibly contain */
  static const int UDF_DECIMAL_MAX_STR_LENGTH = (UDF_DECIMAL_MAX_POSSIBLE_PRECISION + 2);
public :
  enum load_function_type {
    UDF_ORIGIN,
    UDF_INIT,
    UDF_DEINIT,
    UDF_CLEAR,
    UDF_ADD,
  };
  static const char *load_function_postfix[5];
  const char * get_postfix_name(load_function_type type) {
    return load_function_postfix[type];
  }
public :
  /*
   *  calc udf's result type
   * */
  static int calc_udf_result_type(common::ObIAllocator &allocator,
                                  const ObUdfFunction *udf_func,
                                  const share::schema::ObUDFMeta &udf_meta,
                                  const common::ObIArray<common::ObString> &udf_attributes,
                                  const common::ObIArray<ObExprResType> &udf_attributes_types,
                                  ObExprResType &type,
                                  ObExprResType *param_types,
                                  const int64_t param_num,
                                  common::ObExprTypeCtx &type_ctx);

  /*
   *  load all function to our server
   * */
  static void unload_so(ObUdfSoHandler &handler);
  static int load_so(const common::ObString dl, ObUdfSoHandler &handler);

  template <typename T>
  static int load_function(const common::ObString &name,
                           const ObUdfSoHandler handler,
                           const common::ObString &postfix,
                           const bool ignore_error,
                           T &func);

  /*
   *  helper function
   * */
  static int deep_copy_udf_args_cell(common::ObIAllocator &allocator, int64_t param_num, const common::ObObj *src_objs, common::ObObj *& dst_objs);
  static int set_udf_args(common::ObExprCtx &expr_ctx, int64_t param_num, common::ObObj *src_objs, ObUdfArgs &udf_args);
  // set the copied datum (with %alloc) to udf arg.
  static int set_udf_arg(common::ObIAllocator &alloc,
                         const ObDatum &src_datum,
                         const ObExpr &expr,
                         ObUdfArgs &udf_arg,
                         const int64_t arg_idx);

  static int init_udf_args(common::ObIAllocator &allocator,
                           const common::ObIArray<common::ObString> &udf_attributes,
                           const common::ObIArray<ObExprResType> &udf_attributes_types,
                           ObUdfArgs &udf_args);
  static int init_const_args(common::ObIAllocator &allocator,
                             const common::ObIArray<ObUdfConstArgs> &const_results,
                             ObUdfArgs &udf_args,
                             common::ObExprCtx &expr_ctx);
  static void construct_udf_args(ObUdfArgs &args);
  static void construct_udf_init(ObUdfInit &init);
  static const char* result_type_name(enum UdfItemResult result);
  static void print_udf_args_to_log(const ObUdfArgs &args);
  static void print_udf_init_to_log(const ObUdfInit &init);
  static void assign_udf_args(const ObUdfArgs &src_args, ObUdfArgs &dst_args);
  static void assign_udf_init(const ObUdfInit &src_init, ObUdfInit &dst_init);
  static int is_udf_ctx_valid(const ObUdfArgs &src_args, const ObUdfInit &dst_init);

  /*
   *  interface to invoke udf
   * */
  static int process_udf_func(share::schema::ObUDF::UDFRetType ret_type,
                              common::ObIAllocator &allocator,
                              ObUdfInit &udf_init,
                              ObUdfArgs &udf_args,
                              const ObUdfFuncAny func,
                              common::ObObj &result);
  static int process_str(common::ObIAllocator &allocator, ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAny func, common::ObObj &result);
  static int process_dec(common::ObIAllocator &allocator, ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAny func, common::ObObj &result);
  static int process_int(common::ObIAllocator &allocator, ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAny func, common::ObObj &result);
  static int process_real(common::ObIAllocator &allocator, ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAny func, common::ObObj &result);
  static int process_add_func(ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAdd func);
  static int process_clear_func(ObUdfInit &udf_init, const ObUdfFuncClear func);


  /*
   *  convert function, from ob type to mysql field type
   * */
  static int convert_ob_type_to_udf_type(common::ObObjType ob_type, UdfItemResult &udf_type);
  static int convert_mysql_type_to_udf_type(const obmysql::EMySQLFieldType &mysql_type, UdfItemResult &udf_type);

  // return ObNullType for invalid udf_type
  static common::ObObjType udf_type2obj_type(const UdfItemResult udf_type);
};

template <typename T>
int ObUdfUtil::load_function(const common::ObString &name,
                             const ObUdfSoHandler handler,
                             const common::ObString &postfix,
                             const bool ignore_error,
                             T &func)
{
  int ret = common::OB_SUCCESS;
  T func_tmp = nullptr;
  int func_len = common::OB_MAX_UDF_NAME_LENGTH + ObUdfUtil::UDF_MAX_EXPAND_LENGTH;
  char func_name[common::OB_MAX_UDF_NAME_LENGTH + ObUdfUtil::UDF_MAX_EXPAND_LENGTH];
  MEMSET(func_name, 0, func_len);
  MEMCPY(func_name, name.ptr(), name.length());
  char *postfix_start_pos = func_name + name.length();
  MEMCPY(postfix_start_pos, postfix.ptr(), postfix.length());
  if (OB_ISNULL(((func_tmp = (T)dlsym(handler, func_name))))) {
    ret = ignore_error ? common::OB_SUCCESS : common::OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "Can't find symbol", K(ret), K(name), K(func_name));
  } else {
    func = func_tmp;
    SQL_LOG(DEBUG, "get func_init function", K(handler), K(func));
  }
  return ret;
}

}
}

#endif
