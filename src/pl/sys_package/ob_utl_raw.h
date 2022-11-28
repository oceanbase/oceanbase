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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_UTL_RAW_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_UTL_RAW_H_
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

#define UTL_RAW_FUNC_ARG_DECL sql::ObExecContext &exec_ctx, sql::ParamStore &params, \
                              common::ObObj &result

class ObUtlRaw
{
public:
  static int cast_to_raw(UTL_RAW_FUNC_ARG_DECL);
  static int cast_to_varchar2(UTL_RAW_FUNC_ARG_DECL);
  static int length(UTL_RAW_FUNC_ARG_DECL);
  static int bit_and(UTL_RAW_FUNC_ARG_DECL);
  static int bit_or(UTL_RAW_FUNC_ARG_DECL);
  static int bit_xor(UTL_RAW_FUNC_ARG_DECL);
  static int bit_complement(UTL_RAW_FUNC_ARG_DECL);
  static int copies(UTL_RAW_FUNC_ARG_DECL);
  static int compare(UTL_RAW_FUNC_ARG_DECL);
  static int concat(UTL_RAW_FUNC_ARG_DECL);
  static int substr(UTL_RAW_FUNC_ARG_DECL);
  static int reverse(UTL_RAW_FUNC_ARG_DECL);
  static int cast_from_binary_integer(UTL_RAW_FUNC_ARG_DECL);
  static int cast_to_binary_integer(UTL_RAW_FUNC_ARG_DECL);
  static int cast_from_binary_float(UTL_RAW_FUNC_ARG_DECL);
  static int cast_to_binary_float(UTL_RAW_FUNC_ARG_DECL);
  static int cast_from_binary_double(UTL_RAW_FUNC_ARG_DECL);
  static int cast_to_binary_double(UTL_RAW_FUNC_ARG_DECL);
  static int cast_from_number(UTL_RAW_FUNC_ARG_DECL);
  static int cast_to_number(UTL_RAW_FUNC_ARG_DECL);

private:
  enum BitOperator
  {
    BIT_AND,
    BIT_OR,
    BIT_XOR,
    BIT_MAX,
  };
  static int bitwise_calc(UTL_RAW_FUNC_ARG_DECL, const BitOperator op);
  // 1: big endian, 2 little_endian
  static int get_endianess() {
    const union { int32_t u; char c[4]; } one = { 1 };
    return 1 == one.c[0] ? 2 : 1;
  }

};

} // end of pl
} // end of oceanbase
#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_UTL_RAW_H_ */
