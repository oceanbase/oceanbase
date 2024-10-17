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

#include "sql/engine/expr/ob_expr_cmp_func.ipp"

namespace oceanbase
{
namespace sql
{

#define DEF_COMPILE_STR_FUNC_INIT(COLLATION, unit_idx)                                                     \
  void __init_str_expr_cmp_func##unit_idx()                                                                \
  {                                                                                                        \
    StrExprFuncIniter<COLLATION, CO_EQ>::init_array();                                                     \
    StrExprFuncIniter<COLLATION, CO_LE>::init_array();                                                     \
    StrExprFuncIniter<COLLATION, CO_LT>::init_array();                                                     \
    StrExprFuncIniter<COLLATION, CO_GE>::init_array();                                                     \
    StrExprFuncIniter<COLLATION, CO_GT>::init_array();                                                     \
    StrExprFuncIniter<COLLATION, CO_NE>::init_array();                                                     \
    StrExprFuncIniter<COLLATION, CO_CMP>::init_array();                                                    \
    DatumStrExprCmpIniter<COLLATION>::init_array();                                                        \
    TextExprFuncIniter<COLLATION, CO_EQ>::init_array();                                                    \
    TextExprFuncIniter<COLLATION, CO_LE>::init_array();                                                    \
    TextExprFuncIniter<COLLATION, CO_LT>::init_array();                                                    \
    TextExprFuncIniter<COLLATION, CO_GE>::init_array();                                                    \
    TextExprFuncIniter<COLLATION, CO_GT>::init_array();                                                    \
    TextExprFuncIniter<COLLATION, CO_NE>::init_array();                                                    \
    TextExprFuncIniter<COLLATION, CO_CMP>::init_array();                                                   \
    DatumTextExprCmpIniter<COLLATION>::init_array();                                                       \
    TextStrExprFuncIniter<COLLATION, CO_EQ>::init_array();                                                 \
    TextStrExprFuncIniter<COLLATION, CO_LE>::init_array();                                                 \
    TextStrExprFuncIniter<COLLATION, CO_LT>::init_array();                                                 \
    TextStrExprFuncIniter<COLLATION, CO_GE>::init_array();                                                 \
    TextStrExprFuncIniter<COLLATION, CO_GT>::init_array();                                                 \
    TextStrExprFuncIniter<COLLATION, CO_NE>::init_array();                                                 \
    TextStrExprFuncIniter<COLLATION, CO_CMP>::init_array();                                                \
    DatumTextStrExprCmpIniter<COLLATION>::init_array();                                                    \
    StrTextExprFuncIniter<COLLATION, CO_EQ>::init_array();                                                 \
    StrTextExprFuncIniter<COLLATION, CO_LE>::init_array();                                                 \
    StrTextExprFuncIniter<COLLATION, CO_LT>::init_array();                                                 \
    StrTextExprFuncIniter<COLLATION, CO_GE>::init_array();                                                 \
    StrTextExprFuncIniter<COLLATION, CO_GT>::init_array();                                                 \
    StrTextExprFuncIniter<COLLATION, CO_NE>::init_array();                                                 \
    StrTextExprFuncIniter<COLLATION, CO_CMP>::init_array();                                                \
    DatumStrTextExprCmpIniter<COLLATION>::init_array();                                                    \
  }

} // end sql
} // end oceanbase