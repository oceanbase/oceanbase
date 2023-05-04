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

#include "sql/engine/expr/ob_expr_agg_param_list.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprAggParamList::ObExprAggParamList(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_OP_AGG_PARAM_LIST, N_AGG_PARAM_LIST,
                         1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE,
                         INTERNAL_IN_ORACLE_MODE)
{
}

ObExprAggParamList::~ObExprAggParamList()
{
}

}/* ns sql*/
}/* ns oceanbase */




