/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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




