/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file is for implementation of func json_append
 */


#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_append.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonAppend::ObExprJsonAppend(ObIAllocator &alloc)
    : ObExprJsonArrayAppend(alloc,
      T_FUN_SYS_JSON_APPEND,
      N_JSON_APPEND,
      MORE_THAN_TWO,
      VALID_FOR_GENERATED_COL,
      NOT_ROW_DIMENSION)
{
}

ObExprJsonAppend::~ObExprJsonAppend()
{
}

}
}